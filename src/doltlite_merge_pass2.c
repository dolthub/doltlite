#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Catalog merge pass 2: adopt theirs-only tables/indexes, rootpage remap. */

int mergeCatalogPass2(
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  Pgno *piNextMerged,
  int bDisjointSchemaChanges,
  MergeConflictTable *aConflictTables,
  int nConflictTables,
  SchemaRootpageRemap **ppaRemap,
  int *pnRemap,
  char ***pazReindex,
  int *pnReindex
){
  int i, rc = SQLITE_OK;

  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 ) continue;

    if( !zName ){
      SchemaEntry *pTheirSe = findSchemaEntryByRootpage(
          aTheirsSchema, nTheirsSchema, aTheirs[i].iTable);
      if( pTheirSe && pTheirSe->zName && pTheirSe->zType
       && strcmp(pTheirSe->zType, "index")==0 ){
        struct TableEntry *pTheirTable = doltliteFindTableByName(
            aTheirs, nTheirs, pTheirSe->zTblName);
        if( hasSchemaConflictObject(aConflictTables, nConflictTables,
                                    pTheirSe->zName)
         || hasSchemaConflictObject(aConflictTables, nConflictTables,
                                    pTheirSe->zTblName)
         || hasSchemaConflictTable(aConflictTables, nConflictTables,
                                   pTheirSe->zTblName)
         || !pTheirTable
         || !doltliteFindTableByName(aMerged, *pnMerged,
                                     pTheirSe->zTblName)
         || mergeTableRenameOtherDrop(
              aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
              aAncSchema, nAncSchema, aTheirsSchema, nTheirsSchema,
              pTheirTable) ){
          continue;
        }
        oursEntry = findCatalogEntryBySchemaObject(
            aOurs, nOurs, aOursSchema, nOursSchema,
            pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
        if( oursEntry ) continue;

        {
          struct TableEntry *ancEntry = findCatalogEntryBySchemaObject(
              aAnc, nAnc, aAncSchema, nAncSchema,
              pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
          if( !ancEntry ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            int forceRemap = bDisjointSchemaChanges;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict || forceRemap ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                                     (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
            aMerged[(*pnMerged)++] = newEntry;
            /* The adopted tree covers only theirs' rows; ours' row changes
            ** never touched it. Record the index for a rebuild over the
            ** merged table once the merged catalog is live. */
            rc = mergeAppendReindexName(pazReindex, pnReindex, pTheirSe->zName);
            if( rc!=SQLITE_OK ) return rc;
          }else if( schemaEntryChangedByName(
                        aAncSchema, nAncSchema,
                        aTheirsSchema, nTheirsSchema,
                        pTheirSe->zName) ){
            struct TableEntry newEntry = aTheirs[i];
            int j, conflict = 0;
            Pgno oldPg = newEntry.iTable;
            for(j=0; j<*pnMerged; j++){
              if( aMerged[j].iTable==newEntry.iTable ){
                conflict = 1;
                break;
              }
            }
            if( conflict ){
              SchemaRootpageRemap *aNew;
              int nOld = *pnRemap;
              newEntry.iTable = (*piNextMerged)++;
              aNew = sqlite3_realloc(*ppaRemap,
                  (nOld+1)*(int)sizeof(SchemaRootpageRemap));
              if( !aNew ) return SQLITE_NOMEM;
              *ppaRemap = aNew;
              aNew[nOld].oldPg = oldPg;
              aNew[nOld].newPg = newEntry.iTable;
              *pnRemap = nOld + 1;
            }
            if( newEntry.iTable >= *piNextMerged ){
              *piNextMerged = newEntry.iTable + 1;
            }
            aMerged[(*pnMerged)++] = newEntry;
            rc = mergeAppendReindexName(pazReindex, pnReindex, pTheirSe->zName);
            if( rc!=SQLITE_OK ) return rc;
          }else{
            /* An unmodified index does not override our explicit DROP. */
          }
        }
        continue;
      }

      if( !doltliteFindTableByNumber(aOurs, nOurs, aTheirs[i].iTable) ){
        struct TableEntry newEntry = aTheirs[i];
        int j, conflict = 0;
        Pgno oldPg = newEntry.iTable;
        int forceRemap = bDisjointSchemaChanges;
        for(j=0; j<*pnMerged; j++){
          if( aMerged[j].iTable==newEntry.iTable ){
            conflict = 1;
            break;
          }
        }
        if( conflict || forceRemap ){
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = oldPg;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;
        aMerged[(*pnMerged)++] = newEntry;
      }else if( bDisjointSchemaChanges ){
        struct TableEntry *oursIdx = doltliteFindTableByNumber(aOurs, nOurs, aTheirs[i].iTable);
        struct TableEntry *ancIdx = doltliteFindTableByNumber(aAnc, nAnc, aTheirs[i].iTable);
        if( oursIdx && !ancIdx
         && (prollyHashCompare(&oursIdx->root, &aTheirs[i].root)!=0
             || prollyHashCompare(&oursIdx->schemaHash, &aTheirs[i].schemaHash)!=0) ){
          struct TableEntry newEntry = aTheirs[i];
          SchemaRootpageRemap *aNew;
          int nOld = *pnRemap;
          Pgno newPg = *piNextMerged;
          aNew = sqlite3_realloc(*ppaRemap,
                                 (nOld+1)*(int)sizeof(SchemaRootpageRemap));
          if( !aNew ) return SQLITE_NOMEM;
          *ppaRemap = aNew;
          aNew[nOld].oldPg = aTheirs[i].iTable;
          aNew[nOld].newPg = newPg;
          *pnRemap = nOld + 1;
          newEntry.iTable = newPg;
          (*piNextMerged)++;
          aMerged[(*pnMerged)++] = newEntry;
        }
      }
      continue;
    }

    if( hasSchemaConflictObject(aConflictTables, nConflictTables, zName) ){
      continue;
    }
    if( mergeTableRenameOtherDrop(
          aAnc, nAnc, aOurs, nOurs, aTheirs, nTheirs,
          aAncSchema, nAncSchema, aTheirsSchema, nTheirsSchema,
          &aTheirs[i]) ){
      continue;
    }
    oursEntry = doltliteFindTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;

    {
      struct TableEntry *ancEntry = doltliteFindTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){

        struct TableEntry newEntry = aTheirs[i];
        int forceRemap = bDisjointSchemaChanges;
        {
          int j, conflict = 0;
          for(j=0; j<*pnMerged; j++){
            if( aMerged[j].iTable==newEntry.iTable ){ conflict = 1; break; }
          }
          if( conflict || forceRemap ) newEntry.iTable = (*piNextMerged)++;
        }
        if( newEntry.iTable >= *piNextMerged ) *piNextMerged = newEntry.iTable + 1;

        newEntry.zName = sqlite3_mprintf("%s", zName);
        aMerged[(*pnMerged)++] = newEntry;
      }else{

        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          return SQLITE_ERROR;
        }

      }
    }
  }
  return SQLITE_OK;
}

/* If zTable is a derived shadow (%_idx, %_meta, %_model, %_config) of a
** vec1 virtual table whose %_base still holds raw vectors, return the
** malloc'd owner name; else NULL. Uncompressed builds store bucket
** numbers in %_base instead of vectors, so auto-resolving their index
** conflicts would silently lose the discarded side's vectors — those
** stay loud. %_base itself is authoritative and is never derived. */
static char *mergeVec1DerivedShadowOwner(sqlite3 *db, const char *zTable){
  static const char *azSuffix[] = { "_idx", "_meta", "_model", "_config" };
  unsigned int i;
  size_t nTable = strlen(zTable);

  for(i=0; i<sizeof(azSuffix)/sizeof(azSuffix[0]); i++){
    size_t nSfx = strlen(azSuffix[i]);
    char *zOwner;
    Table *pTab;
    if( nTable<=nSfx || strcmp(&zTable[nTable-nSfx], azSuffix[i])!=0 ){
      continue;
    }
    zOwner = sqlite3_mprintf("%.*s", (int)(nTable-nSfx), zTable);
    if( !zOwner ) return 0;
    pTab = sqlite3FindTable(db, zOwner, "main");
    if( pTab && IsVirtual(pTab)
     && pTab->u.vtab.nArg>0
     && sqlite3_stricmp(pTab->u.vtab.azArg[0], "vec1")==0
     && sqlite3IsShadowTableOf(db, pTab, zTable) ){
      char *zQry = sqlite3_mprintf(
          "SELECT count(*) FROM \"%w_base\" WHERE typeof(vector)!='blob'",
          zOwner);
      sqlite3_stmt *pStmt = 0;
      int bRaw = 0;
      if( zQry && sqlite3_prepare_v2(db, zQry, -1, &pStmt, 0)==SQLITE_OK ){
        bRaw = sqlite3_step(pStmt)==SQLITE_ROW
            && sqlite3_column_int64(pStmt, 0)==0;
      }
      sqlite3_finalize(pStmt);
      sqlite3_free(zQry);
      if( bRaw ) return zOwner;
    }
    sqlite3_free(zOwner);
    return 0;
  }
  return 0;
}

/* Derived-shadow merge policy: when EVERY conflict in the merge is a
** rebuildable vec1 shadow, the conflicts carry nothing a rebuild from the
** merged %_base and stored model does not regenerate. Drop them (the
** merged roots already hold ours' rows, exactly what --ours resolution
** leaves) and hand the owners back so the caller can rebuild once the
** merged catalog is live. Any non-derived conflict keeps every conflict
** loud, today's behavior. */
int mergeFilterDerivedShadowConflicts(
  sqlite3 *db,
  MergeConflictTable *aConflictTables,
  int *pnConflictTables,
  int *pTotalConflicts,
  char ***pazRebuild,
  int *pnRebuild
){
  int i, j, rc = SQLITE_OK;
  char **azOwner = 0;

  azOwner = sqlite3_malloc((*pnConflictTables)*(int)sizeof(char*));
  if( !azOwner ) return SQLITE_NOMEM;
  memset(azOwner, 0, (*pnConflictTables)*sizeof(char*));

  for(i=0; i<*pnConflictTables; i++){
    if( aConflictTables[i].nSchemaObjects>0
     || aConflictTables[i].nConflicts<=0
     || (azOwner[i] = mergeVec1DerivedShadowOwner(
            db, aConflictTables[i].zName))==0 ){
      doltliteFreeNameList(azOwner, *pnConflictTables);
      return SQLITE_OK;
    }
  }

  for(i=0; i<*pnConflictTables && rc==SQLITE_OK; i++){
    int seen = 0;
    for(j=0; j<i; j++){
      if( strcmp(azOwner[j], azOwner[i])==0 ){ seen = 1; break; }
    }
    if( !seen ){
      rc = mergeAppendReindexName(pazRebuild, pnRebuild, azOwner[i]);
    }
  }
  doltliteFreeNameList(azOwner, *pnConflictTables);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<*pnConflictTables; i++){
    sqlite3_free(aConflictTables[i].zName);
    aConflictTables[i].zName = 0;
    freeConflictRows(aConflictTables[i].aRows, aConflictTables[i].nConflicts);
    aConflictTables[i].aRows = 0;
    aConflictTables[i].nConflicts = 0;
  }
  *pnConflictTables = 0;
  *pTotalConflicts = 0;
  return SQLITE_OK;
}

#endif
