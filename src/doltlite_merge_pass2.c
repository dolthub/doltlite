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
        SchemaEntry *pAncSe = findSchemaEntry(
            aAncSchema, nAncSchema, pTheirSe->zName);
        SchemaEntry *pOurSe = findSchemaEntry(
            aOursSchema, nOursSchema, pTheirSe->zName);
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
         ){
          continue;
        }
        if( mergeIndexFollowsDualRename(
              aAncSchema, nAncSchema, aOursSchema, nOursSchema,
              aTheirsSchema, nTheirsSchema, pAncSe, pOurSe, pTheirSe) ){
          continue;
        }
        /* An index of theirs over a column we dropped or renamed away cannot
        ** be adopted: the merged table has no such column, so the catalog it
        ** would produce cannot be loaded at all. Dolt drops the index with
        ** the column, so leave it behind. */
        {
          SchemaEntry *pAncTbl = findSchemaEntry(
              aAncSchema, nAncSchema, pTheirSe->zTblName);
          SchemaEntry *pOurTbl = findSchemaEntry(
              aOursSchema, nOursSchema, pTheirSe->zTblName);
          char *zGone = 0;
          if( pAncTbl && pOurTbl
           && mergeIndexColumnGoneFrom(pTheirSe->zSql, pAncTbl->zSql,
                                           pOurTbl->zSql, &zGone) ){
            sqlite3_free(zGone);
            continue;
          }
          sqlite3_free(zGone);
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

/* Is this fts table contentless? Such a table has no authoritative copy of
** the indexed text -- the index IS the data -- so nothing can regenerate a
** discarded side, and fts5 refuses 'rebuild' on one outright. */
static int mergeFtsIsContentless(Table *pTab){
  int i;
  for(i=3; i<pTab->u.vtab.nArg; i++){
    const char *z = pTab->u.vtab.azArg[i];
    const char *zEq;
    if( !z ) continue;
    while( *z==' ' ) z++;
    if( sqlite3_strnicmp(z, "content", 7)!=0 ) continue;
    zEq = &z[7];
    while( *zEq==' ' ) zEq++;
    if( *zEq!='=' ) continue;
    zEq++;
    while( *zEq==' ' ) zEq++;
    /* content='' names no table; content='x' names an authoritative one. */
    if( (zEq[0]=='\'' && zEq[1]=='\'') || (zEq[0]=='"' && zEq[1]=='"')
     || zEq[0]==0 ){
      return 1;
    }
    return 0;
  }
  return 0;
}

/* Classify zTable as a derived shadow of a virtual table: a shadow holding
** state that is regenerated from data living elsewhere. Returns the malloc'd
** owner name and sets *pbRebuildable to whether that regeneration is actually
** possible. A shadow that is itself authoritative (%_content, %_base) is not
** derived and is never reported here -- a conflict there is a conflict in the
** data, which stays loud. */
static char *mergeDerivedShadowOwner(
  sqlite3 *db,
  const char *zTable,
  int *pbRebuildable
){
  static const char *azSuffix[] = {
    /* vec1 */   "_idx", "_meta", "_model", "_config",
    /* fts5 */   "_data", "_docsize",
    /* fts3/4 */ "_segments", "_segdir", "_stat",
    /* rtree */  "_node", "_parent", "_rowid"
  };
  unsigned int i;
  size_t nTable = strlen(zTable);

  *pbRebuildable = 0;
  for(i=0; i<sizeof(azSuffix)/sizeof(azSuffix[0]); i++){
    size_t nSfx = strlen(azSuffix[i]);
    const char *zModule;
    char *zOwner;
    Table *pTab;
    if( nTable<=nSfx || strcmp(&zTable[nTable-nSfx], azSuffix[i])!=0 ){
      continue;
    }
    zOwner = sqlite3_mprintf("%.*s", (int)(nTable-nSfx), zTable);
    if( !zOwner ) return 0;
    pTab = sqlite3FindTable(db, zOwner, "main");
    if( !pTab || !IsVirtual(pTab) || pTab->u.vtab.nArg<=0
     || !sqlite3IsShadowTableOf(db, pTab, zTable) ){
      sqlite3_free(zOwner);
      return 0;
    }
    zModule = pTab->u.vtab.azArg[0];

    if( sqlite3_stricmp(zModule, "fts5")==0
     || sqlite3_stricmp(zModule, "fts4")==0
     || sqlite3_stricmp(zModule, "fts3")==0 ){
      /* The merged content is authoritative, so a rebuild regenerates the
      ** index over both sides' rows. Contentless has no such content. */
      *pbRebuildable = !mergeFtsIsContentless(pTab);
      return zOwner;
    }
    if( sqlite3_stricmp(zModule, "rtree")==0
     || sqlite3_stricmp(zModule, "rtree_i32")==0 ){
      /* An r-tree keeps its coordinates in %_node and offers no rebuild, so
      ** a conflicting node is a conflict in the data itself. */
      *pbRebuildable = 0;
      return zOwner;
    }
    if( sqlite3_stricmp(zModule, "vec1")!=0 ){
      sqlite3_free(zOwner);
      return 0;
    }
    {
      /* Eligible only when %_base still holds raw vectors AND the stored
      ** model the rebuild depends on is present: vec1 treats a NULL
      ** rebuild argument as "keep the current model" and proceeds on
      ** cached state, so a missing model row would let the merge commit
      ** a stale index instead of failing. */
      /* Eligible only when %_base still holds raw vectors AND the stored
      ** model the rebuild depends on is present: vec1 treats a NULL
      ** rebuild argument as "keep the current model" and proceeds on
      ** cached state, so a missing model row would let the merge commit
      ** a stale index instead of failing. Uncompressed builds store bucket
      ** numbers in %_base instead of vectors, so auto-resolving their index
      ** conflicts would silently lose the discarded side's vectors. */
      char *zQry = sqlite3_mprintf(
          "SELECT (SELECT count(*) FROM \"%w_base\""
          "         WHERE typeof(vector)!='blob')=0"
          " AND EXISTS(SELECT 1 FROM \"%w_model\""
          "             WHERE id=1 AND typeof(val)='blob' AND length(val)>0)",
          zOwner, zOwner);
      sqlite3_stmt *pStmt = 0;
      int bEligible = 0;
      if( zQry && sqlite3_prepare_v2(db, zQry, -1, &pStmt, 0)==SQLITE_OK ){
        bEligible = sqlite3_step(pStmt)==SQLITE_ROW
                 && sqlite3_column_int(pStmt, 0)==1;
      }
      sqlite3_finalize(pStmt);
      sqlite3_free(zQry);
      if( bEligible ){
        *pbRebuildable = 1;
        return zOwner;
      }
      /* An ineligible vec1 shadow keeps its conflicts resolvable, which is
      ** this module's existing contract; it is not reported as derived. */
      sqlite3_free(zOwner);
      return 0;
    }
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
  int *pnRebuild,
  char **pzRefuse
){
  int i, j, rc = SQLITE_OK;
  char **azOwner = 0;

  azOwner = sqlite3_malloc((*pnConflictTables)*(int)sizeof(char*));
  if( !azOwner ) return SQLITE_NOMEM;
  memset(azOwner, 0, (*pnConflictTables)*sizeof(char*));

  for(i=0; i<*pnConflictTables; i++){
    int bRebuildable = 0;
    if( aConflictTables[i].nSchemaObjects>0
     || aConflictTables[i].nConflicts<=0
     || (azOwner[i] = mergeDerivedShadowOwner(
            db, aConflictTables[i].zName, &bRebuildable))==0 ){
      doltliteFreeNameList(azOwner, *pnConflictTables);
      return SQLITE_OK;
    }
    if( !bRebuildable ){
      /* Neither resolution can be right: the discarded side's rows are gone
      ** from an index nothing can regenerate, and committing either one puts
      ** an index that disagrees with its own table into history. Refuse. */
      if( pzRefuse && *pzRefuse==0 ){
        *pzRefuse = sqlite3_mprintf(
            "cannot merge: '%s' indexes data that cannot be rebuilt from the "
            "merged rows, and both sides changed it. Merge on one branch, or "
            "drop and recreate '%s' after merging",
            azOwner[i], azOwner[i]);
      }
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
