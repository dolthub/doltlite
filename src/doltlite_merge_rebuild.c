#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Assembly of the merged catalog's schema rows: the master root is
** rewritten from the merge's own schema arrays so the serialized blob
** never depends on this connection's live schema. */

static Pgno remapSchemaRootpage(
  SchemaRootpageRemap *aRemap,
  int nRemap,
  Pgno iRootpage
){
  int i;
  for(i=0; i<nRemap; i++){
    if( aRemap[i].oldPg==iRootpage ) return aRemap[i].newPg;
  }
  return iRootpage;
}

static int tableSqlNeedsClusteredPkAutoindex(const char *zSql){
  const char *z;
  if( !zSql ) return 0;
  for(z=zSql; *z; z++){
    if( sqlite3_strnicmp(z, "INTEGER PRIMARY KEY", 19)==0 ) return 0;
  }
  for(z=zSql; *z; z++){
    if( sqlite3_strnicmp(z, "PRIMARY KEY", 11)==0 ) return 1;
  }
  return 0;
}

static int schemaHasName(SchemaEntry *a, int n, const char *zName){
  int i;
  if( !zName ) return 0;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ) return 1;
  }
  return 0;
}

/* UNIQUE sqlite_autoindex_<table>_1 is a physical btree. Clustered PK
** autoindexes are catalog-only and must not reuse that name. */
static char *mergedClusteredPkAutoindexName(
  const char *zTable,
  SchemaEntry *aAnc, int nAnc,
  SchemaEntry *aOurs, int nOurs,
  SchemaEntry *aTheirs, int nTheirs
){
  int n = 1;
  for(;;){
    char *z = sqlite3_mprintf("sqlite_autoindex_%s_%d", zTable, n);
    if( !z ) return 0;
    if( !schemaHasName(aAnc, nAnc, z)
     && !schemaHasName(aOurs, nOurs, z)
     && !schemaHasName(aTheirs, nTheirs, z) ){
      return z;
    }
    sqlite3_free(z);
    n++;
    if( n>1000 ) return 0;
  }
}

static u8 *mergeBuildSchemaCatalogRecord(
  const char *zType,
  const char *zName,
  const char *zTblName,
  i64 iRootpage,
  const char *zSql,
  int *pnOut
){
  DoltliteSerialValue aMem[5];

  memset(aMem, 0, sizeof(aMem));

  aMem[0].eType = SQLITE_TEXT;    aMem[0].p = zType;    aMem[0].n = (int)strlen(zType);
  aMem[1].eType = SQLITE_TEXT;    aMem[1].p = zName;    aMem[1].n = (int)strlen(zName);
  aMem[2].eType = SQLITE_TEXT;    aMem[2].p = zTblName; aMem[2].n = (int)strlen(zTblName);
  aMem[3].eType = SQLITE_INTEGER; aMem[3].i = iRootpage;
  if( zSql ){
    aMem[4].eType = SQLITE_TEXT;
    aMem[4].p = zSql;
    aMem[4].n = (int)strlen(zSql);
  }else{
    aMem[4].eType = SQLITE_NULL;
    aMem[4].p = 0;
    aMem[4].n = 0;
  }

  return doltliteBuildRecord(aMem, 5, pnOut);
}

static SchemaEntry *mergedSchemaChoice(
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  const char *zName
){
  SchemaEntry *pAnc = findSchemaEntry(aAncSchema, nAncSchema, zName);
  SchemaEntry *pOurs = findSchemaEntry(aOursSchema, nOursSchema, zName);
  SchemaEntry *pTheirs = findSchemaEntry(aTheirsSchema, nTheirsSchema, zName);
  int oursChanged = schemaEntryChangedByName(aAncSchema, nAncSchema,
                                             aOursSchema, nOursSchema, zName);
  int theirsChanged = schemaEntryChangedByName(aAncSchema, nAncSchema,
                                               aTheirsSchema, nTheirsSchema, zName);
  if( hasSchemaConflictObject(aConflictTables, nConflictTables, zName) ){
    return pOurs;
  }
  if( oursChanged && !theirsChanged ) return pOurs;
  if( theirsChanged && !oursChanged ) return pTheirs;
  /* Both sides dropped it: ancestor fallback would resurrect it. */
  if( oursChanged && theirsChanged && !pOurs && !pTheirs ) return 0;
  if( pOurs ) return pOurs;
  if( pTheirs ) return pTheirs;
  return pAnc;
}

static int appendMergedSchemaCatalogRecord(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 iRowid,
  const SchemaEntry *pSe,
  Pgno iRootpage
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  u8 *pRec = 0;
  int nRec = 0;
  int rc;

  if( !pSe || !pSe->zName || !pSe->zType ) return SQLITE_OK;
  pRec = mergeBuildSchemaCatalogRecord(pSe->zType, pSe->zName,
                                  pSe->zTblName ? pSe->zTblName : pSe->zName,
                                  (i64)iRootpage, pSe->zSql, &nRec);
  if( !pRec ) return SQLITE_NOMEM;
  rc = prollyMutateInsert(cs, pCache, pRoot, flags, 0, 0,
                          iRowid, pRec, nRec, pRoot);
  sqlite3_free(pRec);
  return rc;
}

static int appendMergedHiddenIndexRow(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 *piNextRowid,
  struct TableEntry *aMerged, int nMerged,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap,
  const char *zName
){
  SchemaEntry *pSe;
  Pgno iRootpage;

  if( !zName ) return SQLITE_OK;
  pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                           aOursSchema, nOursSchema,
                           aTheirsSchema, nTheirsSchema,
                           aConflictTables, nConflictTables,
                           zName);
  if( !pSe || !pSe->zType || !pSe->zName ) return SQLITE_OK;
  if( strcmp(pSe->zType, "index")!=0 ) return SQLITE_OK;
  if( pSe->zSql ) return SQLITE_OK;
  if( !pSe->zTblName
   || !doltliteFindTableByName(aMerged, nMerged, pSe->zTblName) ){
    return SQLITE_OK;
  }

  iRootpage = pSe->iRootpage;
  if( pSe>=aTheirsSchema && pSe<aTheirsSchema+nTheirsSchema ){
    iRootpage = remapSchemaRootpage(aRemap, nRemap, iRootpage);
  }
  return appendMergedSchemaCatalogRecord(db, pRoot, flags, (*piNextRowid)++, pSe, iRootpage);
}

static int appendMergedAuxSchemaRow(
  sqlite3 *db,
  ProllyHash *pRoot,
  u8 flags,
  i64 *piNextRowid,
  struct TableEntry *aMerged, int nMerged,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap,
  const char *zName
){
  SchemaEntry *pSe;
  Pgno iRootpage;

  if( !zName ) return SQLITE_OK;
  pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                           aOursSchema, nOursSchema,
                           aTheirsSchema, nTheirsSchema,
                           aConflictTables, nConflictTables,
                           zName);
  if( !pSe || !pSe->zType || !pSe->zName ) return SQLITE_OK;
  /* Virtual tables have no catalog entry; their schema rows (type "table",
  ** rootpage 0) ride with storage-free objects here. */
  if( strcmp(pSe->zType, "index")==0 ) return SQLITE_OK;
  if( strcmp(pSe->zType, "table")==0 && pSe->iRootpage!=0 ){
    return SQLITE_OK;
  }
  /* A trigger follows its table: when the merge drops the table, the
  ** trigger goes with it rather than orphaning the catalog. Views are
  ** resolved lazily and stay, matching how a plain DROP TABLE leaves them. */
  if( strcmp(pSe->zType, "trigger")==0
   && pSe->zTblName
   && sqlite3_stricmp(pSe->zTblName, pSe->zName)!=0
   && !doltliteFindTableByName(aMerged, nMerged, pSe->zTblName) ){
    return SQLITE_OK;
  }

  iRootpage = pSe->iRootpage;
  if( pSe>=aTheirsSchema && pSe<aTheirsSchema+nTheirsSchema ){
    iRootpage = remapSchemaRootpage(aRemap, nRemap, iRootpage);
  }
  return appendMergedSchemaCatalogRecord(db, pRoot, flags, (*piNextRowid)++, pSe, iRootpage);
}

int rebuildDisjointSchemaRows(
  sqlite3 *db,
  struct TableEntry *aMerged, int nMerged,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  MergeConflictTable *aConflictTables, int nConflictTables,
  SchemaRootpageRemap *aRemap, int nRemap
){
  struct TableEntry *pMaster = 0;
  ProllyHash root;
  i64 iNextRowid = 1;
  int i, rc = SQLITE_OK;

  for(i=0; i<nMerged; i++){
    if( aMerged[i].iTable==1 ){
      pMaster = &aMerged[i];
      break;
    }
  }
  if( !pMaster ) return SQLITE_OK;
  memset(&root, 0, sizeof(root));

  for(i=0; i<nMerged; i++){
    const char *zName = aMerged[i].zName;
    SchemaEntry *pSe = 0;

    if( aMerged[i].iTable<=1 || !zName ) continue;
    pSe = mergedSchemaChoice(aAncSchema, nAncSchema,
                             aOursSchema, nOursSchema,
                             aTheirsSchema, nTheirsSchema,
                             aConflictTables, nConflictTables,
                             zName);
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, aMerged[i].iTable);
    if( rc!=SQLITE_OK ) return rc;
    if( pSe && pSe->zType && strcmp(pSe->zType, "table")==0
     && pSe->zSql && tableSqlNeedsClusteredPkAutoindex(pSe->zSql) ){
      SchemaEntry autoIdx;
      char *zAuto = mergedClusteredPkAutoindexName(
          zName, aAncSchema, nAncSchema, aOursSchema, nOursSchema,
          aTheirsSchema, nTheirsSchema);
      if( !zAuto ) return SQLITE_NOMEM;
      memset(&autoIdx, 0, sizeof(autoIdx));
      autoIdx.zType = "index";
      autoIdx.zName = zAuto;
      autoIdx.zTblName = (char*)zName;
      autoIdx.zSql = 0;
      rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                           &autoIdx, aMerged[i].iTable);
      sqlite3_free(zAuto);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  /* Every surviving index writes its row from the merge's own arrays. An
  ** unchanged index used to ride on the serializer's live-schema top-up,
  ** which resurrects this connection's pre-merge text; when the merge has
  ** normalized a rename away, that text names a column the merged table
  ** does not have. Survival is judged by name — a rootpage number can be
  ** reused by a drop-and-recreate on the other side. */
  for(i=0; i<nOursSchema; i++){
    SchemaEntry *pSe = &aOursSchema[i];
    if( !pSe->zName || !pSe->zType || !pSe->zSql ) continue;
    if( strcmp(pSe->zType, "index")!=0 ) continue;
    if( !pSe->zTblName
     || !doltliteFindTableByName(aMerged, nMerged, pSe->zTblName) ){
      continue;
    }
    if( findSchemaEntry(aAncSchema, nAncSchema, pSe->zName)
     && !findSchemaEntry(aTheirsSchema, nTheirsSchema, pSe->zName) ){
      /* Theirs dropped it. */
      continue;
    }
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  pSe->zName) ){
      /* Unchanged here: theirs' loop below writes it if they changed it,
      ** and a conflicted object keeps its pre-merge projection. */
      if( hasSchemaConflictObject(aConflictTables, nConflictTables, pSe->zName)
       || (pSe->zTblName
           && hasSchemaConflictTable(aConflictTables, nConflictTables,
                                     pSe->zTblName)) ){
        continue;
      }
      if( findSchemaEntry(aTheirsSchema, nTheirsSchema, pSe->zName)
       && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                   aTheirsSchema, nTheirsSchema,
                                   pSe->zName) ){
        continue;
      }
    }
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, pSe->iRootpage);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nTheirsSchema; i++){
    SchemaEntry *pSe = &aTheirsSchema[i];
    Pgno iRootpage;
    if( !pSe->zName || !pSe->zType ) continue;
    if( strcmp(pSe->zType, "index")!=0 ) continue;
    if( !pSe->zTblName
     || !doltliteFindTableByName(aMerged, nMerged, pSe->zTblName) ){
      continue;
    }
    if( hasSchemaConflictObject(aConflictTables, nConflictTables, pSe->zName)
     || hasSchemaConflictTable(aConflictTables, nConflictTables,
                               pSe->zTblName) ){
      continue;
    }
    if( !schemaEntryChangedByName(aAncSchema, nAncSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  pSe->zName) ){
      continue;
    }
    if( findSchemaEntry(aOursSchema, nOursSchema, pSe->zName)
     && schemaEntryChangedByName(aAncSchema, nAncSchema,
                                 aOursSchema, nOursSchema,
                                 pSe->zName) ){
      continue;
    }
    /* Pass 2 already declined this index: writing it here would displace
    ** one of ours with an index the catalog cannot load. */
    {
      SchemaEntry *pAncTbl = findSchemaEntry(aAncSchema, nAncSchema,
                                             pSe->zTblName);
      SchemaEntry *pOurTbl = findSchemaEntry(aOursSchema, nOursSchema,
                                             pSe->zTblName);
      if( pAncTbl && pOurTbl
       && mergeIndexColumnGoneFrom(pSe->zSql, pAncTbl->zSql,
                                   pOurTbl->zSql, 0) ){
        continue;
      }
    }
    /* Rootpage numbers are per-branch: once histories renumber, theirs'
    ** number can be a different object here. The remap table records where
    ** pass 2 actually placed their entry, so a mapping is authoritative.
    ** Without one, an index we also hold by name was adopted by pass 1
    ** into our-numbered entry, so the row must carry our number; theirs'
    ** raw number is only trustworthy when neither side relocated it. */
    {
      int bMapped = 0;
      int j;
      for(j=0; j<nRemap; j++){
        if( aRemap[j].oldPg==pSe->iRootpage ){
          iRootpage = aRemap[j].newPg;
          bMapped = 1;
          break;
        }
      }
      if( !bMapped ){
        /* Mirror pass 2's adoption test: it declines theirs' entry only
        ** when ours holds the same object by name AND table, so a
        ** same-named index on a different table stays pass-2-installed
        ** at theirs' own number. */
        SchemaEntry *pOurSe = findSchemaEntry(aOursSchema, nOursSchema,
                                              pSe->zName);
        if( pOurSe && pOurSe->zType && strcmp(pOurSe->zType, "index")==0
         && pOurSe->zTblName && pSe->zTblName
         && strcmp(pOurSe->zTblName, pSe->zTblName)==0 ){
          iRootpage = pOurSe->iRootpage;
        }else{
          iRootpage = pSe->iRootpage;
        }
      }
    }
    rc = appendMergedSchemaCatalogRecord(db, &root, pMaster->flags, iNextRowid++,
                                         pSe, iRootpage);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nAncSchema; i++){
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aMerged, nMerged,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aConflictTables, nConflictTables,
                                    aRemap, nRemap,
                                    aAncSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nOursSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aOursSchema[i].zName) ) continue;
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aMerged, nMerged,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aConflictTables, nConflictTables,
                                    aRemap, nRemap,
                                    aOursSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nTheirsSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aTheirsSchema[i].zName) ) continue;
    if( findSchemaEntry(aOursSchema, nOursSchema, aTheirsSchema[i].zName) ) continue;
    rc = appendMergedHiddenIndexRow(db, &root, pMaster->flags, &iNextRowid,
                                    aMerged, nMerged,
                                    aAncSchema, nAncSchema,
                                    aOursSchema, nOursSchema,
                                    aTheirsSchema, nTheirsSchema,
                                    aConflictTables, nConflictTables,
                                    aRemap, nRemap,
                                    aTheirsSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }

  for(i=0; i<nAncSchema; i++){
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aMerged, nMerged,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aConflictTables, nConflictTables,
                                  aRemap, nRemap,
                                  aAncSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nOursSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aOursSchema[i].zName) ) continue;
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aMerged, nMerged,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aConflictTables, nConflictTables,
                                  aRemap, nRemap,
                                  aOursSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }
  for(i=0; i<nTheirsSchema; i++){
    if( findSchemaEntry(aAncSchema, nAncSchema, aTheirsSchema[i].zName) ) continue;
    if( findSchemaEntry(aOursSchema, nOursSchema, aTheirsSchema[i].zName) ) continue;
    rc = appendMergedAuxSchemaRow(db, &root, pMaster->flags, &iNextRowid,
                                  aMerged, nMerged,
                                  aAncSchema, nAncSchema,
                                  aOursSchema, nOursSchema,
                                  aTheirsSchema, nTheirsSchema,
                                  aConflictTables, nConflictTables,
                                  aRemap, nRemap,
                                  aTheirsSchema[i].zName);
    if( rc!=SQLITE_OK ) return rc;
  }

  memcpy(&pMaster->root, &root, sizeof(root));
  return rc;
}

#endif /* DOLTLITE_PROLLY */
