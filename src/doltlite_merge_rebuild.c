#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"
#include "vdbeInt.h"

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
      /* Unchanged here: theirs' loop writes it when they changed it.
      ** A schema conflict must not skip the row. The live-schema top-up
      ** would otherwise put this connection's pre-merge text beside a
      ** table the merge already rewrote. */
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


/* Evaluate declared defaults once. A pre-ADD-COLUMN row omits the
** new field; reads materialize the default. Rewriting into a wider
** record covers that slot, so leaving NULL would replace the default. */
static void mergeColDefaultsFree(MergeColDefaults *p){
  int i;
  if( p->apOwned ){
    for(i=0; i<p->nCol; i++) sqlite3_free(p->apOwned[i]);
    sqlite3_free(p->apOwned);
  }
  sqlite3_free(p->aVal);
  memset(p, 0, sizeof(*p));
}

static int mergeColDefaultsLoad(
  const char *zSql,
  const char *zTable,
  MergeColDefaults *pOut
){
  sqlite3 *tmp = 0;
  sqlite3_stmt *pStmt = 0;
  char *zQuery = 0;
  int nCol = 0;
  int rc;

  memset(pOut, 0, sizeof(*pOut));
  rc = sqlite3_open(":memory:", &tmp);
  if( tmp ) sqlite3_mutex_enter(tmp->mutex);
  if( rc!=SQLITE_OK ) goto done;
  rc = sqlite3_exec(tmp, zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto done;

  zQuery = sqlite3_mprintf(
      "SELECT cid, dflt_value, type FROM pragma_table_xinfo(%Q) ORDER BY cid",
      zTable);
  if( !zQuery ){ rc = SQLITE_NOMEM; goto done; }
  rc = sqlite3_prepare_v2(tmp, zQuery, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) goto done;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ) nCol++;
  if( rc!=SQLITE_DONE ) goto done;
  sqlite3_reset(pStmt);

  if( nCol>0 ){
    pOut->aVal = sqlite3_malloc(nCol * (int)sizeof(DoltliteSerialValue));
    pOut->apOwned = sqlite3_malloc(nCol * (int)sizeof(u8*));
    if( !pOut->aVal || !pOut->apOwned ){ rc = SQLITE_NOMEM; goto done; }
    memset(pOut->aVal, 0, nCol * (int)sizeof(DoltliteSerialValue));
    memset(pOut->apOwned, 0, nCol * (int)sizeof(u8*));
    pOut->nCol = nCol;
    do{
      pOut->aVal[--nCol].eType = SQLITE_NULL;
    }while( nCol>0 );
  }

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    int cid = sqlite3_column_int(pStmt, 0);
    const char *zDflt = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
    sqlite3_stmt *pEval = 0;
    char *zEval;
    if( !zDflt || !zDflt[0] || cid<0 || cid>=pOut->nCol ) continue;
    zEval = sqlite3_mprintf("SELECT %s", zDflt);
    if( !zEval ){ rc = SQLITE_NOMEM; goto done; }
    if( sqlite3_prepare_v2(tmp, zEval, -1, &pEval, 0)==SQLITE_OK
     && sqlite3_step(pEval)==SQLITE_ROW ){
      DoltliteSerialValue *m = &pOut->aVal[cid];
      sqlite3_value *pVal = sqlite3_column_value(pEval, 0);
      sqlite3ValueApplyAffinity(
          pVal, sqlite3AffinityType(zType ? zType : "", 0), SQLITE_UTF8);
      switch( sqlite3_value_type(pVal) ){
        case SQLITE_INTEGER:
          m->eType = SQLITE_INTEGER;
          m->i = sqlite3_value_int64(pVal);
          break;
        case SQLITE_FLOAT:
          m->eType = SQLITE_FLOAT;
          m->r = sqlite3_value_double(pVal);
          break;
        case SQLITE_TEXT:
        case SQLITE_BLOB: {
          int isText = sqlite3_value_type(pVal)==SQLITE_TEXT;
          const void *p = isText
              ? (const void*)sqlite3_value_text(pVal)
              : sqlite3_value_blob(pVal);
          int n = sqlite3_value_bytes(pVal);
          u8 *pCopy = 0;
          if( n>0 ){
            pCopy = sqlite3_malloc(n);
            if( !pCopy ){
              sqlite3_finalize(pEval);
              sqlite3_free(zEval);
              rc = SQLITE_NOMEM;
              goto done;
            }
            memcpy(pCopy, p, n);
          }
          pOut->apOwned[cid] = pCopy;
          m->eType = isText ? SQLITE_TEXT : SQLITE_BLOB;
          m->p = pCopy;
          m->n = n;
          break;
        }
        default:
          break;
      }
    }
    sqlite3_finalize(pEval);
    sqlite3_free(zEval);
  }
  rc = rc==SQLITE_DONE ? SQLITE_OK : rc;

done:
  if( pStmt ) sqlite3_finalize(pStmt);
  sqlite3_free(zQuery);
  if( tmp ){
    sqlite3_mutex_leave(tmp->mutex);
    sqlite3_close(tmp);
  }
  if( rc!=SQLITE_OK ) mergeColDefaultsFree(pOut);
  return rc;
}


static char parsedColumnAffinity(const ParsedColumn *pCol){
  const u8 *z;
  int token = 0;
  i64 n;
  if( !pCol || !pCol->zDef || !pCol->zDef[0] ) return SQLITE_AFF_NUMERIC;
  z = (const u8 *)pCol->zDef;
  while( *z && (n = sqlite3GetToken(z, &token))>0
        && (token==TK_SPACE || token==TK_COMMENT) ){
    z += n;
  }
  if( *z && (n = sqlite3GetToken(z, &token))>0 ) z += n;
  while( *z && (n = sqlite3GetToken(z, &token))>0
        && (token==TK_SPACE || token==TK_COMMENT) ){
    z += n;
  }
  if( !*z ) return SQLITE_AFF_NUMERIC;
  return sqlite3AffinityType((const char *)z, 0);
}

/* True when a column kept its position but the merged declaration
** would store a different serial type than this side wrote. */
static int sideAffinityDiffers(
  ParsedColumn *aMerged, int nMerged,
  ParsedColumn *aSide, int nSide,
  const int *aMap
){
  int j;
  for(j=0; j<nSide; j++){
    int dst = aMap[j];
    if( dst<0 || dst>=nMerged ) continue;
    if( parsedColumnAffinity(&aMerged[dst])
     != parsedColumnAffinity(&aSide[j]) ) return 1;
  }
  return 0;
}

static void clearOwnedFields(u8 **apOwned, int n){
  int i;
  if( !apOwned ) return;
  for(i=0; i<n; i++){
    sqlite3_free(apOwned[i]);
    apOwned[i] = 0;
  }
}

/* Apply the merged column affinity. TEXT turns the other side's
** integer into text; a numeric column turns convertible text into
** a number. *ppOwned is the buffer for a converted text or blob. */
static int relayoutApplyAffinity(
  sqlite3 *db,
  DoltliteSerialValue *m,
  char aff,
  u8 **ppOwned
){
  sqlite3_value *pVal;
  int rc = SQLITE_OK;
  int eType;

  *ppOwned = 0;
  if( !m || m->eType==SQLITE_NULL || aff==SQLITE_AFF_BLOB
   || aff==SQLITE_AFF_NONE ){
    return SQLITE_OK;
  }
  pVal = sqlite3ValueNew(db);
  if( !pVal ) return SQLITE_NOMEM;
  switch( m->eType ){
    case SQLITE_INTEGER:
      sqlite3VdbeMemSetInt64(pVal, m->i);
      break;
    case SQLITE_FLOAT:
      sqlite3VdbeMemSetDouble(pVal, m->r);
      break;
    case SQLITE_TEXT:
      rc = sqlite3VdbeMemSetStr(pVal, (const char*)m->p, m->n,
                                SQLITE_UTF8, SQLITE_TRANSIENT);
      break;
    case SQLITE_BLOB:
      rc = sqlite3VdbeMemSetStr(pVal, (const char*)m->p, m->n,
                                0, SQLITE_TRANSIENT);
      break;
    default:
      break;
  }
  if( rc==SQLITE_OK ){
    const void *p = 0;
    int n = 0;
    u8 *pCopy = 0;
    sqlite3ValueApplyAffinity(pVal, (u8)aff, SQLITE_UTF8);
    eType = sqlite3_value_type(pVal);
    memset(m, 0, sizeof(*m));
    m->eType = eType;
    if( eType==SQLITE_INTEGER ){
      m->i = sqlite3_value_int64(pVal);
    }else if( eType==SQLITE_FLOAT ){
      m->r = sqlite3_value_double(pVal);
    }else if( eType==SQLITE_TEXT || eType==SQLITE_BLOB ){
      if( eType==SQLITE_TEXT ) p = sqlite3_value_text(pVal);
      else p = sqlite3_value_blob(pVal);
      n = sqlite3_value_bytes(pVal);
      if( n>0 ){
        if( !p ) rc = SQLITE_NOMEM;
        else{
          pCopy = sqlite3_malloc(n);
          if( !pCopy ) rc = SQLITE_NOMEM;
          else memcpy(pCopy, p, n);
        }
      }
      if( rc==SQLITE_OK ){
        m->p = pCopy;
        m->n = n;
        *ppOwned = pCopy;
      }
    }else{
      m->eType = SQLITE_NULL;
    }
  }
  sqlite3ValueFree(pVal);
  return rc;
}

static void relayoutFieldValue(
  const u8 *pRec,
  const DoltliteRecordInfo *pInfo,
  int src,
  DoltliteSerialValue *m
){
  int st = pInfo->aType[src];
  const u8 *body = pRec + pInfo->aOffset[src];
  memset(m, 0, sizeof(*m));
  m->eType = SQLITE_NULL;
  if( st==0 ) return;
  if( dlSerialIsInt(st) ){
    m->eType = SQLITE_INTEGER;
    if( st==8 ) m->i = 0;
    else if( st==9 ) m->i = 1;
    else m->i = dlReadIntBytes(body, dlSerialTypeLen((u64)st));
  }else if( st==7 ){
    u64 bits = (u64)dlReadIntBytes(body, 8);
    double d;
    memcpy(&d, &bits, sizeof(d));
    m->eType = SQLITE_FLOAT;
    m->r = d;
  }else if( st>=12 ){
    m->eType = (st & 1) ? SQLITE_TEXT : SQLITE_BLOB;
    m->p = body;
    m->n = dlSerialTypeLen((u64)st);
  }
}

int normalizeSideToMergedLayout(
  sqlite3 *db,
  const char *zTable,
  const ProllyHash *pOursRoot,
  const ProllyHash *pTheirsRoot,
  u8 flags,
  u8 srcFlags,
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  int bFillSharedDefaults,
  const char *zSharedSql,
  ProllyHash *pOutRoot
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *cache = doltliteGetCache(db);
  ParsedColumn *aAnc = 0, *aOurs = 0, *aTheirs = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0;
  int *aMap = 0;
  int *aMergedRecord = 0;
  int *aTheirsRecord = 0;
  int nMerged;
  int nMergedRecord = 0;
  int nTheirsRecord = 0;
  int nDropped = 0;
  ProllyMutMap mm;
  int mmInit = 0;
  ProllyCursor cur;
  int curInit = 0;
  int isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;
  MergeColDefaults oursDefaults;
  MergeColDefaults theirsDefaults;
  DoltliteColInfo sideCi;
  int sideCiInit = 0;
  u8 *pKeyRec = 0;
  ProllyCursor oursCur;
  int oursCurInit = 0;
  DoltliteSerialValue *aMem = 0;
  u8 **apOwned = 0;
  int bSameKey;
  int rc, res, j;

  memset(&oursDefaults, 0, sizeof(oursDefaults));
  memset(&theirsDefaults, 0, sizeof(theirsDefaults));
  memset(&sideCi, 0, sizeof(sideCi));

  memset(pOutRoot, 0, sizeof(*pOutRoot));
  /* Reading an int-key tree with a clustered-key cursor (or the reverse)
  ** is not a place to rewrite values. The fast copy stays. */
  bSameKey = ((flags ^ srcFlags) & PROLLY_NODE_INTKEY)==0;
  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){
    freeColumns(aAnc, nAnc); freeColumns(aOurs, nOurs); return rc;
  }

  nMerged = nOurs;
  aMap = sqlite3_malloc((nTheirs>0 ? nTheirs : 1) * (int)sizeof(int));
  aMergedRecord = sqlite3_malloc(
      (nOurs+nTheirs>0 ? nOurs+nTheirs : 1) * (int)sizeof(int));
  aTheirsRecord = sqlite3_malloc(
      (nTheirs>0 ? nTheirs : 1) * (int)sizeof(int));
  if( !aMap || !aMergedRecord || !aTheirsRecord ){
    rc = SQLITE_NOMEM;
    goto done;
  }
  for(j=0; j<nOurs; j++){
    aMergedRecord[j] = parsedColumnIsVirtual(&aOurs[j])
        ? -1 : nMergedRecord++;
  }
  for(j=0; j<nTheirs; j++){
    int found = parsedColumnIndexByName(
        aOurs, nOurs, aTheirs[j].zName);
    int bInAnc = 0;
    aTheirsRecord[j] = parsedColumnIsVirtual(&aTheirs[j])
        ? -1 : nTheirsRecord++;
    if( found<0 ){
      int ai = parsedColumnIndexByName(
          aAnc, nAnc, aTheirs[j].zName);
      if( ai<0 && j<nAnc
       && sqlite3_stricmp(aTheirs[j].zName, aAnc[j].zName)!=0
       && parsedColumnIndexByName(aTheirs, nTheirs, aAnc[j].zName)<0
       && parsedColumnDefinitionsMatch(&aTheirs[j], &aAnc[j]) ){
        ai = j;
      }
      if( ai>=0 ){
        bInAnc = 1;
        found = parsedColumnIndexByName(
            aOurs, nOurs, aAnc[ai].zName);
        if( found<0 && ai<nOurs
         && sqlite3_stricmp(aOurs[ai].zName, aAnc[ai].zName)!=0
         && parsedColumnIndexByName(aOurs, nOurs, aAnc[ai].zName)<0
         && parsedColumnDefinitionsMatch(&aOurs[ai], &aAnc[ai]) ){
          found = ai;
        }
      }
    }
    /* A swap keeps each value in its slot. Matching by name would move
    ** the cell onto the column that inherited the old name. Same column
    ** count: a drop shifts later same-typed columns and is not a swap. */
    if( nAnc==nOurs && nOurs==nTheirs
     && j<nAnc
     && sqlite3_stricmp(aTheirs[j].zName, aOurs[j].zName)!=0
     && parsedColumnDefinitionsMatch(&aTheirs[j], &aAnc[j])
     && parsedColumnDefinitionsMatch(&aOurs[j], &aAnc[j]) ){
      int dstOfSrcName = parsedColumnIndexByName(
          aOurs, nOurs, aTheirs[j].zName);
      int srcOfDstName = parsedColumnIndexByName(
          aTheirs, nTheirs, aOurs[j].zName);
      if( dstOfSrcName>=0 && dstOfSrcName!=j
       && srcOfDstName>=0 && srcOfDstName!=j ){
        found = j;
        bInAnc = 1;
      }
    }
    if( found>=0 ){
      aMap[j] = found;
    }else if( bInAnc ){
      /* Merged layout dropped it. Appending would resurrect dropped data. */
      aMap[j] = -1;
      nDropped++;
    }else{
      aMap[j] = nMerged;
      aMergedRecord[nMerged] = parsedColumnIsVirtual(&aTheirs[j])
          ? -1 : nMergedRecord++;
      nMerged++;
    }
  }
  if( nMergedRecord > DOLTLITE_MAX_RECORD_FIELDS ){
    rc = SQLITE_ERROR;
    goto done;
  }

  /* Already at merged positions; trailing adds read as absent anyway.
  ** A changed affinity still rewrites each field, so a TEXT column
  ** does not keep the other side's integer. */
  if( nDropped==0 && !bFillSharedDefaults
   && !(bSameKey && sideAffinityDiffers(aOurs, nOurs, aTheirs, nTheirs, aMap)) ){
    int bSamePositions = 1;
    for(j=0; j<nTheirs; j++){
      if( aMap[j]!=j ){ bSamePositions = 0; break; }
    }
    if( bSamePositions ){
      memcpy(pOutRoot, pTheirsRoot, sizeof(*pOutRoot));
      goto done;
    }
  }

  /* Unsupplied slots take the owning schema's declared default. */
  rc = mergeColDefaultsLoad(zOursSql, zTable, &oursDefaults);
  if( rc!=SQLITE_OK ) goto done;
  rc = mergeColDefaultsLoad(zTheirsSql, zTable, &theirsDefaults);
  if( rc!=SQLITE_OK ) goto done;

  if( zSharedSql ){
    ParsedColumn *aShared = 0;
    int nShared = 0;
    rc = parseColumns(zSharedSql, &aShared, &nShared);
    if( rc!=SQLITE_OK ) goto done;
    /* A column added on both sides has no value in the ancestor. */
    for(j=0; j<nOurs && j<oursDefaults.nCol; j++){
      if( parsedColumnIndexByName(aAnc, nAnc, aOurs[j].zName)<0
       && parsedColumnIndexByName(aShared, nShared, aOurs[j].zName)>=0 ){
        oursDefaults.aVal[j].eType = SQLITE_NULL;
      }
    }
    freeColumns(aShared, nShared);
  }

  if( !isIntKey ){
    sqlite3 *tmp = 0;
    rc = sqlite3_open(":memory:", &tmp);
    if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, zTheirsSql, 0, 0, 0);
    if( rc==SQLITE_OK ) rc = doltliteGetColumnNames(tmp, zTable, &sideCi);
    if( tmp ) sqlite3_close(tmp);
    if( rc!=SQLITE_OK ) goto done;
    sideCiInit = 1;
  }

  rc = prollyMutMapInit(&mm, (u8)isIntKey);
  if( rc!=SQLITE_OK ) goto done;
  mmInit = 1;

  if( !prollyHashIsEmpty(pOursRoot) ){
    prollyCursorInit(&oursCur, cs, cache, pOursRoot, flags);
    oursCurInit = 1;
  }

  if( nMergedRecord>0
   && !(aMem = sqlite3_malloc64((sqlite3_uint64)nMergedRecord * sizeof(*aMem))) ){
    rc = SQLITE_NOMEM; goto done;
  }
  if( nMergedRecord>0 ){
    apOwned = sqlite3_malloc64((sqlite3_uint64)nMergedRecord * sizeof(*apOwned));
    if( !apOwned ){ rc = SQLITE_NOMEM; goto done; }
    memset(apOwned, 0, (size_t)nMergedRecord * sizeof(*apOwned));
  }

  prollyCursorInit(&cur, cs, cache, pTheirsRoot, flags);
  curInit = 1;
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK ) goto done;

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal = 0; int nVal = 0;
    const u8 *pKey = 0; int nKey = 0; i64 intKey = 0;
    DoltliteRecordInfo info = {0};
    int nEmit = 0, k;
    int rowOnlyTheirs;
    u8 *pNew = 0; int nNew = 0;

    prollyCursorValue(&cur, &pVal, &nVal);
    if( isIntKey ){
      intKey = prollyCursorIntKey(&cur);
    }else{
      prollyCursorKey(&cur, &pKey, &nKey);
    }

    /* Dual-schema relayout fills defaults only for rows unique to this side.
    ** A one-sided append projects every row into the wider logical layout. */
    rowOnlyTheirs = 0;
    if( oursCurInit ){
      int oursRes = 0;
      if( isIntKey ){
        rc = prollyCursorSeekInt(&oursCur, intKey, &oursRes);
      }else{
        rc = prollyCursorSeekBlob(&oursCur, pKey, nKey, &oursRes);
      }
      if( rc!=SQLITE_OK ) goto done;
      rowOnlyTheirs = !(oursRes==0 && prollyCursorIsValid(&oursCur));
    }else{
      rowOnlyTheirs = 1;
    }

    clearOwnedFields(apOwned, nMergedRecord);
    doltliteParseRecord(pVal, nVal, &info);
    for(k=0; k<nMergedRecord; k++){
      memset(&aMem[k], 0, sizeof(aMem[k]));
      aMem[k].eType = SQLITE_NULL;
    }
    if( rowOnlyTheirs || bFillSharedDefaults ){
      for(k=0; k<nOurs && k<oursDefaults.nCol; k++){
        int tgt = aMergedRecord[k];
        if( tgt>=0 ) aMem[tgt] = oursDefaults.aVal[k];
      }
      for(j=0; j<nTheirs; j++){
        if( aMap[j]>=nOurs && j<theirsDefaults.nCol ){
          int tgt = aMergedRecord[aMap[j]];
          if( tgt>=0 ) aMem[tgt] = theirsDefaults.aVal[j];
        }
      }
    }
    for(j=0; j<nTheirs; j++){
      int src = aTheirsRecord[j];
      int tgt;
      DoltliteSerialValue *m;
      if( src<0 || src>=info.nField || aMap[j]<0 ) continue;
      tgt = aMergedRecord[aMap[j]];
      if( tgt<0 ) continue;
      m = &aMem[tgt];
      relayoutFieldValue(pVal, &info, src, m);
      if( bSameKey && aMap[j]<nOurs ){
        char affTo = parsedColumnAffinity(&aOurs[aMap[j]]);
        char affFrom = parsedColumnAffinity(&aTheirs[j]);
        if( affTo!=affFrom ){
          u8 *pOwned = 0;
          rc = relayoutApplyAffinity(db, m, affTo, &pOwned);
          if( rc!=SQLITE_OK ) goto done;
          sqlite3_free(apOwned[tgt]);
          apOwned[tgt] = pOwned;
        }
      }
      if( m->eType==SQLITE_NULL ){
        if( rowOnlyTheirs && tgt+1>nEmit ) nEmit = tgt+1;
      }else if( tgt+1>nEmit ){
        nEmit = tgt+1;
      }
    }
    for(k=0; k<nMergedRecord; k++){
      if( aMem[k].eType!=SQLITE_NULL && k+1>nEmit ) nEmit = k+1;
    }

    /* A PK-covering row stores an empty record; once a filled default
    ** makes the record non-empty its key columns must be spelled out too. */
    if( nEmit>0 && nVal==0 && sideCiInit ){
      DoltliteRecordInfo kinfo = {0};
      int nKeyRec = 0;
      rc = doltliteRecordFromClusteredKeyCols(db, &sideCi, pKey, nKey,
                                              &pKeyRec, &nKeyRec);
      if( rc!=SQLITE_OK ) goto done;
      doltliteParseRecord(pKeyRec, nKeyRec, &kinfo);
      for(j=0; j<nTheirs; j++){
        int src = aTheirsRecord[j];
        int tgt;
        if( src<0 || src>=kinfo.nField || aMap[j]<0 ) continue;
        tgt = aMergedRecord[aMap[j]];
        if( tgt<0 ) continue;
        relayoutFieldValue(pKeyRec, &kinfo, src, &aMem[tgt]);
        if( bSameKey && aMap[j]<nOurs ){
          char affTo = parsedColumnAffinity(&aOurs[aMap[j]]);
          char affFrom = parsedColumnAffinity(&aTheirs[j]);
          if( affTo!=affFrom ){
            u8 *pOwned = 0;
            rc = relayoutApplyAffinity(db, &aMem[tgt], affTo, &pOwned);
            if( rc!=SQLITE_OK ){
              doltliteRecordInfoClear(&kinfo);
              goto done;
            }
            sqlite3_free(apOwned[tgt]);
            apOwned[tgt] = pOwned;
          }
        }
        if( aMem[tgt].eType!=SQLITE_NULL && tgt+1>nEmit ) nEmit = tgt+1;
      }
      doltliteRecordInfoClear(&kinfo);
    }

    if( nEmit>0 ){
      pNew = doltliteBuildRecord(aMem, nEmit, &nNew);
      if( !pNew ){ rc = SQLITE_NOMEM; goto done; }
    }
    rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pNew, nNew);
    sqlite3_free(pNew);
    sqlite3_free(pKeyRec);
    pKeyRec = 0;
    doltliteRecordInfoClear(&info);
    if( rc!=SQLITE_OK ) goto done;

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) goto done;
  }

  {
    ProllyMutator mut;
    memset(&mut, 0, sizeof(mut));
    mut.pStore = cs;
    mut.pCache = cache;
    memset(&mut.oldRoot, 0, sizeof(mut.oldRoot));
    mut.pEdits = &mm;
    mut.flags = flags;
    rc = prollyMutateFlush(&mut);
    if( rc==SQLITE_OK ) memcpy(pOutRoot, &mut.newRoot, sizeof(ProllyHash));
  }

done:
  clearOwnedFields(apOwned, nMergedRecord);
  sqlite3_free(apOwned);
  sqlite3_free(aMem); sqlite3_free(pKeyRec);
  mergeColDefaultsFree(&oursDefaults);
  mergeColDefaultsFree(&theirsDefaults);
  if( sideCiInit ) doltliteFreeColInfo(&sideCi);
  if( oursCurInit ) prollyCursorClose(&oursCur);
  if( curInit ) prollyCursorClose(&cur);
  if( mmInit ) prollyMutMapFree(&mm);
  sqlite3_free(aMap);
  sqlite3_free(aMergedRecord);
  sqlite3_free(aTheirsRecord);
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
