#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

int mergeAppendReindexName(char ***paz, int *pn, const char *zName){
  char **azNew;
  char *zDup;
  if( !paz ) return SQLITE_OK;
  azNew = sqlite3_realloc(*paz, (*pn+1)*(int)sizeof(char*));
  if( !azNew ) return SQLITE_NOMEM;
  *paz = azNew;
  zDup = sqlite3_mprintf("%s", zName);
  if( !zDup ) return SQLITE_NOMEM;
  (*paz)[(*pn)++] = zDup;
  return SQLITE_OK;
}

static void mergePass1Free(MergePass1Ctx *c){
  sqlite3_free(c->aPatches);
  c->aPatches = 0;
  c->nPatches = c->nPatchesAlloc = 0;
}

static int mergePass1NoteSchemaConflict(
  MergePass1Ctx *c,
  const char *zTable,
  const char *zObject
){
  int added = 0;
  int rc = appendSchemaConflict(
      c->ppConflictTables, c->pnConflictTables,
      zTable ? zTable : "", zObject ? zObject : "", &added);
  if( rc!=SQLITE_OK ) return rc;
  if( added ) (*c->pTotalConflicts)++;
  return SQLITE_OK;
}

static void mergePass1FreeIdxInfo(MergeIndexInfo *aIdxInfo, int nIdxInfo){
  int i;
  if( !aIdxInfo ) return;
  for(i=0; i<nIdxInfo; i++){
    if( aIdxInfo[i].pKeyInfo ){
      sqlite3KeyInfoUnref(aIdxInfo[i].pKeyInfo);
      aIdxInfo[i].pKeyInfo = 0;
    }
  }
  sqlite3_free(aIdxInfo);
}

static int mergePass1SchemaPrefixInfo(
  const char *zAncSql,
  const char *zSideSql,
  int *pbPrefix,
  int *pnAnc,
  int *pnSide,
  int *pnAncRecord
){
  ParsedColumn *aAnc = 0;
  ParsedColumn *aSide = 0;
  int nAnc = 0;
  int nSide = 0;
  int i;
  int rc;

  *pbPrefix = 0;
  *pnAnc = 0;
  *pnSide = 0;
  if( pnAncRecord ) *pnAncRecord = 0;
  if( !zAncSql || !zSideSql ) return SQLITE_OK;
  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zSideSql, &aSide, &nSide);
  if( rc!=SQLITE_OK ){
    freeColumns(aAnc, nAnc);
    return rc;
  }
  if( nSide>=nAnc ){
    *pbPrefix = 1;
    for(i=0; i<nAnc; i++){
      if( sqlite3_stricmp(aAnc[i].zName, aSide[i].zName)!=0
       || !parsedColumnDefinitionsMatch(&aAnc[i], &aSide[i]) ){
        *pbPrefix = 0;
        break;
      }
    }
  }
  *pnAnc = nAnc;
  *pnSide = nSide;
  for(i=0; pnAncRecord && i<nAnc; i++){
    if( !parsedColumnIsVirtual(&aAnc[i]) ) (*pnAncRecord)++;
  }
  freeColumns(aAnc, nAnc);
  freeColumns(aSide, nSide);
  return SQLITE_OK;
}

static int mergePass1CollectIndexes(
  MergePass1Ctx *c,
  const char *zName,
  MergeIndexInfo **paIdxInfo,
  int *pnIdxInfo
){
  Table *pTab;
  Index *pIdx;
  int nIdx = 0;
  MergeIndexInfo *aIdxInfo = 0;
  int nIdxInfo = 0;
  int rc = SQLITE_OK;

  *paIdxInfo = 0;
  *pnIdxInfo = 0;
  if( !zName || !c->db ) return SQLITE_OK;

  pTab = sqlite3FindTable(c->db, zName, "main");
  if( !pTab ) return SQLITE_OK;

  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext) nIdx++;
  if( nIdx<=0 ) return SQLITE_OK;

  aIdxInfo = sqlite3_malloc(nIdx * (int)sizeof(MergeIndexInfo));
  if( !aIdxInfo ){
    /* Else table rows merge with stale secondary indexes. */
    return SQLITE_NOMEM;
  }
  memset(aIdxInfo, 0, nIdx*(int)sizeof(MergeIndexInfo));

  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    struct TableEntry *oursIdx;
    /* WITHOUT ROWID PK is the table tree; skip it. */
    if( pIdx->idxType==SQLITE_IDXTYPE_PRIMARYKEY && !HasRowid(pTab) ) continue;
    oursIdx = doltliteFindTableByNumber(c->aOurs, c->nOurs, pIdx->tnum);
    if( oursIdx ){
      MergeIndexInfo *mi = &aIdxInfo[nIdxInfo];
      mi->iTable = pIdx->tnum;
      memcpy(&mi->oursRoot, &oursIdx->root, sizeof(ProllyHash));
      mi->nColumn = pIdx->nKeyCol;
      mi->aiColumn = pIdx->aiColumn;
      mi->pKeyInfo = doltliteKeyInfoOfIndex(c->db, pIdx);
      if( !mi->pKeyInfo ){
        mergePass1FreeIdxInfo(aIdxInfo, nIdxInfo);
        return SQLITE_NOMEM;
      }
      mi->iPKey = pTab->iPKey;
      mi->pIdx = pIdx;
      nIdxInfo++;
    }
  }

  *paIdxInfo = aIdxInfo;
  *pnIdxInfo = nIdxInfo;
  return rc;
}

static int mergePass1RecordIndexPatches(
  MergePass1Ctx *c,
  MergeIndexInfo *aIdxInfo,
  int nIdxInfo
){
  int ix;
  for(ix=0; ix<nIdxInfo; ix++){
    if( c->nPatches >= c->nPatchesAlloc ){
      int newAlloc = c->nPatchesAlloc ? c->nPatchesAlloc*2 : 4;
      void *pNew = sqlite3_realloc(c->aPatches,
          newAlloc * (int)sizeof(IndexMergePatch));
      if( !pNew ) return SQLITE_NOMEM;
      c->aPatches = pNew;
      c->nPatchesAlloc = newAlloc;
    }
    c->aPatches[c->nPatches].iTable = aIdxInfo[ix].iTable;
    memcpy(&c->aPatches[c->nPatches].mergedRoot,
           &aIdxInfo[ix].mergedRoot, sizeof(ProllyHash));
    c->nPatches++;
  }
  return SQLITE_OK;
}

static int mergePass1MergeTableData(
  MergePass1Ctx *c,
  const char *zName,
  const char *zLogicalName,
  struct TableEntry *pOurs,
  struct TableEntry *pAnc,
  const ProllyHash *pTheirsRoot,
  int ourSchemaChanged,
  int theirSchemaChanged,
  int schemaChoice,
  struct TableEntry *pTheirsEntry
){
  ProllyHash mergedTableRoot;
  int nConflicts = 0;
  DoltliteConflictRow *aConflictRows = 0;
  MergeIndexInfo *aIdxInfo = 0;
  int nIdxInfo = 0;
  MergeRowPolicy rowPolicy;
  const MergeRowPolicy *pRowPolicy = 0;
  int rc;
  int handled = 0;

  memset(&rowPolicy, 0, sizeof(rowPolicy));

  if( zName && (ourSchemaChanged || theirSchemaChanged) ){
    SchemaEntry *pAncSe = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
    SchemaEntry *pOurSe = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
    SchemaEntry *pTheirSe = findSchemaEntry(
        c->aTheirsSchema, c->nTheirsSchema, zName);
    int bOurPrefix = 0, bTheirPrefix = 0;
    int nAncOur = 0, nAncTheir = 0;
    int nOur = 0, nTheir = 0;
    int nAncOurRecord = 0, nAncTheirRecord = 0;
    if( pAncSe && pOurSe && pTheirSe ){
      rc = mergePass1SchemaPrefixInfo(
          pAncSe->zSql, pOurSe->zSql, &bOurPrefix,
          &nAncOur, &nOur, &nAncOurRecord);
      if( rc==SQLITE_OK ){
        rc = mergePass1SchemaPrefixInfo(
            pAncSe->zSql, pTheirSe->zSql,
            &bTheirPrefix, &nAncTheir, &nTheir, &nAncTheirRecord);
      }
      if( rc!=SQLITE_OK ) return rc;
      if( bOurPrefix && bTheirPrefix && nAncOur==nAncTheir
       && nAncOurRecord==nAncTheirRecord
       && (nOur>nAncOur || nTheir>nAncTheir) ){
        rowPolicy.nDeleteCompareFields = nAncOurRecord;
        pRowPolicy = &rowPolicy;
      }
    }
  }

  rc = mergePass1CollectIndexes(c, zName, &aIdxInfo, &nIdxInfo);
  if( rc!=SQLITE_OK ) return rc;

  if( canFastMerge(c->db, zName, !ourSchemaChanged && !theirSchemaChanged) ){
    rc = prollyThreeWayMergeFast(
      doltliteGetChunkStore(c->db), doltliteGetCache(c->db),
      &pAnc->root, &pOurs->root, pTheirsRoot,
      pOurs->flags, &mergedTableRoot, &handled);
    if( rc!=SQLITE_OK ){
      mergePass1FreeIdxInfo(aIdxInfo, nIdxInfo);
      return rc;
    }
  }

  if( !handled ){
    rc = mergeTableRows(c->db, &pAnc->root, &pOurs->root,
                        pTheirsRoot, pOurs->flags,
                        pAnc->flags, pTheirsEntry->flags,
                        &mergedTableRoot, &nConflicts, &aConflictRows,
                        aIdxInfo, nIdxInfo, pRowPolicy);
    if( rc!=SQLITE_OK ){
      mergePass1FreeIdxInfo(aIdxInfo, nIdxInfo);
      return rc;
    }
  }

  rc = mergePass1RecordIndexPatches(c, aIdxInfo, nIdxInfo);
  mergePass1FreeIdxInfo(aIdxInfo, nIdxInfo);
  if( rc!=SQLITE_OK ) return rc;

  {
    struct TableEntry merged = *pOurs;
    memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
    if( schemaChoice==SCHEMA_MERGE_THEIRS
     || (theirSchemaChanged && !ourSchemaChanged) ){
      memcpy(&merged.schemaHash, &pTheirsEntry->schemaHash, sizeof(ProllyHash));
      merged.flags = pTheirsEntry->flags;
    }
    c->aMerged[(*c->pnMerged)++] = merged;
  }

  if( nConflicts>0 ){
    *c->pTotalConflicts += nConflicts;
    rc = appendConflictTable(c->ppConflictTables, c->pnConflictTables,
                             zLogicalName ? zLogicalName : "",
                             nConflicts, aConflictRows);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int mergePass1ResolveOursEntry(
  MergePass1Ctx *c,
  int iOurs,
  const char **pzName,
  const char **pzLogicalName,
  const char **pzSchemaMergeName,
  const char **pzSchemaConflictTable,
  struct TableEntry **ppAnc,
  struct TableEntry **ppTheirs,
  int *pbSkipMaster
){
  const char *zName = c->aOurs[iOurs].zName;

  *pzName = zName;
  *pzLogicalName = zName;
  *pzSchemaMergeName = zName;
  *pzSchemaConflictTable = zName;
  *ppAnc = 0;
  *ppTheirs = 0;
  *pbSkipMaster = 0;

  if( c->aOurs[iOurs].iTable==1 ){
    *pbSkipMaster = 1;
    return SQLITE_OK;
  }

  if( !zName ){
    SchemaEntry *pOurSe = findSchemaEntryByRootpage(
        c->aOursSchema, c->nOursSchema, c->aOurs[iOurs].iTable);
    if( pOurSe && pOurSe->zName && pOurSe->zType
     && strcmp(pOurSe->zType, "index")==0 ){
      *pzLogicalName = pOurSe->zName;
      *pzSchemaMergeName = pOurSe->zName;
      *pzSchemaConflictTable = pOurSe->zTblName
          ? pOurSe->zTblName : pOurSe->zName;
      *ppAnc = findCatalogEntryBySchemaObject(
          c->aAnc, c->nAnc, c->aAncSchema, c->nAncSchema,
          pOurSe->zType, pOurSe->zName, pOurSe->zTblName);
      *ppTheirs = findCatalogEntryBySchemaObject(
          c->aTheirs, c->nTheirs, c->aTheirsSchema, c->nTheirsSchema,
          pOurSe->zType, pOurSe->zName, pOurSe->zTblName);
      return SQLITE_OK;
    }
    *ppAnc = doltliteFindTableByNumber(c->aAnc, c->nAnc, c->aOurs[iOurs].iTable);
    *ppTheirs = doltliteFindTableByNumber(
        c->aTheirs, c->nTheirs, c->aOurs[iOurs].iTable);
    return SQLITE_OK;
  }

  *ppAnc = doltliteFindTableByName(c->aAnc, c->nAnc, zName);
  *ppTheirs = doltliteFindTableByName(c->aTheirs, c->nTheirs, zName);
  if( !*ppAnc ){
    struct TableEntry *pByNo = doltliteFindTableByNumber(
        c->aAnc, c->nAnc, c->aOurs[iOurs].iTable);
    if( pByNo && pByNo->zName && strcmp(pByNo->zName, zName)!=0
     && !doltliteFindTableByName(c->aOurs, c->nOurs, pByNo->zName) ){
      struct TableEntry *pTheirsByNo;
      /* Drop plus a dual add: the new name is on both sides and was not
      ** in the ancestor. Reusing the dropped table's number is not a rename. */
      if( doltliteFindTableByName(c->aTheirs, c->nTheirs, zName)
       && !doltliteFindTableByName(c->aAnc, c->nAnc, zName) ){
        return SQLITE_OK;
      }
      pTheirsByNo = doltliteFindTableByNumber(
          c->aTheirs, c->nTheirs, c->aOurs[iOurs].iTable);
      if( pTheirsByNo && !pTheirsByNo->zName ){
        pTheirsByNo = 0;
        pByNo = 0;
      }else if( pTheirsByNo
             && strcmp(pTheirsByNo->zName, pByNo->zName)!=0
             && strcmp(pTheirsByNo->zName, zName)!=0 ){
        pTheirsByNo = 0;
        pByNo = 0;
      }
      if( pByNo ){
        *ppAnc = pByNo;
        if( !*ppTheirs && pTheirsByNo ) *ppTheirs = pTheirsByNo;
      }
    }
  }
  return SQLITE_OK;
}

static int mergePass1AddedSchemaMatches(
  MergePass1Ctx *c,
  const char *zObjectName,
  int iOurs,
  struct TableEntry *theirsEntry
){
  SchemaEntry *ourSE;
  SchemaEntry *theirSE;
  if( prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                        &theirsEntry->schemaHash)==0 ){
    return 1;
  }
  if( !zObjectName ) return 0;
  ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zObjectName);
  theirSE = findSchemaEntry(c->aTheirsSchema, c->nTheirsSchema, zObjectName);
  if( !ourSE || !theirSE || !ourSE->zSql || !theirSE->zSql ) return 0;
  return strcmp(ourSE->zSql, theirSE->zSql)==0;
}

static int mergePass1OursAdded(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *theirsEntry
){
  int rc;

  /* Index over a column they dropped cannot load. Dolt drops it too. */
  if( !zName && zSchemaMergeName && zSchemaConflictTable ){
    SchemaEntry *pOurIdx = findSchemaEntry(
        c->aOursSchema, c->nOursSchema, zSchemaMergeName);
    SchemaEntry *pAncTbl = findSchemaEntry(
        c->aAncSchema, c->nAncSchema, zSchemaConflictTable);
    SchemaEntry *pTheirTbl = findSchemaEntry(
        c->aTheirsSchema, c->nTheirsSchema, zSchemaConflictTable);
    char *zGone = 0;
    if( pOurIdx && pAncTbl && pTheirTbl
     && mergeIndexColumnGoneFrom(pOurIdx->zSql, pAncTbl->zSql,
                                 pTheirTbl->zSql, &zGone) ){
      sqlite3_free(zGone);
      return SQLITE_OK;
    }
    sqlite3_free(zGone);
  }

  if( theirsEntry ){
    if( !zName && c->bDisjointSchemaChanges ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      return SQLITE_OK;
    }
    if( zName
     && (strcmp(zName, "sqlite_stat1")==0
      || strcmp(zName, "sqlite_stat4")==0) ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      return SQLITE_OK;
    }
    if( mergePass1AddedSchemaMatches(
          c, zName ? zName : zSchemaMergeName, iOurs, theirsEntry) ){
      if( prollyHashCompare(&c->aOurs[iOurs].root, &theirsEntry->root)==0 ){
        c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
        return SQLITE_OK;
      }
      if( !zName ){
        if( zSchemaMergeName && zSchemaMergeName[0] ){
          rc = mergeAppendReindexName(
              c->pazReindex, c->pnReindex, zSchemaMergeName);
          if( rc!=SQLITE_OK ) return rc;
        }
        c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
        return SQLITE_OK;
      }
      if( ((c->aOurs[iOurs].flags ^ theirsEntry->flags)
           & PROLLY_NODE_INTKEY)!=0 ){
        if( c->pzErrMsg ){
          sqlite3_free(*c->pzErrMsg);
          *c->pzErrMsg = sqlite3_mprintf(
              "cannot merge because table '%s' has different primary keys",
              zName);
        }
        return SQLITE_ERROR;
      }
      {
        struct TableEntry ancEmpty;
        memset(&ancEmpty, 0, sizeof(ancEmpty));
        ancEmpty.flags = c->aOurs[iOurs].flags;
        return mergePass1MergeTableData(
            c, zName, zName, &c->aOurs[iOurs], &ancEmpty,
            &theirsEntry->root, 0, 0, SCHEMA_MERGE_DEFAULT, theirsEntry);
      }
    }
    rc = mergePass1NoteSchemaConflict(c, zSchemaConflictTable, zSchemaMergeName);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( !theirsEntry && !zName && zSchemaMergeName && zSchemaMergeName[0]
   && zSchemaConflictTable && zSchemaConflictTable[0] ){
    struct TableEntry *pAncTable = doltliteFindTableByName(
        c->aAnc, c->nAnc, zSchemaConflictTable);
    struct TableEntry *pTheirTable = doltliteFindTableByName(
        c->aTheirs, c->nTheirs, zSchemaConflictTable);
    if( pTheirTable && (!pAncTable
     || prollyHashCompare(&pTheirTable->root, &pAncTable->root)!=0) ){
      rc = mergeAppendReindexName(
          c->pazReindex, c->pnReindex, zSchemaMergeName);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
  return SQLITE_OK;
}

static int mergePass1OursModifyTheirsDelete(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *ancEntry
){
  int oursChanged = prollyHashCompare(&c->aOurs[iOurs].root, &ancEntry->root)!=0
                 || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                                      &ancEntry->schemaHash)!=0;
  int rc;

  if( zName && mergeTableRenameOtherDrop(
        c->aAnc, c->nAnc, c->aTheirs, c->nTheirs, c->aOurs, c->nOurs,
        c->aAncSchema, c->nAncSchema, c->aOursSchema, c->nOursSchema,
        &c->aOurs[iOurs], 0) ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    return SQLITE_OK;
  }

  if( !oursChanged ) return SQLITE_OK;

  /* Index root moves with table data; only a definition change conflicts.
  ** Unchanged definition: DROP wins. */
  if( !zName && zSchemaMergeName && zSchemaMergeName[0]
   && !schemaEntryChangedByName(c->aAncSchema, c->nAncSchema,
                                c->aOursSchema, c->nOursSchema,
                                zSchemaMergeName) ){
    return SQLITE_OK;
  }
  if( !zName && zSchemaMergeName && zSchemaMergeName[0] ){
    if( c->ppSchemaActions && c->pnSchemaActions ){
      rc = recordSchemaAddColumns(c->ppSchemaActions, c->pnSchemaActions,
                                  zSchemaMergeName, 0, 0);
      if( rc!=SQLITE_OK ) return rc;
    }
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    return SQLITE_OK;
  }
  rc = mergePass1NoteSchemaConflict(c, zSchemaConflictTable, zSchemaMergeName);
  if( rc!=SQLITE_OK ) return rc;
  c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
  return SQLITE_OK;
}

/* PK signature: key columns in order, types, collations, sort dirs.
** No declared PK yields empty, matching SQLite rowid-keyed tables. */
static int mergePass1AppendSigRow(
  char **pzSig,
  const char *zName,
  const char *zType,
  const char *zColl,
  int bDesc
){
  char *zNew = sqlite3_mprintf("%s%s%s %s %s %s", *pzSig ? *pzSig : "",
                               *pzSig ? "," : "",
                               zName ? zName : "",
                               zType ? zType : "",
                               zColl ? zColl : "BINARY",
                               bDesc ? "DESC" : "ASC");
  sqlite3_free(*pzSig);
  *pzSig = zNew;
  return zNew ? SQLITE_OK : SQLITE_NOMEM;
}

static int mergePass1PkSignature(
  const char *zSql,
  const char *zTableName,
  char **pzSig
){
  sqlite3 *tmp = 0;
  sqlite3_stmt *pStmt = 0;
  char *zQuery = 0;
  char *zSig = 0;
  int rc;

  *pzSig = 0;
  rc = sqlite3_open(":memory:", &tmp);
  if( rc!=SQLITE_OK ) goto done;
  rc = sqlite3_exec(tmp, zSql, 0, 0, 0);
  if( rc!=SQLITE_OK ) goto done;

  zQuery = sqlite3_mprintf(
      "SELECT x.name, i.type, x.coll, x.desc "
      "FROM pragma_index_list(%Q) AS l "
      "JOIN pragma_index_xinfo(l.name) AS x "
      "LEFT JOIN pragma_table_info(%Q) AS i ON i.name = x.name "
      "WHERE l.origin='pk' AND x.key=1 ORDER BY x.seqno",
      zTableName, zTableName);
  if( !zQuery ){ rc = SQLITE_NOMEM; goto done; }
  rc = sqlite3_prepare_v2(tmp, zQuery, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) goto done;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    rc = mergePass1AppendSigRow(&zSig,
             (const char*)sqlite3_column_text(pStmt, 0),
             (const char*)sqlite3_column_text(pStmt, 1),
             (const char*)sqlite3_column_text(pStmt, 2),
             sqlite3_column_int(pStmt, 3));
    if( rc!=SQLITE_OK ) goto done;
  }
  if( rc!=SQLITE_DONE ) goto done;
  sqlite3_finalize(pStmt);
  pStmt = 0;
  sqlite3_free(zQuery);
  zQuery = 0;

  if( !zSig ){
    zQuery = sqlite3_mprintf(
        "SELECT name, type FROM pragma_table_info(%Q) WHERE pk>0 ORDER BY pk",
        zTableName);
    if( !zQuery ){ rc = SQLITE_NOMEM; goto done; }
    rc = sqlite3_prepare_v2(tmp, zQuery, -1, &pStmt, 0);
    if( rc!=SQLITE_OK ) goto done;
    while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
      rc = mergePass1AppendSigRow(&zSig,
               (const char*)sqlite3_column_text(pStmt, 0),
               (const char*)sqlite3_column_text(pStmt, 1),
               0, 0);
      if( rc!=SQLITE_OK ) goto done;
    }
  }
  rc = rc==SQLITE_DONE ? SQLITE_OK : rc;

done:
  if( pStmt ) sqlite3_finalize(pStmt);
  sqlite3_free(zQuery);
  if( tmp ) sqlite3_close(tmp);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zSig);
    return rc;
  }
  *pzSig = zSig ? zSig : sqlite3_mprintf("");
  return *pzSig ? SQLITE_OK : SQLITE_NOMEM;
}

/* Different PKs are different keyspaces; refuse, as Dolt does. */
static int mergePass1CheckPrimaryKeysMatch(
  MergePass1Ctx *c,
  const char *zName,
  struct TableEntry *pOurs,
  struct TableEntry *pAnc,
  struct TableEntry *pTheirs
){
  SchemaEntry *ancSE = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
  SchemaEntry *ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
  SchemaEntry *theirSE = findSchemaEntry(
      c->aTheirsSchema, c->nTheirsSchema, zName);
  char *zAncSig = 0, *zOursSig = 0, *zTheirsSig = 0;
  int vsTheirs = 0, vsAncestor = 0;
  int rc = SQLITE_OK;

  if( !ancSE || !ancSE->zSql || !ourSE || !ourSE->zSql
   || !theirSE || !theirSE->zSql ){
    return SQLITE_OK;
  }

  rc = mergePass1PkSignature(ancSE->zSql, zName, &zAncSig);
  if( rc==SQLITE_OK ) rc = mergePass1PkSignature(ourSE->zSql, zName, &zOursSig);
  if( rc==SQLITE_OK ){
    rc = mergePass1PkSignature(theirSE->zSql, zName, &zTheirsSig);
  }
  if( rc==SQLITE_OK ){
    vsTheirs = sqlite3_stricmp(zOursSig, zTheirsSig)!=0
            || ((pOurs->flags ^ pTheirs->flags) & PROLLY_NODE_INTKEY)!=0;
    vsAncestor = sqlite3_stricmp(zOursSig, zAncSig)!=0
              || ((pOurs->flags ^ pAnc->flags) & PROLLY_NODE_INTKEY)!=0;
  }
  sqlite3_free(zAncSig);
  sqlite3_free(zOursSig);
  sqlite3_free(zTheirsSig);
  if( rc!=SQLITE_OK ) return rc;
  if( !vsTheirs && !vsAncestor ) return SQLITE_OK;
  if( c->pzErrMsg ){
    sqlite3_free(*c->pzErrMsg);
    *c->pzErrMsg = sqlite3_mprintf(
        "cannot merge because table '%s' has different primary keys%s",
        zName, vsTheirs ? "" : " in its common ancestor");
  }
  return SQLITE_ERROR;
}

static int mergePass1SchemaAppendsColumns(
  const char *zAncSql,
  const char *zSideSql,
  int *pbAppends
);

/* Cell merge is by merged column position. Relayout the unmerged side
** first or dropped columns shift later values and the ancestor looks
** changed. zOtherSql is that side's layout, not always the schema array
** (adopting theirs overwrites our SQL). */
static int mergePass1RelayoutToMergedSchema(
  MergePass1Ctx *c,
  const char *zName,
  const char *zAncSql,
  const char *zMergedSql,
  const char *zOtherSql,
  u8 flags,
  const ProllyHash *pMergedRoot,
  const ProllyHash *pOtherRoot,
  const ProllyHash *pAncRoot,
  ProllyHash *pOtherOut,
  ProllyHash *pAncOut,
  int *pbRelaid
){
  int bFillSharedDefaults = 0;
  int rc;

  *pbRelaid = 0;
  if( !zAncSql || !zMergedSql || !zOtherSql ) return SQLITE_OK;

  rc = mergePass1SchemaAppendsColumns(
      zAncSql, zMergedSql, &bFillSharedDefaults);
  if( rc!=SQLITE_OK ) return rc;

  rc = normalizeSideToMergedLayout(c->db, zName, pMergedRoot, pOtherRoot,
                                   flags, zAncSql,
                                   zMergedSql, zOtherSql,
                                   bFillSharedDefaults, pOtherOut);
  if( rc!=SQLITE_OK ) return rc;
  rc = normalizeSideToMergedLayout(c->db, zName, pMergedRoot, pAncRoot,
                                   flags, zAncSql,
                                   zMergedSql, zAncSql,
                                   bFillSharedDefaults, pAncOut);
  if( rc!=SQLITE_OK ) return rc;
  *pbRelaid = 1;
  return SQLITE_OK;
}

static int mergePass1SchemaAppendsColumns(
  const char *zAncSql,
  const char *zSideSql,
  int *pbAppends
){
  int bPrefix = 0;
  int nAnc = 0, nSide = 0;
  int rc;

  *pbAppends = 0;
  rc = mergePass1SchemaPrefixInfo(
      zAncSql, zSideSql, &bPrefix, &nAnc, &nSide, 0);
  if( rc==SQLITE_OK ) *pbAppends = bPrefix && nSide>nAnc;
  return rc;
}

/* One side changed columns: that layout wins; move the other and ancestor. */
static int mergePass1RelayoutOneSidedSchema(
  MergePass1Ctx *c,
  const char *zName,
  int bMergedIsOurs,
  struct TableEntry *pOurs,
  struct TableEntry *pAnc,
  struct TableEntry *pTheirs,
  ProllyHash *pOtherOut,
  ProllyHash *pAncOut,
  int *pbRelaid
){
  SchemaEntry *ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
  SchemaEntry *theirSE = findSchemaEntry(c->aTheirsSchema, c->nTheirsSchema, zName);
  SchemaEntry *ancSE = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);

  *pbRelaid = 0;
  if( !ancSE || !ourSE || !theirSE ) return SQLITE_OK;

  return mergePass1RelayoutToMergedSchema(c, zName, ancSE->zSql,
      bMergedIsOurs ? ourSE->zSql : theirSE->zSql,
      bMergedIsOurs ? theirSE->zSql : ourSE->zSql,
      pOurs->flags,
      bMergedIsOurs ? &pOurs->root : &pTheirs->root,
      bMergedIsOurs ? &pTheirs->root : &pOurs->root,
      &pAnc->root,
      pOtherOut, pAncOut, pbRelaid);
}

static int mergePass1BothSides(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zLogicalName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *ancEntry,
  struct TableEntry *theirsEntry
){
  int oursChanged = prollyHashCompare(&c->aOurs[iOurs].root, &ancEntry->root)!=0
                 || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                                      &ancEntry->schemaHash)!=0;
  int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
  int ourSchemaChanged = prollyHashCompare(
      &c->aOurs[iOurs].schemaHash, &ancEntry->schemaHash)!=0;
  int theirSchemaChanged = prollyHashCompare(
      &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;
  int bNamedSchemaObject = (!zName && zSchemaMergeName && zSchemaMergeName[0]);
  int skipRowMerge = 0;
  int schemaChoice = SCHEMA_MERGE_DEFAULT;
  int bDualAddColMerge = 0;
  int bSchemaConflict = 0;
  ProllyHash theirsNormRoot;
  ProllyHash otherNormRoot;
  ProllyHash ancNormRoot;
  char *zOursPrevSql = 0;
  struct TableEntry oursAdj;
  struct TableEntry ancAdj;
  struct TableEntry *pMergeOurs = &c->aOurs[iOurs];
  struct TableEntry *pMergeAnc = ancEntry;
  const ProllyHash *pMergeTheirsRoot = &theirsEntry->root;
  int rc = SQLITE_OK;

  if( zSchemaMergeName && zSchemaMergeName[0]
   && !(ancEntry && ancEntry->zName && zName
        && strcmp(ancEntry->zName, zName)!=0) ){
    ourSchemaChanged = ourSchemaChanged || schemaEntryChangedByName(
        c->aAncSchema, c->nAncSchema, c->aOursSchema, c->nOursSchema,
        zSchemaMergeName);
    theirSchemaChanged = theirSchemaChanged || schemaEntryChangedByName(
        c->aAncSchema, c->nAncSchema, c->aTheirsSchema, c->nTheirsSchema,
        zSchemaMergeName);
  }

  /* Untouched theirs keeps ours verbatim; PKs need not agree. */
  if( zName && (ourSchemaChanged || theirSchemaChanged)
   && (theirsChanged || theirSchemaChanged) ){
    rc = mergePass1CheckPrimaryKeysMatch(
        c, zName, &c->aOurs[iOurs], ancEntry, theirsEntry);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( ourSchemaChanged && theirSchemaChanged
   && (bNamedSchemaObject
       || prollyHashCompare(&c->aOurs[iOurs].schemaHash,
                            &theirsEntry->schemaHash)!=0) ){
    rc = tryResolveSchemaDivergence(
      c->db, zSchemaMergeName, c->pCatAnc, c->pCatOurs, c->pCatTheirs,
      c->ppSchemaActions, c->pnSchemaActions, &skipRowMerge,
      &schemaChoice, c->pzErrMsg);
    if( rc==SQLITE_ERROR ){
      sqlite3_free(c->pzErrMsg ? *c->pzErrMsg : 0);
      if( c->pzErrMsg ) *c->pzErrMsg = 0;
      rc = mergePass1NoteSchemaConflict(c, zSchemaConflictTable, zSchemaMergeName);
      if( rc!=SQLITE_OK ) return rc;
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      skipRowMerge = 1;
      bSchemaConflict = 1;
    }else if( rc!=SQLITE_OK ){
      return rc;
    }
    if( !bSchemaConflict && schemaChoice==SCHEMA_MERGE_THEIRS ){
      SchemaEntry *pOurSe;
      SchemaEntry *pTheirSe;
      char *zSql;
      pOurSe = findSchemaEntry(
          c->aOursSchema, c->nOursSchema, zSchemaMergeName);
      pTheirSe = findSchemaEntry(
          c->aTheirsSchema, c->nTheirsSchema, zSchemaMergeName);
      if( !pOurSe && zName ){
        pOurSe = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
      }
      if( !pTheirSe && ancEntry && ancEntry->zName ){
        pTheirSe = findSchemaEntry(
            c->aTheirsSchema, c->nTheirsSchema, ancEntry->zName);
      }
      if( !pTheirSe ){
        pTheirSe = findSchemaEntryByRootpage(
            c->aTheirsSchema, c->nTheirsSchema, c->aOurs[iOurs].iTable);
      }
      zSql = pTheirSe && pTheirSe->zSql
                 ? sqlite3_mprintf("%s", pTheirSe->zSql) : 0;
      if( !pOurSe || !zSql ){
        sqlite3_free(zSql);
        return pOurSe ? SQLITE_NOMEM : SQLITE_CORRUPT;
      }
      /* Ours still uses the old layout; keep it for the relayout below. */
      zOursPrevSql = pOurSe->zSql;
      pOurSe->zSql = zSql;
    }
    if( !bSchemaConflict && skipRowMerge && zName ){
      SchemaEntry *ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
      SchemaEntry *theirSE = findSchemaEntry(
          c->aTheirsSchema, c->nTheirsSchema, zName);
      SchemaEntry *ancSE = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
      if( ancSE && ancSE->zSql && ourSE && ourSE->zSql
       && theirSE && theirSE->zSql ){
        int bRelaid = 0;
        rc = mergePass1RelayoutToMergedSchema(c, zName,
            ancSE->zSql, ourSE->zSql, theirSE->zSql,
            c->aOurs[iOurs].flags, &c->aOurs[iOurs].root,
            &theirsEntry->root, &ancEntry->root,
            &theirsNormRoot, &ancNormRoot, &bRelaid);
        if( rc!=SQLITE_OK ) return rc;
        ancAdj = *ancEntry;
        memcpy(&ancAdj.root, &ancNormRoot, sizeof(ProllyHash));
        pMergeAnc = &ancAdj;
        pMergeTheirsRoot = &theirsNormRoot;
        bDualAddColMerge = 1;
        skipRowMerge = 0;
      }else{
        c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
      }
    }else if( !bSchemaConflict && skipRowMerge ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    }
  }

  /* sqlite_sequence / sqlite_stat* are derived; skip row merge. */
  if( !skipRowMerge && zName
   && strcmp(zName, "sqlite_sequence")==0
   && oursChanged && theirsChanged ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    skipRowMerge = 1;
  }
  if( !skipRowMerge && zName
   && oursChanged && theirsChanged
   && (strcmp(zName, "sqlite_stat1")==0
    || strcmp(zName, "sqlite_stat4")==0) ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    skipRowMerge = 1;
  }

  if( skipRowMerge ){
    sqlite3_free(zOursPrevSql);
    return SQLITE_OK;
  }

  /* Adopted their schema for a rename: our rows are off-layout. Judge
  ** the move on the schema, not on whether either side wrote rows. */
  if( zName && zOursPrevSql ){
    SchemaEntry *ancSE = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
    SchemaEntry *mergedSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
    int bRelaid = 0;
    if( ancSE && mergedSE ){
      rc = mergePass1RelayoutToMergedSchema(c, zName, ancSE->zSql,
          mergedSE->zSql, zOursPrevSql, c->aOurs[iOurs].flags,
          &theirsEntry->root, &c->aOurs[iOurs].root, &ancEntry->root,
          &otherNormRoot, &ancNormRoot, &bRelaid);
      if( rc!=SQLITE_OK ){
        sqlite3_free(zOursPrevSql);
        return rc;
      }
    }
    if( bRelaid ){
      ancAdj = *ancEntry;
      memcpy(&ancAdj.root, &ancNormRoot, sizeof(ProllyHash));
      pMergeAnc = &ancAdj;
      oursAdj = c->aOurs[iOurs];
      memcpy(&oursAdj.root, &otherNormRoot, sizeof(ProllyHash));
      pMergeOurs = &oursAdj;
    }
  }
  sqlite3_free(zOursPrevSql);
  zOursPrevSql = 0;

  /* Mirror: we kept our schema, so theirs is the off-layout side. */
  if( zName && !bSchemaConflict && schemaChoice==SCHEMA_MERGE_OURS ){
    SchemaEntry *ancSE = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
    SchemaEntry *ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
    SchemaEntry *theirSE = findSchemaEntry(c->aTheirsSchema, c->nTheirsSchema, zName);
    int bRelaid = 0;
    if( ancSE && ourSE && theirSE ){
      rc = mergePass1RelayoutToMergedSchema(c, zName, ancSE->zSql,
          ourSE->zSql, theirSE->zSql, c->aOurs[iOurs].flags,
          &c->aOurs[iOurs].root, &theirsEntry->root, &ancEntry->root,
          &otherNormRoot, &ancNormRoot, &bRelaid);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( bRelaid ){
      ancAdj = *ancEntry;
      memcpy(&ancAdj.root, &ancNormRoot, sizeof(ProllyHash));
      pMergeAnc = &ancAdj;
      pMergeTheirsRoot = &otherNormRoot;
    }
  }

  if( zName && oursChanged && theirsChanged
   && ourSchemaChanged!=theirSchemaChanged ){
    int bRelaid = 0;
    rc = mergePass1RelayoutOneSidedSchema(
        c, zName, ourSchemaChanged, &c->aOurs[iOurs], ancEntry, theirsEntry,
        &otherNormRoot, &ancNormRoot, &bRelaid);
    if( rc!=SQLITE_OK ) return rc;
    if( bRelaid ){
      ancAdj = *ancEntry;
      memcpy(&ancAdj.root, &ancNormRoot, sizeof(ProllyHash));
      pMergeAnc = &ancAdj;
      if( ourSchemaChanged ){
        pMergeTheirsRoot = &otherNormRoot;
      }else{
        oursAdj = c->aOurs[iOurs];
        memcpy(&oursAdj.root, &otherNormRoot, sizeof(ProllyHash));
        pMergeOurs = &oursAdj;
      }
    }
  }

  if( bDualAddColMerge || (oursChanged && theirsChanged) ){
    return mergePass1MergeTableData(
        c, zName, zLogicalName, pMergeOurs, pMergeAnc,
        pMergeTheirsRoot, ourSchemaChanged, theirSchemaChanged,
        schemaChoice, theirsEntry);
  }
  if( theirsChanged ){
    struct TableEntry merged = *pMergeOurs;
    memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
    memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
    merged.flags = theirsEntry->flags;
    c->aMerged[(*c->pnMerged)++] = merged;
  }else{
    c->aMerged[(*c->pnMerged)++] = *pMergeOurs;
  }
  return SQLITE_OK;
}

static int mergePass1MergeOursEntry(MergePass1Ctx *c, int iOurs){
  const char *zName = 0;
  const char *zLogicalName = 0;
  const char *zSchemaMergeName = 0;
  const char *zSchemaConflictTable = 0;
  struct TableEntry *ancEntry = 0;
  struct TableEntry *theirsEntry = 0;
  int bSkipMaster = 0;
  int rc;

  rc = mergePass1ResolveOursEntry(c, iOurs,
      &zName, &zLogicalName, &zSchemaMergeName, &zSchemaConflictTable,
      &ancEntry, &theirsEntry, &bSkipMaster);
  if( rc!=SQLITE_OK ) return rc;
  if( bSkipMaster ) return SQLITE_OK;

  if( hasSchemaConflictObject(*c->ppConflictTables, *c->pnConflictTables,
                              zSchemaMergeName)
   || (zName && hasSchemaConflictTable(*c->ppConflictTables,
                                       *c->pnConflictTables, zName)) ){
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
    return SQLITE_OK;
  }

  if( !ancEntry ){
    return mergePass1OursAdded(c, iOurs, zName, zSchemaMergeName,
                               zSchemaConflictTable, theirsEntry);
  }
  if( !theirsEntry ){
    return mergePass1OursModifyTheirsDelete(c, iOurs, zName,
        zSchemaMergeName, zSchemaConflictTable, ancEntry);
  }
  return mergePass1BothSides(c, iOurs, zName, zLogicalName,
      zSchemaMergeName, zSchemaConflictTable, ancEntry, theirsEntry);
}

static int mergePass1TheirsModifyDelete(MergePass1Ctx *c){
  int i, rc;

  for(i=0; i<c->nTheirs; i++){
    const char *zObject = c->aTheirs[i].zName;
    const char *zTable = zObject;
    struct TableEntry *pAncEntry = 0;
    struct TableEntry *pOurEntry = 0;
    int changed = 0;

    if( c->aTheirs[i].iTable<=1 ) continue;
    if( zObject ){
      pAncEntry = doltliteFindTableByName(c->aAnc, c->nAnc, zObject);
      pOurEntry = doltliteFindTableByName(c->aOurs, c->nOurs, zObject);
      if( !pOurEntry ){
        struct TableEntry *pByNo = doltliteFindTableByNumber(
            c->aOurs, c->nOurs, c->aTheirs[i].iTable);
        if( pByNo && pByNo->zName
         && doltliteFindTableByName(c->aTheirs, c->nTheirs, pByNo->zName)
         && doltliteFindTableByName(c->aOurs, c->nOurs, pByNo->zName)
         && !doltliteFindTableByName(c->aAnc, c->nAnc, pByNo->zName) ){
          pByNo = 0;
        }
        if( pByNo && pByNo->zName && pAncEntry && pAncEntry->zName
         && strcmp(pByNo->zName, pAncEntry->zName)!=0
         && strcmp(zObject, pAncEntry->zName)!=0 ){
          pByNo = 0;
        }
        pOurEntry = pByNo;
      }
      if( pAncEntry ){
        changed = prollyHashCompare(&c->aTheirs[i].root, &pAncEntry->root)!=0
               || prollyHashCompare(&c->aTheirs[i].schemaHash,
                                    &pAncEntry->schemaHash)!=0;
      }
    }else{
      SchemaEntry *pTheirSe = findSchemaEntryByRootpage(
          c->aTheirsSchema, c->nTheirsSchema, c->aTheirs[i].iTable);
      if( !pTheirSe || !pTheirSe->zName || !pTheirSe->zType
       || strcmp(pTheirSe->zType, "index")!=0 ){
        continue;
      }
      zObject = pTheirSe->zName;
      zTable = pTheirSe->zTblName ? pTheirSe->zTblName : pTheirSe->zName;
      pAncEntry = findCatalogEntryBySchemaObject(
          c->aAnc, c->nAnc, c->aAncSchema, c->nAncSchema,
          pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
      pOurEntry = findCatalogEntryBySchemaObject(
          c->aOurs, c->nOurs, c->aOursSchema, c->nOursSchema,
          pTheirSe->zType, pTheirSe->zName, pTheirSe->zTblName);
      if( pAncEntry ){
        changed = schemaEntryChangedByName(c->aAncSchema, c->nAncSchema,
                                           c->aTheirsSchema, c->nTheirsSchema,
                                           pTheirSe->zName);
      }
    }
    if( !pAncEntry || pOurEntry || !changed ) continue;
    if( !c->aTheirs[i].zName ){
      if( c->ppSchemaActions && c->pnSchemaActions ){
        rc = recordSchemaAddColumns(c->ppSchemaActions, c->pnSchemaActions,
                                    zObject, 0, 0);
        if( rc!=SQLITE_OK ) return rc;
      }
      continue;
    }
    rc = mergePass1NoteSchemaConflict(c, zTable, zObject);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

/* Ancestor names one side renamed and the other dropped. Dolt keeps
** the renamed table from either branch. */
static int mergePass1CollectRenameOverDrop(
  MergePass1Ctx *c,
  MergeRowPolicy *pPolicy
){
  int dir;
  for(dir=0; dir<2; dir++){
    struct TableEntry *aRenamed = dir ? c->aOurs : c->aTheirs;
    int nRenamed = dir ? c->nOurs : c->nTheirs;
    struct TableEntry *aDropped = dir ? c->aTheirs : c->aOurs;
    int nDropped = dir ? c->nTheirs : c->nOurs;
    SchemaEntry *aRenamedSchema = dir ? c->aOursSchema : c->aTheirsSchema;
    int nRenamedSchema = dir ? c->nOursSchema : c->nTheirsSchema;
    int i;
    for(i=0; i<nRenamed; i++){
      const char *zAnc = 0;
      const char **azNew;
      if( aRenamed[i].iTable<=1 ) continue;
      if( !mergeTableRenameOtherDrop(
            c->aAnc, c->nAnc, aDropped, nDropped, aRenamed, nRenamed,
            c->aAncSchema, c->nAncSchema,
            aRenamedSchema, nRenamedSchema, &aRenamed[i], &zAnc) ){
        continue;
      }
      if( !zAnc ) continue;
      azNew = sqlite3_realloc(
          (void*)pPolicy->azRenameOverDrop,
          (pPolicy->nRenameOverDrop + 1) * (int)sizeof(char*));
      if( !azNew ) return SQLITE_NOMEM;
      pPolicy->azRenameOverDrop = azNew;
      pPolicy->azRenameOverDrop[pPolicy->nRenameOverDrop++] = zAnc;
    }
  }
  return SQLITE_OK;
}

static int mergePass1CollectDualRename(
  MergePass1Ctx *c,
  MergeRowPolicy *pPolicy
){
  int i;
  for(i=0; i<c->nAnc; i++){
    struct TableEntry *pOurs;
    struct TableEntry *pTheirs;
    const char **azNew;
    if( c->aAnc[i].iTable<=1 || !c->aAnc[i].zName ) continue;
    pOurs = doltliteFindTableByNumber(c->aOurs, c->nOurs, c->aAnc[i].iTable);
    pTheirs = doltliteFindTableByNumber(c->aTheirs, c->nTheirs, c->aAnc[i].iTable);
    if( !pOurs || !pTheirs || !pOurs->zName || !pTheirs->zName ) continue;
    if( sqlite3_stricmp(pOurs->zName, c->aAnc[i].zName)==0 ) continue;
    if( sqlite3_stricmp(pTheirs->zName, c->aAnc[i].zName)==0 ) continue;
    if( sqlite3_stricmp(pOurs->zName, pTheirs->zName)==0 ) continue;
    azNew = sqlite3_realloc(
        (void*)pPolicy->azDualRename,
        (pPolicy->nDualRename + 1) * (int)sizeof(char*));
    if( !azNew ) return SQLITE_NOMEM;
    pPolicy->azDualRename = azNew;
    pPolicy->azDualRename[pPolicy->nDualRename++] = c->aAnc[i].zName;
  }
  return SQLITE_OK;
}

static void mergePass1FreeRowPolicy(MergeRowPolicy *pPolicy){
  sqlite3_free((void*)pPolicy->azRenameOverDrop);
  sqlite3_free((void*)pPolicy->azDualRename);
  pPolicy->azRenameOverDrop = 0;
  pPolicy->azDualRename = 0;
  pPolicy->nRenameOverDrop = 0;
  pPolicy->nDualRename = 0;
  pPolicy->nDeleteCompareFields = 0;
}

static int mergePass1IsAuxSchemaType(const char *zType){
  return zType && (strcmp(zType, "view")==0 || strcmp(zType, "trigger")==0);
}

static int mergePass1AuxSchemaSame(const SchemaEntry *pA, const SchemaEntry *pB){
  const char *zASql;
  const char *zBSql;
  const char *zATbl;
  const char *zBTbl;
  if( !pA || !pB ) return 0;
  zASql = pA->zSql ? pA->zSql : "";
  zBSql = pB->zSql ? pB->zSql : "";
  zATbl = pA->zTblName ? pA->zTblName : "";
  zBTbl = pB->zTblName ? pB->zTblName : "";
  return strcmp(zASql, zBSql)==0 && sqlite3_stricmp(zATbl, zBTbl)==0;
}

/* Views/triggers are catalog-only; competing defs are schema conflicts. */
static int mergePass1NoteAuxSchemaConflicts(MergePass1Ctx *c){
  int side, i, rc;

  for(side=0; side<2; side++){
    SchemaEntry *a = side==0 ? c->aOursSchema : c->aTheirsSchema;
    int n = side==0 ? c->nOursSchema : c->nTheirsSchema;
    for(i=0; i<n; i++){
      SchemaEntry *pOurs;
      SchemaEntry *pTheirs;
      const char *zName = a[i].zName;
      const char *zTable;
      int oursChanged;
      int theirsChanged;
      if( !mergePass1IsAuxSchemaType(a[i].zType) || !zName ) continue;
      if( side==1
       && findSchemaEntry(c->aOursSchema, c->nOursSchema, zName) ){
        continue;
      }
      pOurs = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
      pTheirs = findSchemaEntry(c->aTheirsSchema, c->nTheirsSchema, zName);
      if( !pOurs && !pTheirs ) continue;
      if( pOurs && pTheirs && mergePass1AuxSchemaSame(pOurs, pTheirs) ){
        continue;
      }
      oursChanged = schemaEntryChangedByName(
          c->aAncSchema, c->nAncSchema, c->aOursSchema, c->nOursSchema, zName);
      theirsChanged = schemaEntryChangedByName(
          c->aAncSchema, c->nAncSchema, c->aTheirsSchema, c->nTheirsSchema,
          zName);
      if( !oursChanged || !theirsChanged ) continue;
      zTable = pOurs && pOurs->zTblName ? pOurs->zTblName
             : (pTheirs && pTheirs->zTblName ? pTheirs->zTblName : zName);
      rc = mergePass1NoteSchemaConflict(c, zTable, zName);
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  return SQLITE_OK;
}

static int mergePass1MergeMaster(MergePass1Ctx *c, int iTable1Idx){
  struct TableEntry *ancEntry;
  struct TableEntry *theirsEntry;
  int bPreferOurMasterHere;
  int rc = SQLITE_OK;

  if( iTable1Idx < 0 ) return SQLITE_OK;

  ancEntry = doltliteFindTableByNumber(c->aAnc, c->nAnc, 1);
  theirsEntry = doltliteFindTableByNumber(c->aTheirs, c->nTheirs, 1);
  bPreferOurMasterHere = hasAnySchemaConflict(
      *c->ppConflictTables, *c->pnConflictTables)
      || (c->bPreferOurMaster
      && replayDropsDisjointSchemaObject(c->aAncSchema, c->nAncSchema,
                                         c->aTheirsSchema, c->nTheirsSchema));

  if( !ancEntry ){
    if( theirsEntry ){
      if( prollyHashCompare(&c->aOurs[iTable1Idx].root, &theirsEntry->root)!=0
       || prollyHashCompare(&c->aOurs[iTable1Idx].schemaHash,
                            &theirsEntry->schemaHash)!=0 ){
        return SQLITE_ERROR;
      }
    }
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    return SQLITE_OK;
  }
  if( !theirsEntry ){
    int oursChanged = prollyHashCompare(
        &c->aOurs[iTable1Idx].root, &ancEntry->root)!=0;
    if( oursChanged ) return SQLITE_ERROR;
    return SQLITE_OK;
  }

  {
    int oursChanged = prollyHashCompare(
        &c->aOurs[iTable1Idx].root, &ancEntry->root)!=0;
    int theirsChanged = prollyHashCompare(
        &theirsEntry->root, &ancEntry->root)!=0;
    int hasSchemaActions = (c->ppSchemaActions && c->pnSchemaActions
                            && *c->pnSchemaActions > 0);
    if( bPreferOurMasterHere ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
      rc = mergePass1NoteAuxSchemaConflicts(c);
      if( rc!=SQLITE_OK ) return rc;
      return SQLITE_DONE;
    }
    if( oursChanged && theirsChanged
     && (hasSchemaActions || c->bDisjointSchemaChanges) ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
      rc = mergePass1NoteAuxSchemaConflicts(c);
    }else if( oursChanged && theirsChanged ){
      ProllyHash mergedTableRoot;
      int nConflicts = 0;
      DoltliteConflictRow *aConflictRows = 0;
      int theirSchemaChanged2 = prollyHashCompare(
          &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;
      MergeRowPolicy policy;

      memset(&policy, 0, sizeof(policy));
      rc = mergePass1CollectRenameOverDrop(c, &policy);
      if( rc==SQLITE_OK ) rc = mergePass1CollectDualRename(c, &policy);
      if( rc!=SQLITE_OK ){
        mergePass1FreeRowPolicy(&policy);
        return rc;
      }
      rc = mergeTableRows(c->db, &ancEntry->root, &c->aOurs[iTable1Idx].root,
                          &theirsEntry->root, c->aOurs[iTable1Idx].flags,
                          ancEntry->flags, theirsEntry->flags,
                          &mergedTableRoot, &nConflicts, &aConflictRows,
                          NULL, 0, &policy);
      mergePass1FreeRowPolicy(&policy);
      if( rc!=SQLITE_OK ) return rc;

      {
        struct TableEntry merged = c->aOurs[iTable1Idx];
        memcpy(&merged.root, &mergedTableRoot, sizeof(ProllyHash));
        if( theirSchemaChanged2
         && prollyHashCompare(&c->aOurs[iTable1Idx].schemaHash,
                              &ancEntry->schemaHash)==0 ){
          memcpy(&merged.schemaHash, &theirsEntry->schemaHash,
                 sizeof(ProllyHash));
          merged.flags = theirsEntry->flags;
        }
        c->aMerged[(*c->pnMerged)++] = merged;
      }

      freeConflictRows(aConflictRows, nConflicts);
      rc = mergePass1NoteAuxSchemaConflicts(c);
      if( rc!=SQLITE_OK ) return rc;
      if( nConflicts>0
       && !hasAnySchemaConflict(*c->ppConflictTables, *c->pnConflictTables) ){
        int i;
        rc = SQLITE_OK;
        for(i=0; i<c->nAnc && rc==SQLITE_OK; i++){
          const char *zName = c->aAnc[i].zName;
          if( !zName || c->aAnc[i].iTable<=1 ) continue;
          if( !schemaEntryChangedByName(c->aAncSchema, c->nAncSchema,
                                        c->aOursSchema, c->nOursSchema, zName) ){
            continue;
          }
          if( !schemaEntryChangedByName(c->aAncSchema, c->nAncSchema,
                                        c->aTheirsSchema, c->nTheirsSchema,
                                        zName) ){
            continue;
          }
          if( hasSchemaConflictObject(*c->ppConflictTables,
                                      *c->pnConflictTables, zName) ){
            continue;
          }
          rc = mergePass1NoteSchemaConflict(c, zName, zName);
        }
      }
    }else if( theirsChanged ){
      struct TableEntry merged = c->aOurs[iTable1Idx];
      memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
      memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
      merged.flags = theirsEntry->flags;
      c->aMerged[(*c->pnMerged)++] = merged;
    }else{
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    }
  }
  return rc;
}

static void mergePass1ApplyIndexPatches(MergePass1Ctx *c){
  int ip, k;
  for(ip=0; ip<c->nPatches; ip++){
    for(k=0; k<*c->pnMerged; k++){
      if( c->aMerged[k].iTable==c->aPatches[ip].iTable ){
        memcpy(&c->aMerged[k].root, &c->aPatches[ip].mergedRoot,
               sizeof(ProllyHash));
        break;
      }
    }
  }
}

int mergeCatalogPass1(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  struct TableEntry *aOurs, int nOurs,
  struct TableEntry *aTheirs, int nTheirs,
  SchemaEntry *aAncSchema, int nAncSchema,
  SchemaEntry *aOursSchema, int nOursSchema,
  SchemaEntry *aTheirsSchema, int nTheirsSchema,
  struct TableEntry *aMerged, int *pnMerged,
  MergeConflictTable **ppConflictTables, int *pnConflictTables,
  int *pTotalConflicts,
  char **pzErrMsg,
  const ProllyHash *pCatAnc,
  const ProllyHash *pCatOurs,
  const ProllyHash *pCatTheirs,
  SchemaMergeAction **ppSchemaActions, int *pnSchemaActions,
  int bDisjointSchemaChanges,
  int bPreferOurMaster,
  int bBranchMerge,
  char ***pazReindex, int *pnReindex
){
  MergePass1Ctx c;
  int i, rc = SQLITE_OK;
  int iTable1Idx = -1;

  memset(&c, 0, sizeof(c));
  c.db = db;
  c.aAnc = aAnc; c.nAnc = nAnc;
  c.aOurs = aOurs; c.nOurs = nOurs;
  c.aTheirs = aTheirs; c.nTheirs = nTheirs;
  c.aAncSchema = aAncSchema; c.nAncSchema = nAncSchema;
  c.aOursSchema = aOursSchema; c.nOursSchema = nOursSchema;
  c.aTheirsSchema = aTheirsSchema; c.nTheirsSchema = nTheirsSchema;
  c.aMerged = aMerged; c.pnMerged = pnMerged;
  c.ppConflictTables = ppConflictTables;
  c.pnConflictTables = pnConflictTables;
  c.pTotalConflicts = pTotalConflicts;
  c.pzErrMsg = pzErrMsg;
  c.pCatAnc = pCatAnc; c.pCatOurs = pCatOurs; c.pCatTheirs = pCatTheirs;
  c.ppSchemaActions = ppSchemaActions; c.pnSchemaActions = pnSchemaActions;
  c.bDisjointSchemaChanges = bDisjointSchemaChanges;
  c.bPreferOurMaster = bPreferOurMaster;
  c.bBranchMerge = bBranchMerge;
  c.pazReindex = pazReindex; c.pnReindex = pnReindex;

  rc = mergePass1CheckIndexOverRenamedColumn(&c);
  if( rc==SQLITE_OK ) rc = mergePass1CheckTriggerOverRenamedTable(&c);
  if( rc==SQLITE_OK ) rc = mergePass1CheckRowEditOfDroppedColumn(&c);
  if( rc==SQLITE_OK ) rc = mergePass1CheckDuplicateIndexColumns(&c);
  if( rc==SQLITE_OK ) rc = mergePass1CheckDependentOverDualRename(&c);
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  for(i=0; i<nOurs; i++){
    if( aOurs[i].iTable==1 ){
      iTable1Idx = i;
      continue;
    }
    rc = mergePass1MergeOursEntry(&c, i);
    if( rc!=SQLITE_OK ){
      mergePass1Free(&c);
      return rc;
    }
  }

  /* Record theirs-surviving conflicts before choosing the master root. */
  rc = mergePass1TheirsModifyDelete(&c);
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  rc = mergePass1MergeMaster(&c, iTable1Idx);
  if( rc==SQLITE_DONE ){
    mergePass1Free(&c);
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  /* Inline index roots already include row conflicts. */
  mergePass1ApplyIndexPatches(&c);
  mergePass1Free(&c);
  return rc;
}

#endif
