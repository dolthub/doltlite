#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* Catalog merge pass 1: walk ours/theirs entries, row-merge tables, record
** schema conflicts and secondary-index patches. */

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

typedef struct IndexMergePatch IndexMergePatch;
struct IndexMergePatch {
  Pgno iTable;
  ProllyHash mergedRoot;
};

/* Shared state for pass-1 catalog merge. Keeps the long parameter list off
** the leaf helpers and makes the patch list a single ownership root. */
typedef struct MergePass1Ctx MergePass1Ctx;
struct MergePass1Ctx {
  sqlite3 *db;
  struct TableEntry *aAnc; int nAnc;
  struct TableEntry *aOurs; int nOurs;
  struct TableEntry *aTheirs; int nTheirs;
  SchemaEntry *aAncSchema; int nAncSchema;
  SchemaEntry *aOursSchema; int nOursSchema;
  SchemaEntry *aTheirsSchema; int nTheirsSchema;
  struct TableEntry *aMerged; int *pnMerged;
  MergeConflictTable **ppConflictTables; int *pnConflictTables;
  int *pTotalConflicts;
  char **pzErrMsg;
  const ProllyHash *pCatAnc;
  const ProllyHash *pCatOurs;
  const ProllyHash *pCatTheirs;
  SchemaMergeAction **ppSchemaActions; int *pnSchemaActions;
  int bDisjointSchemaChanges;
  int bPreferOurMaster;
  char ***pazReindex; int *pnReindex;
  IndexMergePatch *aPatches;
  int nPatches;
  int nPatchesAlloc;
};

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

/* Free KeyInfo refs owned by collectIndexes before freeing the array. */
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

/* Build secondary-index merge descriptors for a named table. Every secondary
** index is patched inline with a real KeyInfo (NOCASE/RTRIM/DESC included). */
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
    /* Proceeding would merge table rows but leave secondary indexes stale. */
    return SQLITE_NOMEM;
  }
  memset(aIdxInfo, 0, nIdx*(int)sizeof(MergeIndexInfo));

  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    struct TableEntry *oursIdx;
    /* On WITHOUT ROWID tables the PK pseudo-index is the table tree itself;
    ** on rowid tables it is a real unique index that merges like any other. */
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

/* Three-way merge one table's data (and inline secondary indexes). */
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
  int rc;
  int handled = 0;

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
                        &mergedTableRoot, &nConflicts, &aConflictRows,
                        aIdxInfo, nIdxInfo, 0);
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

/* Resolve catalog lookups for one ours entry (named table or unnamed index). */
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

/* Ours added the object (absent from ancestor). */
static int mergePass1OursAdded(
  MergePass1Ctx *c,
  int iOurs,
  const char *zName,
  const char *zSchemaMergeName,
  const char *zSchemaConflictTable,
  struct TableEntry *theirsEntry
){
  int rc;
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
  c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
  return SQLITE_OK;
}

/* Ancestor had it; theirs dropped it. */
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

  if( !oursChanged ) return SQLITE_OK;

  /* An index root moves with table data; only a definition change is a
  ** real modify-versus-delete conflict. Unchanged definition: DROP wins. */
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

/* Ordered primary-key signature ("name type,name type") of the table zSql
** creates, built by executing it in a scratch db. No declared PK yields the
** empty signature, which matches SQLite treating those tables as rowid keyed
** regardless of the rest of the schema. */
/* Ordered primary-key signature of the table zSql creates, built by
** executing it in a scratch db.
**
** The signature must name everything that decides where a row sorts and
** which rows collide, because that is what makes two keyspaces mergeable:
** the columns in key order, their types, their collations, and their sort
** directions. A table with a real primary-key index reads all four from
** that index. Otherwise the key is either a rowid alias, where none of it
** applies, or absent entirely; both fall back to the declared columns, and
** no declared PK yields the empty signature, matching SQLite treating such
** tables as rowid keyed regardless of the rest of the schema. */
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

/* Tables whose primary keys differ never row-merge: the keys identify
** different things, so there is no shared ancestor keyspace to merge in.
** Refuse the merge outright, the same way Dolt does. */
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

/* Both sides still have the object. */
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
  const ProllyHash *pMergeTheirsRoot = &theirsEntry->root;
  int rc = SQLITE_OK;

  if( zSchemaMergeName && zSchemaMergeName[0] ){
    ourSchemaChanged = ourSchemaChanged || schemaEntryChangedByName(
        c->aAncSchema, c->nAncSchema, c->aOursSchema, c->nOursSchema,
        zSchemaMergeName);
    theirSchemaChanged = theirSchemaChanged || schemaEntryChangedByName(
        c->aAncSchema, c->nAncSchema, c->aTheirsSchema, c->nTheirsSchema,
        zSchemaMergeName);
  }

  /* A theirs side that never touched the table keeps ours verbatim, so no
  ** cross-key merge can happen and the primary keys need not agree. */
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
      SchemaEntry *pOurSe = findSchemaEntry(
          c->aOursSchema, c->nOursSchema, zSchemaMergeName);
      SchemaEntry *pTheirSe = findSchemaEntry(
          c->aTheirsSchema, c->nTheirsSchema, zSchemaMergeName);
      char *zSql = pTheirSe && pTheirSe->zSql
                 ? sqlite3_mprintf("%s", pTheirSe->zSql) : 0;
      if( !pOurSe || !zSql ){
        sqlite3_free(zSql);
        return pOurSe ? SQLITE_NOMEM : SQLITE_CORRUPT;
      }
      sqlite3_free(pOurSe->zSql);
      pOurSe->zSql = zSql;
    }
    if( !bSchemaConflict && skipRowMerge && zName ){
      SchemaEntry *ourSE = findSchemaEntry(c->aOursSchema, c->nOursSchema, zName);
      SchemaEntry *theirSE = findSchemaEntry(
          c->aTheirsSchema, c->nTheirsSchema, zName);
      SchemaEntry *ancSE = findSchemaEntry(c->aAncSchema, c->nAncSchema, zName);
      if( ancSE && ancSE->zSql && ourSE && ourSE->zSql
       && theirSE && theirSE->zSql ){
        rc = normalizeTheirsToMergedLayout(c->db, &theirsEntry->root,
                                           c->aOurs[iOurs].flags, ancSE->zSql,
                                           ourSE->zSql, theirSE->zSql,
                                           &theirsNormRoot);
        if( rc!=SQLITE_OK ) return rc;
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

  /* sqlite_sequence / sqlite_stat* are derived; refresh after merge. */
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

  if( skipRowMerge ) return SQLITE_OK;

  if( bDualAddColMerge || (oursChanged && theirsChanged) ){
    return mergePass1MergeTableData(
        c, zName, zLogicalName, &c->aOurs[iOurs], ancEntry,
        pMergeTheirsRoot, ourSchemaChanged, theirSchemaChanged,
        schemaChoice, theirsEntry);
  }
  if( theirsChanged ){
    struct TableEntry merged = c->aOurs[iOurs];
    memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
    memcpy(&merged.schemaHash, &theirsEntry->schemaHash, sizeof(ProllyHash));
    merged.flags = theirsEntry->flags;
    c->aMerged[(*c->pnMerged)++] = merged;
  }else{
    c->aMerged[(*c->pnMerged)++] = c->aOurs[iOurs];
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

/* Modify-versus-delete where the survivor is on theirs. */
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

/* Objects one side renamed and the other dropped, by the name they had in the
** ancestor. Collected from both sides: Dolt keeps such a table whichever branch
** did the renaming. */
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

/* Merge catalog root (iTable==1). Deferred so schema actions are known. */
static int mergePass1MergeMaster(MergePass1Ctx *c, int iTable1Idx){
  struct TableEntry *ancEntry;
  struct TableEntry *theirsEntry;
  int hasSchemaActions;
  int bPreferOurMasterHere;
  int rc = SQLITE_OK;

  if( iTable1Idx < 0 ) return SQLITE_OK;

  ancEntry = doltliteFindTableByNumber(c->aAnc, c->nAnc, 1);
  theirsEntry = doltliteFindTableByNumber(c->aTheirs, c->nTheirs, 1);
  hasSchemaActions = (c->ppSchemaActions && c->pnSchemaActions
                      && *c->pnSchemaActions > 0);
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
    if( bPreferOurMasterHere ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
      /* Original pass1 returned here without applying index patches. */
      return SQLITE_DONE;
    }
    if( oursChanged && theirsChanged && hasSchemaActions ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    }else if( oursChanged && theirsChanged && c->bDisjointSchemaChanges ){
      c->aMerged[(*c->pnMerged)++] = c->aOurs[iTable1Idx];
    }else if( oursChanged && theirsChanged ){
      ProllyHash mergedTableRoot;
      int nConflicts = 0;
      DoltliteConflictRow *aConflictRows = 0;
      int theirSchemaChanged2 = prollyHashCompare(
          &theirsEntry->schemaHash, &ancEntry->schemaHash)!=0;

      MergeRowPolicy policy;
      memset(&policy, 0, sizeof(policy));
      rc = mergePass1CollectRenameOverDrop(c, &policy);
      if( rc!=SQLITE_OK ){
        sqlite3_free((void*)policy.azRenameOverDrop);
        return rc;
      }
      rc = mergeTableRows(c->db, &ancEntry->root, &c->aOurs[iTable1Idx].root,
                          &theirsEntry->root, c->aOurs[iTable1Idx].flags,
                          &mergedTableRoot, &nConflicts, &aConflictRows,
                          NULL, 0, &policy);
      sqlite3_free((void*)policy.azRenameOverDrop);
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

      if( nConflicts>0 ){
        *c->pTotalConflicts += nConflicts;
        rc = appendConflictTable(c->ppConflictTables, c->pnConflictTables,
                                 "(sqlite_master)", nConflicts, aConflictRows);
        if( rc!=SQLITE_OK ) return rc;
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
  c.pazReindex = pazReindex; c.pnReindex = pnReindex;

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

  /* Conflicts whose surviving object is on theirs must be recorded before
  ** the master-catalog root is chosen below. */
  rc = mergePass1TheirsModifyDelete(&c);
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  rc = mergePass1MergeMaster(&c, iTable1Idx);
  if( rc==SQLITE_DONE ){
    /* Prefer-our-master path: original returned before patch apply. */
    mergePass1Free(&c);
    return SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    mergePass1Free(&c);
    return rc;
  }

  /* Prefer inline index roots; they already account for row conflicts. */
  mergePass1ApplyIndexPatches(&c);
  mergePass1Free(&c);
  return rc;
}

#endif
