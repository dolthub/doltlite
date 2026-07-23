
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "prolly_diff.h"
#include "prolly_mutate.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>

#define WS_IDX_STAGED_EQ 0x01

typedef struct WorkspaceRow WorkspaceRow;
struct WorkspaceRow {
  i64 rowid;
  int staged;
  u8 diffType;
  u8 flags;
  u8 *pKey; int nKey; i64 intKey;
  u8 *pOldVal; int nOldVal;
  u8 *pNewVal; int nNewVal;
};

typedef struct WorkspaceVtab WorkspaceVtab;
struct WorkspaceVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
  WorkspaceRow *aCache;
  int nCache;
  int nCacheAlloc;
};

typedef struct WorkspaceCursor WorkspaceCursor;
struct WorkspaceCursor {
  sqlite3_vtab_cursor base;
  int iRow;
  int eof;
  int phase;
  int iterOpen;
  int iterStaged;
  u8 iterFlags;
  ProllyDiffIter iter;
  ProllyHash headRoot, stagedRoot, workingRoot;
  u8 stagedFlags, workingFlags;
  int stagedOnly;
};

static void wsFreeRows(WorkspaceRow *aRow, int nRow){
  int i;
  for(i=0; i<nRow; i++){
    sqlite3_free(aRow[i].pKey);
    sqlite3_free(aRow[i].pOldVal);
    sqlite3_free(aRow[i].pNewVal);
  }
  sqlite3_free(aRow);
}

static void wsClearCache(WorkspaceVtab *p){
  wsFreeRows(p->aCache, p->nCache);
  p->aCache = 0;
  p->nCache = 0;
  p->nCacheAlloc = 0;
}

static char *wsBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(id INTEGER, staged INTEGER, diff_type TEXT");
  if( ci->nCol>0 ){
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol,
                                       "to_", ", ")!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  if( ci->nCol>0 ){
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol,
                                       "from_", ", ")!=SQLITE_OK ){
      sqlite3_str_reset(pStr);
      return 0;
    }
  }
  sqlite3_str_appendall(pStr, ")");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int wsAppendRow(
  WorkspaceVtab *pVtab,
  int staged,
  u8 flags,
  const ProllyDiffChange *pChange
){
  WorkspaceRow row;
  WorkspaceRow *aNew;
  int nNew;

  memset(&row, 0, sizeof(row));
  row.rowid = pVtab->nCache + 1;
  row.staged = staged;
  row.diffType = pChange->type;
  row.flags = flags;
  row.intKey = pChange->intKey;
  if( pChange->nKey>0 ){
    row.pKey = sqlite3_malloc(pChange->nKey);
    if( !row.pKey ) goto nomem;
    memcpy(row.pKey, pChange->pKey, pChange->nKey);
    row.nKey = pChange->nKey;
  }
  if( pChange->nOldVal>0 ){
    row.pOldVal = sqlite3_malloc(pChange->nOldVal);
    if( !row.pOldVal ) goto nomem;
    memcpy(row.pOldVal, pChange->pOldVal, pChange->nOldVal);
    row.nOldVal = pChange->nOldVal;
  }
  if( pChange->nNewVal>0 ){
    row.pNewVal = sqlite3_malloc(pChange->nNewVal);
    if( !row.pNewVal ) goto nomem;
    memcpy(row.pNewVal, pChange->pNewVal, pChange->nNewVal);
    row.nNewVal = pChange->nNewVal;
  }
  /* PK-only clustered rows store an empty value; rebuild the record from the
  ** key for each side the diff type says exists. */
  if( (row.nOldVal==0 && pChange->type!=PROLLY_DIFF_ADD)
   || (row.nNewVal==0 && pChange->type!=PROLLY_DIFF_DELETE) ){
    u8 *pRec = 0; int nRec = 0;
    if( doltliteRecordFromClusteredKey(pVtab->db, pVtab->zTableName,
            pChange->pKey, pChange->nKey, &pRec, &nRec)!=SQLITE_OK ){
      goto nomem;
    }
    if( pRec ){
      if( row.nOldVal==0 && pChange->type!=PROLLY_DIFF_ADD ){
        row.pOldVal = sqlite3_malloc(nRec);
        if( !row.pOldVal ){ sqlite3_free(pRec); goto nomem; }
        memcpy(row.pOldVal, pRec, nRec);
        row.nOldVal = nRec;
      }
      if( row.nNewVal==0 && pChange->type!=PROLLY_DIFF_DELETE ){
        row.pNewVal = sqlite3_malloc(nRec);
        if( !row.pNewVal ){ sqlite3_free(pRec); goto nomem; }
        memcpy(row.pNewVal, pRec, nRec);
        row.nNewVal = nRec;
      }
      sqlite3_free(pRec);
    }
  }
  if( pVtab->nCache>=pVtab->nCacheAlloc ){
    nNew = pVtab->nCacheAlloc ? pVtab->nCacheAlloc*2 : 16;
    aNew = sqlite3_realloc(pVtab->aCache,
                           nNew*(int)sizeof(WorkspaceRow));
    if( !aNew ) goto nomem;
    pVtab->aCache = aNew;
    pVtab->nCacheAlloc = nNew;
  }
  pVtab->aCache[pVtab->nCache] = row;
  pVtab->nCache++;
  return SQLITE_OK;

nomem:
  sqlite3_free(row.pKey);
  sqlite3_free(row.pOldVal);
  sqlite3_free(row.pNewVal);
  return SQLITE_NOMEM;
}

static void wsCloseIter(WorkspaceCursor *c){
  if( c->iterOpen ){
    prollyDiffIterClose(&c->iter);
    c->iterOpen = 0;
  }
}

static int wsInitCursorRoots(WorkspaceCursor *c, WorkspaceVtab *pVtab){
  sqlite3 *db = pVtab->db;
  ProllyHash headHash, headCat, stagedCat, workingCat;
  ProllyHash schemaHash;
  u8 headFlags = 0;
  int rc;

  memset(&headCat, 0, sizeof(headCat));
  memset(&stagedCat, 0, sizeof(stagedCat));
  memset(&workingCat, 0, sizeof(workingCat));
  memset(&c->headRoot, 0, sizeof(c->headRoot));
  memset(&c->stagedRoot, 0, sizeof(c->stagedRoot));
  memset(&c->workingRoot, 0, sizeof(c->workingRoot));
  memset(&schemaHash, 0, sizeof(schemaHash));
  c->stagedFlags = 0;
  c->workingFlags = 0;

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    c->eof = 1;
    return SQLITE_OK;
  }
  rc = doltliteCommitCatalogHash(db, &headHash, &headCat);
  if( rc!=SQLITE_OK ) return rc;

  doltliteGetSessionStaged(db, &stagedCat);
  if( c->stagedOnly==1 && prollyHashIsEmpty(&stagedCat) ){
    c->eof = 1;
    return SQLITE_OK;
  }
  if( prollyHashIsEmpty(&stagedCat) ) stagedCat = headCat;
  if( c->stagedOnly!=1 ){
    rc = doltliteFlushCatalogToHash(db, &workingCat);
    if( rc!=SQLITE_OK ) return rc;
  }

  rc = doltliteLoadTableRootByNameOrEmpty(db, &headCat, pVtab->zTableName,
                                          &c->headRoot, &headFlags,
                                          &schemaHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadTableRootByNameOrEmpty(db, &stagedCat, pVtab->zTableName,
                                          &c->stagedRoot, &c->stagedFlags,
                                          &schemaHash);
  if( rc!=SQLITE_OK ) return rc;
  if( c->stagedOnly!=1 ){
    rc = doltliteLoadTableRootByNameOrEmpty(db, &workingCat, pVtab->zTableName,
                                            &c->workingRoot, &c->workingFlags,
                                            &schemaHash);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( !c->stagedFlags ){
    c->stagedFlags = headFlags ? headFlags : c->workingFlags;
  }
  if( !c->workingFlags ){
    c->workingFlags = c->stagedFlags ? c->stagedFlags : headFlags;
  }
  return SQLITE_OK;
}

static int wsOpenNextIter(WorkspaceCursor *c, WorkspaceVtab *pVtab){
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  ProllyCache *pCache = doltliteGetCache(pVtab->db);
  const ProllyHash *pFromRoot;
  const ProllyHash *pToRoot;
  u8 flags;
  int staged;
  int rc;

  if( !cs || !pCache ){
    c->eof = 1;
    return SQLITE_OK;
  }
  while( c->phase<2 ){
    if( c->phase==0 ){
      if( c->stagedOnly==0 ){
        c->phase++;
        continue;
      }
      pFromRoot = &c->headRoot;
      pToRoot = &c->stagedRoot;
      flags = c->stagedFlags;
      staged = 1;
    }else{
      if( c->stagedOnly==1 ){
        c->phase++;
        continue;
      }
      pFromRoot = &c->stagedRoot;
      pToRoot = &c->workingRoot;
      flags = c->workingFlags;
      staged = 0;
    }
    c->phase++;
    if( prollyHashCompare(pFromRoot, pToRoot)==0 ) continue;
    rc = prollyDiffIterOpen(&c->iter, cs, pCache, pFromRoot, pToRoot, flags);
    if( rc!=SQLITE_OK ) return rc;
    c->iterOpen = 1;
    c->iterStaged = staged;
    c->iterFlags = flags;
    return SQLITE_OK;
  }
  c->eof = 1;
  return SQLITE_OK;
}

static int wsLoadNextRow(WorkspaceCursor *c, WorkspaceVtab *pVtab){
  ProllyDiffChange *pChange = 0;
  int rc;

  if( c->iRow < pVtab->nCache ) return SQLITE_OK;
  while( !c->eof ){
    if( !c->iterOpen ){
      rc = wsOpenNextIter(c, pVtab);
      if( rc!=SQLITE_OK || c->eof ) return rc;
    }
    rc = prollyDiffIterStep(&c->iter, &pChange);
    if( rc==SQLITE_ROW && pChange ){
      return wsAppendRow(pVtab, c->iterStaged, c->iterFlags, pChange);
    }
    if( rc!=SQLITE_DONE && rc!=SQLITE_ROW ) return rc;
    wsCloseIter(c);
  }
  return SQLITE_OK;
}

static int wsConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  (void)pAux;
  /* WorkspaceVtab has trailing aCache/nCache fields, but they start zeroed
  ** and only fill in during xFilter, so the shared Connect (which inits the
  ** common prefix and cleans up via the common disconnect on failure) is
  ** safe here; wsDisconnect still frees the cache at teardown. */
  return doltliteVtabConnectUserTable(db, argc, argv, "dolt_workspace_",
                                      sizeof(WorkspaceVtab), wsBuildSchema,
                                      ppVtab, pzErr);
}

static int wsDisconnect(sqlite3_vtab *pBase){
  WorkspaceVtab *p = (WorkspaceVtab*)pBase;
  wsClearCache(p);
  doltliteFreeColInfo(&p->cols);
  sqlite3_free(p->zTableName);
  sqlite3_free(p);
  return SQLITE_OK;
}

static int wsBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i;
  int iStagedEq = -1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->iColumn==1 && pC->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iStagedEq = i;
      break;
    }
  }

  if( iStagedEq>=0 ){
    pInfo->aConstraintUsage[iStagedEq].argvIndex = 1;
    pInfo->idxNum = WS_IDX_STAGED_EQ;
    pInfo->estimatedCost = 500.0;
    pInfo->estimatedRows = 100;
  }else{
    pInfo->idxNum = 0;
    pInfo->estimatedCost = 1000.0;
  }
  return SQLITE_OK;
}

static int wsOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  return doltliteVtabOpenCursor(pp, sizeof(WorkspaceCursor));
}

static int wsClose(sqlite3_vtab_cursor *cur){
  wsCloseIter((WorkspaceCursor*)cur);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int wsFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  WorkspaceVtab *p = (WorkspaceVtab*)cur->pVtab;
  int rc;
  (void)idxStr;
  wsCloseIter(c);
  wsClearCache(p);
  c->iRow = 0;
  c->eof = 0;
  c->phase = 0;
  c->iterStaged = 0;
  c->iterFlags = 0;
  c->stagedOnly = -1;
  if( idxNum & WS_IDX_STAGED_EQ ){
    if( argc<1 ) return SQLITE_OK;
    c->stagedOnly = sqlite3_value_int(argv[0]);
    if( c->stagedOnly!=0 && c->stagedOnly!=1 ){
      c->eof = 1;
      return SQLITE_OK;
    }
  }
  rc = wsInitCursorRoots(c, p);
  if( rc!=SQLITE_OK ) return rc;
  return wsLoadNextRow(c, p);
}

static int wsNext(sqlite3_vtab_cursor *cur){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  WorkspaceVtab *p = (WorkspaceVtab*)cur->pVtab;
  c->iRow++;
  return wsLoadNextRow(c, p);
}

static int wsEof(sqlite3_vtab_cursor *cur){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  return c->eof && c->iRow >= ((WorkspaceVtab*)cur->pVtab)->nCache;
}

static int wsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  WorkspaceVtab *p = (WorkspaceVtab*)cur->pVtab;
  WorkspaceRow *r = &p->aCache[c->iRow];
  int nCols = p->cols.nCol;
  if( col==0 ){
    sqlite3_result_int64(ctx, r->rowid);
  }else if( col==1 ){
    sqlite3_result_int(ctx, r->staged);
  }else if( col==2 ){
    const char *zType = prollyDiffTypeName(r->diffType);
    if( zType ) sqlite3_result_text(ctx, zType, -1, SQLITE_STATIC);
    else sqlite3_result_null(ctx);
  }else if( col>=3 && col<3+nCols ){
    doltliteResultUserCol(ctx, &p->cols, r->pNewVal, r->nNewVal,
                          r->intKey, col-3);
  }else if( col>=3+nCols && col<3+2*nCols ){
    doltliteResultUserCol(ctx, &p->cols, r->pOldVal, r->nOldVal,
                          r->intKey, col-3-nCols);
  }else{
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int wsRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  WorkspaceVtab *p = (WorkspaceVtab*)cur->pVtab;
  *pRowid = p->aCache[c->iRow].rowid;
  return SQLITE_OK;
}

static WorkspaceRow *wsFindCachedRow(WorkspaceVtab *p, i64 rowid){
  if( rowid>=1 && rowid<=p->nCache ){
    WorkspaceRow *r = &p->aCache[rowid - 1];
    if( r->rowid==rowid ) return r;
  }
  return 0;
}

/* Apply one row's old/new records to one secondary-index root. */
static int wsApplyRowToIndex(
  ChunkStore *cs, ProllyCache *pCache,
  struct TableEntry *idxEntry, Index *pIdx, int iPKey,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pSrc, int nSrc, const u8 *pTgt, int nTgt
){
  ProllyHash root = idxEntry->root;
  int rc = SQLITE_OK;
  if( pSrc ){
    u8 *pIK = 0; int nIK = 0;
    ProllyHash next;
    rc = doltliteBuildIndexSortKey(pSrc, nSrc, pIdx->aiColumn, pIdx->nKeyCol,
                                   0, iPKey, intKey, pKey, nKey, &pIK, &nIK);
    if( rc==SQLITE_OK ){
      rc = prollyMutateDelete(cs, pCache, &root, idxEntry->flags,
                              pIK, nIK, 0, &next);
      sqlite3_free(pIK);
      if( rc==SQLITE_OK ) root = next;
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pTgt ){
    u8 *pIK = 0; int nIK = 0;
    ProllyHash next;
    rc = doltliteBuildIndexSortKey(pTgt, nTgt, pIdx->aiColumn, pIdx->nKeyCol,
                                   0, iPKey, intKey, pKey, nKey, &pIK, &nIK);
    if( rc==SQLITE_OK ){
      rc = prollyMutateInsert(cs, pCache, &root, idxEntry->flags,
                              pIK, nIK, 0, 0, 0, &next);
      sqlite3_free(pIK);
      if( rc==SQLITE_OK ) root = next;
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  idxEntry->root = root;
  return SQLITE_OK;
}

static int wsApplyRowToStaged(WorkspaceVtab *p, WorkspaceRow *r, int makeStaged){
  sqlite3 *db;
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash headHash, headCat, stagedCat, newRoot, newCat;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  struct TableEntry *pData;
  Table *pTab;
  u8 *pCatBuf = 0;
  int nCatBuf = 0;
  int rc;
  const u8 *pHeadVal;
  int nHeadVal;
  const u8 *pWorkVal;
  int nWorkVal;
  const u8 *pSrc;
  int nSrc;
  const u8 *pTgt;
  int nTgt;

  assert( p!=0 && r!=0 );
  assert( p->zTableName!=0 );
  assert( makeStaged==0 || makeStaged==1 );
  assert( r->staged==0 || r->staged==1 );
  db = p->db;
  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);

  /* Staging rewrites data by PK and secondary indexes by old/new index key. */
  pHeadVal = (r->diffType==PROLLY_DIFF_ADD)    ? 0 : r->pOldVal;
  nHeadVal = (r->diffType==PROLLY_DIFF_ADD)    ? 0 : r->nOldVal;
  pWorkVal = (r->diffType==PROLLY_DIFF_DELETE) ? 0 : r->pNewVal;
  nWorkVal = (r->diffType==PROLLY_DIFF_DELETE) ? 0 : r->nNewVal;
  pSrc = makeStaged ? pHeadVal : pWorkVal;
  nSrc = makeStaged ? nHeadVal : nWorkVal;
  pTgt = makeStaged ? pWorkVal : pHeadVal;
  nTgt = makeStaged ? nWorkVal : nHeadVal;

  if( !cs || !pCache ) return SQLITE_ERROR;
  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_ERROR;
  rc = doltliteCommitCatalogHash(db, &headHash, &headCat);
  if( rc!=SQLITE_OK ) return rc;

  doltliteGetSessionStaged(db, &stagedCat);
  if( prollyHashIsEmpty(&stagedCat) ) stagedCat = headCat;

  /* Load the staged catalog once and patch the data root and every secondary
  ** index root in place, then reserialize as a single new catalog. */
  rc = doltliteLoadCatalog(db, &stagedCat, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pData = doltliteFindTableByName(aTables, nTables, p->zTableName);
  if( !pData ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_NOTFOUND;
  }

  if( pTgt ){
    rc = prollyMutateInsert(cs, pCache, &pData->root, pData->flags,
                            r->pKey, r->nKey, r->intKey, pTgt, nTgt, &newRoot);
  }else{
    rc = prollyMutateDelete(cs, pCache, &pData->root, pData->flags,
                            r->pKey, r->nKey, r->intKey, &newRoot);
  }
  if( rc!=SQLITE_OK ){ doltliteFreeCatalog(aTables, nTables); return rc; }
  pData->root = newRoot;

  pTab = sqlite3FindTable(db, p->zTableName, "main");
  if( pTab ){
    Index *pIdx;
    for(pIdx=pTab->pIndex; pIdx && rc==SQLITE_OK; pIdx=pIdx->pNext){
      struct TableEntry *idxEntry;
      /* For WITHOUT ROWID tables the PK is exposed as a pseudo-index whose
      ** root is the table tree itself; it is not a separate secondary index. */
      if( pIdx->idxType==SQLITE_IDXTYPE_PRIMARYKEY ) continue;
      idxEntry = doltliteFindTableByNumber(aTables, nTables, pIdx->tnum);
      if( !idxEntry ) continue;
      rc = wsApplyRowToIndex(cs, pCache, idxEntry, pIdx, pTab->iPKey,
                             r->pKey, r->nKey, r->intKey,
                             pSrc, nSrc, pTgt, nTgt);
    }
  }
  if( rc!=SQLITE_OK ){ doltliteFreeCatalog(aTables, nTables); return rc; }

  rc = doltliteSerializeCatalogEntries(db, aTables, nTables, &pCatBuf, &nCatBuf);
  if( rc==SQLITE_OK ) rc = chunkStorePut(cs, pCatBuf, nCatBuf, &newCat);
  sqlite3_free(pCatBuf);
  doltliteFreeCatalog(aTables, nTables);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionStaged(db, &newCat);
  return rc;
}


static int wsUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                    sqlite3_int64 *pRowid){
  WorkspaceVtab *p = (WorkspaceVtab*)pBase;
  WorkspaceRow *r;
  int newStaged;
  assert( pBase!=0 && argv!=0 );
  assert( p->db!=0 && p->zTableName!=0 );
  (void)pRowid;
  if( argc==1 ){
    const u8 *pVal;
    int nVal;
    r = wsFindCachedRow(p, sqlite3_value_int64(argv[0]));
    if( !r ){
      pBase->zErrMsg = sqlite3_mprintf("workspace row is no longer available");
      return SQLITE_ABORT;
    }
    if( r->staged ){
      pBase->zErrMsg = sqlite3_mprintf(
          "cannot delete staged rows from workspace");
      return SQLITE_ERROR;
    }
    /* Discard unstaged working edit: restore staged/HEAD side for this PK. */
    if( r->diffType==PROLLY_DIFF_ADD ){
      pVal = 0;
      nVal = 0;
    }else{
      pVal = r->pOldVal;
      nVal = r->nOldVal;
    }
    return doltliteApplyRawRowMutation(p->db, p->zTableName,
                                       r->pKey, r->nKey, r->intKey,
                                       pVal, nVal);
  }
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    pBase->zErrMsg = sqlite3_mprintf(
        "INSERT into dolt_workspace_%s is not supported", p->zTableName);
    return SQLITE_CONSTRAINT;
  }
  if( argc < 2 + 3 + p->cols.nCol*2 ) return SQLITE_MISUSE;
  r = wsFindCachedRow(p, sqlite3_value_int64(argv[0]));
  if( !r ){
    pBase->zErrMsg = sqlite3_mprintf("workspace row is no longer available");
    return SQLITE_ABORT;
  }
  newStaged = sqlite3_value_int(argv[2 + 1]) ? 1 : 0;
  if( newStaged==r->staged ) return SQLITE_OK;
  return wsApplyRowToStaged(p, r, newStaged);
}

static sqlite3_module workspaceModule = {
  0, wsConnect, wsConnect, wsBestIndex, wsDisconnect, wsDisconnect,
  wsOpen, wsClose, wsFilter, wsNext, wsEof, wsColumn, wsRowid,
  wsUpdate,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteRegisterWorkspaceTables(sqlite3 *db){
  return doltliteForEachUserTable(db, "dolt_workspace_", &workspaceModule);
}

#endif
