
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "prolly_diff.h"
#include "prolly_mutate.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>

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
};

typedef struct WorkspaceCursor WorkspaceCursor;
struct WorkspaceCursor {
  sqlite3_vtab_cursor base;
  int iRow;
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
}

static char *wsBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  int i;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(id INTEGER, staged INTEGER, diff_type TEXT");
  for(i=0; i<ci->nCol; i++){
    char *z = sqlite3_mprintf("to_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !z ){ sqlite3_str_reset(pStr); return 0; }
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedIdent(pStr, z)!=SQLITE_OK ){
      sqlite3_free(z);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(z);
  }
  for(i=0; i<ci->nCol; i++){
    char *z = sqlite3_mprintf("from_%s", ci->azName[i] ? ci->azName[i] : "");
    if( !z ){ sqlite3_str_reset(pStr); return 0; }
    sqlite3_str_appendall(pStr, ", ");
    if( doltliteAppendQuotedIdent(pStr, z)!=SQLITE_OK ){
      sqlite3_free(z);
      sqlite3_str_reset(pStr);
      return 0;
    }
    sqlite3_free(z);
  }
  sqlite3_str_appendall(pStr, ")");
  z = sqlite3_str_finish(pStr);
  return z;
}

static int wsLoadTableRoot(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  ProllyHash *pRoot,
  u8 *pFlags,
  ProllyHash *pSchemaHash
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  struct TableEntry *pEntry;
  int rc;

  memset(pRoot, 0, sizeof(*pRoot));
  if( pFlags ) *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(*pSchemaHash));
  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = doltliteFindTableByName(aTables, nTables, zTableName);
  if( pEntry ){
    memcpy(pRoot, &pEntry->root, sizeof(*pRoot));
    if( pFlags ) *pFlags = pEntry->flags;
    if( pSchemaHash ) memcpy(pSchemaHash, &pEntry->schemaHash, sizeof(*pSchemaHash));
  }
  doltliteFreeCatalog(aTables, nTables);
  return SQLITE_OK;
}

static int wsAppendRow(
  WorkspaceVtab *pVtab,
  int staged,
  u8 flags,
  const ProllyDiffChange *pChange
){
  WorkspaceRow *aNew;
  WorkspaceRow *r;
  aNew = sqlite3_realloc(pVtab->aCache,
                         (pVtab->nCache+1)*(int)sizeof(WorkspaceRow));
  if( !aNew ) return SQLITE_NOMEM;
  pVtab->aCache = aNew;
  r = &pVtab->aCache[pVtab->nCache];
  memset(r, 0, sizeof(*r));
  r->rowid = pVtab->nCache + 1;
  r->staged = staged;
  r->diffType = pChange->type;
  r->flags = flags;
  r->intKey = pChange->intKey;
  if( pChange->nKey>0 ){
    r->pKey = sqlite3_malloc(pChange->nKey);
    if( !r->pKey ) return SQLITE_NOMEM;
    memcpy(r->pKey, pChange->pKey, pChange->nKey);
    r->nKey = pChange->nKey;
  }
  if( pChange->nOldVal>0 ){
    r->pOldVal = sqlite3_malloc(pChange->nOldVal);
    if( !r->pOldVal ) return SQLITE_NOMEM;
    memcpy(r->pOldVal, pChange->pOldVal, pChange->nOldVal);
    r->nOldVal = pChange->nOldVal;
  }
  if( pChange->nNewVal>0 ){
    r->pNewVal = sqlite3_malloc(pChange->nNewVal);
    if( !r->pNewVal ) return SQLITE_NOMEM;
    memcpy(r->pNewVal, pChange->pNewVal, pChange->nNewVal);
    r->nNewVal = pChange->nNewVal;
  }
  pVtab->nCache++;
  return SQLITE_OK;
}

static int wsDiffPair(
  WorkspaceVtab *pVtab,
  const ProllyHash *pFromRoot,
  const ProllyHash *pToRoot,
  u8 flags,
  int staged
){
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  ProllyCache *pCache = doltliteGetCache(pVtab->db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  int rc;
  if( !cs || !pCache ) return SQLITE_OK;
  if( prollyHashCompare(pFromRoot, pToRoot)==0 ) return SQLITE_OK;
  rc = prollyDiffIterOpen(&iter, cs, pCache, pFromRoot, pToRoot, flags);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW && pChange ){
    rc = wsAppendRow(pVtab, staged, flags, pChange);
    if( rc!=SQLITE_OK ) break;
  }
  prollyDiffIterClose(&iter);
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  return rc;
}

static int wsLoadRows(WorkspaceVtab *pVtab){
  sqlite3 *db = pVtab->db;
  ProllyHash headHash, headCat, stagedCat, workingCat;
  ProllyHash headRoot, stagedRoot, workingRoot;
  ProllyHash schemaHash;
  DoltliteCommit headCommit;
  u8 headFlags = 0, stagedFlags = 0, workingFlags = 0;
  int rc;

  wsClearCache(pVtab);
  memset(&headCommit, 0, sizeof(headCommit));
  memset(&headCat, 0, sizeof(headCat));
  memset(&stagedCat, 0, sizeof(stagedCat));
  memset(&workingCat, 0, sizeof(workingCat));

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;
  rc = doltliteLoadCommit(db, &headHash, &headCommit);
  if( rc!=SQLITE_OK ) return rc;
  headCat = headCommit.catalogHash;
  doltliteCommitClear(&headCommit);

  doltliteGetSessionStaged(db, &stagedCat);
  if( prollyHashIsEmpty(&stagedCat) ) stagedCat = headCat;
  rc = doltliteFlushCatalogToHash(db, &workingCat);
  if( rc!=SQLITE_OK ) return rc;

  rc = wsLoadTableRoot(db, &headCat, pVtab->zTableName,
                       &headRoot, &headFlags, &schemaHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = wsLoadTableRoot(db, &stagedCat, pVtab->zTableName,
                       &stagedRoot, &stagedFlags, &schemaHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = wsLoadTableRoot(db, &workingCat, pVtab->zTableName,
                       &workingRoot, &workingFlags, &schemaHash);
  if( rc!=SQLITE_OK ) return rc;

  if( !stagedFlags ) stagedFlags = headFlags ? headFlags : workingFlags;
  if( !workingFlags ) workingFlags = stagedFlags ? stagedFlags : headFlags;
  rc = wsDiffPair(pVtab, &headRoot, &stagedRoot, stagedFlags, 1);
  if( rc==SQLITE_OK ){
    rc = wsDiffPair(pVtab, &stagedRoot, &workingRoot, workingFlags, 0);
  }
  return rc;
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
  (void)pVtab;
  pInfo->estimatedCost = 1000.0;
  return SQLITE_OK;
}

static int wsOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  return doltliteVtabOpenCursor(pp, sizeof(WorkspaceCursor));
}

static int wsClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int wsFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  WorkspaceVtab *p = (WorkspaceVtab*)cur->pVtab;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  c->iRow = 0;
  return wsLoadRows(p);
}

static int wsNext(sqlite3_vtab_cursor *cur){
  ((WorkspaceCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int wsEof(sqlite3_vtab_cursor *cur){
  WorkspaceCursor *c = (WorkspaceCursor*)cur;
  WorkspaceVtab *p = (WorkspaceVtab*)cur->pVtab;
  return c->iRow >= p->nCache;
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
    switch( r->diffType ){
      case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
      case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
      case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
      default: sqlite3_result_null(ctx); break;
    }
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
  int i;
  for(i=0; i<p->nCache; i++){
    if( p->aCache[i].rowid==rowid ) return &p->aCache[i];
  }
  return 0;
}

static int wsPatchCatalogRoot(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  const ProllyHash *pNewRoot,
  ProllyHash *pNewCatHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  struct TableEntry *aTables = 0;
  int nTables = 0;
  struct TableEntry *pEntry;
  u8 *pData = 0;
  int nData = 0;
  int rc;
  if( !cs ) return SQLITE_ERROR;
  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  pEntry = doltliteFindTableByName(aTables, nTables, zTableName);
  if( !pEntry ){
    doltliteFreeCatalog(aTables, nTables);
    return SQLITE_NOTFOUND;
  }
  pEntry->root = *pNewRoot;
  rc = doltliteSerializeCatalogEntries(db, aTables, nTables, &pData, &nData);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(cs, pData, nData, pNewCatHash);
  }
  sqlite3_free(pData);
  doltliteFreeCatalog(aTables, nTables);
  return rc;
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
  sqlite3 *db = p->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyHash headHash, headCat, stagedCat, newRoot, newCat;
  DoltliteCommit headCommit;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  struct TableEntry *pData;
  Table *pTab;
  u8 *pCatBuf = 0;
  int nCatBuf = 0;
  int rc;

  /* Staging rewrites data by PK and secondary indexes by old/new index key. */
  const u8 *pHeadVal = (r->diffType==PROLLY_DIFF_ADD)    ? 0 : r->pOldVal;
  int        nHeadVal = (r->diffType==PROLLY_DIFF_ADD)    ? 0 : r->nOldVal;
  const u8 *pWorkVal = (r->diffType==PROLLY_DIFF_DELETE) ? 0 : r->pNewVal;
  int        nWorkVal = (r->diffType==PROLLY_DIFF_DELETE) ? 0 : r->nNewVal;
  const u8 *pSrc = makeStaged ? pHeadVal : pWorkVal;
  int        nSrc = makeStaged ? nHeadVal : nWorkVal;
  const u8 *pTgt = makeStaged ? pWorkVal : pHeadVal;
  int        nTgt = makeStaged ? nWorkVal : nHeadVal;

  if( !cs || !pCache ) return SQLITE_ERROR;
  memset(&headCommit, 0, sizeof(headCommit));
  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_ERROR;
  rc = doltliteLoadCommit(db, &headHash, &headCommit);
  if( rc!=SQLITE_OK ) return rc;
  headCat = headCommit.catalogHash;
  doltliteCommitClear(&headCommit);

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
  if( rc==SQLITE_OK ) doltliteSetSessionStaged(db, &newCat);
  return rc;
}


static int wsUpdate(sqlite3_vtab *pBase, int argc, sqlite3_value **argv,
                    sqlite3_int64 *pRowid){
  WorkspaceVtab *p = (WorkspaceVtab*)pBase;
  WorkspaceRow *r;
  int newStaged;
  (void)pRowid;
  if( argc==1 ){
    pBase->zErrMsg = sqlite3_mprintf(
        "DELETE from dolt_workspace_%s is not yet supported", p->zTableName);
    return SQLITE_CONSTRAINT;
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
