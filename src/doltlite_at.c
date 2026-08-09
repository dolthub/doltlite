
#ifdef DOLTLITE_PROLLY

#include "doltlite_vtab_util.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"

#include <string.h>
#include <time.h>

#define AT_IDX_REF    0x01
#define AT_IDX_PK_EQ  0x02
#define AT_IDX_PK_GE  0x04
#define AT_IDX_PK_LE  0x08
#define AT_IDX_PK_GT  0x10
#define AT_IDX_PK_LT  0x20
#define AT_IDX_PK_ANY \
  (AT_IDX_PK_EQ|AT_IDX_PK_GE|AT_IDX_PK_LE|AT_IDX_PK_GT|AT_IDX_PK_LT)

static char *atBuildSchema(const DoltliteColInfo *ci){
  sqlite3_str *pStr = sqlite3_str_new(0);
  char *z;
  if( !pStr ) return 0;
  sqlite3_str_appendall(pStr, "CREATE TABLE x(");
  if( doltliteAppendQuotedColumnList(pStr, ci->azName, ci->nCol, 0, 0)!=SQLITE_OK ){
    sqlite3_str_reset(pStr);
    return 0;
  }
  sqlite3_str_appendall(pStr, ", commit_ref TEXT HIDDEN)");
  z = sqlite3_str_finish(pStr);
  return z;
}

typedef struct AtVtab AtVtab;
struct AtVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
  DoltliteColInfo cols;
};

typedef struct AtCursor AtCursor;
struct AtCursor {
  DoltliteVtabCursorCommon common;
  char *zCommitRef;
  int idxNum;
  DoltlitePkRange pkRange;
};

typedef struct AtSeenTable AtSeenTable;
struct AtSeenTable {
  char **azName;
  int nName;
  int nAlloc;
};

static void atSeenTableClear(AtSeenTable *pSeen){
  int i;
  for(i=0; i<pSeen->nName; i++) sqlite3_free(pSeen->azName[i]);
  sqlite3_free(pSeen->azName);
  memset(pSeen, 0, sizeof(*pSeen));
}

static int atSeenTableHas(AtSeenTable *pSeen, const char *zName){
  int i;
  for(i=0; i<pSeen->nName; i++){
    if( strcmp(pSeen->azName[i], zName)==0 ) return 1;
  }
  return 0;
}

static int atSeenTableAdd(AtSeenTable *pSeen, const char *zName){
  char *zCopy;
  if( atSeenTableHas(pSeen, zName) ) return SQLITE_OK;
  if( pSeen->nName>=pSeen->nAlloc ){
    int nNew = pSeen->nAlloc ? pSeen->nAlloc*2 : 16;
    char **azNew;
    if( nNew > 0x7fffffff/(int)sizeof(char*) ) return SQLITE_NOMEM;
    azNew = sqlite3_realloc(pSeen->azName, nNew*(int)sizeof(char*));
    if( !azNew ) return SQLITE_NOMEM;
    pSeen->azName = azNew;
    pSeen->nAlloc = nNew;
  }
  zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  pSeen->azName[pSeen->nName++] = zCopy;
  return SQLITE_OK;
}

static int atEnqueueReachableRoots(
  sqlite3 *db,
  DoltliteCommitQueue *pQueue
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const BranchRef *aBr = 0;
  const TagRef *aTag = 0;
  const TrackingBranch *aTracking = 0;
  int nBr = 0, nTag = 0, nTracking = 0;
  int i, rc = SQLITE_OK;
  ProllyHash head;

  if( !cs ) return SQLITE_OK;
  memset(&head, 0, sizeof(head));
  doltliteGetSessionHead(db, &head);
  rc = doltliteCommitQueueEnqueue(pQueue, &head);
  refsTableGetBranches(&cs->refs, &nBr, &aBr);
  for(i=0; i<nBr && rc==SQLITE_OK; i++){
    rc = doltliteCommitQueueEnqueue(pQueue, &aBr[i].commitHash);
  }
  refsTableGetTags(&cs->refs, &nTag, &aTag);
  for(i=0; i<nTag && rc==SQLITE_OK; i++){
    rc = doltliteCommitQueueEnqueue(pQueue, &aTag[i].commitHash);
  }
  refsTableGetTracking(&cs->refs, &nTracking, &aTracking);
  for(i=0; i<nTracking && rc==SQLITE_OK; i++){
    rc = doltliteCommitQueueEnqueue(pQueue, &aTracking[i].commitHash);
  }
  return rc;
}

int doltliteLoadHistoricalTableColumns(
  sqlite3 *db,
  const char *zTableName,
  DoltliteColInfo *pCols,
  char **pzErr
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  int has, rc;
  DoltliteCommitQueue q;
  ProllyHash cur;

  memset(pCols, 0, sizeof(*pCols));
  pCols->iPkCol = -1;
  if( sqlite3FindTable(db, zTableName, "main") ){
    rc = doltliteGetColumnNames(db, zTableName, pCols);
    if( rc!=SQLITE_OK ) return rc;
    if( pCols->nCol>0 ) return SQLITE_OK;
    doltliteFreeColInfo(pCols);
  }

  memset(&q, 0, sizeof(q));
  memset(&cur, 0, sizeof(cur));
  rc = doltliteCommitQueueInit(&q, &cur);
  if( rc==SQLITE_OK ) rc = atEnqueueReachableRoots(db, &q);
  while( rc==SQLITE_OK && pCols->nCol<=0 ){
    DoltliteCommit commit;
    SchemaEntry entry;
    int found = 0;
    sqlite3 *tmp = 0;
    memset(&commit, 0, sizeof(commit));
    memset(&entry, 0, sizeof(entry));
    rc = doltliteCommitQueueNext(&q, &cur, &has);
    if( rc!=SQLITE_OK || !has ) break;
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc==SQLITE_OK ) rc = doltliteCommitQueueEnqueueParents(&q, &commit);
    if( rc==SQLITE_OK ){
      rc = loadSchemaEntryFromCatalog(db, cs, pCache, &commit.catalogHash,
                                      zTableName, &entry, &found);
    }
    if( rc==SQLITE_OK && found && entry.zSql ){
      rc = sqlite3_open(":memory:", &tmp);
      if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, entry.zSql, 0, 0, 0);
      if( rc==SQLITE_OK ) rc = doltliteGetColumnNames(tmp, zTableName, pCols);
      if( rc==SQLITE_OK && pCols->nCol<=0 ){
        doltliteFreeColInfo(pCols);
        rc = SQLITE_NOTFOUND;
      }
    }
    if( tmp ) sqlite3_close(tmp);
    clearSchemaEntry(&entry);
    doltliteCommitClear(&commit);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
  }
  doltliteCommitQueueClear(&q);

  if( rc==SQLITE_OK && pCols->nCol<=0 ){
    if( pzErr ){
      *pzErr = sqlite3_mprintf("table not found in reachable refs: %s",
                               zTableName);
      if( !*pzErr ) return SQLITE_NOMEM;
    }
    return SQLITE_ERROR;
  }
  return rc;
}

static void atCursorReset(AtCursor *c){
  doltliteVtabCommonReset(&c->common);
  sqlite3_free(c->zCommitRef);
  c->zCommitRef = 0;
}

static int atRowMatchesUpper(AtCursor *c){
  i64 k;
  if( !c->pkRange.hasPkHi ) return 1;
  k = prollyCursorIntKey(&c->common.tblCur);
  if( c->pkRange.pkHiStrict ){
    return k < c->pkRange.pkHi;
  }
  return k <= c->pkRange.pkHi;
}

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DoltliteVtabCommon *v;
  const char *zMod = argv[0];
  size_t nPrefix = strlen("dolt_at_");
  char *zSchema;
  int rc;
  (void)pAux;

  v = sqlite3_malloc(sizeof(AtVtab));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(AtVtab));
  v->db = db;

  if( zMod && strncmp(zMod, "dolt_at_", nPrefix)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zMod + nPrefix);
  }else if( argc > 3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }
  if( !v->zTableName ){
    doltliteVtabCommonDisconnect(&v->base);
    return SQLITE_NOMEM;
  }

  rc = doltliteLoadHistoricalTableColumns(db, v->zTableName,
                                           &v->cols, pzErr);
  if( rc==SQLITE_OK ){
    zSchema = atBuildSchema(&v->cols);
    if( !zSchema ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_declare_vtab(db, zSchema);
      sqlite3_free(zSchema);
    }
  }
  if( rc!=SQLITE_OK ){
    doltliteVtabCommonDisconnect(&v->base);
    return rc;
  }
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int atOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  (void)pVtab;
  return doltliteVtabOpenCursor(pp, sizeof(AtCursor));
}

static int atClose(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  atCursorReset(c); sqlite3_free(c); return SQLITE_OK;
}

static int atBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)pVtab;
  int nCols=v->cols.nCol;
  int iRef=-1, iEq=-1, iGe=-1, iLe=-1, iGt=-1, iLt=-1;
  int i, argvIdx=1, idxNum=0;
  int iPkCol = v->cols.iPkCol;

  int refCol = nCols > 0 ? nCols : 2;
  (void)pVtab;

  for(i=0;i<pInfo->nConstraint;i++){
    if(!pInfo->aConstraint[i].usable) continue;
    if( pInfo->aConstraint[i].iColumn==refCol
     && pInfo->aConstraint[i].op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iRef=i;
    }else if( iPkCol>=0 && pInfo->aConstraint[i].iColumn==iPkCol ){
      switch( pInfo->aConstraint[i].op ){
        case SQLITE_INDEX_CONSTRAINT_EQ: if( iEq<0 ) iEq=i; break;
        case SQLITE_INDEX_CONSTRAINT_GE: if( iGe<0 ) iGe=i; break;
        case SQLITE_INDEX_CONSTRAINT_LE: if( iLe<0 ) iLe=i; break;
        case SQLITE_INDEX_CONSTRAINT_GT: if( iGt<0 ) iGt=i; break;
        case SQLITE_INDEX_CONSTRAINT_LT: if( iLt<0 ) iLt=i; break;
        default: break;
      }
    }
  }

  if(iRef>=0){
    pInfo->aConstraintUsage[iRef].argvIndex=argvIdx++;
    pInfo->aConstraintUsage[iRef].omit=1;
    idxNum |= AT_IDX_REF;
    /* Historical roots may not share the declared schema's key shape, so
    ** PK constraints are never omitted: the values still push down for the
    ** seek fast path, and SQLite re-checks them against the rendered row. */
    if( iEq>=0 ){
      pInfo->aConstraintUsage[iEq].argvIndex=argvIdx++;
      idxNum |= AT_IDX_PK_EQ;
    }else{
      if( iGe>=0 ){
        pInfo->aConstraintUsage[iGe].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_GE;
      }
      if( iGt>=0 ){
        pInfo->aConstraintUsage[iGt].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_GT;
      }
      if( iLe>=0 ){
        pInfo->aConstraintUsage[iLe].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_LE;
      }
      if( iLt>=0 ){
        pInfo->aConstraintUsage[iLt].argvIndex=argvIdx++;
        idxNum |= AT_IDX_PK_LT;
      }
    }
    pInfo->idxNum=idxNum;
    pInfo->estimatedCost=1000.0;
    pInfo->estimatedRows=1000;
    if( idxNum & AT_IDX_PK_EQ ){
      pInfo->estimatedCost=10.0;
      pInfo->estimatedRows=1;
    }else if( idxNum & AT_IDX_PK_ANY ){
      pInfo->estimatedCost=100.0;
      pInfo->estimatedRows=100;
    }
  }else{
    pInfo->idxNum=0;
    pInfo->estimatedCost=1e12;
  }
  return SQLITE_OK;
}

static int atFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  AtCursor *c=(AtCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  sqlite3 *db=v->db;
  ChunkStore *cs=doltliteGetChunkStore(db);
  void *pBt; ProllyCache *pCache;
  const char *zRef;
  ProllyHash catHash;
  ProllyHash tableRoot; u8 flags=0;
  int rc, res;
  int seekable;
  (void)idxStr;

  atCursorReset(c);
  c->idxNum = idxNum;
  if(!cs||(idxNum & AT_IDX_REF)==0||argc<1) return SQLITE_OK;
  doltlitePkRangeFromArgs(idxNum,
      AT_IDX_PK_EQ, AT_IDX_PK_GE, AT_IDX_PK_LE,
      AT_IDX_PK_GT, AT_IDX_PK_LT,
      argc-1, argv+1, &c->pkRange);
  if( c->pkRange.isEmpty ) return SQLITE_OK;

  pBt=doltliteGetBtShared(db);
  if(!pBt) return SQLITE_OK;
  pCache=doltliteGetCache(db);

  zRef=(const char*)sqlite3_value_text(argv[0]);
  if(!zRef) return SQLITE_OK;
  c->zCommitRef = sqlite3_mprintf("%s", zRef);
  if( !c->zCommitRef ) return SQLITE_NOMEM;

  rc=doltliteRefToCatalogHash(db,zRef,&catHash);
  if(rc==SQLITE_NOTFOUND){
    sqlite3_free(cur->pVtab->zErrMsg);
    cur->pVtab->zErrMsg = sqlite3_mprintf("ref not found: %s", zRef);
    return SQLITE_ERROR;
  }
  if(rc!=SQLITE_OK) return rc;

  {
    ProllyHash branchCommit;
    ProllyHash effCatHash;
    int isBranch = (chunkStoreFindBranch(cs,zRef,&branchCommit)==SQLITE_OK
                    && !prollyHashIsEmpty(&branchCommit));
    if( isBranch ){
      doltliteResolveBranchEffectiveCatalog(cs, zRef, &branchCommit,
                                            &catHash, &effCatHash);
    }else{
      memcpy(&effCatHash, &catHash, sizeof(ProllyHash));
    }
    rc=doltliteLoadTableRootByName(db,&effCatHash,v->zTableName,&tableRoot,
                                   &flags,0);
  }
  if(rc==SQLITE_NOTFOUND) return SQLITE_OK;
  if(rc!=SQLITE_OK) return rc;

  if( prollyHashIsEmpty(&tableRoot) ) return SQLITE_OK;

  prollyCursorInit(&c->common.tblCur, cs, pCache, &tableRoot, flags);
  c->common.rootIntKey = (flags & PROLLY_NODE_INTKEY) != 0;

  seekable = c->common.rootIntKey
          && (idxNum & AT_IDX_PK_ANY) != 0;

  if( seekable && (idxNum & AT_IDX_PK_EQ) ){
    rc = prollyCursorSeekInt(&c->common.tblCur, c->pkRange.pkLo, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res!=0 || !prollyCursorIsValid(&c->common.tblCur) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
  }

  if( seekable && c->pkRange.hasPkLo ){
    i64 startKey = c->pkRange.pkLo;
    if( c->pkRange.pkLoStrict ) startKey++;
    rc = prollyCursorSeekInt(&c->common.tblCur, startKey, &res);
    if( rc!=SQLITE_OK ){
      prollyCursorClose(&c->common.tblCur);
      return rc;
    }
    if( res<0 ){
      rc = prollyCursorNext(&c->common.tblCur);
      if( rc!=SQLITE_OK ){
        prollyCursorClose(&c->common.tblCur);
        return rc;
      }
    }
    if( !prollyCursorIsValid(&c->common.tblCur) || !atRowMatchesUpper(c) ){
      prollyCursorClose(&c->common.tblCur);
      return SQLITE_OK;
    }
    c->common.tblCurOpen = 1;
    return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
  }

  rc = prollyCursorFirst(&c->common.tblCur, &res);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->common.tblCur);
    return rc;
  }
  if( res ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  if( seekable && c->pkRange.hasPkHi && !atRowMatchesUpper(c) ){
    prollyCursorClose(&c->common.tblCur);
    return SQLITE_OK;
  }
  c->common.tblCurOpen = 1;
  return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
}

static int atNext(sqlite3_vtab_cursor *cur){
  AtCursor *c=(AtCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  int rc;
  c->common.iRowid++;
  if( !c->common.tblCurOpen ){
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  /* The EQ probe positioned the cursor only on an intkey root; a
  ** shape-mismatched root is scanning and must keep stepping. */
  if( (c->idxNum & AT_IDX_PK_EQ) && c->common.rootIntKey ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  rc = prollyCursorNext(&c->common.tblCur);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return rc;
  }
  if( !prollyCursorIsValid(&c->common.tblCur) ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  if( (c->idxNum & AT_IDX_PK_ANY) && c->common.rootIntKey
   && !atRowMatchesUpper(c) ){
    prollyCursorClose(&c->common.tblCur);
    c->common.tblCurOpen = 0;
    c->common.hasRow = 0;
    return SQLITE_OK;
  }
  return doltliteVtabCommonCaptureRow(&c->common, v->db, v->zTableName);
}

static int atColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  AtCursor *c=(AtCursor*)cur;
  DoltliteVtabCommon *v=(DoltliteVtabCommon*)cur->pVtab;
  int nCols=v->cols.nCol;

  if( !c->common.hasRow ) return SQLITE_OK;

  if( col==nCols ){
    sqlite3_result_text(ctx, c->zCommitRef ? c->zCommitRef : "",
                        -1, SQLITE_TRANSIENT);
  }else if(nCols>0 && col<nCols){
    doltliteResultUserCol(ctx, &v->cols, c->common.pVal, c->common.nVal,
                          c->common.intKey, c->common.rootIntKey, col);
  }

  return SQLITE_OK;
}

static sqlite3_module atModule = {
  0, atConnect, atConnect, atBestIndex,
  doltliteVtabCommonDisconnect, doltliteVtabCommonDisconnect,
  atOpen, atClose,
  atFilter, atNext,
  doltliteVtabCommonEof, atColumn, doltliteVtabCommonRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

static int atRegisterModule(
  sqlite3 *db,
  const char *zPrefix,
  const char *zName,
  const sqlite3_module *pModule
){
  char *zMod;
  int rc;
  zMod = sqlite3_mprintf("%s%s", zPrefix, zName);
  if( !zMod ) return SQLITE_NOMEM;
  rc = sqlite3_create_module(db, zMod, pModule, 0);
  sqlite3_free(zMod);
  return rc;
}

static int atRegisterOne(sqlite3 *db, AtSeenTable *pSeen, const char *zName){
  int rc;
  if( !zName || atSeenTableHas(pSeen, zName) ) return SQLITE_OK;
  rc = atRegisterModule(db, "dolt_at_", zName, &atModule);
  if( rc==SQLITE_OK ){
    rc = atRegisterModule(db, "dolt_diff_", zName,
                          doltliteDiffTableModule());
  }
  if( rc==SQLITE_OK ){
    rc = atRegisterModule(db, "dolt_history_", zName,
                          doltliteHistoryTableModule());
  }
  if( rc!=SQLITE_OK ) return rc;
  return atSeenTableAdd(pSeen, zName);
}

static int atRegisterCatalogTables(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  AtSeenTable *pSeen
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int i, rc;
  if( prollyHashIsEmpty(pCatHash) ) return SQLITE_OK;
  rc = doltliteLoadCatalog(db, pCatHash, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<nTables && rc==SQLITE_OK; i++){
    if( aTables[i].zName && aTables[i].iTable > 1 ){
      rc = atRegisterOne(db, pSeen, aTables[i].zName);
    }
  }
  doltliteFreeCatalog(aTables, nTables);
  return rc;
}

int doltliteRegisterHistoricalTablesForCatalog(
  sqlite3 *db,
  const ProllyHash *pCatHash
){
  AtSeenTable seen;
  int rc;
  memset(&seen, 0, sizeof(seen));
  rc = atRegisterCatalogTables(db, pCatHash, &seen);
  atSeenTableClear(&seen);
  return rc;
}

int doltliteRegisterHistoricalTables(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int has, rc;
  DoltliteCommitQueue q;
  ProllyHash cur;
  AtSeenTable seen;

  memset(&q, 0, sizeof(q));
  memset(&cur, 0, sizeof(cur));
  memset(&seen, 0, sizeof(seen));

  if( !cs ) return SQLITE_OK;

  rc = doltliteCommitQueueInit(&q, &cur);
  if( rc!=SQLITE_OK ) return rc;
  rc = atEnqueueReachableRoots(db, &q);

  while( rc==SQLITE_OK ){
    DoltliteCommit commit;
    rc = doltliteCommitQueueNext(&q, &cur, &has);
    if( rc!=SQLITE_OK || !has ) break;
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ) break;
    rc = atRegisterCatalogTables(db, &commit.catalogHash, &seen);
    if( rc==SQLITE_OK ){
      rc = doltliteCommitQueueEnqueueParents(&q, &commit);
    }
    doltliteCommitClear(&commit);
  }

  doltliteCommitQueueClear(&q);
  atSeenTableClear(&seen);
  return rc;
}

#endif
