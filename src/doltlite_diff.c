
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "doltlite_record.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

static int schemaRecordIsViewOrTrigger(const u8 *pRec, int nRec){
  DoltliteRecordInfo ri;
  int st, off, len;
  const u8 *pBody;
  if( !pRec || nRec<=0 ) return 0;
  doltliteParseRecord(pRec, nRec, &ri);
  if( ri.nField < 1 ) return 0;
  st = ri.aType[0];
  off = ri.aOffset[0];
  if( st < 13 || (st & 1)==0 ) return 0;
  len = (st - 13) / 2;
  if( off < 0 || off + len > nRec ) return 0;
  pBody = pRec + off;
  if( len==4 && memcmp(pBody, "view", 4)==0 ) return 1;
  if( len==7 && memcmp(pBody, "trigger", 7)==0 ) return 1;
  return 0;
}

static int schemaHasViewOrTriggerDiff(sqlite3 *db,
                                      const ProllyHash *pOldRoot,
                                      const ProllyHash *pNewRoot,
                                      u8 flags){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyDiffIter iter;
  ProllyDiffChange *pChange = 0;
  int rc;
  int found = 0;
  if( !cs || !pCache ) return 0;
  if( prollyHashCompare(pOldRoot, pNewRoot)==0 ) return 0;
  rc = prollyDiffIterOpen(&iter, cs, pCache, pOldRoot, pNewRoot, flags);
  if( rc!=SQLITE_OK ) return 0;
  while( (rc = prollyDiffIterStep(&iter, &pChange))==SQLITE_ROW && pChange ){
    if( schemaRecordIsViewOrTrigger(pChange->pNewVal, pChange->nNewVal)
     || schemaRecordIsViewOrTrigger(pChange->pOldVal, pChange->nOldVal) ){
      found = 1;
      break;
    }
  }
  prollyDiffIterClose(&iter);
  return found;
}

static int schemaHasAnyViewOrTrigger(sqlite3 *db,
                                     const ProllyHash *pRoot,
                                     u8 flags){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyCache *pCache = doltliteGetCache(db);
  ProllyCursor cur;
  int rc, res;
  int found = 0;
  if( !cs || !pCache ) return 0;
  if( prollyHashIsEmpty(pRoot) ) return 0;
  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return 0;
  }
  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    prollyCursorValue(&cur, &pVal, &nVal);
    if( schemaRecordIsViewOrTrigger(pVal, nVal) ){
      found = 1;
      break;
    }
    if( prollyCursorNext(&cur)!=SQLITE_OK ) break;
  }
  prollyCursorClose(&cur);
  return found;
}

typedef struct DiffSummaryRow DiffSummaryRow;
struct DiffSummaryRow {
  char zCommitHex[PROLLY_HASH_SIZE*2+1];
  char *zTableName;
  char *zCommitter;
  char *zEmail;
  i64  timestamp;
  char *zMessage;
  u8   dataChange;
  u8   schemaChange;
};

typedef struct DoltliteDiffVtab DoltliteDiffVtab;
struct DoltliteDiffVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

#define DIFF_IDX_TABLE_NAME  0x01

typedef struct DoltliteDiffCursor DoltliteDiffCursor;
struct DoltliteDiffCursor {
  sqlite3_vtab_cursor base;
  /* BFS state for commit walk */
  ProllyHash *aQueue;
  int qHead, qTail, qAlloc;
  ProllyHashSet visited, queued;
  int visitedInit, queuedInit;
  /* Per-commit summary rows (typically 1-5 per commit) */
  DiffSummaryRow *aBatch;
  int nBatch;
  int iBatch;
  /* Table name filter (NULL = no filter) */
  char *zFilterTable;
  /* Phase: 0=working set, 1=commit BFS */
  int phase;
  int hasRow;
  i64 iRowid;
};

static const char *diffSchema =
  "CREATE TABLE x("
  "  commit_hash   TEXT,"
  "  committer     TEXT,"
  "  email         TEXT,"
  "  date          TEXT,"
  "  message       TEXT,"
  "  data_change   INTEGER,"
  "  schema_change INTEGER,"
  "  table_name    TEXT"
  ")";

#define DIFF_COL_COMMIT_HASH   0
#define DIFF_COL_COMMITTER     1
#define DIFF_COL_EMAIL         2
#define DIFF_COL_DATE          3
#define DIFF_COL_MESSAGE       4
#define DIFF_COL_DATA_CHANGE   5
#define DIFF_COL_SCHEMA_CHANGE 6
#define DIFF_COL_TABLE_NAME    7

static void freeBatch(DoltliteDiffCursor *pCur){
  int i;
  for(i=0; i<pCur->nBatch; i++){
    sqlite3_free(pCur->aBatch[i].zTableName);
    sqlite3_free(pCur->aBatch[i].zCommitter);
    sqlite3_free(pCur->aBatch[i].zEmail);
    sqlite3_free(pCur->aBatch[i].zMessage);
  }
  sqlite3_free(pCur->aBatch);
  pCur->aBatch = 0;
  pCur->nBatch = 0;
  pCur->iBatch = 0;
}

static int batchAppend(DoltliteDiffCursor *pCur,
                       const char *zCommitHex,
                       const char *zTableName,
                       const DoltliteCommit *pCommit,
                       u8 dataChange, u8 schemaChange){
  DiffSummaryRow *aNew, *r;
  if( pCur->zFilterTable && strcmp(pCur->zFilterTable, zTableName)!=0 ){
    return SQLITE_OK;
  }
  aNew = sqlite3_realloc(pCur->aBatch,
                         (pCur->nBatch+1)*(int)sizeof(DiffSummaryRow));
  if( !aNew ) return SQLITE_NOMEM;
  pCur->aBatch = aNew;
  r = &aNew[pCur->nBatch];
  memset(r, 0, sizeof(*r));
  memcpy(r->zCommitHex, zCommitHex, PROLLY_HASH_SIZE*2+1);
  r->zTableName = sqlite3_mprintf("%s", zTableName ? zTableName : "");
  if( !r->zTableName ) return SQLITE_NOMEM;
  if( pCommit ){
    r->zCommitter = sqlite3_mprintf("%s", pCommit->zName  ? pCommit->zName  : "");
    r->zEmail     = sqlite3_mprintf("%s", pCommit->zEmail ? pCommit->zEmail : "");
    r->zMessage   = sqlite3_mprintf("%s", pCommit->zMessage ? pCommit->zMessage : "");
    if( !r->zCommitter || !r->zEmail || !r->zMessage ){
      sqlite3_free(r->zTableName);
      sqlite3_free(r->zCommitter);
      sqlite3_free(r->zEmail);
      sqlite3_free(r->zMessage);
      return SQLITE_NOMEM;
    }
    r->timestamp = pCommit->timestamp;
  }
  r->dataChange   = dataChange;
  r->schemaChange = schemaChange;
  pCur->nBatch++;
  return SQLITE_OK;
}

/* Compute summary rows for the working set (uncommitted changes). */
static int computeWorkingBatch(DoltliteDiffCursor *pCur, sqlite3 *db){
  ProllyHash headCat, workCat;
  struct TableEntry *aHead = 0, *aWork = 0;
  int nHead = 0, nWork = 0;
  int rc, i;
  static const char zWorkingHex[] = "WORKING";
  char zHexBuf[PROLLY_HASH_SIZE*2+1];

  memset(&headCat, 0, sizeof(headCat));
  memset(&workCat, 0, sizeof(workCat));

  rc = doltliteGetHeadCatalogHash(db, &headCat);
  if( rc!=SQLITE_OK ) return SQLITE_OK;
  rc = doltliteFlushCatalogToHash(db, &workCat);
  if( rc!=SQLITE_OK ) return rc;

  if( prollyHashCompare(&headCat, &workCat)==0 ) return SQLITE_OK;

  rc = doltliteLoadCatalog(db, &headCat, &aHead, &nHead, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, &workCat, &aWork, &nWork, 0);
  if( rc!=SQLITE_OK ){
    doltliteFreeCatalog(aHead, nHead);
    return rc;
  }

  memset(zHexBuf, 0, sizeof(zHexBuf));
  memcpy(zHexBuf, zWorkingHex, sizeof(zWorkingHex));

  for(i=0; i<nWork; i++){
    struct TableEntry *e = &aWork[i];
    struct TableEntry *p;
    u8 dataChange, schemaChange;
    if( !e->zName ){
      ProllyHash emptyRoot;
      const ProllyHash *pOldRoot;
      struct TableEntry *pOldMaster;
      memset(&emptyRoot, 0, sizeof(emptyRoot));
      pOldMaster = doltliteFindTableByNumber(aHead, nHead, 1);
      pOldRoot = pOldMaster ? &pOldMaster->root : &emptyRoot;
      if( schemaHasViewOrTriggerDiff(db, pOldRoot, &e->root, e->flags) ){
        u8 schemaChangeFlag =
          schemaHasAnyViewOrTrigger(db, pOldRoot, e->flags) ? 0 : 1;
        rc = batchAppend(pCur, zHexBuf, "dolt_schemas", 0,
                         1, schemaChangeFlag);
        if( rc!=SQLITE_OK ) goto done;
      }
      continue;
    }
    p = doltliteFindTableByName(aHead, nHead, e->zName);
    if( !p ){
      dataChange = 1;
      schemaChange = 1;
    }else{
      dataChange   = (prollyHashCompare(&e->root, &p->root) != 0) ? 1 : 0;
      schemaChange = (prollyHashCompare(&e->schemaHash, &p->schemaHash) != 0) ? 1 : 0;
      if( !dataChange && !schemaChange ) continue;
    }
    rc = batchAppend(pCur, zHexBuf, e->zName, 0, dataChange, schemaChange);
    if( rc!=SQLITE_OK ) goto done;
  }
  for(i=0; i<nHead; i++){
    struct TableEntry *p = &aHead[i];
    if( !p->zName ) continue;
    if( doltliteFindTableByName(aWork, nWork, p->zName) ) continue;
    rc = batchAppend(pCur, zHexBuf, p->zName, 0, 1, 1);
    if( rc!=SQLITE_OK ) goto done;
  }

done:
  doltliteFreeCatalog(aHead, nHead);
  doltliteFreeCatalog(aWork, nWork);
  return rc;
}

/* Compute summary rows for a single commit (vs its first parent). */
static int computeCommitBatch(DoltliteDiffCursor *pCur, sqlite3 *db,
                              const ProllyHash *pCommitHash,
                              const DoltliteCommit *pCommit,
                              const char *zCommitHex){
  struct TableEntry *aChild = 0, *aParent = 0;
  int nChild = 0, nParent = 0;
  int rc, i;
  const ProllyHash *pParentHash = doltliteCommitParentHash(pCommit, 0);
  int hasParent = (pParentHash && !prollyHashIsEmpty(pParentHash));
  (void)pCommitHash;

  rc = doltliteLoadCatalog(db, &pCommit->catalogHash, &aChild, &nChild, 0);
  if( rc!=SQLITE_OK ) return rc;

  if( hasParent ){
    DoltliteCommit parent;
    memset(&parent, 0, sizeof(parent));
    rc = doltliteLoadCommit(db, pParentHash, &parent);
    if( rc==SQLITE_OK ){
      rc = doltliteLoadCatalog(db, &parent.catalogHash, &aParent, &nParent, 0);
      doltliteCommitClear(&parent);
    }
    if( rc!=SQLITE_OK ){
      doltliteFreeCatalog(aChild, nChild);
      return rc;
    }
  }

  for(i=0; i<nChild; i++){
    struct TableEntry *e = &aChild[i];
    struct TableEntry *p;
    u8 dataChange, schemaChange;
    if( !e->zName ){
      ProllyHash emptyRoot;
      const ProllyHash *pOldRoot;
      struct TableEntry *pOldMaster;
      memset(&emptyRoot, 0, sizeof(emptyRoot));
      pOldMaster = doltliteFindTableByNumber(aParent, nParent, 1);
      pOldRoot = pOldMaster ? &pOldMaster->root : &emptyRoot;
      if( schemaHasViewOrTriggerDiff(db, pOldRoot, &e->root, e->flags) ){
        u8 schemaChangeFlag =
          schemaHasAnyViewOrTrigger(db, pOldRoot, e->flags) ? 0 : 1;
        rc = batchAppend(pCur, zCommitHex, "dolt_schemas", pCommit,
                         1, schemaChangeFlag);
        if( rc!=SQLITE_OK ) goto done;
      }
      continue;
    }
    p = doltliteFindTableByName(aParent, nParent, e->zName);
    if( !p ){
      dataChange = 1;
      schemaChange = 1;
    }else{
      dataChange   = (prollyHashCompare(&e->root, &p->root) != 0) ? 1 : 0;
      schemaChange = (prollyHashCompare(&e->schemaHash, &p->schemaHash) != 0) ? 1 : 0;
      if( !dataChange && !schemaChange ) continue;
    }
    rc = batchAppend(pCur, zCommitHex, e->zName, pCommit,
                     dataChange, schemaChange);
    if( rc!=SQLITE_OK ) goto done;
  }

  for(i=0; i<nParent; i++){
    struct TableEntry *p = &aParent[i];
    if( !p->zName ) continue;
    if( doltliteFindTableByName(aChild, nChild, p->zName) ) continue;
    rc = batchAppend(pCur, zCommitHex, p->zName, pCommit, 1, 1);
    if( rc!=SQLITE_OK ) goto done;
  }

done:
  doltliteFreeCatalog(aChild, nChild);
  doltliteFreeCatalog(aParent, nParent);
  return rc;
}

/* Advance the cursor to the next summary row. Moves through the
** current batch, then advances BFS to the next commit. */
static int diffAdvance(DoltliteDiffCursor *pCur, sqlite3 *db){
  int rc;

  /* Try next row in current batch. */
  if( pCur->iBatch < pCur->nBatch ){
    pCur->hasRow = 1;
    return SQLITE_OK;
  }

  /* Current batch exhausted — free and compute next. */
  freeBatch(pCur);

  /* Phase 0: working set. */
  if( pCur->phase==0 ){
    pCur->phase = 1;
    rc = computeWorkingBatch(pCur, db);
    if( rc!=SQLITE_OK ) return rc;
    if( pCur->nBatch>0 ){
      pCur->hasRow = 1;
      return SQLITE_OK;
    }
    /* Fall through to BFS if working set had no changes. */
  }

  /* Phase 1: BFS commit walk. */
  while( pCur->qHead < pCur->qTail ){
    ProllyHash cur = pCur->aQueue[pCur->qHead++];
    DoltliteCommit commit;
    char zHex[PROLLY_HASH_SIZE*2+1];
    int i;

    if( prollyHashSetContains(&pCur->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&pCur->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;

    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ) return rc;

    doltliteHashToHex(&cur, zHex);
    rc = computeCommitBatch(pCur, db, &cur, &commit, zHex);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      return rc;
    }

    /* Enqueue parents. */
    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( prollyHashSetContains(&pCur->visited, pParent) ) continue;
      if( prollyHashSetContains(&pCur->queued,  pParent) ) continue;
      if( pCur->qTail >= pCur->qAlloc ){
        int nNew = pCur->qAlloc*2;
        ProllyHash *tmp = sqlite3_realloc(pCur->aQueue,
                                          nNew*(int)sizeof(ProllyHash));
        if( !tmp ){
          doltliteCommitClear(&commit);
          return SQLITE_NOMEM;
        }
        pCur->aQueue = tmp; pCur->qAlloc = nNew;
      }
      pCur->aQueue[pCur->qTail++] = *pParent;
      rc = prollyHashSetAdd(&pCur->queued, pParent);
      if( rc!=SQLITE_OK ){
        doltliteCommitClear(&commit);
        return rc;
      }
    }
    doltliteCommitClear(&commit);

    if( pCur->nBatch>0 ){
      pCur->hasRow = 1;
      return SQLITE_OK;
    }
    /* This commit had no table changes (or all filtered out) — continue. */
  }

  pCur->hasRow = 0;
  return SQLITE_OK;
}

static void diffCursorReset(DoltliteDiffCursor *pCur){
  freeBatch(pCur);
  sqlite3_free(pCur->aQueue);
  pCur->aQueue = 0;
  pCur->qHead = pCur->qTail = pCur->qAlloc = 0;
  if( pCur->visitedInit ){
    prollyHashSetFree(&pCur->visited);
    pCur->visitedInit = 0;
  }
  if( pCur->queuedInit ){
    prollyHashSetFree(&pCur->queued);
    pCur->queuedInit = 0;
  }
  sqlite3_free(pCur->zFilterTable);
  pCur->zFilterTable = 0;
  pCur->phase = 0;
  pCur->hasRow = 0;
  pCur->iRowid = 0;
}

static int diffConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DoltliteDiffVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, diffSchema);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int diffDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int diffBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iTableName = -1;
  int i;
  int argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case DIFF_COL_TABLE_NAME:  iTableName  = i; break;
    }
  }

  if( iTableName>=0 ){
    pInfo->aConstraintUsage[iTableName].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTableName].omit = 1;
  }

  pInfo->idxNum = (iTableName>=0  ? DIFF_IDX_TABLE_NAME  : 0);
  pInfo->estimatedCost = (iTableName>=0) ? 1000.0 : 100000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

static int diffOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  DoltliteDiffCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int diffClose(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  diffCursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int diffFilter(sqlite3_vtab_cursor *pCursor,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DoltliteDiffVtab *pVtab = (DoltliteDiffVtab*)pCursor->pVtab;
  sqlite3 *db = pVtab->db;
  ChunkStore *cs;
  ProllyHash head;
  int argIdx = 0;
  int rc;
  (void)idxStr;

  diffCursorReset(pCur);

  /* Capture table_name filter if present. */
  if( (idxNum & DIFF_IDX_TABLE_NAME) && argIdx<argc ){
    const char *z = (const char*)sqlite3_value_text(argv[argIdx++]);
    if( z ){
      pCur->zFilterTable = sqlite3_mprintf("%s", z);
      if( !pCur->zFilterTable ) return SQLITE_NOMEM;
    }
  }

  cs = doltliteGetChunkStore(db);
  if( !cs ) return SQLITE_OK;

  doltliteGetSessionHead(db, &head);
  if( prollyHashIsEmpty(&head) ) return SQLITE_OK;

  /* Initialize BFS state. */
  rc = prollyHashSetInit(&pCur->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  pCur->visitedInit = 1;

  rc = prollyHashSetInit(&pCur->queued, 64);
  if( rc!=SQLITE_OK ) return rc;
  pCur->queuedInit = 1;

  pCur->qAlloc = 16;
  pCur->aQueue = sqlite3_malloc(pCur->qAlloc * (int)sizeof(ProllyHash));
  if( !pCur->aQueue ) return SQLITE_NOMEM;
  pCur->aQueue[0] = head;
  pCur->qTail = 1;
  rc = prollyHashSetAdd(&pCur->queued, &head);
  if( rc!=SQLITE_OK ) return rc;

  /* Prime the first row (working set first, then BFS). */
  pCur->phase = 0;
  return diffAdvance(pCur, db);
}

static int diffNext(sqlite3_vtab_cursor *pCursor){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DoltliteDiffVtab *pVtab = (DoltliteDiffVtab*)pCursor->pVtab;
  pCur->iRowid++;
  pCur->iBatch++;
  return diffAdvance(pCur, pVtab->db);
}

static int diffEof(sqlite3_vtab_cursor *pCursor){
  return !((DoltliteDiffCursor*)pCursor)->hasRow;
}

static int diffColumn(sqlite3_vtab_cursor *pCursor,
    sqlite3_context *ctx, int iCol){
  DoltliteDiffCursor *pCur = (DoltliteDiffCursor*)pCursor;
  DiffSummaryRow *r;
  if( !pCur->hasRow || pCur->iBatch >= pCur->nBatch ) return SQLITE_OK;
  r = &pCur->aBatch[pCur->iBatch];
  switch( iCol ){
    case DIFF_COL_COMMIT_HASH:
      sqlite3_result_text(ctx, r->zCommitHex, -1, SQLITE_TRANSIENT);
      break;
    case DIFF_COL_COMMITTER:
      if( r->zCommitter ){
        sqlite3_result_text(ctx, r->zCommitter, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_EMAIL:
      if( r->zEmail ){
        sqlite3_result_text(ctx, r->zEmail, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_DATE: {
      if( r->timestamp==0 && r->zCommitter==0 ){
        sqlite3_result_null(ctx);
      }else{
        time_t t = (time_t)r->timestamp;
        struct tm *tm = gmtime(&t);
        if( tm ){
          char buf[32];
          strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
          sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
        }else{
          sqlite3_result_null(ctx);
        }
      }
      break;
    }
    case DIFF_COL_MESSAGE:
      if( r->zMessage ){
        sqlite3_result_text(ctx, r->zMessage, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    case DIFF_COL_DATA_CHANGE:
      sqlite3_result_int(ctx, r->dataChange ? 1 : 0);
      break;
    case DIFF_COL_SCHEMA_CHANGE:
      sqlite3_result_int(ctx, r->schemaChange ? 1 : 0);
      break;
    case DIFF_COL_TABLE_NAME:
      sqlite3_result_text(ctx, r->zTableName, -1, SQLITE_TRANSIENT);
      break;
    default:
      sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int diffRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteDiffCursor*)pCursor)->iRowid;
  return SQLITE_OK;
}

static sqlite3_module doltliteDiffModule = {
  0, 0, diffConnect, diffBestIndex, diffDisconnect, 0,
  diffOpen, diffClose, diffFilter, diffNext, diffEof,
  diffColumn, diffRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteDiffRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_diff", &doltliteDiffModule, 0);
}

#endif
