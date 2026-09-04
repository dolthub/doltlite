
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "chunk_store.h"

#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

#define LOG_IDX_HASH_EQ 0x01
#define LOG_IDX_REVISION 0x02

/* Dolt emits commits by height (its commit_order) descending, breaking ties on
** date descending and then on the order a parent was reached. Height needs the
** ancestor walk, so keys are computed only once more than one commit is
** pending: a linear history never pays for them. */
typedef struct LogHeightMap LogHeightMap;
struct LogHeightMap {
  ProllyHash *aKey;
  i64 *aVal;
  u8 *aUsed;
  int nSlot;
  int nUsed;
};

static void logHeightMapFree(LogHeightMap *p){
  sqlite3_free(p->aKey);
  sqlite3_free(p->aVal);
  sqlite3_free(p->aUsed);
  memset(p, 0, sizeof(*p));
}

static unsigned logHashSlot(const ProllyHash *pHash, int nSlot){
  unsigned h = ((unsigned)pHash->data[0])
             | ((unsigned)pHash->data[1] << 8)
             | ((unsigned)pHash->data[2] << 16)
             | ((unsigned)pHash->data[3] << 24);
  return h & (unsigned)(nSlot - 1);
}

static int logHeightMapFind(LogHeightMap *p, const ProllyHash *pHash, i64 *pVal){
  unsigned i;
  if( p->nSlot==0 ) return 0;
  i = logHashSlot(pHash, p->nSlot);
  while( p->aUsed[i] ){
    if( memcmp(&p->aKey[i], pHash, sizeof(ProllyHash))==0 ){
      *pVal = p->aVal[i];
      return 1;
    }
    i = (i + 1) & (unsigned)(p->nSlot - 1);
  }
  return 0;
}

static int logHeightMapPut(LogHeightMap *p, const ProllyHash *pHash, i64 val){
  unsigned i;
  if( p->nSlot==0 || p->nUsed*10 >= p->nSlot*7 ){
    int nNew = p->nSlot ? p->nSlot * 2 : 256;
    LogHeightMap grown;
    int j;
    memset(&grown, 0, sizeof(grown));
    grown.aKey = sqlite3_malloc64((sqlite3_int64)nNew * sizeof(ProllyHash));
    grown.aVal = sqlite3_malloc64((sqlite3_int64)nNew * sizeof(i64));
    grown.aUsed = sqlite3_malloc64((sqlite3_int64)nNew);
    if( !grown.aKey || !grown.aVal || !grown.aUsed ){
      logHeightMapFree(&grown);
      return SQLITE_NOMEM;
    }
    memset(grown.aUsed, 0, (size_t)nNew);
    grown.nSlot = nNew;
    for(j=0; j<p->nSlot; j++){
      if( p->aUsed[j] ) logHeightMapPut(&grown, &p->aKey[j], p->aVal[j]);
    }
    logHeightMapFree(p);
    *p = grown;
  }
  i = logHashSlot(pHash, p->nSlot);
  while( p->aUsed[i] ){
    if( memcmp(&p->aKey[i], pHash, sizeof(ProllyHash))==0 ){
      p->aVal[i] = val;
      return SQLITE_OK;
    }
    i = (i + 1) & (unsigned)(p->nSlot - 1);
  }
  p->aKey[i] = *pHash;
  p->aVal[i] = val;
  p->aUsed[i] = 1;
  p->nUsed++;
  return SQLITE_OK;
}

/* height(c) = 1 + max(height(parents)), matching Dolt's commit_order. Walked
** iteratively: a deep history must not consume the C stack. */
static int logCommitHeight(
  sqlite3 *db,
  LogHeightMap *pMap,
  const ProllyHash *pHash,
  i64 *pOut
){
  ProllyHash *aStack = 0;
  int nStack = 0, nAlloc = 0;
  int rc = SQLITE_OK;

  if( logHeightMapFind(pMap, pHash, pOut) ) return SQLITE_OK;
  aStack = sqlite3_malloc64(16 * sizeof(ProllyHash));
  if( !aStack ) return SQLITE_NOMEM;
  nAlloc = 16;
  aStack[nStack++] = *pHash;

  while( nStack>0 ){
    ProllyHash cur = aStack[nStack-1];
    DoltliteCommit commit;
    i64 best = 0;
    int nPending = 0;
    int i, nParent;
    i64 ignored;

    if( logHeightMapFind(pMap, &cur, &ignored) ){
      nStack--;
      continue;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&commit);
      break;
    }
    nParent = doltliteCommitParentCount(&commit);
    for(i=0; i<nParent; i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      i64 h = 0;
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( logHeightMapFind(pMap, pParent, &h) ){
        if( h>best ) best = h;
        continue;
      }
      if( nStack+nPending >= nAlloc ){
        i64 nNew = (i64)nAlloc * 2;
        ProllyHash *tmp;
        if( nNew > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ){
          rc = SQLITE_NOMEM;
          break;
        }
        tmp = sqlite3_realloc64(aStack, (sqlite3_uint64)nNew * sizeof(ProllyHash));
        if( !tmp ){ rc = SQLITE_NOMEM; break; }
        aStack = tmp;
        nAlloc = (int)nNew;
      }
      aStack[nStack + nPending] = *pParent;
      nPending++;
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
    if( nPending>0 ){
      nStack += nPending;
      continue;
    }
    rc = logHeightMapPut(pMap, &cur, best + 1);
    if( rc!=SQLITE_OK ) break;
    nStack--;
  }

  sqlite3_free(aStack);
  if( rc!=SQLITE_OK ) return rc;
  if( !logHeightMapFind(pMap, pHash, pOut) ) return SQLITE_CORRUPT;
  return SQLITE_OK;
}

typedef struct LogFrontier LogFrontier;
struct LogFrontier {
  ProllyHash *aPend;
  i64 *aHeight;
  i64 *aDate;
  u8 *aHaveKey;
  int nPend;
  int nAlloc;
  ProllyHashSet visited;
  ProllyHashSet queued;
  int setsInit;
  LogHeightMap heights;
};

static void logFrontierClear(LogFrontier *f){
  sqlite3_free(f->aPend);
  sqlite3_free(f->aHeight);
  sqlite3_free(f->aDate);
  sqlite3_free(f->aHaveKey);
  if( f->setsInit ){
    prollyHashSetFree(&f->visited);
    prollyHashSetFree(&f->queued);
  }
  logHeightMapFree(&f->heights);
  memset(f, 0, sizeof(*f));
}

static int logFrontierEnqueue(LogFrontier *f, const ProllyHash *pHash){
  if( !pHash || prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  if( prollyHashSetContains(&f->visited, pHash) ) return SQLITE_OK;
  if( prollyHashSetContains(&f->queued, pHash) ) return SQLITE_OK;
  if( f->nPend >= f->nAlloc ){
    i64 nNew = f->nAlloc ? (i64)f->nAlloc * 2 : (i64)8;
    ProllyHash *aP;
    i64 *aH; i64 *aD; u8 *aK;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ) return SQLITE_NOMEM;
    aP = sqlite3_realloc64(f->aPend, (sqlite3_uint64)nNew * sizeof(ProllyHash));
    if( !aP ) return SQLITE_NOMEM;
    f->aPend = aP;
    aH = sqlite3_realloc64(f->aHeight, (sqlite3_uint64)nNew * sizeof(i64));
    if( !aH ) return SQLITE_NOMEM;
    f->aHeight = aH;
    aD = sqlite3_realloc64(f->aDate, (sqlite3_uint64)nNew * sizeof(i64));
    if( !aD ) return SQLITE_NOMEM;
    f->aDate = aD;
    aK = sqlite3_realloc64(f->aHaveKey, (sqlite3_uint64)nNew);
    if( !aK ) return SQLITE_NOMEM;
    f->aHaveKey = aK;
    f->nAlloc = (int)nNew;
  }
  f->aPend[f->nPend] = *pHash;
  f->aHeight[f->nPend] = 0;
  f->aDate[f->nPend] = 0;
  f->aHaveKey[f->nPend] = 0;
  f->nPend++;
  return prollyHashSetAdd(&f->queued, pHash);
}

static int logFrontierInit(LogFrontier *f, const ProllyHash *pHead){
  int rc;
  logFrontierClear(f);
  rc = prollyHashSetInit(&f->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyHashSetInit(&f->queued, 64);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&f->visited);
    memset(f, 0, sizeof(*f));
    return rc;
  }
  f->setsInit = 1;
  rc = logFrontierEnqueue(f, pHead);
  if( rc!=SQLITE_OK ) logFrontierClear(f);
  return rc;
}

static int logFrontierEnqueueParents(LogFrontier *f, const DoltliteCommit *pCommit){
  int i, rc;
  for(i=0; i<doltliteCommitParentCount(pCommit); i++){
    rc = logFrontierEnqueue(f, doltliteCommitParentHash(pCommit, i));
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int logFrontierKey(LogFrontier *f, sqlite3 *db, int i){
  DoltliteCommit commit;
  int rc;
  if( f->aHaveKey[i] ) return SQLITE_OK;
  rc = logCommitHeight(db, &f->heights, &f->aPend[i], &f->aHeight[i]);
  if( rc!=SQLITE_OK ) return rc;
  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, &f->aPend[i], &commit);
  if( rc==SQLITE_OK ) f->aDate[i] = commit.timestamp;
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return rc;
  f->aHaveKey[i] = 1;
  return SQLITE_OK;
}

static int logFrontierNext(
  LogFrontier *f,
  sqlite3 *db,
  ProllyHash *pHash,
  int *pHas
){
  int rc;
  *pHas = 0;
  while( f->nPend>0 ){
    int iBest = 0;
    ProllyHash cur;
    if( f->nPend>1 ){
      int i;
      for(i=0; i<f->nPend; i++){
        rc = logFrontierKey(f, db, i);
        if( rc!=SQLITE_OK ) return rc;
      }
      for(i=1; i<f->nPend; i++){
        if( f->aHeight[i] > f->aHeight[iBest]
         || (f->aHeight[i]==f->aHeight[iBest] && f->aDate[i] > f->aDate[iBest]) ){
          iBest = i;
        }
      }
    }
    cur = f->aPend[iBest];
    if( iBest < f->nPend-1 ){
      int nMove = f->nPend - iBest - 1;
      memmove(&f->aPend[iBest], &f->aPend[iBest+1], (size_t)nMove * sizeof(ProllyHash));
      memmove(&f->aHeight[iBest], &f->aHeight[iBest+1], (size_t)nMove * sizeof(i64));
      memmove(&f->aDate[iBest], &f->aDate[iBest+1], (size_t)nMove * sizeof(i64));
      memmove(&f->aHaveKey[iBest], &f->aHaveKey[iBest+1], (size_t)nMove);
    }
    f->nPend--;
    if( prollyHashSetContains(&f->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&f->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;
    *pHash = cur;
    *pHas = 1;
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

typedef struct DoltliteLogVtab DoltliteLogVtab;
struct DoltliteLogVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DoltliteLogCursor DoltliteLogCursor;
struct DoltliteLogCursor {
  sqlite3_vtab_cursor base;
  LogFrontier frontier;
  ProllyHashSet excluded;
  int excludedInit;
  char zHex[PROLLY_HASH_SIZE*2+1];
  DoltliteCommit curCommit;
  int hasRow;
  i64 iRowid;
  int singleCommit;
};

static int logMapChunkSourceError(
  DoltliteLogCursor *pCur,
  sqlite3 *db,
  int sourceRc,
  int mappedRc
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int pendingRc = SQLITE_OK;
  char *zErr = cs ? chunkStoreSourceTakeError(cs, &pendingRc) : 0;
  if( !zErr && pendingRc==SQLITE_OK ) return mappedRc;
  if( zErr ){
    sqlite3_free(pCur->base.pVtab->zErrMsg);
    pCur->base.pVtab->zErrMsg = zErr;
  }
  return pendingRc!=SQLITE_OK ? pendingRc : sourceRc;
}

static const char *doltliteLogSchema =
  "CREATE TABLE x("
  "  commit_hash TEXT,"
  "  committer TEXT,"
  "  email TEXT,"
  "  date TEXT,"
  "  message TEXT,"
  "  revision TEXT HIDDEN"
  ")";

static int doltliteLogConnect(
  sqlite3 *db, void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab, char **pzErr
){
  DoltliteLogVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;

  rc = doltliteVtabConnectSimple(db, doltliteLogSchema,
                                 sizeof(*pVtab), ppVtab);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = (DoltliteLogVtab*)*ppVtab;
  pVtab->db = db;
  return SQLITE_OK;
}

static int doltliteLogOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  (void)pVtab;
  return doltliteVtabOpenCursor(ppCursor, sizeof(DoltliteLogCursor));
}

static void logCursorReset(DoltliteLogCursor *pCur){
  doltliteCommitClear(&pCur->curCommit);
  memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
  logFrontierClear(&pCur->frontier);
  if( pCur->excludedInit ){
    prollyHashSetFree(&pCur->excluded);
    pCur->excludedInit = 0;
  }
  pCur->hasRow = 0;
  pCur->iRowid = 0;
  pCur->singleCommit = 0;
}

static int doltliteLogClose(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  logCursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int logAdvance(DoltliteLogCursor *pCur, sqlite3 *db){
  int rc, hasHash;

  doltliteCommitClear(&pCur->curCommit);
  memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
  pCur->hasRow = 0;

  for(;;){
    ProllyHash cur;
    rc = logFrontierNext(&pCur->frontier, db, &cur, &hasHash);
    if( rc!=SQLITE_OK ) return rc;
    if( !hasHash ) break;
    if( pCur->excludedInit
     && prollyHashSetContains(&pCur->excluded, &cur) ){
      continue;
    }

    rc = doltliteLoadCommit(db, &cur, &pCur->curCommit);
    if( rc!=SQLITE_OK ){
      if( pCur->singleCommit ){
        rc = logMapChunkSourceError(pCur, db, rc, SQLITE_OK);
        doltliteCommitClear(&pCur->curCommit);
        memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
        return rc;
      }
      return rc;
    }

    doltliteHashToHex(&cur, pCur->zHex);
    pCur->hasRow = 1;

    if( pCur->singleCommit ) return SQLITE_OK;

    rc = logFrontierEnqueueParents(&pCur->frontier, &pCur->curCommit);
    if( rc!=SQLITE_OK ) return rc;
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static int logAddReachable(
  sqlite3 *db,
  ProllyHashSet *pSet,
  const ProllyHash *pHead
){
  DoltliteCommitQueue queue;
  int rc;

  memset(&queue, 0, sizeof(queue));
  rc = doltliteCommitQueueInit(&queue, pHead);
  while( rc==SQLITE_OK ){
    ProllyHash hash;
    DoltliteCommit commit;
    int hasHash = 0;
    rc = doltliteCommitQueueNext(&queue, &hash, &hasHash);
    if( rc!=SQLITE_OK || !hasHash ) break;
    rc = prollyHashSetAdd(pSet, &hash);
    if( rc!=SQLITE_OK ) break;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &hash, &commit);
    if( rc==SQLITE_OK ){
      rc = doltliteCommitQueueEnqueueParents(&queue, &commit);
    }
    doltliteCommitClear(&commit);
  }
  doltliteCommitQueueClear(&queue);
  return rc;
}

static int logIsReachable(
  sqlite3 *db,
  const ProllyHash *pHead,
  const ProllyHash *pTarget,
  int *pReachable
){
  DoltliteCommitQueue queue;
  int rc;

  memset(&queue, 0, sizeof(queue));
  *pReachable = 0;
  rc = doltliteCommitQueueInit(&queue, pHead);
  while( rc==SQLITE_OK ){
    ProllyHash hash;
    DoltliteCommit commit;
    int hasHash = 0;
    rc = doltliteCommitQueueNext(&queue, &hash, &hasHash);
    if( rc!=SQLITE_OK || !hasHash ) break;
    if( prollyHashCompare(&hash, pTarget)==0 ){
      *pReachable = 1;
      break;
    }
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &hash, &commit);
    if( rc==SQLITE_OK ){
      rc = doltliteCommitQueueEnqueueParents(&queue, &commit);
    }
    doltliteCommitClear(&commit);
  }
  doltliteCommitQueueClear(&queue);
  return rc;
}

static int doltliteLogNext(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  DoltliteLogVtab *pVtab = (DoltliteLogVtab*)pCursor->pVtab;
  pCur->iRowid++;
  return logAdvance(pCur, pVtab->db);
}

static int doltliteLogFilter(
  sqlite3_vtab_cursor *pCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  DoltliteLogVtab *pVtab = (DoltliteLogVtab*)pCursor->pVtab;
  ProllyHash head;
  ProllyHash secondHead;
  ProllyHash startHash;
  ChunkStore *cs;
  int rc;
  int useStart = 0;
  int useSecondHead = 0;
  (void)idxStr;

  logCursorReset(pCur);
  memset(&startHash, 0, sizeof(startHash));

  cs = doltliteGetChunkStore(pVtab->db);
  if( !cs ){
    if( doltliteIsStockSqliteDb(pVtab->db) ){
      pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s",
          doltliteVcUnavailableMessage(pVtab->db));
      return pCursor->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }

  if( (idxNum & LOG_IDX_HASH_EQ) && argc>0 ){
    const char *z = (const char*)sqlite3_value_text(argv[0]);
    if( z && doltliteHexToHash(z, &startHash)==SQLITE_OK
        && !prollyHashIsEmpty(&startHash) ){
      useStart = 1;
    }else{
      return SQLITE_OK;
    }
  }else if( (idxNum & LOG_IDX_REVISION) && argc>0 ){
    const char *zSpec = (const char*)sqlite3_value_text(argv[0]);
    char *zLeft = 0;
    char *zRight = 0;
    int rangeType = DOLTLITE_RANGE_NONE;

    rc = doltliteSplitRevisionRange(zSpec, &zLeft, &zRight, &rangeType);
    if( rc==SQLITE_NOTFOUND ){
      rc = doltliteResolveRef(pVtab->db, zSpec, &head);
    }else if( rc==SQLITE_OK ){
      ProllyHash leftHash;
      rc = doltliteResolveRef(pVtab->db, zLeft, &leftHash);
      if( rc==SQLITE_OK ) rc = doltliteResolveRef(pVtab->db, zRight, &head);
      if( rc==SQLITE_OK ){
        rc = prollyHashSetInit(&pCur->excluded, 64);
        if( rc==SQLITE_OK ) pCur->excludedInit = 1;
      }
      if( rc==SQLITE_OK && rangeType==DOLTLITE_RANGE_THREE_DOT ){
        ProllyHash ancestor;
        secondHead = leftHash;
        useSecondHead = 1;
        rc = doltliteFindAncestor(pVtab->db, &leftHash, &head, &ancestor);
        if( rc==SQLITE_OK ){
          rc = logAddReachable(pVtab->db, &pCur->excluded, &ancestor);
        }
      }else if( rc==SQLITE_OK ){
        rc = logAddReachable(pVtab->db, &pCur->excluded, &leftHash);
      }
    }
    sqlite3_free(zLeft);
    sqlite3_free(zRight);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pCursor->pVtab->zErrMsg);
      pCursor->pVtab->zErrMsg = sqlite3_mprintf(
          "invalid dolt_log revision: %s", zSpec ? zSpec : "");
      return pCursor->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
    useStart = 2;
  }

  if( useStart==1 ){
    int reachable;
    doltliteGetSessionHead(pVtab->db, &head);
    if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
    rc = logIsReachable(pVtab->db, &head, &startHash, &reachable);
    if( rc!=SQLITE_OK ) return rc;
    if( !reachable ) return SQLITE_OK;
    head = startHash;
    pCur->singleCommit = 1;
  }else if( useStart==0 ){
    doltliteGetSessionHead(pVtab->db, &head);
    if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
  }

  rc = logFrontierInit(&pCur->frontier, &head);
  if( rc!=SQLITE_OK ) return rc;
  if( useSecondHead ){
    rc = logFrontierEnqueue(&pCur->frontier, &secondHead);
    if( rc!=SQLITE_OK ) return rc;
  }

  return logAdvance(pCur, pVtab->db);
}

static int doltliteLogEof(sqlite3_vtab_cursor *pCursor){
  return !((DoltliteLogCursor*)pCursor)->hasRow;
}

static int doltliteLogColumn(
  sqlite3_vtab_cursor *pCursor,
  sqlite3_context *ctx,
  int iCol
){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  DoltliteCommit *c;

  if( !pCur->hasRow ) return SQLITE_OK;
  c = &pCur->curCommit;

  switch( iCol ){
    case 0:
      sqlite3_result_text(ctx, pCur->zHex, -1, SQLITE_TRANSIENT);
      break;
    case 1:
      sqlite3_result_text(ctx, c->zName ? c->zName : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 2:
      sqlite3_result_text(ctx, c->zEmail ? c->zEmail : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 3:
      {
        doltliteResultTimestamp(ctx, c->timestamp);
      }
      break;
    case 4:
      sqlite3_result_text(ctx, c->zMessage ? c->zMessage : "",
                          -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}

static int doltliteLogRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteLogCursor*)pCursor)->iRowid;
  return SQLITE_OK;
}

static int doltliteLogBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int i, iHashEq = -1, iRevisionEq = -1;
  int idxNum = 0;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->op != SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pC->iColumn == 0 && iHashEq < 0
     && doltliteVtabConstraintIsBinary(pInfo, i) ){
      iHashEq = i;
    }else if( pC->iColumn == 5 && iRevisionEq < 0 ){
      iRevisionEq = i;
    }
  }

  if( iRevisionEq >= 0 ){
    pInfo->aConstraintUsage[iRevisionEq].argvIndex = 1;
    pInfo->aConstraintUsage[iRevisionEq].omit = 1;
    idxNum |= LOG_IDX_REVISION;
    pInfo->estimatedCost = 100.0;
    pInfo->estimatedRows = 10;
  }else if( iHashEq >= 0 ){
    pInfo->aConstraintUsage[iHashEq].argvIndex = 1;
    pInfo->aConstraintUsage[iHashEq].omit = 0;
    idxNum |= LOG_IDX_HASH_EQ;
    pInfo->estimatedCost = 10.0;
    pInfo->estimatedRows = 1;
  }else{
    pInfo->estimatedCost = 1000.0;
    pInfo->estimatedRows = 100;
  }

  pInfo->idxNum = idxNum;
  return SQLITE_OK;
}

static sqlite3_module doltliteLogModule = {
  0, 0,
  doltliteLogConnect, doltliteLogBestIndex, doltliteVtabDisconnect, 0,
  doltliteLogOpen, doltliteLogClose, doltliteLogFilter, doltliteLogNext,
  doltliteLogEof, doltliteLogColumn, doltliteLogRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteLogRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_log", &doltliteLogModule, 0);
}

#endif
