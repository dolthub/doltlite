
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

typedef struct DoltliteLogVtab DoltliteLogVtab;
struct DoltliteLogVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DoltliteLogCursor DoltliteLogCursor;
struct DoltliteLogCursor {
  sqlite3_vtab_cursor base;
  DoltliteCommitQueue queue;
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
  doltliteCommitQueueClear(&pCur->queue);
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
    rc = doltliteCommitQueueNext(&pCur->queue, &cur, &hasHash);
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

    rc = doltliteCommitQueueEnqueueParents(&pCur->queue, &pCur->curCommit);
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

  rc = doltliteCommitQueueInit(&pCur->queue, &head);
  if( rc!=SQLITE_OK ) return rc;
  if( useSecondHead ){
    rc = doltliteCommitQueueEnqueue(&pCur->queue, &secondHead);
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
