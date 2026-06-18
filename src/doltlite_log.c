
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"

#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

#define LOG_IDX_HASH_EQ 0x01

typedef struct DoltliteLogVtab DoltliteLogVtab;
struct DoltliteLogVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DoltliteLogCursor DoltliteLogCursor;
struct DoltliteLogCursor {
  sqlite3_vtab_cursor base;
  ProllyHash *aQueue;
  int qHead, qTail, qAlloc;
  ProllyHashSet visited;
  int visitedInit;
  char zHex[PROLLY_HASH_SIZE*2+1];
  DoltliteCommit curCommit;
  int hasRow;
  i64 iRowid;
  int singleCommit;
};

static const char *doltliteLogSchema =
  "CREATE TABLE x("
  "  commit_hash TEXT,"
  "  committer TEXT,"
  "  email TEXT,"
  "  date TEXT,"
  "  message TEXT"
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
  sqlite3_free(pCur->aQueue);
  pCur->aQueue = 0;
  pCur->qHead = pCur->qTail = pCur->qAlloc = 0;
  if( pCur->visitedInit ){
    prollyHashSetFree(&pCur->visited);
    pCur->visitedInit = 0;
  }
  pCur->hasRow = 0;
  pCur->iRowid = 0;
}

static int doltliteLogClose(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  logCursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int logAdvance(DoltliteLogCursor *pCur, sqlite3 *db){
  int i, rc;

  doltliteCommitClear(&pCur->curCommit);
  memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
  pCur->hasRow = 0;

  while( pCur->qHead < pCur->qTail ){
    ProllyHash cur = pCur->aQueue[pCur->qHead++];

    if( prollyHashSetContains(&pCur->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&pCur->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;

    rc = doltliteLoadCommit(db, &cur, &pCur->curCommit);
    if( rc!=SQLITE_OK ){
      if( pCur->singleCommit ){
        doltliteCommitClear(&pCur->curCommit);
        memset(&pCur->curCommit, 0, sizeof(pCur->curCommit));
        return SQLITE_OK;
      }
      return rc;
    }

    doltliteHashToHex(&cur, pCur->zHex);
    pCur->hasRow = 1;

    if( pCur->singleCommit ) return SQLITE_OK;

    for(i = 0; i < doltliteCommitParentCount(&pCur->curCommit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&pCur->curCommit, i);
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      if( pCur->qTail >= pCur->qAlloc ){
        i64 nNew = pCur->qAlloc ? (i64)pCur->qAlloc * 2 : (i64)16;
        ProllyHash *tmp;
        if( nNew > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ) return SQLITE_NOMEM;
        tmp = sqlite3_realloc(pCur->aQueue,
                              (int)(nNew * (i64)sizeof(ProllyHash)));
        if( !tmp ) return SQLITE_NOMEM;
        pCur->aQueue = tmp;
        pCur->qAlloc = (int)nNew;
      }
      pCur->aQueue[pCur->qTail++] = *pParent;
    }
    return SQLITE_OK;
  }
  return SQLITE_OK;
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
  ProllyHash startHash;
  ChunkStore *cs;
  int rc;
  int useStart = 0;
  (void)idxStr;

  logCursorReset(pCur);
  memset(&startHash, 0, sizeof(startHash));

  if( (idxNum & LOG_IDX_HASH_EQ) && argc>0 ){
    const char *z = (const char*)sqlite3_value_text(argv[0]);
    if( z && doltliteHexToHash(z, &startHash)==SQLITE_OK
        && !prollyHashIsEmpty(&startHash) ){
      useStart = 1;
    }else{
      return SQLITE_OK;
    }
  }

  cs = doltliteGetChunkStore(pVtab->db);
  if( !cs ){
    if( doltliteIsStockSqliteDb(pVtab->db) ){
      pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s",
          doltliteVcUnavailableMessage(pVtab->db));
      return pCursor->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }

  if( useStart ){
    head = startHash;
    pCur->singleCommit = 1;
  }else{
    doltliteGetSessionHead(pVtab->db, &head);
    if( prollyHashIsEmpty(&head) ) return SQLITE_OK;
    pCur->singleCommit = 0;
  }

  rc = prollyHashSetInit(&pCur->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  pCur->visitedInit = 1;

  pCur->qAlloc = 16;
  pCur->aQueue = sqlite3_malloc(pCur->qAlloc * (int)sizeof(ProllyHash));
  if( !pCur->aQueue ) return SQLITE_NOMEM;
  pCur->aQueue[0] = head;
  pCur->qTail = 1;

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
  int i, iHashEq = -1;
  int idxNum = 0;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->op != SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pC->iColumn == 0 && iHashEq < 0 ){
      iHashEq = i;
    }
  }

  if( iHashEq >= 0 ){
    pInfo->aConstraintUsage[iHashEq].argvIndex = 1;
    pInfo->aConstraintUsage[iHashEq].omit = 1;
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
