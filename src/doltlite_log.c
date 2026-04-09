/* dolt_log: BFS traversal of all reachable commits, sorted by timestamp desc.
** Follows ALL parents (not just the first), deduplicating by commit hash. */
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "chunk_store.h"

#include "doltlite_internal.h"
#include <string.h>
#include <time.h>

typedef struct DoltliteLogVtab DoltliteLogVtab;
struct DoltliteLogVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct LogEntry LogEntry;
struct LogEntry {
  ProllyHash hash;
  char zHex[PROLLY_HASH_SIZE*2+1];
  DoltliteCommit commit;
};

typedef struct DoltliteLogCursor DoltliteLogCursor;
struct DoltliteLogCursor {
  sqlite3_vtab_cursor base;
  LogEntry *aEntries;
  int nEntries;
  int iCur;
};

static const char *doltliteLogSchema =
  "CREATE TABLE x("
  "  commit_hash TEXT,"
  "  committer TEXT,"
  "  email TEXT,"
  "  date TEXT,"
  "  message TEXT"
  ")";

static int logCollectAll(sqlite3 *db, const ProllyHash *pHead,
                         LogEntry **ppOut, int *pnOut){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 0;
  ProllyHash *visited = 0;
  int nVisited = 0, nVisitedAlloc = 0;
  LogEntry *aEntries = 0;
  int nEntries = 0, nAlloc = 0;
  int rc = SQLITE_OK;
  int i;

  if( !cs || prollyHashIsEmpty(pHead) ){
    *ppOut = 0; *pnOut = 0;
    return SQLITE_OK;
  }

  /* Seed queue */
  qAlloc = 16;
  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ) return SQLITE_NOMEM;
  queue[qTail++] = *pHead;

  while( qHead < qTail ){
    ProllyHash cur = queue[qHead++];
    DoltliteCommit commit;
    LogEntry *pEntry;
    int dup = 0;

    /* Dedup by hash */
    for(i = 0; i < nVisited; i++){
      if( prollyHashCompare(&visited[i], &cur)==0 ){ dup = 1; break; }
    }
    if( dup ) continue;

    /* Add to visited */
    if( nVisited >= nVisitedAlloc ){
      int newAlloc = nVisitedAlloc ? nVisitedAlloc*2 : 16;
      ProllyHash *tmp = sqlite3_realloc(visited, newAlloc*(int)sizeof(ProllyHash));
      if( !tmp ){ rc = SQLITE_NOMEM; break; }
      visited = tmp; nVisitedAlloc = newAlloc;
    }
    visited[nVisited++] = cur;

    /* Load commit */
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &cur, &commit);
    if( rc!=SQLITE_OK ) break;

    /* Add to results */
    if( nEntries >= nAlloc ){
      int newAlloc = nAlloc ? nAlloc*2 : 16;
      LogEntry *tmp = sqlite3_realloc(aEntries, newAlloc*(int)sizeof(LogEntry));
      if( !tmp ){
        doltliteCommitClear(&commit);
        rc = SQLITE_NOMEM; break;
      }
      aEntries = tmp; nAlloc = newAlloc;
    }
    pEntry = &aEntries[nEntries++];
    pEntry->hash = cur;
    doltliteHashToHex(&cur, pEntry->zHex);
    pEntry->commit = commit;

    /* Enqueue all parents */
    for(i = 0; i < commit.nParents; i++){
      if( prollyHashIsEmpty(&commit.aParents[i]) ) continue;
      if( qTail >= qAlloc ){
        int newAlloc = qAlloc*2;
        ProllyHash *tmp = sqlite3_realloc(queue, newAlloc*(int)sizeof(ProllyHash));
        if( !tmp ){ rc = SQLITE_NOMEM; break; }
        queue = tmp; qAlloc = newAlloc;
      }
      queue[qTail++] = commit.aParents[i];
    }
    if( rc!=SQLITE_OK ) break;
  }

  sqlite3_free(queue);
  sqlite3_free(visited);

  if( rc!=SQLITE_OK ){
    for(i = 0; i < nEntries; i++) doltliteCommitClear(&aEntries[i].commit);
    sqlite3_free(aEntries);
    *ppOut = 0; *pnOut = 0;
    return rc;
  }

  /* BFS order is topological: each commit before its parents */
  *ppOut = aEntries;
  *pnOut = nEntries;
  return SQLITE_OK;
}

static int doltliteLogConnect(
  sqlite3 *db, void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab, char **pzErr
){
  DoltliteLogVtab *pVtab;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;

  rc = sqlite3_declare_vtab(db, doltliteLogSchema);
  if( rc!=SQLITE_OK ) return rc;

  pVtab = sqlite3_malloc(sizeof(*pVtab));
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;

  *ppVtab = &pVtab->base;
  return SQLITE_OK;
}

static int doltliteLogDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int doltliteLogOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  DoltliteLogCursor *pCur;
  (void)pVtab;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static void logCursorFree(DoltliteLogCursor *pCur){
  int i;
  for(i = 0; i < pCur->nEntries; i++){
    doltliteCommitClear(&pCur->aEntries[i].commit);
  }
  sqlite3_free(pCur->aEntries);
  pCur->aEntries = 0;
  pCur->nEntries = 0;
}

static int doltliteLogClose(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  logCursorFree(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int doltliteLogNext(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  pCur->iCur++;
  return SQLITE_OK;
}

static int doltliteLogFilter(
  sqlite3_vtab_cursor *pCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  DoltliteLogVtab *pVtab = (DoltliteLogVtab*)pCursor->pVtab;
  ProllyHash head;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  logCursorFree(pCur);
  pCur->iCur = 0;

  doltliteGetSessionHead(pVtab->db, &head);
  return logCollectAll(pVtab->db, &head, &pCur->aEntries, &pCur->nEntries);
}

static int doltliteLogEof(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  return pCur->iCur >= pCur->nEntries;
}

static int doltliteLogColumn(
  sqlite3_vtab_cursor *pCursor,
  sqlite3_context *ctx,
  int iCol
){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  LogEntry *e;

  if( pCur->iCur >= pCur->nEntries ) return SQLITE_OK;
  e = &pCur->aEntries[pCur->iCur];

  switch( iCol ){
    case 0:
      sqlite3_result_text(ctx, e->zHex, -1, SQLITE_TRANSIENT);
      break;
    case 1:
      sqlite3_result_text(ctx, e->commit.zName ? e->commit.zName : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 2:
      sqlite3_result_text(ctx, e->commit.zEmail ? e->commit.zEmail : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 3:
      {
        time_t t = (time_t)e->commit.timestamp;
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
    case 4:
      sqlite3_result_text(ctx, e->commit.zMessage ? e->commit.zMessage : "",
                          -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}

static int doltliteLogRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteLogCursor*)pCursor)->iCur;
  return SQLITE_OK;
}

static int doltliteLogBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 1000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

static sqlite3_module doltliteLogModule = {
  0, 0,
  doltliteLogConnect, doltliteLogBestIndex, doltliteLogDisconnect, 0,
  doltliteLogOpen, doltliteLogClose, doltliteLogFilter, doltliteLogNext,
  doltliteLogEof, doltliteLogColumn, doltliteLogRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

int doltliteLogRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_log", &doltliteLogModule, 0);
}

#endif
