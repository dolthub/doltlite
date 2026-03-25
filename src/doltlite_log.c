/*
** dolt_log virtual table: walks the commit chain from HEAD.
**
** Usage:
**   SELECT * FROM dolt_log;
**   SELECT commit_hash, message FROM dolt_log LIMIT 10;
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "chunk_store.h"

#include <time.h>

/* Forward declarations */
typedef struct BtShared BtShared;
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);

/* --------------------------------------------------------------------------
** Virtual table structure
** -------------------------------------------------------------------------- */

typedef struct DoltliteLogVtab DoltliteLogVtab;
struct DoltliteLogVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct DoltliteLogCursor DoltliteLogCursor;
struct DoltliteLogCursor {
  sqlite3_vtab_cursor base;
  ProllyHash currentHash;      /* Hash of current commit */
  DoltliteCommit current;      /* Deserialized current commit */
  int eof;                     /* True if no more commits */
  i64 iRow;                 /* Row counter */
  char zHashHex[PROLLY_HASH_SIZE*2+1]; /* Hex string of currentHash */
};

/* --------------------------------------------------------------------------
** Schema
** -------------------------------------------------------------------------- */

static const char *doltliteLogSchema =
  "CREATE TABLE x("
  "  commit_hash TEXT,"
  "  committer TEXT,"
  "  email TEXT,"
  "  date TEXT,"
  "  message TEXT"
  ")";

/* --------------------------------------------------------------------------
** Load a commit by hash from the chunk store
** -------------------------------------------------------------------------- */

static int loadCommit(sqlite3 *db, const ProllyHash *hash,
                      DoltliteCommit *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(hash) ) return SQLITE_NOTFOUND;

  rc = chunkStoreGet(cs, hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteCommitDeserialize(data, nData, pCommit);
  sqlite3_free(data);
  return rc;
}

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static int doltliteLogConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
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

static int doltliteLogClose(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  doltliteCommitClear(&pCur->current);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int doltliteLogNext(sqlite3_vtab_cursor *pCursor){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  DoltliteLogVtab *pVtab = (DoltliteLogVtab*)pCursor->pVtab;
  ProllyHash parent;
  int rc;

  /* Follow parent pointer */
  memcpy(&parent, &pCur->current.parentHash, sizeof(ProllyHash));
  doltliteCommitClear(&pCur->current);

  if( prollyHashIsEmpty(&parent) ){
    pCur->eof = 1;
    return SQLITE_OK;
  }

  pCur->currentHash = parent;
  doltliteHashToHex(&pCur->currentHash, pCur->zHashHex);
  rc = loadCommit(pVtab->db, &pCur->currentHash, &pCur->current);
  if( rc!=SQLITE_OK ){
    pCur->eof = 1;
    return SQLITE_OK;  /* Treat missing parent as end of log */
  }
  pCur->iRow++;
  return SQLITE_OK;
}

static int doltliteLogFilter(
  sqlite3_vtab_cursor *pCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;
  DoltliteLogVtab *pVtab = (DoltliteLogVtab*)pCursor->pVtab;
  ChunkStore *cs;
  int rc;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  /* Start from HEAD */
  cs = doltliteGetChunkStore(pVtab->db);
  if( !cs ){
    pCur->eof = 1;
    return SQLITE_OK;
  }

  /* Use this session's HEAD, not the shared store's */
  doltliteGetSessionHead(pVtab->db, &pCur->currentHash);
  if( prollyHashIsEmpty(&pCur->currentHash) ){
    pCur->eof = 1;
    return SQLITE_OK;
  }

  doltliteHashToHex(&pCur->currentHash, pCur->zHashHex);
  rc = loadCommit(pVtab->db, &pCur->currentHash, &pCur->current);
  if( rc!=SQLITE_OK ){
    pCur->eof = 1;
    return SQLITE_OK;
  }
  pCur->iRow = 0;
  pCur->eof = 0;
  return SQLITE_OK;
}

static int doltliteLogEof(sqlite3_vtab_cursor *pCursor){
  return ((DoltliteLogCursor*)pCursor)->eof;
}

static int doltliteLogColumn(
  sqlite3_vtab_cursor *pCursor,
  sqlite3_context *ctx,
  int iCol
){
  DoltliteLogCursor *pCur = (DoltliteLogCursor*)pCursor;

  switch( iCol ){
    case 0: /* commit_hash */
      sqlite3_result_text(ctx, pCur->zHashHex, -1, SQLITE_TRANSIENT);
      break;
    case 1: /* committer */
      sqlite3_result_text(ctx, pCur->current.zName ? pCur->current.zName : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 2: /* email */
      sqlite3_result_text(ctx, pCur->current.zEmail ? pCur->current.zEmail : "",
                          -1, SQLITE_TRANSIENT);
      break;
    case 3: /* date */
      {
        time_t t = (time_t)pCur->current.timestamp;
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
    case 4: /* message */
      sqlite3_result_text(ctx, pCur->current.zMessage ? pCur->current.zMessage : "",
                          -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}

static int doltliteLogRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid){
  *pRowid = ((DoltliteLogCursor*)pCursor)->iRow;
  return SQLITE_OK;
}

static int doltliteLogBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 1000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Module definition
** -------------------------------------------------------------------------- */

static sqlite3_module doltliteLogModule = {
  0,                         /* iVersion */
  0,                         /* xCreate (not used for eponymous) */
  doltliteLogConnect,        /* xConnect */
  doltliteLogBestIndex,      /* xBestIndex */
  doltliteLogDisconnect,     /* xDisconnect */
  0,                         /* xDestroy */
  doltliteLogOpen,           /* xOpen */
  doltliteLogClose,          /* xClose */
  doltliteLogFilter,         /* xFilter */
  doltliteLogNext,           /* xNext */
  doltliteLogEof,            /* xEof */
  doltliteLogColumn,         /* xColumn */
  doltliteLogRowid,          /* xRowid */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindFunction */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0,                         /* xShadowName */
  0                          /* xIntegrity */
};

int doltliteLogRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_log", &doltliteLogModule, 0);
}

#endif /* DOLTLITE_PROLLY */
