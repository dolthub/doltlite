/*
** dolt_history_<tablename> — time-travel virtual tables.
**
** Shows every version of every row across all commits:
**
**   SELECT * FROM dolt_history_users;
**   -- rowid | value | commit_hash | committer | commit_date
**
** This is NOT a diff — it shows the actual row data as it existed
** at each point in history.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include "doltlite_record.h"
#include <string.h>
#include <time.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void *doltliteGetBtShared(sqlite3 *db);

struct TableEntry { Pgno iTable; ProllyHash root; u8 flags; char *zName; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* --------------------------------------------------------------------------
** Buffered history row
** -------------------------------------------------------------------------- */

typedef struct HistoryRow HistoryRow;
struct HistoryRow {
  i64 intKey;
  u8 *pVal; int nVal;
  char zCommit[PROLLY_HASH_SIZE*2+1];
  char *zCommitter;
  i64 commitDate;
};

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct HistVtab HistVtab;
struct HistVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
};

typedef struct HistCursor HistCursor;
struct HistCursor {
  sqlite3_vtab_cursor base;
  HistoryRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

static void freeHistoryRows(HistCursor *c){
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aRows[i].pVal);
    sqlite3_free(c->aRows[i].zCommitter);
  }
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
  c->nAlloc = 0;
}

/* --------------------------------------------------------------------------
** Find table root hash by name in a catalog
** -------------------------------------------------------------------------- */

static int findRootByName(
  struct TableEntry *a, int n, const char *zName,
  ProllyHash *pRoot, u8 *pFlags
){
  int i;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ){
      memcpy(pRoot, &a[i].root, sizeof(ProllyHash));
      if( pFlags ) *pFlags = a[i].flags;
      return SQLITE_OK;
    }
  }
  memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  return SQLITE_NOTFOUND;
}

/* --------------------------------------------------------------------------
** Scan all rows from a table at a given root hash and append to cursor
** -------------------------------------------------------------------------- */

static int scanTableAtCommit(
  HistCursor *pCur,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const char *zCommitHex,
  const char *zCommitter,
  i64 commitDate
){
  ProllyCursor cur;
  int res, rc;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){ prollyCursorClose(&cur); return rc; }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    HistoryRow *r;

    if( pCur->nRows >= pCur->nAlloc ){
      int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 128;
      HistoryRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(HistoryRow));
      if( !aNew ){ prollyCursorClose(&cur); return SQLITE_NOMEM; }
      pCur->aRows = aNew;
      pCur->nAlloc = nNew;
    }

    r = &pCur->aRows[pCur->nRows];
    memset(r, 0, sizeof(*r));

    r->intKey = prollyCursorIntKey(&cur);
    prollyCursorValue(&cur, &pVal, &nVal);
    if( pVal && nVal>0 ){
      r->pVal = sqlite3_malloc(nVal);
      if( r->pVal ) memcpy(r->pVal, pVal, nVal);
      r->nVal = nVal;
    }
    memcpy(r->zCommit, zCommitHex, PROLLY_HASH_SIZE*2+1);
    r->zCommitter = sqlite3_mprintf("%s", zCommitter ? zCommitter : "");
    r->commitDate = commitDate;

    pCur->nRows++;

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }

  prollyCursorClose(&cur);
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Walk commit history and scan table at each commit
** -------------------------------------------------------------------------- */

static int walkHistoryAndScan(
  HistCursor *pCur,
  sqlite3 *db,
  const char *zTableName
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt = doltliteGetBtShared(db);
  ProllyCache *pCache;
  ProllyHash curHash;
  int rc;

  if( !cs || !pBt ) return SQLITE_OK;
  pCache = (ProllyCache*)(((char*)pBt) + sizeof(ChunkStore));

  chunkStoreGetHeadCommit(cs, &curHash);

  while( !prollyHashIsEmpty(&curHash) ){
    u8 *data = 0; int nData = 0;
    DoltliteCommit commit;
    ProllyHash tableRoot;
    u8 flags = 0;
    char hexBuf[PROLLY_HASH_SIZE*2+1];

    memset(&commit, 0, sizeof(commit));
    rc = chunkStoreGet(cs, &curHash, &data, &nData);
    if( rc!=SQLITE_OK ) break;
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;

    doltliteHashToHex(&curHash, hexBuf);

    /* Find table in this commit's catalog */
    {
      struct TableEntry *aTables = 0; int nTables = 0;
      rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
      if( rc==SQLITE_OK ){
        if( findRootByName(aTables, nTables, zTableName, &tableRoot, &flags)==SQLITE_OK ){
          scanTableAtCommit(pCur, cs, pCache, &tableRoot, flags,
                            hexBuf, commit.zName, commit.timestamp);
        }
        sqlite3_free(aTables);
      }
    }

    /* Move to parent */
    {
      ProllyHash next;
      memcpy(&next, &commit.parentHash, sizeof(ProllyHash));
      doltliteCommitClear(&commit);
      memcpy(&curHash, &next, sizeof(ProllyHash));
    }
  }

  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static int htConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  HistVtab *v;
  int rc;
  const char *zMod;
  (void)pAux; (void)pzErr;

  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x("
    "  rowid_val INTEGER,"
    "  value TEXT,"
    "  commit_hash TEXT,"
    "  committer TEXT,"
    "  commit_date TEXT"
    ")");
  if( rc!=SQLITE_OK ) return rc;

  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;

  zMod = argv[0];
  if( zMod && strncmp(zMod, "dolt_history_", 13)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zMod + 13);
  }else if( argc > 3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }

  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int htDisconnect(sqlite3_vtab *pVtab){
  HistVtab *v = (HistVtab*)pVtab;
  sqlite3_free(v->zTableName);
  sqlite3_free(v);
  return SQLITE_OK;
}

static int htBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v;
  p->estimatedCost = 100000.0;
  p->estimatedRows = 10000;
  return SQLITE_OK;
}

static int htOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  HistCursor *c;
  (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int htClose(sqlite3_vtab_cursor *cur){
  HistCursor *c = (HistCursor*)cur;
  freeHistoryRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int htFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  HistCursor *c = (HistCursor*)cur;
  HistVtab *v = (HistVtab*)cur->pVtab;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  freeHistoryRows(c);
  c->iRow = 0;
  walkHistoryAndScan(c, v->db, v->zTableName);
  return SQLITE_OK;
}

static int htNext(sqlite3_vtab_cursor *cur){
  ((HistCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int htEof(sqlite3_vtab_cursor *cur){
  HistCursor *c = (HistCursor*)cur;
  return c->iRow >= c->nRows;
}

static int htColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  HistCursor *c = (HistCursor*)cur;
  HistoryRow *r = &c->aRows[c->iRow];

  switch( col ){
    case 0: /* rowid_val */
      sqlite3_result_int64(ctx, r->intKey);
      break;
    case 1: /* value */
      doltliteResultRecord(ctx, r->pVal, r->nVal);
      break;
    case 2: /* commit_hash */
      sqlite3_result_text(ctx, r->zCommit, -1, SQLITE_TRANSIENT);
      break;
    case 3: /* committer */
      sqlite3_result_text(ctx, r->zCommitter, -1, SQLITE_TRANSIENT);
      break;
    case 4: /* commit_date */
      { time_t t = (time_t)r->commitDate; struct tm *tm = gmtime(&t);
        if(tm){ char b[32]; strftime(b,sizeof(b),"%Y-%m-%d %H:%M:%S",tm);
          sqlite3_result_text(ctx,b,-1,SQLITE_TRANSIENT);
        }else sqlite3_result_null(ctx); }
      break;
  }
  return SQLITE_OK;
}

static int htRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((HistCursor*)cur)->iRow;
  return SQLITE_OK;
}

static sqlite3_module historyModule = {
  0,             /* iVersion */
  htConnect,     /* xCreate (eponymous) */
  htConnect,     /* xConnect */
  htBestIndex,   /* xBestIndex */
  htDisconnect,  /* xDisconnect */
  htDisconnect,  /* xDestroy */
  htOpen, htClose, htFilter, htNext, htEof,
  htColumn, htRowid,
  0,0,0,0,0,0,0,0,0,0,0,0  /* remaining */
};

/* --------------------------------------------------------------------------
** Registration: register dolt_history_<tablename> for each user table.
** -------------------------------------------------------------------------- */

void doltliteRegisterHistoryTables(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCommit;
  u8 *data = 0; int nData = 0;
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0, i, rc;

  if( !cs ) return;

  chunkStoreGetHeadCommit(cs, &headCommit);
  if( prollyHashIsEmpty(&headCommit) ) return;

  rc = chunkStoreGet(cs, &headCommit, &data, &nData);
  if( rc!=SQLITE_OK ) return;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return;

  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return;

  for(i=0; i<nTables; i++){
    if( aTables[i].zName && aTables[i].iTable > 1 ){
      char *zMod = sqlite3_mprintf("dolt_history_%s", aTables[i].zName);
      if( zMod ){
        sqlite3_create_module(db, zMod, &historyModule, 0);
        sqlite3_free(zMod);
      }
    }
  }
  sqlite3_free(aTables);
}

#endif /* DOLTLITE_PROLLY */
