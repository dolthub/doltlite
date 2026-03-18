/*
** dolt_at('tablename', 'commit_hash') — point-in-time table query.
**
** Returns the full contents of a table as it existed at a specific commit:
**
**   SELECT * FROM dolt_at('users', 'abc123...');
**   -- rowid_val | value
**
** Also accepts branch names and tag names as the second argument:
**
**   SELECT * FROM dolt_at('users', 'main');
**   SELECT * FROM dolt_at('users', 'v1.0');
**
** This is the SQLite equivalent of Dolt's AS OF clause.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void *doltliteGetBtShared(sqlite3 *db);

struct TableEntry { Pgno iTable; ProllyHash root; ProllyHash schemaHash; u8 flags; char *zName; void *pPending; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* --------------------------------------------------------------------------
** Buffered row from a point-in-time scan
** -------------------------------------------------------------------------- */

typedef struct AtRow AtRow;
struct AtRow {
  i64 intKey;
  u8 *pVal; int nVal;
};

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct AtVtab AtVtab;
struct AtVtab {
  sqlite3_vtab base;
  sqlite3 *db;
};

typedef struct AtCursor AtCursor;
struct AtCursor {
  sqlite3_vtab_cursor base;
  AtRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

static void freeAtRows(AtCursor *c){
  int i;
  for(i=0; i<c->nRows; i++){
    sqlite3_free(c->aRows[i].pVal);
  }
  sqlite3_free(c->aRows);
  c->aRows = 0;
  c->nRows = 0;
  c->nAlloc = 0;
}

/* --------------------------------------------------------------------------
** Resolve a ref string to a commit hash.
** Tries in order: 40-char hex hash, branch name, tag name.
** -------------------------------------------------------------------------- */

static int resolveRef(
  ChunkStore *cs,
  const char *zRef,
  ProllyHash *pCommit
){
  int rc;

  /* Try as 40-char hex hash */
  if( zRef && strlen(zRef)==40 ){
    rc = doltliteHexToHash(zRef, pCommit);
    if( rc==SQLITE_OK ){
      /* Verify it exists */
      if( chunkStoreHas(cs, pCommit) ) return SQLITE_OK;
    }
  }

  /* Try as branch name */
  rc = chunkStoreFindBranch(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ) return SQLITE_OK;

  /* Try as tag name */
  rc = chunkStoreFindTag(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ) return SQLITE_OK;

  return SQLITE_NOTFOUND;
}

/* --------------------------------------------------------------------------
** Find table root by name in a catalog
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
** Scan all rows from a prolly tree root
** -------------------------------------------------------------------------- */

static int scanTree(
  AtCursor *pCur,
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags
){
  ProllyCursor cur;
  int res, rc;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_OK;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){ prollyCursorClose(&cur); return rc; }

  while( prollyCursorIsValid(&cur) ){
    const u8 *pVal; int nVal;
    AtRow *r;

    if( pCur->nRows >= pCur->nAlloc ){
      int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 128;
      AtRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(AtRow));
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

    pCur->nRows++;

    rc = prollyCursorNext(&cur);
    if( rc!=SQLITE_OK ) break;
  }

  prollyCursorClose(&cur);
  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static const char *atSchema =
  "CREATE TABLE x("
  "  rowid_val INTEGER,"
  "  value BLOB,"
  "  table_name TEXT HIDDEN,"
  "  commit_ref TEXT HIDDEN"
  ")";

static int atConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  AtVtab *v;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, atSchema);
  if( rc!=SQLITE_OK ) return rc;
  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;
  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int atDisconnect(sqlite3_vtab *v){
  sqlite3_free(v);
  return SQLITE_OK;
}

static int atBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iTable = -1, iCommit = -1;
  int i, argvIdx = 1;
  (void)pVtab;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pInfo->aConstraint[i].iColumn ){
      case 2: iTable = i; break;
      case 3: iCommit = i; break;
    }
  }

  if( iTable<0 || iCommit<0 ){
    pInfo->estimatedCost = 1e12;
    return SQLITE_OK;
  }

  pInfo->aConstraintUsage[iTable].argvIndex = argvIdx++;
  pInfo->aConstraintUsage[iTable].omit = 1;
  pInfo->aConstraintUsage[iCommit].argvIndex = argvIdx++;
  pInfo->aConstraintUsage[iCommit].omit = 1;
  pInfo->idxNum = 1;
  pInfo->estimatedCost = 1000.0;
  pInfo->estimatedRows = 100;
  return SQLITE_OK;
}

static int atOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  AtCursor *c;
  (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int atClose(sqlite3_vtab_cursor *cur){
  AtCursor *c = (AtCursor*)cur;
  freeAtRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int atFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  AtCursor *c = (AtCursor*)cur;
  AtVtab *v = (AtVtab*)cur->pVtab;
  sqlite3 *db = v->db;
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt;
  ProllyCache *pCache;
  const char *zTable, *zRef;
  ProllyHash commitHash;
  DoltliteCommit commit;
  struct TableEntry *aTables = 0;
  int nTables = 0;
  ProllyHash tableRoot;
  u8 flags = 0;
  u8 *data = 0;
  int nData = 0;
  int rc;
  (void)idxStr;

  freeAtRows(c);
  c->iRow = 0;

  if( !cs || idxNum!=1 || argc<2 ) return SQLITE_OK;

  pBt = doltliteGetBtShared(db);
  if( !pBt ) return SQLITE_OK;
  pCache = (ProllyCache*)(((char*)pBt) + sizeof(ChunkStore));

  zTable = (const char*)sqlite3_value_text(argv[0]);
  zRef = (const char*)sqlite3_value_text(argv[1]);
  if( !zTable || !zRef ) return SQLITE_OK;

  /* Resolve ref to commit hash */
  rc = resolveRef(cs, zRef, &commitHash);
  if( rc!=SQLITE_OK ) return SQLITE_OK; /* Unknown ref = empty result */

  /* Load commit */
  memset(&commit, 0, sizeof(commit));
  rc = chunkStoreGet(cs, &commitHash, &data, &nData);
  if( rc!=SQLITE_OK ) return SQLITE_OK;
  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return SQLITE_OK;

  /* Load catalog and find table */
  rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ) return SQLITE_OK;

  rc = findRootByName(aTables, nTables, zTable, &tableRoot, &flags);
  sqlite3_free(aTables);
  if( rc!=SQLITE_OK ) return SQLITE_OK;

  /* Scan the tree */
  scanTree(c, cs, pCache, &tableRoot, flags);

  return SQLITE_OK;
}

static int atNext(sqlite3_vtab_cursor *cur){
  ((AtCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int atEof(sqlite3_vtab_cursor *cur){
  AtCursor *c = (AtCursor*)cur;
  return c->iRow >= c->nRows;
}

static int atColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  AtCursor *c = (AtCursor*)cur;
  AtRow *r = &c->aRows[c->iRow];

  switch( col ){
    case 0: /* rowid_val */
      sqlite3_result_int64(ctx, r->intKey);
      break;
    case 1: /* value */
      if( r->pVal )
        sqlite3_result_blob(ctx, r->pVal, r->nVal, SQLITE_TRANSIENT);
      else
        sqlite3_result_null(ctx);
      break;
  }
  return SQLITE_OK;
}

static int atRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((AtCursor*)cur)->iRow;
  return SQLITE_OK;
}

static sqlite3_module atModule = {
  0, 0, atConnect, atBestIndex, atDisconnect, 0,
  atOpen, atClose, atFilter, atNext, atEof,
  atColumn, atRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteAtRegister(sqlite3 *db){
  return sqlite3_create_module(db, "dolt_at", &atModule, 0);
}

#endif /* DOLTLITE_PROLLY */
