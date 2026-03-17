/*
** dolt_diff_<tablename> — per-table audit log virtual tables.
**
** Walks the full commit history and produces row-level diffs between
** each consecutive pair of commits, annotated with commit context:
**
**   SELECT * FROM dolt_diff_users;
**   -- diff_type | rowid_val | from_value | to_value |
**   --   from_commit | to_commit | from_commit_date | to_commit_date
**
** Filter on commit range:
**   SELECT * FROM dolt_diff_users
**   WHERE from_commit = 'abc...' AND to_commit = 'def...';
**
** These tables are dynamically registered for each user table.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_diff.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern void *doltliteGetBtShared(sqlite3 *db);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);

struct TableEntry { Pgno iTable; ProllyHash root; u8 flags; char *zName; };
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/* --------------------------------------------------------------------------
** Buffered audit row — one row-level change with commit context
** -------------------------------------------------------------------------- */

typedef struct AuditRow AuditRow;
struct AuditRow {
  u8 diffType;
  i64 intKey;
  u8 *pOldVal; int nOldVal;
  u8 *pNewVal; int nNewVal;
  char zFromCommit[PROLLY_HASH_SIZE*2+1];
  char zToCommit[PROLLY_HASH_SIZE*2+1];
  i64 fromDate;
  i64 toDate;
};

/* --------------------------------------------------------------------------
** Virtual table structures
** -------------------------------------------------------------------------- */

typedef struct DiffTblVtab DiffTblVtab;
struct DiffTblVtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zTableName;
};

typedef struct DiffTblCursor DiffTblCursor;
struct DiffTblCursor {
  sqlite3_vtab_cursor base;
  AuditRow *aRows;
  int nRows;
  int nAlloc;
  int iRow;
};

/* --------------------------------------------------------------------------
** Diff callback: collect rows into cursor
** -------------------------------------------------------------------------- */

typedef struct CollectCtx CollectCtx;
struct CollectCtx {
  DiffTblCursor *pCur;
  char *zFromCommit;
  char *zToCommit;
  i64 fromDate;
  i64 toDate;
};

static int auditDiffCollect(void *pCtx, const ProllyDiffChange *pChange){
  CollectCtx *c = (CollectCtx*)pCtx;
  DiffTblCursor *pCur = c->pCur;
  AuditRow *r;

  if( pCur->nRows >= pCur->nAlloc ){
    int nNew = pCur->nAlloc ? pCur->nAlloc*2 : 64;
    AuditRow *aNew = sqlite3_realloc(pCur->aRows, nNew*(int)sizeof(AuditRow));
    if( !aNew ) return SQLITE_NOMEM;
    pCur->aRows = aNew;
    pCur->nAlloc = nNew;
  }

  r = &pCur->aRows[pCur->nRows];
  memset(r, 0, sizeof(*r));
  r->diffType = pChange->type;
  r->intKey = pChange->intKey;

  if( pChange->pOldVal && pChange->nOldVal>0 ){
    r->pOldVal = sqlite3_malloc(pChange->nOldVal);
    if( r->pOldVal ) memcpy(r->pOldVal, pChange->pOldVal, pChange->nOldVal);
    r->nOldVal = pChange->nOldVal;
  }
  if( pChange->pNewVal && pChange->nNewVal>0 ){
    r->pNewVal = sqlite3_malloc(pChange->nNewVal);
    if( r->pNewVal ) memcpy(r->pNewVal, pChange->pNewVal, pChange->nNewVal);
    r->nNewVal = pChange->nNewVal;
  }

  memcpy(r->zFromCommit, c->zFromCommit, PROLLY_HASH_SIZE*2+1);
  memcpy(r->zToCommit, c->zToCommit, PROLLY_HASH_SIZE*2+1);
  r->fromDate = c->fromDate;
  r->toDate = c->toDate;

  pCur->nRows++;
  return SQLITE_OK;
}

static void freeAuditRows(DiffTblCursor *pCur){
  int i;
  for(i=0; i<pCur->nRows; i++){
    sqlite3_free(pCur->aRows[i].pOldVal);
    sqlite3_free(pCur->aRows[i].pNewVal);
  }
  sqlite3_free(pCur->aRows);
  pCur->aRows = 0;
  pCur->nRows = 0;
  pCur->nAlloc = 0;
}

/* --------------------------------------------------------------------------
** Find table root hash by name in a catalog
** -------------------------------------------------------------------------- */

static int findTableRootByName(
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
** Walk commit history and diff each consecutive pair
** -------------------------------------------------------------------------- */

static int walkHistoryAndDiff(
  DiffTblCursor *pCur,
  sqlite3 *db,
  const char *zTableName,
  const char *zFilterFrom,  /* Optional: only this commit pair */
  const char *zFilterTo
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  void *pBt = doltliteGetBtShared(db);
  ProllyCache *pCache;
  ProllyHash headHash;
  ProllyHash curHash;
  int rc;

  if( !cs || !pBt ) return SQLITE_OK;
  pCache = (ProllyCache*)(((char*)pBt) + sizeof(ChunkStore));

  /* Get HEAD commit */
  chunkStoreGetHeadCommit(cs, &headHash);
  if( prollyHashIsEmpty(&headHash) ) return SQLITE_OK;

  memcpy(&curHash, &headHash, sizeof(ProllyHash));

  /* Walk commit chain: cur → parent → parent → ... */
  while( !prollyHashIsEmpty(&curHash) ){
    u8 *data = 0; int nData = 0;
    DoltliteCommit commit;
    ProllyHash curRoot, parentRoot;
    u8 flags = 0;
    char curHex[PROLLY_HASH_SIZE*2+1];
    char parentHex[PROLLY_HASH_SIZE*2+1];

    memset(&commit, 0, sizeof(commit));

    rc = chunkStoreGet(cs, &curHash, &data, &nData);
    if( rc!=SQLITE_OK ) break;
    rc = doltliteCommitDeserialize(data, nData, &commit);
    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;

    doltliteHashToHex(&curHash, curHex);

    /* Load table root from this commit's catalog */
    {
      struct TableEntry *aTables = 0; int nTables = 0;
      rc = doltliteLoadCatalog(db, &commit.catalogHash, &aTables, &nTables, 0);
      if( rc==SQLITE_OK ){
        findTableRootByName(aTables, nTables, zTableName, &curRoot, &flags);
        sqlite3_free(aTables);
      }else{
        memset(&curRoot, 0, sizeof(curRoot));
      }
    }

    /* If this commit has a parent, diff parent→current */
    if( !prollyHashIsEmpty(&commit.parentHash) ){
      DoltliteCommit parentCommit;
      u8 *pdata = 0; int npdata = 0;

      memset(&parentCommit, 0, sizeof(parentCommit));
      rc = chunkStoreGet(cs, &commit.parentHash, &pdata, &npdata);
      if( rc==SQLITE_OK ){
        rc = doltliteCommitDeserialize(pdata, npdata, &parentCommit);
        sqlite3_free(pdata);
      }

      if( rc==SQLITE_OK ){
        doltliteHashToHex(&commit.parentHash, parentHex);

        /* Load parent table root */
        {
          struct TableEntry *aPTables = 0; int nPTables = 0;
          rc = doltliteLoadCatalog(db, &parentCommit.catalogHash,
                                    &aPTables, &nPTables, 0);
          if( rc==SQLITE_OK ){
            findTableRootByName(aPTables, nPTables, zTableName,
                                &parentRoot, 0);
            sqlite3_free(aPTables);
          }else{
            memset(&parentRoot, 0, sizeof(parentRoot));
          }
        }

        /* Apply commit filter if specified */
        {
          int skip = 0;
          if( zFilterFrom && zFilterTo ){
            if( strcmp(parentHex, zFilterFrom)!=0
             || strcmp(curHex, zFilterTo)!=0 ){
              skip = 1;
            }
          }

          if( !skip && prollyHashCompare(&parentRoot, &curRoot)!=0 ){
            CollectCtx ctx;
            ctx.pCur = pCur;
            ctx.zFromCommit = parentHex;
            ctx.zToCommit = curHex;
            ctx.fromDate = parentCommit.timestamp;
            ctx.toDate = commit.timestamp;

            prollyDiff(cs, pCache, &parentRoot, &curRoot, flags,
                       auditDiffCollect, &ctx);
          }
        }

        doltliteCommitClear(&parentCommit);
      }
    }else{
      /* Initial commit — diff from empty tree */
      if( !prollyHashIsEmpty(&curRoot) ){
        ProllyHash emptyRoot;
        int skip = 0;
        memset(&emptyRoot, 0, sizeof(emptyRoot));
        memset(parentHex, '0', PROLLY_HASH_SIZE*2);
        parentHex[PROLLY_HASH_SIZE*2] = 0;

        if( zFilterFrom && zFilterTo ){
          if( strcmp(curHex, zFilterTo)!=0 ) skip = 1;
        }

        if( !skip ){
          CollectCtx ctx;
          ctx.pCur = pCur;
          ctx.zFromCommit = parentHex;
          ctx.zToCommit = curHex;
          ctx.fromDate = 0;
          ctx.toDate = commit.timestamp;

          prollyDiff(cs, pCache, &emptyRoot, &curRoot, flags,
                     auditDiffCollect, &ctx);
        }
      }
    }

    /* Move to parent */
    {
      ProllyHash nextHash;
      memcpy(&nextHash, &commit.parentHash, sizeof(ProllyHash));
      doltliteCommitClear(&commit);
      memcpy(&curHash, &nextHash, sizeof(ProllyHash));
    }
  }

  return SQLITE_OK;
}

/* --------------------------------------------------------------------------
** Virtual table methods
** -------------------------------------------------------------------------- */

static int dtConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  DiffTblVtab *v;
  int rc;
  const char *zModName;
  (void)pAux; (void)pzErr;

  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x("
    "  diff_type TEXT,"
    "  rowid_val INTEGER,"
    "  from_value BLOB,"
    "  to_value BLOB,"
    "  from_commit TEXT,"
    "  to_commit TEXT,"
    "  from_commit_date INTEGER,"
    "  to_commit_date INTEGER"
    ")");
  if( rc!=SQLITE_OK ) return rc;

  v = sqlite3_malloc(sizeof(*v));
  if( !v ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  v->db = db;

  /* Extract table name from module name: "dolt_diff_<tablename>" */
  zModName = argv[0];
  if( zModName && strncmp(zModName, "dolt_diff_", 10)==0 ){
    v->zTableName = sqlite3_mprintf("%s", zModName + 10);
  }else if( argc > 3 ){
    v->zTableName = sqlite3_mprintf("%s", argv[3]);
  }else{
    v->zTableName = sqlite3_mprintf("");
  }

  *ppVtab = &v->base;
  return SQLITE_OK;
}

static int dtDisconnect(sqlite3_vtab *pVtab){
  DiffTblVtab *v = (DiffTblVtab*)pVtab;
  sqlite3_free(v->zTableName);
  sqlite3_free(v);
  return SQLITE_OK;
}

static int dtBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab;
  pInfo->estimatedCost = 10000.0;
  pInfo->estimatedRows = 1000;
  return SQLITE_OK;
}

static int dtOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **pp){
  DiffTblCursor *c;
  (void)pVtab;
  c = sqlite3_malloc(sizeof(*c));
  if( !c ) return SQLITE_NOMEM;
  memset(c, 0, sizeof(*c));
  *pp = &c->base;
  return SQLITE_OK;
}

static int dtClose(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  freeAuditRows(c);
  sqlite3_free(c);
  return SQLITE_OK;
}

static int dtFilter(sqlite3_vtab_cursor *cur,
    int idxNum, const char *idxStr, int argc, sqlite3_value **argv){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  DiffTblVtab *v = (DiffTblVtab*)cur->pVtab;
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;

  freeAuditRows(c);
  c->iRow = 0;

  walkHistoryAndDiff(c, v->db, v->zTableName, 0, 0);
  return SQLITE_OK;
}

static int dtNext(sqlite3_vtab_cursor *cur){
  ((DiffTblCursor*)cur)->iRow++;
  return SQLITE_OK;
}

static int dtEof(sqlite3_vtab_cursor *cur){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  return c->iRow >= c->nRows;
}

static int dtColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int col){
  DiffTblCursor *c = (DiffTblCursor*)cur;
  AuditRow *r = &c->aRows[c->iRow];

  switch( col ){
    case 0: /* diff_type */
      switch( r->diffType ){
        case PROLLY_DIFF_ADD:    sqlite3_result_text(ctx,"added",-1,SQLITE_STATIC); break;
        case PROLLY_DIFF_DELETE: sqlite3_result_text(ctx,"removed",-1,SQLITE_STATIC); break;
        case PROLLY_DIFF_MODIFY: sqlite3_result_text(ctx,"modified",-1,SQLITE_STATIC); break;
      }
      break;
    case 1: /* rowid_val */
      sqlite3_result_int64(ctx, r->intKey);
      break;
    case 2: /* from_value */
      if( r->pOldVal )
        sqlite3_result_blob(ctx, r->pOldVal, r->nOldVal, SQLITE_TRANSIENT);
      else
        sqlite3_result_null(ctx);
      break;
    case 3: /* to_value */
      if( r->pNewVal )
        sqlite3_result_blob(ctx, r->pNewVal, r->nNewVal, SQLITE_TRANSIENT);
      else
        sqlite3_result_null(ctx);
      break;
    case 4: /* from_commit */
      sqlite3_result_text(ctx, r->zFromCommit, -1, SQLITE_TRANSIENT);
      break;
    case 5: /* to_commit */
      sqlite3_result_text(ctx, r->zToCommit, -1, SQLITE_TRANSIENT);
      break;
    case 6: /* from_commit_date */
      sqlite3_result_int64(ctx, r->fromDate);
      break;
    case 7: /* to_commit_date */
      sqlite3_result_int64(ctx, r->toDate);
      break;
  }
  return SQLITE_OK;
}

static int dtRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *r){
  *r = ((DiffTblCursor*)cur)->iRow;
  return SQLITE_OK;
}

static sqlite3_module diffTableModule = {
  0,                /* iVersion */
  dtConnect,        /* xCreate (eponymous) */
  dtConnect,        /* xConnect */
  dtBestIndex,      /* xBestIndex */
  dtDisconnect,     /* xDisconnect */
  dtDisconnect,     /* xDestroy */
  dtOpen,           /* xOpen */
  dtClose,          /* xClose */
  dtFilter,         /* xFilter */
  dtNext,           /* xNext */
  dtEof,            /* xEof */
  dtColumn,         /* xColumn */
  dtRowid,          /* xRowid */
  0,0,0,0,0,0,0,0,0,0,0,0  /* remaining */
};

/* --------------------------------------------------------------------------
** Registration: register dolt_diff_<tablename> for each user table.
** Called at database open and after schema changes.
** -------------------------------------------------------------------------- */

void doltliteRegisterDiffTables(sqlite3 *db){
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
      char *zModName = sqlite3_mprintf("dolt_diff_%s", aTables[i].zName);
      if( zModName ){
        sqlite3_create_module(db, zModName, &diffTableModule, 0);
        sqlite3_free(zModName);
      }
    }
  }
  sqlite3_free(aTables);
}

#endif /* DOLTLITE_PROLLY */
