/*
** Doltlite branch management with per-session branch state.
**
** Each connection tracks its own active branch. The chunk store holds
** the refs (branch → commit mapping) shared across all connections.
** When dolt_checkout is called, the session's branch pointer is updated
** AND the shared BtShared working state is reloaded (safe because
** SQLite serializes writers).
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);

struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  ProllyHash schemaHash;
  u8 flags;
  char *zName;
  void *pPending;
};
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/*
** Check whether there are uncommitted working changes by doing a deep
** per-table comparison (roots + schema hashes) between the working state
** and HEAD.  This is the same approach dolt_status uses.
**
** Returns 1 if dirty, 0 if clean, or -1 on error.
*/
static int checkWorkingDirty(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headCatHash, workingCatHash;
  int rc;

  if( !cs ) return -1;

  /* Get HEAD catalog hash */
  rc = doltliteGetHeadCatalogHash(db, &headCatHash);
  if( rc!=SQLITE_OK ) return -1;
  if( prollyHashIsEmpty(&headCatHash) ) return 0;

  /* Get working catalog hash — serialize current state to a chunk.
  ** With V2 catalog format, the hash is purely data-derived (no aMeta),
  ** so this comparison is reliable: equal hashes ↔ identical data. */
  {
    u8 *catData = 0; int nCatData = 0;
    rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
    if( rc!=SQLITE_OK ) return -1;
    rc = chunkStorePut(cs, catData, nCatData, &workingCatHash);
    sqlite3_free(catData);
    if( rc!=SQLITE_OK ) return -1;
  }

  /* O(1) dirty check: compare catalog hashes */
  return prollyHashCompare(&headCatHash, &workingCatHash) != 0;
}

/* Per-session branch state (in Btree struct) */
extern const char *doltliteGetSessionBranch(sqlite3 *db);
extern void doltliteSetSessionBranch(sqlite3 *db, const char *zBranch);
extern void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
extern void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead);
extern void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged);
extern void doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged);

/* --------------------------------------------------------------------------
** active_branch() — returns this session's current branch name
** -------------------------------------------------------------------------- */
static void activeBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, doltliteGetSessionBranch(db), -1, SQLITE_TRANSIENT);
}

/* --------------------------------------------------------------------------
** dolt_branch('name') — create branch at this session's HEAD
** dolt_branch('-d', 'name') — delete branch
** -------------------------------------------------------------------------- */
static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash head;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  const char *arg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !arg0 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  if( strcmp(arg0, "-d")==0 || strcmp(arg0, "--delete")==0 ){
    if( argc<2 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
    const char *zName = (const char*)sqlite3_value_text(argv[1]);
    if( !zName ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
    if( strcmp(zName, doltliteGetSessionBranch(db))==0 ){
      sqlite3_result_error(ctx, "cannot delete the current branch", -1);
      return;
    }
    rc = chunkStoreDeleteBranch(cs, zName);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch not found", -1); return; }
  }else{
    /* Create branch at this session's HEAD */
    doltliteGetSessionHead(db, &head);
    if( prollyHashIsEmpty(&head) ){
      sqlite3_result_error(ctx, "no commits yet — commit first", -1);
      return;
    }
    rc = chunkStoreAddBranch(cs, arg0, &head);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch already exists", -1); return; }
  }

  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){ sqlite3_result_error_code(ctx, rc); return; }
  sqlite3_result_int(ctx, 0);
}

/*
** Load the target commit, deserialize it, and perform a hard reset to its
** catalog state. On success, writes the commit hash's catalog hash to
** *pCatHash so the caller can use it for session state updates.
*/
static int checkoutLoadAndApply(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCommitHash,
  ProllyHash *pCatHash
){
  DoltliteCommit commit;
  u8 *data = 0;
  int nData = 0;
  int rc;

  rc = chunkStoreGet(cs, pCommitHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ) return rc;

  memcpy(pCatHash, &commit.catalogHash, sizeof(ProllyHash));
  doltliteCommitClear(&commit);

  /* Save current branch's WorkingSet BEFORE hard reset overwrites session */
  {
    extern int doltliteSaveWorkingSet(sqlite3*);
    doltliteSaveWorkingSet(db);
  }

  rc = doltliteHardReset(db, pCatHash);
  return rc;
}

/* --------------------------------------------------------------------------
** dolt_checkout('branch') — switch this session's active branch
** -------------------------------------------------------------------------- */
static void doltCheckoutFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash targetCommit;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
  const char *zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  /* Handle -b flag: create branch then check it out */
  if( strcmp(zBranch, "-b")==0 ){
    ProllyHash head;
    if( argc<2 ){ sqlite3_result_error(ctx, "branch name required after -b", -1); return; }
    zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( !zBranch ){ sqlite3_result_error(ctx, "branch name required after -b", -1); return; }

    doltliteGetSessionHead(db, &head);
    if( prollyHashIsEmpty(&head) ){
      sqlite3_result_error(ctx, "no commits yet — commit first", -1);
      return;
    }
    rc = chunkStoreAddBranch(cs, zBranch, &head);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "branch already exists", -1);
      return;
    }
    rc = chunkStoreSerializeRefs(cs);
    if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    /* Fall through to checkout the newly created branch */
  }

  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    sqlite3_result_int(ctx, 0);
    return;
  }

  rc = chunkStoreFindBranch(cs, zBranch, &targetCommit);
  if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch not found", -1); return; }

  /* Check for uncommitted changes via catalog hash comparison.
  ** With V2 catalogs (no aMeta), hash comparison is reliable. */
  {
    int dirty = checkWorkingDirty(db);
    if( dirty>0 ){
      sqlite3_result_error(ctx, "uncommitted changes — commit or reset first", -1);
      return;
    }
  }

  if( prollyHashIsEmpty(&targetCommit) ){
    sqlite3_result_error(ctx, "target branch has no commits", -1);
    return;
  }

  /* Load target commit's catalog and hard reset */
  {
    ProllyHash catHash;
    rc = checkoutLoadAndApply(db, cs, zBranch, &targetCommit, &catHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "checkout failed", -1);
      return;
    }

    /* Update this session's branch state */
    doltliteSetSessionBranch(db, zBranch);
    doltliteSetSessionHead(db, &targetCommit);

    /* Load the target branch's WorkingSet (staged + merge state).
    ** If the target branch has no saved WorkingSet, defaults to
    ** staged = empty (which means "same as HEAD" = clean). */
    {
      extern int doltliteLoadWorkingSet(sqlite3*, const char*);
      doltliteLoadWorkingSet(db, zBranch);
      /* If no WorkingSet was loaded (staged is empty), set staged = HEAD catalog */
      {
        ProllyHash staged;
        doltliteGetSessionStaged(db, &staged);
        if( prollyHashIsEmpty(&staged) ){
          doltliteSetSessionStaged(db, &catHash);
        }
      }
    }

    /* Update the default branch in the store (for next open) */
    chunkStoreSetDefaultBranch(cs, zBranch);
    chunkStoreSetHeadCommit(cs, &targetCommit);
  }
  {
    extern int doltliteSaveWorkingSet(sqlite3*);
    doltliteSaveWorkingSet(db);
  }
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);

  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_branches virtual table
** -------------------------------------------------------------------------- */
typedef struct BrVtab BrVtab;
struct BrVtab { sqlite3_vtab base; sqlite3 *db; };
typedef struct BrCur BrCur;
struct BrCur { sqlite3_vtab_cursor base; int iRow; };

static int brConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  BrVtab *p; int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(name TEXT, hash TEXT, is_current INTEGER)");
  if( rc!=SQLITE_OK ) return rc;
  p = sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p)); p->db = db;
  *ppVtab = &p->base;
  return SQLITE_OK;
}
static int brDisconnect(sqlite3_vtab *v){ sqlite3_free(v); return SQLITE_OK; }
static int brOpen(sqlite3_vtab *v, sqlite3_vtab_cursor **pp){
  BrCur *c = sqlite3_malloc(sizeof(*c)); (void)v;
  if(!c) return SQLITE_NOMEM; memset(c,0,sizeof(*c)); *pp=&c->base; return SQLITE_OK;
}
static int brClose(sqlite3_vtab_cursor *c){ sqlite3_free(c); return SQLITE_OK; }
static int brFilter(sqlite3_vtab_cursor *c, int n, const char *s, int a, sqlite3_value **v){
  (void)n;(void)s;(void)a;(void)v;
  ((BrCur*)c)->iRow = 0; return SQLITE_OK;
}
static int brNext(sqlite3_vtab_cursor *c){ ((BrCur*)c)->iRow++; return SQLITE_OK; }
static int brEof(sqlite3_vtab_cursor *c){
  BrVtab *v = (BrVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  return !cs || ((BrCur*)c)->iRow >= cs->nBranches;
}
static int brColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  BrVtab *v = (BrVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  struct BranchRef *br;
  if(!cs) return SQLITE_OK;
  br = &cs->aBranches[((BrCur*)c)->iRow];
  switch(col){
    case 0: sqlite3_result_text(ctx, br->zName, -1, SQLITE_TRANSIENT); break;
    case 1: { char h[41]; doltliteHashToHex(&br->commitHash, h);
              sqlite3_result_text(ctx, h, -1, SQLITE_TRANSIENT); break; }
    case 2: sqlite3_result_int(ctx,
              strcmp(br->zName, doltliteGetSessionBranch(v->db))==0 ? 1 : 0); break;
  }
  return SQLITE_OK;
}
static int brRowid(sqlite3_vtab_cursor *c, sqlite3_int64 *r){
  *r=((BrCur*)c)->iRow; return SQLITE_OK;
}
static int brBestIndex(sqlite3_vtab *v, sqlite3_index_info *p){
  (void)v; p->estimatedCost=10; p->estimatedRows=5; return SQLITE_OK;
}
static sqlite3_module brMod = {
  0,0,brConnect,brBestIndex,brDisconnect,0,
  brOpen,brClose,brFilter,brNext,brEof,brColumn,brRowid,
  0,0,0,0,0,0,0,0,0,0,0,0
};

int doltliteBranchRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "dolt_branch", -1, SQLITE_UTF8, 0, doltBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "dolt_checkout", -1, SQLITE_UTF8, 0, doltCheckoutFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_function(db, "active_branch", 0, SQLITE_UTF8, 0, activeBranchFunc, 0, 0);
  if(rc==SQLITE_OK) rc = sqlite3_create_module(db, "dolt_branches", &brMod, 0);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
