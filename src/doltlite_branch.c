
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include <string.h>



static void activeBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, doltliteGetSessionBranch(db), -1, SQLITE_TRANSIENT);
}

static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash head;
  const char *arg0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  arg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !arg0 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  if( strcmp(arg0, "-d")==0 || strcmp(arg0, "--delete")==0 ){
    const char *zName;
    if( argc<2 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
    zName = (const char*)sqlite3_value_text(argv[1]);
    if( !zName ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
    if( strcmp(zName, doltliteGetSessionBranch(db))==0 ){
      sqlite3_result_error(ctx, "cannot delete the current branch", -1);
      return;
    }
    rc = chunkStoreDeleteBranch(cs, zName);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch not found", -1); return; }
  }else{
    
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

  
  {
    extern int doltliteSaveWorkingSet(sqlite3*);
    doltliteSaveWorkingSet(db);
  }

  rc = doltliteHardReset(db, pCatHash);
  return rc;
}

static void doltCheckoutFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash targetCommit;
  const char *zBranch;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  
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
    
  }

  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    sqlite3_result_int(ctx, 0);
    return;
  }

  rc = chunkStoreFindBranch(cs, zBranch, &targetCommit);
  if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch not found", -1); return; }

  if( prollyHashIsEmpty(&targetCommit) ){
    sqlite3_result_error(ctx, "target branch has no commits", -1);
    return;
  }

  
  {
    ProllyHash catHash;
    rc = checkoutLoadAndApply(db, cs, zBranch, &targetCommit, &catHash);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "checkout failed", -1);
      return;
    }

    
    doltliteSetSessionBranch(db, zBranch);
    doltliteSetSessionHead(db, &targetCommit);

    
    {
      extern int doltliteLoadWorkingSet(sqlite3*, const char*);
      doltliteLoadWorkingSet(db, zBranch);
      
      {
        ProllyHash staged;
        doltliteGetSessionStaged(db, &staged);
        if( prollyHashIsEmpty(&staged) ){
          doltliteSetSessionStaged(db, &catHash);
        }
      }
    }

    
    chunkStoreSetDefaultBranch(cs, zBranch);

    {
      extern int doltliteSaveWorkingSet(sqlite3*);
      extern int doltliteUpdateBranchWorkingState(sqlite3*, const char*, const ProllyHash*);
      doltliteSaveWorkingSet(db);
      /* Record the target branch's working catalog in the per-branch working
      ** state so cross-branch connections find the correct catalog on refresh. */
      doltliteUpdateBranchWorkingState(db, zBranch, &catHash);
    }
  }
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);

  sqlite3_result_int(ctx, 0);
}

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

#endif 
