
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

typedef struct BranchMutationCtx BranchMutationCtx;
struct BranchMutationCtx {
  const char *zName;
  ProllyHash head;
  int isDelete;
};

static int mutateBranchRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMutationCtx *p = (BranchMutationCtx*)pArg;
  int rc;

  if( p->isDelete ){
    ProllyHash empty;
    rc = chunkStoreDeleteBranch(cs, p->zName);
    if( rc!=SQLITE_OK ) return rc;
    memset(&empty, 0, sizeof(empty));
    return doltliteUpdateBranchWorkingState(db, p->zName, &empty, NULL);
  }

  return chunkStoreAddBranch(cs, p->zName, &p->head);
}

static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  BranchMutationCtx m;
  const char *arg0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  memset(&m, 0, sizeof(m));

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
    m.zName = zName;
    m.isDelete = 1;
    rc = doltliteMutateRefs(db, mutateBranchRef, &m);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch not found", -1); return; }
  }else{
    doltliteGetSessionHead(db, &m.head);
    if( prollyHashIsEmpty(&m.head) ){
      sqlite3_result_error(ctx, "no commits yet — commit first", -1);
      return;
    }
    m.zName = arg0;
    rc = doltliteMutateRefs(db, mutateBranchRef, &m);
    if( rc!=SQLITE_OK ){ sqlite3_result_error(ctx, "branch already exists", -1); return; }
  }

  sqlite3_result_int(ctx, 0);
}

static int checkoutLoadAndApply(
  sqlite3 *db,
  ChunkStore *cs,
  const char *zBranch,
  ProllyHash *pCommitHash,
  ProllyHash *pCatHash
){
  int rc;
  ProllyHash committedCatHash;

  /* Load the committed catalog for this branch. */
  {
    DoltliteCommit commit;

    rc = doltliteLoadCommit(db, pCommitHash, &commit);
    if( rc!=SQLITE_OK ) return rc;

    memcpy(&committedCatHash, &commit.catalogHash, sizeof(ProllyHash));
    doltliteCommitClear(&commit);
  }

  /* Check working state for uncommitted changes. If the stored commitHash
  ** matches the branch's current commit AND the stored catHash differs
  ** from the committed catalog, the working state has uncommitted changes
  ** that should be preserved. */
  {
    ProllyHash wsCatHash, wsCommitHash;
    memset(&wsCatHash, 0, sizeof(wsCatHash));
    memset(&wsCommitHash, 0, sizeof(wsCommitHash));
    if( chunkStoreReadBranchWorkingCatalog(cs, zBranch, &wsCatHash, &wsCommitHash)==SQLITE_OK
     && !prollyHashIsEmpty(&wsCommitHash)
     && memcmp(wsCommitHash.data, pCommitHash->data, PROLLY_HASH_SIZE)==0
     && memcmp(wsCatHash.data, committedCatHash.data, PROLLY_HASH_SIZE)!=0 ){
      /* Working state has uncommitted changes — use it. */
      memcpy(pCatHash, &wsCatHash, sizeof(ProllyHash));
      rc = doltliteSwitchCatalog(db, pCatHash);
      return rc;
    }
  }

  memcpy(pCatHash, &committedCatHash, sizeof(ProllyHash));
  rc = doltliteSwitchCatalog(db, pCatHash);
  return rc;
}

typedef struct CheckoutMutationCtx CheckoutMutationCtx;
struct CheckoutMutationCtx {
  const char *zTargetBranch;
  const char *zCurrentBranch;
  ProllyHash oldCatHash;
  ProllyHash oldCommitHash;
  ProllyHash targetCommit;
  ProllyHash targetCatHash;
  int haveOldState;
};

static int checkoutMutateRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  CheckoutMutationCtx *p = (CheckoutMutationCtx*)pArg;
  int rc;

  rc = chunkStoreFindBranch(cs, p->zTargetBranch, &p->targetCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&p->targetCommit) ) return SQLITE_EMPTY;

  rc = checkoutLoadAndApply(db, cs, p->zTargetBranch,
                            &p->targetCommit, &p->targetCatHash);
  if( rc!=SQLITE_OK ) return rc;

  doltliteSetSessionBranch(db, p->zTargetBranch);
  doltliteSetSessionHead(db, &p->targetCommit);

  rc = doltliteLoadWorkingSet(db, p->zTargetBranch);
  if( rc!=SQLITE_OK ) return rc;

  {
    ProllyHash staged;
    doltliteGetSessionStaged(db, &staged);
    if( prollyHashIsEmpty(&staged) ){
      doltliteSetSessionStaged(db, &p->targetCatHash);
    }
  }

  rc = chunkStoreSetDefaultBranch(cs, p->zTargetBranch);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;

  if( p->haveOldState ){
    rc = doltliteUpdateBranchWorkingState(db, p->zCurrentBranch,
                                          &p->oldCatHash, &p->oldCommitHash);
    if( rc!=SQLITE_OK ) return rc;
  }

  return doltliteUpdateBranchWorkingState(db, p->zTargetBranch,
                                          &p->targetCatHash, &p->targetCommit);
}

static void doltCheckoutFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  CheckoutMutationCtx m;
  BranchMutationCtx branchCreate;
  const char *zBranch;
  char *zCurrentBranch = 0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
  zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  memset(&m, 0, sizeof(m));
  memset(&branchCreate, 0, sizeof(branchCreate));

  
  if( strcmp(zBranch, "-b")==0 ){
    if( argc<2 ){ sqlite3_result_error(ctx, "branch name required after -b", -1); return; }
    zBranch = (const char*)sqlite3_value_text(argv[1]);
    if( !zBranch ){ sqlite3_result_error(ctx, "branch name required after -b", -1); return; }

    doltliteGetSessionHead(db, &branchCreate.head);
    if( prollyHashIsEmpty(&branchCreate.head) ){
      sqlite3_result_error(ctx, "no commits yet — commit first", -1);
      return;
    }
    branchCreate.zName = zBranch;
    rc = doltliteMutateRefs(db, mutateBranchRef, &branchCreate);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "branch already exists", -1);
      return;
    }
  }

  if( strcmp(zBranch, doltliteGetSessionBranch(db))==0 ){
    sqlite3_result_int(ctx, 0);
    return;
  }

  /* Block checkout during unresolved merge conflicts. */
  {
    u8 isMerging = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( isMerging ){
      sqlite3_result_error(ctx, "unresolved merge conflicts \xe2\x80\x94 commit or abort first", -1);
      return;
    }
  }

  /* Save the current branch's working catalog before hardReset overwrites
  ** it with the target branch's catalog. */
  {
    u8 *oldCatData = 0; int nOldCat = 0;
    doltliteGetSessionHead(db, &m.oldCommitHash);
    zCurrentBranch = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
    if( !zCurrentBranch ){
      sqlite3_result_error_nomem(ctx);
      return;
    }
    if( doltliteFlushAndSerializeCatalog(db, &oldCatData, &nOldCat)==SQLITE_OK ){
      rc = chunkStorePut(cs, oldCatData, nOldCat, &m.oldCatHash);
      sqlite3_free(oldCatData);
      if( rc==SQLITE_OK ) m.haveOldState = 1;
    }
  }

  m.zTargetBranch = zBranch;
  m.zCurrentBranch = zCurrentBranch;
  rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
  sqlite3_free(zCurrentBranch);
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_error(ctx, "branch not found", -1);
    return;
  }
  if( rc==SQLITE_EMPTY ){
    sqlite3_result_error(ctx, "target branch has no commits", -1);
    return;
  }
  if( rc==SQLITE_BUSY ){
    sqlite3_result_error(ctx, "database is locked by another connection", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "checkout failed", -1);
    return;
  }
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
