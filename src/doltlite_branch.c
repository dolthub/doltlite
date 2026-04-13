
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include <string.h>
#include <time.h>



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
  int force;
};

static void branchResultError(
  sqlite3_context *ctx,
  int rc,
  const char *zNotFound,
  const char *zExists
){
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_error(ctx, zNotFound, -1);
  }else if( rc==SQLITE_ERROR && zExists ){
    sqlite3_result_error(ctx, zExists, -1);
  }else{
    sqlite3_result_error(ctx, sqlite3_errstr(rc), -1);
  }
}

static int mutateBranchRef(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMutationCtx *p = (BranchMutationCtx*)pArg;
  int rc;

  if( p->isDelete ){
    (void)db;
    return chunkStoreDeleteBranch(cs, p->zName);
  }

  if( p->force && chunkStoreFindBranch(cs, p->zName, 0)==SQLITE_OK ){
    return chunkStoreUpdateBranch(cs, p->zName, &p->head);
  }
  return chunkStoreAddBranch(cs, p->zName, &p->head);
}

/* Copy context: look up source branch's head, add destination at that head. */
typedef struct BranchCopyCtx BranchCopyCtx;
struct BranchCopyCtx {
  const char *zSrc;
  const char *zDest;
  int force;
};

static int mutateBranchCopy(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchCopyCtx *p = (BranchCopyCtx*)pArg;
  ProllyHash srcCommit;
  int rc;
  (void)db;

  rc = chunkStoreFindBranch(cs, p->zSrc, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( p->force ){
    /* Force-create: update if exists, add otherwise. */
    if( chunkStoreFindBranch(cs, p->zDest, 0)==SQLITE_OK ){
      return chunkStoreUpdateBranch(cs, p->zDest, &srcCommit);
    }
  }
  return chunkStoreAddBranch(cs, p->zDest, &srcCommit);
}

/* Move context: atomically rename src to dest. If the current session
** branch is being renamed, the caller updates it after a successful
** mutation. */
typedef struct BranchMoveCtx BranchMoveCtx;
struct BranchMoveCtx {
  const char *zSrc;
  const char *zDest;
};

static int mutateBranchMove(sqlite3 *db, ChunkStore *cs, void *pArg){
  BranchMoveCtx *p = (BranchMoveCtx*)pArg;
  ProllyHash srcCommit, srcWorkingSet;
  int rc;
  (void)db;

  rc = chunkStoreFindBranch(cs, p->zSrc, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreGetBranchWorkingSet(cs, p->zSrc, &srcWorkingSet);
  if( rc!=SQLITE_OK ) memset(&srcWorkingSet, 0, sizeof(srcWorkingSet));
  rc = chunkStoreAddBranch(cs, p->zDest, &srcCommit);
  if( rc!=SQLITE_OK ) return rc;
  if( !prollyHashIsEmpty(&srcWorkingSet) ){
    rc = chunkStoreSetBranchWorkingSet(cs, p->zDest, &srcWorkingSet);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = chunkStoreDeleteBranch(cs, p->zSrc);
  return rc;
}

static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  enum { MODE_CREATE, MODE_DELETE, MODE_COPY, MODE_MOVE } mode = MODE_CREATE;
  int force = 0;
  const char *aPositional[3] = {0, 0, 0};
  int nPositional = 0;
  int i, rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "dolt_branch requires arguments", -1); return; }

  /* Parse flags. Multiple mode flags is an error. */
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-d")==0 || strcmp(arg, "--delete")==0 ){
      if( mode!=MODE_CREATE ){
        sqlite3_result_error(ctx, "conflicting flags", -1); return;
      }
      mode = MODE_DELETE;
    }else if( strcmp(arg, "-D")==0 ){
      /* Dolt's -D is delete + force; doltlite has no unmerged-branch
      ** safety check, so -D is equivalent to -d here. Keeping the flag
      ** separately so unknown-flag rejection stays strict. */
      if( mode!=MODE_CREATE ){
        sqlite3_result_error(ctx, "conflicting flags", -1); return;
      }
      mode = MODE_DELETE;
      force = 1;
    }else if( strcmp(arg, "-c")==0 || strcmp(arg, "--copy")==0 ){
      if( mode!=MODE_CREATE ){
        sqlite3_result_error(ctx, "conflicting flags", -1); return;
      }
      mode = MODE_COPY;
    }else if( strcmp(arg, "-m")==0 || strcmp(arg, "--move")==0 ){
      if( mode!=MODE_CREATE ){
        sqlite3_result_error(ctx, "conflicting flags", -1); return;
      }
      mode = MODE_MOVE;
    }else if( strcmp(arg, "-f")==0 || strcmp(arg, "--force")==0 ){
      force = 1;
    }else if( arg[0]=='-' ){
      char *zErr = sqlite3_mprintf("unknown option `%s`", arg);
      if( zErr ){
        sqlite3_result_error(ctx, zErr, -1);
        sqlite3_free(zErr);
      }else{
        sqlite3_result_error_nomem(ctx);
      }
      return;
    }else{
      if( nPositional >= 3 ){
        sqlite3_result_error(ctx, "too many arguments", -1); return;
      }
      aPositional[nPositional++] = arg;
    }
  }

  switch( mode ){
    case MODE_DELETE: {
      BranchMutationCtx m;
      if( nPositional<1 ){
        sqlite3_result_error(ctx, "branch name required", -1); return;
      }
      if( strcmp(aPositional[0], doltliteGetSessionBranch(db))==0 ){
        sqlite3_result_error(ctx, "cannot delete the current branch", -1);
        return;
      }
      memset(&m, 0, sizeof(m));
      m.zName = aPositional[0];
      m.isDelete = 1;
      rc = doltliteMutateRefs(db, mutateBranchRef, &m);
      if( rc!=SQLITE_OK ){
        branchResultError(ctx, rc, "branch not found", 0);
        return;
      }
      break;
    }

    case MODE_COPY: {
      BranchCopyCtx m;
      if( nPositional<2 ){
        sqlite3_result_error(ctx, "copy requires source and destination", -1);
        return;
      }
      memset(&m, 0, sizeof(m));
      m.zSrc = aPositional[0];
      m.zDest = aPositional[1];
      m.force = force;
      rc = doltliteMutateRefs(db, mutateBranchCopy, &m);
      if( rc!=SQLITE_OK ){
        branchResultError(ctx, rc, "source branch not found", "branch already exists");
        return;
      }
      break;
    }

    case MODE_MOVE: {
      BranchMoveCtx m;
      int renamingCurrent;
      if( nPositional<2 ){
        sqlite3_result_error(ctx, "move requires source and destination", -1);
        return;
      }
      memset(&m, 0, sizeof(m));
      m.zSrc = aPositional[0];
      m.zDest = aPositional[1];
      renamingCurrent = strcmp(m.zSrc, doltliteGetSessionBranch(db))==0;
      rc = doltliteMutateRefs(db, mutateBranchMove, &m);
      if( rc!=SQLITE_OK ){
        branchResultError(ctx, rc, "source branch not found", "destination already exists");
        return;
      }
      if( renamingCurrent ){
        doltliteSetSessionBranch(db, m.zDest);
        chunkStoreSetDefaultBranch(cs, m.zDest);
      }
      break;
    }

    case MODE_CREATE: {
      BranchMutationCtx m;
      const char *zName, *zStart;
      if( nPositional<1 ){
        sqlite3_result_error(ctx, "branch name required", -1); return;
      }
      zName = aPositional[0];
      zStart = nPositional>=2 ? aPositional[1] : 0;
      memset(&m, 0, sizeof(m));
      if( zStart ){
        rc = doltliteResolveRef(db, zStart, &m.head);
        if( rc!=SQLITE_OK ){
          sqlite3_result_error(ctx, "start point not found", -1);
          return;
        }
      }else{
        doltliteGetSessionHead(db, &m.head);
        if( prollyHashIsEmpty(&m.head) ){
          sqlite3_result_error(ctx, "no commits yet — commit first", -1);
          return;
        }
      }
      m.zName = zName;
      m.force = force;
      rc = doltliteMutateRefs(db, mutateBranchRef, &m);
      if( rc!=SQLITE_OK ){
        branchResultError(ctx, rc, "branch not found", "branch already exists");
        return;
      }
      break;
    }
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
  ProllyHash savedSessionHead;
  ProllyHash savedSessionStaged;
  ProllyHash savedMergeCommit;
  ProllyHash savedConflictsCatalog;
  ProllyHash oldCatHash;
  ProllyHash oldCommitHash;
  ProllyHash targetCommit;
  ProllyHash targetCatHash;
  u8 savedIsMerging;
  int haveOldState;
};

static void checkoutRestoreSession(sqlite3 *db, CheckoutMutationCtx *p){
  doltliteSetSessionBranch(db, p->zCurrentBranch);
  doltliteSetSessionHead(db, &p->savedSessionHead);
  doltliteSetSessionStaged(db, &p->savedSessionStaged);
  doltliteSetSessionMergeState(db, p->savedIsMerging,
                               &p->savedMergeCommit,
                               &p->savedConflictsCatalog);
  if( p->haveOldState ){
    doltliteSwitchCatalog(db, &p->oldCatHash);
  }
}

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
    if( rc!=SQLITE_OK ){
      checkoutRestoreSession(db, p);
      return rc;
    }
  }

  rc = doltliteUpdateBranchWorkingState(db, p->zTargetBranch,
                                        &p->targetCatHash, &p->targetCommit);
  if( rc!=SQLITE_OK ){
    checkoutRestoreSession(db, p);
  }
  return rc;
}

/* Per-table checkout: revert each named table's working state back
** to whatever the staged catalog has for that table (or HEAD's entry
** if nothing is staged). Matches git's `git checkout file` semantics
** — reverts to the index, NOT all the way to HEAD. To go to HEAD,
** the user uses dolt_reset.
**
** Returns SQLITE_NOTFOUND if any of the named tables doesn't exist
** in the working catalog or in the source (staged/HEAD) catalog —
** in that case nothing is mutated.
*/
static int doltliteCheckoutTables(
  sqlite3 *db,
  sqlite3_value **argv,
  int nNames
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash workingHash, headCatHash, stagedHash;
  ProllyHash sourceCatHash;       /* staged-or-HEAD */
  struct TableEntry *aWorking = 0, *aSource = 0;
  int nWorking = 0, nSource = 0;
  int i, j;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  if( nNames<=0 ) return SQLITE_NOTFOUND;

  /* Build the source catalog: staged if non-empty, else HEAD. */
  doltliteGetSessionStaged(db, &stagedHash);
  if( !prollyHashIsEmpty(&stagedHash) ){
    memcpy(&sourceCatHash, &stagedHash, sizeof(ProllyHash));
  }else{
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc!=SQLITE_OK ) return rc;
    memcpy(&sourceCatHash, &headCatHash, sizeof(ProllyHash));
  }
  if( prollyHashIsEmpty(&sourceCatHash) ){
    /* Nothing tracked yet — nothing to revert. */
    return SQLITE_NOTFOUND;
  }

  /* Snapshot the current working catalog so we can build a modified
  ** copy and pass it to switchCatalog. */
  rc = doltliteFlushCatalogToHash(db, &workingHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCatalog(db, &workingHash, &aWorking, &nWorking, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteLoadCatalog(db, &sourceCatHash, &aSource, &nSource, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aWorking);
    return rc;
  }

  /* For each named table, replace the working entry with the
  ** source-catalog entry. If the table is in working but not in
  ** the source, drop it from working (it was untracked, revert
  ** removes it). If it's in neither, that's an error. */
  for(i=0; i<nNames; i++){
    const char *zName = (const char*)sqlite3_value_text(argv[i]);
    int srcIdx = -1, workIdx = -1;
    if( !zName ) continue;

    for(j=0; j<nSource; j++){
      if( aSource[j].zName && strcmp(aSource[j].zName, zName)==0 ){
        srcIdx = j; break;
      }
    }
    for(j=0; j<nWorking; j++){
      if( aWorking[j].zName && strcmp(aWorking[j].zName, zName)==0 ){
        workIdx = j; break;
      }
    }

    if( srcIdx<0 && workIdx<0 ){
      sqlite3_free(aWorking); sqlite3_free(aSource);
      return SQLITE_NOTFOUND;
    }

    if( srcIdx<0 ){
      /* Table is in working but not in source — drop from working. */
      if( workIdx+1<nWorking ){
        memmove(&aWorking[workIdx], &aWorking[workIdx+1],
                (nWorking-workIdx-1)*(int)sizeof(struct TableEntry));
      }
      nWorking--;
    }else if( workIdx<0 ){
      /* Source has it, working does not (the user dropped the
      ** table; checkout brings it back). */
      struct TableEntry *aNew = sqlite3_realloc(aWorking,
          (nWorking+1)*(int)sizeof(struct TableEntry));
      if( !aNew ){
        sqlite3_free(aWorking); sqlite3_free(aSource);
        return SQLITE_NOMEM;
      }
      aWorking = aNew;
      aWorking[nWorking++] = aSource[srcIdx];
    }else{
      aWorking[workIdx] = aSource[srcIdx];
    }
  }

  /* Serialize the new working catalog and switch to it. */
  {
    u8 *buf = 0;
    int nBuf = 0;
    ProllyHash newWorkingHash;
    rc = doltliteSerializeCatalogEntries(db, aWorking, nWorking, &buf, &nBuf);
    if( rc==SQLITE_OK ){
      rc = chunkStorePut(cs, buf, nBuf, &newWorkingHash);
    }
    sqlite3_free(buf);
    if( rc==SQLITE_OK ){
      rc = doltliteSwitchCatalog(db, &newWorkingHash);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteSaveWorkingSet(db);
    }
  }

  sqlite3_free(aWorking);
  sqlite3_free(aSource);
  return rc;
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

  /* Block checkout during unresolved merge conflicts. */
  {
    u8 isMerging = 0;
    doltliteGetSessionMergeState(db, &isMerging, 0, 0);
    if( isMerging ){
      sqlite3_result_error(ctx, "unresolved merge conflicts \xe2\x80\x94 commit or abort first", -1);
      return;
    }
  }

  
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
    rc = doltliteFlushAndSerializeCatalog(db, &oldCatData, &nOldCat);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      sqlite3_result_error(ctx, "failed to snapshot current branch state", -1);
      return;
    }
    rc = chunkStorePut(cs, oldCatData, nOldCat, &m.oldCatHash);
    sqlite3_free(oldCatData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(zCurrentBranch);
      sqlite3_result_error(ctx, "failed to snapshot current branch state", -1);
      return;
    }
    m.haveOldState = 1;
  }
  doltliteGetSessionHead(db, &m.savedSessionHead);
  doltliteGetSessionStaged(db, &m.savedSessionStaged);
  doltliteGetSessionMergeState(db, &m.savedIsMerging,
                               &m.savedMergeCommit,
                               &m.savedConflictsCatalog);

  m.zTargetBranch = zBranch;
  m.zCurrentBranch = zCurrentBranch;
  rc = doltliteMutateRefs(db, checkoutMutateRefs, &m);
  if( rc!=SQLITE_OK ){
    checkoutRestoreSession(db, &m);
  }
  sqlite3_free(zCurrentBranch);
  zCurrentBranch = 0;
  if( rc==SQLITE_NOTFOUND ){
    /* Not a branch — fall through to per-table checkout (revert
    ** the named tables in the working set to whatever the staged
    ** catalog has, or HEAD if nothing is staged). Matches git
    ** semantics: `git checkout file` reverts working to staged. */
    rc = doltliteCheckoutTables(db, argv, argc);
    if( rc==SQLITE_NOTFOUND ){
      char *zErr = sqlite3_mprintf(
          "no such branch or table: %s", zBranch);
      sqlite3_result_error(ctx, zErr ? zErr : "no such branch or table", -1);
      sqlite3_free(zErr);
      return;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      return;
    }
    sqlite3_result_int(ctx, 0);
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
  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x("
      "name TEXT, "
      "hash TEXT, "
      "latest_committer TEXT, "
      "latest_committer_email TEXT, "
      "latest_commit_date TEXT, "
      "latest_commit_message TEXT, "
      "remote TEXT, "
      "branch TEXT, "
      "dirty INTEGER"
    ")");
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
/*
** Compute the "dirty" bit for a branch. The current branch is dirty when
** the in-memory working state differs from HEAD's catalog (already
** computed by doltliteHasUncommittedChanges). Other branches have a
** persisted working-set blob on the BranchRef whose hash points at a
** versioned record containing the staged catalog hash; the branch is
** dirty when that staged catalog differs from HEAD's catalog.
*/
static int brIsDirty(
  sqlite3 *db,
  ChunkStore *cs,
  struct BranchRef *br,
  int *pDirty
){
  ProllyHash stagedCat;
  ProllyHash commitCat;
  u8 *wsData = 0;
  int nWsData = 0;
  int rc;

  *pDirty = 0;

  if( strcmp(br->zName, doltliteGetSessionBranch(db))==0 ){
    *pDirty = doltliteHasUncommittedChanges(db) ? 1 : 0;
    return SQLITE_OK;
  }
  if( prollyHashIsEmpty(&br->workingSetHash) ){
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, &br->workingSetHash, &wsData, &nWsData);
  if( rc!=SQLITE_OK || !wsData || nWsData < WS_TOTAL_SIZE || wsData[0] != WS_FORMAT_VERSION ){
    sqlite3_free(wsData);
    return rc==SQLITE_OK ? SQLITE_CORRUPT : rc;
  }
  memcpy(stagedCat.data, wsData + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  sqlite3_free(wsData);

  if( prollyHashIsEmpty(&stagedCat) ){
    return SQLITE_OK;
  }

  {
    DoltliteCommit c;
    memset(&c, 0, sizeof(c));
    rc = doltliteLoadCommit(db, &br->commitHash, &c);
    if( rc==SQLITE_OK ){
      memcpy(commitCat.data, c.catalogHash.data, PROLLY_HASH_SIZE);
      *pDirty = prollyHashCompare(&stagedCat, &commitCat)!=0;
    }
    doltliteCommitClear(&c);
  }
  return rc;
}

static int brColumn(sqlite3_vtab_cursor *c, sqlite3_context *ctx, int col){
  BrVtab *v = (BrVtab*)c->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(v->db);
  struct BranchRef *br;
  if(!cs) return SQLITE_OK;
  br = &cs->aBranches[((BrCur*)c)->iRow];

  switch(col){
    case 0:
      sqlite3_result_text(ctx, br->zName, -1, SQLITE_TRANSIENT);
      return SQLITE_OK;
    case 1: {
      char h[41];
      doltliteHashToHex(&br->commitHash, h);
      sqlite3_result_text(ctx, h, -1, SQLITE_TRANSIENT);
      return SQLITE_OK;
    }
    case 6: case 7:
      /* Upstream tracking: doltlite has no local-branch upstream concept,
      ** so these are always empty (matches Dolt's output for branches
      ** without an upstream configured). */
      sqlite3_result_text(ctx, "", -1, SQLITE_STATIC);
      return SQLITE_OK;
    case 8: {
      int dirty = 0;
      int rc = brIsDirty(v->db, cs, br, &dirty);
      if( rc!=SQLITE_OK ){
        sqlite3_result_error_code(ctx, rc);
        return rc;
      }
      sqlite3_result_int(ctx, dirty);
      return SQLITE_OK;
    }
  }

  /* Columns 2-5 require the head commit. Load once. */
  {
    DoltliteCommit cm;
    int rc;
    memset(&cm, 0, sizeof(cm));
    rc = doltliteLoadCommit(v->db, &br->commitHash, &cm);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(ctx, rc);
      doltliteCommitClear(&cm);
      return rc;
    }
    switch(col){
      case 2:
        sqlite3_result_text(ctx, cm.zName ? cm.zName : "",
                            -1, SQLITE_TRANSIENT);
        break;
      case 3:
        sqlite3_result_text(ctx, cm.zEmail ? cm.zEmail : "",
                            -1, SQLITE_TRANSIENT);
        break;
      case 4: {
        time_t t = (time_t)cm.timestamp;
        struct tm *tm = gmtime(&t);
        if( tm ){
          char buf[32];
          strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
          sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
        }else{
          sqlite3_result_null(ctx);
        }
        break;
      }
      case 5:
        sqlite3_result_text(ctx, cm.zMessage ? cm.zMessage : "",
                            -1, SQLITE_TRANSIENT);
        break;
    }
    doltliteCommitClear(&cm);
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
