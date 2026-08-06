#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "chunk_store.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "prolly_diff.h"
#include "doltlite_commit.h"
#include "doltlite_record.h"
#include "doltlite_internal.h"
#include "doltlite_name_index.h"
#include <stddef.h>
#include "doltlite_ignore.h"

#include <string.h>
#include <ctype.h>
#include <time.h>

typedef struct RebaseFinalizeRefsCtx RebaseFinalizeRefsCtx;
struct RebaseFinalizeRefsCtx {
  const char *zOrigBranch;
  const char *zWorkingBranch;
  const ProllyHash *pExpectedOrigHead;
  const ProllyHash *pCurHead;
  const ProllyHash *pCurCat;
};

typedef struct RebaseCreateRefsCtx RebaseCreateRefsCtx;
struct RebaseCreateRefsCtx {
  const char *zWorkingBranch;
  const ProllyHash *pHead;
  const ProllyHash *pCatalog;
};

typedef struct RebaseAbortRefsCtx RebaseAbortRefsCtx;
struct RebaseAbortRefsCtx {
  const char *zOrigBranch;
  const char *zWorkingBranch;
  const ProllyHash *pExpectedOrigHead;
  const ProllyHash *pOrigCatalog;
};

static char *rebaseBuildWorkingBranchName(const char *zOrigBranch);
static int rebaseRestoreBranchState(sqlite3 *db, const char *zBranch);
static int rebaseFinalizeContinueRefs(sqlite3*, ChunkStore*, void*);
static int rebaseFinalizeLinearRefs(sqlite3*, ChunkStore*, void*);
static int rebaseDeleteWorkingBranchRefs(sqlite3*, ChunkStore*, void*);

static int rebaseCreateWorkingBranchRefs(
  sqlite3 *db,
  ChunkStore *cs,
  void *pArg
){
  RebaseCreateRefsCtx *p = (RebaseCreateRefsCtx*)pArg;
  ProllyHash probe;
  int rc;
  rc = chunkStoreFindBranch(cs, p->zWorkingBranch, &probe);
  if( rc==SQLITE_OK ) return SQLITE_CONSTRAINT;
  if( rc!=SQLITE_NOTFOUND ) return rc;
  rc = chunkStoreAddBranch(cs, p->zWorkingBranch, p->pHead);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteWriteBranchCleanWorkingState(
      db, p->zWorkingBranch, p->pCatalog, p->pHead);
}

static int rebaseAbortLinearRefs(
  sqlite3 *db,
  ChunkStore *cs,
  void *pArg
){
  RebaseAbortRefsCtx *p = (RebaseAbortRefsCtx*)pArg;
  ProllyHash origHead;
  int rc;
  rc = chunkStoreFindBranch(cs, p->zOrigBranch, &origHead);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(&origHead, p->pExpectedOrigHead)==0 ){
    rc = doltliteWriteBranchCleanWorkingState(
        db, p->zOrigBranch, p->pOrigCatalog, p->pExpectedOrigHead);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = chunkStoreDeleteBranch(cs, p->zWorkingBranch);
  return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
}

static int rebaseRestoreReturnBranchWorkingState(
  sqlite3 *db,
  const char *zBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit c;
  ProllyHash headHash;
  int rc;

  if( !cs || !zBranch || !zBranch[0] ) return SQLITE_OK;
  rc = chunkStoreFindBranch(cs, zBranch, &headHash);
  if( rc!=SQLITE_OK ) return rc;
  memset(&c, 0, sizeof(c));
  rc = doltliteLoadCommit(db, &headHash, &c);
  if( rc==SQLITE_OK ){
    rc = doltliteWriteBranchCleanWorkingState(db, zBranch, &c.catalogHash, &headHash);
  }
  doltliteCommitClear(&c);
  return rc;
}
/* True if zBranch holds working-set changes its head commit does not. An
** interactive rebase mirrors its working set onto the return branch so a
** reopen can resume, and clears that branch at the end -- both of which
** destroy uncommitted work, so a dirty branch must not be adopted as the
** mirror target. Rebase already refuses to start with a dirty current branch;
** the return branch is a different one and has no such guarantee. */
static int rebaseBranchHasUncommittedWork(
  sqlite3 *db,
  const char *zBranch,
  int *pDirty
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit c;
  ProllyHash headHash, wsCat, wsCommit;
  int rc;

  *pDirty = 0;
  if( !cs || !zBranch || !zBranch[0] ) return SQLITE_OK;
  rc = chunkStoreFindBranch(cs, zBranch, &headHash);
  if( rc!=SQLITE_OK ) return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
  memset(&wsCat, 0, sizeof(wsCat));
  memset(&wsCommit, 0, sizeof(wsCommit));
  if( chunkStoreReadBranchWorkingCatalog(cs, zBranch, &wsCat, &wsCommit)
        !=SQLITE_OK ){
    return SQLITE_OK;
  }
  memset(&c, 0, sizeof(c));
  rc = doltliteLoadCommit(db, &headHash, &c);
  if( rc==SQLITE_OK
   && prollyHashCompare(&wsCommit, &headHash)==0
   && prollyHashCompare(&wsCat, &c.catalogHash)!=0 ){
    *pDirty = 1;
  }
  doltliteCommitClear(&c);
  return rc;
}

static int doltliteRebaseCollectReplaySet(
  sqlite3 *db,
  const ProllyHash *pHeadHash,
  const ProllyHash *pUpstreamHash,
  ProllyHash **paReplay,
  int *pnReplay
){
  ProllyHashSet upstreamAncestors;
  ProllyHashSet visited;
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 0;
  ProllyHash *aReplay = 0;
  int nReplay = 0, nAllocReplay = 0;
  int rc;
  int upstreamInit = 0;
  int visitedInit = 0;
  int i, j;
  assert( db!=0 && pHeadHash!=0 && pUpstreamHash!=0 );
  assert( paReplay!=0 && pnReplay!=0 );

  *paReplay = 0;
  *pnReplay = 0;

  rc = prollyHashSetInit(&upstreamAncestors, 256);
  if( rc!=SQLITE_OK ) return rc;
  upstreamInit = 1;

  qAlloc = 64;
  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ){ rc = SQLITE_NOMEM; goto cleanup; }
  queue[qTail++] = *pUpstreamHash;

  while( qHead < qTail ){
    ProllyHash cur = queue[qHead++];
    DoltliteCommit c;

    if( prollyHashIsEmpty(&cur) ) continue;
    if( prollyHashSetContains(&upstreamAncestors, &cur) ) continue;
    rc = prollyHashSetAdd(&upstreamAncestors, &cur);
    if( rc!=SQLITE_OK ) goto cleanup;

    memset(&c, 0, sizeof(c));
    rc = sqlite3FaultSim(956) ? SQLITE_IOERR
                              : doltliteLoadCommit(db, &cur, &c);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&c);
      goto cleanup;
    }
    for(i=0; i<doltliteCommitParentCount(&c); i++){
      const ProllyHash *pp = doltliteCommitParentHash(&c, i);
      if( !pp || prollyHashIsEmpty(pp) ) continue;
      if( prollyHashSetContains(&upstreamAncestors, pp) ) continue;
      if( qTail >= qAlloc ){
        int nNew = qAlloc * 2;
        ProllyHash *tmp = sqlite3_realloc(queue, nNew*(int)sizeof(ProllyHash));
        if( !tmp ){ doltliteCommitClear(&c); rc = SQLITE_NOMEM; goto cleanup; }
        queue = tmp;
        qAlloc = nNew;
      }
      queue[qTail++] = *pp;
    }
    doltliteCommitClear(&c);
  }

  /* BFS from HEAD dissolves merge commits: traverse all parents but only
  ** include non-merge commits in the replay set, linearizing the history. */
  qHead = qTail = 0;
  queue[qTail++] = *pHeadHash;

  rc = prollyHashSetInit(&visited, 256);
  if( rc!=SQLITE_OK ) goto cleanup;
  visitedInit = 1;

  while( qHead < qTail ){
    ProllyHash cur = queue[qHead++];
    DoltliteCommit c;
    int nParents;

    if( prollyHashIsEmpty(&cur) ) continue;
    if( prollyHashSetContains(&upstreamAncestors, &cur) ) continue;
    if( prollyHashSetContains(&visited, &cur) ) continue;
    rc = prollyHashSetAdd(&visited, &cur);
    if( rc!=SQLITE_OK ) goto cleanup;

    memset(&c, 0, sizeof(c));
    rc = doltliteLoadCommit(db, &cur, &c);
    if( rc!=SQLITE_OK ) goto cleanup;
    nParents = doltliteCommitParentCount(&c);

    if( nParents<=1 ){
      if( nReplay >= nAllocReplay ){
        int nNew = nAllocReplay ? nAllocReplay*2 : 16;
        ProllyHash *tmp = sqlite3_realloc(aReplay, nNew*(int)sizeof(ProllyHash));
        if( !tmp ){ doltliteCommitClear(&c); rc = SQLITE_NOMEM; goto cleanup; }
        aReplay = tmp;
        nAllocReplay = nNew;
      }
      aReplay[nReplay++] = cur;
    }

    for( j=0; j<nParents; j++ ){
      const ProllyHash *pp = doltliteCommitParentHash(&c, j);
      if( !pp || prollyHashIsEmpty(pp) ) continue;
      if( prollyHashSetContains(&upstreamAncestors, pp) ) continue;
      if( prollyHashSetContains(&visited, pp) ) continue;
      if( qTail >= qAlloc ){
        int nNew = qAlloc * 2;
        ProllyHash *tmp = sqlite3_realloc(queue, nNew*(int)sizeof(ProllyHash));
        if( !tmp ){ doltliteCommitClear(&c); rc = SQLITE_NOMEM; goto cleanup; }
        queue = tmp;
        qAlloc = nNew;
      }
      queue[qTail++] = *pp;
    }
    doltliteCommitClear(&c);
  }

  for(i=0; i<nReplay/2; i++){
    ProllyHash tmp = aReplay[i];
    aReplay[i] = aReplay[nReplay-1-i];
    aReplay[nReplay-1-i] = tmp;
  }

  *paReplay = aReplay;
  *pnReplay = nReplay;
  aReplay = 0;
  rc = SQLITE_OK;

cleanup:
  sqlite3_free(queue);
  sqlite3_free(aReplay);
  if( upstreamInit ) prollyHashSetFree(&upstreamAncestors);
  if( visitedInit ) prollyHashSetFree(&visited);
  return rc;
}

static int rebaseSwitchToWorkingBranch(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit c;
  ProllyHash headHash;
  ProllyHash emptyHash;
  int rc;

  if( !cs || !zBranch || !zBranch[0] ) return SQLITE_ERROR;
  rc = chunkStoreFindBranch(cs, zBranch, &headHash);
  if( rc!=SQLITE_OK ) return rc;
  memset(&c, 0, sizeof(c));
  rc = doltliteLoadCommit(db, &headHash, &c);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSetSessionBranch(db, zBranch);
  if( rc==SQLITE_OK ) doltliteSetSessionHead(db, &headHash);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionStaged(db, &c.catalogHash);
  if( rc==SQLITE_OK ) rc = doltliteSwitchCatalog(db, &c.catalogHash);
  if( rc==SQLITE_OK ) rc = doltliteClearSessionMergeState(db);
  memset(&emptyHash, 0, sizeof(emptyHash));
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(db, &emptyHash);
  }
  doltliteCommitClear(&c);
  return rc;
}

static int doltliteRebaseLinearReplay(
  sqlite3 *db,
  sqlite3_context *context,
  const char *zUpstream,
  char **pzFinalMessage
){
  ChunkStore *cs;
  int sealTopLevel;
  ProllyHash upstreamHash, headHash;
  ProllyHash lockedHead;
  ProllyHash curHead, curCat;
  ProllyHash origCat;
  ProllyHash *aReplay = 0;
  int nReplay = 0;
  DoltliteCommit upstreamCommit;
  DoltliteCommit origCommit;
  DoltliteCommit finalCommit;
  RebaseCreateRefsCtx createCtx;
  RebaseAbortRefsCtx abortCtx;
  RebaseFinalizeRefsCtx refsCtx;
  char *zOrig = 0;
  char *zWorking = 0;
  int workingCreated = 0;
  int graphLocked = 0;
  int rc;
  int i;
  int dirty = 0;
  char *zFailedMsg = 0;
  int bConflict = 0;
  assert( db!=0 && context!=0 && zUpstream!=0 && pzFinalMessage!=0 );
  cs = doltliteGetChunkStore(db);
  sealTopLevel = db->pSavepoint!=0 && db->nSavepoint==0;

  *pzFinalMessage = 0;
  memset(&upstreamCommit, 0, sizeof(upstreamCommit));
  memset(&origCommit, 0, sizeof(origCommit));
  memset(&finalCommit, 0, sizeof(finalCommit));
  memset(&curHead, 0, sizeof(curHead));
  memset(&curCat, 0, sizeof(curCat));
  memset(&origCat, 0, sizeof(origCat));
  memset(&lockedHead, 0, sizeof(lockedHead));

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return rc;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "cannot start a rebase with uncommitted changes", -1);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return SQLITE_ERROR;
  }

  zOrig = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  zWorking = rebaseBuildWorkingBranchName(zOrig);
  if( !zOrig || !zWorking ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_code(context, SQLITE_NOMEM);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return SQLITE_NOMEM;
  }

  rc = doltliteResolveRef(db, zUpstream, &upstreamHash);
  if( rc!=SQLITE_OK ){
    char *zErr = sqlite3_mprintf("branch not found: %s", zUpstream);
    sqlite3_result_error(context, zErr ? zErr : "branch not found", -1);
    sqlite3_free(zErr);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return SQLITE_ERROR;
  }

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return SQLITE_ERROR;
  }

  rc = doltliteRebaseCollectReplaySet(db, &headHash, &upstreamHash,
                                      &aReplay, &nReplay);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_code(context, rc);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return rc;
  }
  if( nReplay==0 ){
    sqlite3_free(aReplay);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error(context, "didn't identify any commits!", -1);
    if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
    return SQLITE_ERROR;
  }

  rc = doltliteLoadCommit(db, &headHash, &origCommit);
  if( rc!=SQLITE_OK ) goto rollback;
  origCat = origCommit.catalogHash;
  rc = doltliteLoadCommit(db, &upstreamHash, &upstreamCommit);
  if( rc!=SQLITE_OK ) goto rollback;

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) goto rollback;
  graphLocked = 1;
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ) goto rollback;
  rc = chunkStoreFindBranch(cs, zOrig, &lockedHead);
  if( rc!=SQLITE_OK ) goto rollback;
  if( prollyHashCompare(&lockedHead, &headHash)!=0 ){
    rc = SQLITE_BUSY;
    goto rollback;
  }

  memset(&createCtx, 0, sizeof(createCtx));
  createCtx.zWorkingBranch = zWorking;
  createCtx.pHead = &upstreamHash;
  createCtx.pCatalog = &upstreamCommit.catalogHash;
  {
    DoltliteBranchExpectation expected;
    expected.zBranch = zWorking;
    expected.pTip = 0;
    rc = doltliteMutateRefsExpected(
        db, &expected, 1, rebaseCreateWorkingBranchRefs, &createCtx);
  }
  if( rc==SQLITE_OK ) workingCreated = 1;
  doltliteCommitClear(&upstreamCommit);
  memset(&upstreamCommit, 0, sizeof(upstreamCommit));
  if( rc!=SQLITE_OK ) goto rollback;

  rc = rebaseSwitchToWorkingBranch(db, zWorking);
  if( rc!=SQLITE_OK ) goto rollback;

  for(i=0; i<nReplay; i++){
    DoltliteCommit replayCommit, parentCommit, curHeadCommit;
    ProllyHash curHead;
    int nConflicts = 0;
    char hexBuf[PROLLY_HASH_SIZE*2+1];

    memset(&replayCommit, 0, sizeof(replayCommit));
    memset(&parentCommit, 0, sizeof(parentCommit));
    memset(&curHeadCommit, 0, sizeof(curHeadCommit));

    rc = doltliteLoadCommit(db, &aReplay[i], &replayCommit);
    if( rc!=SQLITE_OK ) goto rollback;

    sqlite3_free(zFailedMsg);
    zFailedMsg = sqlite3_mprintf("%.200s",
        replayCommit.zMessage ? replayCommit.zMessage : "");

    if( doltliteCommitParentCount(&replayCommit)==0 ){
      doltliteCommitClear(&replayCommit);
      rc = SQLITE_ERROR;
      goto rollback;
    }
    rc = doltliteLoadFirstParentCommit(db, &replayCommit, &parentCommit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&replayCommit);
      goto rollback;
    }

    doltliteGetSessionHead(db, &curHead);
    rc = doltliteLoadCommit(db, &curHead, &curHeadCommit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&replayCommit);
      doltliteCommitClear(&parentCommit);
      goto rollback;
    }

    rc = applyMergedCatalogAndCommit(db, context,
        &parentCommit.catalogHash,
        &curHeadCommit.catalogHash,
        &replayCommit.catalogHash,
        &curHead,
        replayCommit.zMessage ? replayCommit.zMessage : "",
        &nConflicts, hexBuf);

    doltliteCommitClear(&replayCommit);
    doltliteCommitClear(&parentCommit);
    doltliteCommitClear(&curHeadCommit);

    if( rc!=SQLITE_OK ) goto rollback;
    if( nConflicts>0 ){ bConflict = 1; rc = SQLITE_ERROR; goto rollback; }
  }

  doltliteGetSessionHead(db, &curHead);
  rc = doltliteLoadCommit(db, &curHead, &finalCommit);
  if( rc!=SQLITE_OK ) goto rollback;
  curCat = finalCommit.catalogHash;
  doltliteCommitClear(&finalCommit);
  memset(&finalCommit, 0, sizeof(finalCommit));

  memset(&refsCtx, 0, sizeof(refsCtx));
  refsCtx.zOrigBranch = zOrig;
  refsCtx.zWorkingBranch = zWorking;
  refsCtx.pExpectedOrigHead = &headHash;
  refsCtx.pCurHead = &curHead;
  refsCtx.pCurCat = &curCat;
  {
    DoltliteBranchExpectation expected[2];
    expected[0].zBranch = zOrig;
    expected[0].pTip = &headHash;
    expected[1].zBranch = zWorking;
    expected[1].pTip = &curHead;
    doltliteTestCrashFinalize("rebase");
    rc = doltliteMutateRefsExpected(
        db, expected, 2, rebaseFinalizeLinearRefs, &refsCtx);
  }
  if( rc!=SQLITE_OK ) goto rollback;
  workingCreated = 0;

  rc = rebaseRestoreBranchState(db, zOrig);
  if( rc!=SQLITE_OK ) goto rollback;

  chunkStoreUnlock(cs);
  graphLocked = 0;
  doltliteCommitClear(&origCommit);
  sqlite3_free(aReplay);
  sqlite3_free(zFailedMsg);
  *pzFinalMessage = sqlite3_mprintf(
    "Successfully rebased and updated refs/heads/%s",
    zOrig);
  sqlite3_free(zOrig);
  sqlite3_free(zWorking);
  return SQLITE_OK;

rollback:
  doltliteCommitClear(&upstreamCommit);
  doltliteCommitClear(&finalCommit);
  if( workingCreated ){
    memset(&abortCtx, 0, sizeof(abortCtx));
    abortCtx.zOrigBranch = zOrig;
    abortCtx.zWorkingBranch = zWorking;
    abortCtx.pExpectedOrigHead = &headHash;
    abortCtx.pOrigCatalog = &origCommit.catalogHash;
    (void)doltliteMutateRefs(db, rebaseAbortLinearRefs, &abortCtx);
  }
  doltliteCommitClear(&origCommit);
  if( rebaseRestoreBranchState(db, zOrig)==SQLITE_OK ){
    if( workingCreated && db->autoCommit && !prollyHashIsEmpty(&origCat) ){
      doltliteAdoptRollbackBaseline(db, &origCat);
    }
  }
  if( graphLocked ) chunkStoreUnlock(cs);
  sqlite3_free(aReplay);
  {
    char *zErr;
    if( bConflict && zFailedMsg && zFailedMsg[0] ){
      zErr = sqlite3_mprintf(
          "conflict rebasing \"%s\"; rebase aborted, branch restored to pre-rebase state",
          zFailedMsg);
    }else if( zFailedMsg && zFailedMsg[0] ){
      zErr = sqlite3_mprintf(
          "error rebasing \"%s\"; rebase aborted, branch restored to pre-rebase state",
          zFailedMsg);
    }else{
      zErr = sqlite3_mprintf(
          "rebase aborted, branch restored to pre-rebase state");
    }
    if( zErr ){
      sqlite3_result_error(context, zErr, -1);
      sqlite3_free(zErr);
    }else{
      sqlite3_result_error_code(context, rc);
    }
  }
  sqlite3_free(zFailedMsg);
  sqlite3_free(zOrig);
  sqlite3_free(zWorking);
  if( sealTopLevel ) (void)doltliteVcSealTopLevelSavepointTxn(db);
  return SQLITE_ERROR;
}

typedef struct RebasePlanRow RebasePlanRow;
struct RebasePlanRow {
  double order;
  char *zAction;
  ProllyHash commitHash;
  char *zCommitMessage;
};

enum DoltliteRebaseSchemaState {
  DOLTLITE_REBASE_SCHEMA_ABSENT = 0,
  DOLTLITE_REBASE_SCHEMA_OK = 1,
  DOLTLITE_REBASE_SCHEMA_BAD = 2
};

static int doltliteRebaseSchemaState(sqlite3 *db, int *pState){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int nCol = 0;

  *pState = DOLTLITE_REBASE_SCHEMA_ABSENT;
  rc = sqlite3_prepare_v2(db, "PRAGMA main.table_info(\"dolt_rebase\")",
                          -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
    int notNull = sqlite3_column_int(pStmt, 3);
    int pkPos = sqlite3_column_int(pStmt, 5);
    char aff;
    if( !zName ) goto bad;
    aff = sqlite3AffinityType(zType ? zType : "", 0);
    if( nCol==0 ){
      if( sqlite3_stricmp(zName, "rebase_order")!=0 ) goto bad;
      if( aff!=SQLITE_AFF_REAL && aff!=SQLITE_AFF_NUMERIC ) goto bad;
      if( !notNull ) goto bad;
      if( pkPos!=1 ) goto bad;
    }else if( nCol==1 ){
      if( sqlite3_stricmp(zName, "action")!=0 ) goto bad;
      if( aff!=SQLITE_AFF_TEXT ) goto bad;
      if( pkPos!=0 ) goto bad;
    }else if( nCol==2 ){
      if( sqlite3_stricmp(zName, "commit_hash")!=0 ) goto bad;
      if( aff!=SQLITE_AFF_TEXT ) goto bad;
      if( pkPos!=0 ) goto bad;
    }else if( nCol==3 ){
      if( sqlite3_stricmp(zName, "commit_message")!=0 ) goto bad;
      if( aff!=SQLITE_AFF_TEXT ) goto bad;
      if( pkPos!=0 ) goto bad;
    }else{
      goto bad;
    }
    nCol++;
  }
  if( rc!=SQLITE_DONE ){
    sqlite3_finalize(pStmt);
    return rc;
  }
  sqlite3_finalize(pStmt);

  if( nCol==0 ){
    *pState = DOLTLITE_REBASE_SCHEMA_ABSENT;
  }else if( nCol==4 ){
    *pState = DOLTLITE_REBASE_SCHEMA_OK;
  }else{
    *pState = DOLTLITE_REBASE_SCHEMA_BAD;
  }
  return SQLITE_OK;

bad:
  *pState = DOLTLITE_REBASE_SCHEMA_BAD;
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static int doltliteValidateRebasePlanTable(sqlite3 *db, char **pzErr){
  int rc;
  int state;
  if( pzErr ) *pzErr = 0;
  rc = doltliteRebaseSchemaState(db, &state);
  if( rc!=SQLITE_OK ) return rc;
  if( state==DOLTLITE_REBASE_SCHEMA_OK ) return SQLITE_OK;
  if( state==DOLTLITE_REBASE_SCHEMA_ABSENT ){
    if( pzErr ) *pzErr = sqlite3_mprintf("no rebase in progress");
    return SQLITE_NOTFOUND;
  }
  if( pzErr ){
    *pzErr = sqlite3_mprintf(
      "dolt_rebase has an unexpected schema; expected: "
      "CREATE TABLE dolt_rebase("
      "rebase_order REAL PRIMARY KEY, "
      "action TEXT, "
      "commit_hash TEXT, "
      "commit_message TEXT)");
  }
  return SQLITE_CONSTRAINT;
}

static void rebaseFreePlan(RebasePlanRow *aPlan, int nPlan){
  int i;
  for(i=0; i<nPlan; i++){
    sqlite3_free(aPlan[i].zAction);
    sqlite3_free(aPlan[i].zCommitMessage);
  }
  sqlite3_free(aPlan);
}

static int rebaseReadPlan(sqlite3 *db, RebasePlanRow **paPlan, int *pnPlan){
  sqlite3_stmt *pStmt = 0;
  RebasePlanRow *aPlan = 0;
  int nPlan = 0, nAlloc = 0;
  int rc;
  char *zErr = 0;

  *paPlan = 0;
  *pnPlan = 0;

  rc = doltliteValidateRebasePlanTable(db, &zErr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zErr);
    return rc;
  }

  rc = sqlite3_prepare_v2(db,
    "SELECT rebase_order, action, commit_hash, commit_message "
    "FROM main.dolt_rebase ORDER BY rebase_order", -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    RebasePlanRow *r;
    const char *zHex;

    if( nPlan >= nAlloc ){
      int nNew = nAlloc ? nAlloc*2 : 16;
      RebasePlanRow *tmp = sqlite3_realloc(aPlan, nNew*(int)sizeof(RebasePlanRow));
      if( !tmp ){ rc = SQLITE_NOMEM; goto fail; }
      aPlan = tmp;
      nAlloc = nNew;
    }
    r = &aPlan[nPlan];
    memset(r, 0, sizeof(*r));
    r->order = sqlite3_column_double(pStmt, 0);
    r->zAction = sqlite3_mprintf("%s",
        (const char*)sqlite3_column_text(pStmt, 1));
    zHex = (const char*)sqlite3_column_text(pStmt, 2);
    if( !zHex || doltliteHexToHash(zHex, &r->commitHash)!=SQLITE_OK ){
      rc = SQLITE_CORRUPT;
      sqlite3_free(r->zAction);
      goto fail;
    }
    r->zCommitMessage = sqlite3_mprintf("%s",
        (const char*)sqlite3_column_text(pStmt, 3));
    if( !r->zAction || !r->zCommitMessage ){
      rc = SQLITE_NOMEM;
      sqlite3_free(r->zAction);
      sqlite3_free(r->zCommitMessage);
      goto fail;
    }
    nPlan++;
    if( sqlite3FaultSim(951) ){
      rc = SQLITE_IOERR;
      break;
    }
  }
  if( rc!=SQLITE_DONE ) goto fail;
  sqlite3_finalize(pStmt);

  *paPlan = aPlan;
  *pnPlan = nPlan;
  return SQLITE_OK;

fail:
  sqlite3_finalize(pStmt);
  rebaseFreePlan(aPlan, nPlan);
  return rc;
}

static char *rebaseBuildWorkingBranchName(const char *zOrigBranch){
  if( !zOrigBranch ) return 0;
  return sqlite3_mprintf("dolt_rebase_%s", zOrigBranch);
}

static int rebaseRestoreBranchState(sqlite3 *db, const char *zBranch){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash headHash;
  ProllyHash emptyHash;
  DoltliteCommit headCommit;
  int rc;

  if( !cs || !zBranch || !zBranch[0] ) return SQLITE_ERROR;
  if( sqlite3FaultSim(952) ) return SQLITE_IOERR;
  rc = chunkStoreFindBranch(cs, zBranch, &headHash);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteLoadCommit(db, &headHash, &headCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSwitchCatalog(db, &headCommit.catalogHash);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&headCommit);
    return rc;
  }
  rc = doltliteSetSessionBranch(db, zBranch);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&headCommit);
    return rc;
  }
  doltliteSetSessionHead(db, &headHash);
  rc = doltliteSetSessionStaged(db, &headCommit.catalogHash);
  if( rc==SQLITE_OK ) rc = doltliteClearSessionMergeState(db);
  memset(&emptyHash, 0, sizeof(emptyHash));
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionConstraintViolationsCatalog(db, &emptyHash);
  }
  doltliteCommitClear(&headCommit);
  return rc;
}

static int rebaseCreateAndPopulatePlanTable(
  sqlite3 *db,
  const ProllyHash *aReplay,
  int nReplay
){
  int rc;
  int i;

  rc = sqlite3_exec(db,
    "CREATE TABLE main.dolt_rebase("
    "  rebase_order REAL PRIMARY KEY,"
    "  action TEXT,"
    "  commit_hash TEXT,"
    "  commit_message TEXT"
    ")", 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nReplay; i++){
    DoltliteCommit c;
    char zHex[PROLLY_HASH_SIZE*2+1];
    char zOrder[64];
    char *zSql;
    memset(&c, 0, sizeof(c));
    rc = doltliteLoadCommit(db, &aReplay[i], &c);
    if( rc!=SQLITE_OK ) break;
    doltliteHashToHex(&aReplay[i], zHex);
    sqlite3_snprintf(sizeof(zOrder), zOrder, "%!.17g", (double)(i + 1));
    zSql = sqlite3_mprintf(
      "INSERT INTO main.dolt_rebase VALUES (%s, 'pick', %Q, %Q)",
      zOrder,
      zHex,
      c.zMessage ? c.zMessage : ""
    );
    if( !zSql ){
      doltliteCommitClear(&c);
      return SQLITE_NOMEM;
    }
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    doltliteCommitClear(&c);
    if( rc!=SQLITE_OK ) break;
  }
  return rc;
}

static int rebaseApplyPlanRowCatalog(
  sqlite3 *db,
  const RebasePlanRow *pRow,
  const ProllyHash *pCurCat,
  ProllyHash *pMergedCat,
  int bSkipConstraintDetect
){
  DoltliteCommit parentC, replayC;
  int nConflicts = 0;
  int nViolations = 0;
  int rc;

  memset(&parentC, 0, sizeof(parentC));
  memset(&replayC, 0, sizeof(replayC));

  rc = doltliteLoadCommit(db, &pRow->commitHash, &replayC);
  if( rc!=SQLITE_OK ) return rc;
  if( doltliteCommitParentCount(&replayC)==0 ){
    doltliteCommitClear(&replayC);
    return SQLITE_ERROR;
  }
  rc = doltliteLoadFirstParentCommit(db, &replayC, &parentC);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&replayC);
    return rc;
  }

  {
    char **azReindex = 0;
    int nReindex = 0;
    rc = doltliteMergeCatalogs(db, &parentC.catalogHash, pCurCat,
                               &replayC.catalogHash, pMergedCat,
                               &nConflicts, 0, 0, 0, 0,
                               &azReindex, &nReindex);
    if( rc==SQLITE_OK && nConflicts==0 ){
      rc = doltliteSwitchCatalog(db, pMergedCat);
    }
    if( rc==SQLITE_OK && nConflicts==0 && nReindex>0 ){
      rc = doltliteReindexNamedIndexes(db, azReindex, nReindex);
      if( rc==SQLITE_OK ) rc = doltliteFlushCatalogToHash(db, pMergedCat);
      if( rc==SQLITE_OK ) rc = doltliteSwitchCatalog(db, pMergedCat);
    }
    doltliteFreeNameList(azReindex, nReindex);
  }
  if( rc==SQLITE_OK && nConflicts==0 && !bSkipConstraintDetect ){
    rc = doltliteDetectPostMergeConstraintViolations(db,
                                                     &parentC.catalogHash,
                                                     &nViolations);
  }
  doltliteCommitClear(&parentC);
  doltliteCommitClear(&replayC);
  if( rc!=SQLITE_OK ) return rc;
  if( nConflicts>0 || nViolations>0 ) return SQLITE_CONSTRAINT;
  return SQLITE_OK;
}

static int rebaseAdvanceWorkingBranch(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch = doltliteGetSessionBranch(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = chunkStoreUpdateBranch(cs, zBranch, pNewHead);
  if( rc!=SQLITE_OK ) return rc;

  doltliteSetSessionHead(db, pNewHead);
  rc = doltliteSetSessionStaged(db, pCatalogHash);
  if( rc==SQLITE_OK ) rc = doltliteSwitchCatalog(db, pCatalogHash);
  if( rc!=SQLITE_OK ) return rc;

  return doltlitePersistWorkingSetWithHash(db, 0);
}

static int rebaseReplayPlanGroup(
  sqlite3 *db,
  RebasePlanRow *aPlan,
  int nPlan,
  int iStart,
  ProllyHash *pCurCat,
  ProllyHash *pCurHead,
  int *piNext,
  int bSkipConstraintDetect
){
  char *combinedMsg = 0;
  int rc;
  int j;

  rc = rebaseApplyPlanRowCatalog(db, &aPlan[iStart], pCurCat, pCurCat,
                                 bSkipConstraintDetect);
  if( rc!=SQLITE_OK ) return rc;

  combinedMsg = sqlite3_mprintf("%s",
      aPlan[iStart].zCommitMessage ? aPlan[iStart].zCommitMessage : "");
  if( !combinedMsg ) return SQLITE_NOMEM;

  j = iStart + 1;
  while( j < nPlan
      && (strcmp(aPlan[j].zAction, "squash")==0
       || strcmp(aPlan[j].zAction, "fixup")==0
       || strcmp(aPlan[j].zAction, "drop")==0) ){
    if( strcmp(aPlan[j].zAction, "drop")==0 ){
      j++;
      continue;
    }

    rc = rebaseApplyPlanRowCatalog(db, &aPlan[j], pCurCat, pCurCat,
                                   bSkipConstraintDetect);
    if( rc!=SQLITE_OK ){
      sqlite3_free(combinedMsg);
      return rc;
    }

    if( strcmp(aPlan[j].zAction, "squash")==0 ){
      char *zNew = sqlite3_mprintf("%s\n\n%s", combinedMsg,
                                   aPlan[j].zCommitMessage ? aPlan[j].zCommitMessage : "");
      sqlite3_free(combinedMsg);
      combinedMsg = zNew;
      if( !combinedMsg ) return SQLITE_NOMEM;
    }
    j++;
  }

  {
    ProllyHash newCommit;
    rc = doltliteCreateAndStoreCommit(db, pCurHead, pCurCat, combinedMsg,
                                      NULL, NULL, NULL, 0, &newCommit);
    if( rc==SQLITE_OK ){
      rc = doltliteSwitchCatalog(db, pCurCat);
    }
    if( rc==SQLITE_OK ){
      rc = doltliteSetSessionStaged(db, pCurCat);
    }
    if( rc==SQLITE_OK ){
      rc = rebaseAdvanceWorkingBranch(db, &newCommit, pCurCat);
    }
    if( rc==SQLITE_OK ) *pCurHead = newCommit;
  }
  sqlite3_free(combinedMsg);
  if( rc!=SQLITE_OK ) return rc;

  *piNext = j;
  return SQLITE_OK;
}

static int rebaseFinalizeContinueRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  RebaseFinalizeRefsCtx *p = (RebaseFinalizeRefsCtx*)pArg;
  ProllyHash origHead;
  int rc;
  rc = chunkStoreFindBranch(cs, p->zOrigBranch, &origHead);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(&origHead, p->pExpectedOrigHead)!=0 ){
    return SQLITE_BUSY;
  }
  rc = chunkStoreUpdateBranch(cs, p->zOrigBranch, p->pCurHead);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteWriteBranchCleanWorkingState(
      db, p->zOrigBranch, p->pCurCat, p->pCurHead);
  if( rc!=SQLITE_OK ) return rc;
  return SQLITE_OK;
}

static int rebaseFinalizeLinearRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  RebaseFinalizeRefsCtx *p = (RebaseFinalizeRefsCtx*)pArg;
  int rc;
  rc = rebaseFinalizeContinueRefs(db, cs, pArg);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreDeleteBranch(cs, p->zWorkingBranch);
  return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
}

static int rebaseDeleteWorkingBranchRefs(sqlite3 *db, ChunkStore *cs, void *pArg){
  const char *zWorkingBranch = (const char*)pArg;
  int rc;
  UNUSED_PARAMETER(db);
  rc = chunkStoreDeleteBranch(cs, zWorkingBranch);
  return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
}

static void rebaseKeepFirstError(int *pRc, int rc){
  if( *pRc==SQLITE_OK && rc!=SQLITE_OK ) *pRc = rc;
}

static void rebaseResultRecoveryFailure(sqlite3_context *context, int rc){
  sqlite3_result_error(context,
    "rebase recovery failed — pre-rebase state may not have been fully restored",
    -1);
  sqlite3_result_error_code(context, rc);
}

/* Claim exclusive ownership of ending the active rebase. Under the graph lock,
** reload durable working-set state for the current branch; if isRebasing is
** already clear a peer won and this returns SQLITE_DONE. Otherwise clear the
** flag, persist it under the same lock, drop dolt_rebase, and return SQLITE_OK
** so only the winner continues cleanup. Callers must copy branch names out of
** session state before claiming. */
static int rebaseClaimActiveEnd(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch;
  u8 isRebasing = 0;
  int rc;
  int locked = 0;

  if( !cs || !db ) return SQLITE_ERROR;
  zBranch = doltliteGetSessionBranch(db);
  if( !zBranch || !zBranch[0] ) zBranch = "main";

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;
  locked = 1;
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ) goto claim_done;
  rc = doltliteLoadWorkingSet(db, zBranch);
  if( rc!=SQLITE_OK ) goto claim_done;
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
  if( !isRebasing ){
    rc = SQLITE_DONE;
    goto claim_done;
  }

  rc = doltliteClearSessionRebaseState(db);
  if( rc!=SQLITE_OK ) goto claim_done;
  rc = doltliteSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) goto claim_done;
  rc = chunkStoreSerializeRefs(cs);
  if( rc!=SQLITE_OK ) goto claim_done;
  rc = chunkStoreCommit(cs);

claim_done:
  if( locked ) chunkStoreUnlock(cs);
  if( rc==SQLITE_OK ){
    int rcDrop = sqlite3FaultSim(953) ? SQLITE_IOERR :
        sqlite3_exec(db, "DROP TABLE IF EXISTS main.dolt_rebase", 0, 0, 0);
    if( rcDrop!=SQLITE_OK ) rc = rcDrop;
  }else if( rc==SQLITE_DONE ){
    (void)sqlite3_exec(db, "DROP TABLE IF EXISTS main.dolt_rebase", 0, 0, 0);
  }
  return rc;
}

static int rebaseClaimActiveEndRetry(sqlite3 *db){
  int i;
  int rc = SQLITE_BUSY;
  for(i=0; i<40; i++){
    rc = rebaseClaimActiveEnd(db);
    if( rc!=SQLITE_BUSY ) return rc;
    sqlite3_sleep(1);
  }
  return rc;
}

/* Checkout original branch and delete the temporary working branch. Idempotent
** for a missing working branch. Assumes rebaseClaimActiveEnd already cleared
** durable isRebasing / the plan table. */
static int rebaseCleanupAfterClaim(
  sqlite3 *db,
  const char *zOrigBranch,
  const char *zWorkingBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc = SQLITE_OK;
  int rc2;

  if( zOrigBranch && zOrigBranch[0] ){
    rc2 = doltliteCheckoutBranchForRebase(db, zOrigBranch);
    if( rc2!=SQLITE_OK ){
      rc2 = rebaseRestoreBranchState(db, zOrigBranch);
      rebaseKeepFirstError(&rc, rc2);
    }
  }
  if( cs && zWorkingBranch && zWorkingBranch[0] ){
    rc2 = doltliteMutateRefs(db, rebaseDeleteWorkingBranchRefs,
                             (void*)zWorkingBranch);
    rebaseKeepFirstError(&rc, rc2);
    rc2 = doltlitePersistWorkingSet(db);
    rebaseKeepFirstError(&rc, rc2);
  }
  return rc;
}

static int rebaseDiscardWorkingBranch(
  sqlite3 *db,
  const char *zOrigBranch,
  const char *zWorkingBranch
){
  int rc;

  rc = rebaseClaimActiveEndRetry(db);
  if( rc==SQLITE_DONE ){
    /* Peer already claimed; still try idempotent cleanup. */
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) return rc;
  return rebaseCleanupAfterClaim(db, zOrigBranch, zWorkingBranch);
}

static int rebaseAbortConflictedContinue(
  sqlite3 *db,
  const char *zOrigBranch,
  const char *zReturnBranch,
  const char *zWorkingBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc = SQLITE_OK;
  int rc2;

  rc2 = rebaseClaimActiveEndRetry(db);
  if( rc2!=SQLITE_OK && rc2!=SQLITE_DONE ) rebaseKeepFirstError(&rc, rc2);
  rc2 = doltliteClearSessionMergeState(db);
  rebaseKeepFirstError(&rc, rc2);
  if( zOrigBranch && zOrigBranch[0] ){
    rc2 = rebaseRestoreBranchState(db, zOrigBranch);
    rebaseKeepFirstError(&rc, rc2);
    rc2 = doltliteClearSessionRebaseState(db);
    rebaseKeepFirstError(&rc, rc2);
  }
  if( cs && zReturnBranch && zReturnBranch[0] ){
    rc2 = rebaseRestoreReturnBranchWorkingState(db, zReturnBranch);
    rebaseKeepFirstError(&rc, rc2);
  }
  if( cs && zWorkingBranch && zWorkingBranch[0] ){
    rc2 = doltliteMutateRefs(db, rebaseDeleteWorkingBranchRefs,
                             (void*)zWorkingBranch);
    rebaseKeepFirstError(&rc, rc2);
    rc2 = doltlitePersistWorkingSet(db);
    rebaseKeepFirstError(&rc, rc2);
  }
  return rc;
}

static void doltliteRebaseInteractiveStart(
  sqlite3_context *context,
  sqlite3 *db,
  const char *zUpstream
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  char *zOrig = 0;
  char *zReturnBranch = 0;
  char *zWorking = 0;
  ProllyHash upstreamHash, headHash;
  ProllyHash preRebaseCat;
  ProllyHash *aReplay = 0;
  int nReplay = 0;
  int rc;
  int dirty = 0;
  u8 curIsRebasing = 0;
  int bWorkingBranchCreated = 0;
  const char *zFailMsg = 0;

  memset(&preRebaseCat, 0, sizeof(preRebaseCat));

  doltliteGetSessionRebaseState(db, &curIsRebasing, 0, 0, 0, 0);
  if( curIsRebasing ){
    sqlite3_result_error(context,
      "rebase already in progress; use --continue or --abort", -1);
    return;
  }

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  rc = doltliteHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "cannot start a rebase with uncommitted changes", -1);
    return;
  }

  rc = doltliteResolveRef(db, zUpstream, &upstreamHash);
  if( rc!=SQLITE_OK ){
    char *zErr = sqlite3_mprintf("branch not found: %s", zUpstream);
    sqlite3_result_error(context, zErr ? zErr : "branch not found", -1);
    sqlite3_free(zErr);
    return;
  }

  doltliteGetSessionHead(db, &headHash);
  if( prollyHashIsEmpty(&headHash) ){
    sqlite3_result_error(context, "no commits on current branch", -1);
    return;
  }

  rc = doltliteRebaseCollectReplaySet(db, &headHash, &upstreamHash,
                                      &aReplay, &nReplay);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( nReplay==0 ){
    sqlite3_free(aReplay);
    sqlite3_result_error(context, "didn't identify any commits!", -1);
    return;
  }

  zOrig = sqlite3_mprintf("%s", doltliteGetSessionBranch(db));
  zReturnBranch = sqlite3_mprintf("%s", chunkStoreGetDefaultBranch(cs));
  zWorking = sqlite3_mprintf("dolt_rebase_%s", zOrig ? zOrig : "");
  if( !zOrig || !zWorking || !zReturnBranch ){
    sqlite3_free(zOrig);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(aReplay);
    sqlite3_result_error_code(context, SQLITE_NOMEM);
    return;
  }
  if( strlen(zOrig)>=WS_REBASE_BRANCH_LEN ){
    sqlite3_free(zOrig);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(aReplay);
    sqlite3_result_error(context,
      "cannot start interactive rebase: current branch name exceeds "
      "the 63-byte persisted-state limit", -1);
    return;
  }
  {
    ProllyHash probe;
    if( chunkStoreFindBranch(cs, zWorking, &probe)==SQLITE_OK ){
      sqlite3_free(zOrig);
      sqlite3_free(zReturnBranch);
      sqlite3_free(zWorking);
      sqlite3_free(aReplay);
      sqlite3_result_error(context,
        "rebase working branch already exists", -1);
      return;
    }
  }

  {
    int dirty = 0;
    rc = rebaseBranchHasUncommittedWork(db, zReturnBranch, &dirty);
    if( rc!=SQLITE_OK ) goto fail;
    if( dirty ){
      /* Empty disables both the mirror and the end-of-rebase clear. The rebase
      ** still runs; only resuming it after a reopen is given up. */
      zReturnBranch[0] = 0;
    }
  }
  if( strlen(zReturnBranch)>=WS_REBASE_BRANCH_LEN ){
    sqlite3_free(zOrig);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(aReplay);
    sqlite3_result_error(context,
      "cannot start interactive rebase: default branch name exceeds "
      "the 63-byte persisted-state limit", -1);
    return;
  }

  rc = doltliteFlushCatalogToHash(db, &preRebaseCat);
  if( rc!=SQLITE_OK ) goto fail;

  rc = chunkStoreAddBranch(cs, zWorking, &upstreamHash);
  if( rc!=SQLITE_OK ){
    zFailMsg = "rebase working branch already exists";
    goto fail;
  }
  bWorkingBranchCreated = 1;
  /* Persist the working branch now. The sub-ops below (checkout, plan table)
  ** each reload persisted refs at their lock — the VC fresh-read that closes
  ** the concurrency window — which would discard this still-in-memory branch.
  ** Committing it makes the reload re-read it instead. */
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ) goto fail;
  rc = doltliteCheckoutBranchForRebase(db, zWorking);
  if( rc!=SQLITE_OK ) goto fail;

  rc = rebaseCreateAndPopulatePlanTable(db, aReplay, nReplay);
  if( rc!=SQLITE_OK ){
    zFailMsg = "failed to create dolt_rebase table";
    goto fail;
  }

  rc = doltliteSetSessionRebaseState(db, 1, &preRebaseCat, &headHash,
                                     zOrig, zReturnBranch);
  if( rc==SQLITE_OK ) rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ) goto fail;
  rc = doltliteVcSealBranchStyleTxn(db);
  if( rc!=SQLITE_OK ) goto fail;
  sqlite3ExpirePreparedStatements(db, 0);
  sqlite3ResetAllSchemasOfConnection(db);

  sqlite3_free(zOrig);
  sqlite3_free(zReturnBranch);
  sqlite3_free(aReplay);
  {
    char *zMsg = sqlite3_mprintf(
      "interactive rebase started on branch %s; adjust the rebase plan "
      "in the dolt_rebase table, then continue rebasing by calling "
      "dolt_rebase('--continue')", zWorking);
    sqlite3_free(zWorking);
    if( zMsg ) sqlite3_result_text(context, zMsg, -1, sqlite3_free);
    else sqlite3_result_text(context, "interactive rebase started", -1, SQLITE_STATIC);
  }
  return;

fail:
  if( bWorkingBranchCreated ){
    int recoveryRc = rebaseDiscardWorkingBranch(db, zOrig, zWorking);
    if( recoveryRc!=SQLITE_OK ){
      rc = recoveryRc;
      zFailMsg = 0;
    }
  }
  sqlite3_free(zOrig);
  sqlite3_free(zReturnBranch);
  sqlite3_free(zWorking);
  sqlite3_free(aReplay);
  if( zFailMsg ){
    sqlite3_result_error(context, zFailMsg, -1);
  }else{
    sqlite3_result_error_code(context, rc);
  }
}

static void doltliteRebaseInteractiveAbort(
  sqlite3_context *context,
  sqlite3 *db
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 isRebasing = 0;
  const char *zOrigBranchConst = 0;
  const char *zReturnBranchConst = 0;
  char *zReturnBranch = 0;
  char *zOrigBranch = 0;
  char *zWorking = 0;
  int rc;
  int rc2;

  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0,
                                &zOrigBranchConst, &zReturnBranchConst);
  if( !isRebasing || !zOrigBranchConst || !zReturnBranchConst ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  zOrigBranch = sqlite3_mprintf("%s", zOrigBranchConst);
  zReturnBranch = sqlite3_mprintf("%s", zReturnBranchConst);
  zWorking = rebaseBuildWorkingBranchName(zOrigBranchConst);
  if( !zReturnBranch || !zWorking || !zOrigBranch ){
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    sqlite3_result_error_code(context, SQLITE_NOMEM);
    return;
  }

  /* Capture names before claim clears session rebase state. */
  rc = rebaseClaimActiveEndRetry(db);
  if( rc==SQLITE_DONE ){
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    rebaseResultRecoveryFailure(context, rc);
    return;
  }

  rc = rebaseCleanupAfterClaim(db, zOrigBranch, zWorking);
  if( cs && zReturnBranch && zReturnBranch[0] ){
    rc2 = rebaseRestoreReturnBranchWorkingState(db, zReturnBranch);
    /* A concurrent --continue may already have finished; if the temporary
    ** working branch is gone, treat restore failure as the peer winning. */
    if( rc2!=SQLITE_OK
     && zWorking && zWorking[0]
     && chunkStoreFindBranch(cs, zWorking, 0)==SQLITE_NOTFOUND ){
      rc2 = SQLITE_OK;
    }
    rebaseKeepFirstError(&rc, rc2);
  }
  rc2 = doltlitePersistWorkingSet(db);
  rebaseKeepFirstError(&rc, rc2);

  rc2 = doltliteVcSealBranchStyleTxn(db);
  rebaseKeepFirstError(&rc, rc2);
  if( rc!=SQLITE_OK ){
    /* Peer finished cleanup after we claimed: report lost race, not a broken
    ** recovery that leaves the user stuck. */
    if( cs && zWorking && zWorking[0]
     && chunkStoreFindBranch(cs, zWorking, 0)==SQLITE_NOTFOUND ){
      sqlite3_free(zReturnBranch);
      sqlite3_free(zWorking);
      sqlite3_free(zOrigBranch);
      sqlite3_result_error(context, "no rebase in progress", -1);
      return;
    }
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    rebaseResultRecoveryFailure(context, rc);
    return;
  }

  sqlite3_free(zReturnBranch);
  sqlite3_free(zWorking);
  sqlite3_free(zOrigBranch);
  sqlite3_result_text(context, "Interactive rebase aborted", -1, SQLITE_STATIC);
}

static void doltliteRebaseInteractiveContinue(
  sqlite3_context *context,
  sqlite3 *db
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 isRebasing = 0;
  const char *zOrigBranchConst = 0;
  const char *zReturnBranchConst = 0;
  char *zReturnBranch = 0;
  char *zOrigBranch = 0;
  char *zWorking = 0;
  RebasePlanRow *aPlan = 0;
  int nPlan = 0;
  int rc;
  int rc2;
  int recoveryRc;
  int i;
  int bPlanDropped = 0;
  int bSkipConstraintDetect = (!db->autoCommit || db->pSavepoint!=0);
  ProllyHash curCat;
  ProllyHash curHead;
  ProllyHash expectedOrigHead;
  RebaseFinalizeRefsCtx refsCtx;
  char *zPlanErr = 0;

  memset(&curCat, 0, sizeof(curCat));
  memset(&curHead, 0, sizeof(curHead));
  memset(&expectedOrigHead, 0, sizeof(expectedOrigHead));

  doltliteGetSessionRebaseState(db, &isRebasing, 0, &expectedOrigHead,
                                &zOrigBranchConst, &zReturnBranchConst);
  if( !isRebasing || !zOrigBranchConst || !zReturnBranchConst ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  zOrigBranch = sqlite3_mprintf("%s", zOrigBranchConst);
  zReturnBranch = sqlite3_mprintf("%s", zReturnBranchConst);
  zWorking = rebaseBuildWorkingBranchName(zOrigBranchConst);
  if( !zReturnBranch || !zWorking || !zOrigBranch ){ rc = SQLITE_NOMEM; goto abort_err; }

  (void)sqlite3_exec(db, "SELECT 1 FROM main.dolt_rebase LIMIT 0", 0, 0, 0);

  rc = doltliteValidateRebasePlanTable(db, &zPlanErr);
  if( rc!=SQLITE_OK ){
    if( zPlanErr ) sqlite3_result_error(context, zPlanErr, -1);
    sqlite3_free(zPlanErr);
    goto abort_err_silent;
  }

  rc = rebaseReadPlan(db, &aPlan, &nPlan);
  if( rc!=SQLITE_OK ) goto abort_err;

  rc = doltliteVcSealBranchStyleTxnMaybeKeepTopLevelSavepoint(db);
  if( rc!=SQLITE_OK ) goto abort_err;

  rc = doltliteEnsureWriteTxnAndSavepoints(db);
  if( rc!=SQLITE_OK ) goto abort_err;

  /* Reject any unknown plan verb (e.g. a typo in dolt_rebase.action) instead of
  ** silently treating it as a pick. The rebase stays in progress so the plan can
  ** be corrected and --continue retried, so name the offending action rather
  ** than reporting a bare failure. */
  for(i=0; i<nPlan; i++){
    const char *zAct = aPlan[i].zAction;
    if( strcmp(zAct,"pick")!=0 && strcmp(zAct,"reword")!=0
     && strcmp(zAct,"squash")!=0 && strcmp(zAct,"fixup")!=0
     && strcmp(zAct,"drop")!=0 ){
      char *zMsg = sqlite3_mprintf(
          "unknown rebase action \"%s\": expected pick, reword, squash, "
          "fixup or drop", zAct);
      rc = SQLITE_ERROR;
      if( zMsg ){
        sqlite3_result_error(context, zMsg, -1);
        sqlite3_free(zMsg);
      }else{
        sqlite3_result_error_nomem(context);
      }
      goto abort_err_silent;
    }
  }

  i = 0;
  while( i < nPlan && strcmp(aPlan[i].zAction, "drop")==0 ) i++;
  if( i < nPlan
   && strcmp(aPlan[i].zAction, "pick")!=0
   && strcmp(aPlan[i].zAction, "reword")!=0 ){
    rc = SQLITE_ERROR;
    sqlite3_result_error(context,
      "first non-drop action must be pick or reword", -1);
    goto abort_err_silent;
  }

  /* Claim before replaying so a concurrent --abort loses cleanly with
  ** "no rebase in progress" instead of both failing mid-recovery. */
  rc = rebaseClaimActiveEndRetry(db);
  if( rc==SQLITE_DONE ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    goto abort_err_silent;
  }
  if( rc!=SQLITE_OK ) goto abort_err;
  bPlanDropped = 1;

  rc = doltliteFlushCatalogToHash(db, &curCat);
  if( rc!=SQLITE_OK ) goto abort_err;
  doltliteGetSessionHead(db, &curHead);

  i = 0;
  while( i < nPlan ){
    int j;

    while( i < nPlan && strcmp(aPlan[i].zAction, "drop")==0 ) i++;
    if( i >= nPlan ) break;

    rc = rebaseReplayPlanGroup(db, aPlan, nPlan, i, &curCat, &curHead, &j,
                               bSkipConstraintDetect);
    if( rc==SQLITE_CONSTRAINT ) goto abort_err_conflict;
    if( rc!=SQLITE_OK ) goto abort_err;
    i = j;
  }

  memset(&refsCtx, 0, sizeof(refsCtx));
  refsCtx.zOrigBranch = zOrigBranch;
  refsCtx.zWorkingBranch = zWorking;
  refsCtx.pExpectedOrigHead = &expectedOrigHead;
  refsCtx.pCurHead = &curHead;
  refsCtx.pCurCat = &curCat;
  {
    DoltliteBranchExpectation expected[2];
    expected[0].zBranch = zOrigBranch;
    expected[0].pTip = &expectedOrigHead;
    expected[1].zBranch = zWorking;
    expected[1].pTip = &curHead;
    doltliteTestCrashFinalize("rebase");
    rc = doltliteMutateRefsExpected(
        db, expected, 2, rebaseFinalizeContinueRefs, &refsCtx);
  }
  if( rc!=SQLITE_OK ) goto abort_err;

  /* Claim already cleared durable isRebasing; keep session clear and finish
  ** checkout / working-branch deletion / return-branch restore. */
  rc = doltliteClearSessionRebaseState(db);
  if( rc==SQLITE_OK ) rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ) goto abort_err;

  /* curCat is already the exact flushed catalog installed by the replay and
  ** persisted above. Under a caller savepoint, the live SQLite schema is
  ** transitional until checkout completes, so do not re-prepare and
  ** re-serialize it merely to capture the branch being discarded. */
  rc = doltliteCheckoutBranchForRebaseWithOldCatalog(
      db, zOrigBranch, &curCat);
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = doltliteMutateRefs(db, rebaseDeleteWorkingBranchRefs, zWorking);
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = rebaseRestoreReturnBranchWorkingState(db, zReturnBranch);
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = doltlitePersistWorkingSet(db);
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = doltliteVcSealBranchStyleTxnMaybeKeepTopLevelSavepoint(db);
  if( rc!=SQLITE_OK ) goto abort_err;

  rebaseFreePlan(aPlan, nPlan);
  {
    char *zMsg = sqlite3_mprintf(
      "Successfully rebased and updated refs/heads/%s", zOrigBranch);
    sqlite3_free(zOrigBranch);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    if( zMsg ) sqlite3_result_text(context, zMsg, -1, sqlite3_free);
    else sqlite3_result_text(context, "Successfully rebased", -1, SQLITE_STATIC);
  }
  return;

abort_err_conflict:
  rebaseFreePlan(aPlan, nPlan);
  recoveryRc = rebaseAbortConflictedContinue(
      db, zOrigBranch, zReturnBranch, zWorking);
  if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE ){
    (void)sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  sqlite3_free(zOrigBranch);
  sqlite3_free(zReturnBranch);
  sqlite3_free(zWorking);
  if( recoveryRc!=SQLITE_OK ){
    rebaseResultRecoveryFailure(context, recoveryRc);
  }else{
    sqlite3_result_error(context,
      "data conflicts from rebase — rebase has been aborted", -1);
  }
  return;

abort_err:
  rebaseFreePlan(aPlan, nPlan);
  if( !bPlanDropped ){
    sqlite3_free(zOrigBranch);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_result_error(context, "rebase failed", -1);
    return;
  }
  recoveryRc = SQLITE_OK;
  recoveryRc = rebaseDiscardWorkingBranch(
      db, zOrigBranch ? zOrigBranch : "main", zWorking);
  if( cs && zReturnBranch && zReturnBranch[0] ){
    rc2 = rebaseRestoreReturnBranchWorkingState(db, zReturnBranch);
    if( rc2!=SQLITE_OK
     && zWorking && zWorking[0]
     && chunkStoreFindBranch(cs, zWorking, 0)==SQLITE_NOTFOUND ){
      rc2 = SQLITE_OK;
    }
    rebaseKeepFirstError(&recoveryRc, rc2);
  }
  if( recoveryRc!=SQLITE_OK
   && cs && zWorking && zWorking[0]
   && chunkStoreFindBranch(cs, zWorking, 0)==SQLITE_NOTFOUND ){
    recoveryRc = SQLITE_OK;
  }
  sqlite3_free(zOrigBranch);
  sqlite3_free(zReturnBranch);
  sqlite3_free(zWorking);
  if( recoveryRc!=SQLITE_OK ){
    rebaseResultRecoveryFailure(context, recoveryRc);
  }else{
    sqlite3_result_error(context,
      "rebase failed — branch restored to pre-rebase state", -1);
  }
  return;

abort_err_silent:
  rebaseFreePlan(aPlan, nPlan);
  sqlite3_free(zOrigBranch);
  sqlite3_free(zReturnBranch);
  sqlite3_free(zWorking);
}

static void doltliteRebaseFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zArg0;
  int sealTopLevel = db->pSavepoint!=0 && db->nSavepoint==0;
  int keepTopLevelSavepoint = 0;

  if( !cs ){ sqlite3_result_error(context, "no database", -1); goto rebase_cleanup; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_rebase('upstream_branch')", -1);
    goto rebase_cleanup;
  }

  zArg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !zArg0 ){
    sqlite3_result_error(context, "upstream ref required", -1);
    goto rebase_cleanup;
  }

  if( strcmp(zArg0, "--abort")==0 ){
    doltliteRebaseInteractiveAbort(context, db);
    goto rebase_cleanup;
  }
  if( strcmp(zArg0, "--continue")==0 ){
    keepTopLevelSavepoint = 1;
    doltliteRebaseInteractiveContinue(context, db);
    goto rebase_cleanup;
  }
  if( strcmp(zArg0, "-i")==0 || strcmp(zArg0, "--interactive")==0 ){
    const char *zUpstream;
    keepTopLevelSavepoint = 1;
    if( argc<2 ){
      sqlite3_result_error(context,
        "interactive rebase requires upstream branch: "
        "dolt_rebase('-i', 'upstream')", -1);
      goto rebase_cleanup;
    }
    if( argc!=2 ){
      sqlite3_result_error(context,
        "interactive rebase takes exactly one upstream branch", -1);
      goto rebase_cleanup;
    }
    zUpstream = (const char*)sqlite3_value_text(argv[1]);
    if( !zUpstream ){
      sqlite3_result_error(context, "upstream ref required", -1);
      goto rebase_cleanup;
    }
    doltliteRebaseInteractiveStart(context, db, zUpstream);
    goto rebase_cleanup;
  }

  if( zArg0[0]=='-' ){
    doltliteCmdResultUnknownOption(context, zArg0);
    goto rebase_cleanup;
  }
  if( argc!=1 ){
    sqlite3_result_error(context,
      "too many positional arguments to dolt_rebase", -1);
    goto rebase_cleanup;
  }

  {
    char *zFinalMessage = 0;
    int rc = doltliteRebaseLinearReplay(db, context, zArg0, &zFinalMessage);
    if( rc==SQLITE_OK && zFinalMessage ){
      sqlite3_result_text(context, zFinalMessage, -1, sqlite3_free);
    }
  }

rebase_cleanup:
  if( sealTopLevel && !keepTopLevelSavepoint ){
    (void)doltliteVcSealTopLevelSavepointTxn(db);
  }
}


int doltliteRebaseRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_rebase", -1,
                                 DOLTLITE_COMMAND_FUNC_FLAGS, 0,
                                 doltliteRebaseFunc, 0, 0);
}

#endif
