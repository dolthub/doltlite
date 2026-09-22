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
#include <stdio.h>
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
static int rebasePauseLinearConflict(
  sqlite3 *db,
  sqlite3_context *context,
  ChunkStore *cs,
  const ProllyHash *aReplay,
  int iConflict,
  int nReplay,
  const ProllyHash *pOrigCat,
  const ProllyHash *pOrigHead,
  const char *zOrig,
  const char *zMessage,
  int *pGraphLocked
);

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
  u8 flags = 0;
  int rc;

  if( !cs || !zBranch || !zBranch[0] ) return SQLITE_OK;
  rc = doltliteBranchWorkingSetRebaseFlags(db, zBranch, &flags);
  if( rc!=SQLITE_OK ) return rc;
  if( flags & WS_REBASE_FLAG_META_MIRROR ){
    return doltliteClearBranchRebaseMetadata(db, zBranch);
  }
  if( (flags & WS_REBASE_FLAG_ACTIVE)==0 ) return SQLITE_OK;
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

typedef struct RebaseWalkCommit RebaseWalkCommit;
struct RebaseWalkCommit {
  ProllyHash hash;
  ProllyHash aParents[DOLTLITE_MAX_PARENTS];
  int nParents;
  i64 timestamp;
  int height;
  int replay;
  int queued;
};

static int rebaseWalkFind(
  const RebaseWalkCommit *a,
  int n,
  const ProllyHash *pHash
){
  int i;
  for(i=0; i<n; i++){
    if( prollyHashCompare(&a[i].hash, pHash)==0 ) return i;
  }
  return -1;
}

/* Higher generation, then newer timestamp. Equal keys are not ordered, so
** the heap keeps whichever commit was pushed first, matching Dolt's walk. */
static int rebaseHeapLess(
  const RebaseWalkCommit *aAll,
  const int *h,
  int i,
  int j
){
  const RebaseWalkCommit *ai = &aAll[h[i]];
  const RebaseWalkCommit *aj = &aAll[h[j]];
  if( ai->height!=aj->height ) return ai->height>aj->height;
  if( ai->timestamp!=aj->timestamp ) return ai->timestamp>aj->timestamp;
  return 0;
}

static void rebaseHeapSwap(int *h, int i, int j){
  int tmp = h[i];
  h[i] = h[j];
  h[j] = tmp;
}

static void rebaseHeapUp(const RebaseWalkCommit *aAll, int *h, int j){
  while( 1 ){
    int i = (j - 1) / 2;
    if( i==j || !rebaseHeapLess(aAll, h, j, i) ) break;
    rebaseHeapSwap(h, i, j);
    j = i;
  }
}

static void rebaseHeapDown(const RebaseWalkCommit *aAll, int *h, int i0, int n){
  int i = i0;
  while( 1 ){
    int j1 = 2*i + 1;
    int j, j2;
    if( j1>=n || j1<0 ) break;
    j = j1;
    j2 = j1 + 1;
    if( j2<n && rebaseHeapLess(aAll, h, j2, j1) ) j = j2;
    if( !rebaseHeapLess(aAll, h, j, i) ) break;
    rebaseHeapSwap(h, i, j);
    i = j;
  }
}

static void rebaseHeapPush(
  const RebaseWalkCommit *aAll,
  int *h,
  int *pn,
  int idx
){
  int n = *pn;
  h[n] = idx;
  *pn = n + 1;
  rebaseHeapUp(aAll, h, n);
}

static int rebaseHeapPop(const RebaseWalkCommit *aAll, int *h, int *pn){
  int n = *pn - 1;
  int out;
  rebaseHeapSwap(h, 0, n);
  rebaseHeapDown(aAll, h, 0, n);
  out = h[n];
  *pn = n;
  return out;
}

/* Dolt replays the dot-dot walk with merges removed: higher generation
** and newer timestamps come first, then that list is reversed. A reversed
** breadth-first walk plays a side-branch child before its parent when a
** merge reaches that parent by a longer path, and replaying the whole
** first-parent chain before every other commit plays a later mainline
** commit before the merge's other parent. */
static int rebaseOrderReplayCommits(
  sqlite3 *db,
  const ProllyHash *pHeadHash,
  ProllyHashSet *pUpstream,
  ProllyHash *aReplay,
  int nReplay
){
  RebaseWalkCommit *aAll = 0;
  ProllyHash *queue = 0;
  ProllyHash *aNewest = 0;
  ProllyHashSet seen;
  int *aKnown = 0;
  int *aHeap = 0;
  int nAll = 0, nAlloc = 0;
  int qHead = 0, qTail = 0, qAlloc = 0;
  int nHeap = 0;
  int nNewest = 0;
  int seenInit = 0;
  int nKnown = 0;
  int rc = SQLITE_OK;
  int i;

  memset(&seen, 0, sizeof(seen));
  if( nReplay<=1 ) return SQLITE_OK;

  qAlloc = 16;
  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ) return SQLITE_NOMEM;
  queue[qTail++] = *pHeadHash;
  rc = prollyHashSetInit(&seen, nReplay);
  if( rc!=SQLITE_OK ) goto done;
  seenInit = 1;

  while( qHead<qTail ){
    ProllyHash cur = queue[qHead++];
    DoltliteCommit c;
    RebaseWalkCommit *p;
    int nParents;
    if( prollyHashIsEmpty(&cur) ) continue;
    if( prollyHashSetContains(pUpstream, &cur) ) continue;
    if( prollyHashSetContains(&seen, &cur) ) continue;
    rc = prollyHashSetAdd(&seen, &cur);
    if( rc!=SQLITE_OK ) goto done;

    memset(&c, 0, sizeof(c));
    rc = doltliteLoadCommit(db, &cur, &c);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&c);
      goto done;
    }
    if( nAll>=nAlloc ){
      int nNew = nAlloc ? nAlloc*2 : 16;
      RebaseWalkCommit *tmp = sqlite3_realloc(
          aAll, nNew*(int)sizeof(RebaseWalkCommit));
      if( !tmp ){
        doltliteCommitClear(&c);
        rc = SQLITE_NOMEM;
        goto done;
      }
      aAll = tmp;
      nAlloc = nNew;
    }
    p = &aAll[nAll++];
    memset(p, 0, sizeof(*p));
    p->hash = cur;
    p->timestamp = c.timestamp;
    p->height = -1;
    nParents = doltliteCommitParentCount(&c);
    if( nParents>DOLTLITE_MAX_PARENTS ) nParents = DOLTLITE_MAX_PARENTS;
    p->nParents = nParents;
    p->replay = nParents<=1;
    for(i=0; i<nParents; i++){
      const ProllyHash *pp = doltliteCommitParentHash(&c, i);
      if( pp ) p->aParents[i] = *pp;
    }
    for(i=0; i<nParents; i++){
      if( prollyHashIsEmpty(&p->aParents[i]) ) continue;
      if( prollyHashSetContains(pUpstream, &p->aParents[i]) ) continue;
      if( prollyHashSetContains(&seen, &p->aParents[i]) ) continue;
      if( qTail>=qAlloc ){
        int nNew = qAlloc*2;
        ProllyHash *tmp = sqlite3_realloc(queue, nNew*(int)sizeof(ProllyHash));
        if( !tmp ){
          doltliteCommitClear(&c);
          rc = SQLITE_NOMEM;
          goto done;
        }
        queue = tmp;
        qAlloc = nNew;
      }
      queue[qTail++] = p->aParents[i];
    }
    doltliteCommitClear(&c);
  }

  if( nAll>0 ){
    aKnown = sqlite3_malloc(nAll * (int)sizeof(int));
    if( !aKnown ){ rc = SQLITE_NOMEM; goto done; }
    memset(aKnown, 0, (size_t)nAll * sizeof(int));
  }
  while( nKnown<nAll ){
    int progressed = 0;
    for(i=0; i<nAll; i++){
      int ready = 1;
      int maxH = 0;
      int pidx;
      if( aKnown[i] ) continue;
      for(pidx=0; pidx<aAll[i].nParents; pidx++){
        int pi;
        if( prollyHashIsEmpty(&aAll[i].aParents[pidx]) ) continue;
        if( prollyHashSetContains(pUpstream, &aAll[i].aParents[pidx]) ) continue;
        pi = rebaseWalkFind(aAll, nAll, &aAll[i].aParents[pidx]);
        if( pi<0 || aAll[pi].height<0 ){
          ready = 0;
          break;
        }
        if( aAll[pi].height>maxH ) maxH = aAll[pi].height;
      }
      if( !ready ) continue;
      aAll[i].height = maxH + 1;
      aKnown[i] = 1;
      nKnown++;
      progressed = 1;
    }
    if( !progressed ){
      rc = SQLITE_CORRUPT;
      goto done;
    }
  }

  aHeap = sqlite3_malloc(nAll>0 ? nAll*(int)sizeof(int) : (int)sizeof(int));
  aNewest = sqlite3_malloc(nReplay * (int)sizeof(ProllyHash));
  if( !aHeap || !aNewest ){ rc = SQLITE_NOMEM; goto done; }
  {
    int headIdx = rebaseWalkFind(aAll, nAll, pHeadHash);
    int pidx;
    if( headIdx<0 ){ rc = SQLITE_CORRUPT; goto done; }
    aAll[headIdx].queued = 1;
    rebaseHeapPush(aAll, aHeap, &nHeap, headIdx);
    while( nHeap>0 ){
      int idx = rebaseHeapPop(aAll, aHeap, &nHeap);
      for(pidx=0; pidx<aAll[idx].nParents; pidx++){
        int pi;
        if( prollyHashIsEmpty(&aAll[idx].aParents[pidx]) ) continue;
        if( prollyHashSetContains(pUpstream, &aAll[idx].aParents[pidx]) ) continue;
        pi = rebaseWalkFind(aAll, nAll, &aAll[idx].aParents[pidx]);
        if( pi<0 ){ rc = SQLITE_CORRUPT; goto done; }
        if( aAll[pi].queued ) continue;
        aAll[pi].queued = 1;
        rebaseHeapPush(aAll, aHeap, &nHeap, pi);
      }
      if( !aAll[idx].replay ) continue;
      if( nNewest>=nReplay ){ rc = SQLITE_CORRUPT; goto done; }
      aNewest[nNewest++] = aAll[idx].hash;
    }
  }
  if( nNewest!=nReplay ){ rc = SQLITE_CORRUPT; goto done; }
  for(i=0; i<nNewest; i++) aReplay[i] = aNewest[nNewest-1-i];

done:
  sqlite3_free(queue);
  sqlite3_free(aAll);
  sqlite3_free(aKnown);
  sqlite3_free(aHeap);
  sqlite3_free(aNewest);
  if( seenInit ) prollyHashSetFree(&seen);
  return rc;
}

/* Dirty zBranch. Return-branch mirror is loadable only when workingCommit
** equals that HEAD; otherwise overlay rebase metadata so restore matches. */
static int rebaseBranchHasUncommittedWork(
  sqlite3 *db,
  const char *zBranch,
  int *pDirty
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit c;
  ProllyHash headHash, wsHash, wsCat, wsCommit;
  int rc;

  *pDirty = 0;
  if( !cs || !zBranch || !zBranch[0] ) return SQLITE_OK;
  rc = chunkStoreFindBranch(cs, zBranch, &headHash);
  if( rc!=SQLITE_OK ) return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
  memset(&wsHash, 0, sizeof(wsHash));
  memset(&wsCat, 0, sizeof(wsCat));
  memset(&wsCommit, 0, sizeof(wsCommit));
  rc = chunkStoreGetBranchWorkingSet(cs, zBranch, &wsHash);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&wsHash) ) return SQLITE_OK;
  rc = chunkStoreReadBranchWorkingCatalog(cs, zBranch, &wsCat, &wsCommit);
  if( rc!=SQLITE_OK ) return rc;
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

  /* BFS from HEAD: walk all parents, replay only non-merge commits. */
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

  rc = rebaseOrderReplayCommits(db, pHeadHash, &upstreamAncestors,
                                aReplay, nReplay);
  if( rc!=SQLITE_OK ) goto cleanup;

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
  char **pzFinalMessage,
  int *pbPaused
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
  char *zApplyErr = 0;
  int bConflict = 0;
  int bViolation = 0;
  assert( db!=0 && context!=0 && zUpstream!=0 && pzFinalMessage!=0 );
  assert( pbPaused!=0 );
  cs = doltliteGetChunkStore(db);
  sealTopLevel = db->pSavepoint!=0 && db->nSavepoint==0;

  *pzFinalMessage = 0;
  *pbPaused = 0;
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
    int nViolations = 0;
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
        &curHead, 0,
        replayCommit.zMessage ? replayCommit.zMessage : "",
        0, 0, 0, 1, &nConflicts, &nViolations, &zApplyErr, hexBuf);

    doltliteCommitClear(&replayCommit);
    doltliteCommitClear(&parentCommit);
    doltliteCommitClear(&curHeadCommit);

    if( rc==SQLITE_DONE ){
      sqlite3_free(zApplyErr);
      zApplyErr = 0;
      continue;
    }
    if( rc!=SQLITE_OK ) goto rollback;
    /* Violations cannot be kept and continued. Data conflicts inside
    ** BEGIN can: autocommit and a nested savepoint still abort. */
    if( nViolations>0 ){ bViolation = 1; rc = SQLITE_ERROR; goto rollback; }
    if( nConflicts>0 ){
      if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_PLAIN ){
        int pauseRc = rebasePauseLinearConflict(
            db, context, cs, aReplay, i, nReplay,
            &origCat, &headHash, zOrig, zFailedMsg, &graphLocked);
        if( pauseRc==SQLITE_OK ){
          *pbPaused = 1;
          doltliteCommitClear(&upstreamCommit);
          doltliteCommitClear(&finalCommit);
          doltliteCommitClear(&origCommit);
          sqlite3_free(aReplay);
          sqlite3_free(zFailedMsg);
          sqlite3_free(zApplyErr);
          sqlite3_free(zOrig);
          sqlite3_free(zWorking);
          return SQLITE_ERROR;
        }
      }
      bConflict = 1;
      rc = SQLITE_ERROR;
      goto rollback;
    }
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
  sqlite3_free(zApplyErr);
  rc = doltliteVcSealEnclosingTxn(db);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    return rc;
  }
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
    if( workingCreated && !prollyHashIsEmpty(&origCat) ){
      doltliteAdoptRollbackBaseline(db, &origCat);
    }
  }
  if( graphLocked ) chunkStoreUnlock(cs);
  sqlite3_free(aReplay);
  {
    char *zErr;
    if( zApplyErr ){
      zErr = zApplyErr;
      zApplyErr = 0;
    }else if( bConflict && zFailedMsg && zFailedMsg[0] ){
      zErr = sqlite3_mprintf(
          "conflict rebasing \"%s\"; rebase aborted, branch restored to pre-rebase state",
          zFailedMsg);
    }else if( bViolation && zFailedMsg && zFailedMsg[0] ){
      zErr = sqlite3_mprintf(
          "constraint violations rebasing \"%s\"; rebase aborted, "
          "branch restored to pre-rebase state",
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
  sqlite3_free(zApplyErr);
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
      "commit_message TEXT NOT NULL)");
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

    if( sqlite3_column_type(pStmt, 3)==SQLITE_NULL ){
      rc = SQLITE_CONSTRAINT_NOTNULL;
      goto fail;
    }
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

/* SwitchCatalog does not update the txn rollback snapshot. Pin HEAD's
** catalog so ROLLBACK of an enclosing BEGIN keeps the restored branch. */
static void rebaseAdoptRestoredCatalog(sqlite3 *db){
  ProllyHash cat;
  memset(&cat, 0, sizeof(cat));
  if( doltliteGetHeadCatalogHash(db, &cat)==SQLITE_OK
   && !prollyHashIsEmpty(&cat) ){
    doltliteAdoptRollbackBaseline(db, &cat);
  }
}

static int rebaseWritePlanRows(
  sqlite3 *db,
  const RebasePlanRow *aPlan,
  int nPlan
){
  int rc;
  int i;

  rc = sqlite3_exec(db,
    "DROP TABLE IF EXISTS main.dolt_rebase;"
    "CREATE TABLE main.dolt_rebase("
    "  rebase_order REAL PRIMARY KEY,"
    "  action TEXT,"
    "  commit_hash TEXT,"
    "  commit_message TEXT NOT NULL"
    ")", 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nPlan; i++){
    char zHex[PROLLY_HASH_SIZE*2+1];
    char zOrder[64];
    char *zSql;
    doltliteHashToHex(&aPlan[i].commitHash, zHex);
    sqlite3_snprintf(sizeof(zOrder), zOrder, "%!.17g", aPlan[i].order);
    zSql = sqlite3_mprintf(
      "INSERT INTO main.dolt_rebase VALUES (%s, %Q, %Q, %Q)",
      zOrder,
      aPlan[i].zAction ? aPlan[i].zAction : "pick",
      zHex,
      aPlan[i].zCommitMessage ? aPlan[i].zCommitMessage : "");
    if( !zSql ) return SQLITE_NOMEM;
    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int rebaseEndBusyRetry(sqlite3 *db);
static int rebaseRetryableRc(int rc);

/* CAS reject must not delete the plan or working branch; --abort still
** needs them, matching Dolt. */
static int rebaseRestoreInProgress(
  sqlite3 *db,
  const RebasePlanRow *aPlan,
  int nPlan,
  const ProllyHash *pPreRebaseCat,
  const ProllyHash *pExpectedOrigHead,
  const char *zOrigBranch,
  const char *zReturnBranch
){
  int rc;
  /* Retry the whole restore: later writes can hit the CAS-winning peer's
  ** lock, and aborting mid-restore reports a false unrestored state. */
  db->busyHandler.nBusy = 0;
  do {
    rc = rebaseWritePlanRows(db, aPlan, nPlan);
    if( rc==SQLITE_OK ){
      /* Persist remirrors the working catalog onto the return branch unless
      ** META_MIRROR, which would clobber uncommitted work. Overlay metadata. */
      u8 flags = (u8)(WS_REBASE_FLAG_ACTIVE | WS_REBASE_FLAG_META_MIRROR);
      rc = doltliteSetSessionRebaseState(db, flags, pPreRebaseCat, pExpectedOrigHead,
                                         zOrigBranch, zReturnBranch);
    }
    if( rc==SQLITE_OK ) rc = doltlitePersistWorkingSet(db);
    if( rc==SQLITE_OK ) rc = doltliteVcSealBranchStyleTxn(db);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  if( rc==SQLITE_OK ){
    sqlite3ExpirePreparedStatements(db, 0);
    sqlite3ResetAllSchemasOfConnection(db);
  }
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
    "  commit_message TEXT NOT NULL"
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

/* Data conflict inside BEGIN. Leave the working branch, plan, and
** conflict tables in place. META_MIRROR keeps a later save from copying
** the conflicted catalog onto the return branch. */
static int rebasePauseLinearConflict(
  sqlite3 *db,
  sqlite3_context *context,
  ChunkStore *cs,
  const ProllyHash *aReplay,
  int iConflict,
  int nReplay,
  const ProllyHash *pOrigCat,
  const ProllyHash *pOrigHead,
  const char *zOrig,
  const char *zMessage,
  int *pGraphLocked
){
  char zHex[PROLLY_HASH_SIZE*2+1];
  char *zReturn = 0;
  char *zErr = 0;
  u8 flags;
  int rc;

  if( !zOrig || !zOrig[0] || strlen(zOrig)>=WS_REBASE_BRANCH_LEN ){
    return SQLITE_TOOBIG;
  }
  zReturn = sqlite3_mprintf("%s",
      cs ? chunkStoreGetDefaultBranch(cs) : "");
  if( !zReturn ) return SQLITE_NOMEM;
  if( zReturn[0]==0 || strlen(zReturn)>=WS_REBASE_BRANCH_LEN ){
    sqlite3_free(zReturn);
    return SQLITE_TOOBIG;
  }

  rc = doltliteSetSessionPendingReplayCommit(db, 0);
  if( rc==SQLITE_OK ){
    rc = rebaseCreateAndPopulatePlanTable(
        db, aReplay + iConflict, nReplay - iConflict);
  }
  flags = (u8)(WS_REBASE_FLAG_ACTIVE | WS_REBASE_FLAG_PAUSED
               | WS_REBASE_FLAG_META_MIRROR);
  if( rc==SQLITE_OK ){
    rc = doltliteSetSessionRebaseState(
        db, flags, pOrigCat, pOrigHead, zOrig, zReturn);
  }
  if( rc!=SQLITE_OK ){
    (void)doltliteClearSessionRebaseState(db);
    sqlite3_free(zReturn);
    return rc;
  }

  if( pGraphLocked && *pGraphLocked && cs ){
    chunkStoreUnlock(cs);
    *pGraphLocked = 0;
  }

  doltliteHashToHex(&aReplay[iConflict], zHex);
  zErr = sqlite3_mprintf(
      "data conflict detected while rebasing commit %s (%s). \n\n"
      "Resolve the conflicts and remove them from the "
      "dolt_conflicts_<table> tables, "
      "then continue the rebase by calling dolt_rebase('--continue')",
      zHex, zMessage ? zMessage : "");
  sqlite3_free(zReturn);
  if( zErr ){
    sqlite3_result_error(context, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(context);
  }
  return SQLITE_OK;
}

static int rebaseApplyPlanRowCatalog(
  sqlite3 *db,
  const RebasePlanRow *pRow,
  const ProllyHash *pCurCat,
  ProllyHash *pMergedCat,
  char **pzMessage,
  char **pzErr
){
  DoltliteCommit parentC, replayC;
  int nConflicts = 0;
  int nViolations = 0;
  int rc;

  *pzMessage = 0;
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
                               &nConflicts, 0, 0, 0, 0, 0,
                               &azReindex, &nReindex, 0, 0);
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
  if( rc==SQLITE_OK && nConflicts==0 ){
    rc = doltliteDetectConstraintViolationsFiltered(
        db, &parentC.catalogHash, 0, 0, 1, &nViolations, pzErr);
  }
  if( rc==SQLITE_OK && (nConflicts>0 || nViolations>0) ){
    rc = SQLITE_CONSTRAINT;
  }
  if( rc==SQLITE_OK ){
    const char *zMessage = replayC.zMessage;
    if( strcmp(pRow->zAction, "reword")==0 && pRow->zCommitMessage[0] ){
      zMessage = pRow->zCommitMessage;
    }
    *pzMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
    if( !*pzMessage ) rc = SQLITE_NOMEM;
  }
  doltliteCommitClear(&parentC);
  doltliteCommitClear(&replayC);
  return rc;
}

static void (*rebaseBeforeAdvanceHook)(void) = 0;

void doltliteTestSetRebaseBeforeAdvanceHook(void (*xHook)(void)){
  rebaseBeforeAdvanceHook = xHook;
}

/* Refresh now at lockDepth==1. A post-CreateAndStoreCommit ForceRefresh
** would drop the pending commit; CompareAndAdvance's reentrant refresh is
** a no-op. */
static int rebaseRefreshWorkingRefs(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  if( rebaseBeforeAdvanceHook ){
    void (*xHook)(void) = rebaseBeforeAdvanceHook;
    rebaseBeforeAdvanceHook = 0;
    xHook();
  }
  if( !cs ) return SQLITE_ERROR;
  return chunkStoreForceRefresh(cs);
}

static int rebaseAdvanceWorkingBranch(
  sqlite3 *db,
  const ProllyHash *pExpectedHead,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash
){
  int rc;
  /* BUSY is lock contention or a moved tip; the caller aborts either as
  ** "source changed". Retry so lock-only BUSY does not abort a claimed rebase. */
  db->busyHandler.nBusy = 0;
  do {
    rc = doltliteCompareAndAdvanceBranch(
        db, pExpectedHead, pNewHead, pCatalogHash, 0);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  return rc;
}

static int rebaseReplayPlanGroup(
  sqlite3 *db,
  RebasePlanRow *aPlan,
  int nPlan,
  int iStart,
  ProllyHash *pCurCat,
  ProllyHash *pCurHead,
  int *piNext,
  char **pzErr
){
  char *combinedMsg = 0;
  ProllyHash startCat;
  int rc;
  int j;

  startCat = *pCurCat;
  rc = rebaseApplyPlanRowCatalog(
      db, &aPlan[iStart], pCurCat, pCurCat, &combinedMsg, pzErr);
  if( rc!=SQLITE_OK ) return rc;

  j = iStart + 1;
  while( j < nPlan
      && (strcmp(aPlan[j].zAction, "squash")==0
       || strcmp(aPlan[j].zAction, "fixup")==0
       || strcmp(aPlan[j].zAction, "drop")==0) ){
    char *zMessage = 0;
    if( strcmp(aPlan[j].zAction, "drop")==0 ){
      j++;
      continue;
    }

    rc = rebaseApplyPlanRowCatalog(
        db, &aPlan[j], pCurCat, pCurCat, &zMessage, pzErr);
    if( rc!=SQLITE_OK ){
      sqlite3_free(combinedMsg);
      return rc;
    }

    if( strcmp(aPlan[j].zAction, "squash")==0 ){
      char *zNew = sqlite3_mprintf("%s\n\n%s", combinedMsg, zMessage);
      sqlite3_free(combinedMsg);
      combinedMsg = zNew;
    }
    sqlite3_free(zMessage);
    if( !combinedMsg ) return SQLITE_NOMEM;
    j++;
  }

  if( prollyHashCompare(pCurCat, &startCat)==0 ){
    sqlite3_free(combinedMsg);
    *piNext = j;
    return SQLITE_OK;
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
      rc = rebaseAdvanceWorkingBranch(db, pCurHead, &newCommit, pCurCat);
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

static int rebaseReadActive(
  sqlite3 *db,
  const char *zWorkingBranch,
  int *pActive
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 isRebasing = 0;
  int rc;

  *pActive = 0;
  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreForceRefresh(cs);
  if( rc==SQLITE_OK ) rc = doltliteLoadWorkingSet(db, zWorkingBranch);
  if( rc==SQLITE_OK ){
    doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, 0, 0);
    *pActive = isRebasing!=0;
  }
  chunkStoreUnlock(cs);
  return rc;
}

/* Retry BUSY, LOCKED, and extended forms (BUSY_SNAPSHOT). Matching only
** primary codes skips the write-txn retry after a peer commit. */
static int rebaseRetryableRc(int rc){
  return (rc&0xff)==SQLITE_BUSY || (rc&0xff)==SQLITE_LOCKED;
}

static int rebaseEndBusyRetry(sqlite3 *db){
  if( db->busyHandler.xBusyHandler ){
    return sqlite3InvokeBusyHandler(&db->busyHandler);
  }
  if( db->busyHandler.nBusy>=200 ) return 0;
  sqlite3OsSleep(db->pVfs, 5000);
  db->busyHandler.nBusy++;
  return 1;
}

static int rebaseReadActiveRetry(
  sqlite3 *db,
  const char *zWorkingBranch,
  int *pActive
){
  int rc;
  db->busyHandler.nBusy = 0;
  do {
    rc = rebaseReadActive(db, zWorkingBranch, pActive);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  return rc;
}

/* Restore claim ownership for a later abort/continue. Rollback persist
** must not share the claim-commit fault point. */
static int rebaseUnclaimActiveEnd(
  sqlite3 *db,
  u8 flags,
  const ProllyHash *pPreRebaseCat,
  const ProllyHash *pRebaseOnto,
  const char *zOrigBranch,
  const char *zReturnBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ) return SQLITE_ERROR;
  rc = doltliteSetSessionRebaseState(db, flags, pPreRebaseCat, pRebaseOnto,
                                     zOrigBranch, zReturnBranch);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreSerializeRefs(cs);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(cs);
}

/* Exclusive claim to end the rebase. DONE if isRebasing already clear;
** OK after this connection clears it. Storage failure restores ownership. */
static int rebaseClaimActiveEnd(
  sqlite3 *db,
  const char *zWorkingBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  const char *zBranch;
  const char *zReturnBranchConst = 0;
  const char *zOrigBranchConst = 0;
  char *zReturnBranch = 0;
  char *zSavedOrig = 0;
  ProllyHash savedPre;
  ProllyHash savedOnto;
  u8 isRebasing = 0;
  u8 savedFlags = 0;
  int rc;
  int locked = 0;
  int mutated = 0;

  memset(&savedPre, 0, sizeof(savedPre));
  memset(&savedOnto, 0, sizeof(savedOnto));
  if( !cs || !db || !zWorkingBranch || !zWorkingBranch[0] ){
    return SQLITE_ERROR;
  }
  zBranch = doltliteGetSessionBranch(db);
  if( !zBranch || !zBranch[0] ) zBranch = "main";

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;
  locked = 1;
  rc = chunkStoreForceRefresh(cs);
  if( rc!=SQLITE_OK ) goto claim_done;
  /* Return branch is a reopen mirror; the temp working branch owns terminal ops. */
  rc = doltliteLoadWorkingSet(db, zWorkingBranch);
  if( rc!=SQLITE_OK ) goto claim_done;
  savedFlags = doltliteGetSessionRebaseFlags(db);
  doltliteGetSessionRebaseState(db, &isRebasing, &savedPre, &savedOnto,
                                &zOrigBranchConst, &zReturnBranchConst);
  if( !isRebasing ){
    rc = SQLITE_DONE;
    goto claim_done;
  }
  zReturnBranch = sqlite3_mprintf("%s", zReturnBranchConst ? zReturnBranchConst : "");
  zSavedOrig = sqlite3_mprintf("%s", zOrigBranchConst ? zOrigBranchConst : "");
  if( !zReturnBranch || !zSavedOrig ){
    rc = SQLITE_NOMEM;
    goto claim_done;
  }

  rc = doltliteClearSessionRebaseState(db);
  if( rc!=SQLITE_OK ) goto claim_done;
  mutated = 1;
  rc = doltliteSaveWorkingSet(db);
  if( rc!=SQLITE_OK ) goto claim_done;
  if( zReturnBranch[0]
   && sqlite3_stricmp(zBranch, zReturnBranch)!=0 ){
    rc = rebaseRestoreReturnBranchWorkingState(db, zReturnBranch);
    if( rc!=SQLITE_OK ) goto claim_done;
  }
  if( zWorkingBranch[0] && sqlite3_stricmp(zBranch, zWorkingBranch)!=0 ){
    rc = rebaseRestoreReturnBranchWorkingState(db, zWorkingBranch);
    if( rc!=SQLITE_OK ) goto claim_done;
  }
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK && sqlite3FaultSim(961) ) rc = SQLITE_IOERR;
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);

claim_done:
  if( mutated && rc!=SQLITE_OK ){
    (void)rebaseUnclaimActiveEnd(db, savedFlags, &savedPre, &savedOnto,
                                 zSavedOrig, zReturnBranch);
  }
  if( locked ) chunkStoreUnlock(cs);
  sqlite3_free(zReturnBranch);
  sqlite3_free(zSavedOrig);
  return rc;
}

static int rebaseDropPlan(sqlite3 *db){
  return sqlite3FaultSim(953) ? SQLITE_IOERR :
      sqlite3_exec(db, "DROP TABLE IF EXISTS main.dolt_rebase", 0, 0, 0);
}

static int rebaseRetryBranchOp(
  sqlite3 *db,
  int (*xOp)(sqlite3*, const char*),
  const char *zBranch
){
  int rc;
  db->busyHandler.nBusy = 0;
  do {
    rc = xOp(db, zBranch);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  return rc;
}

static int rebaseRetryDbOp(sqlite3 *db, int (*xOp)(sqlite3*)){
  int rc;
  db->busyHandler.nBusy = 0;
  do {
    rc = xOp(db);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  return rc;
}

/* Checkout original and drop the temp working branch (ok if missing).
** Assumes claim already cleared isRebasing and the plan table. */
static int rebaseCleanupAfterClaim(
  sqlite3 *db,
  const char *zOrigBranch,
  const char *zWorkingBranch
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc = SQLITE_OK;
  int rc2;

  if( zOrigBranch && zOrigBranch[0] ){
    rc2 = rebaseRetryBranchOp(db, doltliteCheckoutBranchForRebase,
                              zOrigBranch);
    if( rc2!=SQLITE_OK ){
      rc2 = rebaseRetryBranchOp(db, rebaseRestoreBranchState, zOrigBranch);
    }
    rebaseKeepFirstError(&rc, rc2);
  }
  if( cs && zWorkingBranch && zWorkingBranch[0] ){
    db->busyHandler.nBusy = 0;
    do {
      rc2 = doltliteMutateRefs(db, rebaseDeleteWorkingBranchRefs,
                               (void*)zWorkingBranch);
    }while( rebaseRetryableRc(rc2) && rebaseEndBusyRetry(db) );
    rebaseKeepFirstError(&rc, rc2);
    rc2 = rebaseRetryDbOp(db, doltlitePersistWorkingSet);
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

  rc = rebaseRetryBranchOp(db, rebaseClaimActiveEnd, zWorkingBranch);
  if( rc!=SQLITE_OK && rc!=SQLITE_DONE ) return rc;
  rc = rebaseRetryDbOp(db, rebaseDropPlan);
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

  rc2 = doltliteClearSessionMergeState(db);
  rebaseKeepFirstError(&rc, rc2);
  if( zOrigBranch && zOrigBranch[0] ){
    rc2 = rebaseRestoreBranchState(db, zOrigBranch);
    rebaseKeepFirstError(&rc, rc2);
    if( rc2==SQLITE_OK ) rebaseAdoptRestoredCatalog(db);
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
  u8 rebaseFlags = WS_REBASE_FLAG_ACTIVE;
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
    ProllyHash returnHead;
    rc = rebaseBranchHasUncommittedWork(db, zReturnBranch, &dirty);
    if( rc!=SQLITE_OK ) goto fail;
    memset(&returnHead, 0, sizeof(returnHead));
    rc = chunkStoreFindBranch(cs, zReturnBranch, &returnHead);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK ) goto fail;
    if( dirty || prollyHashCompare(&returnHead, &upstreamHash)!=0 ){
      rebaseFlags = (u8)(WS_REBASE_FLAG_ACTIVE | WS_REBASE_FLAG_META_MIRROR);
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
  /* Persist the working branch now. Checkout/plan reload persisted refs
  ** under lock and would drop this still-in-memory branch. */
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc==SQLITE_OK && sqlite3FaultSim(960) ) rc = SQLITE_BUSY;
  if( rc!=SQLITE_OK ) goto fail;
  rc = doltliteCheckoutBranchForRebase(db, zWorking);
  if( rc!=SQLITE_OK ) goto fail;

  rc = rebaseCreateAndPopulatePlanTable(db, aReplay, nReplay);
  if( rc!=SQLITE_OK ){
    zFailMsg = "failed to create dolt_rebase table";
    goto fail;
  }

  rc = doltliteSetSessionRebaseState(db, rebaseFlags, &preRebaseCat, &headHash,
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

/* Reopen of $db lands on the default branch; continue/abort must run on
** dolt_rebase_<orig> so replay CASes that tip, not feat. */
static int rebaseAdoptPersistedRebase(sqlite3 *db){
  const char *zCur;
  const char *zOrig = 0;
  char *zWorking = 0;
  u8 isRebasing = 0;
  int active = 0;
  int rc;

  zCur = doltliteGetSessionBranch(db);
  doltliteGetSessionRebaseState(db, &isRebasing, 0, 0, &zOrig, 0);
  if( isRebasing && zOrig && zOrig[0] ){
    zWorking = rebaseBuildWorkingBranchName(zOrig);
  }else if( zCur && zCur[0] ){
    zWorking = rebaseBuildWorkingBranchName(zCur);
  }else{
    return SQLITE_OK;
  }
  if( !zWorking ) return SQLITE_NOMEM;
  db->busyHandler.nBusy = 0;
  do {
    rc = doltliteBranchWorkingSetIsRebasing(db, zWorking, &active);
    if( rc==SQLITE_OK && !active ) rc = SQLITE_DONE;
    if( rc==SQLITE_OK
     && (!zCur || sqlite3_stricmp(zCur, zWorking)!=0) ){
      rc = doltliteCheckoutPersistedRebase(db, zWorking);
    }
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_DONE;
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  sqlite3_free(zWorking);
  return rc;
}

static int rebaseSessionIsPaused(sqlite3 *db){
  u8 flags = doltliteGetSessionRebaseFlags(db);
  return (flags & (WS_REBASE_FLAG_ACTIVE|WS_REBASE_FLAG_PAUSED))
      == (u8)(WS_REBASE_FLAG_ACTIVE|WS_REBASE_FLAG_PAUSED);
}

static int rebaseFormatConflictTables(sqlite3 *db, char **pzNames){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pStr;
  int rc;
  int err;
  int n = 0;

  *pzNames = 0;
  rc = sqlite3_prepare_v2(db, "SELECT \"table\" FROM dolt_conflicts",
                          -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  pStr = sqlite3_str_new(db);
  if( !pStr ){
    sqlite3_finalize(pStmt);
    return SQLITE_NOMEM;
  }
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *z = (const char*)sqlite3_column_text(pStmt, 0);
    if( !z || !z[0] ) continue;
    if( n ) sqlite3_str_appendall(pStr, ", ");
    sqlite3_str_appendall(pStr, z);
    n++;
  }
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  {
    int frc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = frc;
  }
  err = sqlite3_str_errcode(pStr);
  *pzNames = sqlite3_str_finish(pStr);
  if( rc!=SQLITE_OK ){
    sqlite3_free(*pzNames);
    *pzNames = 0;
    return rc;
  }
  if( err!=SQLITE_OK ) return err;
  if( !*pzNames ) *pzNames = sqlite3_mprintf("");
  return *pzNames ? SQLITE_OK : SQLITE_NOMEM;
}

static int rebaseHasUnstagedResolution(sqlite3 *db, int *pUnstaged){
  sqlite3_stmt *pStmt = 0;
  int rc;

  *pUnstaged = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT staged, status FROM main.dolt_status "
      "WHERE table_name<>'dolt_rebase'",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  while( (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
    const char *zStatus = (const char*)sqlite3_column_text(pStmt, 1);
    int staged = sqlite3_column_int(pStmt, 0);
    if( zStatus
     && (sqlite3_stricmp(zStatus, "conflict")==0
      || sqlite3_stricmp(zStatus, "schema conflict")==0) ){
      continue;
    }
    if( !staged ){
      *pUnstaged = 1;
      break;
    }
  }
  if( rc==SQLITE_ROW || rc==SQLITE_DONE ) rc = SQLITE_OK;
  {
    int frc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ) rc = frc;
  }
  return rc;
}

/* Drop the conflicted working-set blob, then delete the temp branch and
** return the session to the pre-rebase branch. */
static int rebaseAbortPausedSession(sqlite3 *db){
  const char *zOrigConst = 0;
  char *zOrig = 0;
  char *zWorking = 0;
  ProllyHash origHead;
  ProllyHash origCat;
  ProllyHash head;
  ProllyHash empty;
  DoltliteCommit c;
  RebaseAbortRefsCtx abortCtx;
  int rc;

  memset(&origHead, 0, sizeof(origHead));
  memset(&origCat, 0, sizeof(origCat));
  memset(&head, 0, sizeof(head));
  memset(&empty, 0, sizeof(empty));
  memset(&c, 0, sizeof(c));
  doltliteGetSessionRebaseState(db, 0, &origCat, &origHead, &zOrigConst, 0);
  if( !zOrigConst || !zOrigConst[0] ) return SQLITE_ERROR;
  zOrig = sqlite3_mprintf("%s", zOrigConst);
  zWorking = rebaseBuildWorkingBranchName(zOrig);
  if( !zOrig || !zWorking ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    return SQLITE_NOMEM;
  }

  doltliteGetSessionHead(db, &head);
  rc = doltliteLoadCommit(db, &head, &c);
  if( rc==SQLITE_OK ){
    rc = doltliteWriteBranchCleanWorkingState(
        db, zWorking, &c.catalogHash, &head);
  }
  doltliteCommitClear(&c);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionConflictsCatalog(db, &empty);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionPendingReplayCommit(db, 0);
  if( rc==SQLITE_OK ) rc = doltliteClearSessionMergeState(db);
  if( rc==SQLITE_OK ) rc = doltliteClearSessionRebaseState(db);
  if( rc==SQLITE_OK ){
    memset(&abortCtx, 0, sizeof(abortCtx));
    abortCtx.zOrigBranch = zOrig;
    abortCtx.zWorkingBranch = zWorking;
    abortCtx.pExpectedOrigHead = &origHead;
    abortCtx.pOrigCatalog = &origCat;
    rc = doltliteMutateRefs(db, rebaseAbortLinearRefs, &abortCtx);
  }
  if( rc==SQLITE_OK ) rc = rebaseRestoreBranchState(db, zOrig);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(&origCat) ){
    doltliteAdoptRollbackBaseline(db, &origCat);
  }
  sqlite3_free(zOrig);
  sqlite3_free(zWorking);
  return rc;
}

static int rebaseHashesFromPlan(
  const RebasePlanRow *aPlan,
  int iStart,
  int nPlan,
  ProllyHash **pa,
  int *pn
){
  int n;
  int i;
  *pa = 0;
  *pn = 0;
  if( iStart>=nPlan ) return SQLITE_OK;
  n = nPlan - iStart;
  *pa = sqlite3_malloc(n * (int)sizeof(ProllyHash));
  if( !*pa ) return SQLITE_NOMEM;
  for(i=0; i<n; i++) (*pa)[i] = aPlan[iStart + i].commitHash;
  *pn = n;
  return SQLITE_OK;
}

static int rebaseCommitResolvedStep(
  sqlite3 *db,
  const char *zMessage,
  int *pCommitted
){
  ProllyHash cat;
  ProllyHash headCat;
  ProllyHash curHead;
  ProllyHash newCommit;
  int rc;

  *pCommitted = 0;
  memset(&cat, 0, sizeof(cat));
  memset(&headCat, 0, sizeof(headCat));
  memset(&curHead, 0, sizeof(curHead));
  memset(&newCommit, 0, sizeof(newCommit));
  rc = doltliteFlushCatalogToHash(db, &cat);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteGetHeadCatalogHash(db, &headCat);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashIsEmpty(&cat) || prollyHashCompare(&cat, &headCat)==0 ){
    return SQLITE_OK;
  }
  doltliteGetSessionHead(db, &curHead);
  rc = doltliteCreateAndStoreCommit(
      db, &curHead, &cat, zMessage ? zMessage : "",
      0, 0, 0, 0, &newCommit);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSwitchCatalog(db, &cat);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionStaged(db, &cat);
  if( rc==SQLITE_OK ){
    rc = rebaseAdvanceWorkingBranch(db, &curHead, &newCommit, &cat);
  }
  if( rc==SQLITE_OK ) *pCommitted = 1;
  return rc;
}

static void rebaseResultDataConflict(
  sqlite3_context *context,
  const char *zHash,
  const char *zMessage
){
  char *zErr = sqlite3_mprintf(
      "data conflict detected while rebasing commit %s (%s). \n\n"
      "Resolve the conflicts and remove them from the "
      "dolt_conflicts_<table> tables, "
      "then continue the rebase by calling dolt_rebase('--continue')",
      zHash ? zHash : "", zMessage ? zMessage : "");
  if( zErr ){
    sqlite3_result_error(context, zErr, -1);
    sqlite3_free(zErr);
  }else{
    sqlite3_result_error_nomem(context);
  }
}

/* Replay plan rows after the paused step. A later data conflict inside
** BEGIN pauses again. Anything else asks the caller to abort. */
static int rebaseReplayPausedTail(
  sqlite3 *db,
  sqlite3_context *context,
  RebasePlanRow *aPlan,
  int iStart,
  int nPlan,
  int *pbPaused
){
  int i;

  *pbPaused = 0;
  for(i=iStart; i<nPlan; i++){
    DoltliteCommit replayCommit;
    DoltliteCommit parentCommit;
    DoltliteCommit curHeadCommit;
    ProllyHash curHead;
    int nConflicts = 0;
    int nViolations = 0;
    char *zApplyErr = 0;
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    int rc;

    if( strcmp(aPlan[i].zAction, "drop")==0 ) continue;
    memset(&replayCommit, 0, sizeof(replayCommit));
    memset(&parentCommit, 0, sizeof(parentCommit));
    memset(&curHeadCommit, 0, sizeof(curHeadCommit));
    memset(&curHead, 0, sizeof(curHead));
    hexBuf[0] = 0;

    rc = doltliteLoadCommit(db, &aPlan[i].commitHash, &replayCommit);
    if( rc!=SQLITE_OK ) return rc;
    if( doltliteCommitParentCount(&replayCommit)==0 ){
      doltliteCommitClear(&replayCommit);
      return SQLITE_ERROR;
    }
    rc = doltliteLoadFirstParentCommit(db, &replayCommit, &parentCommit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&replayCommit);
      return rc;
    }
    doltliteGetSessionHead(db, &curHead);
    rc = doltliteLoadCommit(db, &curHead, &curHeadCommit);
    if( rc!=SQLITE_OK ){
      doltliteCommitClear(&replayCommit);
      doltliteCommitClear(&parentCommit);
      return rc;
    }
    rc = applyMergedCatalogAndCommit(db, context,
        &parentCommit.catalogHash,
        &curHeadCommit.catalogHash,
        &replayCommit.catalogHash,
        &curHead, 0,
        aPlan[i].zCommitMessage ? aPlan[i].zCommitMessage : "",
        0, 0, 0, 1, &nConflicts, &nViolations, &zApplyErr, hexBuf);
    doltliteCommitClear(&replayCommit);
    doltliteCommitClear(&parentCommit);
    doltliteCommitClear(&curHeadCommit);
    if( rc==SQLITE_DONE ){
      sqlite3_free(zApplyErr);
      continue;
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zApplyErr);
      return rc;
    }
    sqlite3_free(zApplyErr);
    if( nViolations>0 ) return SQLITE_CONSTRAINT;
    if( nConflicts>0 ){
      (void)doltliteSetSessionPendingReplayCommit(db, 0);
      if( doltliteVcTxnMode(db)==DOLTLITE_VC_TXN_PLAIN ){
        ProllyHash *aLeft = 0;
        int nLeft = 0;
        char zHex[PROLLY_HASH_SIZE*2+1];
        int prc = rebaseHashesFromPlan(aPlan, i, nPlan, &aLeft, &nLeft);
        if( prc==SQLITE_OK ){
          prc = rebaseCreateAndPopulatePlanTable(db, aLeft, nLeft);
        }
        sqlite3_free(aLeft);
        if( prc!=SQLITE_OK ) return prc;
        doltliteHashToHex(&aPlan[i].commitHash, zHex);
        rebaseResultDataConflict(context, zHex,
            aPlan[i].zCommitMessage ? aPlan[i].zCommitMessage : "");
        *pbPaused = 1;
        return SQLITE_OK;
      }
      return SQLITE_CONSTRAINT;
    }
  }
  return SQLITE_OK;
}

static int rebaseFinishPaused(
  sqlite3 *db,
  const char *zOrig,
  const char *zWorking
){
  ProllyHash expectedOrig;
  ProllyHash curHead;
  ProllyHash curCat;
  ProllyHash empty;
  RebaseFinalizeRefsCtx refsCtx;
  int rc;

  memset(&expectedOrig, 0, sizeof(expectedOrig));
  memset(&curHead, 0, sizeof(curHead));
  memset(&curCat, 0, sizeof(curCat));
  memset(&empty, 0, sizeof(empty));
  doltliteGetSessionRebaseState(db, 0, 0, &expectedOrig, 0, 0);
  doltliteGetSessionHead(db, &curHead);
  rc = doltliteFlushCatalogToHash(db, &curCat);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteWriteBranchCleanWorkingState(db, zWorking, &curCat, &curHead);
  if( rc!=SQLITE_OK ) return rc;

  memset(&refsCtx, 0, sizeof(refsCtx));
  refsCtx.zOrigBranch = zOrig;
  refsCtx.zWorkingBranch = zWorking;
  refsCtx.pExpectedOrigHead = &expectedOrig;
  refsCtx.pCurHead = &curHead;
  refsCtx.pCurCat = &curCat;
  {
    DoltliteBranchExpectation expected[2];
    expected[0].zBranch = zOrig;
    expected[0].pTip = &expectedOrig;
    expected[1].zBranch = zWorking;
    expected[1].pTip = &curHead;
    doltliteTestCrashFinalize("rebase");
    rc = doltliteMutateRefsExpected(
        db, expected, 2, rebaseFinalizeLinearRefs, &refsCtx);
  }
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSetSessionConflictsCatalog(db, &empty);
  if( rc==SQLITE_OK ) rc = doltliteSetSessionPendingReplayCommit(db, 0);
  if( rc==SQLITE_OK ) rc = doltliteClearSessionRebaseState(db);
  if( rc==SQLITE_OK ) rc = rebaseRestoreBranchState(db, zOrig);
  if( rc!=SQLITE_OK ) return rc;
  return doltliteVcSealEnclosingTxn(db);
}

static void doltliteRebasePausedAbort(
  sqlite3_context *context,
  sqlite3 *db
){
  int rc = rebaseAbortPausedSession(db);
  if( rc!=SQLITE_OK ){
    rebaseResultRecoveryFailure(context, rc);
  }else{
    sqlite3_result_text(context,
        "Interactive rebase aborted", -1, SQLITE_STATIC);
  }
}

static void doltliteRebasePausedContinue(
  sqlite3_context *context,
  sqlite3 *db
){
  const char *zOrigConst = 0;
  char *zOrig = 0;
  char *zWorking = 0;
  char *zNames = 0;
  char *zErr = 0;
  RebasePlanRow *aPlan = 0;
  int nPlan = 0;
  int idx;
  int unstaged = 0;
  int bDropped = 0;
  int pausedAgain = 0;
  int rc;

  doltliteGetSessionRebaseState(db, 0, 0, 0, &zOrigConst, 0);
  if( !zOrigConst || !zOrigConst[0] ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  zOrig = sqlite3_mprintf("%s", zOrigConst);
  zWorking = rebaseBuildWorkingBranchName(zOrig);
  if( !zOrig || !zWorking ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_nomem(context);
    return;
  }

  if( doltliteSessionHasUnresolvedConflicts(db) ){
    rc = rebaseFormatConflictTables(db, &zNames);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error_code(context, rc);
      return;
    }
    zErr = sqlite3_mprintf(
        "conflicts detected in tables %s; resolve conflicts before "
        "continuing the rebase",
        (zNames && zNames[0]) ? zNames : "?");
    sqlite3_free(zNames);
    if( zErr ){
      sqlite3_result_error(context, zErr, -1);
      sqlite3_free(zErr);
    }else{
      sqlite3_result_error_nomem(context);
    }
    return;
  }

  rc = rebaseHasUnstagedResolution(db, &unstaged);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_code(context, rc);
    return;
  }
  if( unstaged ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error(context,
        "cannot continue a rebase with unstaged changes. "
        "Use dolt_add() to stage tables and then continue the rebase",
        -1);
    return;
  }

  rc = rebaseReadPlan(db, &aPlan, &nPlan);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_code(context, rc);
    return;
  }
  idx = 0;
  while( idx<nPlan && strcmp(aPlan[idx].zAction, "drop")==0 ) idx++;
  if( idx<nPlan
   && strcmp(aPlan[idx].zAction, "pick")!=0
   && strcmp(aPlan[idx].zAction, "reword")!=0 ){
    rebaseFreePlan(aPlan, nPlan);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error(context,
        "first non-drop action must be pick or reword", -1);
    return;
  }

  rc = rebaseDropPlan(db);
  if( rc==SQLITE_OK ) bDropped = 1;
  if( rc==SQLITE_OK && idx<nPlan ){
    int committed = 0;
    const char *zMsg = aPlan[idx].zCommitMessage
        ? aPlan[idx].zCommitMessage : "";
    rc = rebaseCommitResolvedStep(db, zMsg, &committed);
    (void)committed;
  }
  if( rc!=SQLITE_OK ){
    if( bDropped ){
      ProllyHash *aLeft = 0;
      int nLeft = 0;
      int prc = rebaseHashesFromPlan(
          aPlan, idx<nPlan ? idx : nPlan, nPlan, &aLeft, &nLeft);
      if( prc==SQLITE_OK && nLeft>0 ){
        (void)rebaseCreateAndPopulatePlanTable(db, aLeft, nLeft);
      }
      sqlite3_free(aLeft);
    }
    rebaseFreePlan(aPlan, nPlan);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_code(context, rc);
    return;
  }

  rc = rebaseReplayPausedTail(
      db, context, aPlan, idx<nPlan ? idx + 1 : nPlan, nPlan, &pausedAgain);
  if( pausedAgain ){
    rebaseFreePlan(aPlan, nPlan);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    return;
  }
  if( rc!=SQLITE_OK ){
    int arc = rebaseAbortPausedSession(db);
    rebaseFreePlan(aPlan, nPlan);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    if( arc!=SQLITE_OK ){
      rebaseResultRecoveryFailure(context, arc);
    }else if( rc==SQLITE_CONSTRAINT ){
      sqlite3_result_error(context,
          "data conflicts from rebase — rebase has been aborted", -1);
    }else{
      sqlite3_result_error(context,
          "rebase failed — branch restored to pre-rebase state", -1);
    }
    return;
  }

  rc = rebaseFinishPaused(db, zOrig, zWorking);
  rebaseFreePlan(aPlan, nPlan);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    sqlite3_result_error_code(context, rc);
    return;
  }
  {
    char *zOk = sqlite3_mprintf(
        "Successfully rebased and updated refs/heads/%s", zOrig);
    sqlite3_free(zOrig);
    sqlite3_free(zWorking);
    if( zOk ) sqlite3_result_text(context, zOk, -1, sqlite3_free);
    else sqlite3_result_text(context, "Successfully rebased", -1, SQLITE_STATIC);
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

  if( rebaseSessionIsPaused(db) ){
    doltliteRebasePausedAbort(context, db);
    return;
  }

  rc = rebaseAdoptPersistedRebase(db);
  if( rc==SQLITE_DONE ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
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

  /* Names before claim clears session rebase state. */
  rc = rebaseRetryBranchOp(db, rebaseClaimActiveEnd, zWorking);
  if( rc==SQLITE_DONE ){
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    /* Only claim returns DONE when a durable read showed isRebasing clear.
    ** Any other error is recovery failure. */
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    rebaseResultRecoveryFailure(context, rc);
    return;
  }

  rc = rebaseDropPlan(db);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zOrigBranch);
    rebaseResultRecoveryFailure(context, rc);
    return;
  }

  rc = rebaseCleanupAfterClaim(db, zOrigBranch, zWorking);
  if( cs && zReturnBranch && zReturnBranch[0] ){
    int rc2 = rebaseRetryBranchOp(
        db, rebaseRestoreReturnBranchWorkingState, zReturnBranch);
    rebaseKeepFirstError(&rc, rc2);
  }
  {
    int rc2 = rebaseRetryDbOp(db, doltlitePersistWorkingSet);
    rebaseKeepFirstError(&rc, rc2);
  }
  {
    int rc2 = rebaseRetryDbOp(db, doltliteVcSealBranchStyleTxn);
    rebaseKeepFirstError(&rc, rc2);
  }
  if( rc!=SQLITE_OK ){
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

static int rebaseHasUncommittedChanges(sqlite3 *db, int *pDirty){
  sqlite3_stmt *pStmt = 0;
  int rc;
  int rc2;
  *pDirty = 0;
  rc = sqlite3_prepare_v2(db,
      "SELECT 1 FROM main.dolt_status WHERE table_name<>'dolt_rebase' LIMIT 1",
      -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    *pDirty = 1;
    rc = SQLITE_OK;
  }else if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
  }
  rc2 = sqlite3_finalize(pStmt);
  return rc==SQLITE_OK ? rc2 : rc;
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
  int stateRc;
  int rebaseActive = 1;
  int i;
  int bPlanDropped = 0;
  int dirty = 0;
  ProllyHash curCat;
  ProllyHash curHead;
  ProllyHash expectedOrigHead;
  ProllyHash preRebaseCat;
  RebaseFinalizeRefsCtx refsCtx;
  char *zPlanErr = 0;
  char *zReplayErr = 0;

  if( rebaseSessionIsPaused(db) ){
    doltliteRebasePausedContinue(context, db);
    return;
  }

  memset(&curCat, 0, sizeof(curCat));
  memset(&curHead, 0, sizeof(curHead));
  memset(&expectedOrigHead, 0, sizeof(expectedOrigHead));
  memset(&preRebaseCat, 0, sizeof(preRebaseCat));

  rc = rebaseAdoptPersistedRebase(db);
  if( rc==SQLITE_DONE ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }
  doltliteGetSessionRebaseState(db, &isRebasing, &preRebaseCat, &expectedOrigHead,
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
  if( rc==SQLITE_CONSTRAINT_NOTNULL ){
    sqlite3_result_error(context,
      "dolt_rebase.commit_message must not be NULL", -1);
    goto abort_err_silent;
  }
  if( rc!=SQLITE_OK ) goto abort_err;

  /* Unknown plan verbs (typos in dolt_rebase.action) are not silent picks.
  ** Stay in progress and name the action so --continue can retry. */
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

  for(i=0; i<nPlan; i++){
    DoltliteCommit commit;
    memset(&commit, 0, sizeof(commit));
    rc = doltliteLoadCommit(db, &aPlan[i].commitHash, &commit);
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
        char zHex[PROLLY_HASH_SIZE*2+1];
        char *zMsg;
        doltliteHashToHex(&aPlan[i].commitHash, zHex);
        zMsg = sqlite3_mprintf("invalid commit hash: %s", zHex);
        if( zMsg ){
          sqlite3_result_error(context, zMsg, -1);
          sqlite3_free(zMsg);
        }else{
          sqlite3_result_error_nomem(context);
        }
      }else{
        sqlite3_result_error_code(context, rc);
      }
      goto abort_err_silent;
    }
  }

  rc = rebaseHasUncommittedChanges(db, &dirty);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    goto abort_err_silent;
  }
  if( dirty ){
    sqlite3_result_error(context,
      "cannot start a rebase with uncommitted changes", -1);
    goto abort_err_silent;
  }

  /* Claim before replay so a concurrent --abort loses with "no rebase
  ** in progress" rather than both failing mid-recovery. */
  rc = rebaseRetryBranchOp(db, rebaseClaimActiveEnd, zWorking);
  if( rc==SQLITE_DONE ){
    sqlite3_result_error(context, "no rebase in progress", -1);
    goto abort_err_silent;
  }
  if( rc!=SQLITE_OK ) goto abort_err;
  bPlanDropped = 1;

  /* Peer may still read main.dolt_rebase; a transient lock on DROP must
  ** not abort a claimed rebase. rebaseCleanupAfterClaim drops it this way. */
  rc = rebaseRetryDbOp(db, rebaseDropPlan);
  if( rc!=SQLITE_OK ) goto abort_err;

  /* Seal savepoints and enclosing BEGIN: replay constraint detection needs
  ** claim/plan-drop committed. Releasing savepoints alone leaves BEGIN open. */
  rc = doltliteVcSealActiveSavepoints(db);
  if( rc==SQLITE_OK ) rc = doltliteVcSealBranchStyleTxn(db);
  if( rc!=SQLITE_OK ) goto abort_err;

  rc = rebaseRetryDbOp(db, doltliteEnsureWriteTxnAndSavepoints);
  if( rc!=SQLITE_OK ) goto abort_err;

  rc = rebaseRefreshWorkingRefs(db);
  if( rc!=SQLITE_OK ) goto abort_err;

  rc = doltliteFlushCatalogToHash(db, &curCat);
  if( rc!=SQLITE_OK ) goto abort_err;
  doltliteGetSessionHead(db, &curHead);

  i = 0;
  while( i < nPlan ){
    int j;

    while( i < nPlan && strcmp(aPlan[i].zAction, "drop")==0 ) i++;
    if( i >= nPlan ) break;

    rc = rebaseReplayPlanGroup(
        db, aPlan, nPlan, i, &curCat, &curHead, &j, &zReplayErr);
    if( rc==SQLITE_CONSTRAINT ) goto abort_err_conflict;
    if( rc==SQLITE_BUSY ) goto abort_err_cas;
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
    db->busyHandler.nBusy = 0;
    do {
      rc = doltliteMutateRefsExpected(
          db, expected, 2, rebaseFinalizeContinueRefs, &refsCtx);
    }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  }
  if( rc==SQLITE_BUSY ) goto abort_err_cas;
  if( rc!=SQLITE_OK ) goto abort_err;

  /* Claim already cleared durable isRebasing; finish checkout, working-branch
  ** delete, and return-branch restore. */
  rc = doltliteClearSessionRebaseState(db);
  if( rc==SQLITE_OK ) rc = rebaseRetryDbOp(db, doltlitePersistWorkingSet);
  if( rc!=SQLITE_OK ) goto abort_err;

  /* curCat is the flushed catalog just persisted; do not re-serialize the
  ** discarded branch while schema is transitional. */
  db->busyHandler.nBusy = 0;
  do {
    rc = doltliteCheckoutBranchForRebaseWithOldCatalog(
        db, zOrigBranch, &curCat);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  if( rc!=SQLITE_OK ) goto abort_err;
  db->busyHandler.nBusy = 0;
  do {
    rc = doltliteMutateRefs(db, rebaseDeleteWorkingBranchRefs, zWorking);
  }while( rebaseRetryableRc(rc) && rebaseEndBusyRetry(db) );
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = rebaseRetryBranchOp(
      db, rebaseRestoreReturnBranchWorkingState, zReturnBranch);
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = rebaseRetryDbOp(db, doltlitePersistWorkingSet);
  if( rc!=SQLITE_OK ) goto abort_err;
  rc = rebaseRetryDbOp(db, doltliteVcSealBranchStyleTxn);
  if( rc!=SQLITE_OK ) goto abort_err;

  rebaseFreePlan(aPlan, nPlan);
  sqlite3_free(zReplayErr);
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
  sqlite3_free(zReplayErr);
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

abort_err_cas:
  recoveryRc = rebaseRestoreInProgress(
      db, aPlan, nPlan, &preRebaseCat, &expectedOrigHead,
      zOrigBranch, zReturnBranch);
  rebaseFreePlan(aPlan, nPlan);
  sqlite3_free(zReplayErr);
  if( recoveryRc!=SQLITE_OK ){
    sqlite3_free(zOrigBranch);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    rebaseResultRecoveryFailure(context, recoveryRc);
    return;
  }
  {
    char *zMsg = sqlite3_mprintf(
        "rebase aborted due to changes in branch %s",
        zOrigBranch ? zOrigBranch : "main");
    sqlite3_free(zOrigBranch);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    if( zMsg ){
      sqlite3_result_error(context, zMsg, -1);
      sqlite3_free(zMsg);
    }else{
      sqlite3_result_error(context,
        "rebase aborted due to changes in the source branch", -1);
    }
  }
  return;

abort_err:
  rebaseFreePlan(aPlan, nPlan);
  if( !bPlanDropped ){
    stateRc = zWorking ?
        rebaseReadActiveRetry(db, zWorking, &rebaseActive) : SQLITE_MISUSE;
    sqlite3_free(zOrigBranch);
    sqlite3_free(zReturnBranch);
    sqlite3_free(zWorking);
    sqlite3_free(zReplayErr);
    if( (stateRc==SQLITE_OK && !rebaseActive) || rc==SQLITE_NOTFOUND ){
      sqlite3_result_error(context, "no rebase in progress", -1);
    }else{
      sqlite3_result_error(context, "rebase failed", -1);
    }
    return;
  }
  recoveryRc = SQLITE_OK;
  recoveryRc = rebaseCleanupAfterClaim(
      db, zOrigBranch ? zOrigBranch : "main", zWorking);
  if( cs && zReturnBranch && zReturnBranch[0] ){
    rc2 = rebaseRetryBranchOp(
        db, rebaseRestoreReturnBranchWorkingState, zReturnBranch);
    rebaseKeepFirstError(&recoveryRc, rc2);
  }
  sqlite3_free(zOrigBranch);
  sqlite3_free(zReturnBranch);
  sqlite3_free(zWorking);
  if( recoveryRc!=SQLITE_OK ){
    sqlite3_free(zReplayErr);
    rebaseResultRecoveryFailure(context, recoveryRc);
  }else if( zReplayErr ){
    char *zMsg = sqlite3_mprintf(
      "rebase failed — %s — branch restored to pre-rebase state", zReplayErr);
    sqlite3_free(zReplayErr);
    if( zMsg ){
      sqlite3_result_error(context, zMsg, -1);
      sqlite3_free(zMsg);
    }else{
      sqlite3_result_error_nomem(context);
    }
  }else{
    sqlite3_result_error(context,
      "rebase failed — branch restored to pre-rebase state", -1);
  }
  return;

abort_err_silent:
  rebaseFreePlan(aPlan, nPlan);
  sqlite3_free(zReplayErr);
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
  DoltliteCmdArgs args;
  const char *zArg0 = 0;
  int isAbort = 0, isContinue = 0, isInteractive = 0;
  DoltliteCmdOption aOption[] = {
    { "abort", 0, DOLTLITE_CMD_OPTION_FLAG, &isAbort, 0 },
    { "continue", 0, DOLTLITE_CMD_OPTION_FLAG, &isContinue, 0 },
    { "interactive", 'i', DOLTLITE_CMD_OPTION_FLAG, &isInteractive, 0 }
  };
  int sealTopLevel = db->pSavepoint!=0 && db->nSavepoint==0;
  int keepTopLevelSavepoint = 0;
  int rc;

  memset(&args, 0, sizeof(args));

  if( doltliteCmdRejectDetached(context) ) return;
  if( doltliteCmdRejectReadOnly(context) ) return;
  if( !cs ){ sqlite3_result_error(context, "no database", -1); goto rebase_cleanup; }
  if( argc<1 ){
    sqlite3_result_error(context, "usage: dolt_rebase('upstream_branch')", -1);
    goto rebase_cleanup;
  }

  rc = doltliteCmdParseArgs(context, argc, argv, aOption, ArraySize(aOption),
                            0, &args);
  if( rc!=SQLITE_OK ) goto rebase_cleanup;
  if( isAbort + isContinue + isInteractive > 1 ){
    sqlite3_result_error(context, "conflicting flags", -1);
    goto rebase_cleanup;
  }
  if( args.nPositional>0 ) zArg0 = args.azPositional[0];

  if( isAbort ){
    if( args.nPositional!=0 ){
      sqlite3_result_error(context, "--abort does not take other arguments", -1);
      goto rebase_cleanup;
    }
    doltliteRebaseInteractiveAbort(context, db);
    goto rebase_cleanup;
  }
  if( isContinue ){
    if( args.nPositional!=0 ){
      sqlite3_result_error(context, "--continue does not take other arguments", -1);
      goto rebase_cleanup;
    }
    keepTopLevelSavepoint = 1;
    doltliteRebaseInteractiveContinue(context, db);
    goto rebase_cleanup;
  }
  if( isInteractive ){
    const char *zUpstream;
    keepTopLevelSavepoint = 1;
    if( args.nPositional<1 ){
      sqlite3_result_error(context,
        "interactive rebase requires upstream branch: "
        "dolt_rebase('-i', 'upstream')", -1);
      goto rebase_cleanup;
    }
    if( args.nPositional!=1 ){
      sqlite3_result_error(context,
        "interactive rebase takes exactly one upstream branch", -1);
      goto rebase_cleanup;
    }
    zUpstream = args.azPositional[0];
    doltliteRebaseInteractiveStart(context, db, zUpstream);
    goto rebase_cleanup;
  }

  if( !zArg0 ){
    sqlite3_result_error(context, "upstream ref required", -1);
    goto rebase_cleanup;
  }
  if( args.nPositional!=1 ){
    sqlite3_result_error(context,
      "too many positional arguments to dolt_rebase", -1);
    goto rebase_cleanup;
  }

  {
    char *zFinalMessage = 0;
    int paused = 0;
    int rc = doltliteRebaseLinearReplay(
        db, context, zArg0, &zFinalMessage, &paused);
    if( paused ) keepTopLevelSavepoint = 1;
    if( rc==SQLITE_OK && zFinalMessage ){
      sqlite3_result_text(context, zFinalMessage, -1, sqlite3_free);
    }
  }

rebase_cleanup:
  doltliteCmdArgsClear(&args);
  if( sealTopLevel && !keepTopLevelSavepoint ){
    (void)doltliteVcSealTopLevelSavepointTxn(db);
  }
}


int doltliteRebaseRegister(sqlite3 *db){
  return doltliteCreateCommandFunc(db, "dolt_rebase", -1,
                                 doltliteRebaseFunc);
}

#endif
