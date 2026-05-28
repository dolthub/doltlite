
#ifdef DOLTLITE_PROLLY

#include "doltlite_ancestor.h"
#include "doltlite_commit.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include "prolly_hashset.h"
#include <string.h>

static int loadCommitByHash(sqlite3 *db, const ProllyHash *hash,
                            DoltliteCommit *pCommit){
  if( prollyHashIsEmpty(hash) ) return SQLITE_NOTFOUND;
  return doltliteLoadCommit(db, hash, pCommit);
}

static int ancestorBfsCollect(
  sqlite3 *db,
  const ProllyHash *pStart,
  ProllyHashSet *pVisited
){
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 64;
  ProllyHash current;
  DoltliteCommit commit;
  int rc = SQLITE_OK;
  int i;

  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ) return SQLITE_NOMEM;
  queue[qTail++] = *pStart;

  while( qHead < qTail ){
    current = queue[qHead++];
    if( prollyHashIsEmpty(&current) ) continue;
    if( prollyHashSetContains(pVisited, &current) ) continue;
    rc = prollyHashSetAdd(pVisited, &current);
    if( rc!=SQLITE_OK ) break;
    memset(&commit, 0, sizeof(commit));
    rc = loadCommitByHash(db, &current, &commit);
    if( rc!=SQLITE_OK ) break;
    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent ) continue;
      if( qTail >= qAlloc ){
        ProllyHash *q2;
        qAlloc *= 2;
        q2 = sqlite3_realloc(queue, qAlloc*(int)sizeof(ProllyHash));
        if( !q2 ){ doltliteCommitClear(&commit); rc=SQLITE_NOMEM; break; }
        queue = q2;
      }
      queue[qTail++] = *pParent;
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
  }

  sqlite3_free(queue);
  return rc;
}

static int ancestorMarkRedundant(
  sqlite3 *db,
  const ProllyHash *pStart,
  ProllyHashSet *pCommon,
  u8 *aRedundant,
  int nCommon,
  const ProllyHash *aCommon
){
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 64;
  ProllyHash current;
  DoltliteCommit commit;
  ProllyHashSet visited;
  int rc;
  int i;

  rc = prollyHashSetInit(&visited, 64);
  if( rc!=SQLITE_OK ) return rc;

  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ){ prollyHashSetFree(&visited); return SQLITE_NOMEM; }

  memset(&commit, 0, sizeof(commit));
  rc = loadCommitByHash(db, pStart, &commit);
  if( rc!=SQLITE_OK ){
    sqlite3_free(queue);
    prollyHashSetFree(&visited);
    return rc;
  }
  for(i=0; i<doltliteCommitParentCount(&commit); i++){
    const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
    if( !pParent ) continue;
    queue[qTail++] = *pParent;
  }
  doltliteCommitClear(&commit);

  while( qHead < qTail ){
    current = queue[qHead++];
    if( prollyHashIsEmpty(&current) ) continue;
    if( prollyHashSetContains(&visited, &current) ) continue;
    rc = prollyHashSetAdd(&visited, &current);
    if( rc!=SQLITE_OK ) break;
    if( prollyHashSetContains(pCommon, &current) ){
      int j;
      for(j=0; j<nCommon; j++){
        if( prollyHashCompare(&aCommon[j], &current)==0 ){
          aRedundant[j] = 1;
          break;
        }
      }
    }
    memset(&commit, 0, sizeof(commit));
    rc = loadCommitByHash(db, &current, &commit);
    if( rc!=SQLITE_OK ) break;
    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent ) continue;
      if( qTail >= qAlloc ){
        ProllyHash *q2;
        qAlloc *= 2;
        q2 = sqlite3_realloc(queue, qAlloc*(int)sizeof(ProllyHash));
        if( !q2 ){ doltliteCommitClear(&commit); rc=SQLITE_NOMEM; break; }
        queue = q2;
      }
      queue[qTail++] = *pParent;
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
  }

  sqlite3_free(queue);
  prollyHashSetFree(&visited);
  return rc;
}

static int ancestorBfsPosition(
  sqlite3 *db,
  const ProllyHash *pStart,
  const ProllyHash *pTarget,
  int *pDistance,
  int *pOrder
){
  typedef struct AncestorQueueItem AncestorQueueItem;
  struct AncestorQueueItem {
    ProllyHash hash;
    int distance;
  };
  AncestorQueueItem *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 64;
  DoltliteCommit commit;
  ProllyHashSet visited;
  int rc;
  int order = 0;
  int i;

  *pDistance = 0x7fffffff;
  *pOrder = 0x7fffffff;

  rc = prollyHashSetInit(&visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  queue = sqlite3_malloc(qAlloc * (int)sizeof(AncestorQueueItem));
  if( !queue ){
    prollyHashSetFree(&visited);
    return SQLITE_NOMEM;
  }

  queue[qTail].hash = *pStart;
  queue[qTail].distance = 0;
  qTail++;

  while( qHead < qTail ){
    AncestorQueueItem item = queue[qHead++];
    if( prollyHashIsEmpty(&item.hash) ) continue;
    if( prollyHashSetContains(&visited, &item.hash) ) continue;
    rc = prollyHashSetAdd(&visited, &item.hash);
    if( rc!=SQLITE_OK ) break;

    if( prollyHashCompare(&item.hash, pTarget)==0 ){
      *pDistance = item.distance;
      *pOrder = order;
      break;
    }
    order++;

    memset(&commit, 0, sizeof(commit));
    rc = loadCommitByHash(db, &item.hash, &commit);
    if( rc!=SQLITE_OK ) break;
    for(i=0; i<doltliteCommitParentCount(&commit); i++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
      if( !pParent ) continue;
      if( qTail >= qAlloc ){
        AncestorQueueItem *q2;
        qAlloc *= 2;
        q2 = sqlite3_realloc(queue, qAlloc*(int)sizeof(AncestorQueueItem));
        if( !q2 ){ doltliteCommitClear(&commit); rc=SQLITE_NOMEM; break; }
        queue = q2;
      }
      queue[qTail].hash = *pParent;
      queue[qTail].distance = item.distance + 1;
      qTail++;
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
  }

  sqlite3_free(queue);
  prollyHashSetFree(&visited);
  return rc;
}

static int ancestorCompareCandidate(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  const ProllyHash *pLeft,
  const ProllyHash *pRight,
  int *pCmp
){
  int leftDist1, leftOrder1, leftDist2, leftOrder2;
  int rightDist1, rightOrder1, rightDist2, rightOrder2;
  int leftSum, rightSum;
  int rc;

  rc = ancestorBfsPosition(db, commitHash1, pLeft, &leftDist1, &leftOrder1);
  if( rc!=SQLITE_OK ) return rc;
  rc = ancestorBfsPosition(db, commitHash2, pLeft, &leftDist2, &leftOrder2);
  if( rc!=SQLITE_OK ) return rc;
  rc = ancestorBfsPosition(db, commitHash1, pRight, &rightDist1, &rightOrder1);
  if( rc!=SQLITE_OK ) return rc;
  rc = ancestorBfsPosition(db, commitHash2, pRight, &rightDist2, &rightOrder2);
  if( rc!=SQLITE_OK ) return rc;

  leftSum = leftDist1 + leftDist2;
  rightSum = rightDist1 + rightDist2;
  if( leftSum!=rightSum ){
    *pCmp = leftSum<rightSum ? -1 : 1;
  }else if( leftDist1!=rightDist1 ){
    *pCmp = leftDist1<rightDist1 ? -1 : 1;
  }else if( leftDist2!=rightDist2 ){
    *pCmp = leftDist2<rightDist2 ? -1 : 1;
  }else if( leftOrder1!=rightOrder1 ){
    *pCmp = leftOrder1<rightOrder1 ? -1 : 1;
  }else if( leftOrder2!=rightOrder2 ){
    *pCmp = leftOrder2<rightOrder2 ? -1 : 1;
  }else{
    *pCmp = prollyHashCompare(pLeft, pRight);
  }
  return SQLITE_OK;
}

int doltliteFindAncestor(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  ProllyHash *pAncestor
){
  ProllyHashSet anc1, anc2, common;
  ProllyHash *aCommon = 0;
  u8 *aRedundant = 0;
  int nCommon = 0;
  int i;
  int rc;

  memset(pAncestor, 0, sizeof(*pAncestor));

  if( prollyHashIsEmpty(commitHash1) || prollyHashIsEmpty(commitHash2) ){
    return SQLITE_NOTFOUND;
  }

  if( prollyHashCompare(commitHash1, commitHash2)==0 ){
    *pAncestor = *commitHash1;
    return SQLITE_OK;
  }

  rc = prollyHashSetInit(&anc1, 64);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyHashSetInit(&anc2, 64);
  if( rc!=SQLITE_OK ){ prollyHashSetFree(&anc1); return rc; }
  rc = prollyHashSetInit(&common, 64);
  if( rc!=SQLITE_OK ){ prollyHashSetFree(&anc2); prollyHashSetFree(&anc1); return rc; }

  rc = ancestorBfsCollect(db, commitHash1, &anc1);
  if( rc!=SQLITE_OK ) goto done;
  rc = ancestorBfsCollect(db, commitHash2, &anc2);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<anc1.nSlots; i++){
    if( anc1.aUsed[i] && prollyHashSetContains(&anc2, &anc1.aSlots[i]) ){
      rc = prollyHashSetAdd(&common, &anc1.aSlots[i]);
      if( rc!=SQLITE_OK ) goto done;
      nCommon++;
    }
  }

  if( nCommon==0 ){
    rc = SQLITE_NOTFOUND;
    goto done;
  }

  aCommon = sqlite3_malloc(nCommon * (int)sizeof(ProllyHash));
  if( !aCommon ){ rc = SQLITE_NOMEM; goto done; }
  aRedundant = sqlite3_malloc(nCommon);
  if( !aRedundant ){ rc = SQLITE_NOMEM; goto done; }
  memset(aRedundant, 0, nCommon);
  {
    int k = 0;
    for(i=0; i<common.nSlots; i++){
      if( common.aUsed[i] ){
        aCommon[k++] = common.aSlots[i];
      }
    }
  }

  for(i=0; i<nCommon; i++){
    if( aRedundant[i] ) continue;
    rc = ancestorMarkRedundant(db, &aCommon[i], &common,
                               aRedundant, nCommon, aCommon);
    if( rc!=SQLITE_OK ) goto done;
  }

  {
    int iBest = -1;
    for(i=0; i<nCommon; i++){
      int cmp;
      if( aRedundant[i] ) continue;
      if( iBest<0 ){
        iBest = i;
        continue;
      }
      rc = ancestorCompareCandidate(db, commitHash1, commitHash2,
                                    &aCommon[i], &aCommon[iBest], &cmp);
      if( rc!=SQLITE_OK ) goto done;
      if( cmp<0 ) iBest = i;
    }
    if( iBest<0 ){
      rc = SQLITE_NOTFOUND;
    }else{
      *pAncestor = aCommon[iBest];
      rc = SQLITE_OK;
    }
  }

done:
  sqlite3_free(aRedundant);
  sqlite3_free(aCommon);
  prollyHashSetFree(&common);
  prollyHashSetFree(&anc2);
  prollyHashSetFree(&anc1);
  return rc;
}

extern int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
extern int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

static void doltMergeBaseFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ProllyHash hash1, hash2, ancestor;
  const char *zRef1, *zRef2;
  int rc;

  if( argc!=2 ){
    sqlite3_result_error(ctx, "dolt_merge_base requires 2 arguments", -1);
    return;
  }

  zRef1 = (const char*)sqlite3_value_text(argv[0]);
  zRef2 = (const char*)sqlite3_value_text(argv[1]);
  if( !zRef1 || !zRef2 ){
    sqlite3_result_error(ctx, "invalid arguments", -1);
    return;
  }

  rc = doltliteResolveRef(db,zRef1, &hash1);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "could not resolve first argument to a commit", -1);
    return;
  }
  rc = doltliteResolveRef(db,zRef2, &hash2);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "could not resolve second argument to a commit", -1);
    return;
  }

  rc = doltliteFindAncestor(db, &hash1, &hash2, &ancestor);
  if( rc==SQLITE_OK ){
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    doltliteHashToHex(&ancestor, hexBuf);
    sqlite3_result_text(ctx, hexBuf, -1, SQLITE_TRANSIENT);
  }else if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_null(ctx);
  }else{
    sqlite3_result_error(ctx, "error finding common ancestor", -1);
  }
}

int doltliteAncestorRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_merge_base", 2, SQLITE_UTF8, 0,
                                 doltMergeBaseFunc, 0, 0);
}

#endif
