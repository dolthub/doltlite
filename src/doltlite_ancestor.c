
#ifdef DOLTLITE_PROLLY

#include "doltlite_ancestor.h"
#include "doltlite_commit.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

typedef struct HashSet HashSet;
struct HashSet {
  ProllyHash *aSlot;
  u8 *aUsed;
  int nSlot;
  int nUsed;
};

static int hashSetInit(HashSet *hs, int nInitial){
  int n = 16;
  while( n < nInitial*2 ) n *= 2;
  hs->aSlot = sqlite3_malloc(n * sizeof(ProllyHash));
  if( !hs->aSlot ) return SQLITE_NOMEM;
  hs->aUsed = sqlite3_malloc(n);
  if( !hs->aUsed ){
    sqlite3_free(hs->aSlot);
    hs->aSlot = 0;
    return SQLITE_NOMEM;
  }
  memset(hs->aUsed, 0, n);
  hs->nSlot = n;
  hs->nUsed = 0;
  return SQLITE_OK;
}

static void hashSetFree(HashSet *hs){
  sqlite3_free(hs->aSlot);
  sqlite3_free(hs->aUsed);
  memset(hs, 0, sizeof(*hs));
}

static int hashSetIndex(const HashSet *hs, const ProllyHash *h){
  u32 v = (u32)h->data[0] | ((u32)h->data[1]<<8)
        | ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24);
  return (int)(v & (u32)(hs->nSlot - 1));
}

static int hashSetGrow(HashSet *hs){
  HashSet newHs;
  int i;
  int newN = hs->nSlot * 2;
  newHs.aSlot = sqlite3_malloc(newN * sizeof(ProllyHash));
  if( !newHs.aSlot ) return SQLITE_NOMEM;
  newHs.aUsed = sqlite3_malloc(newN);
  if( !newHs.aUsed ){
    sqlite3_free(newHs.aSlot);
    return SQLITE_NOMEM;
  }
  memset(newHs.aUsed, 0, newN);
  newHs.nSlot = newN;
  newHs.nUsed = 0;
  for(i=0; i<hs->nSlot; i++){
    if( hs->aUsed[i] ){
      int idx = hashSetIndex(&newHs, &hs->aSlot[i]);
      while( newHs.aUsed[idx] ){
        idx = (idx + 1) & (newN - 1);
      }
      newHs.aSlot[idx] = hs->aSlot[i];
      newHs.aUsed[idx] = 1;
      newHs.nUsed++;
    }
  }
  sqlite3_free(hs->aSlot);
  sqlite3_free(hs->aUsed);
  *hs = newHs;
  return SQLITE_OK;
}

static int hashSetInsert(HashSet *hs, const ProllyHash *h){
  int idx;
  if( hs->nUsed * 2 >= hs->nSlot ){
    int rc = hashSetGrow(hs);
    if( rc!=SQLITE_OK ) return rc;
  }
  idx = hashSetIndex(hs, h);
  while( hs->aUsed[idx] ){
    if( prollyHashCompare(&hs->aSlot[idx], h)==0 ) return SQLITE_OK;
    idx = (idx + 1) & (hs->nSlot - 1);
  }
  hs->aSlot[idx] = *h;
  hs->aUsed[idx] = 1;
  hs->nUsed++;
  return SQLITE_OK;
}

static int hashSetContains(const HashSet *hs, const ProllyHash *h){
  int idx = hashSetIndex(hs, h);
  while( hs->aUsed[idx] ){
    if( prollyHashCompare(&hs->aSlot[idx], h)==0 ) return 1;
    idx = (idx + 1) & (hs->nSlot - 1);
  }
  return 0;
}

static int loadCommitByHash(sqlite3 *db, const ProllyHash *hash,
                            DoltliteCommit *pCommit){
  if( prollyHashIsEmpty(hash) ) return SQLITE_NOTFOUND;
  return doltliteLoadCommit(db, hash, pCommit);
}

static int ancestorBfsCollect(
  sqlite3 *db,
  const ProllyHash *pStart,
  HashSet *pVisited
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
    if( hashSetContains(pVisited, &current) ) continue;
    rc = hashSetInsert(pVisited, &current);
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
  const HashSet *pCommon,
  u8 *aRedundant,
  int nCommon,
  const ProllyHash *aCommon
){
  ProllyHash *queue = 0;
  int qHead = 0, qTail = 0, qAlloc = 64;
  ProllyHash current;
  DoltliteCommit commit;
  HashSet visited;
  int rc;
  int i;

  rc = hashSetInit(&visited, 64);
  if( rc!=SQLITE_OK ) return rc;

  queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
  if( !queue ){ hashSetFree(&visited); return SQLITE_NOMEM; }

  memset(&commit, 0, sizeof(commit));
  rc = loadCommitByHash(db, pStart, &commit);
  if( rc!=SQLITE_OK ){
    sqlite3_free(queue);
    hashSetFree(&visited);
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
    if( hashSetContains(&visited, &current) ) continue;
    rc = hashSetInsert(&visited, &current);
    if( rc!=SQLITE_OK ) break;
    if( hashSetContains(pCommon, &current) ){
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
  hashSetFree(&visited);
  return rc;
}

int doltliteFindAncestor(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  ProllyHash *pAncestor
){
  HashSet anc1, anc2, common;
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

  rc = hashSetInit(&anc1, 64);
  if( rc!=SQLITE_OK ) return rc;
  rc = hashSetInit(&anc2, 64);
  if( rc!=SQLITE_OK ){ hashSetFree(&anc1); return rc; }
  rc = hashSetInit(&common, 64);
  if( rc!=SQLITE_OK ){ hashSetFree(&anc2); hashSetFree(&anc1); return rc; }

  rc = ancestorBfsCollect(db, commitHash1, &anc1);
  if( rc!=SQLITE_OK ) goto done;
  rc = ancestorBfsCollect(db, commitHash2, &anc2);
  if( rc!=SQLITE_OK ) goto done;

  for(i=0; i<anc1.nSlot; i++){
    if( anc1.aUsed[i] && hashSetContains(&anc2, &anc1.aSlot[i]) ){
      rc = hashSetInsert(&common, &anc1.aSlot[i]);
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
    for(i=0; i<common.nSlot; i++){
      if( common.aUsed[i] ){
        aCommon[k++] = common.aSlot[i];
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
    i64 bestTs = 0;
    DoltliteCommit commit;
    for(i=0; i<nCommon; i++){
      if( aRedundant[i] ) continue;
      memset(&commit, 0, sizeof(commit));
      rc = loadCommitByHash(db, &aCommon[i], &commit);
      if( rc!=SQLITE_OK ) goto done;
      if( iBest<0
       || commit.timestamp > bestTs
       || (commit.timestamp == bestTs
           && prollyHashCompare(&aCommon[i], &aCommon[iBest])<0) ){
        iBest = i;
        bestTs = commit.timestamp;
      }
      doltliteCommitClear(&commit);
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
  hashSetFree(&common);
  hashSetFree(&anc2);
  hashSetFree(&anc1);
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
