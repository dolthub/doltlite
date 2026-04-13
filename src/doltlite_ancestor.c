
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

/* Merge-base by set-intersection BFS: walk commit1's ancestry in full,
** then BFS commit2 and return the first hash that appears in the
** ancestor set. Fine for shallow histories; for Git-style "lowest"
** LCA on deep merges the right algorithm is both-sides BFS with
** generation numbers, but Dolt's merge semantics accept any common
** ancestor produced here. */
int doltliteFindAncestor(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  ProllyHash *pAncestor
){
  HashSet ancestors;
  int rc;

  memset(pAncestor, 0, sizeof(*pAncestor));

  if( prollyHashIsEmpty(commitHash1) || prollyHashIsEmpty(commitHash2) ){
    return SQLITE_NOTFOUND;
  }

  if( prollyHashCompare(commitHash1, commitHash2)==0 ){
    *pAncestor = *commitHash1;
    return SQLITE_OK;
  }


  rc = hashSetInit(&ancestors, 64);
  if( rc!=SQLITE_OK ) return rc;

  rc = ancestorBfsCollect(db, commitHash1, &ancestors);
  if( rc!=SQLITE_OK ){
    hashSetFree(&ancestors);
    return rc;
  }


  {
    ProllyHash *queue = 0;
    int qHead = 0, qTail = 0, qAlloc = 64;
    ProllyHash current;
    DoltliteCommit commit;
    HashSet visited;
    int i;

    rc = hashSetInit(&visited, 64);
    if( rc!=SQLITE_OK ){ hashSetFree(&ancestors); return rc; }

    queue = sqlite3_malloc(qAlloc * (int)sizeof(ProllyHash));
    if( !queue ){ hashSetFree(&visited); hashSetFree(&ancestors); return SQLITE_NOMEM; }
    queue[qTail++] = *commitHash2;

    while( qHead < qTail ){
      current = queue[qHead++];
      if( prollyHashIsEmpty(&current) ) continue;
      if( hashSetContains(&visited, &current) ) continue;
      rc = hashSetInsert(&visited, &current);
      if( rc!=SQLITE_OK ){
        hashSetFree(&visited);
        sqlite3_free(queue);
        hashSetFree(&ancestors);
        return rc;
      }
      if( hashSetContains(&ancestors, &current) ){
        *pAncestor = current;
        hashSetFree(&visited);
        sqlite3_free(queue);
        hashSetFree(&ancestors);
        return SQLITE_OK;
      }
      memset(&commit, 0, sizeof(commit));
      rc = loadCommitByHash(db, &current, &commit);
      if( rc!=SQLITE_OK ){
        hashSetFree(&visited);
        sqlite3_free(queue);
        hashSetFree(&ancestors);
        return rc;
      }
      for(i=0; i<doltliteCommitParentCount(&commit); i++){
        const ProllyHash *pParent = doltliteCommitParentHash(&commit, i);
        if( !pParent ) continue;
        if( qTail >= qAlloc ){
          ProllyHash *q2;
          qAlloc *= 2;
          q2 = sqlite3_realloc(queue, qAlloc*(int)sizeof(ProllyHash));
          if( !q2 ){ doltliteCommitClear(&commit); hashSetFree(&visited); sqlite3_free(queue); hashSetFree(&ancestors); return SQLITE_NOMEM; }
          queue = q2;
        }
        queue[qTail++] = *pParent;
      }
      doltliteCommitClear(&commit);
    }

    hashSetFree(&visited);
    sqlite3_free(queue);
  }

  hashSetFree(&ancestors);
  return SQLITE_NOTFOUND;
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
