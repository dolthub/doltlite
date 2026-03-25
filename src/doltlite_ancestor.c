/*
** Find the common ancestor (merge base) of two commits.
**
** Algorithm: collect all ancestors of commit1 (including itself) into a
** hash set, then walk commit2's chain until finding one in the set.
** This is the simplest correct approach for single-parent commit chains.
*/
#ifdef DOLTLITE_PROLLY

#include "doltlite_ancestor.h"
#include "doltlite_commit.h"
#include "chunk_store.h"
#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);

/* --------------------------------------------------------------------------
** Simple hash set for ProllyHash values.
** Open-addressing with linear probing. Sized to a power of 2.
** -------------------------------------------------------------------------- */

typedef struct HashSet HashSet;
struct HashSet {
  ProllyHash *aSlot;   /* Array of slots */
  u8 *aUsed;           /* 1 if slot occupied, 0 if empty */
  int nSlot;           /* Number of slots (power of 2) */
  int nUsed;           /* Number of occupied slots */
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

/* Hash a ProllyHash to a slot index. Use first 4 bytes as a u32. */
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

/* Insert a hash. Returns SQLITE_OK or SQLITE_NOMEM. */
static int hashSetInsert(HashSet *hs, const ProllyHash *h){
  int idx;
  if( hs->nUsed * 2 >= hs->nSlot ){
    int rc = hashSetGrow(hs);
    if( rc!=SQLITE_OK ) return rc;
  }
  idx = hashSetIndex(hs, h);
  while( hs->aUsed[idx] ){
    if( prollyHashCompare(&hs->aSlot[idx], h)==0 ) return SQLITE_OK; /* dup */
    idx = (idx + 1) & (hs->nSlot - 1);
  }
  hs->aSlot[idx] = *h;
  hs->aUsed[idx] = 1;
  hs->nUsed++;
  return SQLITE_OK;
}

/* Check if a hash is in the set. Returns 1 if found, 0 if not. */
static int hashSetContains(const HashSet *hs, const ProllyHash *h){
  int idx = hashSetIndex(hs, h);
  while( hs->aUsed[idx] ){
    if( prollyHashCompare(&hs->aSlot[idx], h)==0 ) return 1;
    idx = (idx + 1) & (hs->nSlot - 1);
  }
  return 0;
}

/* --------------------------------------------------------------------------
** Load a commit by hash from the chunk store (same pattern as doltlite_log.c)
** -------------------------------------------------------------------------- */

static int loadCommitByHash(sqlite3 *db, const ProllyHash *hash,
                            DoltliteCommit *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !cs ) return SQLITE_ERROR;
  if( prollyHashIsEmpty(hash) ) return SQLITE_NOTFOUND;

  rc = chunkStoreGet(cs, hash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteCommitDeserialize(data, nData, pCommit);
  sqlite3_free(data);
  return rc;
}

/* --------------------------------------------------------------------------
** BFS from one commit, populating the visited set with all ancestors.
** -------------------------------------------------------------------------- */

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
    if( rc!=SQLITE_OK ){ rc = SQLITE_OK; continue; } /* end of chain */
    for(i=0; i<commit.nParents; i++){
      if( qTail >= qAlloc ){
        qAlloc *= 2;
        ProllyHash *q2 = sqlite3_realloc(queue, qAlloc*(int)sizeof(ProllyHash));
        if( !q2 ){ doltliteCommitClear(&commit); rc=SQLITE_NOMEM; break; }
        queue = q2;
      }
      queue[qTail++] = commit.aParents[i];
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) break;
  }

  sqlite3_free(queue);
  return rc;
}

/* --------------------------------------------------------------------------
** doltliteFindAncestor
** -------------------------------------------------------------------------- */

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

  /* Step 1: Collect all ancestors of commit1 via BFS */
  rc = hashSetInit(&ancestors, 64);
  if( rc!=SQLITE_OK ) return rc;

  rc = ancestorBfsCollect(db, commitHash1, &ancestors);
  if( rc!=SQLITE_OK ){
    hashSetFree(&ancestors);
    return rc;
  }

  /* Step 2: BFS commit2's ancestors, checking against the set */
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
      hashSetInsert(&visited, &current);
      if( hashSetContains(&ancestors, &current) ){
        *pAncestor = current;
        hashSetFree(&visited);
        sqlite3_free(queue);
        hashSetFree(&ancestors);
        return SQLITE_OK;
      }
      memset(&commit, 0, sizeof(commit));
      rc = loadCommitByHash(db, &current, &commit);
      if( rc!=SQLITE_OK ){ rc = SQLITE_OK; continue; }
      for(i=0; i<commit.nParents; i++){
        if( qTail >= qAlloc ){
          qAlloc *= 2;
          ProllyHash *q2 = sqlite3_realloc(queue, qAlloc*(int)sizeof(ProllyHash));
          if( !q2 ){ doltliteCommitClear(&commit); hashSetFree(&visited); sqlite3_free(queue); hashSetFree(&ancestors); return SQLITE_NOMEM; }
          queue = q2;
        }
        queue[qTail++] = commit.aParents[i];
      }
      doltliteCommitClear(&commit);
    }

    hashSetFree(&visited);
    sqlite3_free(queue);
  }

  hashSetFree(&ancestors);
  return SQLITE_NOTFOUND;
}

/* --------------------------------------------------------------------------
** dolt_merge_base(hash1, hash2) SQL function
**
** Returns the hex hash of the common ancestor commit, or NULL if none.
** Arguments can be commit hashes (40-char hex) or branch names.
** -------------------------------------------------------------------------- */

extern int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
extern int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

static int resolveCommitRef(sqlite3 *db, const char *zRef, ProllyHash *pHash){
  ChunkStore *cs;
  /* Try as hex hash first */
  if( zRef && strlen(zRef)==PROLLY_HASH_SIZE*2 ){
    if( doltliteHexToHash(zRef, pHash)==SQLITE_OK ) return SQLITE_OK;
  }
  cs = doltliteGetChunkStore(db);
  if( !cs ) return SQLITE_ERROR;
  /* Try as branch name */
  if( chunkStoreFindBranch(cs, zRef, pHash)==SQLITE_OK ){
    return SQLITE_OK;
  }
  /* Try as tag name */
  if( chunkStoreFindTag(cs, zRef, pHash)==SQLITE_OK ){
    return SQLITE_OK;
  }
  return SQLITE_ERROR;
}

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

  rc = resolveCommitRef(db, zRef1, &hash1);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "could not resolve first argument to a commit", -1);
    return;
  }
  rc = resolveCommitRef(db, zRef2, &hash2);
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

#endif /* DOLTLITE_PROLLY */
