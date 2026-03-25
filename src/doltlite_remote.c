/*
** Transport-agnostic sync engine for Doltlite remotes.
**
** Implements BFS-based chunk synchronization between a local ChunkStore
** and an abstract DoltliteRemote destination.  The filesystem remote
** implementation wraps a second ChunkStore as the remote.
**
** SQL interface (registered elsewhere):
**   SELECT dolt_push('origin', 'main');
**   SELECT dolt_fetch('origin', 'main');
**   SELECT dolt_clone('path/to/remote.doltlite');
*/
#ifdef DOLTLITE_PROLLY

#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "prolly_node.h"
#include <string.h>

/* ----------------------------------------------------------------
** Hash set (reused from doltlite_gc.c pattern).
** Open-addressing hash table keyed by ProllyHash.
** ---------------------------------------------------------------- */

typedef struct SyncHashSet SyncHashSet;
struct SyncHashSet {
  ProllyHash *aSlots;
  u8 *aUsed;
  int nSlots;
  int nUsed;
};

static int syncHashSetInit(SyncHashSet *hs, int nCapacity){
  int n = 256;
  while( n < nCapacity*2 ) n *= 2;
  hs->aSlots = sqlite3_malloc(n * sizeof(ProllyHash));
  hs->aUsed = sqlite3_malloc(n);
  if( !hs->aSlots || !hs->aUsed ){
    sqlite3_free(hs->aSlots);
    sqlite3_free(hs->aUsed);
    return SQLITE_NOMEM;
  }
  memset(hs->aUsed, 0, n);
  hs->nSlots = n;
  hs->nUsed = 0;
  return SQLITE_OK;
}

static void syncHashSetFree(SyncHashSet *hs){
  sqlite3_free(hs->aSlots);
  sqlite3_free(hs->aUsed);
  memset(hs, 0, sizeof(*hs));
}

static u32 syncSlotIndex(const ProllyHash *h, int nSlots){
  u32 v = (u32)h->data[0] | ((u32)h->data[1]<<8) |
          ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24);
  return v & (nSlots - 1);
}

static int syncHashSetContains(SyncHashSet *hs, const ProllyHash *h){
  u32 idx = syncSlotIndex(h, hs->nSlots);
  int i;
  for(i=0; i<hs->nSlots; i++){
    u32 slot = (idx + i) & (hs->nSlots - 1);
    if( !hs->aUsed[slot] ) return 0;
    if( memcmp(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE)==0 ) return 1;
  }
  return 0;
}

static int syncHashSetGrow(SyncHashSet *hs);

static int syncHashSetAdd(SyncHashSet *hs, const ProllyHash *h){
  u32 idx;
  int i;
  if( hs->nUsed >= hs->nSlots / 2 ){
    int rc = syncHashSetGrow(hs);
    if( rc!=SQLITE_OK ) return rc;
  }
  idx = syncSlotIndex(h, hs->nSlots);
  for(i=0; i<hs->nSlots; i++){
    u32 slot = (idx + i) & (hs->nSlots - 1);
    if( !hs->aUsed[slot] ){
      memcpy(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE);
      hs->aUsed[slot] = 1;
      hs->nUsed++;
      return SQLITE_OK;
    }
    if( memcmp(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE)==0 ){
      return SQLITE_OK; /* already present */
    }
  }
  return SQLITE_FULL;
}

static int syncHashSetGrow(SyncHashSet *hs){
  SyncHashSet newHs;
  int i, rc;
  int newSize = hs->nSlots * 2;
  newHs.aSlots = sqlite3_malloc(newSize * sizeof(ProllyHash));
  newHs.aUsed = sqlite3_malloc(newSize);
  if( !newHs.aSlots || !newHs.aUsed ){
    sqlite3_free(newHs.aSlots);
    sqlite3_free(newHs.aUsed);
    return SQLITE_NOMEM;
  }
  memset(newHs.aUsed, 0, newSize);
  newHs.nSlots = newSize;
  newHs.nUsed = 0;
  for(i=0; i<hs->nSlots; i++){
    if( hs->aUsed[i] ){
      rc = syncHashSetAdd(&newHs, &hs->aSlots[i]);
      if( rc!=SQLITE_OK ){
        sqlite3_free(newHs.aSlots);
        sqlite3_free(newHs.aUsed);
        return rc;
      }
    }
  }
  sqlite3_free(hs->aSlots);
  sqlite3_free(hs->aUsed);
  *hs = newHs;
  return SQLITE_OK;
}

/* ----------------------------------------------------------------
** BFS queue for sync traversal.
** ---------------------------------------------------------------- */

typedef struct SyncQueue SyncQueue;
struct SyncQueue {
  ProllyHash *aItems;
  int nItems;
  int nAlloc;
  int iHead;
};

static int syncQueueInit(SyncQueue *q){
  q->nAlloc = 256;
  q->aItems = sqlite3_malloc(q->nAlloc * sizeof(ProllyHash));
  if( !q->aItems ) return SQLITE_NOMEM;
  q->nItems = 0;
  q->iHead = 0;
  return SQLITE_OK;
}

static void syncQueueFree(SyncQueue *q){
  sqlite3_free(q->aItems);
  memset(q, 0, sizeof(*q));
}

static int syncQueuePush(SyncQueue *q, const ProllyHash *h){
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  if( q->nItems >= q->nAlloc ){
    int newAlloc = q->nAlloc * 2;
    ProllyHash *aNew = sqlite3_realloc(q->aItems, newAlloc * sizeof(ProllyHash));
    if( !aNew ) return SQLITE_NOMEM;
    q->aItems = aNew;
    q->nAlloc = newAlloc;
  }
  memcpy(&q->aItems[q->nItems], h, sizeof(ProllyHash));
  q->nItems++;
  return SQLITE_OK;
}

static int syncQueuePop(SyncQueue *q, ProllyHash *h){
  if( q->iHead >= q->nItems ) return 0;
  memcpy(h, &q->aItems[q->iHead], sizeof(ProllyHash));
  q->iHead++;
  return 1;
}

static int syncQueuePending(SyncQueue *q){
  return q->nItems - q->iHead;
}

/* ----------------------------------------------------------------
** Chunk type detection (mirrors doltlite_gc.c helpers).
** ---------------------------------------------------------------- */

#define SYNC_PROLLY_NODE_MAGIC 0x504E4F44

static int syncIsProllyNodeChunk(const u8 *data, int nData){
  u32 m;
  if( nData < 8 ) return 0;
  m = (u32)data[0] | ((u32)data[1]<<8) |
      ((u32)data[2]<<16) | ((u32)data[3]<<24);
  return m == SYNC_PROLLY_NODE_MAGIC;
}

static int syncIsCommitChunk(const u8 *data, int nData){
  if( nData < 30 ) return 0;
  if( data[0] != DOLTLITE_COMMIT_V2 ) return 0;
  if( nData >= 4 ){
    u32 m = (u32)data[0] | ((u32)data[1]<<8) |
            ((u32)data[2]<<16) | ((u32)data[3]<<24);
    if( m == SYNC_PROLLY_NODE_MAGIC ) return 0;
  }
  return 1;
}

/* ----------------------------------------------------------------
** Extract child hashes from a chunk and enqueue them.
** ---------------------------------------------------------------- */

static int syncEnqueueChildren(
  const u8 *data,
  int nData,
  SyncQueue *q,
  SyncHashSet *seen
){
  int rc = SQLITE_OK;
  int i;

  if( syncIsProllyNodeChunk(data, nData) ){
    /* Prolly tree node: follow child hashes if internal (level > 0) */
    ProllyNode node;
    int parseRc = prollyNodeParse(&node, data, nData);
    if( parseRc==SQLITE_OK && node.level > 0 ){
      for(i=0; i<(int)node.nItems; i++){
        ProllyHash childHash;
        prollyNodeChildHash(&node, i, &childHash);
        if( !prollyHashIsEmpty(&childHash) && !syncHashSetContains(seen, &childHash) ){
          rc = syncHashSetAdd(seen, &childHash);
          if( rc==SQLITE_OK ) rc = syncQueuePush(q, &childHash);
        }
        if( rc!=SQLITE_OK ) break;
      }
    }
  }else if( syncIsCommitChunk(data, nData) ){
    /* Commit: follow all parents + catalog hash */
    DoltliteCommit commit;
    memset(&commit, 0, sizeof(commit));
    int drc = doltliteCommitDeserialize(data, nData, &commit);
    if( drc==SQLITE_OK ){
      int pi;
      for(pi=0; pi<commit.nParents && rc==SQLITE_OK; pi++){
        if( !prollyHashIsEmpty(&commit.aParents[pi])
            && !syncHashSetContains(seen, &commit.aParents[pi]) ){
          rc = syncHashSetAdd(seen, &commit.aParents[pi]);
          if( rc==SQLITE_OK ) rc = syncQueuePush(q, &commit.aParents[pi]);
        }
      }
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&commit.rootHash)
          && !syncHashSetContains(seen, &commit.rootHash) ){
        rc = syncHashSetAdd(seen, &commit.rootHash);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &commit.rootHash);
      }
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&commit.catalogHash)
          && !syncHashSetContains(seen, &commit.catalogHash) ){
        rc = syncHashSetAdd(seen, &commit.catalogHash);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &commit.catalogHash);
      }
      doltliteCommitClear(&commit);
    }
  }else{
    /* WorkingSet: version(1=0x01) + staged(20) + merging(1) + mergeCommit(20) + conflicts(20) = 62 bytes */
    if( nData == WS_TOTAL_SIZE && data[0] == 1 ){
      ProllyHash h;
      memcpy(h.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
      if( !prollyHashIsEmpty(&h) && !syncHashSetContains(seen, &h) ){
        rc = syncHashSetAdd(seen, &h);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &h);
      }
      memcpy(h.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&h) && !syncHashSetContains(seen, &h) ){
        rc = syncHashSetAdd(seen, &h);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &h);
      }
      if( rc==SQLITE_OK && data[WS_MERGING_OFF] ){
        memcpy(h.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
        if( !prollyHashIsEmpty(&h) && !syncHashSetContains(seen, &h) ){
          rc = syncHashSetAdd(seen, &h);
          if( rc==SQLITE_OK ) rc = syncQueuePush(q, &h);
        }
      }
    }

    /* Catalog chunk: version(1='C'=0x43) */
    if( nData >= 9 && data[0] == 0x43 ){
      int nTables = (int)(data[5] | (data[6]<<8) |
                          (data[7]<<16) | (data[8]<<24));
      if( nTables >= 0 && nTables < 10000 ){
        const u8 *p = data + 9;
        for(i=0; i<nTables && rc==SQLITE_OK; i++){
          if( p + 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2 > data + nData ) break;
          {
            ProllyHash tableRoot;
            memcpy(tableRoot.data, p + 5, PROLLY_HASH_SIZE);
            if( !prollyHashIsEmpty(&tableRoot) && !syncHashSetContains(seen, &tableRoot) ){
              rc = syncHashSetAdd(seen, &tableRoot);
              if( rc==SQLITE_OK ) rc = syncQueuePush(q, &tableRoot);
            }
          }
          {
            int nameLen;
            p += 4 + 1 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE;
            nameLen = p[0] | (p[1]<<8);
            p += 2 + nameLen;
          }
        }
      }
    }
  }

  return rc;
}

/* ----------------------------------------------------------------
** doltliteSyncChunks: BFS sync engine.
** ---------------------------------------------------------------- */

#define SYNC_BATCH_SIZE 256

int doltliteSyncChunks(
  ChunkStore *pSrc,
  DoltliteRemote *pDst,
  ProllyHash *aRoots,
  int nRoots
){
  SyncQueue queue;
  SyncHashSet seen;
  ProllyHash aBatch[SYNC_BATCH_SIZE];
  u8 aPresent[SYNC_BATCH_SIZE];
  int rc, i;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;

  rc = syncHashSetInit(&seen, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    return rc;
  }

  /* Seed the queue with root hashes */
  for(i=0; i<nRoots && rc==SQLITE_OK; i++){
    if( !prollyHashIsEmpty(&aRoots[i]) && !syncHashSetContains(&seen, &aRoots[i]) ){
      rc = syncHashSetAdd(&seen, &aRoots[i]);
      if( rc==SQLITE_OK ) rc = syncQueuePush(&queue, &aRoots[i]);
    }
  }

  /* BFS loop */
  while( rc==SQLITE_OK && syncQueuePending(&queue) > 0 ){
    int nBatch = 0;

    /* Dequeue a batch of hashes */
    while( nBatch < SYNC_BATCH_SIZE && syncQueuePop(&queue, &aBatch[nBatch]) ){
      nBatch++;
    }
    if( nBatch == 0 ) break;

    /* Check which hashes the destination already has */
    rc = pDst->xHasChunks(pDst, aBatch, nBatch, aPresent);
    if( rc!=SQLITE_OK ) break;

    /* For each missing chunk: fetch from src, put to dst, discover children */
    for(i=0; i<nBatch && rc==SQLITE_OK; i++){
      u8 *data = 0;
      int nData = 0;

      if( aPresent[i] ){
        /* Destination already has this chunk and its entire reachable subtree
        ** (because chunks are content-addressed and immutable). Prune. */
        continue;
      }

      /* Fetch from source */
      rc = chunkStoreGet(pSrc, &aBatch[i], &data, &nData);
      if( rc==SQLITE_NOTFOUND ){
        /* Chunk missing from source -- skip, don't fail the whole sync */
        rc = SQLITE_OK;
        continue;
      }
      if( rc!=SQLITE_OK ) break;

      /* Put to destination */
      rc = pDst->xPutChunk(pDst, &aBatch[i], data, nData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(data);
        break;
      }

      /* Discover child hashes and enqueue them */
      rc = syncEnqueueChildren(data, nData, &queue, &seen);
      sqlite3_free(data);
    }
  }

  syncHashSetFree(&seen);
  syncQueueFree(&queue);
  return rc;
}

/* ----------------------------------------------------------------
** Filesystem remote implementation.
**
** Wraps a second ChunkStore opened on another .doltlite file.
** ---------------------------------------------------------------- */

typedef struct FsRemote FsRemote;
struct FsRemote {
  DoltliteRemote base;   /* Must be first: vtable */
  ChunkStore store;      /* The remote chunk store */
};

static int fsGetChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                      u8 **ppData, int *pnData){
  FsRemote *p = (FsRemote*)pRemote;
  return chunkStoreGet(&p->store, pHash, ppData, pnData);
}

static int fsPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                      const u8 *pData, int nData){
  FsRemote *p = (FsRemote*)pRemote;
  ProllyHash computed;
  (void)pHash; /* Hash is computed by chunkStorePut */
  return chunkStorePut(&p->store, pData, nData, &computed);
}

static int fsHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                       int nHash, u8 *aResult){
  FsRemote *p = (FsRemote*)pRemote;
  int i;
  for(i=0; i<nHash; i++){
    aResult[i] = chunkStoreHas(&p->store, &aHash[i]) ? 1 : 0;
  }
  return SQLITE_OK;
}

static int fsGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  FsRemote *p = (FsRemote*)pRemote;
  *ppData = 0;
  *pnData = 0;
  if( prollyHashIsEmpty(&p->store.refsHash) ){
    return SQLITE_NOTFOUND;
  }
  return chunkStoreGet(&p->store, &p->store.refsHash, ppData, pnData);
}

static int fsSetRefs(DoltliteRemote *pRemote, const u8 *pData, int nData){
  FsRemote *p = (FsRemote*)pRemote;
  ProllyHash refsHash;
  int rc = chunkStorePut(&p->store, pData, nData, &refsHash);
  if( rc==SQLITE_OK ){
    memcpy(&p->store.refsHash, &refsHash, sizeof(ProllyHash));
  }
  return rc;
}

static int fsCommit(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  return chunkStoreCommit(&p->store);
}

static void fsClose(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  chunkStoreClose(&p->store);
  sqlite3_free(p);
}

DoltliteRemote *doltliteFsRemoteOpen(sqlite3_vfs *pVfs, const char *zPath){
  FsRemote *p;
  int rc;
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;

  p = sqlite3_malloc(sizeof(FsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(FsRemote));

  p->base.xGetChunk = fsGetChunk;
  p->base.xPutChunk = fsPutChunk;
  p->base.xHasChunks = fsHasChunks;
  p->base.xGetRefs = fsGetRefs;
  p->base.xSetRefs = fsSetRefs;
  p->base.xCommit = fsCommit;
  p->base.xClose = fsClose;

  rc = chunkStoreOpen(&p->store, pVfs, zPath, flags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(p);
    return 0;
  }

  return &p->base;
}

/* ----------------------------------------------------------------
** "Local as remote" adapter.
**
** Wraps the local ChunkStore with the DoltliteRemote vtable so that
** doltliteSyncChunks can write to it (used by fetch).
** ---------------------------------------------------------------- */

typedef struct LocalAsRemote LocalAsRemote;
struct LocalAsRemote {
  DoltliteRemote base;
  ChunkStore *pStore;   /* Borrowed pointer, NOT owned */
};

static int localGetChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                         u8 **ppData, int *pnData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  return chunkStoreGet(p->pStore, pHash, ppData, pnData);
}

static int localPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                         const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  ProllyHash computed;
  (void)pHash;
  return chunkStorePut(p->pStore, pData, nData, &computed);
}

static int localHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                          int nHash, u8 *aResult){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int i;
  for(i=0; i<nHash; i++){
    aResult[i] = chunkStoreHas(p->pStore, &aHash[i]) ? 1 : 0;
  }
  return SQLITE_OK;
}

static int localGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  *ppData = 0;
  *pnData = 0;
  if( prollyHashIsEmpty(&p->pStore->refsHash) ){
    return SQLITE_NOTFOUND;
  }
  return chunkStoreGet(p->pStore, &p->pStore->refsHash, ppData, pnData);
}

static int localSetRefs(DoltliteRemote *pRemote, const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  ProllyHash refsHash;
  int rc = chunkStorePut(p->pStore, pData, nData, &refsHash);
  if( rc==SQLITE_OK ){
    memcpy(&p->pStore->refsHash, &refsHash, sizeof(ProllyHash));
  }
  return rc;
}

static int localCommit(DoltliteRemote *pRemote){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  return chunkStoreCommit(p->pStore);
}

static void localClose(DoltliteRemote *pRemote){
  /* Do NOT close the chunk store -- we don't own it */
  sqlite3_free(pRemote);
}

static DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal){
  LocalAsRemote *p = sqlite3_malloc(sizeof(LocalAsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(LocalAsRemote));

  p->base.xGetChunk = localGetChunk;
  p->base.xPutChunk = localPutChunk;
  p->base.xHasChunks = localHasChunks;
  p->base.xGetRefs = localGetRefs;
  p->base.xSetRefs = localSetRefs;
  p->base.xCommit = localCommit;
  p->base.xClose = localClose;
  p->pStore = pLocal;

  return &p->base;
}

/* ----------------------------------------------------------------
** Helper: check if commitA is an ancestor of commitB.
** Walks commitB's parent chain looking for commitA.
** Returns 1 if ancestor, 0 if not, negative on error.
** ---------------------------------------------------------------- */

static int syncIsAncestor(
  ChunkStore *cs,
  const ProllyHash *pAncestor,
  const ProllyHash *pDescendant
){
  SyncQueue queue;
  SyncHashSet visited;
  int found = 0;
  int rc;

  if( prollyHashCompare(pAncestor, pDescendant)==0 ) return 1;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return -1;
  rc = syncHashSetInit(&visited, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    return -1;
  }

  syncQueuePush(&queue, pDescendant);
  syncHashSetAdd(&visited, pDescendant);

  while( !found ){
    ProllyHash current;
    u8 *data = 0;
    int nData = 0;

    if( !syncQueuePop(&queue, &current) ) break;

    rc = chunkStoreGet(cs, &current, &data, &nData);
    if( rc!=SQLITE_OK ) break;

    if( syncIsCommitChunk(data, nData) ){
      DoltliteCommit commit;
      memset(&commit, 0, sizeof(commit));
      if( doltliteCommitDeserialize(data, nData, &commit)==SQLITE_OK ){
        int pi;
        for(pi=0; pi<commit.nParents; pi++){
          if( prollyHashIsEmpty(&commit.aParents[pi]) ) continue;
          if( prollyHashCompare(&commit.aParents[pi], pAncestor)==0 ){
            found = 1;
            break;
          }
          if( !syncHashSetContains(&visited, &commit.aParents[pi]) ){
            syncHashSetAdd(&visited, &commit.aParents[pi]);
            syncQueuePush(&queue, &commit.aParents[pi]);
          }
        }
        doltliteCommitClear(&commit);
      }
    }
    sqlite3_free(data);
  }

  syncHashSetFree(&visited);
  syncQueueFree(&queue);
  return found;
}

/* ----------------------------------------------------------------
** doltlitePush: push a branch from local to remote.
** ---------------------------------------------------------------- */

int doltlitePush(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zBranch,
  int bForce
){
  ProllyHash localCommit;
  ProllyHash remoteCommit;
  int rc;
  int i;

  /* 1. Find local branch commit */
  rc = chunkStoreFindBranch(pLocal, zBranch, &localCommit);
  if( rc!=SQLITE_OK ){
    return SQLITE_ERROR; /* branch not found */
  }

  /* 2. If !bForce, check fast-forward safety */
  if( !bForce ){
    u8 *refsData = 0;
    int nRefsData = 0;
    rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
    if( rc==SQLITE_OK && refsData ){
      /* Parse remote refs to find the branch */
      ChunkStore tmpCs;
      memset(&tmpCs, 0, sizeof(tmpCs));
      /* Use a temporary ChunkStore just for ref parsing.
      ** We only need the deserialized branch list. */
      /* Manual parse: version(1) + defLen(2) + def(var) + nBranches(2) ... */
      const u8 *p = refsData;
      if( nRefsData >= 5 ){
        u8 ver = p[0]; p++;
        int defLen = p[0]|(p[1]<<8); p += 2;
        if( p + defLen + 2 <= refsData + nRefsData ){
          int nBranches;
          p += defLen;
          nBranches = p[0]|(p[1]<<8); p += 2;
          for(i=0; i<nBranches; i++){
            int nameLen;
            if( p+2 > refsData+nRefsData ) break;
            nameLen = p[0]|(p[1]<<8); p += 2;
            if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
            if( nameLen==(int)strlen(zBranch) && memcmp(p, zBranch, nameLen)==0 ){
              memcpy(remoteCommit.data, p+nameLen, PROLLY_HASH_SIZE);
              if( !prollyHashIsEmpty(&remoteCommit)
                  && prollyHashCompare(&remoteCommit, &localCommit)!=0 ){
                /* Remote branch exists and differs: check ancestry */
                int isAnc = syncIsAncestor(pLocal, &remoteCommit, &localCommit);
                if( isAnc <= 0 ){
                  sqlite3_free(refsData);
                  return SQLITE_ERROR; /* not a fast-forward */
                }
              }
              break;
            }
            p += nameLen + PROLLY_HASH_SIZE;
            /* Skip workingSetHash if version >= 3 */
            if( ver >= 3 && p+PROLLY_HASH_SIZE <= refsData+nRefsData ){
              p += PROLLY_HASH_SIZE;
            }
          }
        }
      }
      sqlite3_free(refsData);
      (void)tmpCs;
    }else if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK; /* No refs yet on remote -- that's fine */
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  /* 3. Sync all chunks reachable from local branch commit */
  rc = doltliteSyncChunks(pLocal, pRemote, &localCommit, 1);
  if( rc!=SQLITE_OK ) return rc;

  /* 4. Update remote branch ref and manifest state.
  ** For filesystem remotes, we can manipulate the store directly. */
  {
    FsRemote *pFs = (FsRemote*)pRemote;
    /* Update the remote's in-memory branch to point at localCommit */
    rc = chunkStoreUpdateBranch(&pFs->store, zBranch, &localCommit);
    if( rc==SQLITE_NOTFOUND ){
      /* Branch doesn't exist on remote yet -- add it */
      rc = chunkStoreAddBranch(&pFs->store, zBranch, &localCommit);
    }
    if( rc!=SQLITE_OK ) return rc;

    /* If this is the default branch (or the only branch), update manifest
    ** headCommit and catalog so opening the remote shows the new state. */
    {
      const char *zDef = chunkStoreGetDefaultBranch(&pFs->store);
      int isDefault = (zDef && strcmp(zDef, zBranch)==0)
                   || (!zDef && pFs->store.nBranches <= 1);
      if( isDefault ){
        u8 *commitData = 0; int nCommitData = 0;
        rc = chunkStoreGet(&pFs->store, &localCommit, &commitData, &nCommitData);
        if( rc==SQLITE_OK && commitData ){
          DoltliteCommit commit;
          rc = doltliteCommitDeserialize(commitData, nCommitData, &commit);
          sqlite3_free(commitData);
          if( rc==SQLITE_OK ){
            chunkStoreSetHeadCommit(&pFs->store, &localCommit);
            chunkStoreSetCatalog(&pFs->store, &commit.catalogHash);
            doltliteCommitClear(&commit);
          }
        }
      }
    }

    /* Serialize refs and commit */
    rc = chunkStoreSerializeRefs(&pFs->store);
    if( rc==SQLITE_OK ) rc = pRemote->xCommit(pRemote);
  }

  return rc;
}

/* ----------------------------------------------------------------
** doltliteFetch: fetch a branch from remote into local tracking branch.
** ---------------------------------------------------------------- */

int doltliteFetch(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zRemoteName,
  const char *zBranch
){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash remoteCommit;
  DoltliteRemote *pLocalDst = 0;
  int rc;
  int found = 0;

  memset(&remoteCommit, 0, sizeof(remoteCommit));

  /* 1. Get remote refs to find the branch commit hash */
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  /* Parse remote refs to find the target branch */
  if( nRefsData >= 5 ){
    const u8 *p = refsData;
    u8 ver = p[0]; p++;
    int defLen = p[0]|(p[1]<<8); p += 2;
    if( p + defLen + 2 <= refsData + nRefsData ){
      int nBranches, i;
      p += defLen;
      nBranches = p[0]|(p[1]<<8); p += 2;
      for(i=0; i<nBranches; i++){
        int nameLen;
        if( p+2 > refsData+nRefsData ) break;
        nameLen = p[0]|(p[1]<<8); p += 2;
        if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
        if( nameLen==(int)strlen(zBranch) && memcmp(p, zBranch, nameLen)==0 ){
          memcpy(remoteCommit.data, p+nameLen, PROLLY_HASH_SIZE);
          found = 1;
        }
        p += nameLen + PROLLY_HASH_SIZE;
        if( ver >= 3 && p+PROLLY_HASH_SIZE <= refsData+nRefsData ){
          p += PROLLY_HASH_SIZE;
        }
        if( found ) break;
      }
    }
  }
  sqlite3_free(refsData);

  if( !found || prollyHashIsEmpty(&remoteCommit) ){
    return SQLITE_NOTFOUND; /* Branch not found on remote */
  }

  /* 2. Sync chunks from remote's ChunkStore to local.
  ** We need a ChunkStore for the source (remote) and a DoltliteRemote wrapping local. */
  pLocalDst = doltliteLocalAsRemote(pLocal);
  if( !pLocalDst ) return SQLITE_NOMEM;

  /* For filesystem remotes, the source ChunkStore is inside the FsRemote */
  {
    FsRemote *pFs = (FsRemote*)pRemote;
    rc = doltliteSyncChunks(&pFs->store, pLocalDst, &remoteCommit, 1);
  }

  pLocalDst->xClose(pLocalDst);
  if( rc!=SQLITE_OK ) return rc;

  /* 3. Update local tracking branch (e.g., origin/main) */
  rc = chunkStoreUpdateTracking(pLocal, zRemoteName, zBranch, &remoteCommit);
  if( rc!=SQLITE_OK ) return rc;

  /* 4. Serialize refs and commit */
  rc = chunkStoreSerializeRefs(pLocal);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(pLocal);

  return rc;
}

/* ----------------------------------------------------------------
** doltliteClone: copy ALL chunks and refs from remote to local.
** ---------------------------------------------------------------- */

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash *aRoots = 0;
  int nRoots = 0;
  int nRootsAlloc = 0;
  DoltliteRemote *pLocalDst = 0;
  int rc;

  /* 1. Get remote refs */
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  /* 2. Parse refs to collect all branch and tag commit hashes */
  if( nRefsData >= 5 ){
    const u8 *p = refsData;
    u8 ver = p[0]; p++;
    int defLen = p[0]|(p[1]<<8); p += 2;
    if( p + defLen + 2 <= refsData + nRefsData ){
      int nBranches, nTags, i;
      p += defLen;
      nBranches = p[0]|(p[1]<<8); p += 2;

      nRootsAlloc = nBranches + 16;
      aRoots = sqlite3_malloc(nRootsAlloc * sizeof(ProllyHash));
      if( !aRoots ){
        sqlite3_free(refsData);
        return SQLITE_NOMEM;
      }

      for(i=0; i<nBranches; i++){
        int nameLen;
        if( p+2 > refsData+nRefsData ) break;
        nameLen = p[0]|(p[1]<<8); p += 2;
        if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
        p += nameLen;
        /* Collect commit hash */
        memcpy(aRoots[nRoots].data, p, PROLLY_HASH_SIZE);
        if( !prollyHashIsEmpty(&aRoots[nRoots]) ) nRoots++;
        p += PROLLY_HASH_SIZE;
        /* Skip workingSetHash for v3+ */
        if( ver >= 3 && p+PROLLY_HASH_SIZE <= refsData+nRefsData ){
          p += PROLLY_HASH_SIZE;
        }
      }

      /* Tags */
      if( ver >= 2 && p+2 <= refsData+nRefsData ){
        nTags = p[0]|(p[1]<<8); p += 2;
        if( nRoots + nTags > nRootsAlloc ){
          nRootsAlloc = nRoots + nTags + 8;
          aRoots = sqlite3_realloc(aRoots, nRootsAlloc * sizeof(ProllyHash));
          if( !aRoots ){
            sqlite3_free(refsData);
            return SQLITE_NOMEM;
          }
        }
        for(i=0; i<nTags; i++){
          int nameLen;
          if( p+2 > refsData+nRefsData ) break;
          nameLen = p[0]|(p[1]<<8); p += 2;
          if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
          p += nameLen;
          memcpy(aRoots[nRoots].data, p, PROLLY_HASH_SIZE);
          if( !prollyHashIsEmpty(&aRoots[nRoots]) ) nRoots++;
          p += PROLLY_HASH_SIZE;
        }
      }
    }
  }

  if( nRoots == 0 ){
    /* Remote is empty -- just copy the refs (which might set default branch) */
    sqlite3_free(aRoots);
    /* Fall through to copy refs below */
  }else{
    /* 3. Sync all reachable chunks */
    pLocalDst = doltliteLocalAsRemote(pLocal);
    if( !pLocalDst ){
      sqlite3_free(aRoots);
      sqlite3_free(refsData);
      return SQLITE_NOMEM;
    }

    {
      FsRemote *pFs = (FsRemote*)pRemote;
      rc = doltliteSyncChunks(&pFs->store, pLocalDst, aRoots, nRoots);
    }

    pLocalDst->xClose(pLocalDst);
    sqlite3_free(aRoots);
    aRoots = 0;

    if( rc!=SQLITE_OK ){
      sqlite3_free(refsData);
      return rc;
    }
  }

  /* 4. Copy refs to local: put the raw refs chunk and update the local refsHash */
  if( refsData && nRefsData > 0 ){
    ProllyHash refsHash;
    rc = chunkStorePut(pLocal, refsData, nRefsData, &refsHash);
    if( rc==SQLITE_OK ){
      memcpy(&pLocal->refsHash, &refsHash, sizeof(ProllyHash));
    }
  }
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ) return rc;

  /* 5. Commit local */
  rc = chunkStoreCommit(pLocal);
  if( rc!=SQLITE_OK ) return rc;

  /* 6. Reload refs to populate in-memory branch/tag/remote arrays */
  rc = chunkStoreReloadRefs(pLocal);

  return rc;
}

#endif /* DOLTLITE_PROLLY */
