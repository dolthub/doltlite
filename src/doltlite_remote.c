
#ifdef DOLTLITE_PROLLY

#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include "doltlite_chunk_walk.h"
#include <string.h>
#ifndef _WIN32
#include <errno.h>
#include <unistd.h>
#endif

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
  int rc;
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  rc = DOLTLITE_GROW_ARRAY(&q->aItems, &q->nAlloc, q->nItems + 1, 16);
  if( rc!=SQLITE_OK ) return rc;
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

static int remoteLoadRefsView(const u8 *pData, int nData, ChunkStore *pRefs){
  memset(pRefs, 0, sizeof(*pRefs));
  if( !pData || nData<=0 ) return SQLITE_NOTFOUND;
  return chunkStoreLoadRefsFromBlob(pRefs, pData, nData);
}

static int remoteFindBranchFromRefsBlob(
  const u8 *pData, int nData, const char *zBranch, ProllyHash *pCommit
){
  ChunkStore refsView;
  int rc = remoteLoadRefsView(pData, nData, &refsView);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStoreFindBranch(&refsView, zBranch, pCommit);
  chunkStoreClose(&refsView);
  return rc;
}

static int remoteCollectRootsFromRefsBlob(
  const u8 *pData, int nData, ProllyHash **paRoots, int *pnRoots
){
  ChunkStore refsView;
  ProllyHash *aRoots = 0;
  int nRoots = 0;
  int nAlloc = 0;
  int rc;
  int i;

  *paRoots = 0;
  *pnRoots = 0;
  rc = remoteLoadRefsView(pData, nData, &refsView);
  if( rc!=SQLITE_OK ) return rc;

  {
    int nBr, nTg;
    i64 nAlloc64;
    const BranchRef *aBr;
    const TagRef *aTg;
    refsTableGetBranches(&refsView.refs, &nBr, &aBr);
    refsTableGetTags(&refsView.refs, &nTg, &aTg);
    nAlloc64 = (i64)nBr + (i64)nTg + 1;
    if( nAlloc64 > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ){
      chunkStoreClose(&refsView);
      return SQLITE_CORRUPT;
    }
    nAlloc = (int)nAlloc64;
    if( nAlloc>0 ){
      aRoots = sqlite3_malloc64((sqlite3_uint64)nAlloc * sizeof(ProllyHash));
      if( !aRoots ){
        chunkStoreClose(&refsView);
        return SQLITE_NOMEM;
      }
    }
    for(i=0; i<nBr; i++){
      if( !prollyHashIsEmpty(&aBr[i].commitHash) ){
        aRoots[nRoots++] = aBr[i].commitHash;
      }
    }
    for(i=0; i<nTg; i++){
      if( !prollyHashIsEmpty(&aTg[i].commitHash) ){
        aRoots[nRoots++] = aTg[i].commitHash;
      }
    }
  }

  chunkStoreClose(&refsView);
  *paRoots = aRoots;
  *pnRoots = nRoots;
  return SQLITE_OK;
}

static int remoteLoadCommitCatalogHash(
  ChunkStore *cs,
  const ProllyHash *pCommitHash,
  ProllyHash *pCatalogHash
){
  u8 *pData = 0;
  int nData = 0;
  DoltliteCommit c;
  int rc;

  if( pCatalogHash ) memset(pCatalogHash, 0, sizeof(*pCatalogHash));
  if( !cs || !pCommitHash || prollyHashIsEmpty(pCommitHash) || !pCatalogHash ){
    return SQLITE_ERROR;
  }

  rc = chunkStoreGet(cs, pCommitHash, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;

  memset(&c, 0, sizeof(c));
  rc = doltliteCommitDeserialize(pData, nData, &c);
  sqlite3_free(pData);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&c);
    return rc;
  }

  memcpy(pCatalogHash, &c.catalogHash, sizeof(*pCatalogHash));
  doltliteCommitClear(&c);
  return SQLITE_OK;
}

typedef struct SyncEnqCtx SyncEnqCtx;
struct SyncEnqCtx {
  SyncQueue *q;
  ProllyHashSet *seen;
};

static int syncChildCb(void *pCtx, const ProllyHash *pHash){
  SyncEnqCtx *ctx = (SyncEnqCtx*)pCtx;
  int rc;
  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  if( prollyHashSetContains(ctx->seen, pHash) ) return SQLITE_OK;
  rc = prollyHashSetAdd(ctx->seen, pHash);
  if( rc==SQLITE_OK ) rc = syncQueuePush(ctx->q, pHash);
  return rc;
}

static int syncEnqueueChildren(
  const u8 *data,
  int nData,
  SyncQueue *q,
  ProllyHashSet *seen
){
  SyncEnqCtx ctx;
  ctx.q = q;
  ctx.seen = seen;
  return doltliteEnumerateChunkChildren(data, nData, syncChildCb, &ctx);
}

#define SYNC_BATCH_SIZE 256

/* A fetched chunk must hash to the address it was requested under. Without this
** a remote serving the wrong bytes is stored under their own address, leaving
** the requested one absent and surfacing much later as a confusing NOTFOUND --
** and the payload is walked by syncEnqueueChildren either way, so unverified
** content would steer the traversal. */
static int syncVerifyFetchedChunk(
  const ProllyHash *pWant,
  const u8 *pData,
  int nData
){
  ProllyHash computed;
  if( !pData || nData<0 ) return SQLITE_CORRUPT;
  prollyHashCompute(pData, nData, &computed);
  if( prollyHashCompare(&computed, pWant)!=0 ) return SQLITE_CORRUPT;
  return SQLITE_OK;
}

int doltliteSyncChunks(
  DoltliteRemote *pSrc,
  DoltliteRemote *pDst,
  ProllyHash *aRoots,
  int nRoots
){
  SyncQueue queue;
  ProllyHashSet seen;
  ProllyHash aBatch[SYNC_BATCH_SIZE];
  u8 aPresent[SYNC_BATCH_SIZE];
  int bFirstBatch = 1;
  int bResumeScan = 0;
  int rc, i;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyHashSetInit(&seen, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    return rc;
  }

  for(i=0; i<nRoots && rc==SQLITE_OK; i++){
    if( !prollyHashIsEmpty(&aRoots[i]) && !prollyHashSetContains(&seen, &aRoots[i]) ){
      rc = prollyHashSetAdd(&seen, &aRoots[i]);
      if( rc==SQLITE_OK ) rc = syncQueuePush(&queue, &aRoots[i]);
    }
  }

  while( rc==SQLITE_OK && syncQueuePending(&queue) > 0 ){
    int nBatch = 0;

    while( nBatch < SYNC_BATCH_SIZE && syncQueuePop(&queue, &aBatch[nBatch]) ){
      nBatch++;
    }
    if( nBatch == 0 ) break;

    rc = pDst->xHasChunks(pDst, aBatch, nBatch, aPresent);
    if( rc!=SQLITE_OK ) break;
    if( bFirstBatch && pDst->bResumePartialPuts ){
      bResumeScan = 1;
      for(i=0; i<nBatch; i++){
        if( !aPresent[i] ){
          bResumeScan = 0;
          break;
        }
      }
    }
    bFirstBatch = 0;

    /* A resumed partial put may have persisted a parent before its missing
    ** descendants. Scan below present roots in that case. */
    {
      ProllyHash aFetch[SYNC_BATCH_SIZE];
      u8 aPut[SYNC_BATCH_SIZE];
      int nFetch = 0;

      for(i=0; i<nBatch; i++){
        if( !aPresent[i] || bResumeScan ){
          aFetch[nFetch] = aBatch[i];
          aPut[nFetch] = !aPresent[i];
          nFetch++;
        }
      }
      if( nFetch==0 ) continue;

      if( pSrc->xGetChunks ){
        u8 *apData[SYNC_BATCH_SIZE];
        int anData[SYNC_BATCH_SIZE];

        memset(apData, 0, sizeof(apData[0]) * nFetch);
        rc = pSrc->xGetChunks(pSrc, aFetch, nFetch, apData, anData);
        for(i=0; i<nFetch && rc==SQLITE_OK; i++){
          if( !apData[i] ){ rc = SQLITE_NOTFOUND; break; }
          rc = syncVerifyFetchedChunk(&aFetch[i], apData[i], anData[i]);
          if( rc!=SQLITE_OK ) break;
          if( aPut[i] ){
            rc = pDst->xPutChunk(pDst, &aFetch[i], apData[i], anData[i]);
          }
          if( rc==SQLITE_OK ){
            rc = syncEnqueueChildren(apData[i], anData[i], &queue, &seen);
          }
        }
        for(i=0; i<nFetch; i++) sqlite3_free(apData[i]);
      }else{
        for(i=0; i<nFetch && rc==SQLITE_OK; i++){
          u8 *data = 0;
          int nData = 0;
          rc = pSrc->xGetChunk(pSrc, &aFetch[i], &data, &nData);
          if( rc!=SQLITE_OK ) break;
          rc = syncVerifyFetchedChunk(&aFetch[i], data, nData);
          if( rc!=SQLITE_OK ){
            sqlite3_free(data);
            break;
          }
          if( aPut[i] ){
            rc = pDst->xPutChunk(pDst, &aFetch[i], data, nData);
          }
          if( rc==SQLITE_OK ){
            rc = syncEnqueueChildren(data, nData, &queue, &seen);
          }
          sqlite3_free(data);
        }
      }
    }
  }

  prollyHashSetFree(&seen);
  syncQueueFree(&queue);
  return rc;
}

/* Shared prefix: both FsRemote and LocalAsRemote start with the vtable
** base followed by a ChunkStore*, so the chunk/refs read ops below can
** reach the backing store through one cast regardless of family. */
typedef struct RemoteStoreHdr RemoteStoreHdr;
struct RemoteStoreHdr {
  DoltliteRemote base;
  ChunkStore *pStore;
};

typedef struct FsRemote FsRemote;
struct FsRemote {
  DoltliteRemote base;
  ChunkStore *pStore;
  ChunkStore store;
  int locked;
};

static int remoteGetChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                          u8 **ppData, int *pnData){
  return chunkStoreGet(((RemoteStoreHdr*)pRemote)->pStore, pHash, ppData, pnData);
}

static int remotePutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                          const u8 *pData, int nData){
  ChunkStore *pStore = ((RemoteStoreHdr*)pRemote)->pStore;
  ProllyHash computed;
  (void)pHash;
  return chunkStorePut(pStore, pData, nData, &computed);
}

static int remoteHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                           int nHash, u8 *aResult){
  ChunkStore *pStore = ((RemoteStoreHdr*)pRemote)->pStore;
  int i;
  for(i=0; i<nHash; i++){
    int has = 0;
    int rc = chunkStoreHas(pStore, &aHash[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    aResult[i] = has ? 1 : 0;
  }
  return SQLITE_OK;
}

static int remoteGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  ChunkStore *pStore = ((RemoteStoreHdr*)pRemote)->pStore;
  *ppData = 0;
  *pnData = 0;
  if( prollyHashIsEmpty(refsTableGetHash(&pStore->refs)) ){
    return SQLITE_NOTFOUND;
  }
  return chunkStoreGet(pStore, refsTableGetHash(&pStore->refs), ppData, pnData);
}

static int fsEnsureLocked(FsRemote *p){
  int rc;
  if( p->locked ) return SQLITE_OK;
  rc = chunkStoreLockAndRefresh(&p->store);
  if( rc==SQLITE_OK ) p->locked = 1;
  return rc;
}

static int fsPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                      const u8 *pData, int nData){
  FsRemote *p = (FsRemote*)pRemote;
  int rc = fsEnsureLocked(p);
  if( rc!=SQLITE_OK ) return rc;
  return remotePutChunk(pRemote, pHash, pData, nData);
}

static int fsGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  FsRemote *p = (FsRemote*)pRemote;
  int rc;
  if( p->locked ){
    return remoteGetRefs(pRemote, ppData, pnData);
  }
  rc = chunkStoreLockAndRefresh(&p->store);
  if( rc!=SQLITE_OK ) return rc;
  rc = remoteGetRefs(pRemote, ppData, pnData);
  chunkStoreUnlock(&p->store);
  return rc;
}

static int fsSetRefs(DoltliteRemote *pRemote, const char *zBranch, int bForce,
                     const u8 *pData, int nData){
  FsRemote *p = (FsRemote*)pRemote;
  int rc = doltliteValidateScopedRefsUpdate(&p->store, pData, nData,
                                            zBranch, bForce);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreInstallRefsBlob(&p->store, pData, nData);
}

static int fsSetRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pData,
  int nData
){
  FsRemote *p = (FsRemote*)pRemote;
  ProllyHash expected;
  int rc;
  if( pExpectedRefsHash ){
    memcpy(&expected, pExpectedRefsHash, sizeof(expected));
  }else{
    memset(&expected, 0, sizeof(expected));
  }
  rc = fsEnsureLocked(p);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(refsTableGetHash(&p->store.refs), &expected)!=0 ){
    chunkStoreRollback(&p->store);
    chunkStoreUnlock(&p->store);
    p->locked = 0;
    return SQLITE_BUSY;
  }
  rc = fsSetRefs(pRemote, zBranch, bForce, pData, nData);
  if( rc!=SQLITE_OK ){
    chunkStoreRollback(&p->store);
    chunkStoreUnlock(&p->store);
    p->locked = 0;
  }
  return rc;
}

static int fsCommit(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  int rc = doltliteRemotePersistRefs(&p->store);
  if( p->locked ){
    if( rc!=SQLITE_OK ) chunkStoreRollback(&p->store);
    chunkStoreUnlock(&p->store);
    p->locked = 0;
  }
  return rc;
}

static void fsClose(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  if( p->locked ){
    chunkStoreRollback(&p->store);
    chunkStoreUnlock(&p->store);
  }
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

  p->base.xGetChunk = remoteGetChunk;
  p->base.xPutChunk = fsPutChunk;
  p->base.xHasChunks = remoteHasChunks;
  p->base.xGetRefs = fsGetRefs;
  p->base.xSetRefs = fsSetRefs;
  p->base.xSetRefsIf = fsSetRefsIf;
  p->base.xCommit = fsCommit;
  p->base.xClose = fsClose;
  p->pStore = &p->store;

  rc = chunkStoreOpen(&p->store, pVfs, zPath, flags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(p);
    return 0;
  }

  return &p->base;
}

typedef struct LocalAsRemote LocalAsRemote;
struct LocalAsRemote {
  DoltliteRemote base;
  ChunkStore *pStore;
  int locked;
};

static int localEnsureLocked(LocalAsRemote *p){
  int rc;
  if( p->locked ) return SQLITE_OK;
  rc = chunkStoreLockAndRefresh(p->pStore);
  if( rc==SQLITE_OK ) rc = chunkStoreForceRefresh(p->pStore);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(p->pStore);
    return rc;
  }
  p->locked = 1;
  return SQLITE_OK;
}

static int localPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                         const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int rc = localEnsureLocked(p);
  if( rc!=SQLITE_OK ) return rc;
  return remotePutChunk(pRemote, pHash, pData, nData);
}

static int localSetRefs(DoltliteRemote *pRemote, const char *zBranch,
                        int bForce, const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int rc = doltliteValidateScopedRefsUpdate(p->pStore, pData, nData,
                                            zBranch, bForce);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreInstallRefsBlob(p->pStore, pData, nData);
}

static int localSetRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pData,
  int nData
){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  ProllyHash expected;
  if( pExpectedRefsHash ){
    memcpy(&expected, pExpectedRefsHash, sizeof(expected));
  }else{
    memset(&expected, 0, sizeof(expected));
  }
  if( prollyHashCompare(refsTableGetHash(&p->pStore->refs), &expected)!=0 ){
    return SQLITE_BUSY;
  }
  return localSetRefs(pRemote, zBranch, bForce, pData, nData);
}

static int localCommit(DoltliteRemote *pRemote){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int rc;
  if( !p->locked ) return SQLITE_OK;
  rc = chunkStoreCommit(p->pStore);
  if( rc!=SQLITE_OK ) chunkStoreRollback(p->pStore);
  chunkStoreUnlock(p->pStore);
  p->locked = 0;
  return rc;
}

static void localClose(DoltliteRemote *pRemote){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  if( p->locked ){
    chunkStoreRollback(p->pStore);
    chunkStoreUnlock(p->pStore);
  }
  sqlite3_free(p);
}

DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal){
  LocalAsRemote *p = sqlite3_malloc(sizeof(LocalAsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(LocalAsRemote));

  p->base.xGetChunk = remoteGetChunk;
  p->base.xPutChunk = localPutChunk;
  p->base.xHasChunks = remoteHasChunks;
  p->base.xGetRefs = remoteGetRefs;
  p->base.xSetRefs = localSetRefs;
  p->base.xSetRefsIf = localSetRefsIf;
  p->base.xCommit = localCommit;
  p->base.xClose = localClose;
  p->pStore = pLocal;

  return &p->base;
}

static int syncIsAncestor(
  ChunkStore *cs,
  const ProllyHash *pAncestor,
  const ProllyHash *pDescendant
){
  SyncQueue queue;
  ProllyHashSet visited;
  int found = 0;
  int rc;

  if( prollyHashCompare(pAncestor, pDescendant)==0 ) return 1;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return -1;
  rc = prollyHashSetInit(&visited, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    return -1;
  }

  rc = syncQueuePush(&queue, pDescendant);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&visited);
    syncQueueFree(&queue);
    return -1;
  }
  rc = prollyHashSetAdd(&visited, pDescendant);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&visited);
    syncQueueFree(&queue);
    return -1;
  }

  while( !found ){
    ProllyHash current;
    u8 *data = 0;
    int nData = 0;

    if( !syncQueuePop(&queue, &current) ) break;

    rc = chunkStoreGet(cs, &current, &data, &nData);
    if( rc!=SQLITE_OK ) break;

    if( doltliteClassifyChunk(data, nData) == CHUNK_COMMIT ){
      DoltliteCommit commit;
      memset(&commit, 0, sizeof(commit));
      if( doltliteCommitDeserialize(data, nData, &commit)==SQLITE_OK ){
        int pi;
        for(pi=0; pi<doltliteCommitParentCount(&commit); pi++){
          const ProllyHash *pParent = doltliteCommitParentHash(&commit, pi);
          if( !pParent || prollyHashIsEmpty(pParent) ) continue;
          if( prollyHashCompare(pParent, pAncestor)==0 ){
            found = 1;
            break;
          }
          if( !prollyHashSetContains(&visited, pParent) ){
            rc = prollyHashSetAdd(&visited, pParent);
            if( rc!=SQLITE_OK ){
              found = -1;
              break;
            }
            rc = syncQueuePush(&queue, pParent);
            if( rc!=SQLITE_OK ){
              found = -1;
              break;
            }
          }
        }
        doltliteCommitClear(&commit);
      }
    }
    sqlite3_free(data);
    if( found<0 ) break;
  }

  prollyHashSetFree(&visited);
  syncQueueFree(&queue);
  return found;
}

int doltliteValidateScopedRefsUpdate(
  ChunkStore *pStore,
  const u8 *pBlob,
  int nBlob,
  const char *zBranch,
  int bForce
){
  ChunkStore inc;
  const BranchRef *aCur = 0, *aInc = 0;
  const TagRef *aCurTag = 0, *aIncTag = 0;
  int nCur = 0, nInc = 0, nCurTag = 0, nIncTag = 0;
  const BranchRef *curB = 0, *incB = 0;
  int rc;
  int i, j;

  if( !zBranch || !zBranch[0] ) return SQLITE_MISUSE;
  memset(&inc, 0, sizeof(inc));
  rc = csDeserializeRefsIntoTemp(&inc, pBlob, nBlob);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&inc);
    return rc;
  }

  refsTableGetBranches(&pStore->refs, &nCur, &aCur);
  refsTableGetBranches(&inc.refs, &nInc, &aInc);
  refsTableGetTags(&pStore->refs, &nCurTag, &aCurTag);
  refsTableGetTags(&inc.refs, &nIncTag, &aIncTag);

  /* Every current branch other than the one being pushed must survive
  ** unchanged: same name present, same commit. Blocks rewrite and delete. */
  for(i=0; i<nCur; i++){
    if( strcmp(aCur[i].zName, zBranch)==0 ) continue;
    for(j=0; j<nInc; j++){
      if( strcmp(aInc[j].zName, aCur[i].zName)==0 ) break;
    }
    if( j>=nInc
     || prollyHashCompare(&aInc[j].commitHash, &aCur[i].commitHash)!=0 ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
  }

  /* The push may not introduce any branch other than the one declared. */
  for(j=0; j<nInc; j++){
    if( strcmp(aInc[j].zName, zBranch)==0 ) continue;
    for(i=0; i<nCur; i++){
      if( strcmp(aCur[i].zName, aInc[j].zName)==0 ) break;
    }
    if( i>=nCur ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
  }

  /* Tags are immutable over push: identical set, identical targets. */
  if( nCurTag!=nIncTag ){
    rc = SQLITE_CONSTRAINT;
    goto done;
  }
  for(i=0; i<nCurTag; i++){
    for(j=0; j<nIncTag; j++){
      if( strcmp(aCurTag[i].zName, aIncTag[j].zName)==0 ) break;
    }
    if( j>=nIncTag
     || prollyHashCompare(&aIncTag[j].commitHash, &aCurTag[i].commitHash)!=0 ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
  }

  /* The declared branch may be created freely, but an existing branch may only
  ** move as a fast-forward unless the push is forced. */
  for(i=0; i<nCur; i++){
    if( strcmp(aCur[i].zName, zBranch)==0 ){ curB = &aCur[i]; break; }
  }
  for(j=0; j<nInc; j++){
    if( strcmp(aInc[j].zName, zBranch)==0 ){ incB = &aInc[j]; break; }
  }
  if( !bForce && curB && incB
   && prollyHashCompare(&curB->commitHash, &incB->commitHash)!=0 ){
    int anc = syncIsAncestor(pStore, &curB->commitHash, &incB->commitHash);
    if( anc<0 ){ rc = SQLITE_NOMEM; goto done; }
    if( anc==0 ){ rc = SQLITE_CONSTRAINT; goto done; }
  }

  rc = SQLITE_OK;

done:
  csFreeRefsState(&inc);
  return rc;
}

static int remoteSequencesWouldAdvance(
  ChunkStore *pLocal,
  ChunkStore *pRemoteRefs,
  int *pWouldAdvance
){
  int iSeq;
  const SequenceRef *aRemSeq = 0;
  int nRemSeq = 0;

  *pWouldAdvance = 0;
  refsTableGetSequences(&pRemoteRefs->refs, &nRemSeq, &aRemSeq);
  for(iSeq=0; iSeq<nRemSeq; iSeq++){
    if( aRemSeq[iSeq].zTableName
     && aRemSeq[iSeq].iSeq >
        chunkStoreGetSequenceValue(pLocal, aRemSeq[iSeq].zTableName) ){
      *pWouldAdvance = 1;
      break;
    }
  }
  return SQLITE_OK;
}

int doltlitePush(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zBranch,
  int bForce
){
  ProllyHash localCommit;
  ProllyHash localCatalog;
  ProllyHash remoteCommit;
  ProllyHash expectedRefsHash;
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;

  memset(&expectedRefsHash, 0, sizeof(expectedRefsHash));
  rc = chunkStoreFindBranch(pLocal, zBranch, &localCommit);
  if( rc!=SQLITE_OK ){
    return SQLITE_ERROR;
  }

  rc = remoteLoadCommitCatalogHash(pLocal, &localCommit, &localCatalog);
  if( rc!=SQLITE_OK ) return rc;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc==SQLITE_OK && refsData ){
    prollyHashCompute(refsData, nRefsData, &expectedRefsHash);
  }else if( rc==SQLITE_NOTFOUND ){
    refsData = 0;
    nRefsData = 0;
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(refsData);
    return rc;
  }

  if( refsData ){
    rc = remoteFindBranchFromRefsBlob(refsData, nRefsData, zBranch, &remoteCommit);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(&remoteCommit) ){
      int cmp = prollyHashCompare(&remoteCommit, &localCommit);
      if( cmp==0 ){
        sqlite3_free(refsData);
        return SQLITE_OK;
      }else if( !bForce ){
        int isAnc = syncIsAncestor(pLocal, &remoteCommit, &localCommit);
        if( isAnc <= 0 ){
          sqlite3_free(refsData);
          return isAnc<0 ? SQLITE_NOMEM : SQLITE_CONSTRAINT;
        }
      }
    }
    if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK;
    }else if( rc!=SQLITE_OK ){
      sqlite3_free(refsData);
      return rc;
    }
  }
  sqlite3_free(refsData);
  refsData = 0;

  {
    DoltliteRemote *pLocalSrc = doltliteLocalAsRemote(pLocal);
    if( !pLocalSrc ) return SQLITE_NOMEM;
    rc = doltliteSyncChunks(pLocalSrc, pRemote, &localCommit, 1);
    pLocalSrc->xClose(pLocalSrc);
  }
  if( rc!=SQLITE_OK ) return rc;

  {
    u8 *refsData2 = 0; int nRefsData2 = 0;
    rc = pRemote->xGetRefs(pRemote, &refsData2, &nRefsData2);
    if( rc==SQLITE_NOTFOUND ){ refsData2 = 0; nRefsData2 = 0; rc = SQLITE_OK; }
    if( rc!=SQLITE_OK ) return rc;

    {
      ChunkStore tmpCs;
      u8 *newRefs = 0; int nNewRefs = 0;
      u8 *wsData = 0; int nWsData = 0;
      ProllyHash wsHash;
      memset(&tmpCs, 0, sizeof(tmpCs));
      if( refsData2 && nRefsData2 > 0 ){
        rc = chunkStoreLoadRefsFromBlob(&tmpCs, refsData2, nRefsData2);
      }else{
        /* A fresh target's default branch must name a branch it actually
        ** has -- the one being pushed. Inheriting the source's default
        ** (usually "main") left the target opening on a ref-less branch,
        ** the way a clone of a foo1-only remote lands on foo1. */
        chunkStoreSetDefaultBranch(&tmpCs, zBranch);
      }
      sqlite3_free(refsData2);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&tmpCs);
        return rc;
      }

      rc = chunkStoreUpdateBranch(&tmpCs, zBranch, &localCommit);
      if( rc==SQLITE_NOTFOUND ){
        rc = chunkStoreAddBranch(&tmpCs, zBranch, &localCommit);
      }
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&tmpCs);
        return rc;
      }

      rc = chunkStoreWriteBranchWorkingCatalog(
        &tmpCs, zBranch, &localCatalog, &localCommit);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&tmpCs);
        return rc;
      }
      rc = chunkStoreGetBranchWorkingSet(&tmpCs, zBranch, &wsHash);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&tmpCs);
        return rc;
      }
      rc = chunkStoreGet(&tmpCs, &wsHash, &wsData, &nWsData);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&tmpCs);
        return rc;
      }
      rc = pRemote->xPutChunk(pRemote, &wsHash, wsData, nWsData);
      sqlite3_free(wsData);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&tmpCs);
        return rc;
      }

      {
        int iSeq;
        const SequenceRef *aLocalSeq = 0;
        int nLocalSeq = 0;
        refsTableGetSequences(&pLocal->refs, &nLocalSeq, &aLocalSeq);
        for(iSeq=0; iSeq<nLocalSeq; iSeq++){
          if( aLocalSeq[iSeq].zTableName ){
            chunkStoreBumpSequence(&tmpCs, aLocalSeq[iSeq].zTableName,
                                   aLocalSeq[iSeq].iSeq);
          }
        }
      }

      rc = chunkStoreSerializeRefsToBlob(&tmpCs, &newRefs, &nNewRefs);
      chunkStoreClose(&tmpCs);
      if( rc!=SQLITE_OK ) return rc;

      if( pRemote->xSetRefsIf ){
        rc = pRemote->xSetRefsIf(pRemote, &expectedRefsHash, zBranch, bForce,
                                 newRefs, nNewRefs);
      }else{
        rc = pRemote->xSetRefs(pRemote, zBranch, bForce, newRefs, nNewRefs);
      }
      sqlite3_free(newRefs);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  doltliteTestCrashFinalize("push");
  rc = pRemote->xCommit(pRemote);

  return rc;
}

static int installFetchedRefs(
  ChunkStore *pLocal,
  ChunkStore *pRemoteRefs,
  const char *zRemoteName,
  const char *zBranch,
  const ProllyHash *pRemoteCommit
){
  ChunkStore nextRefs;
  SavedRefsState savedRefs;
  ProllyHash oldRefsHash;
  ProllyHash oldCommittedRefsHash;
  u8 *currentData = 0;
  u8 *nextData = 0;
  int nCurrentData = 0;
  int nNextData = 0;
  const SequenceRef *aRemSeq = 0;
  int nRemSeq = 0;
  int refsDetached = 0;
  int locked = 0;
  int i;
  int rc;

  memset(&nextRefs, 0, sizeof(nextRefs));
  memset(&savedRefs, 0, sizeof(savedRefs));

  rc = chunkStoreLockAndRefresh(pLocal);
  if( rc==SQLITE_OK ){
    locked = 1;
    rc = chunkStoreForceRefresh(pLocal);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(
        pLocal, &currentData, &nCurrentData);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreLoadRefsFromBlob(
        &nextRefs, currentData, nCurrentData);
  }
  if( rc==SQLITE_OK ){
    refsTableGetSequences(&pRemoteRefs->refs, &nRemSeq, &aRemSeq);
    for(i=0; i<nRemSeq && rc==SQLITE_OK; i++){
      if( aRemSeq[i].zTableName ){
        rc = chunkStoreBumpSequence(
            &nextRefs, aRemSeq[i].zTableName, aRemSeq[i].iSeq);
      }
    }
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreUpdateTracking(
        &nextRefs, zRemoteName, zBranch, pRemoteCommit);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(&nextRefs, &nextData, &nNextData);
  }
  if( rc==SQLITE_OK ){
    memcpy(&oldRefsHash, refsTableGetHash(&pLocal->refs),
           sizeof(oldRefsHash));
    memcpy(&oldCommittedRefsHash, &pLocal->refs.committedRefsHash,
           sizeof(oldCommittedRefsHash));
    csDetachSavedRefsState(pLocal, &savedRefs);
    refsDetached = 1;
    rc = chunkStoreInstallRefsBlob(pLocal, nextData, nNextData);
  }
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(pLocal);

  if( refsDetached ){
    if( rc==SQLITE_OK ){
      csFreeSavedRefsState(&savedRefs);
    }else{
      csFreeRefsState(pLocal);
      csRestoreSavedRefsState(pLocal, &savedRefs);
      memcpy(&pLocal->refs.refsHash, &oldRefsHash, sizeof(oldRefsHash));
      memcpy(&pLocal->refs.committedRefsHash, &oldCommittedRefsHash,
             sizeof(oldCommittedRefsHash));
    }
  }
  if( locked ) chunkStoreUnlock(pLocal);
  chunkStoreClose(&nextRefs);
  sqlite3_free(currentData);
  sqlite3_free(nextData);
  return rc;
}

int doltliteFetch(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zRemoteName,
  const char *zBranch
){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash remoteCommit;
  ProllyHash trackingCommit;
  DoltliteRemote *pLocalDst = 0;
  ChunkStore remoteRefs;
  int rc;

  memset(&remoteCommit, 0, sizeof(remoteCommit));
  memset(&trackingCommit, 0, sizeof(trackingCommit));
  memset(&remoteRefs, 0, sizeof(remoteRefs));

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreLoadRefsFromBlob(&remoteRefs, refsData, nRefsData);
  sqlite3_free(refsData);
  if( rc==SQLITE_OK ){
    rc = chunkStoreFindBranch(&remoteRefs, zBranch, &remoteCommit);
  }
  if( rc!=SQLITE_OK ){
    chunkStoreClose(&remoteRefs);
    return rc==SQLITE_NOTFOUND ? SQLITE_NOTFOUND : rc;
  }

  if( prollyHashIsEmpty(&remoteCommit) ){
    chunkStoreClose(&remoteRefs);
    return SQLITE_NOTFOUND;
  }

  rc = chunkStoreFindTracking(pLocal, zRemoteName, zBranch, &trackingCommit);
  if( rc==SQLITE_OK ){
    if( prollyHashCompare(&trackingCommit, &remoteCommit)==0 ){
      int seqWouldAdvance = 0;
      rc = remoteSequencesWouldAdvance(pLocal, &remoteRefs, &seqWouldAdvance);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&remoteRefs);
        return rc;
      }
      if( !seqWouldAdvance ){
        chunkStoreClose(&remoteRefs);
        return SQLITE_OK;
      }
    }
  }else if( rc!=SQLITE_NOTFOUND ){
    chunkStoreClose(&remoteRefs);
    return rc;
  }

  pLocalDst = doltliteLocalAsRemote(pLocal);
  if( !pLocalDst ){
    chunkStoreClose(&remoteRefs);
    return SQLITE_NOMEM;
  }

  rc = doltliteSyncChunks(pRemote, pLocalDst, &remoteCommit, 1);

  if( rc==SQLITE_OK ) rc = pLocalDst->xCommit(pLocalDst);
  pLocalDst->xClose(pLocalDst);
  if( rc==SQLITE_OK ){
    rc = installFetchedRefs(
        pLocal, &remoteRefs, zRemoteName, zBranch, &remoteCommit);
  }

  chunkStoreClose(&remoteRefs);
  return rc;
}

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash *aRoots = 0;
  int nRoots = 0;
  DoltliteRemote *pLocalDst = 0;
  ProllyHash oldRefsHash;
  ProllyHash oldCommittedRefsHash;
  SavedRefsState savedRefs;
  int refsDetached = 0;
  u8 oldRefsStale;
  int rc;

  memcpy(&oldRefsHash, refsTableGetHash(&pLocal->refs), sizeof(ProllyHash));
  memcpy(&oldCommittedRefsHash, &pLocal->refs.committedRefsHash,
         sizeof(ProllyHash));
  memset(&savedRefs, 0, sizeof(savedRefs));
  oldRefsStale = pLocal->bRefsStale;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = remoteCollectRootsFromRefsBlob(refsData, nRefsData, &aRoots, &nRoots);
  if( rc!=SQLITE_OK ){
    sqlite3_free(refsData);
    return rc;
  }

  if( nRoots == 0 ){

    sqlite3_free(aRoots);

  }else{

    pLocalDst = doltliteLocalAsRemote(pLocal);
    if( !pLocalDst ){
      sqlite3_free(aRoots);
      sqlite3_free(refsData);
      return SQLITE_NOMEM;
    }

    rc = doltliteSyncChunks(pRemote, pLocalDst, aRoots, nRoots);

    if( rc==SQLITE_OK ) rc = pLocalDst->xCommit(pLocalDst);
    pLocalDst->xClose(pLocalDst);
    sqlite3_free(aRoots);
    aRoots = 0;

    if( rc!=SQLITE_OK ){
      sqlite3_free(refsData);
      return rc;
    }
  }

  if( refsData && nRefsData > 0 ){
    csDetachSavedRefsState(pLocal, &savedRefs);
    refsDetached = 1;
    rc = chunkStoreInstallRefsBlob(pLocal, refsData, nRefsData);
  }
  sqlite3_free(refsData);
  refsData = 0;
  if( rc!=SQLITE_OK ) goto clone_restore_refs;

  rc = chunkStoreCommit(pLocal);
  if( rc!=SQLITE_OK ){
    goto clone_restore_refs;
  }

  if( refsDetached ) csFreeSavedRefsState(&savedRefs);
  return rc;

clone_restore_refs:
  sqlite3_free(refsData);
  if( refsDetached ){
    csFreeRefsState(pLocal);
    csRestoreSavedRefsState(pLocal, &savedRefs);
    memset(&savedRefs, 0, sizeof(savedRefs));
  }
  refsTableSetHash(&pLocal->refs, &oldRefsHash);
  memcpy(&pLocal->refs.committedRefsHash, &oldCommittedRefsHash,
         sizeof(ProllyHash));
  pLocal->bRefsStale = oldRefsStale;
  return rc;
}

#endif
