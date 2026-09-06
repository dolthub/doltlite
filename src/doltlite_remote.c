
#ifndef DOLTLITE_ENABLE_REMOTES
#define DOLTLITE_ENABLE_REMOTES 1
#endif

#if defined(DOLTLITE_PROLLY) && DOLTLITE_ENABLE_REMOTES

#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include "prolly_chunk_walk.h"
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

#define REMOTE_TAG_SCOPE_PREFIX "tag:"

static const char *remoteScopedTagName(const char *zRef){
  int nPrefix = (int)sizeof(REMOTE_TAG_SCOPE_PREFIX) - 1;
  if( !zRef || strncmp(zRef, REMOTE_TAG_SCOPE_PREFIX, nPrefix)!=0 ) return 0;
  return zRef[nPrefix] ? zRef + nPrefix : 0;
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

static int remoteValidateGraph(
  ChunkStore *pStore,
  const ProllyHash *aRoots,
  int nRoots
){
  SyncQueue queue;
  ProllyHashSet seen;
  ProllyHash hash;
  int rc;
  int i;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyHashSetInit(&seen, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    return rc;
  }
  for(i=0; i<nRoots && rc==SQLITE_OK; i++){
    if( !prollyHashIsEmpty(&aRoots[i])
     && !prollyHashSetContains(&seen, &aRoots[i]) ){
      rc = prollyHashSetAdd(&seen, &aRoots[i]);
      if( rc==SQLITE_OK ) rc = syncQueuePush(&queue, &aRoots[i]);
    }
  }
  while( rc==SQLITE_OK && syncQueuePop(&queue, &hash) ){
    u8 *pData = 0;
    int nData = 0;
    rc = chunkStoreGet(pStore, &hash, &pData, &nData);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_CORRUPT;
    if( rc==SQLITE_OK ){
      rc = syncEnqueueChildren(pData, nData, &queue, &seen);
    }
    sqlite3_free(pData);
  }
  prollyHashSetFree(&seen);
  syncQueueFree(&queue);
  return rc;
}

int doltliteValidateRefsTargetGraph(
  ChunkStore *pStore,
  const u8 *pBlob,
  int nBlob,
  const char *zRef
){
  ChunkStore refsView;
  ProllyHash aRoots[2];
  const BranchRef *aBranch;
  const TagRef *aTag;
  const char *zTag = remoteScopedTagName(zRef);
  int nBranch;
  int nTag;
  int rc;
  int i;

  rc = remoteLoadRefsView(pBlob, nBlob, &refsView);
  if( rc!=SQLITE_OK ) return rc;
  if( zTag ){
    refsTableGetTags(&refsView.refs, &nTag, &aTag);
    rc = SQLITE_NOTFOUND;
    for(i=0; i<nTag; i++){
      if( strcmp(aTag[i].zName, zTag)==0 ){
        rc = remoteValidateGraph(pStore, &aTag[i].commitHash, 1);
        break;
      }
    }
    chunkStoreClose(&refsView);
    return rc;
  }
  refsTableGetBranches(&refsView.refs, &nBranch, &aBranch);
  rc = SQLITE_NOTFOUND;
  for(i=0; i<nBranch; i++){
    if( strcmp(aBranch[i].zName, zRef)==0 ){
      aRoots[0] = aBranch[i].commitHash;
      aRoots[1] = aBranch[i].workingSetHash;
      rc = remoteValidateGraph(pStore, aRoots, 2);
      break;
    }
  }
  chunkStoreClose(&refsView);
  return rc;
}

#define SYNC_BATCH_SIZE 256

/* Fetched chunk must hash to the requested address, else wrong bytes are
** stored under their own hash and still walked by syncEnqueueChildren. */
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

    /* A resumed partial put may have persisted a parent before missing
    ** descendants; scan below present roots. */
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

/* FsRemote and LocalAsRemote both start with the vtable then ChunkStore*,
** so chunk/refs reads can use one cast. */
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
  int rc = syncVerifyFetchedChunk(pHash, pData, nData);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStorePut(pStore, pData, nData, 0);
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

static int fsLockAndForceRefresh(ChunkStore *cs){
  int rc = chunkStoreLockAndRefresh(cs);
  if( rc==SQLITE_OK ){
    rc = chunkStoreForceRefresh(cs);
    if( rc==SQLITE_CANTOPEN || rc==SQLITE_NOTADB ) rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK ) chunkStoreUnlock(cs);
  return rc;
}

static int fsEnsureLocked(FsRemote *p){
  int rc;
  if( p->locked ) return SQLITE_OK;
  rc = fsLockAndForceRefresh(&p->store);
  if( rc!=SQLITE_OK ) return rc;
  p->locked = 1;
  return SQLITE_OK;
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
  rc = fsLockAndForceRefresh(&p->store);
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
  if( rc==SQLITE_OK ){
    rc = doltliteValidateRefsTargetGraph(&p->store, pData, nData, zBranch);
  }
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

static int fsCheckRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pData,
  int nData
){
  FsRemote *p = (FsRemote*)pRemote;
  int rc = fsEnsureLocked(p);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(refsTableGetHash(&p->store.refs),
                        pExpectedRefsHash)!=0 ){
    rc = SQLITE_BUSY;
  }
  if( rc==SQLITE_OK ){
    rc = doltliteValidateScopedRefsUpdate(&p->store, pData, nData,
                                          zBranch, bForce);
  }
  chunkStoreUnlock(&p->store);
  p->locked = 0;
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

static DoltliteRemote *fsRemoteOpen(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int flags
){
  FsRemote *p;
  int rc;

  p = sqlite3_malloc(sizeof(FsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(FsRemote));

  p->base.xGetChunk = remoteGetChunk;
  p->base.xPutChunk = fsPutChunk;
  p->base.xHasChunks = remoteHasChunks;
  p->base.xGetRefs = fsGetRefs;
  p->base.xSetRefs = fsSetRefs;
  p->base.xSetRefsIf = fsSetRefsIf;
  p->base.xCheckRefsIf = fsCheckRefsIf;
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

DoltliteRemote *doltliteFsRemoteOpen(sqlite3_vfs *pVfs, const char *zPath){
  return fsRemoteOpen(pVfs, zPath,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
}

DoltliteRemote *doltliteRemoteOpenReadOnly(
  sqlite3_vfs *pVfs,
  const char *zUrl
){
  if( strncmp(zUrl, "file://", 7)==0 ){
    return fsRemoteOpen(pVfs, zUrl + 7,
                        SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  }
  if( strncmp(zUrl, "http://", 7)==0 || strncmp(zUrl, "https://", 8)==0 ){
    return doltliteHttpRemoteOpen(zUrl);
  }
  return 0;
}

ChunkStore *doltliteFsRemoteStoreForTest(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  return p ? p->pStore : 0;
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
  if( rc==SQLITE_OK ){
    rc = doltliteValidateRefsTargetGraph(p->pStore, pData, nData, zBranch);
  }
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

static int localCheckRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
  const char *zBranch,
  int bForce,
  const u8 *pData,
  int nData
){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int rc = localEnsureLocked(p);
  if( rc!=SQLITE_OK ) return rc;
  if( prollyHashCompare(refsTableGetHash(&p->pStore->refs),
                        pExpectedRefsHash)!=0 ){
    rc = SQLITE_BUSY;
  }
  if( rc==SQLITE_OK ){
    rc = doltliteValidateScopedRefsUpdate(p->pStore, pData, nData,
                                          zBranch, bForce);
  }
  chunkStoreUnlock(p->pStore);
  p->locked = 0;
  return rc;
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

static DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal){
  LocalAsRemote *p = sqlite3_malloc(sizeof(LocalAsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(LocalAsRemote));

  p->base.xGetChunk = remoteGetChunk;
  p->base.xPutChunk = localPutChunk;
  p->base.xHasChunks = remoteHasChunks;
  p->base.xGetRefs = remoteGetRefs;
  p->base.xSetRefs = localSetRefs;
  p->base.xSetRefsIf = localSetRefsIf;
  p->base.xCheckRefsIf = localCheckRefsIf;
  p->base.xCommit = localCommit;
  p->base.xClose = localClose;
  p->pStore = pLocal;

  return &p->base;
}

static int syncIsAncestor(
  ChunkStore *cs,
  const ProllyHash *pAncestor,
  const ProllyHash *pDescendant,
  int *pIsAncestor
){
  SyncQueue queue;
  ProllyHashSet visited;
  int rc;

  *pIsAncestor = 0;
  if( prollyHashCompare(pAncestor, pDescendant)==0 ){
    *pIsAncestor = 1;
    return SQLITE_OK;
  }

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;
  rc = prollyHashSetInit(&visited, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    return rc;
  }

  rc = syncQueuePush(&queue, pDescendant);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&visited);
    syncQueueFree(&queue);
    return rc;
  }
  rc = prollyHashSetAdd(&visited, pDescendant);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&visited);
    syncQueueFree(&queue);
    return rc;
  }

  while( rc==SQLITE_OK && !*pIsAncestor ){
    ProllyHash current;
    u8 *data = 0;
    int nData = 0;

    if( !syncQueuePop(&queue, &current) ) break;

    rc = chunkStoreGet(cs, &current, &data, &nData);
    if( rc==SQLITE_NOTFOUND && !cs->pChunkSource ) rc = SQLITE_OK;
    if( rc!=SQLITE_OK || !data ) break;

    if( doltliteClassifyChunk(data, nData) == CHUNK_COMMIT ){
      DoltliteCommit commit;
      memset(&commit, 0, sizeof(commit));
      rc = doltliteCommitDeserialize(data, nData, &commit);
      if( rc==SQLITE_OK ){
        int pi;
        for(pi=0; pi<doltliteCommitParentCount(&commit); pi++){
          const ProllyHash *pParent = doltliteCommitParentHash(&commit, pi);
          if( !pParent || prollyHashIsEmpty(pParent) ) continue;
          if( prollyHashCompare(pParent, pAncestor)==0 ){
            *pIsAncestor = 1;
            break;
          }
          if( !prollyHashSetContains(&visited, pParent) ){
            rc = prollyHashSetAdd(&visited, pParent);
            if( rc!=SQLITE_OK ) break;
            rc = syncQueuePush(&queue, pParent);
            if( rc!=SQLITE_OK ) break;
          }
        }
      }
      doltliteCommitClear(&commit);
    }
    sqlite3_free(data);
  }

  prollyHashSetFree(&visited);
  syncQueueFree(&queue);
  return rc;
}

static int scopedSameText(const char *zA, const char *zB){
  return strcmp(zA ? zA : "", zB ? zB : "")==0;
}

/* Unset default and explicit "main" compare equal (same serialize fallback). */
static const char *scopedDefaultBranch(const RefsTable *rt){
  return rt->zDefaultBranch ? rt->zDefaultBranch : "main";
}

/* Refs lookups use the first matching slot, so a duplicate is a shadow
** entry still carried by reserialization. Every named section must be a set. */
static int scopedNamesAreUnique(const void *aBase, int n, int stride){
  const char *p = (const char*)aBase;
  int i, j;
  for(i=0; i<n; i++){
    const char *zI = *(const char *const*)(p + (size_t)i*stride);
    if( !zI ) return 0;
    for(j=0; j<i; j++){
      const char *zJ = *(const char *const*)(p + (size_t)j*stride);
      if( zJ && strcmp(zJ, zI)==0 ) return 0;
    }
  }
  return 1;
}

/* Tracking entries are keyed by remote/branch, not the leading name. */
static int scopedTrackingIsUnique(const TrackingBranch *a, int n){
  int i, j;
  for(i=0; i<n; i++){
    if( !a[i].zRemote || !a[i].zBranch ) return 0;
    for(j=0; j<i; j++){
      if( scopedSameText(a[j].zRemote, a[i].zRemote)
       && scopedSameText(a[j].zBranch, a[i].zBranch) ){
        return 0;
      }
    }
  }
  return 1;
}

static int scopedTagsMatch(const TagRef *aCur, int nCur,
                           const TagRef *aInc, int nInc){
  int i, j;
  if( nCur!=nInc ) return 0;
  for(i=0; i<nCur; i++){
    for(j=0; j<nInc; j++){
      if( scopedSameText(aInc[j].zName, aCur[i].zName) ) break;
    }
    if( j>=nInc ) return 0;
    if( prollyHashCompare(&aInc[j].commitHash, &aCur[i].commitHash)!=0 ) return 0;
    if( aInc[j].timestamp!=aCur[i].timestamp ) return 0;
    if( !scopedSameText(aInc[j].zTagger, aCur[i].zTagger)
     || !scopedSameText(aInc[j].zEmail, aCur[i].zEmail)
     || !scopedSameText(aInc[j].zMessage, aCur[i].zMessage) ){
      return 0;
    }
  }
  return 1;
}

static int scopedBranchesMatch(const BranchRef *aCur, int nCur,
                               const BranchRef *aInc, int nInc){
  int i, j;
  if( nCur!=nInc ) return 0;
  for(i=0; i<nCur; i++){
    for(j=0; j<nInc; j++){
      if( scopedSameText(aInc[j].zName, aCur[i].zName) ) break;
    }
    if( j>=nInc
     || prollyHashCompare(&aInc[j].commitHash, &aCur[i].commitHash)!=0
     || prollyHashCompare(&aInc[j].workingSetHash,
                          &aCur[i].workingSetHash)!=0 ){
      return 0;
    }
  }
  return 1;
}

static int scopedRemotesMatch(const RemoteRef *aCur, int nCur,
                              const RemoteRef *aInc, int nInc){
  int i, j;
  if( nCur!=nInc ) return 0;
  for(i=0; i<nCur; i++){
    for(j=0; j<nInc; j++){
      if( scopedSameText(aInc[j].zName, aCur[i].zName) ) break;
    }
    if( j>=nInc || !scopedSameText(aInc[j].zUrl, aCur[i].zUrl) ) return 0;
  }
  return 1;
}

static int scopedTrackingMatch(const TrackingBranch *aCur, int nCur,
                               const TrackingBranch *aInc, int nInc){
  int i, j;
  if( nCur!=nInc ) return 0;
  for(i=0; i<nCur; i++){
    for(j=0; j<nInc; j++){
      if( scopedSameText(aInc[j].zRemote, aCur[i].zRemote)
       && scopedSameText(aInc[j].zBranch, aCur[i].zBranch) ){
        break;
      }
    }
    if( j>=nInc
     || prollyHashCompare(&aInc[j].commitHash, &aCur[i].commitHash)!=0 ){
      return 0;
    }
  }
  return 1;
}

/* Push may bump AUTOINCREMENT counters; regressing or dropping one would
** reuse row ids. */
static int scopedSequencesOnlyAdvance(const SequenceRef *aCur, int nCur,
                                      const SequenceRef *aInc, int nInc){
  int i, j;
  for(i=0; i<nCur; i++){
    for(j=0; j<nInc; j++){
      if( scopedSameText(aInc[j].zTableName, aCur[i].zTableName) ) break;
    }
    if( j>=nInc || aInc[j].iSeq < aCur[i].iSeq ) return 0;
  }
  return 1;
}

static int scopedSequencesMatch(const SequenceRef *aCur, int nCur,
                                const SequenceRef *aInc, int nInc){
  int i, j;
  if( nCur!=nInc ) return 0;
  for(i=0; i<nCur; i++){
    for(j=0; j<nInc; j++){
      if( scopedSameText(aInc[j].zTableName, aCur[i].zTableName) ) break;
    }
    if( j>=nInc || aInc[j].iSeq!=aCur[i].iSeq ) return 0;
  }
  return 1;
}

static int scopedCatalogIsEmpty(
  ChunkStore *pStore,
  const ProllyHash *pHash,
  int *pEmpty
){
  u8 *pData = 0;
  const u8 *pEntries = 0;
  int nData = 0;
  int version = 0;
  int nTables = 0;
  const u8 *pEnd;
  int rc;
  int i;

  *pEmpty = 1;
  if( prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  rc = chunkStoreGet(pStore, pHash, &pData, &nData);
  if( rc!=SQLITE_OK ) return rc;
  if( !catalogParseHeaderEx(pData, nData, &version, &nTables, &pEntries) ){
    sqlite3_free(pData);
    return SQLITE_CORRUPT;
  }
  pEnd = pData + nData;
  if( nTables==0 ){
    if( pEntries!=pEnd ){
      sqlite3_free(pData);
      return SQLITE_CORRUPT;
    }
    *pEmpty = 1;
  }else if( nTables==1 ){
    const u8 *q = pEntries;
    ProllyHash root;
    int nSkip;
    u32 iTable;
    int nFixed = version==CATALOG_FORMAT_V3 ?
                 CAT_ENTRY_FIXED_SIZE_V3 : CAT_ENTRY_FIXED_SIZE_V4;
    if( pEnd-q<nFixed ){
      sqlite3_free(pData);
      return SQLITE_CORRUPT;
    }
    iTable = (u32)q[0] | ((u32)q[1]<<8) | ((u32)q[2]<<16)
           | ((u32)q[3]<<24);
    memcpy(root.data, q + CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE,
           PROLLY_HASH_SIZE);
    if( version==CATALOG_FORMAT_V3 ){
      q += CAT_ENTRY_FIXED_SIZE_V3 - 2;
      nSkip = q[0] | (q[1]<<8);
      q += 2;
    }else{
      q += CAT_ENTRY_FIXED_SIZE_V4 - 6;
      nSkip = (q[0] | (q[1]<<8))
            + (q[2] | (q[3]<<8))
            + (q[4] | (q[5]<<8));
      q += 6;
    }
    if( nSkip>pEnd-q ){
      sqlite3_free(pData);
      return SQLITE_CORRUPT;
    }
    q += nSkip;
    *pEmpty = iTable==1 && prollyHashIsEmpty(&root) && q==pEnd;
  }else{
    *pEmpty = 0;
  }
  if( *pEmpty && version==CATALOG_FORMAT_V5 ){
    for(i=CAT_HEADER_SIZE_V3; i<CAT_HEADER_SIZE_V5; i++){
      if( pData[i]!=0 ){
        *pEmpty = 0;
        break;
      }
    }
  }
  sqlite3_free(pData);
  return SQLITE_OK;
}

static int scopedWorkingSetIsDirty(
  ChunkStore *pStore,
  const BranchRef *pBranch,
  int *pDirty
){
  ProllyHash workingCat, stagedCat, conflicts, violations;
  DoltliteCommit commit;
  u8 *pWs = 0;
  u8 *pCommit = 0;
  int nWs = 0;
  int nCommit = 0;
  int version;
  int rc;

  *pDirty = 0;
  if( prollyHashIsEmpty(&pBranch->workingSetHash) ) return SQLITE_OK;
  rc = chunkStoreGet(pStore, &pBranch->workingSetHash, &pWs, &nWs);
  if( rc==SQLITE_OK ) rc = chunkStoreValidateWorkingSetBlob(pWs, nWs);
  if( rc!=SQLITE_OK ) goto done;

  version = pWs[0];
  memcpy(workingCat.data, pWs + WS_WORKING_CAT_OFF, PROLLY_HASH_SIZE);
  memcpy(stagedCat.data, pWs + WS_STAGED_OFF, PROLLY_HASH_SIZE);
  memcpy(conflicts.data, pWs + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
  memset(&violations, 0, sizeof(violations));
  if( version==WS_FORMAT_VERSION_V4 ){
    memcpy(violations.data, pWs + WS_CONSTRAINT_VIOLATIONS_OFF_V4,
           PROLLY_HASH_SIZE);
  }else if( version==WS_FORMAT_VERSION_V5 ){
    memcpy(violations.data, pWs + WS_CONSTRAINT_VIOLATIONS_OFF,
           PROLLY_HASH_SIZE);
  }

  if( pWs[WS_MERGING_OFF]!=0
   || !prollyHashIsEmpty(&conflicts)
   || (version>=WS_FORMAT_VERSION_V3 && pWs[WS_REBASING_OFF]!=0)
   || !prollyHashIsEmpty(&violations) ){
    *pDirty = 1;
    goto done;
  }
  if( prollyHashIsEmpty(&pBranch->commitHash) ){
    int workingEmpty = 0;
    int stagedEmpty = 0;
    rc = scopedCatalogIsEmpty(pStore, &workingCat, &workingEmpty);
    if( rc==SQLITE_OK ){
      rc = scopedCatalogIsEmpty(pStore, &stagedCat, &stagedEmpty);
    }
    if( rc==SQLITE_OK ) *pDirty = !workingEmpty || !stagedEmpty;
    goto done;
  }

  rc = chunkStoreGet(pStore, &pBranch->commitHash, &pCommit, &nCommit);
  if( rc!=SQLITE_OK ) goto done;
  rc = doltliteCommitDeserialize(pCommit, nCommit, &commit);
  if( rc==SQLITE_OK ){
    *pDirty = (!prollyHashIsEmpty(&workingCat)
            && prollyHashCompare(&workingCat, &commit.catalogHash)!=0)
           || (!prollyHashIsEmpty(&stagedCat)
            && prollyHashCompare(&stagedCat, &commit.catalogHash)!=0);
    if( *pDirty && prollyHashIsEmpty(&commit.catalogHash) ){
      int workingEmpty = 0;
      int stagedEmpty = 0;
      if( !prollyHashIsEmpty(&workingCat) ){
        rc = scopedCatalogIsEmpty(pStore, &workingCat, &workingEmpty);
      }
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&stagedCat) ){
        rc = scopedCatalogIsEmpty(pStore, &stagedCat, &stagedEmpty);
      }
      if( rc==SQLITE_OK ){
        *pDirty = (!prollyHashIsEmpty(&workingCat)
                && prollyHashCompare(&workingCat, &commit.catalogHash)!=0
                && !workingEmpty)
               || (!prollyHashIsEmpty(&stagedCat)
                && prollyHashCompare(&stagedCat, &commit.catalogHash)!=0
                && !stagedEmpty);
      }
    }
    doltliteCommitClear(&commit);
  }

done:
  sqlite3_free(pWs);
  sqlite3_free(pCommit);
  return rc;
}

int doltliteValidateScopedRefsUpdate(
  ChunkStore *pStore,
  const u8 *pBlob,
  int nBlob,
  const char *zRef,
  int bForce
){
  ChunkStore inc;
  const BranchRef *aCur = 0, *aInc = 0;
  const TagRef *aCurTag = 0, *aIncTag = 0;
  const RemoteRef *aCurRem = 0, *aIncRem = 0;
  const TrackingBranch *aCurTrk = 0, *aIncTrk = 0;
  const SequenceRef *aCurSeq = 0, *aIncSeq = 0;
  const char *zTag = remoteScopedTagName(zRef);
  int nCur = 0, nInc = 0, nCurTag = 0, nIncTag = 0;
  int nCurRem = 0, nIncRem = 0, nCurTrk = 0, nIncTrk = 0;
  int nCurSeq = 0, nIncSeq = 0;
  const BranchRef *curB = 0, *incB = 0;
  int rc;
  int i, j;

  if( !zRef || !zRef[0] ) return SQLITE_MISUSE;
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
  refsTableGetRemotes(&pStore->refs, &nCurRem, &aCurRem);
  refsTableGetRemotes(&inc.refs, &nIncRem, &aIncRem);
  refsTableGetTracking(&pStore->refs, &nCurTrk, &aCurTrk);
  refsTableGetTracking(&inc.refs, &nIncTrk, &aIncTrk);
  refsTableGetSequences(&pStore->refs, &nCurSeq, &aCurSeq);
  refsTableGetSequences(&inc.refs, &nIncSeq, &aIncSeq);

  if( !scopedNamesAreUnique(aInc, nInc, (int)sizeof(BranchRef))
   || !scopedNamesAreUnique(aIncTag, nIncTag, (int)sizeof(TagRef))
   || !scopedNamesAreUnique(aIncRem, nIncRem, (int)sizeof(RemoteRef))
   || !scopedNamesAreUnique(aIncSeq, nIncSeq, (int)sizeof(SequenceRef))
   || !scopedTrackingIsUnique(aIncTrk, nIncTrk) ){
    rc = SQLITE_CONSTRAINT;
    goto done;
  }

  if( zTag ){
    const TagRef *incTag = 0;
    if( !scopedBranchesMatch(aCur, nCur, aInc, nInc)
     || !scopedRemotesMatch(aCurRem, nCurRem, aIncRem, nIncRem)
     || !scopedTrackingMatch(aCurTrk, nCurTrk, aIncTrk, nIncTrk)
     || !scopedSequencesMatch(aCurSeq, nCurSeq, aIncSeq, nIncSeq)
     || !scopedSameText(scopedDefaultBranch(&pStore->refs),
                        scopedDefaultBranch(&inc.refs)) ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
    for(i=0; i<nCurTag; i++){
      if( strcmp(aCurTag[i].zName, zTag)==0 ) continue;
      for(j=0; j<nIncTag; j++){
        if( strcmp(aIncTag[j].zName, aCurTag[i].zName)==0 ) break;
      }
      if( j>=nIncTag
       || !scopedTagsMatch(&aCurTag[i], 1, &aIncTag[j], 1) ){
        rc = SQLITE_CONSTRAINT;
        goto done;
      }
    }
    for(j=0; j<nIncTag; j++){
      if( strcmp(aIncTag[j].zName, zTag)==0 ){
        incTag = &aIncTag[j];
        continue;
      }
      for(i=0; i<nCurTag; i++){
        if( strcmp(aCurTag[i].zName, aIncTag[j].zName)==0 ) break;
      }
      if( i>=nCurTag ){
        rc = SQLITE_CONSTRAINT;
        goto done;
      }
    }
    rc = incTag ? SQLITE_OK : SQLITE_CONSTRAINT;
    goto done;
  }

  /* Every other current branch must survive unchanged (name, commit, working set). */
  for(i=0; i<nCur; i++){
    if( strcmp(aCur[i].zName, zRef)==0 ) continue;
    for(j=0; j<nInc; j++){
      if( strcmp(aInc[j].zName, aCur[i].zName)==0 ) break;
    }
    if( j>=nInc
     || prollyHashCompare(&aInc[j].commitHash, &aCur[i].commitHash)!=0
     || prollyHashCompare(&aInc[j].workingSetHash, &aCur[i].workingSetHash)!=0 ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
  }

  /* Push may not introduce any branch other than the declared one. */
  for(j=0; j<nInc; j++){
    if( strcmp(aInc[j].zName, zRef)==0 ) continue;
    for(i=0; i<nCur; i++){
      if( strcmp(aCur[i].zName, aInc[j].zName)==0 ) break;
    }
    if( i>=nCur ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
  }

  /* Tags, remotes, and tracking are immutable over push. */
  if( !scopedTagsMatch(aCurTag, nCurTag, aIncTag, nIncTag)
   || !scopedRemotesMatch(aCurRem, nCurRem, aIncRem, nIncRem)
   || !scopedTrackingMatch(aCurTrk, nCurTrk, aIncTrk, nIncTrk)
   || !scopedSequencesOnlyAdvance(aCurSeq, nCurSeq, aIncSeq, nIncSeq) ){
    rc = SQLITE_CONSTRAINT;
    goto done;
  }

  /* Push may not repoint the default branch (clone checkout / GET /root).
  ** An empty target may adopt the pushed branch. */
  if( !scopedSameText(scopedDefaultBranch(&inc.refs),
                      nCur==0 ? zRef : scopedDefaultBranch(&pStore->refs)) ){
    rc = SQLITE_CONSTRAINT;
    goto done;
  }

  /* Declared branch may be created; an existing one must fast-forward unless forced. */
  for(i=0; i<nCur; i++){
    if( strcmp(aCur[i].zName, zRef)==0 ){ curB = &aCur[i]; break; }
  }
  for(j=0; j<nInc; j++){
    if( strcmp(aInc[j].zName, zRef)==0 ){ incB = &aInc[j]; break; }
  }
  /* Push creates or advances the declared branch, never deletes it. */
  if( !incB ){
    rc = SQLITE_CONSTRAINT;
    goto done;
  }
  if( curB ){
    int dirty = 0;
    rc = scopedWorkingSetIsDirty(pStore, curB, &dirty);
    if( rc!=SQLITE_OK ) goto done;
    if( dirty ){
      rc = SQLITE_LOCKED;
      goto done;
    }
  }
  if( !bForce && curB && incB
   && prollyHashCompare(&curB->commitHash, &incB->commitHash)!=0 ){
    int anc = 0;
    rc = syncIsAncestor(
        pStore, &curB->commitHash, &incB->commitHash, &anc);
    if( rc!=SQLITE_OK ) goto done;
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

static int remoteTagTargetAvailable(
  ChunkStore *pLocal,
  const ProllyHash *pCommit,
  int *pAvailable
){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  *pAvailable = 0;
  rc = chunkStoreGet(pLocal, pCommit, &pData, &nData);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc==SQLITE_OK ){
    if( doltliteClassifyChunk(pData, nData)!=CHUNK_COMMIT ){
      rc = SQLITE_CORRUPT;
    }else{
      *pAvailable = 1;
    }
  }
  sqlite3_free(pData);
  return rc;
}

static int remoteTagsWouldInstall(
  ChunkStore *pLocal,
  ChunkStore *pRemoteRefs,
  int *pWouldInstall
){
  const TagRef *aLocalTag = 0;
  const TagRef *aRemoteTag = 0;
  int nLocalTag = 0;
  int nRemoteTag = 0;
  int i;
  int rc;

  *pWouldInstall = 0;
  refsTableGetTags(&pLocal->refs, &nLocalTag, &aLocalTag);
  refsTableGetTags(&pRemoteRefs->refs, &nRemoteTag, &aRemoteTag);
  for(i=0; i<nRemoteTag; i++){
    int available = 0;
    int j;
    for(j=0; j<nLocalTag; j++){
      if( strcmp(aLocalTag[j].zName, aRemoteTag[i].zName)==0 ) break;
    }
    if( j<nLocalTag
     && scopedTagsMatch(&aLocalTag[j], 1, &aRemoteTag[i], 1) ) continue;
    rc = remoteTagTargetAvailable(
        pLocal, &aRemoteTag[i].commitHash, &available);
    if( rc!=SQLITE_OK ) return rc;
    if( available ){
      *pWouldInstall = 1;
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
        rc = pRemote->xCheckRefsIf(
            pRemote, &expectedRefsHash, zBranch, bForce,
            refsData, nRefsData);
        sqlite3_free(refsData);
        return rc;
      }else if( !bForce ){
        int isAnc = 0;
        rc = syncIsAncestor(pLocal, &remoteCommit, &localCommit, &isAnc);
        if( rc!=SQLITE_OK || !isAnc ){
          sqlite3_free(refsData);
          return rc!=SQLITE_OK ? rc : SQLITE_CONSTRAINT;
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
      memset(&tmpCs, 0, sizeof(tmpCs));
      if( refsData2 && nRefsData2 > 0 ){
        rc = chunkStoreLoadRefsFromBlob(&tmpCs, refsData2, nRefsData2);
      }else{
        /* A fresh target's default must name a branch it has — the one
        ** being pushed. Inheriting "main" left clones on a missing ref. */
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

      /* Working sets do not push; clear any leftover working-set hash so
      ** cloners are not pointed at an unfetched chunk. */
      {
        ProllyHash emptyWs;
        memset(&emptyWs, 0, sizeof(emptyWs));
        rc = chunkStoreSetBranchWorkingSet(&tmpCs, zBranch, &emptyWs);
        if( rc!=SQLITE_OK ){
          chunkStoreClose(&tmpCs);
          return rc;
        }
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

int doltlitePushTag(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zTag
){
  const TagRef *aLocalTag = 0;
  const TagRef *pLocalTag = 0;
  ProllyHash expectedRefsHash;
  u8 *refsData = 0;
  int nRefsData = 0;
  int nLocalTag = 0;
  int rc;
  int i;

  refsTableGetTags(&pLocal->refs, &nLocalTag, &aLocalTag);
  for(i=0; i<nLocalTag; i++){
    if( strcmp(aLocalTag[i].zName, zTag)==0 ){
      pLocalTag = &aLocalTag[i];
      break;
    }
  }
  if( !pLocalTag ) return SQLITE_NOTFOUND;

  memset(&expectedRefsHash, 0, sizeof(expectedRefsHash));
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc==SQLITE_OK && refsData ){
    ChunkStore refsView;
    const TagRef *aRemoteTag = 0;
    int nRemoteTag = 0;
    prollyHashCompute(refsData, nRefsData, &expectedRefsHash);
    memset(&refsView, 0, sizeof(refsView));
    rc = chunkStoreLoadRefsFromBlob(&refsView, refsData, nRefsData);
    if( rc==SQLITE_OK ){
      refsTableGetTags(&refsView.refs, &nRemoteTag, &aRemoteTag);
      for(i=0; i<nRemoteTag; i++){
        if( strcmp(aRemoteTag[i].zName, zTag)==0
         && scopedTagsMatch(pLocalTag, 1, &aRemoteTag[i], 1) ){
          chunkStoreClose(&refsView);
          sqlite3_free(refsData);
          return SQLITE_OK;
        }
      }
    }
    chunkStoreClose(&refsView);
  }else if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  sqlite3_free(refsData);
  refsData = 0;
  if( rc!=SQLITE_OK ) return rc;

  {
    DoltliteRemote *pLocalSrc = doltliteLocalAsRemote(pLocal);
    ProllyHash tagCommit;
    if( !pLocalSrc ) return SQLITE_NOMEM;
    memcpy(&tagCommit, &pLocalTag->commitHash, sizeof(tagCommit));
    rc = doltliteSyncChunks(
        pLocalSrc, pRemote, &tagCommit, 1);
    pLocalSrc->xClose(pLocalSrc);
  }
  if( rc!=SQLITE_OK ) return rc;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc==SQLITE_NOTFOUND ){
    refsData = 0;
    nRefsData = 0;
    rc = SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    ChunkStore nextRefs;
    u8 *newRefs = 0;
    char *zScope = 0;
    int nNewRefs = 0;

    memset(&nextRefs, 0, sizeof(nextRefs));
    if( refsData && nRefsData>0 ){
      rc = chunkStoreLoadRefsFromBlob(&nextRefs, refsData, nRefsData);
    }
    if( rc==SQLITE_OK
     && chunkStoreFindTag(&nextRefs, zTag, 0)==SQLITE_OK ){
      rc = chunkStoreDeleteTag(&nextRefs, zTag);
    }
    if( rc==SQLITE_OK ){
      rc = chunkStoreAddTagFull(
          &nextRefs, zTag, &pLocalTag->commitHash,
          pLocalTag->zTagger, pLocalTag->zEmail,
          pLocalTag->timestamp, pLocalTag->zMessage);
    }
    if( rc==SQLITE_OK ){
      rc = chunkStoreSerializeRefsToBlob(&nextRefs, &newRefs, &nNewRefs);
    }
    if( rc==SQLITE_OK ){
      zScope = sqlite3_mprintf("%s%s", REMOTE_TAG_SCOPE_PREFIX, zTag);
      if( !zScope ) rc = SQLITE_NOMEM;
    }
    if( rc==SQLITE_OK ){
      if( pRemote->xSetRefsIf ){
        rc = pRemote->xSetRefsIf(
            pRemote, &expectedRefsHash, zScope, 1, newRefs, nNewRefs);
      }else{
        rc = pRemote->xSetRefs(pRemote, zScope, 1, newRefs, nNewRefs);
      }
    }
    sqlite3_free(zScope);
    sqlite3_free(newRefs);
    chunkStoreClose(&nextRefs);
  }
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ) return rc;

  doltliteTestCrashFinalize("push");
  return pRemote->xCommit(pRemote);
}

static int installFetchedRefs(
  ChunkStore *pLocal,
  ChunkStore *pRemoteRefs,
  const char *zRemoteName,
  const char *zBranch,
  const ProllyHash *pRemoteCommit,
  int bSkipGraphValidation
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
  const TagRef *aRemTag = 0;
  int nRemSeq = 0;
  int nRemTag = 0;
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
  if( rc==SQLITE_OK && bSkipGraphValidation
   && !chunkStoreOriginSourceEnabled(pLocal) ){
    bSkipGraphValidation = 0;
  }
  if( rc==SQLITE_OK && !bSkipGraphValidation ){
    /* Nothing roots synced chunks until this tracking ref lands. A gc in that
    ** window collects the fetch; installing then wedges later connections. */
    ProllyHash aRoots[1];
    memcpy(&aRoots[0], pRemoteCommit, sizeof(aRoots[0]));
    rc = remoteValidateGraph(pLocal, aRoots, 1);
    if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ) rc = SQLITE_BUSY_SNAPSHOT;
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
    refsTableGetTags(&pRemoteRefs->refs, &nRemTag, &aRemTag);
    for(i=0; i<nRemTag && rc==SQLITE_OK; i++){
      int available = 0;
      const TagRef *aLocalTag = 0;
      int nLocalTag = 0;
      int j;
      refsTableGetTags(&nextRefs.refs, &nLocalTag, &aLocalTag);
      for(j=0; j<nLocalTag; j++){
        if( strcmp(aLocalTag[j].zName, aRemTag[i].zName)==0 ) break;
      }
      if( j<nLocalTag
       && scopedTagsMatch(&aLocalTag[j], 1, &aRemTag[i], 1) ) continue;
      rc = remoteTagTargetAvailable(
          pLocal, &aRemTag[i].commitHash, &available);
      if( rc==SQLITE_OK && available ){
        if( j<nLocalTag ){
          rc = chunkStoreDeleteTag(&nextRefs, aRemTag[i].zName);
        }
      }
      if( rc==SQLITE_OK && available ){
        rc = chunkStoreAddTagFull(
            &nextRefs, aRemTag[i].zName, &aRemTag[i].commitHash,
            aRemTag[i].zTagger, aRemTag[i].zEmail,
            aRemTag[i].timestamp, aRemTag[i].zMessage);
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
  int bLazyOrigin;
  int rc;

  memset(&remoteCommit, 0, sizeof(remoteCommit));
  memset(&trackingCommit, 0, sizeof(trackingCommit));
  memset(&remoteRefs, 0, sizeof(remoteRefs));
  bLazyOrigin = chunkStoreOriginSourceEnabled(pLocal)
             && strcmp(zRemoteName, "origin")==0;

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
      int tagWouldInstall = 0;
      rc = remoteSequencesWouldAdvance(pLocal, &remoteRefs, &seqWouldAdvance);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&remoteRefs);
        return rc;
      }
      rc = remoteTagsWouldInstall(pLocal, &remoteRefs, &tagWouldInstall);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&remoteRefs);
        return rc;
      }
      if( !seqWouldAdvance && !tagWouldInstall ){
        chunkStoreClose(&remoteRefs);
        return SQLITE_OK;
      }
    }
  }else if( rc!=SQLITE_NOTFOUND ){
    chunkStoreClose(&remoteRefs);
    return rc;
  }

  if( !bLazyOrigin ){
    pLocalDst = doltliteLocalAsRemote(pLocal);
    if( !pLocalDst ){
      chunkStoreClose(&remoteRefs);
      return SQLITE_NOMEM;
    }

    rc = doltliteSyncChunks(pRemote, pLocalDst, &remoteCommit, 1);

    if( rc==SQLITE_OK ) rc = pLocalDst->xCommit(pLocalDst);
    pLocalDst->xClose(pLocalDst);
  }else{
    rc = SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    doltliteTestRunBeforeRefInstallHook();
    rc = installFetchedRefs(
        pLocal, &remoteRefs, zRemoteName, zBranch, &remoteCommit,
        bLazyOrigin);
  }

  chunkStoreClose(&remoteRefs);
  return rc;
}

static int remotePrepareCloneRefs(ChunkStore *pStore, const char *zUrl){
  const BranchRef *aBranch;
  int nBranch;
  int rc;
  int i;

  csFreeRemotes(pStore);
  csFreeTracking(pStore);
  rc = chunkStoreAddRemote(pStore, "origin", zUrl);
  refsTableGetBranches(&pStore->refs, &nBranch, &aBranch);
  for(i=0; i<nBranch && rc==SQLITE_OK; i++){
    rc = chunkStoreUpdateTracking(
        pStore, "origin", aBranch[i].zName, &aBranch[i].commitHash);
  }
  return rc;
}

int doltliteCloneLazy(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zUrl
){
  ChunkStore refsView;
  ChunkStoreRefsSnapshot snapshot;
  const BranchRef *aBranch = 0;
  ProllyHash defaultCommit;
  ProllyHash emptyWs;
  u8 *pRemoteRefs = 0;
  u8 *pLocalRefs = 0;
  int nRemoteRefs = 0;
  int nLocalRefs = 0;
  int nBranch = 0;
  int haveSnapshot = 0;
  int locked = 0;
  int rc;
  int i;

  memset(&refsView, 0, sizeof(refsView));
  memset(&snapshot, 0, sizeof(snapshot));
  memset(&defaultCommit, 0, sizeof(defaultCommit));
  memset(&emptyWs, 0, sizeof(emptyWs));
  if( !zUrl ) return SQLITE_MISUSE;

  rc = pRemote->xGetRefs(pRemote, &pRemoteRefs, &nRemoteRefs);
  if( rc==SQLITE_OK ){
    rc = chunkStoreLoadRefsFromBlob(&refsView, pRemoteRefs, nRemoteRefs);
  }
  sqlite3_free(pRemoteRefs);
  if( rc!=SQLITE_OK ) goto lazy_clone_done;

  refsTableGetBranches(&refsView.refs, &nBranch, &aBranch);
  if( nBranch>0 ){
    rc = chunkStoreFindBranch(
        &refsView, chunkStoreGetDefaultBranch(&refsView), &defaultCommit);
    if( rc!=SQLITE_OK || prollyHashIsEmpty(&defaultCommit) ){
      rc = SQLITE_ERROR;
      goto lazy_clone_done;
    }
  }

  for(i=0; i<nBranch && rc==SQLITE_OK; i++){
    rc = chunkStoreSetBranchWorkingSet(
        &refsView, aBranch[i].zName, &emptyWs);
  }
  if( rc==SQLITE_OK ) rc = remotePrepareCloneRefs(&refsView, zUrl);
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(
        &refsView, &pLocalRefs, &nLocalRefs);
  }
  if( rc!=SQLITE_OK ) goto lazy_clone_done;

  rc = chunkStoreLockAndRefresh(pLocal);
  if( rc==SQLITE_OK ){
    locked = 1;
    rc = chunkStoreForceRefresh(pLocal);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSnapshotRefs(pLocal, &snapshot);
    if( rc==SQLITE_OK ) haveSnapshot = 1;
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreInstallRefsBlob(pLocal, pLocalRefs, nLocalRefs);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreCommit(pLocal);
  }
  if( haveSnapshot ){
    if( rc==SQLITE_OK ){
      chunkStoreDiscardRefsSnapshot(&snapshot);
    }else{
      chunkStoreRollback(pLocal);
      chunkStoreRestoreRefsSnapshot(pLocal, &snapshot);
    }
  }
  if( locked ) chunkStoreUnlock(pLocal);

lazy_clone_done:
  chunkStoreClose(&refsView);
  sqlite3_free(pLocalRefs);
  return rc;
}

int doltliteClone(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zUrl
){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash *aRoots = 0;
  int nRoots = 0;
  DoltliteRemote *pLocalDst = 0;
  ProllyHash oldRefsHash;
  ProllyHash oldCommittedRefsHash;
  SavedRefsState savedRefs;
  int refsDetached = 0;
  int locked = 0;
  u8 oldRefsStale;
  int rc;

  memcpy(&oldRefsHash, refsTableGetHash(&pLocal->refs), sizeof(ProllyHash));
  memcpy(&oldCommittedRefsHash, &pLocal->refs.committedRefsHash,
         sizeof(ProllyHash));
  memset(&savedRefs, 0, sizeof(savedRefs));
  oldRefsStale = pLocal->bRefsStale;
  if( !zUrl ) return SQLITE_MISUSE;

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = remoteCollectRootsFromRefsBlob(refsData, nRefsData, &aRoots, &nRoots);
  if( rc!=SQLITE_OK ){
    sqlite3_free(refsData);
    return rc;
  }

  if( nRoots>0 ){
    pLocalDst = doltliteLocalAsRemote(pLocal);
    if( !pLocalDst ){
      sqlite3_free(aRoots);
      sqlite3_free(refsData);
      return SQLITE_NOMEM;
    }

    rc = doltliteSyncChunks(pRemote, pLocalDst, aRoots, nRoots);

    if( rc==SQLITE_OK ) rc = pLocalDst->xCommit(pLocalDst);
    pLocalDst->xClose(pLocalDst);
    /* Synced chunks are unrooted until the refs blob lands. The hook lets a
    ** test drive a sweep in that window. */
    if( rc==SQLITE_OK ) doltliteTestRunBeforeRefInstallHook();
    if( rc!=SQLITE_OK ){
      sqlite3_free(aRoots);
      sqlite3_free(refsData);
      return rc;
    }
  }

  /* Hold the store lock across validate+install+commit. Re-walk roots under
  ** the gc lock; BUSY_SNAPSHOT so a retry re-syncs. */
  rc = chunkStoreLockAndRefresh(pLocal);
  if( rc==SQLITE_OK ){
    locked = 1;
    rc = chunkStoreForceRefresh(pLocal);
  }
  if( rc==SQLITE_OK && nRoots>0 ){
    rc = remoteValidateGraph(pLocal, aRoots, nRoots);
    if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ) rc = SQLITE_BUSY_SNAPSHOT;
  }
  sqlite3_free(aRoots);
  aRoots = 0;
  if( rc!=SQLITE_OK ) goto clone_restore_refs;

  if( refsData && nRefsData > 0 ){
    csDetachSavedRefsState(pLocal, &savedRefs);
    refsDetached = 1;
    rc = chunkStoreInstallRefsBlob(pLocal, refsData, nRefsData);
  }
  sqlite3_free(refsData);
  refsData = 0;
  if( rc!=SQLITE_OK ) goto clone_restore_refs;

  /* Cloned branches start clean; clear working-set hashes in installed refs
  ** (the commit-graph sync never fetches those chunks). */
  {
    int nBr = 0;
    int i;
    const BranchRef *aBr = 0;
    ProllyHash emptyWs;
    memset(&emptyWs, 0, sizeof(emptyWs));
    refsTableGetBranches(&pLocal->refs, &nBr, &aBr);
    for(i=0; i<nBr && rc==SQLITE_OK; i++){
      if( !prollyHashIsEmpty(&aBr[i].workingSetHash) ){
        rc = chunkStoreSetBranchWorkingSet(pLocal, aBr[i].zName, &emptyWs);
      }
    }
    if( rc==SQLITE_OK ) rc = remotePrepareCloneRefs(pLocal, zUrl);
    if( rc==SQLITE_OK ){
      rc = chunkStoreSerializeRefs(pLocal);
    }
    if( rc!=SQLITE_OK ) goto clone_restore_refs;
  }

  rc = chunkStoreCommit(pLocal);
  if( rc!=SQLITE_OK ){
    goto clone_restore_refs;
  }

  if( refsDetached ) csFreeSavedRefsState(&savedRefs);
  if( locked ) chunkStoreUnlock(pLocal);
  return rc;

clone_restore_refs:
  sqlite3_free(refsData);
  sqlite3_free(aRoots);
  if( refsDetached ){
    csFreeRefsState(pLocal);
    csRestoreSavedRefsState(pLocal, &savedRefs);
    memset(&savedRefs, 0, sizeof(savedRefs));
  }
  refsTableSetHash(&pLocal->refs, &oldRefsHash);
  memcpy(&pLocal->refs.committedRefsHash, &oldCommittedRefsHash,
         sizeof(ProllyHash));
  pLocal->bRefsStale = oldRefsStale;
  if( locked ) chunkStoreUnlock(pLocal);
  return rc;
}

#endif
