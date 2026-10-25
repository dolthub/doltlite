
#ifndef DOLTLITE_ENABLE_REMOTES
#define DOLTLITE_ENABLE_REMOTES 1
#endif

#if defined(DOLTLITE_PROLLY) && DOLTLITE_ENABLE_REMOTES

#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "record_codec.h"
#include "prolly_chunk_walk.h"
#include <stdlib.h>
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

static int remoteValidateGraphEx(
  ChunkStore *pStore,
  const ProllyHash *aRoots,
  int nRoots,
  ProllyHashSet *pTrusted,
  int *pbMissing
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
    if( pTrusted && prollyHashSetContains(pTrusted, &hash) ) continue;
    rc = chunkStoreGet(pStore, &hash, &pData, &nData);
    if( rc==SQLITE_NOTFOUND ){
      if( pbMissing ) *pbMissing = 1;
      rc = SQLITE_CORRUPT;
    }
    if( rc==SQLITE_OK ){
      rc = syncEnqueueChildren(pData, nData, &queue, &seen);
    }
    sqlite3_free(pData);
  }
  prollyHashSetFree(&seen);
  syncQueueFree(&queue);
  return rc;
}

static int remoteValidateGraph(
  ChunkStore *pStore,
  const ProllyHash *aRoots,
  int nRoots
){
  return remoteValidateGraphEx(pStore, aRoots, nRoots, 0, 0);
}

/* Hashes whose closure the store already holds complete: every chunk under
** the anchors' trees, without their commit history. Every installed ref was
** validated or built locally, and gc keeps what refs reach, so a transferred
** graph only needs reading where it differs. Leaves are named by their
** level-1 parents, so the build reads about one chunk in a node's fanout. */
static int remoteBuildTrustedSet(
  ChunkStore *pStore,
  const ProllyHash *aAnchors,
  int nAnchors,
  ProllyHashSet *pTrusted
){
  SyncQueue queue;
  ProllyHash hash;
  int rc;
  int i;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<nAnchors && rc==SQLITE_OK; i++){
    if( !prollyHashIsEmpty(&aAnchors[i])
     && !prollyHashSetContains(pTrusted, &aAnchors[i]) ){
      rc = prollyHashSetAdd(pTrusted, &aAnchors[i]);
      if( rc==SQLITE_OK ) rc = syncQueuePush(&queue, &aAnchors[i]);
    }
  }
  while( rc==SQLITE_OK && syncQueuePop(&queue, &hash) ){
    u8 *pData = 0;
    int nData = 0;
    rc = chunkStoreGet(pStore, &hash, &pData, &nData);
    if( rc!=SQLITE_OK ) break;
    switch( doltliteClassifyChunk(pData, nData) ){
      case CHUNK_PROLLY_NODE: {
        ProllyNode node;
        int j;
        rc = prollyNodeParse(&node, pData, nData);
        for(j=0; rc==SQLITE_OK && node.level>0 && j<(int)node.nItems; j++){
          ProllyHash child;
          prollyNodeChildHash(&node, j, &child);
          if( prollyHashSetContains(pTrusted, &child) ) continue;
          rc = prollyHashSetAdd(pTrusted, &child);
          if( rc==SQLITE_OK && node.level>1 ) rc = syncQueuePush(&queue, &child);
        }
        break;
      }
      case CHUNK_COMMIT: {
        DoltliteCommit commit;
        memset(&commit, 0, sizeof(commit));
        rc = doltliteCommitDeserialize(pData, nData, &commit);
        if( rc==SQLITE_OK
         && !prollyHashIsEmpty(&commit.catalogHash)
         && !prollyHashSetContains(pTrusted, &commit.catalogHash) ){
          rc = prollyHashSetAdd(pTrusted, &commit.catalogHash);
          if( rc==SQLITE_OK ) rc = syncQueuePush(&queue, &commit.catalogHash);
        }
        doltliteCommitClear(&commit);
        break;
      }
      default:
        rc = syncEnqueueChildren(pData, nData, &queue, pTrusted);
        break;
    }
    sqlite3_free(pData);
  }
  syncQueueFree(&queue);
  return rc;
}

static int remoteValidateGraphWithAnchors(
  ChunkStore *pStore,
  const ProllyHash *aRoots,
  int nRoots,
  const ProllyHash *aAnchors,
  int nAnchors,
  int *pbMissing
){
  ProllyHashSet trusted;
  int bTrusted = 0;
  int rc;

  rc = prollyHashSetInit(&trusted, 1024);
  if( rc!=SQLITE_OK ) return rc;
  if( nAnchors>0 ){
    rc = remoteBuildTrustedSet(pStore, aAnchors, nAnchors, &trusted);
    /* An anchor that cannot be read proves nothing; walk everything. */
    if( rc==SQLITE_OK ) bTrusted = 1;
    else if( rc!=SQLITE_NOMEM ) rc = SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    rc = remoteValidateGraphEx(pStore, aRoots, nRoots,
                               bTrusted ? &trusted : 0, pbMissing);
  }
  prollyHashSetFree(&trusted);
  return rc;
}

static int remoteValidateGraphTrusted(
  ChunkStore *pStore,
  const ProllyHash *aRoots,
  int nRoots,
  const char *zBranch,
  int *pbMissing
){
  ProllyHash aAnchors[4];
  const char *zDefault = chunkStoreGetDefaultBranch(pStore);
  int nAnchors = 0;

  memset(aAnchors, 0, sizeof(aAnchors));
  if( zBranch
   && chunkStoreFindBranch(pStore, zBranch, &aAnchors[nAnchors])==SQLITE_OK ){
    nAnchors++;
    if( chunkStoreGetBranchWorkingSet(pStore, zBranch,
                                      &aAnchors[nAnchors])==SQLITE_OK ){
      nAnchors++;
    }
  }
  if( zDefault
   && chunkStoreFindBranch(pStore, zDefault, &aAnchors[nAnchors])==SQLITE_OK ){
    nAnchors++;
  }
  return remoteValidateGraphWithAnchors(
      pStore, aRoots, nRoots, aAnchors, nAnchors, pbMissing);
}

static int remoteValidateFetchedGraph(
  ChunkStore *pStore,
  const ProllyHash *pCommit,
  const char *zRemoteName,
  const char *zBranch
){
  const BranchRef *aBranch;
  ProllyHash *aAnchors;
  int nBranch;
  int nAnchors = 0;
  int rc;
  int i;

  refsTableGetBranches(&pStore->refs, &nBranch, &aBranch);
  aAnchors = sqlite3_malloc64(((sqlite3_uint64)nBranch+1)*sizeof(*aAnchors));
  if( !aAnchors ) return SQLITE_NOMEM;
  for(i=0; i<nBranch; i++) aAnchors[nAnchors++] = aBranch[i].commitHash;
  rc = chunkStoreFindTracking(pStore, zRemoteName, zBranch, &aAnchors[nAnchors]);
  if( rc==SQLITE_OK ) nAnchors++;
  if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
  if( rc==SQLITE_OK ){
    rc = remoteValidateGraphWithAnchors(
        pStore, pCommit, 1, aAnchors, nAnchors, 0);
  }
  sqlite3_free(aAnchors);
  return rc;
}

/* *pbMissing: the graph is incomplete rather than unreadable, as when a
** push was interrupted or a gc swept chunks not yet referenced. */
int doltliteValidateRefsTargetGraph(
  ChunkStore *pStore,
  const u8 *pBlob,
  int nBlob,
  const char *zRef,
  int *pbMissing
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
  if( zRef && zRef[0]==':' ){
    chunkStoreClose(&refsView);
    return SQLITE_OK;
  }
  if( zTag ){
    refsTableGetTags(&refsView.refs, &nTag, &aTag);
    rc = SQLITE_NOTFOUND;
    for(i=0; i<nTag; i++){
      if( strcmp(aTag[i].zName, zTag)==0 ){
        rc = remoteValidateGraphTrusted(pStore, &aTag[i].commitHash, 1, 0,
                                        pbMissing);
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
      rc = remoteValidateGraphTrusted(pStore, aRoots, 2, zRef, pbMissing);
      break;
    }
  }
  chunkStoreClose(&refsView);
  return rc;
}

#define SYNC_BATCH_SIZE 4096

typedef struct SyncBatchBuf SyncBatchBuf;
struct SyncBatchBuf {
  ProllyHash aBatch[SYNC_BATCH_SIZE];
  ProllyHash aFetch[SYNC_BATCH_SIZE];
  u8 aPresent[SYNC_BATCH_SIZE];
  u8 aPut[SYNC_BATCH_SIZE];
  u8 *apData[SYNC_BATCH_SIZE];
  int anData[SYNC_BATCH_SIZE];
};

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

/* A ref is published only after its whole graph is uploaded, so a root
** the destination already names as a branch or tag target needs no
** partial-put rescan. */
static int syncRootIsPublished(const ChunkStore *pDstRefs,
                               const ProllyHash *pRoot){
  const BranchRef *aBranch;
  const TagRef *aTag;
  int nBranch;
  int nTag;
  int i;
  if( !pDstRefs ) return 0;
  refsTableGetBranches(&pDstRefs->refs, &nBranch, &aBranch);
  for(i=0; i<nBranch; i++){
    if( prollyHashCompare(&aBranch[i].commitHash, pRoot)==0 ) return 1;
  }
  refsTableGetTags(&pDstRefs->refs, &nTag, &aTag);
  for(i=0; i<nTag; i++){
    if( prollyHashCompare(&aTag[i].commitHash, pRoot)==0 ) return 1;
  }
  return 0;
}

/* pDstRefs, when set, holds the destination's refs: roots that are
** already published ref targets skip the partial-put resume scan. */
static int syncChunks(
  DoltliteRemote *pSrc,
  DoltliteRemote *pDst,
  ProllyHash *aRoots,
  int nRoots,
  const ChunkStore *pDstRefs
){
  SyncQueue queue;
  ProllyHashSet seen;
  SyncBatchBuf *pB;
  int bFirstBatch = 1;
  int bResumeScan = 0;
  int rc, i;

  pB = sqlite3_malloc64(sizeof(*pB));
  if( !pB ) return SQLITE_NOMEM;

  rc = syncQueueInit(&queue);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pB);
    return rc;
  }

  rc = prollyHashSetInit(&seen, 256);
  if( rc!=SQLITE_OK ){
    syncQueueFree(&queue);
    sqlite3_free(pB);
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

    while( nBatch < SYNC_BATCH_SIZE
        && syncQueuePop(&queue, &pB->aBatch[nBatch]) ){
      nBatch++;
    }
    if( nBatch == 0 ) break;

    rc = pDst->xHasChunks(pDst, pB->aBatch, nBatch, pB->aPresent);
    if( rc!=SQLITE_OK ) break;
    if( bFirstBatch && pDst->bForceResumeScan ){
      bResumeScan = 1;
      pDst->bForceResumeScan = 0;
    }else if( bFirstBatch && pDst->bResumePartialPuts ){
      int bUnpublished = 0;
      bResumeScan = 1;
      for(i=0; i<nBatch; i++){
        if( !pB->aPresent[i] ){
          bResumeScan = 0;
          break;
        }
        if( !syncRootIsPublished(pDstRefs, &pB->aBatch[i]) ) bUnpublished = 1;
      }
      if( !bUnpublished ) bResumeScan = 0;
    }
    bFirstBatch = 0;

    /* A resumed partial put may have persisted a parent before missing
    ** descendants; scan below present roots. */
    {
      int nFetch = 0;

      for(i=0; i<nBatch; i++){
        if( !pB->aPresent[i] || bResumeScan ){
          pB->aFetch[nFetch] = pB->aBatch[i];
          pB->aPut[nFetch] = !pB->aPresent[i];
          nFetch++;
        }
      }
      if( nFetch==0 ) continue;

      if( pSrc->xGetChunks ){
        memset(pB->apData, 0, sizeof(pB->apData[0]) * nFetch);
        rc = pSrc->xGetChunks(pSrc, pB->aFetch, nFetch, pB->apData, pB->anData);
        for(i=0; i<nFetch && rc==SQLITE_OK; i++){
          if( !pB->apData[i] ){ rc = SQLITE_NOTFOUND; break; }
          rc = syncVerifyFetchedChunk(&pB->aFetch[i], pB->apData[i], pB->anData[i]);
          if( rc!=SQLITE_OK ) break;
          if( pB->aPut[i] ){
            rc = pDst->xPutChunk(pDst, &pB->aFetch[i],
                                 pB->apData[i], pB->anData[i]);
          }
          if( rc==SQLITE_OK ){
            rc = syncEnqueueChildren(pB->apData[i], pB->anData[i],
                                     &queue, &seen);
          }
        }
        for(i=0; i<nFetch; i++) sqlite3_free(pB->apData[i]);
      }else{
        for(i=0; i<nFetch && rc==SQLITE_OK; i++){
          u8 *data = 0;
          int nData = 0;
          rc = pSrc->xGetChunk(pSrc, &pB->aFetch[i], &data, &nData);
          if( rc!=SQLITE_OK ) break;
          rc = syncVerifyFetchedChunk(&pB->aFetch[i], data, nData);
          if( rc!=SQLITE_OK ){
            sqlite3_free(data);
            break;
          }
          if( pB->aPut[i] ){
            rc = pDst->xPutChunk(pDst, &pB->aFetch[i], data, nData);
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
  sqlite3_free(pB);
  return rc;
}

int doltliteSyncChunks(
  DoltliteRemote *pSrc,
  DoltliteRemote *pDst,
  ProllyHash *aRoots,
  int nRoots
){
  return syncChunks(pSrc, pDst, aRoots, nRoots, 0);
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

static int remoteGetChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                           int nHash, u8 **apData, int *anData){
  ChunkStore *pStore = ((RemoteStoreHdr*)pRemote)->pStore;
  int i = 0;
  int iPrefetched = 0;
  int rc = SQLITE_OK;
  if( nHash<0 ) return SQLITE_MISUSE;
  if( nHash==0 ) return SQLITE_OK;
  memset(apData, 0, nHash*sizeof(*apData));
  memset(anData, 0, nHash*sizeof(*anData));
  while( i<nHash && rc==SQLITE_OK ){
    int nRead;
    /* A lazy store faults each absent chunk with its own origin request;
    ** fetch this batch's absent chunks in one batched request first. */
    if( pStore->pChunkSource && i>=iPrefetched ){
      int nPrefetch = MIN(nHash-i, CHUNK_READ_BATCH_MAX);
      rc = chunkStoreSourcePrefetchMany(pStore, &aHash[i], nPrefetch);
      if( rc!=SQLITE_OK ) break;
      iPrefetched = i + nPrefetch;
    }
    rc = chunkStoreReadBatch(pStore, &aHash[i],
                             MIN(nHash-i, CHUNK_READ_BATCH_MAX),
                             &apData[i], &anData[i], &nRead);
    i += nRead;
  }
  return rc;
}

static int remotePutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                          const u8 *pData, int nData){
  ChunkStore *pStore = ((RemoteStoreHdr*)pRemote)->pStore;
  if( !pData || nData<0 ) return SQLITE_CORRUPT;
  return chunkStorePutChecked(pStore, pHash, pData, nData);
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

void doltliteRemoteSetBusyHandler(
  DoltliteRemote *p,
  int (*xBusy)(void*),
  void *pBusyArg
){
  if( !p ) return;
  p->xBusy = xBusy;
  p->pBusyArg = pBusyArg;
}

/* A remote opened outside a connection has no busy handler to consult; keep
** the fixed schedule for it rather than failing on the first contention. */
static int fsBusyRetry(DoltliteRemote *pRemote, int nBusy){
  if( pRemote && pRemote->xBusy ) return pRemote->xBusy(pRemote->pBusyArg);
  if( nBusy>=20 ) return 0;
  sqlite3_sleep(50);
  return 1;
}

static int localLockWithBusy(DoltliteRemote *pRemote, ChunkStore *cs){
  int nBusy = 0;
  int rc;
  do {
    rc = chunkStoreLockAndRefresh(cs);
  }while( rc==SQLITE_BUSY && fsBusyRetry(pRemote, nBusy++) );
  return rc;
}

static int fsLockAndForceRefresh(DoltliteRemote *pRemote, ChunkStore *cs){
  int nBusy = 0;
  int rc;
  do {
    rc = chunkStoreLockAndRefresh(cs);
  }while( rc==SQLITE_BUSY && fsBusyRetry(pRemote, nBusy++) );
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
  rc = fsLockAndForceRefresh(&p->base, &p->store);
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
  int changed = 0;
  int rc;
  if( p->locked ){
    return remoteGetRefs(pRemote, ppData, pnData);
  }
  /* Committed refs are a read. The graph lock is exclusive, so taking it
  ** fails a fetch while a writer holds a transaction. */
  rc = chunkStoreRefreshIfChanged(&p->store, &changed);
  if( rc!=SQLITE_OK ) return rc;
  return remoteGetRefs(pRemote, ppData, pnData);
}

static int fsSetRefs(DoltliteRemote *pRemote, const char *zBranch, int bForce,
                     const u8 *pData, int nData){
  FsRemote *p = (FsRemote*)pRemote;
  int bMissing = 0;
  int rc = doltliteValidateScopedRefsUpdate(&p->store, pData, nData,
                                            zBranch, bForce, &bMissing);
  if( rc==SQLITE_OK ){
    rc = doltliteValidateRefsTargetGraph(&p->store, pData, nData, zBranch,
                                         &bMissing);
    if( rc==SQLITE_CORRUPT && bMissing ) rc = SQLITE_BUSY_SNAPSHOT;
  }
  if( bMissing ) p->base.bForceResumeScan = 1;
  if( rc!=SQLITE_OK ) return rc;
  return doltliteInstallPushedRefsBlob(&p->store, pData, nData, zBranch);
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
    return SQLITE_BUSY_SNAPSHOT;
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
    rc = SQLITE_BUSY_SNAPSHOT;
  }
  if( rc==SQLITE_OK ){
    int bMissing = 0;
    rc = doltliteValidateScopedRefsUpdate(&p->store, pData, nData,
                                          zBranch, bForce, &bMissing);
    if( bMissing ) p->base.bForceResumeScan = 1;
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
  p->base.xGetChunks = remoteGetChunks;
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
  /* A zero-byte or torn creation file opens as an empty database. It has
  ** no repository, so a read-only remote cannot fetch or clone it. A
  ** foreign file still opens and fails later as not a database. */
  if( (flags & SQLITE_OPEN_READONLY)!=0
   && !p->store.notADatabase
   && !p->store.corruptHeader
   && chunkStoreIsEmpty(&p->store) ){
    chunkStoreClose(&p->store);
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
  int nBusy = 0;
  int rc;
  if( p->locked ) return SQLITE_OK;
  do {
    rc = chunkStoreLockAndRefresh(p->pStore);
  }while( rc==SQLITE_BUSY && fsBusyRetry(&p->base, nBusy++) );
  if( rc==SQLITE_OK ) rc = chunkStoreForceRefresh(p->pStore);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(p->pStore);
    return rc;
  }
  p->locked = 1;
  return SQLITE_OK;
}

/* Commit staged chunks. The commit takes the graph lock, adopts the peer's
** refs (a pinned snapshot and this handle's uncommitted ref edits included),
** and drops the lock before returning. A caller that already holds the lock
** keeps it; their own commit publishes the batch. */
static int localFlushChunks(LocalAsRemote *p){
  ChunkStore *cs;
  int rc;
  int nBusy;

  cs = p->pStore;
  if( cs->staging.nPending==0 && cs->staging.nRecentUncommitted==0 ){
    return SQLITE_OK;
  }
  if( p->locked || cs->lockDepth>0 ) return SQLITE_OK;
  nBusy = 0;
  do {
    rc = chunkStoreLockAndRefresh(cs);
  }while( rc==SQLITE_BUSY && fsBusyRetry(&p->base, nBusy++) );
  if( rc!=SQLITE_OK ) return rc;
  p->locked = 1;
  rc = chunkStoreAdoptDiskState(cs);
  if( rc==SQLITE_OK ){
    nBusy = 0;
    do {
      rc = chunkStoreCommitWithBusyHandler(cs, 0, 0);
    }while( rc==SQLITE_BUSY && fsBusyRetry(&p->base, nBusy++) );
  }
  if( rc!=SQLITE_OK ) chunkStoreRollback(cs);
  chunkStoreUnlock(cs);
  p->locked = 0;
  return rc;
}

/* DOLTLITE_FETCH_FLUSH_BYTES, when a positive integer, commits once the
** staged buffer reaches that many bytes. Otherwise the store drain limit
** applies. */
static int localStagingShouldFlush(ChunkStore *cs){
  const char *zEnv;
  int n;

  zEnv = getenv("DOLTLITE_FETCH_FLUSH_BYTES");
  if( zEnv && zEnv[0] ){
    n = atoi(zEnv);
    if( n>0 ) return cs->staging.nWriteBuf >= (i64)n;
  }
  return chunkStoreStagingExceedsDrain(cs);
}

/* DOLTLITE_PAUSE_DURING_LOCAL_PUT names a file. While it exists, the put
** waits (bounded) and writes <file>.ready. <file>.fail after the wait makes
** the put fail so a test can stop a download before refs are installed. */
static int localPauseDuringPut(void){
  const char *zPause;
  sqlite3_vfs *pVfs;
  sqlite3_file *pReady;
  char zReady[512];
  char zFail[512];
  int exists;
  int failExists;
  int waited;

  zPause = getenv("DOLTLITE_PAUSE_DURING_LOCAL_PUT");
  if( !zPause || !zPause[0] ) return SQLITE_OK;
  pVfs = sqlite3_vfs_find(0);
  if( !pVfs ) return SQLITE_OK;
  exists = 0;
  if( sqlite3OsAccess(pVfs, zPause, SQLITE_ACCESS_EXISTS, &exists)!=SQLITE_OK
   || !exists ){
    return SQLITE_OK;
  }
  pReady = 0;
  memset(zReady, 0, sizeof(zReady));
  sqlite3_snprintf(sizeof(zReady)-1, zReady, "%s.ready", zPause);
  if( sqlite3OsOpenMalloc(pVfs, zReady, &pReady,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, 0)
      ==SQLITE_OK ){
    sqlite3OsWrite(pReady, "x", 1, 0);
    sqlite3OsCloseFree(pReady);
  }
  waited = 0;
  while( waited<30000
      && sqlite3OsAccess(pVfs, zPause, SQLITE_ACCESS_EXISTS, &exists)==SQLITE_OK
      && exists ){
    sqlite3_sleep(20);
    waited += 20;
  }
  failExists = 0;
  sqlite3_snprintf(sizeof(zFail), zFail, "%s.fail", zPause);
  if( sqlite3OsAccess(pVfs, zFail, SQLITE_ACCESS_EXISTS, &failExists)==SQLITE_OK
   && failExists ){
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int localPutChunk(DoltliteRemote *pRemote, const ProllyHash *pHash,
                         const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int rc;

  /* Chunks are content-addressed and unreferenced until the tracking ref is
  ** installed, so the download itself does not hold the graph lock. */
  rc = remotePutChunk(pRemote, pHash, pData, nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = localPauseDuringPut();
  if( rc!=SQLITE_OK ) return rc;
  if( p->locked || p->pStore->lockDepth>0 ) return SQLITE_OK;
  if( localStagingShouldFlush(p->pStore) ) return localFlushChunks(p);
  return SQLITE_OK;
}

static int localSetRefs(DoltliteRemote *pRemote, const char *zBranch,
                        int bForce, const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int bMissing = 0;
  int rc = doltliteValidateScopedRefsUpdate(p->pStore, pData, nData,
                                            zBranch, bForce, &bMissing);
  if( rc==SQLITE_OK ){
    rc = doltliteValidateRefsTargetGraph(p->pStore, pData, nData, zBranch,
                                         &bMissing);
    if( rc==SQLITE_CORRUPT && bMissing ) rc = SQLITE_BUSY_SNAPSHOT;
  }
  if( bMissing ) p->base.bForceResumeScan = 1;
  if( rc!=SQLITE_OK ) return rc;
  return doltliteInstallPushedRefsBlob(p->pStore, pData, nData, zBranch);
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
    return SQLITE_BUSY_SNAPSHOT;
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
    rc = SQLITE_BUSY_SNAPSHOT;
  }
  if( rc==SQLITE_OK ){
    int bMissing = 0;
    rc = doltliteValidateScopedRefsUpdate(p->pStore, pData, nData,
                                          zBranch, bForce, &bMissing);
    if( bMissing ) p->base.bForceResumeScan = 1;
  }
  chunkStoreUnlock(p->pStore);
  p->locked = 0;
  return rc;
}

static int localCommit(DoltliteRemote *pRemote){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  int rc;
  if( p->locked ){
    rc = chunkStoreCommit(p->pStore);
    if( rc!=SQLITE_OK ) chunkStoreRollback(p->pStore);
    chunkStoreUnlock(p->pStore);
    p->locked = 0;
    return rc;
  }
  /* The caller already holds the graph lock. Leave the batch for that
  ** commit, and do not drop their hold. */
  if( p->pStore->lockDepth>0 ) return SQLITE_OK;
  return localFlushChunks(p);
}

static void localClose(DoltliteRemote *pRemote){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  ChunkStore *cs;

  cs = p->pStore;
  if( p->locked ){
    chunkStoreRollback(cs);
    chunkStoreUnlock(cs);
  }else if( cs->lockDepth<=0 && cs->staging.nRecentUncommitted==0
         && cs->staging.nPending>0 ){
    /* A failed download must not leave staged chunks for a later commit.
    ** Batches already committed are unreferenced until gc. */
    chunkStoreRollback(cs);
  }
  sqlite3_free(p);
}

/* The local store waits on its lock under the same busy handler as the
** remote it is syncing with: both belong to the calling connection. */
static DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal,
                                             DoltliteRemote *pPeer){
  LocalAsRemote *p = sqlite3_malloc(sizeof(LocalAsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(LocalAsRemote));
  if( pPeer ){
    p->base.xBusy = pPeer->xBusy;
    p->base.pBusyArg = pPeer->pBusyArg;
  }

  p->base.xGetChunk = remoteGetChunk;
  p->base.xGetChunks = remoteGetChunks;
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
  int *pIsAncestor,
  int *pbMissing
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
    if( rc==SQLITE_NOTFOUND && !cs->pChunkSource ){
      if( pbMissing ) *pbMissing = 1;
      rc = SQLITE_OK;
    }
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

static int remoteRefsHaveBranch(const RefsTable *rt, const char *zName){
  int n = 0, i;
  const BranchRef *a = 0;
  refsTableGetBranches(rt, &n, &a);
  for(i=0; i<n; i++){
    if( strcmp(a[i].zName, zName)==0 ) return 1;
  }
  return 0;
}

/* True when no existing branch name sorts before zDefault. */
static int remoteDefaultIsFirstName(const RefsTable *rt, const char *zDefault){
  int n = 0, i;
  const BranchRef *a = 0;
  refsTableGetBranches(rt, &n, &a);
  for(i=0; i<n; i++){
    if( strcmp(a[i].zName, zDefault)<0 ) return 0;
  }
  return 1;
}

/* The default a push leaves on the remote. An empty remote adopts the pushed
** branch. A newly created main, or master while there is no main, takes over.
** With neither, a new branch that sorts first takes over when the current
** default is already the first name and dolt_default_branch did not choose it. */
static const char *remotePushedDefault(
  const RefsTable *pCur,
  const char *zBranch,
  int bCreate
){
  const char *zDefault = scopedDefaultBranch(pCur);
  if( !bCreate ) return zDefault;
  if( refsTableBranchCount(pCur)==0 ) return zBranch;
  if( strcmp(zBranch, "main")==0 ) return zBranch;
  if( strcmp(zBranch, "master")==0 && strcmp(zDefault, "main")!=0
   && !remoteRefsHaveBranch(pCur, "main") ){
    return zBranch;
  }
  if( strcmp(zDefault, "main")!=0 && strcmp(zDefault, "master")!=0
   && !remoteRefsHaveBranch(pCur, "main")
   && !remoteRefsHaveBranch(pCur, "master")
   && !pCur->bDefaultExplicit
   && remoteDefaultIsFirstName(pCur, zDefault)
   && strcmp(zBranch, zDefault)<0 ){
    return zBranch;
  }
  return zDefault;
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

static int remoteValidateScopedRefsUpdate(
  ChunkStore *pStore,
  const RefsTable *pBasis,
  const u8 *pBlob,
  int nBlob,
  const char *zRef,
  int bForce,
  int *pbMissing
){
  ChunkStore inc;
  const BranchRef *aCur = 0, *aInc = 0;
  const TagRef *aCurTag = 0, *aIncTag = 0;
  const RemoteRef *aCurRem = 0, *aIncRem = 0;
  const TrackingBranch *aCurTrk = 0, *aIncTrk = 0;
  const SequenceRef *aCurSeq = 0, *aIncSeq = 0;
  const char *zTag;
  int nCur = 0, nInc = 0, nCurTag = 0, nIncTag = 0;
  int nCurRem = 0, nIncRem = 0, nCurTrk = 0, nIncTrk = 0;
  int nCurSeq = 0, nIncSeq = 0;
  const BranchRef *curB = 0, *incB = 0;
  int bDelete = zRef && zRef[0]==':';
  int rc;
  int i, j;

  if( bDelete ) zRef++;
  /* A ":tag:<name>" scope deletes the tag, as ":<branch>" deletes a branch. */
  zTag = remoteScopedTagName(zRef);
  if( bDelete && !zTag && scopedSameText(zRef, scopedDefaultBranch(pBasis)) ){
    return SQLITE_CONSTRAINT;
  }
  if( !zRef || !zRef[0] ) return SQLITE_MISUSE;
  if( !doltliteUserRefNameIsValid(zTag ? zTag : zRef) ) return SQLITE_CONSTRAINT;
  memset(&inc, 0, sizeof(inc));
  rc = csDeserializeRefsIntoTemp(&inc, pBlob, nBlob);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&inc);
    return rc;
  }

  refsTableGetBranches(pBasis, &nCur, &aCur);
  refsTableGetBranches(&inc.refs, &nInc, &aInc);
  refsTableGetTags(pBasis, &nCurTag, &aCurTag);
  refsTableGetTags(&inc.refs, &nIncTag, &aIncTag);
  refsTableGetRemotes(pBasis, &nCurRem, &aCurRem);
  refsTableGetRemotes(&inc.refs, &nIncRem, &aIncRem);
  refsTableGetTracking(pBasis, &nCurTrk, &aCurTrk);
  refsTableGetTracking(&inc.refs, &nIncTrk, &aIncTrk);
  refsTableGetSequences(pBasis, &nCurSeq, &aCurSeq);
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
    int curHasTag = 0;
    if( !scopedBranchesMatch(aCur, nCur, aInc, nInc)
     || !scopedRemotesMatch(aCurRem, nCurRem, aIncRem, nIncRem)
     || !scopedTrackingMatch(aCurTrk, nCurTrk, aIncTrk, nIncTrk)
     || !scopedSequencesMatch(aCurSeq, nCurSeq, aIncSeq, nIncSeq)
     || !scopedSameText(scopedDefaultBranch(pBasis),
                        scopedDefaultBranch(&inc.refs)) ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
    for(i=0; i<nCurTag; i++){
      if( strcmp(aCurTag[i].zName, zTag)==0 ){
        curHasTag = 1;
        continue;
      }
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
    if( bDelete ){
      rc = !incTag && curHasTag ? SQLITE_OK : SQLITE_CONSTRAINT;
    }else{
      rc = incTag ? SQLITE_OK : SQLITE_CONSTRAINT;
    }
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

  /* Push may repoint the default branch (clone checkout / GET /root) only
  ** as remotePushedDefault allows. An explicit choice stays until that rule
  ** changes the name. */
  {
    const char *zAllowed = remotePushedDefault(pBasis, zRef,
        !bDelete && !remoteRefsHaveBranch(pBasis, zRef));
    int bExplicit = pBasis->bDefaultExplicit;
    if( !scopedSameText(zAllowed, scopedDefaultBranch(pBasis)) ){
      bExplicit = 0;
    }
    if( !scopedSameText(scopedDefaultBranch(&inc.refs), zAllowed)
     || inc.refs.bDefaultExplicit!=bExplicit ){
      rc = SQLITE_CONSTRAINT;
      goto done;
    }
  }

  /* Declared branch may be created; an existing one must fast-forward unless forced. */
  for(i=0; i<nCur; i++){
    if( strcmp(aCur[i].zName, zRef)==0 ){ curB = &aCur[i]; break; }
  }
  for(j=0; j<nInc; j++){
    if( strcmp(aInc[j].zName, zRef)==0 ){ incB = &aInc[j]; break; }
  }
  if( bDelete ? incB!=0 : incB==0 ){
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
    int bMissing = 0;
    rc = syncIsAncestor(
        pStore, &curB->commitHash, &incB->commitHash, &anc, &bMissing);
    if( rc!=SQLITE_OK ) goto done;
    /* An unreachable ancestry is a pushed graph we do not fully hold, not
    ** proof of a non-fast-forward. */
    if( anc==0 && bMissing && pbMissing ){
      *pbMissing = 1;
      rc = SQLITE_BUSY_SNAPSHOT;
      goto done;
    }
    if( anc==0 ){ rc = SQLITE_CONSTRAINT; goto done; }
  }

  /* The declared working set may stay as it is or become empty, which is
  ** what a push writes. Any other hash can name a stored commit chunk, and
  ** the branch then cannot be opened. Force does not widen this. */
  if( !bDelete && incB && !prollyHashIsEmpty(&incB->workingSetHash)
   && (curB==0 || prollyHashCompare(&incB->workingSetHash,
                                    &curB->workingSetHash)!=0) ){
    rc = SQLITE_CONSTRAINT;
    goto done;
  }

  rc = SQLITE_OK;

done:
  csFreeRefsState(&inc);
  return rc;
}


static int remoteCatalogSequences(
  ChunkStore *pStore,
  ChunkStore *pNext,
  ProllyCache *pCache,
  const ProllyHash *pCatalog
){
  struct TableEntry *aTables = 0;
  int nTables = 0;
  int i;
  int rc = doltliteLoadCatalogFromStore(pStore, pCatalog, &aTables, &nTables, 0);
  for(i=0; i<nTables && rc==SQLITE_OK; i++){
    struct TableEntry *pTable = &aTables[i];
    ProllyCursor cursor;
    int res;
    if( pTable->iTable==1 || !pTable->zName
     || !(pTable->flags & PROLLY_NODE_INTKEY) ) continue;
    prollyCursorInit(&cursor, pStore, pCache, &pTable->root, pTable->flags);
    if( strcmp(pTable->zName, "sqlite_sequence")==0 ){
      rc = prollyCursorFirst(&cursor, &res);
      while( rc==SQLITE_OK && prollyCursorIsValid(&cursor) ){
        DoltliteRecordInfo info = {0};
        DoltliteSerialValue name, seq;
        const u8 *pValue;
        int nValue;
        prollyCursorValue(&cursor, &pValue, &nValue);
        rc = doltliteParseRecordStrict(pValue, nValue, &info);
        if( rc==SQLITE_OK && info.nField!=2 ) rc = SQLITE_CORRUPT;
        if( rc==SQLITE_OK ){
          rc = doltliteSerialValueFromPayload(pValue, nValue,
                    info.aType[0], info.aOffset[0], &name);
        }
        if( rc==SQLITE_OK ){
          rc = doltliteSerialValueFromPayload(pValue, nValue,
                    info.aType[1], info.aOffset[1], &seq);
        }
        if( rc==SQLITE_OK && name.eType==SQLITE_TEXT && name.n>0
         && !memchr(name.p, 0, name.n)
         && seq.eType==SQLITE_INTEGER && seq.i>0 ){
          char *zName = sqlite3_mprintf("%.*s", name.n, (const char*)name.p);
          struct TableEntry *pTarget;
          if( !zName ){
            rc = SQLITE_NOMEM;
          }else{
            pTarget = doltliteFindTableByName(aTables, nTables, zName);
            if( pTarget && pTarget->iTable!=1
             && (pTarget->flags & PROLLY_NODE_INTKEY) ){
              rc = chunkStoreBumpSequence(pNext, zName, seq.i);
            }
          }
          sqlite3_free(zName);
        }
        doltliteRecordInfoClear(&info);
        if( rc==SQLITE_OK ) rc = prollyCursorNext(&cursor);
      }
    }else{
      rc = prollyCursorLast(&cursor, &res);
      if( rc==SQLITE_OK && prollyCursorIsValid(&cursor)
       && prollyCursorIntKey(&cursor)>0 ){
        rc = chunkStoreBumpSequence(pNext, pTable->zName,
                                      prollyCursorIntKey(&cursor));
      }
    }
    prollyCursorClose(&cursor);
  }
  doltliteFreeCatalog(aTables, nTables);
  return rc;
}

int doltliteInstallPushedRefsBlob(
  ChunkStore *pStore,
  const u8 *pBlob,
  int nBlob,
  const char *zRef
){
  ChunkStore next;
  DoltliteCommitQueue queue;
  ProllyHashSet catalogs;
  ProllyCache cache;
  ProllyHash head;
  SequenceRef *aSequences = 0;
  int nSequences = 0;
  u8 *pNextBlob = 0;
  int nNextBlob = 0;
  int i;
  int rc;

  PROLLY_ASSERT_STORE_GRAPH_LOCKED(pStore);
  memset(&next, 0, sizeof(next));
  memset(&queue, 0, sizeof(queue));
  memset(&catalogs, 0, sizeof(catalogs));
  memset(&cache, 0, sizeof(cache));
  rc = chunkStoreLoadRefsFromBlob(&next, pBlob, nBlob);
  if( rc==SQLITE_OK ) rc = csCopySequences(pStore, &aSequences, &nSequences);
  if( rc==SQLITE_OK ){
    csInstallSequences(&next, aSequences, nSequences);
    aSequences = 0;
  }
  if( rc==SQLITE_OK && zRef[0]!=':' && !remoteScopedTagName(zRef) ){
    rc = chunkStoreFindBranch(&next, zRef, &head);
    if( rc==SQLITE_OK ) rc = doltliteCommitQueueInit(&queue, &head);
    if( rc==SQLITE_OK ) rc = prollyHashSetInit(&catalogs, 64);
    if( rc==SQLITE_OK ) rc = prollyCacheInit(&cache, 1024*1024);
    for(i=0; i<pStore->refs.nBranches && rc==SQLITE_OK; i++){
      rc = prollyHashSetAdd(&queue.visited, &pStore->refs.aBranches[i].commitHash);
    }
    while( rc==SQLITE_OK ){
      DoltliteCommit commit;
      int has;
      rc = doltliteCommitQueueNext(&queue, &head, &has);
      if( rc!=SQLITE_OK || !has ) break;
      memset(&commit, 0, sizeof(commit));
      {
        u8 *pData = 0;
        int nData = 0;
        rc = chunkStoreGet(pStore, &head, &pData, &nData);
        if( rc==SQLITE_OK ) rc = doltliteCommitDeserialize(pData, nData, &commit);
        sqlite3_free(pData);
      }
      if( rc==SQLITE_OK
       && !prollyHashSetContains(&catalogs, &commit.catalogHash) ){
        rc = prollyHashSetAdd(&catalogs, &commit.catalogHash);
        if( rc==SQLITE_OK ){
          rc = remoteCatalogSequences(pStore, &next, &cache, &commit.catalogHash);
        }
      }
      if( rc==SQLITE_OK ) rc = doltliteCommitQueueEnqueueParents(&queue, &commit);
      doltliteCommitClear(&commit);
    }
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(&next, &pNextBlob, &nNextBlob);
  }
  if( rc==SQLITE_OK ) rc = chunkStoreInstallRefsBlob(pStore, pNextBlob, nNextBlob);
  sqlite3_free(pNextBlob);
  csFreeSequenceArray(aSequences, nSequences);
  prollyCacheFree(&cache);
  prollyHashSetFree(&catalogs);
  doltliteCommitQueueClear(&queue);
  chunkStoreClose(&next);
  return rc;
}

int doltliteValidateScopedRefsUpdate(
  ChunkStore *pStore, const u8 *pBlob, int nBlob,
  const char *zRef, int bForce, int *pbMissing
){
  return remoteValidateScopedRefsUpdate(pStore, &pStore->refs, pBlob, nBlob,
                                       zRef, bForce, pbMissing);
}

int doltliteMergeScopedRefsUpdate(
  ChunkStore *pStore, const ProllyHash *pExpectedRefsHash,
  const u8 *pBlob, int nBlob, const char *zRef, int bForce,
  u8 **ppMerged, int *pnMerged, int *pbMissing
){
  ChunkStore base, incoming, merged;
  SavedRefsState local;
  u8 *pBaseBlob = 0;
  u8 *pCurrentBlob = 0;
  int nBaseBlob = 0;
  int nCurrentBlob = 0;
  int rc;
  const char *zTag = remoteScopedTagName(zRef + (zRef[0]==':'));

  PROLLY_ASSERT_STORE_GRAPH_LOCKED(pStore);
  *ppMerged = 0;
  *pnMerged = 0;
  memset(&base, 0, sizeof(base));
  memset(&incoming, 0, sizeof(incoming));
  memset(&merged, 0, sizeof(merged));
  if( !prollyHashIsEmpty(pExpectedRefsHash) ){
    rc = chunkStoreGet(pStore, pExpectedRefsHash, &pBaseBlob, &nBaseBlob);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_BUSY_SNAPSHOT;
    if( rc!=SQLITE_OK ) goto done;
    rc = chunkStoreLoadRefsFromBlob(&base, pBaseBlob, nBaseBlob);
    if( rc!=SQLITE_OK ) goto done;
  }
  rc = remoteValidateScopedRefsUpdate(pStore, &base.refs, pBlob, nBlob,
                                     zRef, bForce, pbMissing);
  if( rc!=SQLITE_OK ) goto done;
  if( zTag ){
    const TagRef *pBaseTag = 0, *pCurrentTag = 0;
    int i;
    for(i=0; i<base.refs.nTags; i++){
      if( strcmp(base.refs.aTags[i].zName, zTag)==0 ){
        pBaseTag = &base.refs.aTags[i];
        break;
      }
    }
    for(i=0; i<pStore->refs.nTags; i++){
      if( strcmp(pStore->refs.aTags[i].zName, zTag)==0 ){
        pCurrentTag = &pStore->refs.aTags[i];
        break;
      }
    }
    if( !scopedTagsMatch(pBaseTag, pBaseTag!=0,
                         pCurrentTag, pCurrentTag!=0) ){
      rc = SQLITE_BUSY_SNAPSHOT;
      goto done;
    }
  }else{
    const char *zBranch = zRef + (zRef[0]==':');
    ProllyHash baseCommit = {{0}}, currentCommit = {{0}};
    ProllyHash baseWs = {{0}}, currentWs = {{0}};
    int baseExists = chunkStoreFindBranch(&base, zBranch, &baseCommit)==SQLITE_OK;
    int currentExists = chunkStoreFindBranch(pStore, zBranch, &currentCommit)==SQLITE_OK;
    chunkStoreGetBranchWorkingSet(&base, zBranch, &baseWs);
    chunkStoreGetBranchWorkingSet(pStore, zBranch, &currentWs);
    if( baseExists!=currentExists
     || prollyHashCompare(&baseCommit, &currentCommit)!=0
     || prollyHashCompare(&baseWs, &currentWs)!=0 ){
      rc = SQLITE_BUSY_SNAPSHOT;
      goto done;
    }
  }
  rc = chunkStoreLoadRefsFromBlob(&incoming, pBlob, nBlob);
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(pStore, &pCurrentBlob, &nCurrentBlob);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreLoadRefsFromBlob(&merged, pCurrentBlob, nCurrentBlob);
  }
  if( rc==SQLITE_OK ){
    csCaptureSavedRefsState(&incoming, &local);
    rc = csMergeSavedRefsOntoDisk(&merged, &local, &base.refs);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(&merged, ppMerged, pnMerged);
  }

done:
  sqlite3_free(pBaseBlob);
  sqlite3_free(pCurrentBlob);
  chunkStoreClose(&base);
  chunkStoreClose(&incoming);
  chunkStoreClose(&merged);
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
  const char *zRef,
  int bForce
){
  return doltlitePushAs(pLocal, pRemote, 0, zRef, bForce, 0);
}

static void remotePushRetryDelay(int attempt){
  if( attempt ){
    u8 jitter;
    int delay = attempt<5 ? 5 << attempt : 100;
    sqlite3_randomness(1, &jitter);
    sqlite3_sleep(delay + jitter%delay);
  }
}

/* zRef names the remote branch (":name" deletes it). pSrcCommit, when set,
** is that commit. Otherwise zLocal, or zRef, is the local branch pushed. */
int doltlitePushAs(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zLocal,
  const char *zRef,
  int bForce,
  const ProllyHash *pSrcCommit
){
  const int bDelete = zRef[0]==':';
  const char *zBranch = zRef + bDelete;
  ProllyHash localCommit = {{0}};
  ProllyHash originalCommit = {{0}};
  int originalExists = 0;
  int attempt;
  int rc;

  if( !zBranch[0] ) return SQLITE_MISUSE;
  if( !bDelete ){
    if( pSrcCommit ){
      memcpy(&localCommit, pSrcCommit, sizeof(localCommit));
    }else{
      rc = chunkStoreFindBranch(pLocal, zLocal ? zLocal : zBranch, &localCommit);
      if( rc!=SQLITE_OK ) return SQLITE_ERROR;
    }
  }

  for(attempt=0; attempt<64; attempt++){
    ChunkStore refs;
    ProllyHash remoteCommit = {{0}};
    ProllyHash expectedRefsHash = {{0}};
    u8 *refsData = 0;
    int nRefsData = 0;
    int exists;
    int noOp;
    remotePushRetryDelay(attempt);
    memset(&refs, 0, sizeof(refs));
    rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc==SQLITE_OK && refsData ){
      prollyHashCompute(refsData, nRefsData, &expectedRefsHash);
      rc = chunkStoreLoadRefsFromBlob(&refs, refsData, nRefsData);
    }
    if( rc!=SQLITE_OK ) goto push_done;
    exists = chunkStoreFindBranch(&refs, zBranch, &remoteCommit)==SQLITE_OK;
    if( attempt==0 ){
      originalExists = exists;
      originalCommit = remoteCommit;
      if( bDelete && !exists ){
        rc = SQLITE_NOTFOUND;
        goto push_done;
      }
      if( !bDelete && exists && !bForce
       && !prollyHashIsEmpty(&remoteCommit)
       && prollyHashCompare(&remoteCommit, &localCommit)!=0 ){
        int isAnc = 0;
        rc = syncIsAncestor(pLocal, &remoteCommit, &localCommit, &isAnc, 0);
        if( rc==SQLITE_OK && !isAnc ) rc = SQLITE_CONSTRAINT;
        if( rc!=SQLITE_OK ) goto push_done;
      }
    }else if( exists!=originalExists
           || prollyHashCompare(&remoteCommit, &originalCommit)!=0 ){
      rc = SQLITE_BUSY_SNAPSHOT;
      sqlite3_free(refsData);
      chunkStoreClose(&refs);
      return rc;
    }
    noOp = !bDelete && exists
        && prollyHashCompare(&remoteCommit, &localCommit)==0;
    if( noOp ){
      rc = pRemote->xCheckRefsIf(pRemote, &expectedRefsHash, zRef, bForce,
                                  refsData, nRefsData);
      goto push_done;
    }
    sqlite3_free(refsData);
    refsData = 0;
    if( bDelete ){
      if( strcmp(zBranch, scopedDefaultBranch(&refs.refs))==0 ){
        rc = SQLITE_CONSTRAINT;
        goto push_done;
      }
      rc = chunkStoreDeleteBranch(&refs, zBranch);
    }else{
      DoltliteRemote *pLocalSrc = doltliteLocalAsRemote(pLocal, pRemote);
      const SequenceRef *aSeq = 0;
      ProllyHash emptyWs = {{0}};
      int nSeq = 0;
      int i;
      if( !pLocalSrc ){
        rc = SQLITE_NOMEM;
        goto push_done;
      }
      rc = syncChunks(pLocalSrc, pRemote, &localCommit, 1, &refs);
      pLocalSrc->xClose(pLocalSrc);
      if( rc!=SQLITE_OK ) goto push_done;
      {
        const char *zCur = scopedDefaultBranch(&refs.refs);
        const char *zNewDefault =
            remotePushedDefault(&refs.refs, zBranch, !exists);
        if( !scopedSameText(zNewDefault, zCur) ){
          rc = chunkStoreSetDefaultBranch(&refs, zNewDefault);
          if( rc==SQLITE_OK ) refs.refs.bDefaultExplicit = 0;
        }
      }
      if( rc==SQLITE_OK ){
        rc = exists ? chunkStoreUpdateBranch(&refs, zBranch, &localCommit)
                    : chunkStoreAddBranch(&refs, zBranch, &localCommit);
      }
      if( rc==SQLITE_OK ){
        rc = chunkStoreSetBranchWorkingSet(&refs, zBranch, &emptyWs);
      }
      refsTableGetSequences(&pLocal->refs, &nSeq, &aSeq);
      for(i=0; i<nSeq && rc==SQLITE_OK; i++){
        rc = chunkStoreBumpSequence(&refs, aSeq[i].zTableName, aSeq[i].iSeq);
      }
    }
    if( rc==SQLITE_OK ){
      rc = chunkStoreSerializeRefsToBlob(&refs, &refsData, &nRefsData);
    }
    if( rc==SQLITE_OK ){
      rc = pRemote->xSetRefsIf(pRemote, &expectedRefsHash, zRef, bForce,
                                refsData, nRefsData);
    }
    if( rc==SQLITE_OK ){
      doltliteTestCrashFinalize("push");
      rc = pRemote->xCommit(pRemote);
    }

push_done:
    sqlite3_free(refsData);
    chunkStoreClose(&refs);
    if( rc!=SQLITE_BUSY_SNAPSHOT ) return rc;
  }
  return SQLITE_BUSY_SNAPSHOT;
}

static const TagRef *remoteFindTag(ChunkStore *pRefs, const char *zTag){
  int i;
  for(i=0; i<pRefs->refs.nTags; i++){
    if( strcmp(pRefs->refs.aTags[i].zName, zTag)==0 ){
      return &pRefs->refs.aTags[i];
    }
  }
  return 0;
}

int doltlitePushTag(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zTag
){
  const TagRef *pLocalTag = remoteFindTag(pLocal, zTag);
  const TagRef *pOriginalTag;
  ProllyHash tagCommit;
  ChunkStore originalRefs;
  u8 *refsData = 0;
  int nRefsData = 0;
  int attempt;
  int rc;

  if( !pLocalTag ) return SQLITE_NOTFOUND;
  memcpy(&tagCommit, &pLocalTag->commitHash, sizeof(tagCommit));
  memset(&originalRefs, 0, sizeof(originalRefs));
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc==SQLITE_OK && refsData ){
    rc = chunkStoreLoadRefsFromBlob(&originalRefs, refsData, nRefsData);
  }else if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  sqlite3_free(refsData);
  refsData = 0;
  if( rc!=SQLITE_OK ) goto tag_done;
  pOriginalTag = remoteFindTag(&originalRefs, zTag);
  if( pOriginalTag && scopedTagsMatch(pLocalTag, 1, pOriginalTag, 1) ){
    goto tag_done;
  }

  for(attempt=0; attempt<64; attempt++){
    DoltliteRemote *pLocalSrc;
    ChunkStore nextRefs;
    const TagRef *pCurrentTag;
    ProllyHash expectedRefsHash = {{0}};
    u8 *newRefs = 0;
    char *zScope = 0;
    int nNewRefs = 0;
    int targetChanged = 0;

    remotePushRetryDelay(attempt);
    memset(&nextRefs, 0, sizeof(nextRefs));
    pLocalSrc = doltliteLocalAsRemote(pLocal, pRemote);
    if( !pLocalSrc ){
      rc = SQLITE_NOMEM;
      break;
    }
    rc = syncChunks(pLocalSrc, pRemote, &tagCommit, 1,
                              &originalRefs);
    pLocalSrc->xClose(pLocalSrc);
    if( rc!=SQLITE_OK ) goto tag_attempt_done;

    nRefsData = 0;
    rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc==SQLITE_OK && refsData && nRefsData>0 ){
      prollyHashCompute(refsData, nRefsData, &expectedRefsHash);
      rc = chunkStoreLoadRefsFromBlob(&nextRefs, refsData, nRefsData);
    }
    if( rc!=SQLITE_OK ) goto tag_attempt_done;
    pCurrentTag = remoteFindTag(&nextRefs, zTag);
    if( !scopedTagsMatch(pOriginalTag, pOriginalTag!=0,
                         pCurrentTag, pCurrentTag!=0) ){
      targetChanged = 1;
      rc = SQLITE_BUSY_SNAPSHOT;
      goto tag_attempt_done;
    }
    if( pCurrentTag ) rc = chunkStoreDeleteTag(&nextRefs, zTag);
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
    if( rc==SQLITE_OK ){
      doltliteTestCrashFinalize("push");
      rc = pRemote->xCommit(pRemote);
    }

tag_attempt_done:
    sqlite3_free(refsData);
    refsData = 0;
    sqlite3_free(zScope);
    sqlite3_free(newRefs);
    chunkStoreClose(&nextRefs);
    if( rc!=SQLITE_BUSY_SNAPSHOT || targetChanged ) break;
  }

tag_done:
  chunkStoreClose(&originalRefs);
  return rc;
}

/* Delete tag zTag from the remote. As in doltlitePushTag, the refs update is
** a compare-and-swap scoped to the tag: it is retried when other refs change
** and refused when the tag itself moves. SQLITE_NOTFOUND if the remote has no
** such tag. */
int doltliteDeleteRemoteTag(DoltliteRemote *pRemote, const char *zTag){
  const TagRef *pOriginalTag;
  ChunkStore originalRefs;
  u8 *refsData = 0;
  int nRefsData = 0;
  int attempt;
  int rc;

  memset(&originalRefs, 0, sizeof(originalRefs));
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc==SQLITE_OK && refsData ){
    rc = chunkStoreLoadRefsFromBlob(&originalRefs, refsData, nRefsData);
  }else if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  sqlite3_free(refsData);
  refsData = 0;
  if( rc!=SQLITE_OK ) goto delete_tag_done;
  pOriginalTag = remoteFindTag(&originalRefs, zTag);
  if( !pOriginalTag ){
    rc = SQLITE_NOTFOUND;
    goto delete_tag_done;
  }

  for(attempt=0; attempt<64; attempt++){
    ChunkStore nextRefs;
    const TagRef *pCurrentTag;
    ProllyHash expectedRefsHash = {{0}};
    u8 *newRefs = 0;
    char *zScope = 0;
    int nNewRefs = 0;
    int targetChanged = 0;

    remotePushRetryDelay(attempt);
    memset(&nextRefs, 0, sizeof(nextRefs));
    nRefsData = 0;
    rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc==SQLITE_OK && refsData && nRefsData>0 ){
      prollyHashCompute(refsData, nRefsData, &expectedRefsHash);
      rc = chunkStoreLoadRefsFromBlob(&nextRefs, refsData, nRefsData);
    }
    if( rc!=SQLITE_OK ) goto delete_tag_attempt_done;
    pCurrentTag = remoteFindTag(&nextRefs, zTag);
    if( !scopedTagsMatch(pOriginalTag, 1, pCurrentTag, pCurrentTag!=0) ){
      targetChanged = 1;
      rc = pCurrentTag ? SQLITE_BUSY_SNAPSHOT : SQLITE_NOTFOUND;
      goto delete_tag_attempt_done;
    }
    rc = chunkStoreDeleteTag(&nextRefs, zTag);
    if( rc==SQLITE_OK ){
      rc = chunkStoreSerializeRefsToBlob(&nextRefs, &newRefs, &nNewRefs);
    }
    if( rc==SQLITE_OK ){
      zScope = sqlite3_mprintf(":%s%s", REMOTE_TAG_SCOPE_PREFIX, zTag);
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
    if( rc==SQLITE_OK ){
      doltliteTestCrashFinalize("push");
      rc = pRemote->xCommit(pRemote);
    }

delete_tag_attempt_done:
    sqlite3_free(refsData);
    refsData = 0;
    sqlite3_free(zScope);
    sqlite3_free(newRefs);
    chunkStoreClose(&nextRefs);
    if( rc!=SQLITE_BUSY_SNAPSHOT || targetChanged ) break;
  }

delete_tag_done:
  chunkStoreClose(&originalRefs);
  return rc;
}

static int installFetchedRefs(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  ChunkStore *pRemoteRefs,
  const char *zRemoteName,
  const char *zBranch,
  const ProllyHash *pRemoteCommit,
  int bSkipGraphValidation,
  int bHasSeen,
  const ProllyHash *pSeenCommit,
  int *pSettled
){
  ChunkStore nextRefs;
  SavedRefsState savedRefs;
  ProllyHash oldRefsHash;
  ProllyHash oldCommittedRefsHash;
  ProllyHash currentTracking;
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
  int updateTracking;
  int commitRefs;
  int descends;
  int ahead;
  int findRc;
  int i;
  int rc;

  memset(&nextRefs, 0, sizeof(nextRefs));
  memset(&savedRefs, 0, sizeof(savedRefs));
  memset(&currentTracking, 0, sizeof(currentTracking));
  updateTracking = 0;
  commitRefs = 0;
  if( pSettled ) *pSettled = 0;

  rc = localLockWithBusy(pRemote, pLocal);
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
    rc = remoteValidateFetchedGraph(
        pLocal, pRemoteCommit, zRemoteName, zBranch);
    if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
      /* The miss also recorded a missing-chunk error on the origin source,
      ** which would report a healthy store as corrupt. */
      sqlite3_free(chunkStoreSourceTakeError(pLocal, 0));
      rc = SQLITE_BUSY_SNAPSHOT;
    }
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
    findRc = chunkStoreFindTracking(
        &nextRefs, zRemoteName, zBranch, &currentTracking);
    if( findRc==SQLITE_NOTFOUND ){
      updateTracking = 1;
      commitRefs = 1;
      if( pSettled ) *pSettled = 1;
    }else if( findRc!=SQLITE_OK ){
      rc = findRc;
    }else if( prollyHashCompare(&currentTracking, pRemoteCommit)==0
           || (bHasSeen && pSeenCommit
               && prollyHashCompare(&currentTracking, pSeenCommit)==0) ){
      /* Still the commit this fetch observed, or already at the fetched
      ** tip. A remote reset installs when nobody else moved the ref. */
      updateTracking = 1;
      commitRefs = 1;
      if( pSettled ) *pSettled = 1;
    }else{
      descends = 0;
      rc = syncIsAncestor(
          pLocal, &currentTracking, pRemoteCommit, &descends, 0);
      if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
      if( rc==SQLITE_OK && descends ){
        updateTracking = 1;
        commitRefs = 1;
        if( pSettled ) *pSettled = 1;
      }else if( rc==SQLITE_OK ){
        ahead = 0;
        rc = syncIsAncestor(
            pLocal, pRemoteCommit, &currentTracking, &ahead, 0);
        if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
        if( rc==SQLITE_OK && ahead ){
          /* The tracking ref moved past this fetch. Do not commit the
          ** older refs snapshot, or its tags and sequences would follow. */
          if( pSettled ) *pSettled = 1;
        }else if( rc==SQLITE_OK ){
          /* Diverged. The caller re-reads the remote and tries again. */
          if( pSettled ) *pSettled = 0;
        }
      }
    }
  }
  if( rc==SQLITE_OK && updateTracking ){
    rc = chunkStoreUpdateTracking(
        &nextRefs, zRemoteName, zBranch, pRemoteCommit);
  }
  if( rc==SQLITE_OK && commitRefs ){
    rc = chunkStoreSerializeRefsToBlob(&nextRefs, &nextData, &nNextData);
  }
  if( rc==SQLITE_OK && commitRefs ){
    memcpy(&oldRefsHash, refsTableGetHash(&pLocal->refs),
           sizeof(oldRefsHash));
    memcpy(&oldCommittedRefsHash, &pLocal->refs.committedRefsHash,
           sizeof(oldCommittedRefsHash));
    csDetachSavedRefsState(pLocal, &savedRefs);
    refsDetached = 1;
    rc = chunkStoreInstallRefsBlob(pLocal, nextData, nNextData);
  }
  if( rc==SQLITE_OK && commitRefs ) rc = chunkStoreCommit(pLocal);

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
  return doltliteFetchInto(pLocal, pRemote, zRemoteName, zBranch, zBranch, 0);
}

/* Fetch remote branch zBranch into the tracking ref zRemoteName/zTrack.
** A tracking ref that diverged during the download is re-read. A commit
** the ref has already moved past is left in place. */
int doltliteFetchInto(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zRemoteName,
  const char *zBranch,
  const char *zTrack,
  int *pbMissingBranch
){
  int attempt;
  int rc;

  if( pbMissingBranch ) *pbMissingBranch = 0;
  rc = SQLITE_OK;
  for(attempt=0; attempt<8; attempt++){
    u8 *refsData = 0;
    int nRefsData = 0;
    ProllyHash remoteCommit;
    ProllyHash trackingCommit;
    DoltliteRemote *pLocalDst = 0;
    ChunkStore remoteRefs;
    int bLazyOrigin;
    int bHasSeen;
    int settled;

    memset(&remoteCommit, 0, sizeof(remoteCommit));
    memset(&trackingCommit, 0, sizeof(trackingCommit));
    memset(&remoteRefs, 0, sizeof(remoteRefs));
    bLazyOrigin = chunkStoreOriginSourceEnabled(pLocal)
               && strcmp(zRemoteName, "origin")==0;
    bHasSeen = 0;
    settled = 0;

    rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
    if( rc!=SQLITE_OK ) return rc;

    rc = chunkStoreLoadRefsFromBlob(&remoteRefs, refsData, nRefsData);
    sqlite3_free(refsData);
    if( rc==SQLITE_OK ){
      rc = chunkStoreFindBranch(&remoteRefs, zBranch, &remoteCommit);
      if( rc==SQLITE_NOTFOUND && pbMissingBranch ) *pbMissingBranch = 1;
    }
    if( rc!=SQLITE_OK ){
      chunkStoreClose(&remoteRefs);
      return rc==SQLITE_NOTFOUND ? SQLITE_NOTFOUND : rc;
    }

    if( prollyHashIsEmpty(&remoteCommit) ){
      if( pbMissingBranch ) *pbMissingBranch = 1;
      chunkStoreClose(&remoteRefs);
      return SQLITE_NOTFOUND;
    }

    rc = chunkStoreFindTracking(pLocal, zRemoteName, zTrack, &trackingCommit);
    if( rc==SQLITE_OK ){
      bHasSeen = 1;
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
      pLocalDst = doltliteLocalAsRemote(pLocal, pRemote);
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
          pLocal, pRemote, &remoteRefs, zRemoteName, zTrack,
          &remoteCommit, bLazyOrigin,
          bHasSeen, bHasSeen ? &trackingCommit : 0, &settled);
    }

    chunkStoreClose(&remoteRefs);
    if( rc!=SQLITE_OK || settled ) return rc;
  }
  return SQLITE_OK;
}

/* A single-branch clone keeps one local branch: zKeep when that name is a
** branch, otherwise the default. Every other clone keeps the default and
** zKeep. */
static int cloneBranchKept(
  const char *zName,
  const char *zDefault,
  const char *zKeep,
  int bKeepIsBranch,
  int bSingle
){
  if( bSingle ){
    if( bKeepIsBranch ) return strcmp(zName, zKeep)==0;
    return zDefault && strcmp(zName, zDefault)==0;
  }
  if( zDefault && strcmp(zName, zDefault)==0 ) return 1;
  if( bKeepIsBranch && strcmp(zName, zKeep)==0 ) return 1;
  return 0;
}

static int remotePrepareCloneRefs(
  ChunkStore *pStore,
  const char *zUrl,
  const char *zKeep,
  const char *zRemoteName,
  int bSingle
){
  const BranchRef *aBranch;
  int nBranch;
  int rc;
  int i;
  int bKeepIsBranch;

  if( !zRemoteName || !zRemoteName[0] ) zRemoteName = "origin";
  csFreeRemotes(pStore);
  csFreeTracking(pStore);
  rc = chunkStoreAddRemote(pStore, zRemoteName, zUrl);
  bKeepIsBranch = zKeep && zKeep[0]
               && chunkStoreFindBranch(pStore, zKeep, 0)==SQLITE_OK;
  refsTableGetBranches(&pStore->refs, &nBranch, &aBranch);
  for(i=0; i<nBranch && rc==SQLITE_OK; i++){
    const char *zDefault = chunkStoreGetDefaultBranch(pStore);
    if( !cloneBranchKept(aBranch[i].zName, zDefault, zKeep,
                         bKeepIsBranch, bSingle) && bSingle ){
      continue;
    }
    rc = chunkStoreUpdateTracking(
        pStore, zRemoteName, aBranch[i].zName, &aBranch[i].commitHash);
  }
  /* Only the default branch and zKeep are local, as in Dolt; the rest stay
  ** remote-tracking so a later checkout starts from what was fetched.
  ** --single-branch keeps just the one branch that will be checked out. */
  i = 0;
  while( rc==SQLITE_OK && chunkStoreGetDefaultBranch(pStore) ){
    const char *zDefault = chunkStoreGetDefaultBranch(pStore);
    refsTableGetBranches(&pStore->refs, &nBranch, &aBranch);
    while( i<nBranch && cloneBranchKept(aBranch[i].zName, zDefault, zKeep,
                                         bKeepIsBranch, bSingle) ){
      i++;
    }
    if( i>=nBranch ) break;
    rc = chunkStoreDeleteBranch(pStore, aBranch[i].zName);
  }
  /* Deleting the old default under --single-branch leaves the name behind.
  ** Point it at the branch this clone will check out. */
  if( rc==SQLITE_OK ){
    const char *zDefault;
    const char *zCheckout;
    int missing;
    zDefault = chunkStoreGetDefaultBranch(pStore);
    zCheckout = 0;
    missing = !zDefault
           || chunkStoreFindBranch(pStore, zDefault, 0)!=SQLITE_OK;
    if( missing ){
      if( bKeepIsBranch && zKeep && zKeep[0] ) zCheckout = zKeep;
      if( !zCheckout ){
        refsTableGetBranches(&pStore->refs, &nBranch, &aBranch);
        if( nBranch>0 ) zCheckout = aBranch[0].zName;
      }
      if( zCheckout ) rc = chunkStoreSetDefaultBranch(pStore, zCheckout);
    }
  }
  return rc;
}

/* Wait until zPause disappears. path.ready is the signal that this clone
** is inside the window before it locks the target. */
static void clonePauseBeforeRefs(void){
  const char *zPause;
  sqlite3_vfs *pVfs;
  sqlite3_file *pReady;
  char zReady[512];
  int exists;

  zPause = getenv("DOLTLITE_PAUSE_BEFORE_CLONE_REFS");
  if( !zPause || !zPause[0] ) return;
  pVfs = sqlite3_vfs_find(0);
  if( !pVfs ) return;
  exists = 1;
  pReady = 0;
  memset(zReady, 0, sizeof(zReady));
  sqlite3_snprintf(sizeof(zReady)-1, zReady, "%s.ready", zPause);
  if( sqlite3OsOpenMalloc(pVfs, zReady, &pReady,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, 0)
      ==SQLITE_OK ){
    /* xAccess reports an empty file as missing. */
    sqlite3OsWrite(pReady, "x", 1, 0);
    sqlite3OsCloseFree(pReady);
  }
  while( sqlite3OsAccess(pVfs, zPause, SQLITE_ACCESS_EXISTS, &exists)==SQLITE_OK
      && exists ){
    sqlite3_sleep(20);
  }
}

/* 0: still an empty store, or one root commit whose working set matches it.
** 1: anything else, including a commit that can no longer be read.
** 2: that root commit has uncommitted changes. A stored working set that
** still matches the root is the clean session, not a peer write. */
static int cloneTargetOccupied(ChunkStore *pLocal){
  const BranchRef *aBr;
  DoltliteCommit commit;
  u8 *pData;
  int nData;
  int nBr;
  int dirty;
  int one;
  int rc;

  if( chunkStoreIsEmpty(pLocal) ) return 0;
  nBr = 0;
  aBr = 0;
  refsTableGetBranches(&pLocal->refs, &nBr, &aBr);
  if( nBr!=1 || !aBr ) return 1;
  pData = 0;
  nData = 0;
  memset(&commit, 0, sizeof(commit));
  rc = chunkStoreGet(pLocal, &aBr[0].commitHash, &pData, &nData);
  if( rc==SQLITE_OK ){
    rc = doltliteCommitDeserialize(pData, nData, &commit);
  }
  sqlite3_free(pData);
  if( rc!=SQLITE_OK || doltliteCommitParentCount(&commit)!=0 ){
    doltliteCommitClear(&commit);
    return 1;
  }
  doltliteCommitClear(&commit);
  one = 0;
  rc = scopedWorkingSetIsDirty(pLocal, &aBr[0], &one);
  if( rc!=SQLITE_OK ) return 1;
  dirty = one;
  return dirty ? 2 : 0;
}

int doltliteCloneLazy(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zUrl,
  const char *zKeepBranch,
  const char *zRemoteName,
  int bSingleBranch,
  int *pbOccupied
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
  if( pbOccupied ) *pbOccupied = 0;
  if( !zUrl ) return SQLITE_MISUSE;

  doltliteTestRunBeforeCloneRefsHook();
  clonePauseBeforeRefs();
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
  if( rc==SQLITE_OK ){
    rc = remotePrepareCloneRefs(&refsView, zUrl, zKeepBranch,
                                zRemoteName, bSingleBranch);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreSerializeRefsToBlob(
        &refsView, &pLocalRefs, &nLocalRefs);
  }
  if( rc!=SQLITE_OK ) goto lazy_clone_done;

  rc = localLockWithBusy(pRemote, pLocal);
  if( rc==SQLITE_OK ){
    locked = 1;
    rc = chunkStoreForceRefresh(pLocal);
  }
  /* The caller's empty check ran before the remote read. A peer can commit
  ** or write in that gap. Refuse before the snapshot, so failure leaves the
  ** refreshed refs in place. */
  if( rc==SQLITE_OK ){
    int occupied = cloneTargetOccupied(pLocal);
    if( occupied ){
      if( pbOccupied ) *pbOccupied = occupied;
      rc = SQLITE_ERROR;
      if( locked ) chunkStoreUnlock(pLocal);
      locked = 0;
      goto lazy_clone_done;
    }
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

/* The checkout branch has to be in the refs about to be committed. A miss
** is a failed clone and must not land on disk. */
static int cloneCheckoutBranchMissing(
  ChunkStore *pLocal,
  const char *zKeep
){
  const char *zNeed;
  ProllyHash commitHash;
  int nBr;
  const BranchRef *aBr;

  nBr = 0;
  aBr = 0;
  zNeed = zKeep ? zKeep : chunkStoreGetDefaultBranch(pLocal);
  if( !zNeed ){
    refsTableGetBranches(&pLocal->refs, &nBr, &aBr);
    if( nBr>0 ) zNeed = aBr[0].zName;
  }
  if( !zNeed ) return 1;
  memset(&commitHash, 0, sizeof(commitHash));
  if( chunkStoreFindBranch(pLocal, zNeed, &commitHash)!=SQLITE_OK ) return 1;
  if( prollyHashIsEmpty(&commitHash) ) return 1;
  return 0;
}

int doltliteClone(
  ChunkStore *pLocal,
  DoltliteRemote *pRemote,
  const char *zUrl,
  const char *zKeepBranch,
  const char *zRemoteName,
  int bSingleBranch,
  int *pbMissingBranch,
  int *pbOccupied
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
  if( pbMissingBranch ) *pbMissingBranch = 0;
  if( pbOccupied ) *pbOccupied = 0;
  if( !zUrl ) return SQLITE_MISUSE;

  doltliteTestRunBeforeCloneRefsHook();
  clonePauseBeforeRefs();
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = remoteCollectRootsFromRefsBlob(refsData, nRefsData, &aRoots, &nRoots);
  if( rc!=SQLITE_OK ){
    sqlite3_free(refsData);
    return rc;
  }

  if( nRoots>0 ){
    pLocalDst = doltliteLocalAsRemote(pLocal, pRemote);
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
  rc = localLockWithBusy(pRemote, pLocal);
  if( rc==SQLITE_OK ){
    locked = 1;
    rc = chunkStoreForceRefresh(pLocal);
  }
  /* Restoring the pre-refresh hash would put the cleared refs back over a
  ** peer commit. Leave the refreshed tables and skip the install. */
  if( rc==SQLITE_OK ){
    int occupied = cloneTargetOccupied(pLocal);
    if( occupied ){
      if( pbOccupied ) *pbOccupied = occupied;
      sqlite3_free(aRoots);
      sqlite3_free(refsData);
      if( locked ) chunkStoreUnlock(pLocal);
      return SQLITE_ERROR;
    }
  }
  if( rc==SQLITE_OK && nRoots>0 ){
    rc = remoteValidateGraph(pLocal, aRoots, nRoots);
    if( rc==SQLITE_NOTFOUND || rc==SQLITE_CORRUPT ){
      /* A miss on a store that already has origin records the lazy-clone
      ** corruption text. This refusal is retryable; that text is not. */
      sqlite3_free(chunkStoreSourceTakeError(pLocal, 0));
      rc = SQLITE_BUSY_SNAPSHOT;
    }
  }
  sqlite3_free(aRoots);
  aRoots = 0;
  if( rc!=SQLITE_OK ) goto clone_restore_refs;

  if( refsData && nRefsData > 0 ){
    csDetachSavedRefsState(pLocal, &savedRefs);
    refsDetached = 1;
    rc = chunkStoreInstallRefsBlob(pLocal, refsData, nRefsData);
    /* A clone replaces the empty target outright; the working sets it
    ** installs are the ones the session goes on to overwrite. */
    if( rc==SQLITE_OK ){
      pLocal->bWsReplacing = 0;
      chunkStoreReadoptWorkingSetBasis(pLocal);
    }
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
    if( rc==SQLITE_OK ){
      rc = remotePrepareCloneRefs(pLocal, zUrl, zKeepBranch,
                                  zRemoteName, bSingleBranch);
    }
    if( rc==SQLITE_OK ){
      rc = chunkStoreSerializeRefs(pLocal);
    }
    if( rc!=SQLITE_OK ) goto clone_restore_refs;
  }

  if( cloneCheckoutBranchMissing(pLocal, zKeepBranch) ){
    if( pbMissingBranch ) *pbMissingBranch = 1;
    rc = SQLITE_ERROR;
    goto clone_restore_refs;
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
