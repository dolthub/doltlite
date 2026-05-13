
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

#ifndef _WIN32
int doltliteWriteAll(int fd, const void *pBuf, int nBuf){
  int nWritten = 0;
  const u8 *p = (const u8*)pBuf;
  while( nWritten < nBuf ){
    ssize_t n = write(fd, p + nWritten, nBuf - nWritten);
    if( n<0 ){
      if( errno==EINTR ) continue;
      return SQLITE_IOERR_WRITE;
    }
    if( n==0 ) return SQLITE_IOERR_WRITE;
    nWritten += (int)n;
  }
  return SQLITE_OK;
}
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
    const BranchRef *aBr;
    const TagRef *aTg;
    refsTableGetBranches(&refsView.refs, &nBr, &aBr);
    refsTableGetTags(&refsView.refs, &nTg, &aTg);
    nAlloc = nBr + nTg + 1;
    if( nAlloc>0 ){
      aRoots = sqlite3_malloc(nAlloc * (int)sizeof(ProllyHash));
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

    for(i=0; i<nBatch && rc==SQLITE_OK; i++){
      u8 *data = 0;
      int nData = 0;

      if( aPresent[i] ){

        continue;
      }

      rc = pSrc->xGetChunk(pSrc, &aBatch[i], &data, &nData);
      if( rc==SQLITE_NOTFOUND ){

        rc = SQLITE_OK;
        continue;
      }
      if( rc!=SQLITE_OK ) break;

      rc = pDst->xPutChunk(pDst, &aBatch[i], data, nData);
      if( rc!=SQLITE_OK ){
        sqlite3_free(data);
        break;
      }

      rc = syncEnqueueChildren(data, nData, &queue, &seen);
      sqlite3_free(data);
    }
  }

  prollyHashSetFree(&seen);
  syncQueueFree(&queue);
  return rc;
}

typedef struct FsRemote FsRemote;
struct FsRemote {
  DoltliteRemote base;
  ChunkStore store;
  int lockedForCas;
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
  (void)pHash;
  return chunkStorePut(&p->store, pData, nData, &computed);
}

static int fsHasChunks(DoltliteRemote *pRemote, const ProllyHash *aHash,
                       int nHash, u8 *aResult){
  FsRemote *p = (FsRemote*)pRemote;
  int i;
  for(i=0; i<nHash; i++){
    int has = 0;
    int rc = chunkStoreHas(&p->store, &aHash[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    aResult[i] = has ? 1 : 0;
  }
  return SQLITE_OK;
}

static int fsGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  FsRemote *p = (FsRemote*)pRemote;
  *ppData = 0;
  *pnData = 0;
  if( prollyHashIsEmpty(refsTableGetHash(&p->store.refs)) ){
    return SQLITE_NOTFOUND;
  }
  return chunkStoreGet(&p->store, refsTableGetHash(&p->store.refs), ppData, pnData);
}

static int fsSetRefs(DoltliteRemote *pRemote, const u8 *pData, int nData){
  FsRemote *p = (FsRemote*)pRemote;
  ProllyHash oldRefsHash;
  ProllyHash refsHash;
  int rc = chunkStorePut(&p->store, pData, nData, &refsHash);
  if( rc==SQLITE_OK ){
    memcpy(&oldRefsHash, refsTableGetHash(&p->store.refs), sizeof(ProllyHash));
    refsTableSetHash(&p->store.refs, &refsHash);

    rc = chunkStoreReloadRefs(&p->store);
    if( rc!=SQLITE_OK ){
      refsTableSetHash(&p->store.refs, &oldRefsHash);
      if( !prollyHashIsEmpty(&oldRefsHash) ){
        int restoreRc = chunkStoreReloadRefs(&p->store);
        if( restoreRc!=SQLITE_OK ) return restoreRc;
      }
    }
  }
  return rc;
}

static int fsSetRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
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
  rc = chunkStoreLockAndRefresh(&p->store);
  if( rc!=SQLITE_OK ) return rc;
  p->lockedForCas = 1;
  if( prollyHashCompare(refsTableGetHash(&p->store.refs), &expected)!=0 ){
    chunkStoreUnlock(&p->store);
    p->lockedForCas = 0;
    return SQLITE_BUSY;
  }
  rc = fsSetRefs(pRemote, pData, nData);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(&p->store);
    p->lockedForCas = 0;
  }
  return rc;
}

static int remoteStorePersistRefs(ChunkStore *pStore){
  int rc = chunkStoreSerializeRefs(pStore);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(pStore);
  return rc;
}

static int fsCommit(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  int rc = remoteStorePersistRefs(&p->store);
  if( p->lockedForCas ){
    chunkStoreUnlock(&p->store);
    p->lockedForCas = 0;
  }
  return rc;
}

static void fsClose(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  if( p->lockedForCas ) chunkStoreUnlock(&p->store);
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
  p->base.xSetRefsIf = fsSetRefsIf;
  p->base.xCommit = fsCommit;
  p->base.xClose = fsClose;

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
    int has = 0;
    int rc = chunkStoreHas(p->pStore, &aHash[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    aResult[i] = has ? 1 : 0;
  }
  return SQLITE_OK;
}

static int localGetRefs(DoltliteRemote *pRemote, u8 **ppData, int *pnData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  *ppData = 0;
  *pnData = 0;
  if( prollyHashIsEmpty(refsTableGetHash(&p->pStore->refs)) ){
    return SQLITE_NOTFOUND;
  }
  return chunkStoreGet(p->pStore, refsTableGetHash(&p->pStore->refs), ppData, pnData);
}

static int localSetRefs(DoltliteRemote *pRemote, const u8 *pData, int nData){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  ProllyHash refsHash;
  int rc = chunkStorePut(p->pStore, pData, nData, &refsHash);
  if( rc==SQLITE_OK ){
    refsTableSetHash(&p->pStore->refs, &refsHash);
  }
  return rc;
}

static int localSetRefsIf(
  DoltliteRemote *pRemote,
  const ProllyHash *pExpectedRefsHash,
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
  return localSetRefs(pRemote, pData, nData);
}

static int localCommit(DoltliteRemote *pRemote){
  LocalAsRemote *p = (LocalAsRemote*)pRemote;
  return chunkStoreCommit(p->pStore);
}

static void localClose(DoltliteRemote *pRemote){

  sqlite3_free(pRemote);
}

DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal){
  LocalAsRemote *p = sqlite3_malloc(sizeof(LocalAsRemote));
  if( !p ) return 0;
  memset(p, 0, sizeof(LocalAsRemote));

  p->base.xGetChunk = localGetChunk;
  p->base.xPutChunk = localPutChunk;
  p->base.xHasChunks = localHasChunks;
  p->base.xGetRefs = localGetRefs;
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

  if( !bForce ){
    if( rc==SQLITE_OK && refsData ){
      rc = remoteFindBranchFromRefsBlob(refsData, nRefsData, zBranch, &remoteCommit);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&remoteCommit)
          && prollyHashCompare(&remoteCommit, &localCommit)!=0 ){
        int isAnc = syncIsAncestor(pLocal, &remoteCommit, &localCommit);
        if( isAnc <= 0 ){
          sqlite3_free(refsData);
          return isAnc<0 ? SQLITE_NOMEM : SQLITE_ERROR;
        }
      }
      if( rc==SQLITE_NOTFOUND ){
        rc = SQLITE_OK;
      }else if( rc!=SQLITE_OK ){
        sqlite3_free(refsData);
        return rc;
      }
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

        chunkStoreSetDefaultBranch(&tmpCs, "main");
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

      rc = chunkStoreSerializeRefsToBlob(&tmpCs, &newRefs, &nNewRefs);
      chunkStoreClose(&tmpCs);
      if( rc!=SQLITE_OK ) return rc;

      if( pRemote->xSetRefsIf ){
        rc = pRemote->xSetRefsIf(pRemote, &expectedRefsHash, newRefs, nNewRefs);
      }else{
        rc = pRemote->xSetRefs(pRemote, newRefs, nNewRefs);
      }
      sqlite3_free(newRefs);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  rc = pRemote->xCommit(pRemote);

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
  DoltliteRemote *pLocalDst = 0;
  int rc;
  int found = 0;

  memset(&remoteCommit, 0, sizeof(remoteCommit));

  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = remoteFindBranchFromRefsBlob(refsData, nRefsData, zBranch, &remoteCommit);
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ){
    return rc==SQLITE_NOTFOUND ? SQLITE_NOTFOUND : rc;
  }
  found = !prollyHashIsEmpty(&remoteCommit);

  if( !found || prollyHashIsEmpty(&remoteCommit) ){
    return SQLITE_NOTFOUND;
  }

  pLocalDst = doltliteLocalAsRemote(pLocal);
  if( !pLocalDst ) return SQLITE_NOMEM;

  rc = doltliteSyncChunks(pRemote, pLocalDst, &remoteCommit, 1);

  pLocalDst->xClose(pLocalDst);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreUpdateTracking(pLocal, zRemoteName, zBranch, &remoteCommit);
  if( rc!=SQLITE_OK ) return rc;

  return remoteStorePersistRefs(pLocal);
}

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash *aRoots = 0;
  int nRoots = 0;
  DoltliteRemote *pLocalDst = 0;
  ProllyHash oldRefsHash;
  int rc;

  memcpy(&oldRefsHash, refsTableGetHash(&pLocal->refs), sizeof(ProllyHash));

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

    pLocalDst->xClose(pLocalDst);
    sqlite3_free(aRoots);
    aRoots = 0;

    if( rc!=SQLITE_OK ){
      sqlite3_free(refsData);
      return rc;
    }
  }

  if( refsData && nRefsData > 0 ){
    ProllyHash refsHash;
    rc = chunkStorePut(pLocal, refsData, nRefsData, &refsHash);
    if( rc==SQLITE_OK ){
      refsTableSetHash(&pLocal->refs, &refsHash);
    }
  }
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreCommit(pLocal);
  if( rc!=SQLITE_OK ){
    refsTableSetHash(&pLocal->refs, &oldRefsHash);
    return rc;
  }

  rc = chunkStoreReloadRefs(pLocal);
  if( rc!=SQLITE_OK ){
    refsTableSetHash(&pLocal->refs, &oldRefsHash);
    if( !prollyHashIsEmpty(&oldRefsHash) ){
      int restoreRc = chunkStoreReloadRefs(pLocal);
      if( restoreRc!=SQLITE_OK ) return restoreRc;
    }
  }

  return rc;
}

#endif
