
#ifdef DOLTLITE_PROLLY

#include "doltlite_remote.h"
#include "doltlite_commit.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include <string.h>

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

static int syncEnqueueChildren(
  const u8 *data,
  int nData,
  SyncQueue *q,
  ProllyHashSet *seen
){
  int rc = SQLITE_OK;
  int i;

  if( syncIsProllyNodeChunk(data, nData) ){
    
    ProllyNode node;
    int parseRc = prollyNodeParse(&node, data, nData);
    if( parseRc==SQLITE_OK && node.level > 0 ){
      for(i=0; i<(int)node.nItems; i++){
        ProllyHash childHash;
        prollyNodeChildHash(&node, i, &childHash);
        if( !prollyHashIsEmpty(&childHash) && !prollyHashSetContains(seen, &childHash) ){
          rc = prollyHashSetAdd(seen, &childHash);
          if( rc==SQLITE_OK ) rc = syncQueuePush(q, &childHash);
        }
        if( rc!=SQLITE_OK ) break;
      }
    }
  }else if( syncIsCommitChunk(data, nData) ){

    DoltliteCommit commit;
    int drc;
    memset(&commit, 0, sizeof(commit));
    drc = doltliteCommitDeserialize(data, nData, &commit);
    if( drc==SQLITE_OK ){
      int pi;
      for(pi=0; pi<commit.nParents && rc==SQLITE_OK; pi++){
        if( !prollyHashIsEmpty(&commit.aParents[pi])
            && !prollyHashSetContains(seen, &commit.aParents[pi]) ){
          rc = prollyHashSetAdd(seen, &commit.aParents[pi]);
          if( rc==SQLITE_OK ) rc = syncQueuePush(q, &commit.aParents[pi]);
        }
      }
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&commit.catalogHash)
          && !prollyHashSetContains(seen, &commit.catalogHash) ){
        rc = prollyHashSetAdd(seen, &commit.catalogHash);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &commit.catalogHash);
      }
      doltliteCommitClear(&commit);
    }
  }else{
    
    if( nData == WS_TOTAL_SIZE && data[0] == 1 ){
      ProllyHash h;
      memcpy(h.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
      if( !prollyHashIsEmpty(&h) && !prollyHashSetContains(seen, &h) ){
        rc = prollyHashSetAdd(seen, &h);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &h);
      }
      memcpy(h.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(&h) && !prollyHashSetContains(seen, &h) ){
        rc = prollyHashSetAdd(seen, &h);
        if( rc==SQLITE_OK ) rc = syncQueuePush(q, &h);
      }
      if( rc==SQLITE_OK && data[WS_MERGING_OFF] ){
        memcpy(h.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
        if( !prollyHashIsEmpty(&h) && !prollyHashSetContains(seen, &h) ){
          rc = prollyHashSetAdd(seen, &h);
          if( rc==SQLITE_OK ) rc = syncQueuePush(q, &h);
        }
      }
    }

    
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
            if( !prollyHashIsEmpty(&tableRoot) && !prollyHashSetContains(seen, &tableRoot) ){
              rc = prollyHashSetAdd(seen, &tableRoot);
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
    
    rc = chunkStoreReloadRefs(&p->store);
  }
  return rc;
}

static int fsCommit(DoltliteRemote *pRemote){
  FsRemote *p = (FsRemote*)pRemote;
  int rc;

  
  rc = chunkStoreSerializeRefs(&p->store);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreCommit(&p->store);
  if( rc!=SQLITE_OK ) return rc;

  
  {
    const char *zDef = chunkStoreGetDefaultBranch(&p->store);
    ProllyHash branchCommit;
    if( zDef && chunkStoreFindBranch(&p->store, zDef, &branchCommit)==SQLITE_OK ){
      u8 *cdata = 0; int ncdata = 0;
      if( chunkStoreGet(&p->store, &branchCommit, &cdata, &ncdata)==SQLITE_OK && cdata ){
        DoltliteCommit commit;
        if( doltliteCommitDeserialize(cdata, ncdata, &commit)==SQLITE_OK ){
          chunkStoreWriteBranchWorkingCatalog(&p->store, zDef, &commit.catalogHash);
          doltliteCommitClear(&commit);
        }
        sqlite3_free(cdata);
      }
      chunkStoreCommit(&p->store);  
    }
  }

  return SQLITE_OK;
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

  syncQueuePush(&queue, pDescendant);
  prollyHashSetAdd(&visited, pDescendant);

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
          if( !prollyHashSetContains(&visited, &commit.aParents[pi]) ){
            prollyHashSetAdd(&visited, &commit.aParents[pi]);
            syncQueuePush(&queue, &commit.aParents[pi]);
          }
        }
        doltliteCommitClear(&commit);
      }
    }
    sqlite3_free(data);
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
  ProllyHash remoteCommit;
  int rc;
  int i;

  
  rc = chunkStoreFindBranch(pLocal, zBranch, &localCommit);
  if( rc!=SQLITE_OK ){
    return SQLITE_ERROR; 
  }

  
  if( !bForce ){
    u8 *refsData = 0;
    int nRefsData = 0;
    rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
    if( rc==SQLITE_OK && refsData ){

      ChunkStore tmpCs;
      const u8 *p;
      memset(&tmpCs, 0, sizeof(tmpCs));


      p = refsData;
      if( nRefsData >= 9 ){
        u8 ver; int defLen;
        ver = p[0]; p++;
        defLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
        if( p + defLen + 4 <= refsData + nRefsData ){
          int nBranches;
          p += defLen;
          nBranches = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
          for(i=0; i<nBranches; i++){
            int nameLen;
            if( p+4 > refsData+nRefsData ) break;
            nameLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
            if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
            if( nameLen==(int)strlen(zBranch) && memcmp(p, zBranch, nameLen)==0 ){
              memcpy(remoteCommit.data, p+nameLen, PROLLY_HASH_SIZE);
              if( !prollyHashIsEmpty(&remoteCommit)
                  && prollyHashCompare(&remoteCommit, &localCommit)!=0 ){

                int isAnc = syncIsAncestor(pLocal, &remoteCommit, &localCommit);
                if( isAnc <= 0 ){
                  sqlite3_free(refsData);
                  return SQLITE_ERROR;
                }
              }
              break;
            }
            p += nameLen + PROLLY_HASH_SIZE;

            if( p+PROLLY_HASH_SIZE <= refsData+nRefsData ){
              p += PROLLY_HASH_SIZE;
            }
          }
        }
      }
      sqlite3_free(refsData);
      (void)tmpCs;
    }else if( rc==SQLITE_NOTFOUND ){
      rc = SQLITE_OK; 
    }
    if( rc!=SQLITE_OK ) return rc;
  }

  
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

      
      rc = chunkStoreSerializeRefsToBlob(&tmpCs, &newRefs, &nNewRefs);
      chunkStoreClose(&tmpCs);
      if( rc!=SQLITE_OK ) return rc;

      rc = pRemote->xSetRefs(pRemote, newRefs, nNewRefs);
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

  
  if( nRefsData >= 9 ){
    const u8 *p = refsData;
    u8 ver; int defLen;
    ver = p[0]; p++;
    defLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
    if( p + defLen + 4 <= refsData + nRefsData ){
      int nBranches, i;
      p += defLen;
      nBranches = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
      for(i=0; i<nBranches; i++){
        int nameLen;
        if( p+4 > refsData+nRefsData ) break;
        nameLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
        if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
        if( nameLen==(int)strlen(zBranch) && memcmp(p, zBranch, nameLen)==0 ){
          memcpy(remoteCommit.data, p+nameLen, PROLLY_HASH_SIZE);
          found = 1;
        }
        p += nameLen + PROLLY_HASH_SIZE;
        if( p+PROLLY_HASH_SIZE <= refsData+nRefsData ){
          p += PROLLY_HASH_SIZE;
        }
        if( found ) break;
      }
    }
  }
  sqlite3_free(refsData);

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

  
  rc = chunkStoreSerializeRefs(pLocal);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(pLocal);

  return rc;
}

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote){
  u8 *refsData = 0;
  int nRefsData = 0;
  ProllyHash *aRoots = 0;
  int nRoots = 0;
  int nRootsAlloc = 0;
  DoltliteRemote *pLocalDst = 0;
  int rc;

  
  rc = pRemote->xGetRefs(pRemote, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  
  if( nRefsData >= 9 ){
    const u8 *p = refsData;
    u8 ver; int defLen;
    ver = p[0]; p++;
    defLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
    if( p + defLen + 4 <= refsData + nRefsData ){
      int nBranches, nTags, i;
      p += defLen;
      nBranches = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;

      nRootsAlloc = nBranches + 16;
      aRoots = sqlite3_malloc(nRootsAlloc * sizeof(ProllyHash));
      if( !aRoots ){
        sqlite3_free(refsData);
        return SQLITE_NOMEM;
      }

      for(i=0; i<nBranches; i++){
        int nameLen;
        if( p+4 > refsData+nRefsData ) break;
        nameLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
        if( p+nameLen+PROLLY_HASH_SIZE > refsData+nRefsData ) break;
        p += nameLen;

        memcpy(aRoots[nRoots].data, p, PROLLY_HASH_SIZE);
        if( !prollyHashIsEmpty(&aRoots[nRoots]) ) nRoots++;
        p += PROLLY_HASH_SIZE;

        p += PROLLY_HASH_SIZE; /* skip workingSetHash */
      }


      if( p+4 <= refsData+nRefsData ){
        nTags = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
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
          if( p+4 > refsData+nRefsData ) break;
          nameLen = (int)p[0]|((int)p[1]<<8)|((int)p[2]<<16)|((int)p[3]<<24); p += 4;
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
      memcpy(&pLocal->refsHash, &refsHash, sizeof(ProllyHash));
    }
  }
  sqlite3_free(refsData);
  if( rc!=SQLITE_OK ) return rc;

  
  rc = chunkStoreCommit(pLocal);
  if( rc!=SQLITE_OK ) return rc;

  
  rc = chunkStoreReloadRefs(pLocal);

  return rc;
}

#endif 
