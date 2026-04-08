#ifndef DOLTLITE_REMOTE_H
#define DOLTLITE_REMOTE_H

#include "chunk_store.h"

typedef struct DoltliteRemote DoltliteRemote;
struct DoltliteRemote {
  
  int (*xGetChunk)(DoltliteRemote*, const ProllyHash*, u8**, int*);

  
  int (*xPutChunk)(DoltliteRemote*, const ProllyHash*, const u8*, int);

  
  int (*xHasChunks)(DoltliteRemote*, const ProllyHash*, int nHash, u8 *aResult);

  
  int (*xGetRefs)(DoltliteRemote*, u8**, int*);
  int (*xSetRefs)(DoltliteRemote*, const u8*, int);

  
  int (*xCommit)(DoltliteRemote*);

  
  void (*xClose)(DoltliteRemote*);
};

int doltliteSyncChunks(
  DoltliteRemote *pSrc,    
  DoltliteRemote *pDst,    
  ProllyHash *aRoots,      
  int nRoots               
);

int doltlitePush(ChunkStore *pLocal, DoltliteRemote *pRemote,
                 const char *zBranch, int bForce);

int doltliteFetch(ChunkStore *pLocal, DoltliteRemote *pRemote,
                  const char *zRemoteName, const char *zBranch);

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote);

DoltliteRemote *doltliteFsRemoteOpen(sqlite3_vfs *pVfs, const char *zPath);

DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal);

DoltliteRemote *doltliteHttpRemoteOpen(const char *zUrl);

#endif 
