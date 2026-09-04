#ifndef DOLTLITE_REMOTE_H
#define DOLTLITE_REMOTE_H

#ifndef DOLTLITE_ENABLE_REMOTES
#define DOLTLITE_ENABLE_REMOTES 1
#endif

#include "chunk_store.h"

typedef struct DoltliteRemote DoltliteRemote;
struct DoltliteRemote {
  int (*xGetChunk)(DoltliteRemote*, const ProllyHash*, u8**, int*);
  int (*xPutChunk)(DoltliteRemote*, const ProllyHash*, const u8*, int);
  int (*xHasChunks)(DoltliteRemote*, const ProllyHash*, int nHash, u8 *aResult);
  /* Optional batch get. OK fills apData/anData (free non-NULL); NULL slot
  ** is absent. NULL method uses xGetChunk. */
  int (*xGetChunks)(DoltliteRemote*, const ProllyHash *aHash, int nHash,
                    u8 **apData, int *anData);
  int (*xGetRefs)(DoltliteRemote*, u8**, int*);
  /* Push scope; reject other ref changes and non-fast-forwards unless bForce. */
  int (*xSetRefs)(DoltliteRemote*, const char *zBranch, int bForce,
                  const u8*, int);
  int (*xSetRefsIf)(DoltliteRemote*, const ProllyHash*, const char *zBranch,
                    int bForce, const u8*, int);
  int (*xCommit)(DoltliteRemote*);
  void (*xClose)(DoltliteRemote*);
  /* Last remote error, or NULL. Valid until the next op or xClose. */
  const char *(*xErrMsg)(DoltliteRemote*);
  int bResumePartialPuts;
};

static inline int doltliteRemotePersistRefs(ChunkStore *cs){
  int rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  return rc;
}

int doltliteSyncChunks(
  DoltliteRemote *pSrc,
  DoltliteRemote *pDst,
  ProllyHash *aRoots,
  int nRoots
);

int doltlitePush(ChunkStore *pLocal, DoltliteRemote *pRemote,
                 const char *zBranch, int bForce);

int doltlitePushTag(ChunkStore *pLocal, DoltliteRemote *pRemote,
                    const char *zTag);

/* Allow only the declared branch or tag update; every other ref must match. */
int doltliteValidateScopedRefsUpdate(ChunkStore *pStore, const u8 *pBlob,
                                     int nBlob, const char *zRef,
                                     int bForce);

int doltliteValidateRefsTargetGraph(ChunkStore *pStore, const u8 *pBlob,
                                    int nBlob, const char *zRef);

int doltliteFetch(ChunkStore *pLocal, DoltliteRemote *pRemote,
                  const char *zRemoteName, const char *zBranch);

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote,
                  const char *zUrl);

int doltliteCloneLazy(ChunkStore *pLocal, DoltliteRemote *pRemote,
                      const char *zUrl);

DoltliteRemote *doltliteFsRemoteOpen(sqlite3_vfs *pVfs, const char *zPath);

DoltliteRemote *doltliteRemoteOpenReadOnly(sqlite3_vfs *pVfs,
                                           const char *zUrl);

DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal);

DoltliteRemote *doltliteHttpRemoteOpen(const char *zUrl);

int doltliteHttpParseResponseForTest(const u8 *pRaw, int nRaw, int nHash);

#endif
