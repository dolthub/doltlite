#ifndef DOLTLITE_REMOTE_H
#define DOLTLITE_REMOTE_H

#include "chunk_store.h"

typedef struct DoltliteRemote DoltliteRemote;
struct DoltliteRemote {
  int (*xGetChunk)(DoltliteRemote*, const ProllyHash*, u8**, int*);
  int (*xPutChunk)(DoltliteRemote*, const ProllyHash*, const u8*, int);
  int (*xHasChunks)(DoltliteRemote*, const ProllyHash*, int nHash, u8 *aResult);
  /* Optional batched fetch: retrieve nHash chunks in one round trip. On
  ** SQLITE_OK, apData[i]/anData[i] hold each chunk (caller frees every
  ** non-NULL apData[i]); a NULL apData[i] means that chunk was absent. May be
  ** NULL, in which case callers fall back to per-chunk xGetChunk. */
  int (*xGetChunks)(DoltliteRemote*, const ProllyHash *aHash, int nHash,
                    u8 **apData, int *anData);
  int (*xGetRefs)(DoltliteRemote*, u8**, int*);
  /* zBranch/bForce declare the intended push scope so the receiver can reject
  ** any refs change outside that branch and enforce fast-forward. */
  int (*xSetRefs)(DoltliteRemote*, const char *zBranch, int bForce,
                  const u8*, int);
  int (*xSetRefsIf)(DoltliteRemote*, const ProllyHash*, const char *zBranch,
                    int bForce, const u8*, int);
  int (*xCommit)(DoltliteRemote*);
  void (*xClose)(DoltliteRemote*);
  /* Optional. Last human-readable error from a remote op, or NULL. Lifetime
  ** is until the next op or xClose. */
  const char *(*xErrMsg)(DoltliteRemote*);
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

/* Enforce that a pushed refs blob only creates/advances zBranch (fast-forward
** unless bForce) and leaves every other branch and tag byte-identical to
** pStore's current refs. Returns SQLITE_OK if allowed, SQLITE_CONSTRAINT if
** out of scope or a non-fast-forward without force. */
int doltliteValidateScopedRefsUpdate(ChunkStore *pStore, const u8 *pBlob,
                                     int nBlob, const char *zBranch,
                                     int bForce);

int doltliteFetch(ChunkStore *pLocal, DoltliteRemote *pRemote,
                  const char *zRemoteName, const char *zBranch);

int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote);

DoltliteRemote *doltliteFsRemoteOpen(sqlite3_vfs *pVfs, const char *zPath);

DoltliteRemote *doltliteLocalAsRemote(ChunkStore *pLocal);

DoltliteRemote *doltliteHttpRemoteOpen(const char *zUrl);

int doltliteHttpParseResponseForTest(const u8 *pRaw, int nRaw, int nHash);

#endif
