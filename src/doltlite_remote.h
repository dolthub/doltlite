#ifndef DOLTLITE_REMOTE_H
#define DOLTLITE_REMOTE_H

#include "chunk_store.h"

/*
** Abstract remote store interface.
** Different transports (filesystem, HTTP) implement this vtable.
*/
typedef struct DoltliteRemote DoltliteRemote;
struct DoltliteRemote {
  /* Get a chunk by hash. Caller frees *ppData. Returns SQLITE_NOTFOUND if absent. */
  int (*xGetChunk)(DoltliteRemote*, const ProllyHash*, u8**, int*);

  /* Store a chunk. */
  int (*xPutChunk)(DoltliteRemote*, const ProllyHash*, const u8*, int);

  /* Batch existence check. Sets aResult[i]=1 if hash exists, 0 if not. */
  int (*xHasChunks)(DoltliteRemote*, const ProllyHash*, int nHash, u8 *aResult);

  /* Get/set serialized refs blob (branches, tags, remotes). */
  int (*xGetRefs)(DoltliteRemote*, u8**, int*);
  int (*xSetRefs)(DoltliteRemote*, const u8*, int);

  /* Flush any buffered writes. */
  int (*xCommit)(DoltliteRemote*);

  /* Close and free the remote. */
  void (*xClose)(DoltliteRemote*);
};

/*
** Sync chunks reachable from root hashes from source to destination.
** Uses BFS to walk the content-addressed DAG:
**   commits -> parent commits + catalog chunks
**   catalog chunks -> table root hashes
**   prolly tree nodes -> child node hashes (internal nodes only)
**
** Uses batch HasChunks to skip chunks the destination already has.
** When an entire subtree is already present, the BFS prunes it.
*/
int doltliteSyncChunks(
  ChunkStore *pSrc,        /* Source chunk store (local for push, remote for fetch) */
  DoltliteRemote *pDst,    /* Destination (remote for push, wraps local for fetch) */
  ProllyHash *aRoots,      /* Root hashes to start sync from */
  int nRoots               /* Number of root hashes */
);

/*
** Push a branch from local to remote.
** 1. Sync all chunks reachable from local branch commit
** 2. Update remote's branch ref to point to local commit
*/
int doltlitePush(ChunkStore *pLocal, DoltliteRemote *pRemote,
                 const char *zBranch, int bForce);

/*
** Fetch a branch from remote into local tracking branch.
** 1. Sync all chunks reachable from remote branch commit
** 2. Update local tracking branch (remote/branch)
*/
int doltliteFetch(ChunkStore *pLocal, DoltliteRemote *pRemote,
                  const char *zRemoteName, const char *zBranch);

/*
** Clone: copy ALL chunks and refs from remote to local.
*/
int doltliteClone(ChunkStore *pLocal, DoltliteRemote *pRemote);

/*
** Open a filesystem remote (another .doltlite file).
*/
DoltliteRemote *doltliteFsRemoteOpen(sqlite3_vfs *pVfs, const char *zPath);

#endif /* DOLTLITE_REMOTE_H */
