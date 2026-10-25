#ifndef CHUNK_STORE_INT_H
#define CHUNK_STORE_INT_H

#include "chunk_store.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct CsFileLockState *CsFileLock;
struct CsFileLockState {
  sqlite3_file *pFile;
  sqlite3_file *pGuard;
  char *zGuard;
  char *zPath;
  sqlite3_vfs *pVfs;
  int readOnly;
};
typedef struct CsReadLock CsReadLock;
struct CsReadLock {
  sqlite3_file *pFile;
  char *zName;
};
# define CS_FILE_LOCK_INIT 0
# define CS_GRAPH_LOCK(cs) ((cs)->pGraphLockFile)

#define CS_RECENT_FAST_PATH_MAX 16384
#define CS_WRITEBUF_RETAIN_MAX (64*1024)
#define CS_PENDING_DRAIN_LIMIT (64*1024*1024)


int csFileLock(sqlite3_vfs *pVfs, const char *path, int openFlags,
               CsFileLock *ppFile, char **pzName);
int csReadLock(ChunkStore *cs, CsReadLock *pLock);
int csReadUnlock(ChunkStore *cs, CsReadLock *pLock);
int csReclaimTail(ChunkStore *cs);
int csFileLockPromote(sqlite3_file *pFile);
int csFileLockOutput(sqlite3_file *pFile);
int csScanForCommittedRootEx(ChunkStore *cs, int *pEverCommitted,
                             int *pCreationRoot, int *pLaterBatch);
void csFileUnlock(CsFileLock pFile, char **pzName);
int csLockSidecarDelete(sqlite3_vfs *pVfs, const char *zPath);
int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs);
int csRestoreOrMergeLocalRefs(ChunkStore *cs, SavedRefsState *pSaved,
                              const ProllyHash *pSavedRefsHash,
                              const ProllyHash *pBaseRefsHash);
int csFileSizeByName(sqlite3_vfs *pVfs, const char *zPath, i64 *pSize);
int csDiskStateMatchesMemory(ChunkStore *cs);
int csOpenFile(sqlite3_vfs *pVfs, const char *zPath, sqlite3_file **ppFile,
               int flags, int *pOutFlags);
int csSyncFile(ChunkStore *cs);
int csScrubFailedRoot(ChunkStore *cs);
int csVerifyStoredHits(ChunkStore *cs);
void csFillChunkHdr(u8 *p, const ProllyHash *pHash, u32 size);
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
int csManifestHashStateOffsetless(const u8 *aBuf);
int csValidateWalRootManifest(const ChunkStore *cs, const u8 *aBuf,
                              i64 iRootOffset);
int csReadDiskRefsHash(ChunkStore *cs, ProllyHash *pOut);
void csCaptureDamageFinger(ChunkStore *cs);
int csDamageFingerDiffers(ChunkStore *cs);

#endif /* CHUNK_STORE_INT_H */
