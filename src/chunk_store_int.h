#ifndef CHUNK_STORE_INT_H
#define CHUNK_STORE_INT_H

#include "chunk_store.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef sqlite3_file *CsFileLock;
# define CS_FILE_LOCK_INIT 0
# define CS_GRAPH_LOCK(cs) ((cs)->pGraphLockFile)

#define CS_RECENT_FAST_PATH_MAX 16384
#define CS_WRITEBUF_RETAIN_MAX (64*1024)
#define CS_PENDING_DRAIN_LIMIT (64*1024*1024)


int csFileLock(sqlite3_vfs *pVfs, const char *path,
               sqlite3_file **ppFile, char **pzName);
int csFileLockPromote(sqlite3_file *pFile);
void csFileUnlock(sqlite3_file *pFile, char **pzName);
int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs);
int csRestoreOrMergeLocalRefs(ChunkStore *cs, SavedRefsState *pSaved,
                              const ProllyHash *pSavedRefsHash,
                              const ProllyHash *pBaseRefsHash);
int csFileSizeByName(sqlite3_vfs *pVfs, const char *zPath, i64 *pSize);
int csDiskStateMatchesMemory(ChunkStore *cs);
int csOpenFile(sqlite3_vfs *pVfs, const char *zPath, sqlite3_file **ppFile,
               int flags, int *pOutFlags);
int csSyncFile(ChunkStore *cs);
void csFillChunkHdr(u8 *p, const ProllyHash *pHash, u32 size);
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
int csManifestHashStateOffsetless(const u8 *aBuf);
int csValidateWalRootManifest(const ChunkStore *cs, const u8 *aBuf,
                              i64 iRootOffset);
int csReadDiskRefsHash(ChunkStore *cs, ProllyHash *pOut);

#endif /* CHUNK_STORE_INT_H */
