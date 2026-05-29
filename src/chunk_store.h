

#ifndef SQLITE_CHUNK_STORE_H
#define SQLITE_CHUNK_STORE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_wal.h"
#include "chunk_refs.h"
#include "chunk_index.h"
#include "chunk_staging.h"
#include "chunk_file.h"

#define CHUNK_STORE_MAGIC 0x444C5443
#define CHUNK_STORE_VERSION 12
#define CHUNK_MANIFEST_SIZE 168
#define CHUNK_INDEX_ENTRY_SIZE 32

#define CS_READ_U32(p) (             \
  (u32)(((const u8*)(p))[0])       | \
  (u32)(((const u8*)(p))[1]) << 8  | \
  (u32)(((const u8*)(p))[2]) << 16 | \
  (u32)(((const u8*)(p))[3]) << 24   \
)

#define CS_WRITE_U32(p, v) do {      \
  ((u8*)(p))[0] = (u8)((v));        \
  ((u8*)(p))[1] = (u8)((v) >> 8);   \
  ((u8*)(p))[2] = (u8)((v) >> 16);  \
  ((u8*)(p))[3] = (u8)((v) >> 24);  \
} while(0)

#define CS_READ_I64(p) (                  \
  (i64)((u64)(((const u8*)(p))[0])      ) | \
  (i64)((u64)(((const u8*)(p))[1]) << 8 ) | \
  (i64)((u64)(((const u8*)(p))[2]) << 16) | \
  (i64)((u64)(((const u8*)(p))[3]) << 24) | \
  (i64)((u64)(((const u8*)(p))[4]) << 32) | \
  (i64)((u64)(((const u8*)(p))[5]) << 40) | \
  (i64)((u64)(((const u8*)(p))[6]) << 48) | \
  (i64)((u64)(((const u8*)(p))[7]) << 56)   \
)

#define CS_WRITE_I64(p, v) do {            \
  ((u8*)(p))[0] = (u8)((u64)(v));         \
  ((u8*)(p))[1] = (u8)((u64)(v) >> 8);    \
  ((u8*)(p))[2] = (u8)((u64)(v) >> 16);   \
  ((u8*)(p))[3] = (u8)((u64)(v) >> 24);   \
  ((u8*)(p))[4] = (u8)((u64)(v) >> 32);   \
  ((u8*)(p))[5] = (u8)((u64)(v) >> 40);   \
  ((u8*)(p))[6] = (u8)((u64)(v) >> 48);   \
  ((u8*)(p))[7] = (u8)((u64)(v) >> 56);   \
} while(0)

#define CS_MANIFEST_MAGIC_OFF        0
#define CS_MANIFEST_VERSION_OFF      4
#define CS_MANIFEST_CHUNK_COUNT_OFF  28
#define CS_MANIFEST_INDEX_OFFSET_OFF 32
#define CS_MANIFEST_INDEX_SIZE_OFF   40
#define CS_MANIFEST_WAL_OFFSET_OFF   84
#define CS_MANIFEST_REFS_HASH_OFF    104

#define CS_WAL_TAG_CHUNK  0x01
#define CS_WAL_TAG_ROOT   0x02
#define CS_WAL_CHUNK_HASH_OFF  1
#define CS_WAL_CHUNK_LEN_OFF   (1 + PROLLY_HASH_SIZE)
#define CS_WAL_CHUNK_HDR_SIZE  (1 + PROLLY_HASH_SIZE + 4)

#define WS_FORMAT_VERSION_V2 2
#define WS_FORMAT_VERSION_V3 3
#define WS_FORMAT_VERSION_V4 4
#define WS_FORMAT_VERSION_V5 5
#define WS_FORMAT_VERSION    WS_FORMAT_VERSION_V5
#define WS_VERSION_SIZE     1
#define WS_WORKING_CAT_OFF  WS_VERSION_SIZE
#define WS_WORKING_COMMIT_OFF (WS_WORKING_CAT_OFF + PROLLY_HASH_SIZE)
#define WS_STAGED_OFF       (WS_WORKING_COMMIT_OFF + PROLLY_HASH_SIZE)
#define WS_MERGING_OFF      (WS_STAGED_OFF + PROLLY_HASH_SIZE)
#define WS_MERGE_COMMIT_OFF (WS_MERGING_OFF + 1)
#define WS_CONFLICTS_OFF    (WS_MERGE_COMMIT_OFF + PROLLY_HASH_SIZE)
#define WS_TOTAL_SIZE_V2    (WS_CONFLICTS_OFF + PROLLY_HASH_SIZE)
#define WS_REBASING_OFF     WS_TOTAL_SIZE_V2
#define WS_PRE_REBASE_CAT_OFF (WS_REBASING_OFF + 1)
#define WS_REBASE_ONTO_OFF  (WS_PRE_REBASE_CAT_OFF + PROLLY_HASH_SIZE)
#define WS_REBASE_BRANCH_OFF (WS_REBASE_ONTO_OFF + PROLLY_HASH_SIZE)
#define WS_REBASE_BRANCH_LEN 64
#define WS_TOTAL_SIZE_V3    (WS_REBASE_BRANCH_OFF + WS_REBASE_BRANCH_LEN)
#define WS_CONSTRAINT_VIOLATIONS_OFF_V4 WS_TOTAL_SIZE_V3
#define WS_TOTAL_SIZE_V4    (WS_CONSTRAINT_VIOLATIONS_OFF_V4 + PROLLY_HASH_SIZE)
#define WS_REBASE_RETURN_BRANCH_OFF WS_TOTAL_SIZE_V4
#define WS_CONSTRAINT_VIOLATIONS_OFF (WS_REBASE_RETURN_BRANCH_OFF + WS_REBASE_BRANCH_LEN)
#define WS_TOTAL_SIZE       (WS_CONSTRAINT_VIOLATIONS_OFF + PROLLY_HASH_SIZE)

#define CATALOG_FORMAT_V3       0x44
#define CATALOG_FORMAT_V4       0x45
#define CAT_HEADER_SIZE_V3      5
#define CAT_ENTRY_ITABLE_SIZE   4
#define CAT_ENTRY_FLAGS_SIZE    1
#define CAT_ENTRY_FIXED_SIZE_V3 (CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2)
#define CAT_ENTRY_FIXED_SIZE_V4 (CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 6)

static SQLITE_INLINE int catalogParseHeaderEx(
  const u8 *data, int nData, int *pVersion, int *pnTables, const u8 **ppEntries
){
  const u8 *q;
  if( nData < CAT_HEADER_SIZE_V3 ) return 0;
  if( data[0] != CATALOG_FORMAT_V3 && data[0] != CATALOG_FORMAT_V4 ){
    return 0;
  }
  if( pVersion ) *pVersion = data[0];
  q = data + CAT_HEADER_SIZE_V3 - 4;
  *pnTables = (int)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  *ppEntries = data + CAT_HEADER_SIZE_V3;
  return 1;
}

typedef struct ChunkStore ChunkStore;

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#  define CHUNK_STORE_LE_PACKING 1
#else
#  define CHUNK_STORE_LE_PACKING 0
#endif

struct ChunkStore {
  ChunkFile file;
  RefsTable refs;
  ChunkIndex index;
  WalState wal;
  ChunkStaging staging;

  u8 readOnly;
  u8 isMemory;
  u8 snapshotPinned;
#ifdef _WIN32
  sqlite3_file *pGraphLockFile;
#else
  int graphLockFd;
#endif
  sqlite3_mutex *pLockMutex;
  int lockDepth;
};

int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags);

int chunkStoreClose(ChunkStore *cs);

int chunkStoreLockAndRefresh(ChunkStore *cs);
int chunkStoreLockAndRefreshChanged(ChunkStore *cs, int *pChanged);
void chunkStoreUnlock(ChunkStore *cs);
int chunkStoreHasExternalChanges(ChunkStore *cs, int *pChanged);

int chunkStoreWriteBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                        const ProllyHash *pCatHash,
                                        const ProllyHash *pCommitHash);
int chunkStoreReadBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                       ProllyHash *pCatHash,
                                       ProllyHash *pCommitHash);

int chunkStoreReloadRefs(ChunkStore *cs);

const char *chunkStoreGetDefaultBranch(ChunkStore *cs);
int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName);
int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName);
int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreSerializeRefs(ChunkStore *cs);

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash);
int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash);

int chunkStoreAddTag(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreAddTagFull(ChunkStore *cs, const char *zName, const ProllyHash *pCommit,
                         const char *zTagger, const char *zEmail,
                         i64 timestamp, const char *zMessage);
int chunkStoreDeleteTag(ChunkStore *cs, const char *zName);
int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl);
int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName);
int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl);

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit);
int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit);

int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData);

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut);

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult);

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash, int *pHas);

int chunkStoreGet(ChunkStore *cs, const ProllyHash *hash,
                  u8 **ppData, int *pnData);

int chunkStorePut(ChunkStore *cs, const u8 *pData, int nData,
                  ProllyHash *pHash);

int chunkStoreCommit(ChunkStore *cs);

void chunkStoreRollback(ChunkStore *cs);

int chunkStoreIsEmpty(ChunkStore *cs);

void chunkStoreClearRefs(ChunkStore *cs);

const char *chunkStoreFilename(ChunkStore *cs);

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged);

#endif
