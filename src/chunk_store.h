

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
/* Checkpoint is ordinary WAL chunks; unknown stamps replay them. */
#define CS_MANIFEST_CHECKPOINT_MAGIC_OFF 8
#define CS_MANIFEST_CHECKPOINT_OFFSET_OFF 12
#define CS_MANIFEST_CHECKPOINT_SIZE_OFF 20
#define CS_MANIFEST_CHUNK_COUNT_OFF  28
#define CS_MANIFEST_INDEX_OFFSET_OFF 32
#define CS_MANIFEST_INDEX_SIZE_OFF   40
/* Root commit metadata (zero in legacy / offset-0 header):
**   DURABLE_TO: valid prefix at batch start. Damage below is mid-stream
**     corruption; at/above every sealed DURABLE_TO is a torn tail.
**   BATCH_START: first byte of the batch. Without powersafe-overwrite this
**     is a fresh sector, so it can sit past DURABLE_TO (unwritten gap).
**   NEXT_OFF: sector-aligned start of the next batch; replay skips the gap.
**   SELF_HASH: hash of the manifest with this field zeroed plus the root's
**     file offset. Nonzero-and-wrong is damage, not a commit point. */
#define CS_MANIFEST_DURABLE_TO_OFF   44
#define CS_MANIFEST_NEXT_OFF_OFF     52
#define CS_MANIFEST_BATCH_START_OFF  60
#define CS_MANIFEST_CHECKPOINT_REPLAY_OFF 68
#define CS_MANIFEST_CHECKPOINT_DATA_END_OFF 76
#define CS_MANIFEST_WAL_OFFSET_OFF   84
#define CS_MANIFEST_REFS_HASH_OFF    104
#define CS_MANIFEST_SELF_HASH_OFF    124
#define CS_MANIFEST_CHECKPOINT_HASH_OFF 144
#define CS_MANIFEST_CHECKPOINT_COUNT_OFF 164

#define CS_MANIFEST_HASH_LEGACY 0
#define CS_MANIFEST_HASH_OK     1
#define CS_MANIFEST_HASH_BAD    2

#define CS_WAL_TAG_CHUNK  0x01
#define CS_WAL_TAG_ROOT   0x02
#define CS_WAL_CHECKPOINT_MAGIC_V1 0x31504b43
#define CS_WAL_CHECKPOINT_MAGIC_V2 0x32504b43
#define CS_WAL_CHUNK_HASH_OFF  1
#define CS_WAL_CHUNK_LEN_OFF   (1 + PROLLY_HASH_SIZE)
#define CS_WAL_CHUNK_HDR_SIZE  (1 + PROLLY_HASH_SIZE + 4)

#define CS_INDEX_PAGE_LEAF_MAGIC 0x314c5049
#define CS_INDEX_PAGE_INTERNAL_MAGIC 0x31495049
#define CS_INDEX_PAGE_SIZE 4096
#define CS_INDEX_PAGE_HEADER_SIZE 8
#define CS_INDEX_CHILD_SIZE (PROLLY_HASH_SIZE + 8 + 4 + PROLLY_HASH_SIZE)

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
/* Bit 0: rebase in progress. Bit 1: return-branch is a metadata overlay
** (must not replace a dirty catalog). Keep the v5 length; GC uses it. */
#define WS_REBASE_FLAG_ACTIVE      0x01
#define WS_REBASE_FLAG_META_MIRROR 0x02
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

int chunkStoreValidateWorkingSetBlob(const u8 *data, int nData);

#define CATALOG_FORMAT_V3       0x44
#define CATALOG_FORMAT_V4       0x45
#define CATALOG_FORMAT_V5       0x46
#define CAT_HEADER_SIZE_V3      5
#define CAT_HEADER_META_SIZE_V5 8
#define CAT_HEADER_SIZE_V5      (CAT_HEADER_SIZE_V3 + CAT_HEADER_META_SIZE_V5)
#define CAT_ENTRY_ITABLE_SIZE   4
#define CAT_ENTRY_FLAGS_SIZE    1
#define CAT_ENTRY_FIXED_SIZE_V3 (CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2)
#define CAT_ENTRY_FIXED_SIZE_V4 (CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 6)

static SQLITE_INLINE int catalogParseHeaderEx(
  const u8 *data, int nData, int *pVersion, int *pnTables, const u8 **ppEntries
){
  const u8 *q;
  if( nData < CAT_HEADER_SIZE_V3 ) return 0;
  if( data[0] != CATALOG_FORMAT_V3
   && data[0] != CATALOG_FORMAT_V4
   && data[0] != CATALOG_FORMAT_V5 ){
    return 0;
  }
  if( data[0]==CATALOG_FORMAT_V5 && nData < CAT_HEADER_SIZE_V5 ) return 0;
  if( pVersion ) *pVersion = data[0];
  q = data + CAT_HEADER_SIZE_V3 - 4;
  *pnTables = (int)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  *ppEntries = data + (data[0]==CATALOG_FORMAT_V5 ?
                       CAT_HEADER_SIZE_V5 : CAT_HEADER_SIZE_V3);
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
  u8 bRefsStale;          /* OOM cleared refs; reload from refsHash */
  ChunkIndex index;
  WalState wal;
  ChunkStaging staging;

  u8 readOnly;

  /* Path renamed/unlinked with no replacement: writes refused, reads stay
  ** on the open handle. Re-evaluated on refresh. */
  u8 movedReadOnly;

  /* One-shot: adopt the path on next refresh. Only the installer may set
  ** this (backup dest is replaced on purpose). */
  u8 adoptReplacement;
  u8 isMemory;
  u8 isBuffer;
  u8 fullFsync;           /* PRAGMA fullfsync -> SQLITE_SYNC_FULL */
  u8 noSync;              /* PRAGMA synchronous=OFF: skip syncs */
  u8 snapshotPinned;
  /* Bumped on every full reload-from-disk. Session-level state caches
  ** (catalog, working roots) are only valid while this is unchanged. */
  u32 reloadGen;
  u8 corruptMidStream;    /* Mid-stream WAL damage: reads/commits CORRUPT;
                          ** open still succeeds (stock surfaces on first use) */
  u8 notADatabase;        /* Wrong/missing magic. Open succeeds; first use
                          ** is SQLITE_NOTADB and does not overwrite (misc5-4.1). */
  sqlite3_file *pGraphLockFile;
  char *pGraphLockName;        /* owned name for pGraphLockFile (xOpen) */
  sqlite3_mutex *pLockMutex;
  int lockDepth;

  /* Block checkpoint reentry via VFS write hooks. */
  int checkpointActive;
  sqlite3_vfs *pOwnedVfs;

};

void csManifestSeal(u8 *aBuf, i64 iOffset);
int csManifestHashState(const u8 *aBuf, i64 iOffset);

int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags);

int chunkStoreClose(ChunkStore *cs);

int csReadSliced(ChunkStore *cs, void *pBuf, i64 nByte, i64 iOff);

int chunkStoreLockAndRefresh(ChunkStore *cs);
int chunkStoreLockAndRefreshChanged(ChunkStore *cs, int *pChanged);
void chunkStoreUnlock(ChunkStore *cs);

/* Memory/buffer stores have no peer; lock calls are no-ops (lockDepth 0). */
#define PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs) do{ \
  assert( (cs)!=0 ); \
  assert( (cs)->isMemory || (cs)->isBuffer \
       || ((cs)->pGraphLockFile!=0 && (cs)->lockDepth>0) ); \
}while(0)
int chunkStoreHasExternalChanges(ChunkStore *cs, int *pChanged);

int chunkStoreReadBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                       ProllyHash *pCatHash,
                                       ProllyHash *pCommitHash);

int chunkStoreReloadRefs(ChunkStore *cs);

const char *chunkStoreGetDefaultBranch(ChunkStore *cs);
int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName);
int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName);
int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
int chunkStoreReadDiskBranchTip(ChunkStore *cs, const char *zName,
                                ProllyHash *pTip, int *pFound);
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

int chunkStoreInstallRefsBlob(ChunkStore *cs, const u8 *data, int nData);

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut);

typedef struct ChunkStoreRefsSnapshot ChunkStoreRefsSnapshot;
struct ChunkStoreRefsSnapshot {
  SavedRefsState state;
  ProllyHash refsHash;
  ProllyHash committedRefsHash;
};
int chunkStoreSnapshotRefs(ChunkStore *cs, ChunkStoreRefsSnapshot *pSnapshot);
void chunkStoreRestoreRefsSnapshot(
  ChunkStore *cs,
  ChunkStoreRefsSnapshot *pSnapshot
);
void chunkStoreDiscardRefsSnapshot(ChunkStoreRefsSnapshot *pSnapshot);

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult);

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash, int *pHas);

int chunkStoreGet(ChunkStore *cs, const ProllyHash *hash,
                  u8 **ppData, int *pnData);
int chunkStoreGetSparse(ChunkStore *cs, const ProllyHash *hash,
                        u8 **ppData, int *pnData, int *pnDataPhys);

int chunkStorePut(ChunkStore *cs, const u8 *pData, int nData,
                  ProllyHash *pHash);
int chunkStorePutSparse(ChunkStore *cs, const u8 *pPrefix, int nPrefix,
                        i64 nZeroTail, ProllyHash *pHash);

int chunkStoreCommit(ChunkStore *cs);

int chunkStoreCopyIntoEmpty(ChunkStore *pSrc, ChunkStore *pDest);
int chunkStoreCopyIntoEmptyNoCommit(ChunkStore *pSrc, ChunkStore *pDest);

void chunkStoreRollback(ChunkStore *cs);
int chunkStoreEnsureRefsFresh(ChunkStore *cs);

int chunkStoreIsEmpty(ChunkStore *cs);

void chunkStoreClearRefs(ChunkStore *cs);

const char *chunkStoreFilename(ChunkStore *cs);

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged);

/* Reload refs for a VC write, bypassing the size/HAS_MOVED heuristic
** (WAL reuse can hide a peer commit). Outermost lock only. */
int chunkStoreForceRefresh(ChunkStore *cs);

/* Duplicate z with two trailing nuls (SQLITE_OPEN_MAIN_DB xOpen URI term). */
int chunkStoreDupFilenameDoubleNul(const char *z, char **pzOut);

#endif
