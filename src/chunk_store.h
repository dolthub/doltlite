/*
** Content-addressed chunk store backed by a single file.
**
** File layout:
**   [Manifest: 168 bytes at offset 0]
**     magic(4) + version(4) + root_hash(20) + nChunks(4) +
**     index_offset(8) + index_size(4) + reserved(20) +
**     head_commit_hash(20) + wal_offset(8) + reserved(12) +
**     refs_hash(20) + working_state_hash(20) + reserved(24)
**   [Chunk data region: after manifest, before index]
**     Each chunk stored as: length_le32(4) + data(length)
**   [Sorted index: nChunks * 32-byte entries]
**     Each entry: hash(20) + file_offset(8) + data_size(4)
**   [WAL region: iWalOffset to EOF]
**     Append-only log of chunk and root records for crash safety.
**     chunk record: tag(0x01) + hash(20) + len_le32(4) + data(len)
**     root record:  tag(0x02) + manifest_snapshot(168)
*/

#ifndef SQLITE_CHUNK_STORE_H
#define SQLITE_CHUNK_STORE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

#define CHUNK_STORE_MAGIC 0x444C5443  /* "DLTC" in little-endian */
#define CHUNK_STORE_VERSION 7
#define CHUNK_MANIFEST_SIZE 168
#define CHUNK_INDEX_ENTRY_SIZE 32     /* 20-byte hash + 8-byte offset + 4-byte size */

/* Working set blob layout:
**   [version:1][staged_hash:20][is_merging:1][merge_commit:20][conflicts:20] */
#define WS_VERSION_SIZE     1
#define WS_STAGED_OFF       WS_VERSION_SIZE
#define WS_MERGING_OFF      (WS_STAGED_OFF + PROLLY_HASH_SIZE)
#define WS_MERGE_COMMIT_OFF (WS_MERGING_OFF + 1)
#define WS_CONFLICTS_OFF    (WS_MERGE_COMMIT_OFF + PROLLY_HASH_SIZE)
#define WS_TOTAL_SIZE       (WS_CONFLICTS_OFF + PROLLY_HASH_SIZE)

/* Catalog (table registry) binary format V2:
**   magic(1) + iNextTable(4 LE) + nTables(4 LE) + entries...
** Per entry: iTable(4 LE) + flags(1) + root(20) + schema(20) + nameLen(2 LE) + name */
#define CATALOG_FORMAT_V2       0x43
#define CAT_NEXT_TABLE_OFF      1
#define CAT_NUM_TABLES_OFF      5
#define CAT_HEADER_SIZE         9
#define CAT_ENTRY_ITABLE_SIZE   4
#define CAT_ENTRY_FLAGS_SIZE    1
#define CAT_ENTRY_FIXED_SIZE    (CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2)

typedef struct ChunkStore ChunkStore;
typedef struct ChunkIndexEntry ChunkIndexEntry;
typedef struct ConflictEntry ConflictEntry;

/* Three-way merge conflict. "Ours" is the current working row, not stored here. */
struct ConflictEntry {
  u8 *pKey;
  int nKey;
  u8 *pBaseVal;       /* Ancestor value (NULL if row didn't exist in base) */
  int nBaseVal;
  u8 *pTheirVal;      /* Their value (NULL if deleted by theirs) */
  int nTheirVal;
};

struct ChunkIndexEntry {
  ProllyHash hash;
  i64 offset;      /* File offset of the 4-byte length prefix, or negative for WAL-encoded offset */
  int size;        /* Data size (excludes the 4-byte length prefix) */
};

struct ChunkStore {
  char *zFilename;
  sqlite3_file *pFile;
  sqlite3_vfs *pVfs;
  ProllyHash root;
  ProllyHash headCommit;
  ProllyHash refsHash;
  ProllyHash workingState;  /* Per-branch working catalog state chunk hash */

  /* These fields come from the per-branch WorkingSet, not from the manifest. */
  ProllyHash stagedCatalog;
  u8 isMerging;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;

  struct BranchRef {
    char *zName;
    ProllyHash commitHash;
    ProllyHash workingSetHash;
  } *aBranches;
  int nBranches;
  char *zDefaultBranch;

  struct TagRef {
    char *zName;
    ProllyHash commitHash;
  } *aTags;
  int nTags;

  struct RemoteRef {
    char *zName;
    char *zUrl;
  } *aRemotes;
  int nRemotes;

  struct TrackingBranch {
    char *zRemote;
    char *zBranch;
    ProllyHash commitHash;
  } *aTracking;
  int nTracking;

  int nChunks;
  i64 iIndexOffset;
  int nIndexSize;
  i64 iWalOffset;
  i64 iFileSize;             /* Last-known file size; used to detect concurrent writers */

  ChunkIndexEntry *aIndex;   /* Committed index, sorted by hash for binary search */
  int nIndex;
  int nIndexAlloc;

  /* Pending (uncommitted) chunks. Offsets point into pWriteBuf at the length prefix. */
  ChunkIndexEntry *aPending;
  int nPending;
  int nPendingAlloc;
  int *aPendingHT;           /* Hash table buckets: bucket -> aPending index, -1 = empty */
  int *aPendingHTNext;       /* Collision chains: aPending index -> next index, -1 = end */
  int nPendingHTBuilt;       /* How many aPending entries are indexed in the hash table */
  int nPendingHTNextAlloc;
  int nPendingHTSize;        /* Bucket count (always a power of 2) */
  u8 *pWriteBuf;             /* Serialized pending chunks: [4-byte LE length][data]... */
  i64 nWriteBuf;
  i64 nWriteBufAlloc;

  u8 readOnly;
  u8 isMemory;
  u8 snapshotPinned;         /* When set, RefreshIfChanged becomes a no-op */
  int graphLockFd;           /* flock fd held during commit graph mutation, -1 if none */
  i64 nCommittedWriteBuf;    /* In-memory mode: committed portion of pWriteBuf (survives rollback) */

  /* Cached copy of the on-disk WAL region so chunk reads don't hit the file */
  u8 *pWalData;
  i64 nWalData;
};

int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags);

int chunkStoreClose(ChunkStore *cs);

/* Acquire exclusive file lock (non-blocking), refresh from disk.
** Call before modifying the commit graph. Returns SQLITE_BUSY if
** another connection holds the lock. Caller must call Unlock after. */
int chunkStoreLockAndRefresh(ChunkStore *cs);
void chunkStoreUnlock(ChunkStore *cs);

void chunkStoreGetRoot(ChunkStore *cs, ProllyHash *pRoot);

void chunkStoreSetRoot(ChunkStore *cs, const ProllyHash *pRoot);

void chunkStoreGetHeadCommit(ChunkStore *cs, ProllyHash *pHead);
void chunkStoreSetHeadCommit(ChunkStore *cs, const ProllyHash *pHead);

void chunkStoreGetWorkingState(ChunkStore *cs, ProllyHash *pState);
void chunkStoreSetWorkingState(ChunkStore *cs, const ProllyHash *pState);
int chunkStoreWriteBranchWorkingCatalog(ChunkStore *cs, const char *zBranch,
                                        const ProllyHash *pCatHash);

void chunkStoreGetStagedCatalog(ChunkStore *cs, ProllyHash *pStaged);
void chunkStoreSetStagedCatalog(ChunkStore *cs, const ProllyHash *pStaged);

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
int chunkStoreDeleteTag(ChunkStore *cs, const char *zName);
int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl);
int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName);
int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl);

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit);
int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit);
int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch);

int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData);

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut);

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult);

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash);

/* Caller must sqlite3_free(*ppData). Returns SQLITE_NOTFOUND if not present. */
int chunkStoreGet(ChunkStore *cs, const ProllyHash *hash,
                  u8 **ppData, int *pnData);

/* Data is buffered in memory until chunkStoreCommit. */
int chunkStorePut(ChunkStore *cs, const u8 *pData, int nData,
                  ProllyHash *pHash);

int chunkStoreCommit(ChunkStore *cs);

void chunkStoreRollback(ChunkStore *cs);

int chunkStoreIsEmpty(ChunkStore *cs);

const char *chunkStoreFilename(ChunkStore *cs);

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged);

int chunkStoreGetMergeState(ChunkStore *cs, u8 *pIsMerging,
                            ProllyHash *pMergeCommit,
                            ProllyHash *pConflictsCatalog);
void chunkStoreSetMergeState(ChunkStore *cs, u8 isMerging,
                             const ProllyHash *pMergeCommit,
                             const ProllyHash *pConflictsCatalog);
void chunkStoreClearMergeState(ChunkStore *cs);

void chunkStoreGetConflictsCatalog(ChunkStore *cs, ProllyHash *pHash);
void chunkStoreSetConflictsCatalog(ChunkStore *cs, const ProllyHash *pHash);

#endif /* SQLITE_CHUNK_STORE_H */
