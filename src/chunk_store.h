/*
** Single-file content-addressed chunk store.
**
** File layout:
**   [Manifest header: 168 bytes at offset 0]
**     magic(4) + version(4) + root_hash(20) + chunk_count(4) +
**     index_offset(8) + index_size(4) + catalog_hash(20) +
**     head_commit(20) + wal_offset(8) + reserved(12) + refs_hash(20) +
**     reserved(64)
**   [Compacted chunk data: offset 168 to iWalOffset]
**     length(4) + data(length), with sorted index
**   [WAL region: iWalOffset to EOF]
**     Append-only journal of chunk and root records
**     chunk record: tag(0x01) + hash(20) + len(4) + data
**     root record:  tag(0x02) + manifest(168)
**
** GC rewrites the file with all chunks compacted (empty WAL region).
** Normal commits append to the WAL region at EOF.
*/
#ifndef SQLITE_CHUNK_STORE_H
#define SQLITE_CHUNK_STORE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

/* Manifest magic */
#define CHUNK_STORE_MAGIC 0x444C5443  /* "DLTC" */
#define CHUNK_STORE_VERSION 6
#define CHUNK_MANIFEST_SIZE 168
#define CHUNK_INDEX_ENTRY_SIZE 32

/* WorkingSet layout constants */
#define WS_VERSION_SIZE     1
#define WS_STAGED_OFF       WS_VERSION_SIZE                           /* 1  */
#define WS_MERGING_OFF      (WS_STAGED_OFF + PROLLY_HASH_SIZE)       /* 21 */
#define WS_MERGE_COMMIT_OFF (WS_MERGING_OFF + 1)                     /* 22 */
#define WS_CONFLICTS_OFF    (WS_MERGE_COMMIT_OFF + PROLLY_HASH_SIZE) /* 42 */
#define WS_TOTAL_SIZE       (WS_CONFLICTS_OFF + PROLLY_HASH_SIZE)    /* 62 */

typedef struct ChunkStore ChunkStore;
typedef struct ChunkIndexEntry ChunkIndexEntry;
typedef struct ConflictEntry ConflictEntry;

/*
** A single row-level conflict from a three-way merge.
** Stored inline: the key (rowid or PK blob), the base (ancestor) value,
** and the "their" value.  "Ours" is already the working row.
** Conflicts for each table are collected into a prolly tree keyed by
** the conflict key.  The conflict catalog maps table name → conflict
** tree root hash.
*/
struct ConflictEntry {
  u8 *pKey;           /* Primary key blob */
  int nKey;           /* Size of pKey */
  u8 *pBaseVal;       /* Ancestor row value (NULL if row didn't exist) */
  int nBaseVal;       /* Size of pBaseVal */
  u8 *pTheirVal;      /* Their row value (NULL if row was deleted) */
  int nTheirVal;      /* Size of pTheirVal */
};

struct ChunkIndexEntry {
  ProllyHash hash;
  i64 offset;     /* Byte offset in file */
  int size;       /* Chunk data size */
};

struct ChunkStore {
  char *zFilename;           /* Database file path */
  sqlite3_file *pFile;       /* OS file handle */
  sqlite3_vfs *pVfs;         /* VFS for file operations */
  ProllyHash root;           /* Current root hash */
  ProllyHash catalog;        /* Catalog hash (table registry) */
  ProllyHash headCommit;     /* HEAD commit hash */
  ProllyHash refsHash;       /* Hash of refs chunk (branch mapping) */

  /* Legacy fields — kept in struct for code that still reads them,
  ** but no longer persisted in manifest. Values come from WorkingSet. */
  ProllyHash stagedCatalog;
  u8 isMerging;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;

  /* Branch refs (in-memory, loaded from refs chunk) */
  struct BranchRef {
    char *zName;
    ProllyHash commitHash;
    ProllyHash workingSetHash;
  } *aBranches;
  int nBranches;
  char *zDefaultBranch;      /* Default branch for new connections */

  /* Tag refs */
  struct TagRef {
    char *zName;
    ProllyHash commitHash;
  } *aTags;
  int nTags;

  /* Remote refs */
  struct RemoteRef {
    char *zName;    /* e.g., "origin" */
    char *zUrl;     /* file path or URL */
  } *aRemotes;
  int nRemotes;

  /* Remote tracking branches (e.g., origin/main) */
  struct TrackingBranch {
    char *zRemote;  /* Remote name */
    char *zBranch;  /* Branch name */
    ProllyHash commitHash;
  } *aTracking;
  int nTracking;

  /* File layout */
  int nChunks;               /* Number of compacted chunks */
  i64 iIndexOffset;          /* File offset of chunk index */
  int nIndexSize;            /* Size of chunk index in bytes */
  i64 iWalOffset;            /* File offset where WAL region starts */
  i64 iFileSize;             /* Last known total file size */

  /* In-memory index (loaded on open) */
  ChunkIndexEntry *aIndex;   /* Sorted array of index entries */
  int nIndex;
  int nIndexAlloc;

  /* Write buffer for pending chunks */
  ChunkIndexEntry *aPending;
  int nPending;
  int nPendingAlloc;
  int *aPendingHT;           /* Hash table buckets for pending lookup */
  int *aPendingHTNext;       /* Next-chain for hash table */
  int nPendingHTBuilt;       /* Entries indexed so far */
  int nPendingHTNextAlloc;   /* Allocated size of aPendingHTNext */
  int nPendingHTSize;        /* Number of hash table buckets */
  u8 *pWriteBuf;
  i64 nWriteBuf;
  i64 nWriteBufAlloc;

  u8 readOnly;
  u8 isMemory;
  u8 snapshotPinned;         /* True while a read transaction holds a snapshot */
  i64 nCommittedWriteBuf;

  /* WAL region cache (read from file on open, updated on commit) */
  u8 *pWalData;              /* In-memory copy of WAL region */
  i64 nWalData;              /* Size of WAL region data */
};

/* Open or create a chunk store at the given path */
int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags);

/* Close the chunk store */
int chunkStoreClose(ChunkStore *cs);

/* Get the current root hash */
void chunkStoreGetRoot(ChunkStore *cs, ProllyHash *pRoot);

/* Set the root hash (staged, not committed until chunkStoreCommit) */
void chunkStoreSetRoot(ChunkStore *cs, const ProllyHash *pRoot);

/* Get/set the catalog hash (table registry + meta values) */
void chunkStoreGetCatalog(ChunkStore *cs, ProllyHash *pCat);
void chunkStoreSetCatalog(ChunkStore *cs, const ProllyHash *pCat);

/* Get/set the HEAD commit hash */
void chunkStoreGetHeadCommit(ChunkStore *cs, ProllyHash *pHead);
void chunkStoreSetHeadCommit(ChunkStore *cs, const ProllyHash *pHead);

/* Get/set the staged catalog hash */
void chunkStoreGetStagedCatalog(ChunkStore *cs, ProllyHash *pStaged);
void chunkStoreSetStagedCatalog(ChunkStore *cs, const ProllyHash *pStaged);

/* Reload refs (branches, tags, remotes, tracking) from the current refsHash */
int chunkStoreReloadRefs(ChunkStore *cs);

/* Branch management */
const char *chunkStoreGetDefaultBranch(ChunkStore *cs);
int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName);
int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName);
int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreSerializeRefs(ChunkStore *cs);

/* Per-branch WorkingSet management */
int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash);
int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash);

/* Tag management */
int chunkStoreAddTag(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteTag(ChunkStore *cs, const char *zName);
int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

/* Remote management */
int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl);
int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName);
int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl);

/* Tracking branch management */
int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit);
int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit);
int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch);

/* Parse a refs blob into cs->aBranches/aTags/aRemotes/aTracking (no file I/O) */
int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData);

/* Serialize cs->aBranches/aTags/aRemotes/aTracking into a blob (caller frees) */
int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut);

/* Bulk has-check for sync (check multiple hashes at once) */
int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult);

/* Check if a chunk exists */
int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash);

/* Read a chunk by hash. Caller must sqlite3_free(*ppData).
** Returns SQLITE_OK, SQLITE_NOTFOUND, or error. */
int chunkStoreGet(ChunkStore *cs, const ProllyHash *hash,
                  u8 **ppData, int *pnData);

/* Stage a new chunk for writing. Hash is computed from data.
** Returns the hash in *pHash. Data is buffered until commit. */
int chunkStorePut(ChunkStore *cs, const u8 *pData, int nData,
                  ProllyHash *pHash);

/* Commit all pending chunks and new root atomically.
** Writes chunks + updated index, fsyncs, updates manifest. */
int chunkStoreCommit(ChunkStore *cs);

/* Discard all pending chunks (rollback) */
void chunkStoreRollback(ChunkStore *cs);

/* Return 1 if store is empty (no root) */
int chunkStoreIsEmpty(ChunkStore *cs);

/* Get the filename */
const char *chunkStoreFilename(ChunkStore *cs);

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged);

/* Merge state accessors */
int chunkStoreGetMergeState(ChunkStore *cs, u8 *pIsMerging,
                            ProllyHash *pMergeCommit,
                            ProllyHash *pConflictsCatalog);
void chunkStoreSetMergeState(ChunkStore *cs, u8 isMerging,
                             const ProllyHash *pMergeCommit,
                             const ProllyHash *pConflictsCatalog);
void chunkStoreClearMergeState(ChunkStore *cs);

/* Get/set the conflicts catalog hash independently */
void chunkStoreGetConflictsCatalog(ChunkStore *cs, ProllyHash *pHash);
void chunkStoreSetConflictsCatalog(ChunkStore *cs, const ProllyHash *pHash);

#endif /* SQLITE_CHUNK_STORE_H */
