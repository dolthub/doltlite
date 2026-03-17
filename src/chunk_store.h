/*
** File-backed content-addressed chunk store.
**
** File layout:
**   [Manifest header: 64 bytes]
**     magic(4) + version(4) + root_hash(20) + chunk_count(4) +
**     index_offset(8) + index_size(4) + reserved(20)
**   [Chunk data region: variable]
**     Chunks are appended sequentially. Each chunk:
**       length(4) + data(length)
**   [Chunk index: variable]
**     Sorted array of (hash(20) + offset(8) + size(4)) = 32 bytes each
**
** Writes are committed atomically via write-to-temp + fsync + rename.
*/
#ifndef SQLITE_CHUNK_STORE_H
#define SQLITE_CHUNK_STORE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

/* Manifest magic */
#define CHUNK_STORE_MAGIC 0x444C5443  /* "DLTC" */
#define CHUNK_STORE_VERSION 4
#define CHUNK_MANIFEST_SIZE 124
#define CHUNK_INDEX_ENTRY_SIZE 32

typedef struct ChunkStore ChunkStore;
typedef struct ChunkIndexEntry ChunkIndexEntry;

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
  ProllyHash catalog;        /* Catalog hash (table registry + meta) */
  ProllyHash headCommit;     /* HEAD commit hash (linked list of commits) */
  ProllyHash stagedCatalog;  /* Staged catalog (tables added via dolt_add) */
  ProllyHash refsHash;       /* Hash of refs chunk (branch mapping) */

  /* Branch refs (in-memory, loaded from refs chunk) */
  struct BranchRef {
    char *zName;
    ProllyHash commitHash;
  } *aBranches;
  int nBranches;
  char *zDefaultBranch;      /* Default branch for new connections */

  /* Tag refs (in-memory, loaded from refs chunk) */
  struct TagRef {
    char *zName;
    ProllyHash commitHash;
  } *aTags;
  int nTags;
  int nChunks;               /* Number of chunks in store */
  i64 iIndexOffset;          /* File offset of chunk index */
  int nIndexSize;            /* Size of chunk index in bytes */
  i64 iAppendOffset;         /* Next write position for chunks */

  /* In-memory index (loaded on open) */
  ChunkIndexEntry *aIndex;   /* Sorted array of index entries */
  int nIndex;                /* Number of index entries */
  int nIndexAlloc;           /* Allocated capacity */

  /* Write buffer for pending chunks */
  ChunkIndexEntry *aPending; /* Pending new chunks */
  int nPending;
  int nPendingAlloc;
  u8 *pWriteBuf;             /* Buffer for pending chunk data */
  int nWriteBuf;
  int nWriteBufAlloc;

  u8 readOnly;               /* True if opened read-only */
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

/* Branch management */
const char *chunkStoreGetDefaultBranch(ChunkStore *cs);
int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName);
int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName);
int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreSerializeRefs(ChunkStore *cs);

/* Tag management */
int chunkStoreAddTag(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteTag(ChunkStore *cs, const char *zName);
int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

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

#endif /* SQLITE_CHUNK_STORE_H */
