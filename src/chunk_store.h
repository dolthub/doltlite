

/* Single-file content-addressed chunk store.
**
** File layout, all integers little-endian:
**
**   [Manifest: 168 bytes at offset 0]
**     see chunk_store.c::csReadManifest for the offset table
**
**   [Chunk data region: manifest + chunk stream, before index]
**     each chunk: length_le32(4) + data(length)
**
**   [Sorted index: nChunks * CHUNK_INDEX_ENTRY_SIZE entries]
**     each entry: hash(20) + file_offset(8) + data_size(4)
**
**   [WAL region: iWalOffset to EOF]
**     append-only crash-safety log
**       chunk record: tag(0x01) + hash(20) + len_le32(4) + data(len)
**       root record:  tag(0x02) + manifest_snapshot(168)
*/
#ifndef SQLITE_CHUNK_STORE_H
#define SQLITE_CHUNK_STORE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

#define CHUNK_STORE_MAGIC 0x444C5443  /* "DLTC" little-endian */
#define CHUNK_STORE_VERSION 8
#define CHUNK_MANIFEST_SIZE 168
#define CHUNK_INDEX_ENTRY_SIZE 32     /* hash(20) + offset(8) + size(4) */

/* Working set blob format. v2 only has merge state; v3 adds rebase
** state alongside it (parallel to Dolt's working set, which
** contains optional MergeState and RebaseState substructures); v4
** adds a separate constraint-violations hash slot so FK / CHECK /
** UNIQUE violations detected at merge-time can persist alongside
** (and independently of) the row-level merge conflicts hash.
**
**   v2 layout (still readable for backward compat):
**     [version:1]
**     [working_catalog:20][working_commit:20][staged_hash:20]
**     [is_merging:1][merge_commit:20][conflicts:20]
**
**   v3 layout:
**     v2 fields, then:
**     [is_rebasing:1]
**     [pre_rebase_working_cat:20]
**     [rebase_onto_commit:20]
**     [rebase_orig_branch: WS_REBASE_BRANCH_LEN bytes, null-padded]
**
**   v4 layout (current write format):
**     v3 fields, then:
**     [constraint_violations:20]
*/
#define WS_FORMAT_VERSION_V2 2
#define WS_FORMAT_VERSION_V3 3
#define WS_FORMAT_VERSION_V4 4
#define WS_FORMAT_VERSION    WS_FORMAT_VERSION_V4
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
#define WS_CONSTRAINT_VIOLATIONS_OFF WS_TOTAL_SIZE_V3
#define WS_TOTAL_SIZE       (WS_CONSTRAINT_VIOLATIONS_OFF + PROLLY_HASH_SIZE)

/* Catalog (table registry) format:
**   magic(1) + nTables(4 LE) + entries (sorted by name)
** Per entry: iTable(4 LE) + flags(1) + root(20) + schema(20)
**          + nameLen(2 LE) + name
** Entries are sorted by name for deterministic content hashing. */
#define CATALOG_FORMAT_V3       0x44
#define CAT_HEADER_SIZE_V3      5
#define CAT_ENTRY_ITABLE_SIZE   4
#define CAT_ENTRY_FLAGS_SIZE    1
#define CAT_ENTRY_FIXED_SIZE    (CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE + 2)

static SQLITE_INLINE int catalogParseHeader(
  const u8 *data, int nData, int *pnTables, const u8 **ppEntries
){
  const u8 *q;
  if( nData < CAT_HEADER_SIZE_V3 ) return 0;
  if( data[0] != CATALOG_FORMAT_V3 ){
    return 0;
  }
  q = data + CAT_HEADER_SIZE_V3 - 4;
  *pnTables = (int)(q[0] | (q[1]<<8) | (q[2]<<16) | (q[3]<<24));
  *ppEntries = data + CAT_HEADER_SIZE_V3;
  return 1;
}

typedef struct ChunkStore ChunkStore;
typedef struct ChunkIndexEntry ChunkIndexEntry;
typedef struct ConflictEntry ConflictEntry;

struct ConflictEntry {
  u8 *pKey;
  int nKey;
  u8 *pBaseVal;
  int nBaseVal;
  u8 *pTheirVal;
  int nTheirVal;
};

struct ChunkIndexEntry {
  ProllyHash hash;
  /* File offset of the 4-byte length prefix, or negative for a
  ** WAL-encoded offset pointing into cs->pWalData / cs->pWriteBuf —
  ** see csEncodeWalOffset in chunk_store.c. */
  i64 offset;
  int size;
};

struct ChunkStore {
  char *zFilename;
  sqlite3_file *pFile;
  sqlite3_vfs *pVfs;
  ProllyHash refsHash;
  ProllyHash committedRefsHash;

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
    char *zTagger;
    char *zEmail;
    i64 timestamp;
    char *zMessage;
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
  i64 iFileSize;

  ChunkIndexEntry *aIndex;
  int nIndex;
  int nIndexAlloc;


  ChunkIndexEntry *aPending;
  int nPending;
  int nPendingAlloc;
  int *aPendingHT;
  int *aPendingHTNext;
  int nPendingHTBuilt;
  int nPendingHTNextAlloc;
  int nPendingHTSize;
  u8 *pWriteBuf;
  i64 nWriteBuf;
  i64 nWriteBufAlloc;

  u8 readOnly;
  u8 isMemory;
  u8 snapshotPinned;
  int graphLockFd;
  i64 nCommittedWriteBuf;


  u8 *pWalData;
  i64 nWalData;
};

int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags);

int chunkStoreClose(ChunkStore *cs);

int chunkStoreLockAndRefresh(ChunkStore *cs);
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
int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch);

int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData);

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut);

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult);

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash);

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
