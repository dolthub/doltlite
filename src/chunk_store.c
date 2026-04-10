/*
** Implementation of the single-file content-addressed chunk store.
** See chunk_store.h for the file layout. All integers on disk are little-endian.
*/

#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>

#ifdef _WIN32
# include <io.h>
# include <windows.h>
  static int csFileLock(const char *path, int *pFd){
    int fd = _open(path, _O_BINARY | _O_RDWR | _O_CREAT, 0644);
    if( fd < 0 ) return -1;
    {
      HANDLE h = (HANDLE)_get_osfhandle(fd);
      OVERLAPPED ov = {0};
      if( !LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK, 0, MAXDWORD, MAXDWORD, &ov) ){
        _close(fd);
        return -1;
      }
    }
    *pFd = fd;
    return 0;
  }
  static void csFileUnlock(int fd){
    if( fd >= 0 ){
      HANDLE h = (HANDLE)_get_osfhandle(fd);
      OVERLAPPED ov = {0};
      UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov);
      _close(fd);
    }
  }
  static int csFileLockNB(const char *path, int *pFd){
    int fd = _open(path, _O_BINARY | _O_RDWR | _O_CREAT, 0644);
    if( fd < 0 ) return -1;
    {
      HANDLE h = (HANDLE)_get_osfhandle(fd);
      OVERLAPPED ov = {0};
      if( !LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                       0, MAXDWORD, MAXDWORD, &ov) ){
        _close(fd);
        return -1;
      }
    }
    *pFd = fd;
    return 0;
  }
#else
# include <unistd.h>
# include <sys/file.h>
  static int csFileLock(const char *path, int *pFd){
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if( fd < 0 ) return -1;
    if( flock(fd, LOCK_EX) != 0 ){
      close(fd);
      return -1;
    }
    *pFd = fd;
    return 0;
  }
  static void csFileUnlock(int fd){
    if( fd >= 0 ) close(fd);
  }
  static int csFileLockNB(const char *path, int *pFd){
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if( fd < 0 ) return -1;
    if( flock(fd, LOCK_EX | LOCK_NB) != 0 ){
      close(fd);
      return -1;
    }
    *pFd = fd;
    return 0;
  }
#endif

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

static int csOpenFile(sqlite3_vfs *pVfs, const char *zPath,
                      sqlite3_file **ppFile, int flags);
static void csCloseFile(sqlite3_file *pFile);
static int csReadManifest(ChunkStore *cs);
static int csReadIndex(ChunkStore *cs);
static int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData);
static int csSearchIndex(const ChunkIndexEntry *aIdx, int nIdx,
                         const ProllyHash *pHash);
static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash);
static int csIndexEntryCmp(const void *a, const void *b);
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
static void csSerializeIndexEntry(const ChunkIndexEntry *e, u8 *aBuf);
static void csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e);
static int csMergeIndex(ChunkStore *cs, ChunkIndexEntry **ppMerged,
                        int *pnMerged);
static int csGrowPending(ChunkStore *cs);
static int csGrowWriteBuf(ChunkStore *cs, int nNeeded);

static int csReplayWalRegion(ChunkStore *cs, int updateManifest);
static int csReplayWal(ChunkStore *cs){ return csReplayWalRegion(cs, 1); }

#define CS_WAL_TAG_CHUNK  0x01
#define CS_WAL_TAG_ROOT   0x02

/*
** WAL offsets are stored as negative values in ChunkIndexEntry.offset to
** distinguish them from regular file offsets. The encoding maps walPos 0
** to -1, 1 to -2, etc., so that 0 remains available as "no offset."
** A negative offset means the chunk data lives in the in-memory pWalData
** buffer rather than at a direct file position.
*/
static i64 csEncodeWalOffset(i64 walPos){ return -(walPos) - 1; }
static i64 csDecodeWalOffset(i64 encoded){ return -(encoded + 1); }
static int csIsWalOffset(i64 offset){ return offset < 0; }

static void csFreeBranches(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nBranches; k++) sqlite3_free(cs->aBranches[k].zName);
  sqlite3_free(cs->aBranches);
  cs->aBranches = 0;
  cs->nBranches = 0;
}
static void csFreeTags(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nTags; k++) sqlite3_free(cs->aTags[k].zName);
  sqlite3_free(cs->aTags);
  cs->aTags = 0;
  cs->nTags = 0;
}
static void csFreeRemotes(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nRemotes; k++){
    sqlite3_free(cs->aRemotes[k].zName);
    sqlite3_free(cs->aRemotes[k].zUrl);
  }
  sqlite3_free(cs->aRemotes);
  cs->aRemotes = 0;
  cs->nRemotes = 0;
}
static void csFreeTracking(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nTracking; k++){
    sqlite3_free(cs->aTracking[k].zRemote);
    sqlite3_free(cs->aTracking[k].zBranch);
  }
  sqlite3_free(cs->aTracking);
  cs->aTracking = 0;
  cs->nTracking = 0;
}

#define CS_INIT_INDEX_ALLOC   64
#define CS_INIT_PENDING_ALLOC 16
#define CS_INIT_WRITEBUF_SIZE 4096

static int csOpenFile(
  sqlite3_vfs *pVfs,
  const char *zPath,
  sqlite3_file **ppFile,
  int flags
){
  int rc;
  int outFlags = 0;
  rc = sqlite3OsOpenMalloc(pVfs, zPath, ppFile, flags, &outFlags);
  return rc;
}

static void csCloseFile(sqlite3_file *pFile){
  if( pFile ){
    sqlite3OsCloseFree(pFile);
  }
}

static int csSearchIndex(
  const ChunkIndexEntry *aIdx,
  int nIdx,
  const ProllyHash *pHash
){
  int lo = 0;
  int hi = nIdx - 1;
  while( lo <= hi ){
    int mid = lo + (hi - lo) / 2;
    int cmp = prollyHashCompare(&aIdx[mid].hash, pHash);
    if( cmp == 0 ) return mid;
    if( cmp < 0 ){
      lo = mid + 1;
    }else{
      hi = mid - 1;
    }
  }
  return -1;
}

#define CS_PEND_HT_INIT_BITS 12
#define CS_PEND_HT_MAX_LOAD  4   /* max entries per bucket before resize */

static u32 csPendBucket(const ProllyHash *h, int nHTMask){
  return ((u32)h->data[0] | ((u32)h->data[1]<<8)
        | ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24)) & (u32)nHTMask;
}

static void csPendHTClear(ChunkStore *cs){
  sqlite3_free(cs->aPendingHT);
  sqlite3_free(cs->aPendingHTNext);
  cs->aPendingHT = 0;
  cs->aPendingHTNext = 0;
  cs->nPendingHTBuilt = 0;
  cs->nPendingHTSize = 0;
}

/* Fill aPendingHT with 0xff (-1 as int) to mark all buckets empty. */
static int csPendHTRebuild(ChunkStore *cs){
  int i;
  memset(cs->aPendingHT, 0xff, cs->nPendingHTSize * sizeof(int));
  for(i=0; i<cs->nPending; i++){
    u32 b = csPendBucket(&cs->aPending[i].hash, cs->nPendingHTSize - 1);
    cs->aPendingHTNext[i] = cs->aPendingHT[b];
    cs->aPendingHT[b] = i;
  }
  cs->nPendingHTBuilt = cs->nPending;
  return SQLITE_OK;
}

static int csPendHTEnsure(ChunkStore *cs){
  int i;
  if( cs->nPending==0 ) return SQLITE_OK;
  if( !cs->aPendingHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->aPendingHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->aPendingHT ) return SQLITE_NOMEM;
    memset(cs->aPendingHT, 0xff, initSize * sizeof(int));
    cs->nPendingHTSize = initSize;
    cs->nPendingHTBuilt = 0;
  }
  if( cs->nPending > cs->nPendingHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->nPendingHTSize * 4;
    int *aNew = sqlite3_realloc(cs->aPendingHT, newSize * (int)sizeof(int));
    if( aNew ){
      cs->aPendingHT = aNew;
      cs->nPendingHTSize = newSize;
      if( !cs->aPendingHTNext || cs->nPendingAlloc > cs->nPendingHTNextAlloc ){
        int nAlloc = cs->nPendingAlloc > 0 ? cs->nPendingAlloc : 64;
        int *aNew2 = sqlite3_realloc(cs->aPendingHTNext, nAlloc*(int)sizeof(int));
        if( !aNew2 ) return SQLITE_NOMEM;
        cs->aPendingHTNext = aNew2;
        cs->nPendingHTNextAlloc = nAlloc;
      }
      return csPendHTRebuild(cs);
    }
    /* realloc failed; fall through to incremental insert with old table size */
  }
  if( !cs->aPendingHTNext || cs->nPendingAlloc > cs->nPendingHTNextAlloc ){
    int nAlloc = cs->nPendingAlloc > 0 ? cs->nPendingAlloc : 64;
    int *aNew = sqlite3_realloc(cs->aPendingHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aPendingHTNext = aNew;
    cs->nPendingHTNextAlloc = nAlloc;
  }
  /* Incrementally insert only the entries added since the last ensure. */
  for(i=cs->nPendingHTBuilt; i<cs->nPending; i++){
    u32 b = csPendBucket(&cs->aPending[i].hash, cs->nPendingHTSize - 1);
    cs->aPendingHTNext[i] = cs->aPendingHT[b];
    cs->aPendingHT[b] = i;
  }
  cs->nPendingHTBuilt = cs->nPending;
  return SQLITE_OK;
}

static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash){
  int i; u32 b;
  if( cs->nPending==0 ) return -1;
  if( csPendHTEnsure(cs)!=SQLITE_OK ){
    /* Hash table alloc failed; fall back to linear scan. */
    for(i=0; i<cs->nPending; i++){
      if( prollyHashCompare(&cs->aPending[i].hash, pHash)==0 ) return i;
    }
    return -1;
  }
  b = csPendBucket(pHash, cs->nPendingHTSize - 1);
  i = cs->aPendingHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->aPending[i].hash, pHash)==0 ) return i;
    i = cs->aPendingHTNext[i];
  }
  return -1;
}

/*
** Manifest layout (168 bytes, all little-endian):
**   0: magic(4) | 4: version(4) | 8: reserved(20) | 28: nChunks(4)
**   32: indexOffset(8) | 40: indexSize(4) | 44: reserved(20)
**   64: reserved(20) | 84: walOffset(8) | 92: reserved(12)
**   104: refs_hash(20) | 124: working_state_hash(20) | 144: reserved(24)
*/
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + 0, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + 4, CHUNK_STORE_VERSION);
  /* offset 8: reserved (was root_hash) */
  CS_WRITE_U32(aBuf + 28, (u32)cs->nChunks);
  CS_WRITE_I64(aBuf + 32, cs->iIndexOffset);
  CS_WRITE_U32(aBuf + 40, (u32)cs->nIndexSize);
  /* offset 44: reserved (was catalog_hash, removed in v7) */
  /* offset 64: reserved (was headCommit_hash) */
  CS_WRITE_I64(aBuf + 84, cs->iWalOffset);
  memcpy(aBuf + 104, cs->refsHash.data, PROLLY_HASH_SIZE);
  memcpy(aBuf + 124, cs->workingState.data, PROLLY_HASH_SIZE);
}

static void csSerializeIndexEntry(const ChunkIndexEntry *e, u8 *aBuf){
  memcpy(aBuf, e->hash.data, PROLLY_HASH_SIZE);
  CS_WRITE_I64(aBuf + PROLLY_HASH_SIZE, e->offset);
  CS_WRITE_U32(aBuf + PROLLY_HASH_SIZE + 8, (u32)e->size);
}

static void csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e){
  memcpy(e->hash.data, aBuf, PROLLY_HASH_SIZE);
  e->offset = CS_READ_I64(aBuf + PROLLY_HASH_SIZE);
  e->size = (int)CS_READ_U32(aBuf + PROLLY_HASH_SIZE + 8);
}

static int csReadManifest(ChunkStore *cs){
  u8 aBuf[CHUNK_MANIFEST_SIZE];
  u32 magic, version;
  int rc;

  rc = sqlite3OsRead(cs->pFile, aBuf, CHUNK_MANIFEST_SIZE, 0);
  if( rc != SQLITE_OK ) return rc;

  magic = CS_READ_U32(aBuf + 0);
  version = CS_READ_U32(aBuf + 4);
  if( magic != CHUNK_STORE_MAGIC ) return SQLITE_NOTADB;
  if( version != CHUNK_STORE_VERSION ) return SQLITE_NOTADB;

  /* offset 8: reserved (was root_hash) */
  cs->nChunks = (int)CS_READ_U32(aBuf + 28);
  cs->iIndexOffset = CS_READ_I64(aBuf + 32);
  cs->nIndexSize = (int)CS_READ_U32(aBuf + 40);
  /* offset 44: reserved (was catalog_hash, removed in v7) */
  /* offset 64: reserved (was headCommit_hash) */
  cs->iWalOffset = CS_READ_I64(aBuf + 84);
  memcpy(cs->refsHash.data, aBuf + 104, PROLLY_HASH_SIZE);
  memcpy(cs->workingState.data, aBuf + 124, PROLLY_HASH_SIZE);

  return SQLITE_OK;
}

static int csReadIndex(ChunkStore *cs){
  int rc;
  int nEntries;
  u8 *aBuf;
  int i;

  if( cs->nIndexSize == 0 || cs->nChunks == 0 ){
    cs->nIndex = 0;
    return SQLITE_OK;
  }

  nEntries = cs->nIndexSize / CHUNK_INDEX_ENTRY_SIZE;
  if( nEntries * CHUNK_INDEX_ENTRY_SIZE != cs->nIndexSize ){
    return SQLITE_CORRUPT;
  }

  cs->aIndex = (ChunkIndexEntry *)sqlite3_malloc(
    nEntries * (int)sizeof(ChunkIndexEntry)
  );
  if( cs->aIndex == 0 ) return SQLITE_NOMEM;
  cs->nIndex = nEntries;
  cs->nIndexAlloc = nEntries;

  aBuf = (u8 *)sqlite3_malloc(cs->nIndexSize);
  if( aBuf == 0 ){
    sqlite3_free(cs->aIndex);
    cs->aIndex = 0;
    cs->nIndex = 0;
    return SQLITE_NOMEM;
  }

  rc = sqlite3OsRead(cs->pFile, aBuf, cs->nIndexSize, cs->iIndexOffset);
  if( rc != SQLITE_OK ){
    sqlite3_free(aBuf);
    sqlite3_free(cs->aIndex);
    cs->aIndex = 0;
    cs->nIndex = 0;
    return rc;
  }

  for( i = 0; i < nEntries; i++ ){
    csDeserializeIndexEntry(aBuf + i * CHUNK_INDEX_ENTRY_SIZE, &cs->aIndex[i]);
  }

  sqlite3_free(aBuf);
  return SQLITE_OK;
}

static int csGrowPending(ChunkStore *cs){
  if( cs->nPending >= cs->nPendingAlloc ){
    int nNew = cs->nPendingAlloc ? cs->nPendingAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->aPending, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->aPending = aNew;
    cs->nPendingAlloc = nNew;
  }
  return SQLITE_OK;
}

static int csGrowWriteBuf(ChunkStore *cs, int nNeeded){
  i64 nRequired = cs->nWriteBuf + (i64)nNeeded;
  if( nRequired > cs->nWriteBufAlloc ){
    i64 nNew = cs->nWriteBufAlloc ? cs->nWriteBufAlloc : CS_INIT_WRITEBUF_SIZE;
    u8 *pNew;
    /* Double below 64MB, then grow by 1.5x to limit wasted memory. */
    while( nNew < nRequired ){
      if( nNew < 64*1024*1024 ){
        nNew *= 2;
      }else{
        nNew += nNew / 2;
      }
    }
    pNew = (u8 *)sqlite3_realloc64(cs->pWriteBuf, (sqlite3_uint64)nNew);
    if( pNew == 0 ) return SQLITE_NOMEM;
    cs->pWriteBuf = pNew;
    cs->nWriteBufAlloc = nNew;
  }
  return SQLITE_OK;
}

/*
** Replay the WAL region (everything from iWalOffset to EOF).
** WAL records are: [tag:1][payload...].
**   CHUNK record: tag(0x01) + hash(20) + len_le32(4) + data(len)
**   ROOT record:  tag(0x02) + manifest_snapshot(168)
** Chunks found here are merged into the in-memory index with WAL-encoded
** (negative) offsets pointing into the cached pWalData buffer.
*/
static int csReplayWalRegion(ChunkStore *cs, int updateManifest){
  i64 walSize;
  u8 *walData;
  i64 pos;

  if( cs->iWalOffset <= 0 || !cs->pFile ) return SQLITE_OK;

  {
    i64 fileSize = 0;
    int rc = sqlite3OsFileSize(cs->pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
    walSize = fileSize - cs->iWalOffset;
    cs->iFileSize = fileSize;
  }
  if( walSize <= 0 ) return SQLITE_OK;

  walData = (u8*)sqlite3_malloc64(walSize);
  if( !walData ) return SQLITE_NOMEM;
  {
    int rc = sqlite3OsRead(cs->pFile, walData, (int)walSize, cs->iWalOffset);
    if( rc != SQLITE_OK ){
      sqlite3_free(walData);
      return rc;
    }
  }

  sqlite3_free(cs->pWalData);
  cs->pWalData = walData;
  cs->nWalData = walSize;

  pos = 0;
  while( pos < walSize ){
    u8 tag = walData[pos];
    pos++;

    if( tag == CS_WAL_TAG_CHUNK ){
      ProllyHash hash;
      u32 len;
      if( pos + 20 + 4 > walSize ) break;
      memcpy(&hash, walData + pos, 20);
      pos += 20;
      len = CS_READ_U32(walData + pos);
      pos += 4;
      if( pos < 0 || (u64)pos + len > (u64)walSize ) break;

      {
        int existing = csSearchIndex(cs->aIndex, cs->nIndex, &hash);
        if( existing < 0 ){
          int rc = csGrowPending(cs);
          ChunkIndexEntry *e;
          if( rc != SQLITE_OK ){
            sqlite3_free(walData);
            cs->pWalData = 0;
            return rc;
          }
          e = &cs->aPending[cs->nPending];
          memcpy(&e->hash, &hash, sizeof(ProllyHash));
          e->offset = csEncodeWalOffset((i64)pos);
          e->size = (int)len;
          cs->nPending++;
        }
      }
      pos += len;

    } else if( tag == CS_WAL_TAG_ROOT ){
      if( pos + CHUNK_MANIFEST_SIZE > walSize ) break;
      if( updateManifest ){
        u8 *m = walData + pos;
        u32 magic = CS_READ_U32(m);
        if( magic == CHUNK_STORE_MAGIC ){
          /* offset 8: reserved (was root_hash) */
          cs->nChunks = (int)CS_READ_U32(m + 28);
          /* offset 64: reserved (was headCommit_hash) */
          memcpy(cs->refsHash.data, m + 104, PROLLY_HASH_SIZE);
          memcpy(cs->workingState.data, m + 124, PROLLY_HASH_SIZE);
          /* Don't update iWalOffset/iIndexOffset from WAL root records --
          ** those fields describe the compacted region and only change on GC. */
        }
      }
      pos += CHUNK_MANIFEST_SIZE;

    } else {
      break;
    }
  }

  if( cs->nPending > 0 ){
    ChunkIndexEntry *aMerged = 0;
    int nMerged = 0;
    int rc = csMergeIndex(cs, &aMerged, &nMerged);
    if( rc != SQLITE_OK ){
      sqlite3_free(walData);
      cs->pWalData = 0;
      return rc;
    }
    sqlite3_free(cs->aIndex);
    cs->aIndex = aMerged;
    cs->nIndex = nMerged;
    cs->nIndexAlloc = nMerged;
    cs->nPending = 0;
    csPendHTClear(cs);
  }

  if( !prollyHashIsEmpty(&cs->refsHash) ){
    u8 *refsData = 0; int nRefsData = 0;
    int rc2 = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
    if( rc2==SQLITE_OK && refsData ){
      csFreeBranches(cs);
      csFreeTags(cs);
      csFreeRemotes(cs);
      csFreeTracking(cs);
      rc2 = csDeserializeRefs(cs, refsData, nRefsData);
      sqlite3_free(refsData);
      if( rc2!=SQLITE_OK ) return rc2;
    }else if( rc2!=SQLITE_OK ){
      return rc2;
    }
  }
  if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");

  return SQLITE_OK;
}

static int csIndexEntryCmp(const void *a, const void *b){
  const ChunkIndexEntry *ea = (const ChunkIndexEntry *)a;
  const ChunkIndexEntry *eb = (const ChunkIndexEntry *)b;
  return prollyHashCompare(&ea->hash, &eb->hash);
}

static int csMergeIndex(
  ChunkStore *cs,
  ChunkIndexEntry **ppMerged,
  int *pnMerged
){
  int nTotal = cs->nIndex + cs->nPending;
  ChunkIndexEntry *aMerged;
  int idxPos, pendPos, outPos;

  *ppMerged = 0;
  *pnMerged = 0;
  if( nTotal == 0 ) return SQLITE_OK;

  aMerged = (ChunkIndexEntry *)sqlite3_malloc(
    nTotal * (int)sizeof(ChunkIndexEntry)
  );
  if( aMerged == 0 ) return SQLITE_NOMEM;

  /* aIndex is already sorted; sort aPending before merge. */
  if( cs->nPending > 1 ){
    qsort(cs->aPending, cs->nPending, sizeof(ChunkIndexEntry),
          csIndexEntryCmp);
  }

  idxPos = 0;
  pendPos = 0;
  outPos = 0;
  while( idxPos < cs->nIndex && pendPos < cs->nPending ){
    int cmp = prollyHashCompare(&cs->aIndex[idxPos].hash, &cs->aPending[pendPos].hash);
    if( cmp < 0 ){
      aMerged[outPos++] = cs->aIndex[idxPos++];
    }else if( cmp > 0 ){
      aMerged[outPos++] = cs->aPending[pendPos++];
    }else{
      /* Duplicate hash: pending entry wins (it may carry a WAL offset). */
      aMerged[outPos++] = cs->aPending[pendPos++];
      idxPos++;
    }
  }
  while( idxPos < cs->nIndex ) aMerged[outPos++] = cs->aIndex[idxPos++];
  while( pendPos < cs->nPending ) aMerged[outPos++] = cs->aPending[pendPos++];

  *ppMerged = aMerged;
  *pnMerged = outPos;
  return SQLITE_OK;
}

int chunkStoreOpen(
  ChunkStore *cs,
  sqlite3_vfs *pVfs,
  const char *zFilename,
  int flags
){
  int rc;
  int exists = 0;
  int n;

  memset(cs, 0, sizeof(*cs));
  cs->pVfs = pVfs;
  cs->graphLockFd = -1;

  if( zFilename==0 || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0 ){
    cs->isMemory = 1;
    cs->zFilename = sqlite3_mprintf(":memory:");
    if( cs->zFilename==0 ) return SQLITE_NOMEM;
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->pFile = 0;
    return SQLITE_OK;
  }

  n = (int)strlen(zFilename);
  cs->zFilename = (char *)sqlite3_malloc(n + 1);
  if( cs->zFilename == 0 ) return SQLITE_NOMEM;
  memcpy(cs->zFilename, zFilename, n + 1);

  rc = sqlite3OsAccess(pVfs, cs->zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc != SQLITE_OK ){
    sqlite3_free(cs->zFilename);
    cs->zFilename = 0;
    return rc;
  }

  /* Treat a zero-byte file as non-existent (leftover from a failed create). */
  if( exists ){
    struct stat mainStat;
    if( stat(cs->zFilename, &mainStat)==0 && mainStat.st_size==0 ){
      exists = 0;
    }
  }

  if( exists ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc != SQLITE_OK ){
      openFlags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB;
      rc = csOpenFile(pVfs, cs->zFilename, &cs->pFile, openFlags);
      if( rc != SQLITE_OK ){
        sqlite3_free(cs->zFilename);
        cs->zFilename = 0;
        return rc;
      }
      cs->readOnly = 1;
    }

    rc = csReadManifest(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    rc = csReadIndex(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    if( !prollyHashIsEmpty(&cs->refsHash) ){
      u8 *refsData = 0; int nRefsData = 0;
      rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
      if( rc==SQLITE_OK ){
        rc = csDeserializeRefs(cs, refsData, nRefsData);
        sqlite3_free(refsData);
      }
      if( rc!=SQLITE_OK ){
        csCloseFile(cs->pFile);
        cs->pFile = 0;
        sqlite3_free(cs->zFilename);
        cs->zFilename = 0;
        return rc;
      }
    }
    if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");
  }else{
    if( !(flags & SQLITE_OPEN_CREATE) ){
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return SQLITE_CANTOPEN;
    }
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;
    /* File not yet created; WAL starts right after the manifest. */
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->iFileSize = 0;
    cs->pFile = 0;
  }

  return SQLITE_OK;
}

int chunkStoreClose(ChunkStore *cs){
  chunkStoreUnlock(cs);
  if( cs->pFile ){
    csCloseFile(cs->pFile);
    cs->pFile = 0;
  }
  sqlite3_free(cs->pWalData);
  sqlite3_free(cs->zFilename);
  sqlite3_free(cs->aIndex);
  sqlite3_free(cs->aPending);
  csPendHTClear(cs);
  sqlite3_free(cs->pWriteBuf);
  sqlite3_free(cs->zDefaultBranch);
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  memset(cs, 0, sizeof(*cs));
  return SQLITE_OK;
}

void chunkStoreGetWorkingState(ChunkStore *cs, ProllyHash *pState){
  memcpy(pState, &cs->workingState, sizeof(ProllyHash));
}

void chunkStoreSetWorkingState(ChunkStore *cs, const ProllyHash *pState){
  memcpy(&cs->workingState, pState, sizeof(ProllyHash));
}

void chunkStoreGetStagedCatalog(ChunkStore *cs, ProllyHash *pStaged){
  memcpy(pStaged, &cs->stagedCatalog, sizeof(ProllyHash));
}

void chunkStoreSetStagedCatalog(ChunkStore *cs, const ProllyHash *pStaged){
  memcpy(&cs->stagedCatalog, pStaged, sizeof(ProllyHash));
}

const char *chunkStoreGetDefaultBranch(ChunkStore *cs){
  return cs->zDefaultBranch ? cs->zDefaultBranch : "main";
}

int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName){
  char *zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = zCopy;
  return SQLITE_OK;
}

int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zName)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->aBranches[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  struct BranchRef *aNew;
  if( chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->aBranches, (cs->nBranches+1)*(int)sizeof(struct BranchRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->aBranches = aNew;
  memset(&aNew[cs->nBranches], 0, sizeof(struct BranchRef));
  aNew[cs->nBranches].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->nBranches].zName ) return SQLITE_NOMEM;
  memcpy(&aNew[cs->nBranches].commitHash, pCommit, sizeof(ProllyHash));
  cs->nBranches++;
  return SQLITE_OK;
}

int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zName)==0 ){
      memcpy(&cs->aBranches[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zName)==0 ){
      sqlite3_free(cs->aBranches[i].zName);
      cs->aBranches[i] = cs->aBranches[cs->nBranches-1];
      cs->nBranches--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zBranch)==0 ){
      memcpy(pHash, &cs->aBranches[i].workingSetHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  memset(pHash, 0, sizeof(ProllyHash));
  return SQLITE_NOTFOUND;
}

int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zBranch)==0 ){
      memcpy(&cs->aBranches[i].workingSetHash, pHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nTags; i++){
    if( strcmp(cs->aTags[i].zName, zName)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->aTags[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddTag(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  struct TagRef *aNew;
  if( chunkStoreFindTag(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->aTags, (cs->nTags+1)*(int)sizeof(struct TagRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->aTags = aNew;
  aNew[cs->nTags].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->nTags].zName ) return SQLITE_NOMEM;
  memcpy(&aNew[cs->nTags].commitHash, pCommit, sizeof(ProllyHash));
  cs->nTags++;
  return SQLITE_OK;
}

int chunkStoreDeleteTag(ChunkStore *cs, const char *zName){
  int i;
  for(i=0; i<cs->nTags; i++){
    if( strcmp(cs->aTags[i].zName, zName)==0 ){
      sqlite3_free(cs->aTags[i].zName);
      cs->aTags[i] = cs->aTags[cs->nTags-1];
      cs->nTags--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl){
  int i;
  for(i=0; i<cs->nRemotes; i++){
    if( strcmp(cs->aRemotes[i].zName, zName)==0 ){
      if( pzUrl ) *pzUrl = cs->aRemotes[i].zUrl;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl){
  struct RemoteRef *aNew;
  if( chunkStoreFindRemote(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->aRemotes, (cs->nRemotes+1)*(int)sizeof(struct RemoteRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->aRemotes = aNew;
  aNew[cs->nRemotes].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->nRemotes].zName ) return SQLITE_NOMEM;
  aNew[cs->nRemotes].zUrl = sqlite3_mprintf("%s", zUrl);
  if( !aNew[cs->nRemotes].zUrl ){
    sqlite3_free(aNew[cs->nRemotes].zName);
    return SQLITE_NOMEM;
  }
  cs->nRemotes++;
  return SQLITE_OK;
}

int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName){
  int i, j;
  for(i=0; i<cs->nRemotes; i++){
    if( strcmp(cs->aRemotes[i].zName, zName)==0 ){
      sqlite3_free(cs->aRemotes[i].zName);
      sqlite3_free(cs->aRemotes[i].zUrl);
      cs->aRemotes[i] = cs->aRemotes[cs->nRemotes-1];
      cs->nRemotes--;
      /* Also remove all tracking branches for this remote. */
      for(j=cs->nTracking-1; j>=0; j--){
        if( strcmp(cs->aTracking[j].zRemote, zName)==0 ){
          sqlite3_free(cs->aTracking[j].zRemote);
          sqlite3_free(cs->aTracking[j].zBranch);
          cs->aTracking[j] = cs->aTracking[cs->nTracking-1];
          cs->nTracking--;
        }
      }
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->aTracking[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      memcpy(&cs->aTracking[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  /* Not found: create a new tracking entry. */
  {
    struct TrackingBranch *aNew;
    aNew = sqlite3_realloc(cs->aTracking, (cs->nTracking+1)*(int)sizeof(struct TrackingBranch));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aTracking = aNew;
    aNew[cs->nTracking].zRemote = sqlite3_mprintf("%s", zRemote);
    if( !aNew[cs->nTracking].zRemote ) return SQLITE_NOMEM;
    aNew[cs->nTracking].zBranch = sqlite3_mprintf("%s", zBranch);
    if( !aNew[cs->nTracking].zBranch ){
      sqlite3_free(aNew[cs->nTracking].zRemote);
      return SQLITE_NOMEM;
    }
    memcpy(&aNew[cs->nTracking].commitHash, pCommit, sizeof(ProllyHash));
    cs->nTracking++;
  }
  return SQLITE_OK;
}

int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch){
  int i;
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      sqlite3_free(cs->aTracking[i].zRemote);
      sqlite3_free(cs->aTracking[i].zBranch);
      cs->aTracking[i] = cs->aTracking[cs->nTracking-1];
      cs->nTracking--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult){
  int i;
  for(i=0; i<nHash; i++){
    aResult[i] = chunkStoreHas(cs, &aHash[i]) ? 1 : 0;
  }
  return SQLITE_OK;
}

/*
** Refs blob format (version 5):
**   [version:1][default_branch_len:4][default_branch:N]
**   [nBranches:4] { [name_len:4][name:N][commit_hash:20][ws_hash:20] }...
**   [nTags:4]     { [name_len:4][name:N][commit_hash:20] }...
**   [nRemotes:4]  { [name_len:4][name:N][url_len:4][url:N] }...
**   [nTracking:4] { [remote_len:4][remote:N][branch_len:4][branch:N][commit_hash:20] }...
** All length fields are little-endian u32.
*/
static int csSerializeRefsBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  const char *def = cs->zDefaultBranch ? cs->zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  int sz = 1 + 4 + defLen + 4 + 4 + 4 + 4;
  int i;
  u8 *buf, *bufCur;

  *ppOut = 0;
  *pnOut = 0;

  for(i=0; i<cs->nBranches; i++){
    int inc = 4 + (int)strlen(cs->aBranches[i].zName) + PROLLY_HASH_SIZE*2;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nTags; i++){
    int inc = 4 + (int)strlen(cs->aTags[i].zName) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nRemotes; i++){
    int inc = 4 + (int)strlen(cs->aRemotes[i].zName) + 4 + (int)strlen(cs->aRemotes[i].zUrl);
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nTracking; i++){
    int inc = 4 + (int)strlen(cs->aTracking[i].zRemote) + 4 + (int)strlen(cs->aTracking[i].zBranch) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  bufCur = buf;
  *bufCur++ = 5;  /* refs format version */
  CS_WRITE_U32(bufCur,defLen); bufCur+=4;
  memcpy(bufCur, def, defLen); bufCur+=defLen;
  CS_WRITE_U32(bufCur,cs->nBranches); bufCur+=4;
  for(i=0; i<cs->nBranches; i++){
    int nameLen = (int)strlen(cs->aBranches[i].zName);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->aBranches[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aBranches[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    memcpy(bufCur, cs->aBranches[i].workingSetHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->nTags); bufCur+=4;
  for(i=0; i<cs->nTags; i++){
    int nameLen = (int)strlen(cs->aTags[i].zName);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->aTags[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aTags[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->nRemotes); bufCur+=4;
  for(i=0; i<cs->nRemotes; i++){
    int nameLen = (int)strlen(cs->aRemotes[i].zName);
    int urlLen = (int)strlen(cs->aRemotes[i].zUrl);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->aRemotes[i].zName, nameLen); bufCur+=nameLen;
    CS_WRITE_U32(bufCur,urlLen); bufCur+=4;
    memcpy(bufCur, cs->aRemotes[i].zUrl, urlLen); bufCur+=urlLen;
  }
  CS_WRITE_U32(bufCur,cs->nTracking); bufCur+=4;
  for(i=0; i<cs->nTracking; i++){
    int remoteLen = (int)strlen(cs->aTracking[i].zRemote);
    int branchLen = (int)strlen(cs->aTracking[i].zBranch);
    CS_WRITE_U32(bufCur,remoteLen); bufCur+=4;
    memcpy(bufCur, cs->aTracking[i].zRemote, remoteLen); bufCur+=remoteLen;
    CS_WRITE_U32(bufCur,branchLen); bufCur+=4;
    memcpy(bufCur, cs->aTracking[i].zBranch, branchLen); bufCur+=branchLen;
    memcpy(bufCur, cs->aTracking[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  *ppOut = buf;
  *pnOut = sz;
  return SQLITE_OK;
}

int chunkStoreSerializeRefs(ChunkStore *cs){
  int rc;
  u8 *buf = 0;
  int sz = 0;
  ProllyHash refsHash;

  rc = csSerializeRefsBlob(cs, &buf, &sz);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, buf, sz, &refsHash);
  sqlite3_free(buf);
  if( rc==SQLITE_OK ) memcpy(&cs->refsHash, &refsHash, sizeof(ProllyHash));
  return rc;
}

static int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData){
  const u8 *bufCur = data;
  int defLen, nBranches, nTags, i;
  u8 version;
  if( nData<5 ) return SQLITE_CORRUPT;
  version = *bufCur++;
  if( version!=5 ) return SQLITE_CORRUPT;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  defLen = (int)CS_READ_U32(bufCur); bufCur+=4;
  if( bufCur+defLen>data+nData ) return SQLITE_CORRUPT;
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = sqlite3_malloc(defLen+1);
  if(!cs->zDefaultBranch) return SQLITE_NOMEM;
  memcpy(cs->zDefaultBranch, bufCur, defLen); cs->zDefaultBranch[defLen]=0; bufCur+=defLen;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  nBranches = (int)CS_READ_U32(bufCur); bufCur+=4;
  csFreeBranches(cs);
  if( nBranches>0 ){
    cs->aBranches = sqlite3_malloc(nBranches*(int)sizeof(struct BranchRef));
    if(!cs->aBranches) return SQLITE_NOMEM;
    for(i=0;i<nBranches;i++){
      int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
      nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
      if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
      memset(&cs->aBranches[i], 0, sizeof(struct BranchRef));
      cs->aBranches[i].zName=sqlite3_malloc(nameLen+1);
      if(!cs->aBranches[i].zName) return SQLITE_NOMEM;
      memcpy(cs->aBranches[i].zName,bufCur,nameLen); cs->aBranches[i].zName[nameLen]=0; bufCur+=nameLen;
      memcpy(cs->aBranches[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      if( bufCur+PROLLY_HASH_SIZE<=data+nData ){
        memcpy(cs->aBranches[i].workingSetHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      }
      cs->nBranches++;
    }
  }

  csFreeTags(cs);
  if( bufCur+4<=data+nData ){
    nTags = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nTags>0 ){
      cs->aTags = sqlite3_malloc(nTags*(int)sizeof(struct TagRef));
      if(!cs->aTags) return SQLITE_NOMEM;
      memset(cs->aTags, 0, nTags*(int)sizeof(struct TagRef));
      for(i=0;i<nTags;i++){
        int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
        cs->aTags[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->aTags[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aTags[i].zName,bufCur,nameLen); cs->aTags[i].zName[nameLen]=0; bufCur+=nameLen;
        memcpy(cs->aTags[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
        cs->nTags++;
      }
    }
  }

  csFreeRemotes(cs);
  csFreeTracking(cs);
  if( bufCur+4<=data+nData ){
    int nRemotes = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nRemotes>0 ){
      cs->aRemotes = sqlite3_malloc(nRemotes*(int)sizeof(struct RemoteRef));
      if(!cs->aRemotes) return SQLITE_NOMEM;
      memset(cs->aRemotes, 0, nRemotes*(int)sizeof(struct RemoteRef));
      for(i=0;i<nRemotes;i++){
        int nameLen, urlLen;
        if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+nameLen+4>data+nData) return SQLITE_CORRUPT;
        cs->aRemotes[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->aRemotes[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aRemotes[i].zName,bufCur,nameLen); cs->aRemotes[i].zName[nameLen]=0; bufCur+=nameLen;
        urlLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+urlLen>data+nData) return SQLITE_CORRUPT;
        cs->aRemotes[i].zUrl=sqlite3_malloc(urlLen+1);
        if(!cs->aRemotes[i].zUrl) return SQLITE_NOMEM;
        memcpy(cs->aRemotes[i].zUrl,bufCur,urlLen); cs->aRemotes[i].zUrl[urlLen]=0; bufCur+=urlLen;
        cs->nRemotes++;
      }
    }
    if( bufCur+4<=data+nData ){
      int nTracking = (int)CS_READ_U32(bufCur); bufCur+=4;
      if( nTracking>0 ){
        cs->aTracking = sqlite3_malloc(nTracking*(int)sizeof(struct TrackingBranch));
        if(!cs->aTracking) return SQLITE_NOMEM;
        memset(cs->aTracking, 0, nTracking*(int)sizeof(struct TrackingBranch));
        for(i=0;i<nTracking;i++){
          int remoteLen, branchLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          remoteLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+remoteLen+4>data+nData) return SQLITE_CORRUPT;
          cs->aTracking[i].zRemote=sqlite3_malloc(remoteLen+1);
          if(!cs->aTracking[i].zRemote) return SQLITE_NOMEM;
          memcpy(cs->aTracking[i].zRemote,bufCur,remoteLen); cs->aTracking[i].zRemote[remoteLen]=0; bufCur+=remoteLen;
          branchLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+branchLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
          cs->aTracking[i].zBranch=sqlite3_malloc(branchLen+1);
          if(!cs->aTracking[i].zBranch) return SQLITE_NOMEM;
          memcpy(cs->aTracking[i].zBranch,bufCur,branchLen); cs->aTracking[i].zBranch[branchLen]=0; bufCur+=branchLen;
          memcpy(cs->aTracking[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
          cs->nTracking++;
        }
      }
    }
  }

  return SQLITE_OK;
}

int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  return csDeserializeRefs(cs, data, nData);
}

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  return csSerializeRefsBlob(cs, ppOut, pnOut);
}

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash){
  if( csSearchIndex(cs->aIndex, cs->nIndex, hash) >= 0 ) return 1;
  if( csSearchPending(cs, hash) >= 0 ) return 1;
  return 0;
}

/*
** Retrieve a chunk by hash. The lookup order matters:
**   1. Pending (uncommitted) chunks in pWriteBuf
**   2. Committed index -- WAL-encoded offsets read from pWalData cache
**   3. Committed index -- in-memory pWriteBuf (memory-only stores)
**   4. Committed index -- read from file (offset + 4 skips length prefix)
** Caller must sqlite3_free(*ppData).
*/
int chunkStoreGet(
  ChunkStore *cs,
  const ProllyHash *hash,
  u8 **ppData,
  int *pnData
){
  int idx;
  int rc;

  *ppData = 0;
  *pnData = 0;

  /* 1. Check pending (uncommitted) chunks first. */
  idx = csSearchPending(cs, hash);
  if( idx >= 0 ){
    ChunkIndexEntry *e = &cs->aPending[idx];
    i64 off = e->offset;
    int sz = e->size;
    u8 *pCopy = (u8 *)sqlite3_malloc(sz);
    if( pCopy == 0 ) return SQLITE_NOMEM;
    /* offset points at the 4-byte length prefix; data starts at offset+4 */
    memcpy(pCopy, cs->pWriteBuf + off + 4, sz);
    *ppData = pCopy;
    *pnData = sz;
    return SQLITE_OK;
  }

  idx = csSearchIndex(cs->aIndex, cs->nIndex, hash);
  if( idx < 0 ){
    return SQLITE_NOTFOUND;
  }

  /* 2. WAL-cached chunk (negative offset). */
  {
    ChunkIndexEntry *e = &cs->aIndex[idx];
    if( csIsWalOffset(e->offset) && cs->pWalData ){
      i64 walOff = csDecodeWalOffset(e->offset);
      int sz = e->size;
      if( walOff >= 0 && walOff + sz <= cs->nWalData ){
        u8 *pCopy = (u8 *)sqlite3_malloc(sz);
        if( pCopy == 0 ) return SQLITE_NOMEM;
        memcpy(pCopy, cs->pWalData + walOff, sz);
        *ppData = pCopy;
        *pnData = sz;
        return SQLITE_OK;
      }
      return SQLITE_NOTFOUND;
    }
  }

  /* 3. Memory-only store: read from pWriteBuf. */
  if( cs->pFile == 0 ){
    ChunkIndexEntry *e = &cs->aIndex[idx];
    if( cs->pWriteBuf && e->offset >= 0
     && (e->offset + 4 + e->size) <= cs->nWriteBuf ){
      u8 *pCopy = (u8 *)sqlite3_malloc(e->size);
      if( pCopy == 0 ) return SQLITE_NOMEM;
      memcpy(pCopy, cs->pWriteBuf + e->offset + 4, e->size);
      *ppData = pCopy;
      *pnData = e->size;
      return SQLITE_OK;
    }
    return SQLITE_NOTFOUND;
  }

  /* 4. Read from file. Verify stored length matches index for corruption check. */
  {
    ChunkIndexEntry *e = &cs->aIndex[idx];
    i64 fileOff = e->offset;
    int sz = e->size;
    u8 lenBuf[4];
    u8 *pBuf;
    u32 storedLen;

    rc = sqlite3OsRead(cs->pFile, lenBuf, 4, fileOff);
    if( rc != SQLITE_OK ) return rc;

    storedLen = CS_READ_U32(lenBuf);
    if( (int)storedLen != sz ){
      return SQLITE_CORRUPT;
    }

    pBuf = (u8 *)sqlite3_malloc(sz);
    if( pBuf == 0 ) return SQLITE_NOMEM;

    rc = sqlite3OsRead(cs->pFile, pBuf, sz, fileOff + 4);
    if( rc != SQLITE_OK ){
      sqlite3_free(pBuf);
      return rc;
    }

    *ppData = pBuf;
    *pnData = sz;
  }

  return SQLITE_OK;
}

int chunkStorePut(
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  ProllyHash *pHash
){
  int rc;
  ProllyHash h;

  prollyHashCompute(pData, nData, &h);
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  /* Content-addressed: skip if chunk already exists. */
  if( csSearchIndex(cs->aIndex, cs->nIndex, &h) >= 0 ) return SQLITE_OK;
  if( csSearchPending(cs, &h) >= 0 ) return SQLITE_OK;

  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;

  rc = csGrowWriteBuf(cs, 4 + nData);
  if( rc != SQLITE_OK ) return rc;

  {
    ChunkIndexEntry *e = &cs->aPending[cs->nPending];
    e->hash = h;
    e->offset = (i64)cs->nWriteBuf;  /* points at the length prefix in pWriteBuf */
    e->size = nData;
    cs->nPending++;
  }

  /* Write [length_le32][data] into pWriteBuf. */
  CS_WRITE_U32(cs->pWriteBuf + cs->nWriteBuf, (u32)nData);
  cs->nWriteBuf += 4;
  memcpy(cs->pWriteBuf + cs->nWriteBuf, pData, nData);
  cs->nWriteBuf += nData;

  return SQLITE_OK;
}

static int csCommitToMemory(ChunkStore *cs){
  if( cs->nPending > 0 ){
    ChunkIndexEntry *aMem = 0;
    int nMem = 0;
    int rc = csMergeIndex(cs, &aMem, &nMem);
    if( rc!=SQLITE_OK ) return rc;
    sqlite3_free(cs->aIndex);
    cs->aIndex = aMem;
    cs->nIndex = nMem;
    cs->nIndexAlloc = nMem;
    cs->nPending = 0;
    csPendHTClear(cs);
    cs->nCommittedWriteBuf = cs->nWriteBuf;
  }
  return SQLITE_OK;
}

/*
** Commit pending chunks to file. The protocol is crash-safe:
**   1. Acquire exclusive file lock
**   2. Check for concurrent writes (file grew since last known size)
**   3. Append WAL chunk records: tag(0x01) + hash(20) + len(4) + data
**   4. Append WAL root record: tag(0x02) + manifest_snapshot(168)
**   5. fsync -- this is the durability point
**   6. Release lock
** After fsync, copy newly-committed chunks into the in-memory pWalData
** cache and merge pending entries into the sorted aIndex.
** If another writer changed the root since we opened, return SQLITE_BUSY_SNAPSHOT.
*/
static int csCommitToFile(ChunkStore *cs){
  int rc;
  int i;
  i64 fileSize = 0;
  i64 writeOff;
  int lockFd = -1;
  int hadFile = (cs->pFile != 0);

  if( cs->pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc != SQLITE_OK ) return SQLITE_CANTOPEN;
  }

  /* Use the graph lock if already held (from chunkStoreLockAndRefresh),
  ** otherwise acquire our own. Avoids deadlock from double-locking. */
  if( cs->graphLockFd >= 0 ){
    lockFd = -1;
  }else{
    if( csFileLock(cs->zFilename, &lockFd) != 0 ){
      return SQLITE_BUSY;
    }
  }

  rc = cs->pFile->pMethods->xFileSize(cs->pFile, &fileSize);
  if( rc != SQLITE_OK ) goto commit_done;

  /* Detect concurrent writer: if file grew, re-read WAL to pick up new chunks. */
  if( fileSize > cs->iFileSize && hadFile ){
    sqlite3_free(cs->pWalData);
    cs->pWalData = 0;
    cs->nWalData = 0;
    {
      int savePending = cs->nPending;
      cs->nPending = 0;
      csPendHTClear(cs);
      csReplayWalRegion(cs, 1);
      cs->nPending = savePending;
    }
  }

  if( fileSize == 0 ){
    u8 manifest[CHUNK_MANIFEST_SIZE];
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    csSerializeManifest(cs, manifest);
    rc = sqlite3OsWrite(cs->pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc != SQLITE_OK ) goto commit_done;
    fileSize = CHUNK_MANIFEST_SIZE;
  }

  writeOff = fileSize;

  /* Write WAL chunk records. */
  for( i = 0; i < cs->nPending; i++ ){
    ChunkIndexEntry *pe = &cs->aPending[i];
    u8 recHdr[25];
    i64 bufOff;
    recHdr[0] = CS_WAL_TAG_CHUNK;
    memcpy(recHdr + 1, &pe->hash, 20);
    CS_WRITE_U32(recHdr + 21, (u32)pe->size);

    bufOff = pe->offset + 4;  /* skip length prefix to get to data */
    rc = sqlite3OsWrite(cs->pFile, recHdr, 25, writeOff);
    if( rc != SQLITE_OK ) goto commit_done;
    writeOff += 25;

    {
      const u8 *pSrc = cs->pWriteBuf + bufOff;
      int remaining = pe->size;
      while( remaining > 0 && rc==SQLITE_OK ){
        int toWrite = remaining > 65536 ? 65536 : remaining;
        rc = sqlite3OsWrite(cs->pFile, pSrc, toWrite, writeOff);
        pSrc += toWrite;
        writeOff += toWrite;
        remaining -= toWrite;
      }
    }
    if( rc != SQLITE_OK ) goto commit_done;
  }

  /* Write WAL root record (manifest snapshot for crash recovery). */
  {
    u8 rootRec[1 + CHUNK_MANIFEST_SIZE];
    rootRec[0] = CS_WAL_TAG_ROOT;
    csSerializeManifest(cs, rootRec + 1);
    rc = sqlite3OsWrite(cs->pFile, rootRec, sizeof(rootRec), writeOff);
    if( rc != SQLITE_OK ) goto commit_done;
    writeOff += sizeof(rootRec);
  }

  /* fsync is the durability point -- all data is recoverable after this. */
  rc = sqlite3OsSync(cs->pFile, SQLITE_SYNC_NORMAL);
  if( rc != SQLITE_OK ) goto commit_done;

  cs->iFileSize = writeOff;

commit_done:
  csFileUnlock(lockFd);

  if( rc != SQLITE_OK ) return rc;

  /* Move pending chunks into the WAL data cache with encoded offsets. */
  for( i = 0; i < cs->nPending; i++ ){
    ChunkIndexEntry *pe = &cs->aPending[i];
    i64 bufOff = pe->offset + 4;
    int sz = pe->size;

    i64 newSize = cs->nWalData + sz;
    u8 *newBuf = (u8*)sqlite3_realloc64(cs->pWalData, newSize);
    if( newBuf ){
      memcpy(newBuf + cs->nWalData, cs->pWriteBuf + bufOff, sz);
      pe->offset = csEncodeWalOffset(cs->nWalData);
      cs->pWalData = newBuf;
      cs->nWalData = newSize;
    } else {
      return SQLITE_NOMEM;
    }
  }

  {
    ChunkIndexEntry *aMerged = 0;
    int nMerged = 0;
    rc = csMergeIndex(cs, &aMerged, &nMerged);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aMerged);
      return rc;
    }
    sqlite3_free(cs->aIndex);
    cs->aIndex = aMerged;
    cs->nIndex = nMerged;
    cs->nIndexAlloc = nMerged;
  }

  sqlite3_free(cs->pWriteBuf);
  cs->pWriteBuf = 0;
  cs->nWriteBuf = 0;
  cs->nWriteBufAlloc = 0;
  cs->nPending = 0;
  csPendHTClear(cs);

  return SQLITE_OK;
}

int chunkStoreCommit(ChunkStore *cs){
  int rc;
  int acquiredLock = 0;
  if( cs->readOnly ) return SQLITE_READONLY;
  if( cs->isMemory ) return csCommitToMemory(cs);
  /* Auto-acquire graph lock if caller didn't already. Ensures ALL
  ** commit graph mutations are serialized across connections. */
  if( cs->graphLockFd < 0 && cs->zFilename ){
    rc = chunkStoreLockAndRefresh(cs);
    if( rc!=SQLITE_OK ) return rc;
    acquiredLock = 1;
  }
  rc = csCommitToFile(cs);
  if( acquiredLock ) chunkStoreUnlock(cs);
  return rc;
}

void chunkStoreRollback(ChunkStore *cs){
  cs->nPending = 0;
    csPendHTClear(cs);
  if( cs->isMemory ){
    /* Truncate pWriteBuf back to the last committed point. */
    cs->nWriteBuf = cs->nCommittedWriteBuf;
  }else{
    cs->nWriteBuf = 0;
  }
}

int chunkStoreIsEmpty(ChunkStore *cs){
  return cs->nBranches == 0 && prollyHashIsEmpty(&cs->refsHash);
}

void chunkStoreClearRefs(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  memset(&cs->refsHash, 0, sizeof(cs->refsHash));
  memset(&cs->workingState, 0, sizeof(cs->workingState));
  memset(&cs->stagedCatalog, 0, sizeof(cs->stagedCatalog));
}

int chunkStoreReloadRefs(ChunkStore *cs){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;

  if( prollyHashIsEmpty(&cs->refsHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);

  rc = csDeserializeRefs(cs, refsData, nRefsData);
  sqlite3_free(refsData);
  return rc;
}

const char *chunkStoreFilename(ChunkStore *cs){
  return cs->zFilename;
}

int chunkStoreLockAndRefresh(ChunkStore *cs){
  int changed = 0;
  int rc;
  if( cs->isMemory ) return SQLITE_OK;
  if( cs->graphLockFd >= 0 ) return SQLITE_OK;
  if( !cs->zFilename ) return SQLITE_ERROR;
  if( csFileLockNB(cs->zFilename, &cs->graphLockFd) != 0 ){
    return SQLITE_BUSY;
  }
  rc = chunkStoreRefreshIfChanged(cs, &changed);
  if( rc!=SQLITE_OK ){
    csFileUnlock(cs->graphLockFd);
    cs->graphLockFd = -1;
  }
  return rc;
}

void chunkStoreUnlock(ChunkStore *cs){
  if( cs->graphLockFd >= 0 ){
    csFileUnlock(cs->graphLockFd);
    cs->graphLockFd = -1;
  }
}

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged){
  int bMoved = 0;
  int rc;
  *pChanged = 0;
  if( cs->isMemory ) return SQLITE_OK;

  if( cs->snapshotPinned ) return SQLITE_OK;

  if( cs->pFile==0 ){
    int exists = 0;
    rc = sqlite3OsAccess(cs->pVfs, cs->zFilename,
                         SQLITE_ACCESS_EXISTS, &exists);
    if( rc!=SQLITE_OK ) return rc;
    if( exists ){
      struct stat mainStat;
      if( stat(cs->zFilename, &mainStat)==0 && mainStat.st_size > 0 ){
        int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
        rc = csOpenFile(cs->pVfs, cs->zFilename, &cs->pFile, openFlags);
        if( rc!=SQLITE_OK ) return rc;
        rc = csReadManifest(cs);
        if( rc!=SQLITE_OK ) return rc;
        rc = csReadIndex(cs);
        if( rc!=SQLITE_OK ) return rc;
        rc = csReplayWal(cs);
        if( rc!=SQLITE_OK ) return rc;
        *pChanged = 1;
      }
    }
    return SQLITE_OK;
  }

  rc = sqlite3OsFileControl(cs->pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
  if( rc!=SQLITE_OK ) return rc;

  if( !bMoved ){
    i64 fileSize = 0;
    rc = sqlite3OsFileSize(cs->pFile, &fileSize);
    if( rc!=SQLITE_OK ) return rc;
    if( fileSize > cs->iFileSize ){
      /* File grew from another writer; re-read WAL to pick up new chunks.
      ** Must also clear WAL-offset entries from the index because committed
      ** chunks were appended as raw data to pWalData (at positions that won't
      ** match the on-disk WAL format after re-read). Keeping the on-disk
      ** sorted index entries is safe since their file offsets don't change. */
      sqlite3_free(cs->pWalData);
      cs->pWalData = 0;
      cs->nWalData = 0;
      cs->nPending = 0;
      csPendHTClear(cs);
      /* Remove WAL-offset entries from aIndex (keep file-offset entries). */
      {
        int rd, wr;
        for(rd=0, wr=0; rd<cs->nIndex; rd++){
          if( !csIsWalOffset(cs->aIndex[rd].offset) ){
            if( wr!=rd ) cs->aIndex[wr] = cs->aIndex[rd];
            wr++;
          }
        }
        cs->nIndex = wr;
      }
      rc = csReplayWal(cs);
      if( rc!=SQLITE_OK ) return rc;
      cs->iFileSize = fileSize;
      *pChanged = 1;
    }
    return SQLITE_OK;
  }

  /* File was moved/replaced (e.g., by GC). Reopen and reload everything. */
  csCloseFile(cs->pFile);
  cs->pFile = 0;
  sqlite3_free(cs->aIndex);
  cs->aIndex = 0; cs->nIndex = 0; cs->nIndexAlloc = 0;
  sqlite3_free(cs->pWalData);
  cs->pWalData = 0; cs->nWalData = 0;
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = 0;
  {
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = csReadManifest(cs);
  if( rc!=SQLITE_OK ) return rc;
  rc = csReadIndex(cs);
  if( rc!=SQLITE_OK ) return rc;
  rc = csReplayWal(cs);
  if( rc!=SQLITE_OK ) return rc;
  if( !prollyHashIsEmpty(&cs->refsHash) ){
    u8 *refsData = 0; int nRefsData = 0;
    rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
    if( rc==SQLITE_OK ){
      rc = csDeserializeRefs(cs, refsData, nRefsData);
      sqlite3_free(refsData);
    }
    if( rc!=SQLITE_OK ) return rc;
  }
  if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");
  *pChanged = 1;
  return SQLITE_OK;
}

int chunkStoreGetMergeState(
  ChunkStore *cs,
  u8 *pIsMerging,
  ProllyHash *pMergeCommit,
  ProllyHash *pConflictsCatalog
){
  if( pIsMerging ) *pIsMerging = cs->isMerging;
  if( pMergeCommit ) memcpy(pMergeCommit, &cs->mergeCommitHash, sizeof(ProllyHash));
  if( pConflictsCatalog ) memcpy(pConflictsCatalog, &cs->conflictsCatalogHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

void chunkStoreSetMergeState(
  ChunkStore *cs,
  u8 isMerging,
  const ProllyHash *pMergeCommit,
  const ProllyHash *pConflictsCatalog
){
  cs->isMerging = isMerging;
  if( pMergeCommit ){
    memcpy(&cs->mergeCommitHash, pMergeCommit, sizeof(ProllyHash));
  }else{
    memset(&cs->mergeCommitHash, 0, sizeof(ProllyHash));
  }
  if( pConflictsCatalog ){
    memcpy(&cs->conflictsCatalogHash, pConflictsCatalog, sizeof(ProllyHash));
  }else{
    memset(&cs->conflictsCatalogHash, 0, sizeof(ProllyHash));
  }
}

void chunkStoreClearMergeState(ChunkStore *cs){
  cs->isMerging = 0;
  memset(&cs->mergeCommitHash, 0, sizeof(ProllyHash));
  memset(&cs->conflictsCatalogHash, 0, sizeof(ProllyHash));
}

void chunkStoreGetConflictsCatalog(ChunkStore *cs, ProllyHash *pHash){
  memcpy(pHash, &cs->conflictsCatalogHash, sizeof(ProllyHash));
}

void chunkStoreSetConflictsCatalog(ChunkStore *cs, const ProllyHash *pHash){
  memcpy(&cs->conflictsCatalogHash, pHash, sizeof(ProllyHash));
}

#endif /* DOLTLITE_PROLLY */
