/*
** Implementation of the file-backed content-addressed chunk store.
**
** Two-file architecture:
**
** Main file (filename):
**   [Manifest header: 168 bytes]
**   [Chunk data region: variable]  — chunks: length(4) + data(length) each
**   [Chunk index: variable]        — sorted (hash(20) + offset(8) + size(4))
**
** WAL region (appended to same file):
**   Append-only log of new chunks and root hash records.
**   [Record]*  where each record is:
**     Chunk record:  tag(1)=0x01 + hash(20) + length(4) + data(length)
**     Root record:   tag(1)=0x02 + manifest(168)
**
** Commit protocol (O(new_chunks), not O(total_chunks)):
**   1. Append new chunk records to journal
**   2. Append root record with updated manifest
**   3. fsync journal — this is the durability point
**
** Recovery (on open):
**   1. Read main file (manifest + index)
**   2. Replay journal: add chunk records to in-memory index,
**      use last root record as the current manifest state
**   3. If journal exists, main file manifest may be stale — that's OK
**
** GC / Checkpoint (future):
**   Merge journal chunks into main file, rewrite main file, delete journal.
**
** All multi-byte integers are little-endian.
*/
#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "prolly_hash.h"
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/uio.h>

/* --------------------------------------------------------------------
** Little-endian helper macros for reading/writing integers from byte
** buffers. These work on any alignment and any host byte order.
** -------------------------------------------------------------------- */

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

/* --------------------------------------------------------------------
** Forward declarations for internal helpers.
** -------------------------------------------------------------------- */
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
static char *csJournalPath(const char *zFilename);
static int csReplayWalRegion(ChunkStore *cs, int updateManifest);
static int csReplayWal(ChunkStore *cs){ return csReplayWalRegion(cs, 1); }

/* Journal record tags */
#define CS_WAL_TAG_CHUNK  0x01   /* Chunk record: tag(1)+hash(20)+len(4)+data */
#define CS_WAL_TAG_ROOT   0x02   /* Root record: tag(1)+manifest(168) */

/*
** WAL offset encoding: file offsets are stored as-is (>= 0).
** WAL-region offsets are stored as -(walPos) - 1 (always < 0).
** This distinguishes file-resident chunks from WAL-resident chunks.
*/
static i64 csEncodeWalOffset(i64 walPos){ return -(walPos) - 1; }
static i64 csDecodeWalOffset(i64 encoded){ return -(encoded + 1); }
static int csIsWalOffset(i64 offset){ return offset < 0; }

/*
** Helper functions to free branch and tag arrays, used in multiple places.
*/
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

/* Initial allocation sizes */
#define CS_INIT_INDEX_ALLOC   64
#define CS_INIT_PENDING_ALLOC 16
#define CS_INIT_WRITEBUF_SIZE 4096

/* --------------------------------------------------------------------
** csOpenFile -- allocate and open a sqlite3_file via the VFS layer.
** -------------------------------------------------------------------- */
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

/* --------------------------------------------------------------------
** csCloseFile -- close and free a sqlite3_file.
** -------------------------------------------------------------------- */
static void csCloseFile(sqlite3_file *pFile){
  if( pFile ){
    sqlite3OsCloseFree(pFile);
  }
}

/* --------------------------------------------------------------------
** csSearchIndex -- binary search the sorted index for a hash.
** Returns the index if found, or -1 if not found.
** -------------------------------------------------------------------- */
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

/* --------------------------------------------------------------------
** csSearchPending -- O(1) hash table lookup for pending chunks.
** Uses an incrementally-built hash table with dynamic resizing.
** Starts at 4K buckets and doubles when load factor exceeds 4.
** -------------------------------------------------------------------- */
#define CS_PEND_HT_INIT_BITS 12
#define CS_PEND_HT_MAX_LOAD  4   /* Rebuild when nPending/nBuckets > this */

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

/*
** Rebuild the entire hash table at the current size.
*/
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
  /* Grow hash table when load factor exceeds threshold */
  if( cs->nPending > cs->nPendingHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->nPendingHTSize * 4;
    int *aNew = sqlite3_realloc(cs->aPendingHT, newSize * (int)sizeof(int));
    if( aNew ){
      cs->aPendingHT = aNew;
      cs->nPendingHTSize = newSize;
      /* Must rebuild chains with new mask */
      if( !cs->aPendingHTNext || cs->nPendingAlloc > cs->nPendingHTNextAlloc ){
        int nAlloc = cs->nPendingAlloc > 0 ? cs->nPendingAlloc : 64;
        int *aNew2 = sqlite3_realloc(cs->aPendingHTNext, nAlloc*(int)sizeof(int));
        if( !aNew2 ) return SQLITE_NOMEM;
        cs->aPendingHTNext = aNew2;
        cs->nPendingHTNextAlloc = nAlloc;
      }
      return csPendHTRebuild(cs);
    }
    /* On OOM, continue with existing table (degraded perf, not incorrect) */
  }
  if( !cs->aPendingHTNext || cs->nPendingAlloc > cs->nPendingHTNextAlloc ){
    int nAlloc = cs->nPendingAlloc > 0 ? cs->nPendingAlloc : 64;
    int *aNew = sqlite3_realloc(cs->aPendingHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aPendingHTNext = aNew;
    cs->nPendingHTNextAlloc = nAlloc;
  }
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
    /* Fallback to linear scan on OOM */
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

/* --------------------------------------------------------------------
** csSerializeManifest -- write the 168-byte manifest header into aBuf.
** -------------------------------------------------------------------- */
/*
** V6 manifest layout (168 bytes):
**   0-3:   magic (DLTC)
**   4-7:   version (6)
**   8-27:  root hash
**   28-31: nChunks
**   32-39: iIndexOffset
**   40-43: nIndexSize
**   44-63: catalog hash
**   64-83: headCommit hash
**   84-91: iWalOffset (new in V6 — where WAL region starts)
**   92-103: reserved (zero)
**   104-123: refsHash
**   124-167: reserved (zero)
*/
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + 0, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + 4, CHUNK_STORE_VERSION);
  memcpy(aBuf + 8, cs->root.data, PROLLY_HASH_SIZE);
  CS_WRITE_U32(aBuf + 28, (u32)cs->nChunks);
  CS_WRITE_I64(aBuf + 32, cs->iIndexOffset);
  CS_WRITE_U32(aBuf + 40, (u32)cs->nIndexSize);
  memcpy(aBuf + 44, cs->catalog.data, PROLLY_HASH_SIZE);
  memcpy(aBuf + 64, cs->headCommit.data, PROLLY_HASH_SIZE);
  CS_WRITE_I64(aBuf + 84, cs->iWalOffset);
  memcpy(aBuf + 104, cs->refsHash.data, PROLLY_HASH_SIZE);
}

/* --------------------------------------------------------------------
** csSerializeIndexEntry -- write one 32-byte index entry.
** -------------------------------------------------------------------- */
static void csSerializeIndexEntry(const ChunkIndexEntry *e, u8 *aBuf){
  memcpy(aBuf, e->hash.data, PROLLY_HASH_SIZE);
  CS_WRITE_I64(aBuf + PROLLY_HASH_SIZE, e->offset);
  CS_WRITE_U32(aBuf + PROLLY_HASH_SIZE + 8, (u32)e->size);
}

/* --------------------------------------------------------------------
** csDeserializeIndexEntry -- read one 32-byte index entry.
** -------------------------------------------------------------------- */
static void csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e){
  memcpy(e->hash.data, aBuf, PROLLY_HASH_SIZE);
  e->offset = CS_READ_I64(aBuf + PROLLY_HASH_SIZE);
  e->size = (int)CS_READ_U32(aBuf + PROLLY_HASH_SIZE + 8);
}

/* --------------------------------------------------------------------
** csReadManifest -- read and validate the 168-byte manifest from an
** already-open file. Populates cs->root, cs->nChunks, cs->iIndexOffset,
** cs->nIndexSize.  Returns SQLITE_OK or error.
** -------------------------------------------------------------------- */
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

  memcpy(cs->root.data, aBuf + 8, PROLLY_HASH_SIZE);
  cs->nChunks = (int)CS_READ_U32(aBuf + 28);
  cs->iIndexOffset = CS_READ_I64(aBuf + 32);
  cs->nIndexSize = (int)CS_READ_U32(aBuf + 40);
  memcpy(cs->catalog.data, aBuf + 44, PROLLY_HASH_SIZE);
  memcpy(cs->headCommit.data, aBuf + 64, PROLLY_HASH_SIZE);
  cs->iWalOffset = CS_READ_I64(aBuf + 84);
  memcpy(cs->refsHash.data, aBuf + 104, PROLLY_HASH_SIZE);

  return SQLITE_OK;
}

/* --------------------------------------------------------------------
** csReadIndex -- read the chunk index from the file into cs->aIndex.
** Must be called after csReadManifest populates iIndexOffset/nIndexSize.
** -------------------------------------------------------------------- */
static int csReadIndex(ChunkStore *cs){
  int rc;
  int nEntries;
  u8 *aBuf;
  int i;

  if( cs->nIndexSize == 0 || cs->nChunks == 0 ){
    /* Empty store: no index to read */
    cs->nIndex = 0;
    return SQLITE_OK;
  }

  nEntries = cs->nIndexSize / CHUNK_INDEX_ENTRY_SIZE;
  if( nEntries * CHUNK_INDEX_ENTRY_SIZE != cs->nIndexSize ){
    return SQLITE_CORRUPT;
  }

  /* Allocate the in-memory index array */
  cs->aIndex = (ChunkIndexEntry *)sqlite3_malloc(
    nEntries * (int)sizeof(ChunkIndexEntry)
  );
  if( cs->aIndex == 0 ) return SQLITE_NOMEM;
  cs->nIndex = nEntries;
  cs->nIndexAlloc = nEntries;

  /* Read raw bytes */
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

  /* Deserialize each entry */
  for( i = 0; i < nEntries; i++ ){
    csDeserializeIndexEntry(aBuf + i * CHUNK_INDEX_ENTRY_SIZE, &cs->aIndex[i]);
  }

  sqlite3_free(aBuf);
  return SQLITE_OK;
}

/* --------------------------------------------------------------------
** csGrowPending -- ensure space for at least one more pending entry.
** -------------------------------------------------------------------- */
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

/* --------------------------------------------------------------------
** csGrowWriteBuf -- ensure the write buffer can hold nNeeded more bytes.
** -------------------------------------------------------------------- */
static int csGrowWriteBuf(ChunkStore *cs, int nNeeded){
  i64 nRequired = cs->nWriteBuf + (i64)nNeeded;
  if( nRequired > cs->nWriteBufAlloc ){
    i64 nNew = cs->nWriteBufAlloc ? cs->nWriteBufAlloc : CS_INIT_WRITEBUF_SIZE;
    /* Double for small buffers; grow by 50% once past 64 MB to avoid
    ** excessive over-allocation for long-lived in-memory stores. */
    while( nNew < nRequired ){
      if( nNew < 64*1024*1024 ){
        nNew *= 2;
      }else{
        nNew += nNew / 2;
      }
    }
    u8 *pNew = (u8 *)sqlite3_realloc64(cs->pWriteBuf, (sqlite3_uint64)nNew);
    if( pNew == 0 ) return SQLITE_NOMEM;
    cs->pWriteBuf = pNew;
    cs->nWriteBufAlloc = nNew;
  }
  return SQLITE_OK;
}

/* --------------------------------------------------------------------
** csJournalPath -- allocate "filename-journal" string.  Caller must
** sqlite3_free the result.
** -------------------------------------------------------------------- */
static char *csJournalPath(const char *zFilename){
  int n = (int)strlen(zFilename);
  char *z = (char *)sqlite3_malloc(n + 9);  /* "-journal\0" */
  if( z ){
    memcpy(z, zFilename, n);
    memcpy(z + n, "-journal", 9);  /* includes NUL */
  }
  return z;
}

/*
** Read and replay the WAL region from the main file.
** The WAL region starts at cs->iWalOffset and extends to EOF.
** Reads the WAL data into cs->pWalData, parses chunk and root records,
** and merges new entries into the in-memory index.
*/
static int csReplayWalRegion(ChunkStore *cs, int updateManifest){
  i64 walSize;
  u8 *walData;
  i64 pos;

  if( cs->iWalOffset <= 0 || !cs->pFile ) return SQLITE_OK;

  /* Get file size to determine WAL region size */
  {
    i64 fileSize = 0;
    int rc = sqlite3OsFileSize(cs->pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
    walSize = fileSize - cs->iWalOffset;
    cs->iFileSize = fileSize;
  }
  if( walSize <= 0 ) return SQLITE_OK;  /* Empty WAL region */

  /* Read WAL region into memory */
  walData = (u8*)sqlite3_malloc64(walSize);
  if( !walData ) return SQLITE_NOMEM;
  {
    int rc = sqlite3OsRead(cs->pFile, walData, (int)walSize, cs->iWalOffset);
    if( rc != SQLITE_OK ){
      sqlite3_free(walData);
      return rc;
    }
  }

  /* Store WAL data for chunk reads */
  sqlite3_free(cs->pWalData);
  cs->pWalData = walData;
  cs->nWalData = walSize;

  /* Parse records — same format as old WAL file */
  pos = 0;
  while( pos < walSize ){
    u8 tag = walData[pos];
    pos++;

    if( tag == CS_WAL_TAG_CHUNK ){
      if( pos + 20 + 4 > walSize ) break;
      ProllyHash hash;
      memcpy(&hash, walData + pos, 20);
      pos += 20;
      u32 len = CS_READ_U32(walData + pos);
      pos += 4;
      if( pos < 0 || (u64)pos + len > (u64)walSize ) break;

      {
        int existing = csSearchIndex(cs->aIndex, cs->nIndex, &hash);
        if( existing < 0 ){
          int rc = csGrowPending(cs);
          if( rc != SQLITE_OK ){
            sqlite3_free(walData);
            cs->pWalData = 0;
            return rc;
          }
          ChunkIndexEntry *e = &cs->aPending[cs->nPending];
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
          memcpy(cs->root.data, m + 8, PROLLY_HASH_SIZE);
          cs->nChunks = (int)CS_READ_U32(m + 28);
          memcpy(cs->catalog.data, m + 44, PROLLY_HASH_SIZE);
          memcpy(cs->headCommit.data, m + 64, PROLLY_HASH_SIZE);
          memcpy(cs->refsHash.data, m + 104, PROLLY_HASH_SIZE);
          /* Note: iIndexOffset/nIndexSize from WAL root records refer to
          ** the compacted region, which hasn't changed. Keep existing values. */
        }
      }
      pos += CHUNK_MANIFEST_SIZE;

    } else {
      break;
    }
  }

  /* Merge WAL entries into main index */
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

  /* Reload refs from the WAL-updated refsHash */
  if( !prollyHashIsEmpty(&cs->refsHash) ){
    u8 *refsData = 0; int nRefsData = 0;
    int rc2 = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
    if( rc2==SQLITE_OK && refsData ){
      csFreeBranches(cs);
      csFreeTags(cs);
      csFreeRemotes(cs);
      csFreeTracking(cs);
      csDeserializeRefs(cs, refsData, nRefsData);
      sqlite3_free(refsData);
    }
  }
  if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");

  return SQLITE_OK;
}

/* --------------------------------------------------------------------
** Comparison function for qsort of ChunkIndexEntry by hash.
** -------------------------------------------------------------------- */
static int csIndexEntryCmp(const void *a, const void *b){
  const ChunkIndexEntry *ea = (const ChunkIndexEntry *)a;
  const ChunkIndexEntry *eb = (const ChunkIndexEntry *)b;
  return prollyHashCompare(&ea->hash, &eb->hash);
}

/* --------------------------------------------------------------------
** csMergeIndex -- merge aIndex and aPending into a single sorted array.
** Caller must sqlite3_free *ppMerged.
** -------------------------------------------------------------------- */
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

  /* Merge two sorted arrays. aPending may not be sorted yet. */
  /* Sort aPending first. */
  if( cs->nPending > 1 ){
    qsort(cs->aPending, cs->nPending, sizeof(ChunkIndexEntry),
          csIndexEntryCmp);
  }

  /* Standard merge of two sorted arrays */
  idxPos = 0;   /* index into aIndex */
  pendPos = 0;  /* index into aPending */
  outPos = 0;   /* index into aMerged */
  while( idxPos < cs->nIndex && pendPos < cs->nPending ){
    int cmp = prollyHashCompare(&cs->aIndex[idxPos].hash, &cs->aPending[pendPos].hash);
    if( cmp < 0 ){
      aMerged[outPos++] = cs->aIndex[idxPos++];
    }else if( cmp > 0 ){
      aMerged[outPos++] = cs->aPending[pendPos++];
    }else{
      /* Duplicate hash: pending wins (shouldn't normally happen) */
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

/* ====================================================================
** Public API implementation
** ==================================================================== */

/*
** Open or create a chunk store at the given path.
**
** If the file exists, its manifest and index are loaded into memory.
** If it does not exist and flags include SQLITE_OPEN_CREATE, an empty
** store is initialized (no file is written until commit).
*/
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

  /* Detect in-memory databases: NULL, empty string, or ":memory:" */
  if( zFilename==0 || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0 ){
    cs->isMemory = 1;
    cs->zFilename = sqlite3_mprintf(":memory:");
    if( cs->zFilename==0 ) return SQLITE_NOMEM;
    memset(&cs->root, 0, sizeof(cs->root));
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->pFile = 0;
    return SQLITE_OK;
  }

  /* Copy the filename */
  n = (int)strlen(zFilename);
  cs->zFilename = (char *)sqlite3_malloc(n + 1);
  if( cs->zFilename == 0 ) return SQLITE_NOMEM;
  memcpy(cs->zFilename, zFilename, n + 1);

  /* Check if the file already exists.
  ** Use cs->zFilename (our persistent copy) not the caller's zFilename
  ** which may be freed after this function returns. */
  rc = sqlite3OsAccess(pVfs, cs->zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc != SQLITE_OK ){
    sqlite3_free(cs->zFilename);
    cs->zFilename = 0;
    return rc;
  }

  /* Treat zero-byte placeholder as non-existent */
  if( exists ){
    struct stat mainStat;
    if( stat(cs->zFilename, &mainStat)==0 && mainStat.st_size==0 ){
      exists = 0;
    }
  }

  if( exists ){
    /* Open the existing single file */
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

    /* Read manifest */
    rc = csReadManifest(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    /* Read chunk index from compacted region */
    rc = csReadIndex(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    /* Replay WAL region (iWalOffset to EOF) */
    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    /* Load refs chunk (may be in compacted or WAL region) */
    if( !prollyHashIsEmpty(&cs->refsHash) ){
      u8 *refsData = 0; int nRefsData = 0;
      rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
      if( rc==SQLITE_OK ){
        csDeserializeRefs(cs, refsData, nRefsData);
        sqlite3_free(refsData);
      }
    }
    if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");
  }else{
    /* File doesn't exist — create new */
    if( !(flags & SQLITE_OPEN_CREATE) ){
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return SQLITE_CANTOPEN;
    }
    memset(&cs->root, 0, sizeof(cs->root));
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;  /* WAL starts right after manifest */
    cs->iFileSize = 0;
    cs->pFile = 0;
  }

  return SQLITE_OK;
}

/*
** Close the chunk store.  Any uncommitted pending data is discarded.
*/
int chunkStoreClose(ChunkStore *cs){
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

/*
** Get the current root hash.
*/
void chunkStoreGetRoot(ChunkStore *cs, ProllyHash *pRoot){
  memcpy(pRoot, &cs->root, sizeof(ProllyHash));
}

/*
** Set the root hash. This is staged and not persisted until commit.
*/
void chunkStoreSetRoot(ChunkStore *cs, const ProllyHash *pRoot){
  memcpy(&cs->root, pRoot, sizeof(ProllyHash));
}

void chunkStoreGetCatalog(ChunkStore *cs, ProllyHash *pCat){
  memcpy(pCat, &cs->catalog, sizeof(ProllyHash));
}

void chunkStoreSetCatalog(ChunkStore *cs, const ProllyHash *pCat){
  memcpy(&cs->catalog, pCat, sizeof(ProllyHash));
}

void chunkStoreGetHeadCommit(ChunkStore *cs, ProllyHash *pHead){
  memcpy(pHead, &cs->headCommit, sizeof(ProllyHash));
}

void chunkStoreSetHeadCommit(ChunkStore *cs, const ProllyHash *pHead){
  memcpy(&cs->headCommit, pHead, sizeof(ProllyHash));
}

void chunkStoreGetStagedCatalog(ChunkStore *cs, ProllyHash *pStaged){
  memcpy(pStaged, &cs->stagedCatalog, sizeof(ProllyHash));
}

void chunkStoreSetStagedCatalog(ChunkStore *cs, const ProllyHash *pStaged){
  memcpy(&cs->stagedCatalog, pStaged, sizeof(ProllyHash));
}

/* --- Branch management --- */

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

/* --- Per-branch WorkingSet --- */

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

/* --- Tag management --- */

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

/* --- Remote management --- */

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
      /* Also delete all tracking branches for this remote */
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

/* --- Tracking branch management --- */

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
  /* Try to find existing entry first */
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      memcpy(&cs->aTracking[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  /* Create new entry */
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

/* --- Bulk has-check --- */

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult){
  int i;
  for(i=0; i<nHash; i++){
    aResult[i] = chunkStoreHas(cs, &aHash[i]) ? 1 : 0;
  }
  return SQLITE_OK;
}

/*
** Serialize refs v4: [version:1=4][default_branch_len:2][default_branch:var]
**   [num_branches:2] per branch: [name_len:2][name:var][commitHash:20][workingSetHash:20]
**   [num_tags:2] per tag: [name_len:2][name:var][hash:20]
**   [num_remotes:2] per remote: [name_len:2][name:var][url_len:2][url:var]
**   [num_tracking:2] per tracking: [remote_len:2][remote:var][branch_len:2][branch:var][commitHash:20]
*/
int chunkStoreSerializeRefs(ChunkStore *cs){
  const char *def = cs->zDefaultBranch ? cs->zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  /* version + default + num_branches + num_tags + num_remotes + num_tracking */
  int sz = 1 + 2 + defLen + 2 + 2 + 2 + 2;
  int i, rc;
  u8 *buf, *bufCur;
  ProllyHash refsHash;

  for(i=0; i<cs->nBranches; i++){
    int inc = 2 + (int)strlen(cs->aBranches[i].zName) + PROLLY_HASH_SIZE*2;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nTags; i++){
    int inc = 2 + (int)strlen(cs->aTags[i].zName) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nRemotes; i++){
    int inc = 2 + (int)strlen(cs->aRemotes[i].zName) + 2 + (int)strlen(cs->aRemotes[i].zUrl);
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nTracking; i++){
    int inc = 2 + (int)strlen(cs->aTracking[i].zRemote) + 2 + (int)strlen(cs->aTracking[i].zBranch) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  bufCur = buf;
  *bufCur++ = 4;  /* version 4: branches + tags + remotes + tracking */
  bufCur[0]=(u8)defLen; bufCur[1]=(u8)(defLen>>8); bufCur+=2;
  memcpy(bufCur, def, defLen); bufCur+=defLen;
  /* Branches: name + commitHash + workingSetHash */
  bufCur[0]=(u8)cs->nBranches; bufCur[1]=(u8)(cs->nBranches>>8); bufCur+=2;
  for(i=0; i<cs->nBranches; i++){
    int nameLen = (int)strlen(cs->aBranches[i].zName);
    bufCur[0]=(u8)nameLen; bufCur[1]=(u8)(nameLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aBranches[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aBranches[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    memcpy(bufCur, cs->aBranches[i].workingSetHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  /* Tags */
  bufCur[0]=(u8)cs->nTags; bufCur[1]=(u8)(cs->nTags>>8); bufCur+=2;
  for(i=0; i<cs->nTags; i++){
    int nameLen = (int)strlen(cs->aTags[i].zName);
    bufCur[0]=(u8)nameLen; bufCur[1]=(u8)(nameLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aTags[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aTags[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  /* Remotes */
  bufCur[0]=(u8)cs->nRemotes; bufCur[1]=(u8)(cs->nRemotes>>8); bufCur+=2;
  for(i=0; i<cs->nRemotes; i++){
    int nameLen = (int)strlen(cs->aRemotes[i].zName);
    int urlLen = (int)strlen(cs->aRemotes[i].zUrl);
    bufCur[0]=(u8)nameLen; bufCur[1]=(u8)(nameLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aRemotes[i].zName, nameLen); bufCur+=nameLen;
    bufCur[0]=(u8)urlLen; bufCur[1]=(u8)(urlLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aRemotes[i].zUrl, urlLen); bufCur+=urlLen;
  }
  /* Tracking branches */
  bufCur[0]=(u8)cs->nTracking; bufCur[1]=(u8)(cs->nTracking>>8); bufCur+=2;
  for(i=0; i<cs->nTracking; i++){
    int remoteLen = (int)strlen(cs->aTracking[i].zRemote);
    int branchLen = (int)strlen(cs->aTracking[i].zBranch);
    bufCur[0]=(u8)remoteLen; bufCur[1]=(u8)(remoteLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aTracking[i].zRemote, remoteLen); bufCur+=remoteLen;
    bufCur[0]=(u8)branchLen; bufCur[1]=(u8)(branchLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aTracking[i].zBranch, branchLen); bufCur+=branchLen;
    memcpy(bufCur, cs->aTracking[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
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
  if( version!=1 && version!=2 && version!=3 && version!=4 ) return SQLITE_CORRUPT;
  defLen = bufCur[0]|(bufCur[1]<<8); bufCur+=2;
  if( bufCur+defLen>data+nData ) return SQLITE_CORRUPT;
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = sqlite3_malloc(defLen+1);
  if(!cs->zDefaultBranch) return SQLITE_NOMEM;
  memcpy(cs->zDefaultBranch, bufCur, defLen); cs->zDefaultBranch[defLen]=0; bufCur+=defLen;
  nBranches = bufCur[0]|(bufCur[1]<<8); bufCur+=2;
  csFreeBranches(cs);
  if( nBranches>0 ){
    cs->aBranches = sqlite3_malloc(nBranches*(int)sizeof(struct BranchRef));
    if(!cs->aBranches) return SQLITE_NOMEM;
    for(i=0;i<nBranches;i++){
      int nameLen; if(bufCur+2>data+nData) return SQLITE_CORRUPT;
      nameLen=bufCur[0]|(bufCur[1]<<8); bufCur+=2;
      if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
      memset(&cs->aBranches[i], 0, sizeof(struct BranchRef));
      cs->aBranches[i].zName=sqlite3_malloc(nameLen+1);
      if(!cs->aBranches[i].zName) return SQLITE_NOMEM;
      memcpy(cs->aBranches[i].zName,bufCur,nameLen); cs->aBranches[i].zName[nameLen]=0; bufCur+=nameLen;
      memcpy(cs->aBranches[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      if( version>=3 && bufCur+PROLLY_HASH_SIZE<=data+nData ){
        memcpy(cs->aBranches[i].workingSetHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      }
      cs->nBranches++;
    }
  }

  /* Tags (version 2+) */
  csFreeTags(cs);
  if( version>=2 && bufCur+2<=data+nData ){
    nTags = bufCur[0]|(bufCur[1]<<8); bufCur+=2;
    if( nTags>0 ){
      cs->aTags = sqlite3_malloc(nTags*(int)sizeof(struct TagRef));
      if(!cs->aTags) return SQLITE_NOMEM;
      for(i=0;i<nTags;i++){
        int nameLen; if(bufCur+2>data+nData) break;
        nameLen=bufCur[0]|(bufCur[1]<<8); bufCur+=2;
        if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) break;
        cs->aTags[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->aTags[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aTags[i].zName,bufCur,nameLen); cs->aTags[i].zName[nameLen]=0; bufCur+=nameLen;
        memcpy(cs->aTags[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
        cs->nTags++;
      }
    }
  }

  /* Remotes and tracking branches (version 4+) */
  csFreeRemotes(cs);
  csFreeTracking(cs);
  if( version>=4 && bufCur+2<=data+nData ){
    int nRemotes = bufCur[0]|(bufCur[1]<<8); bufCur+=2;
    if( nRemotes>0 ){
      cs->aRemotes = sqlite3_malloc(nRemotes*(int)sizeof(struct RemoteRef));
      if(!cs->aRemotes) return SQLITE_NOMEM;
      for(i=0;i<nRemotes;i++){
        int nameLen, urlLen;
        if(bufCur+2>data+nData) break;
        nameLen=bufCur[0]|(bufCur[1]<<8); bufCur+=2;
        if(bufCur+nameLen+2>data+nData) break;
        cs->aRemotes[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->aRemotes[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aRemotes[i].zName,bufCur,nameLen); cs->aRemotes[i].zName[nameLen]=0; bufCur+=nameLen;
        urlLen=bufCur[0]|(bufCur[1]<<8); bufCur+=2;
        if(bufCur+urlLen>data+nData){ sqlite3_free(cs->aRemotes[i].zName); break; }
        cs->aRemotes[i].zUrl=sqlite3_malloc(urlLen+1);
        if(!cs->aRemotes[i].zUrl){ sqlite3_free(cs->aRemotes[i].zName); return SQLITE_NOMEM; }
        memcpy(cs->aRemotes[i].zUrl,bufCur,urlLen); cs->aRemotes[i].zUrl[urlLen]=0; bufCur+=urlLen;
        cs->nRemotes++;
      }
    }
    if( bufCur+2<=data+nData ){
      int nTracking = bufCur[0]|(bufCur[1]<<8); bufCur+=2;
      if( nTracking>0 ){
        cs->aTracking = sqlite3_malloc(nTracking*(int)sizeof(struct TrackingBranch));
        if(!cs->aTracking) return SQLITE_NOMEM;
        for(i=0;i<nTracking;i++){
          int remoteLen, branchLen;
          if(bufCur+2>data+nData) break;
          remoteLen=bufCur[0]|(bufCur[1]<<8); bufCur+=2;
          if(bufCur+remoteLen+2>data+nData) break;
          cs->aTracking[i].zRemote=sqlite3_malloc(remoteLen+1);
          if(!cs->aTracking[i].zRemote) return SQLITE_NOMEM;
          memcpy(cs->aTracking[i].zRemote,bufCur,remoteLen); cs->aTracking[i].zRemote[remoteLen]=0; bufCur+=remoteLen;
          branchLen=bufCur[0]|(bufCur[1]<<8); bufCur+=2;
          if(bufCur+branchLen+PROLLY_HASH_SIZE>data+nData){ sqlite3_free(cs->aTracking[i].zRemote); break; }
          cs->aTracking[i].zBranch=sqlite3_malloc(branchLen+1);
          if(!cs->aTracking[i].zBranch){ sqlite3_free(cs->aTracking[i].zRemote); return SQLITE_NOMEM; }
          memcpy(cs->aTracking[i].zBranch,bufCur,branchLen); cs->aTracking[i].zBranch[branchLen]=0; bufCur+=branchLen;
          memcpy(cs->aTracking[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
          cs->nTracking++;
        }
      }
    }
  }

  return SQLITE_OK;
}

/*
** Public wrapper around csDeserializeRefs: parse a refs blob into the
** ChunkStore's in-memory branch/tag/remote/tracking arrays, without
** any file I/O.  Existing arrays are freed first.
*/
int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  return csDeserializeRefs(cs, data, nData);
}

/*
** Serialize the in-memory branches/tags/remotes/tracking into a newly-
** allocated blob.  Caller must sqlite3_free(*ppOut).
** This is the serialization half of chunkStoreSerializeRefs (which
** additionally stores the blob into the chunk store).
*/
int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  const char *def = cs->zDefaultBranch ? cs->zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  int sz = 1 + 2 + defLen + 2 + 2 + 2 + 2;
  int i;
  u8 *buf, *bufCur;

  *ppOut = 0;
  *pnOut = 0;

  for(i=0; i<cs->nBranches; i++){
    int inc = 2 + (int)strlen(cs->aBranches[i].zName) + PROLLY_HASH_SIZE*2;
    if( sz > INT_MAX - inc ) return SQLITE_TOOBIG;
    sz += inc;
  }
  for(i=0; i<cs->nTags; i++){
    int inc = 2 + (int)strlen(cs->aTags[i].zName) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ) return SQLITE_TOOBIG;
    sz += inc;
  }
  for(i=0; i<cs->nRemotes; i++){
    int inc = 2 + (int)strlen(cs->aRemotes[i].zName) + 2 + (int)strlen(cs->aRemotes[i].zUrl);
    if( sz > INT_MAX - inc ) return SQLITE_TOOBIG;
    sz += inc;
  }
  for(i=0; i<cs->nTracking; i++){
    int inc = 2 + (int)strlen(cs->aTracking[i].zRemote) + 2 + (int)strlen(cs->aTracking[i].zBranch) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ) return SQLITE_TOOBIG;
    sz += inc;
  }

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  bufCur = buf;

  *bufCur++ = 4;  /* version 4 */
  bufCur[0]=(u8)defLen; bufCur[1]=(u8)(defLen>>8); bufCur+=2;
  memcpy(bufCur, def, defLen); bufCur+=defLen;

  /* Branches */
  bufCur[0]=(u8)cs->nBranches; bufCur[1]=(u8)(cs->nBranches>>8); bufCur+=2;
  for(i=0; i<cs->nBranches; i++){
    int nameLen = (int)strlen(cs->aBranches[i].zName);
    bufCur[0]=(u8)nameLen; bufCur[1]=(u8)(nameLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aBranches[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aBranches[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    memcpy(bufCur, cs->aBranches[i].workingSetHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }

  /* Tags */
  bufCur[0]=(u8)cs->nTags; bufCur[1]=(u8)(cs->nTags>>8); bufCur+=2;
  for(i=0; i<cs->nTags; i++){
    int nameLen = (int)strlen(cs->aTags[i].zName);
    bufCur[0]=(u8)nameLen; bufCur[1]=(u8)(nameLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aTags[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aTags[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }

  /* Remotes */
  bufCur[0]=(u8)cs->nRemotes; bufCur[1]=(u8)(cs->nRemotes>>8); bufCur+=2;
  for(i=0; i<cs->nRemotes; i++){
    int nameLen = (int)strlen(cs->aRemotes[i].zName);
    int urlLen = (int)strlen(cs->aRemotes[i].zUrl);
    bufCur[0]=(u8)nameLen; bufCur[1]=(u8)(nameLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aRemotes[i].zName, nameLen); bufCur+=nameLen;
    bufCur[0]=(u8)urlLen; bufCur[1]=(u8)(urlLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aRemotes[i].zUrl, urlLen); bufCur+=urlLen;
  }

  /* Tracking branches */
  bufCur[0]=(u8)cs->nTracking; bufCur[1]=(u8)(cs->nTracking>>8); bufCur+=2;
  for(i=0; i<cs->nTracking; i++){
    int remoteLen = (int)strlen(cs->aTracking[i].zRemote);
    int branchLen = (int)strlen(cs->aTracking[i].zBranch);
    bufCur[0]=(u8)remoteLen; bufCur[1]=(u8)(remoteLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aTracking[i].zRemote, remoteLen); bufCur+=remoteLen;
    bufCur[0]=(u8)branchLen; bufCur[1]=(u8)(branchLen>>8); bufCur+=2;
    memcpy(bufCur, cs->aTracking[i].zBranch, branchLen); bufCur+=branchLen;
    memcpy(bufCur, cs->aTracking[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }

  *ppOut = buf;
  *pnOut = sz;
  return SQLITE_OK;
}

/*
** Check whether a chunk with the given hash exists in the store
** (committed index or pending buffer).  Returns 1 if found, 0 if not.
*/
int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash){
  if( csSearchIndex(cs->aIndex, cs->nIndex, hash) >= 0 ) return 1;
  if( csSearchPending(cs, hash) >= 0 ) return 1;
  return 0;
}

/*
** Read a chunk by hash.
**
** On success, *ppData is set to a sqlite3_malloc'd buffer holding the
** chunk data and *pnData is its length.  The caller must sqlite3_free
** *ppData.
**
** Returns SQLITE_OK on success, SQLITE_NOTFOUND if the hash is not
** in the store, or an I/O error code.
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

  /* Check pending buffer first (most recent writes) */
  idx = csSearchPending(cs, hash);
  if( idx >= 0 ){
    ChunkIndexEntry *e = &cs->aPending[idx];
    /* e->offset is the byte offset within pWriteBuf where we stored
    ** the 4-byte length prefix + data. */
    i64 off = e->offset;
    int sz = e->size;
    u8 *pCopy = (u8 *)sqlite3_malloc(sz);
    if( pCopy == 0 ) return SQLITE_NOMEM;
    /* Skip the 4-byte length prefix */
    memcpy(pCopy, cs->pWriteBuf + off + 4, sz);
    *ppData = pCopy;
    *pnData = sz;
    return SQLITE_OK;
  }

  /* Check committed index */
  idx = csSearchIndex(cs->aIndex, cs->nIndex, hash);
  if( idx < 0 ){
    return SQLITE_NOTFOUND;
  }

  /* WAL chunk: negative offset means data is in pWalData */
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

  /* For in-memory stores, chunks in the index may reference pWriteBuf */
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

  {
    ChunkIndexEntry *e = &cs->aIndex[idx];
    i64 fileOff = e->offset;
    int sz = e->size;
    u8 lenBuf[4];
    u8 *pBuf;
    u32 storedLen;

    /* Read the 4-byte length prefix */
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

/*
** Stage a new chunk for writing.
**
** The chunk's hash is computed from pData using prollyHashCompute and
** returned in *pHash.  If the chunk already exists (committed or
** pending), this is a no-op that just fills in *pHash.
**
** Otherwise the data is appended to the in-memory write buffer and a
** new pending index entry is created.  Nothing is written to disk until
** chunkStoreCommit is called.
*/
int chunkStorePut(
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  ProllyHash *pHash
){
  int rc;
  ProllyHash h;

  /* Compute hash */
  prollyHashCompute(pData, nData, &h);
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  /* Already exists? */
  if( csSearchIndex(cs->aIndex, cs->nIndex, &h) >= 0 ) return SQLITE_OK;
  if( csSearchPending(cs, &h) >= 0 ) return SQLITE_OK;

  /* Grow pending array if needed */
  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;

  /* Grow write buffer: 4 bytes for length prefix + nData */
  rc = csGrowWriteBuf(cs, 4 + nData);
  if( rc != SQLITE_OK ) return rc;

  /* Append to pending array. Sorted lazily by csSearchPending. */
  {
    ChunkIndexEntry *e = &cs->aPending[cs->nPending];
    e->hash = h;
    e->offset = (i64)cs->nWriteBuf;
    e->size = nData;
    cs->nPending++;
    /* Hash table is incrementally updated by csSearchPending */
  }

  /* Append length + data to write buffer */
  CS_WRITE_U32(cs->pWriteBuf + cs->nWriteBuf, (u32)nData);
  cs->nWriteBuf += 4;
  memcpy(cs->pWriteBuf + cs->nWriteBuf, pData, nData);
  cs->nWriteBuf += nData;

  return SQLITE_OK;
}

/*
** csCommitToMemory -- handle the in-memory-only commit path.
** Merges pending entries into the committed index and marks the
** write buffer as committed.
*/
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
** csCommitToFile -- handle the file-based commit path.
** Locks the file, writes WAL records for pending chunks, appends a
** root record with the updated manifest, fsyncs, then updates the
** in-memory index and WAL cache.
*/
static int csCommitToFile(ChunkStore *cs){
  int rc;
  int i;

  /* Single-file commit: append WAL records to the main file at EOF.
  ** If the file doesn't exist yet, create it with a manifest header. */
  {
    int fd = open(cs->zFilename, O_WRONLY | O_CREAT, 0644);
    if( fd < 0 ) return SQLITE_CANTOPEN;

    /* Exclusive lock for concurrent writer serialization */
    if( flock(fd, LOCK_EX) != 0 ){
      close(fd);
      return SQLITE_BUSY;
    }

    /* Under lock: check if file grew (another writer appended) */
    {
      struct stat st;
      if( fstat(fd, &st)==0 && (i64)st.st_size > cs->iFileSize && cs->pFile ){
        /* Re-read WAL region to pick up other writer's chunks */
        sqlite3_free(cs->pWalData);
        cs->pWalData = 0;
        cs->nWalData = 0;
        /* Temporarily reset pending count so replay doesn't conflict */
        int savePending = cs->nPending;
        cs->nPending = 0;
    csPendHTClear(cs);
        csReplayWalRegion(cs, 1);
        cs->nPending = savePending;
      }
    }

    /* If file is empty/new, write the manifest header first */
    {
      struct stat st;
      if( fstat(fd, &st)==0 && st.st_size == 0 ){
        /* New file: write manifest at offset 0 */
        u8 manifest[CHUNK_MANIFEST_SIZE];
        cs->iWalOffset = CHUNK_MANIFEST_SIZE;
        csSerializeManifest(cs, manifest);
        lseek(fd, 0, SEEK_SET);
        if( write(fd, manifest, CHUNK_MANIFEST_SIZE) != CHUNK_MANIFEST_SIZE ){
          close(fd);
          return SQLITE_IOERR_WRITE;
        }
      }
    }

    /* Seek to end of file for appending WAL records */
    lseek(fd, 0, SEEK_END);

    /* Write chunk records: tag(1) + hash(20) + length(4) + data */
    for( i = 0; i < cs->nPending; i++ ){
      ChunkIndexEntry *pe = &cs->aPending[i];
      u8 recHdr[25];
      recHdr[0] = CS_WAL_TAG_CHUNK;
      memcpy(recHdr + 1, &pe->hash, 20);
      CS_WRITE_U32(recHdr + 21, (u32)pe->size);

      i64 bufOff = pe->offset + 4;  /* skip length prefix */
      struct iovec iov[2];
      iov[0].iov_base = recHdr;
      iov[0].iov_len = 25;
      iov[1].iov_base = cs->pWriteBuf + bufOff;
      iov[1].iov_len = pe->size;
      ssize_t n = writev(fd, iov, 2);
      if( n != (ssize_t)(25 + pe->size) ){
        close(fd);
        return SQLITE_IOERR_WRITE;
      }
    }

    /* Write root record: tag(1) + manifest(168) */
    {
      u8 rootRec[1 + CHUNK_MANIFEST_SIZE];
      rootRec[0] = CS_WAL_TAG_ROOT;
      csSerializeManifest(cs, rootRec + 1);
      ssize_t n = write(fd, rootRec, sizeof(rootRec));
      if( n != (ssize_t)sizeof(rootRec) ){
        close(fd);
        return SQLITE_IOERR_WRITE;
      }
    }

    /* Also update manifest at offset 0 for fast open (skip WAL replay) */
    {
      u8 manifest[CHUNK_MANIFEST_SIZE];
      csSerializeManifest(cs, manifest);
      lseek(fd, 0, SEEK_SET);
      if( write(fd, manifest, CHUNK_MANIFEST_SIZE) != CHUNK_MANIFEST_SIZE ){
        close(fd);
        return SQLITE_IOERR_WRITE;
      }
    }

    /* fsync — durability point */
    if( fsync(fd)!=0 ){
      close(fd);
      return SQLITE_IOERR_FSYNC;
    }

    /* Track file size */
    {
      struct stat st;
      if( fstat(fd, &st)==0 ) cs->iFileSize = (i64)st.st_size;
    }
    close(fd);
  }

  /* If we didn't have a file handle yet (first commit), open it now */
  if( cs->pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc != SQLITE_OK ) return rc;
  }

  /* Update in-memory index: move chunk data into WAL cache */
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

  /* Merge pending into main index */
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

  /* Clear pending state */
  sqlite3_free(cs->pWriteBuf);
  cs->pWriteBuf = 0;
  cs->nWriteBuf = 0;
  cs->nWriteBufAlloc = 0;
  cs->nPending = 0;
  csPendHTClear(cs);

  return SQLITE_OK;
}

/*
** Commit all pending chunks and the current root hash atomically.
**
** Protocol:
**   1. Build the merged index (old + pending, sorted by hash).
**   2. Write a complete new file to a temporary path:
**      a. 168-byte manifest header
**      b. Copy existing chunk data from the old file
**      c. Append pending chunk data from pWriteBuf
**      d. Write the merged index
**   3. fsync the temporary file.
**   4. Close the old file, rename temp over original (atomic on POSIX).
**   5. Re-open the file, reload in-memory state.
*/
int chunkStoreCommit(ChunkStore *cs){
  if( cs->readOnly ) return SQLITE_READONLY;
  if( cs->isMemory ) return csCommitToMemory(cs);
  return csCommitToFile(cs);
}

/*
** Discard all pending (uncommitted) chunks and reset the write buffer.
** The root hash is NOT reverted -- the caller is responsible for that
** if desired.
*/
void chunkStoreRollback(ChunkStore *cs){
  cs->nPending = 0;
    csPendHTClear(cs);
  if( cs->isMemory ){
    /* For in-memory stores, committed chunks live in pWriteBuf.
    ** Only discard the uncommitted portion. */
    cs->nWriteBuf = cs->nCommittedWriteBuf;
  }else{
    cs->nWriteBuf = 0;
  }
  /* Buffers are kept allocated for potential reuse */
}

/*
** Return 1 if the store is empty (root hash is all zeros), 0 otherwise.
*/
int chunkStoreIsEmpty(ChunkStore *cs){
  return prollyHashIsEmpty(&cs->root);
}

/*
** Reload refs (branches, tags, remotes, tracking) from the current refsHash.
** Used after clone to populate in-memory ref arrays from the copied refs chunk.
*/
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

/*
** Return the filename of this chunk store.
*/
const char *chunkStoreFilename(ChunkStore *cs){
  return cs->zFilename;
}


int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged){
  int bMoved = 0;
  int rc;
  *pChanged = 0;
  if( cs->isMemory ) return SQLITE_OK;

  if( cs->pFile==0 ){
    /* No file handle yet — check if file was created by another connection */
    int exists = 0;
    rc = sqlite3OsAccess(cs->pVfs, cs->zFilename,
                         SQLITE_ACCESS_EXISTS, &exists);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    if( exists ){
      struct stat mainStat;
      if( stat(cs->zFilename, &mainStat)==0 && mainStat.st_size > 0 ){
        /* File appeared with data — open and load */
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

  /* Check if file was replaced (GC) */
  rc = sqlite3OsFileControl(cs->pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
  if( rc!=SQLITE_OK ) return SQLITE_OK;

  if( !bMoved ){
    /* File not replaced — check if it grew (another writer appended WAL records) */
    i64 fileSize = 0;
    rc = sqlite3OsFileSize(cs->pFile, &fileSize);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    if( fileSize > cs->iFileSize ){
      /* File grew — re-read WAL region */
      sqlite3_free(cs->pWalData);
      cs->pWalData = 0;
      cs->nWalData = 0;
      cs->nPending = 0;
    csPendHTClear(cs);
      rc = csReplayWal(cs);
      if( rc!=SQLITE_OK ) return rc;
      cs->iFileSize = fileSize;
      *pChanged = 1;
    }
    return SQLITE_OK;
  }

  /* File was replaced (GC happened) — full reload */
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
      csDeserializeRefs(cs, refsData, nRefsData);
      sqlite3_free(refsData);
    }
  }
  if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");
  *pChanged = 1;
  return SQLITE_OK;
}

/* --- Merge state accessors --- */

/*
** Get the current merge state.  Any output pointer may be NULL.
*/
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

/*
** Set the merge state.  Pass isMerging=1 to begin a merge, 0 to clear.
** pMergeCommit and pConflictsCatalog may be NULL (treated as all-zeros).
*/
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

/*
** Clear all merge state (isMerging=0, hashes zeroed).
*/
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
