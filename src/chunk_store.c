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
** Journal file (filename-wal):
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
static void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
static void csSerializeIndexEntry(const ChunkIndexEntry *e, u8 *aBuf);
static void csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e);
static int csMergeIndex(ChunkStore *cs, ChunkIndexEntry **ppMerged,
                        int *pnMerged);
static int csGrowPending(ChunkStore *cs);
static int csGrowWriteBuf(ChunkStore *cs, int nNeeded);
static char *csJournalPath(const char *zFilename);
static char *csWalPath(const char *zFilename);
static int csReplayWalFull(ChunkStore *cs, int updateManifest);
static int csReplayWal(ChunkStore *cs){ return csReplayWalFull(cs, 1); }

/* Journal record tags */
#define CS_WAL_TAG_CHUNK  0x01   /* Chunk record: tag(1)+hash(20)+len(4)+data */
#define CS_WAL_TAG_ROOT   0x02   /* Root record: tag(1)+manifest(168) */

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
** csSearchPending -- linear search pending entries for a hash.
** Returns the index if found, or -1.
** -------------------------------------------------------------------- */
static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash){
  int i;
  for( i = 0; i < cs->nPending; i++ ){
    if( prollyHashCompare(&cs->aPending[i].hash, pHash) == 0 ){
      return i;
    }
  }
  return -1;
}

/* --------------------------------------------------------------------
** csSerializeManifest -- write the 64-byte manifest header into aBuf.
** -------------------------------------------------------------------- */
static void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + 0, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + 4, CHUNK_STORE_VERSION);
  memcpy(aBuf + 8, cs->root.data, PROLLY_HASH_SIZE);
  CS_WRITE_U32(aBuf + 28, (u32)cs->nChunks);
  CS_WRITE_I64(aBuf + 32, cs->iIndexOffset);
  CS_WRITE_U32(aBuf + 40, (u32)cs->nIndexSize);
  /* bytes 44..63: catalog hash (table registry + meta) */
  memcpy(aBuf + 44, cs->catalog.data, PROLLY_HASH_SIZE);
  /* bytes 64..83: head commit hash */
  memcpy(aBuf + 64, cs->headCommit.data, PROLLY_HASH_SIZE);
  /* bytes 84..103: staged catalog hash */
  memcpy(aBuf + 84, cs->stagedCatalog.data, PROLLY_HASH_SIZE);
  /* bytes 104..123: refs hash (branch mapping) */
  memcpy(aBuf + 104, cs->refsHash.data, PROLLY_HASH_SIZE);
  /* bytes 124..127: isMerging flag (u32, 0 or 1) */
  CS_WRITE_U32(aBuf + 124, (u32)cs->isMerging);
  /* bytes 128..147: mergeCommitHash */
  memcpy(aBuf + 128, cs->mergeCommitHash.data, PROLLY_HASH_SIZE);
  /* bytes 148..167: conflictsCatalogHash */
  memcpy(aBuf + 148, cs->conflictsCatalogHash.data, PROLLY_HASH_SIZE);
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
** csReadManifest -- read and validate the 64-byte manifest from an
** already-open file. Populates cs->root, cs->nChunks, cs->iIndexOffset,
** cs->nIndexSize.  Returns SQLITE_OK or error.
** -------------------------------------------------------------------- */
static int csReadManifest(ChunkStore *cs){
  u8 aBuf[CHUNK_MANIFEST_SIZE];
  u32 magic, version;
  int rc;

  /* Read first 8 bytes to check version, then read the appropriate size */
  rc = sqlite3OsRead(cs->pFile, aBuf, 8, 0);
  if( rc != SQLITE_OK ) return rc;

  magic = CS_READ_U32(aBuf + 0);
  version = CS_READ_U32(aBuf + 4);
  if( magic != CHUNK_STORE_MAGIC ) return SQLITE_NOTADB;
  if( version != CHUNK_STORE_VERSION && version != 4 ) return SQLITE_NOTADB;

  if( version == 4 ){
    /* V4 manifest is 124 bytes — read that, zero-fill the rest */
    rc = sqlite3OsRead(cs->pFile, aBuf, 124, 0);
    if( rc != SQLITE_OK ) return rc;
    memset(aBuf + 124, 0, CHUNK_MANIFEST_SIZE - 124);
  }else{
    rc = sqlite3OsRead(cs->pFile, aBuf, CHUNK_MANIFEST_SIZE, 0);
    if( rc != SQLITE_OK ) return rc;
  }

  memcpy(cs->root.data, aBuf + 8, PROLLY_HASH_SIZE);
  cs->nChunks = (int)CS_READ_U32(aBuf + 28);
  cs->iIndexOffset = CS_READ_I64(aBuf + 32);
  cs->nIndexSize = (int)CS_READ_U32(aBuf + 40);
  memcpy(cs->catalog.data, aBuf + 44, PROLLY_HASH_SIZE);
  memcpy(cs->headCommit.data, aBuf + 64, PROLLY_HASH_SIZE);
  memcpy(cs->stagedCatalog.data, aBuf + 84, PROLLY_HASH_SIZE);
  memcpy(cs->refsHash.data, aBuf + 104, PROLLY_HASH_SIZE);
  /* Merge state (v5+, zero for v4) */
  cs->isMerging = (u8)CS_READ_U32(aBuf + 124);
  memcpy(cs->mergeCommitHash.data, aBuf + 128, PROLLY_HASH_SIZE);
  memcpy(cs->conflictsCatalogHash.data, aBuf + 148, PROLLY_HASH_SIZE);

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

static char *csWalPath(const char *zFilename){
  int n = (int)strlen(zFilename);
  char *z = (char *)sqlite3_malloc(n + 5);  /* "-wal\0" */
  if( z ){
    memcpy(z, zFilename, n);
    memcpy(z + n, "-wal", 5);
  }
  return z;
}

/*
** Replay WAL file into the in-memory chunk index.
** For each chunk record: add to index (pointing into WAL file).
** For each root record: update manifest state.
** Called during chunkStoreOpen after reading the main file.
*/
static int csReplayWalFull(ChunkStore *cs, int updateManifest){
  char *zWal;
  int walFd;
  struct stat walStat;
  u8 *walData;
  i64 pos, walSize;

  zWal = csWalPath(cs->zFilename);
  if( !zWal ) return SQLITE_NOMEM;

  walFd = open(zWal, O_RDONLY);
  if( walFd < 0 ){
    sqlite3_free(zWal);
    return SQLITE_OK;  /* No WAL = nothing to replay */
  }

  if( fstat(walFd, &walStat) != 0 || walStat.st_size == 0 ){
    close(walFd);
    sqlite3_free(zWal);
    return SQLITE_OK;
  }
  walSize = walStat.st_size;

  /* Read entire WAL into memory for replay */
  walData = (u8*)sqlite3_malloc64(walSize);
  if( !walData ){
    close(walFd);
    sqlite3_free(zWal);
    return SQLITE_NOMEM;
  }
  {
    ssize_t n = read(walFd, walData, (size_t)walSize);
    if( n != (ssize_t)walSize ){
      sqlite3_free(walData);
      close(walFd);
      sqlite3_free(zWal);
      return SQLITE_IOERR_READ;
    }
  }
  close(walFd);

  /* Store WAL data and fd info for chunk reads later */
  cs->pWalData = walData;
  cs->nWalData = walSize;
  cs->nWalFileSize = walSize;
  cs->zWalPath = zWal;

  /* Parse records */
  pos = 0;
  while( pos < walSize ){
    u8 tag = walData[pos];
    pos++;

    if( tag == CS_WAL_TAG_CHUNK ){
      /* Chunk record: hash(20) + length(4) + data(length) */
      if( pos + 20 + 4 > walSize ) break;  /* truncated — stop */
      ProllyHash hash;
      memcpy(&hash, walData + pos, 20);
      pos += 20;
      u32 len = CS_READ_U32(walData + pos);
      pos += 4;
      if( pos + len > (u64)walSize ) break;  /* truncated */

      /* Add to in-memory index. Offset is negative to indicate WAL data. */
      /* We use a convention: offset = -(pos_of_data_in_walData) - 1
      ** so that chunkStoreGet can distinguish WAL chunks from file chunks. */
      {
        int existing = csSearchIndex(cs->aIndex, cs->nIndex, &hash);
        if( existing < 0 ){
          /* Not in main index — add as pending-like entry */
          int rc = csGrowPending(cs);
          if( rc != SQLITE_OK ){
            sqlite3_free(walData);
            cs->pWalData = 0;
            return rc;
          }
          ChunkIndexEntry *e = &cs->aPending[cs->nPending];
          memcpy(&e->hash, &hash, sizeof(ProllyHash));
          e->offset = -(i64)pos - 1;  /* negative = WAL offset */
          e->size = (int)len;
          cs->nPending++;
        }
      }
      pos += len;

    } else if( tag == CS_WAL_TAG_ROOT ){
      /* Root record: manifest(168) */
      if( pos + CHUNK_MANIFEST_SIZE > walSize ) break;
      /* Parse manifest from WAL — same layout as csReadManifest.
      ** Only update manifest state on initial load, not on refresh.
      ** During refresh, each connection keeps its own session state;
      ** we only want the new chunk data, not another connection's
      ** root/catalog/headCommit. */
      if( updateManifest ){
        u8 *m = walData + pos;
        u32 magic = CS_READ_U32(m);
        if( magic == CHUNK_STORE_MAGIC ){
          memcpy(cs->root.data, m + 8, PROLLY_HASH_SIZE);
          cs->nChunks = (int)CS_READ_U32(m + 28);
          cs->iIndexOffset = CS_READ_I64(m + 32);
          cs->nIndexSize = (int)CS_READ_U32(m + 40);
          memcpy(cs->catalog.data, m + 44, PROLLY_HASH_SIZE);
          memcpy(cs->headCommit.data, m + 64, PROLLY_HASH_SIZE);
          memcpy(cs->stagedCatalog.data, m + 84, PROLLY_HASH_SIZE);
          memcpy(cs->refsHash.data, m + 104, PROLLY_HASH_SIZE);
          cs->isMerging = (u8)CS_READ_U32(m + 124);
          memcpy(cs->mergeCommitHash.data, m + 128, PROLLY_HASH_SIZE);
          memcpy(cs->conflictsCatalogHash.data, m + 148, PROLLY_HASH_SIZE);
        }
      }
      pos += CHUNK_MANIFEST_SIZE;

    } else {
      break;  /* Unknown tag — stop replay (truncated/corrupt) */
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
  }

  /* Reload refs from the WAL-updated refsHash */
  if( !prollyHashIsEmpty(&cs->refsHash) ){
    u8 *refsData = 0; int nRefsData = 0;
    int rc2 = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
    if( rc2==SQLITE_OK && refsData ){
      /* Free old refs */
      { int k; for(k=0; k<cs->nBranches; k++) sqlite3_free(cs->aBranches[k].zName); }
      sqlite3_free(cs->aBranches);
      cs->aBranches = 0; cs->nBranches = 0;
      { int k; for(k=0; k<cs->nTags; k++) sqlite3_free(cs->aTags[k].zName); }
      sqlite3_free(cs->aTags);
      cs->aTags = 0; cs->nTags = 0;
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
  int i, j, k;

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
  i = 0;  /* index into aIndex */
  j = 0;  /* index into aPending */
  k = 0;  /* index into aMerged */
  while( i < cs->nIndex && j < cs->nPending ){
    int cmp = prollyHashCompare(&cs->aIndex[i].hash, &cs->aPending[j].hash);
    if( cmp < 0 ){
      aMerged[k++] = cs->aIndex[i++];
    }else if( cmp > 0 ){
      aMerged[k++] = cs->aPending[j++];
    }else{
      /* Duplicate hash: pending wins (shouldn't normally happen) */
      aMerged[k++] = cs->aPending[j++];
      i++;
    }
  }
  while( i < cs->nIndex ) aMerged[k++] = cs->aIndex[i++];
  while( j < cs->nPending ) aMerged[k++] = cs->aPending[j++];

  *ppMerged = aMerged;
  *pnMerged = k;
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
    cs->iAppendOffset = CHUNK_MANIFEST_SIZE;
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
    /* Open the existing file */
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc != SQLITE_OK ){
      /* Try read-only */
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

    /* Read chunk index */
    rc = csReadIndex(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    /* Load refs chunk if present */
    if( !prollyHashIsEmpty(&cs->refsHash) ){
      u8 *refsData = 0; int nRefsData = 0;
      rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
      if( rc==SQLITE_OK ){
        csDeserializeRefs(cs, refsData, nRefsData);
        sqlite3_free(refsData);
      }
    }
    if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");

    /* Set append offset: right after the last chunk, before the index.
    ** If there is no index yet, append after the manifest. */
    if( cs->iIndexOffset > 0 ){
      cs->iAppendOffset = cs->iIndexOffset;
    }else{
      cs->iAppendOffset = CHUNK_MANIFEST_SIZE;
    }
    /* Replay WAL if it exists — may update manifest state and index */
    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }
  }else{
    /* No main file. Check if WAL exists (data may be WAL-only). */
    if( !(flags & SQLITE_OPEN_CREATE) ){
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return SQLITE_CANTOPEN;
    }
    memset(&cs->root, 0, sizeof(cs->root));
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;
    cs->iAppendOffset = CHUNK_MANIFEST_SIZE;
    cs->pFile = 0;

    /* Try replaying WAL even without main file */
    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }
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
  sqlite3_free(cs->zWalPath);
  sqlite3_free(cs->zFilename);
  sqlite3_free(cs->aIndex);
  sqlite3_free(cs->aPending);
  sqlite3_free(cs->pWriteBuf);
  sqlite3_free(cs->zDefaultBranch);
  { int i; for(i=0; i<cs->nBranches; i++) sqlite3_free(cs->aBranches[i].zName); }
  sqlite3_free(cs->aBranches);
  { int i; for(i=0; i<cs->nTags; i++) sqlite3_free(cs->aTags[i].zName); }
  sqlite3_free(cs->aTags);
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
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = sqlite3_mprintf("%s", zName);
  return cs->zDefaultBranch ? SQLITE_OK : SQLITE_NOMEM;
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

/*
** Serialize refs v2: [version:1][default_branch_len:2][default_branch:var]
**   [num_branches:2] per branch: [name_len:2][name:var][hash:20]
**   [num_tags:2] per tag: [name_len:2][name:var][hash:20]
*/
int chunkStoreSerializeRefs(ChunkStore *cs){
  const char *def = cs->zDefaultBranch ? cs->zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  int sz = 1 + 2 + defLen + 2 + 2;  /* version + default + num_branches + num_tags */
  int i, rc;
  u8 *buf, *p;
  ProllyHash refsHash;

  for(i=0; i<cs->nBranches; i++) sz += 2 + (int)strlen(cs->aBranches[i].zName) + PROLLY_HASH_SIZE;
  for(i=0; i<cs->nTags; i++) sz += 2 + (int)strlen(cs->aTags[i].zName) + PROLLY_HASH_SIZE;
  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;
  *p++ = 2;  /* version 2: branches + tags */
  p[0]=(u8)defLen; p[1]=(u8)(defLen>>8); p+=2;
  memcpy(p, def, defLen); p+=defLen;
  /* Branches */
  p[0]=(u8)cs->nBranches; p[1]=(u8)(cs->nBranches>>8); p+=2;
  for(i=0; i<cs->nBranches; i++){
    int n = (int)strlen(cs->aBranches[i].zName);
    p[0]=(u8)n; p[1]=(u8)(n>>8); p+=2;
    memcpy(p, cs->aBranches[i].zName, n); p+=n;
    memcpy(p, cs->aBranches[i].commitHash.data, PROLLY_HASH_SIZE); p+=PROLLY_HASH_SIZE;
  }
  /* Tags */
  p[0]=(u8)cs->nTags; p[1]=(u8)(cs->nTags>>8); p+=2;
  for(i=0; i<cs->nTags; i++){
    int n = (int)strlen(cs->aTags[i].zName);
    p[0]=(u8)n; p[1]=(u8)(n>>8); p+=2;
    memcpy(p, cs->aTags[i].zName, n); p+=n;
    memcpy(p, cs->aTags[i].commitHash.data, PROLLY_HASH_SIZE); p+=PROLLY_HASH_SIZE;
  }
  rc = chunkStorePut(cs, buf, sz, &refsHash);
  sqlite3_free(buf);
  if( rc==SQLITE_OK ) memcpy(&cs->refsHash, &refsHash, sizeof(ProllyHash));
  return rc;
}

static int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData){
  const u8 *p = data;
  int defLen, nBranches, nTags, i;
  u8 version;
  if( nData<5 ) return SQLITE_CORRUPT;
  version = *p++;
  if( version!=1 && version!=2 ) return SQLITE_CORRUPT;
  defLen = p[0]|(p[1]<<8); p+=2;
  if( p+defLen>data+nData ) return SQLITE_CORRUPT;
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = sqlite3_malloc(defLen+1);
  if(!cs->zDefaultBranch) return SQLITE_NOMEM;
  memcpy(cs->zDefaultBranch, p, defLen); cs->zDefaultBranch[defLen]=0; p+=defLen;
  nBranches = p[0]|(p[1]<<8); p+=2;
  for(i=0;i<cs->nBranches;i++) sqlite3_free(cs->aBranches[i].zName);
  sqlite3_free(cs->aBranches); cs->aBranches=0; cs->nBranches=0;
  if( nBranches>0 ){
    cs->aBranches = sqlite3_malloc(nBranches*(int)sizeof(struct BranchRef));
    if(!cs->aBranches) return SQLITE_NOMEM;
    for(i=0;i<nBranches;i++){
      int n; if(p+2>data+nData) return SQLITE_CORRUPT;
      n=p[0]|(p[1]<<8); p+=2;
      if(p+n+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
      cs->aBranches[i].zName=sqlite3_malloc(n+1);
      if(!cs->aBranches[i].zName) return SQLITE_NOMEM;
      memcpy(cs->aBranches[i].zName,p,n); cs->aBranches[i].zName[n]=0; p+=n;
      memcpy(cs->aBranches[i].commitHash.data,p,PROLLY_HASH_SIZE); p+=PROLLY_HASH_SIZE;
      cs->nBranches++;
    }
  }

  /* Tags (version 2+) */
  for(i=0;i<cs->nTags;i++) sqlite3_free(cs->aTags[i].zName);
  sqlite3_free(cs->aTags); cs->aTags=0; cs->nTags=0;
  if( version>=2 && p+2<=data+nData ){
    nTags = p[0]|(p[1]<<8); p+=2;
    if( nTags>0 ){
      cs->aTags = sqlite3_malloc(nTags*(int)sizeof(struct TagRef));
      if(!cs->aTags) return SQLITE_NOMEM;
      for(i=0;i<nTags;i++){
        int n; if(p+2>data+nData) break;
        n=p[0]|(p[1]<<8); p+=2;
        if(p+n+PROLLY_HASH_SIZE>data+nData) break;
        cs->aTags[i].zName=sqlite3_malloc(n+1);
        if(!cs->aTags[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aTags[i].zName,p,n); cs->aTags[i].zName[n]=0; p+=n;
        memcpy(cs->aTags[i].commitHash.data,p,PROLLY_HASH_SIZE); p+=PROLLY_HASH_SIZE;
        cs->nTags++;
      }
    }
  }

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
    if( e->offset < 0 && cs->pWalData ){
      i64 walOff = -(e->offset + 1);  /* decode: offset = -(walPos) - 1 */
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

  /* Record the pending index entry.  The offset stored here is relative
  ** to the start of pWriteBuf (a buffer-local offset). During commit
  ** these are translated to file offsets. */
  {
    ChunkIndexEntry *e = &cs->aPending[cs->nPending];
    memcpy(&e->hash, &h, sizeof(ProllyHash));
    e->offset = (i64)cs->nWriteBuf;  /* offset within pWriteBuf */
    e->size = nData;
    cs->nPending++;
  }

  /* Append length + data to write buffer */
  CS_WRITE_U32(cs->pWriteBuf + cs->nWriteBuf, (u32)nData);
  cs->nWriteBuf += 4;
  memcpy(cs->pWriteBuf + cs->nWriteBuf, pData, nData);
  cs->nWriteBuf += nData;

  return SQLITE_OK;
}

/*
** Commit all pending chunks and the current root hash atomically.
**
** Protocol:
**   1. Build the merged index (old + pending, sorted by hash).
**   2. Write a complete new file to a temporary path:
**      a. 64-byte manifest header
**      b. Copy existing chunk data from the old file
**      c. Append pending chunk data from pWriteBuf
**      d. Write the merged index
**   3. fsync the temporary file.
**   4. Close the old file, rename temp over original (atomic on POSIX).
**   5. Re-open the file, reload in-memory state.
*/
int chunkStoreCommit(ChunkStore *cs){
  int rc;
  char *zJournal = 0;
  ChunkIndexEntry *aMerged = 0;
  int nMerged = 0;
  i64 writePos;
  u8 hdrBuf[CHUNK_MANIFEST_SIZE];
  u8 *indexBuf = 0;
  int indexBufSize;
  int i;
  i64 oldDataSize;      /* bytes of existing chunk data to copy */
  i64 pendingFileBase;  /* file offset where pending chunks start */

  if( cs->readOnly ) return SQLITE_READONLY;

  /* For in-memory databases, skip file I/O.  Just merge pending chunks
  ** into the in-memory index and keep the write buffer.  Chunks remain
  ** accessible via the aIndex offsets into pWriteBuf. */
  if( cs->isMemory ){
    if( cs->nPending > 0 ){
      ChunkIndexEntry *aMem = 0;
      int nMem = 0;
      int rc2 = csMergeIndex(cs, &aMem, &nMem);
      if( rc2!=SQLITE_OK ) return rc2;
      sqlite3_free(cs->aIndex);
      cs->aIndex = aMem;
      cs->nIndex = nMem;
      cs->nIndexAlloc = nMem;
      cs->nPending = 0;
      /* Record how much of pWriteBuf is now committed data */
      cs->nCommittedWriteBuf = cs->nWriteBuf;
    }
    return SQLITE_OK;
  }

  if( cs->nPending == 0 ){
    /* No new chunks — only the root/manifest may have changed.
    ** Still need to write a root record to the WAL. */
  }

  /* Append-only WAL commit: write chunk records + root record to WAL.
  ** Cost is O(new_chunks), not O(total_chunks). */
  {
    char *zWal = csWalPath(cs->zFilename);
    if( !zWal ){ rc = SQLITE_NOMEM; goto commit_error; }

    /* Open WAL for append (create if needed) with exclusive lock.
    ** The lock serializes concurrent writers so manifest state isn't
    ** clobbered. We hold the lock until after fsync. */
    int walFd = open(zWal, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if( walFd < 0 ){
      sqlite3_free(zWal);
      rc = SQLITE_CANTOPEN;
      goto commit_error;
    }
    if( flock(walFd, LOCK_EX) != 0 ){
      close(walFd);
      sqlite3_free(zWal);
      rc = SQLITE_BUSY;
      goto commit_error;
    }

    /* Under the lock, re-read WAL to pick up other writers' state.
    ** This ensures our root record builds on the latest manifest. */
    {
      struct stat walStat;
      if( fstat(walFd, &walStat)==0 && (i64)walStat.st_size > cs->nWalFileSize ){
        /* WAL grew since our last read — replay to get latest state */
        sqlite3_free(cs->pWalData);
        cs->pWalData = 0;
        cs->nWalData = 0;
        cs->nPending = 0;
        csReplayWal(cs);  /* full replay including manifest */
      }
    }

    /* Write chunk records: tag(1) + hash(20) + length(4) + data(length) */
    for( i = 0; i < cs->nPending; i++ ){
      ChunkIndexEntry *pe = &cs->aPending[i];
      u8 recHdr[25];  /* tag(1) + hash(20) + length(4) */
      recHdr[0] = CS_WAL_TAG_CHUNK;
      memcpy(recHdr + 1, &pe->hash, 20);
      CS_WRITE_U32(recHdr + 21, (u32)pe->size);

      /* pe->offset is buffer-local offset into pWriteBuf */
      i64 bufOff = pe->offset;
      /* Skip the 4-byte length prefix that chunkStorePut wrote */
      bufOff += 4;

      struct iovec iov[2];
      iov[0].iov_base = recHdr;
      iov[0].iov_len = 25;
      iov[1].iov_base = cs->pWriteBuf + bufOff;
      iov[1].iov_len = pe->size;
      ssize_t n = writev(walFd, iov, 2);
      if( n != (ssize_t)(25 + pe->size) ){
        close(walFd);
        sqlite3_free(zWal);
        rc = SQLITE_IOERR_WRITE;
        goto commit_error;
      }
    }

    /* Write root record: tag(1) + manifest(168) */
    {
      u8 rootRec[1 + CHUNK_MANIFEST_SIZE];
      rootRec[0] = CS_WAL_TAG_ROOT;
      csSerializeManifest(cs, rootRec + 1);
      ssize_t n = write(walFd, rootRec, sizeof(rootRec));
      if( n != (ssize_t)sizeof(rootRec) ){
        close(walFd);
        sqlite3_free(zWal);
        rc = SQLITE_IOERR_WRITE;
        goto commit_error;
      }
    }

    /* fsync — this is the durability point */
    fsync(walFd);
    /* Track WAL file size so we don't re-replay our own writes */
    {
      struct stat walStat;
      if( fstat(walFd, &walStat)==0 ){
        cs->nWalFileSize = (i64)walStat.st_size;
      }
    }
    close(walFd);
    sqlite3_free(zWal);
  }

  /* Update in-memory index: append chunk data to pWalData and set
  ** negative offsets for WAL-based reads. O(new_chunks) only. */
  for( i = 0; i < cs->nPending; i++ ){
    ChunkIndexEntry *pe = &cs->aPending[i];
    i64 bufOff = pe->offset + 4;  /* skip length prefix */
    int sz = pe->size;

    /* Append chunk data to in-memory WAL buffer */
    i64 newSize = cs->nWalData + sz;
    u8 *newBuf = (u8*)sqlite3_realloc64(cs->pWalData, newSize);
    if( newBuf ){
      memcpy(newBuf + cs->nWalData, cs->pWriteBuf + bufOff, sz);
      pe->offset = -(i64)cs->nWalData - 1;  /* negative = WAL offset */
      cs->pWalData = newBuf;
      cs->nWalData = newSize;
    } else {
      /* Realloc failed — abort commit to avoid use-after-free.
      ** pWriteBuf will be freed below, so any pending entries with
      ** buffer-relative offsets would become dangling pointers. */
      rc = SQLITE_NOMEM;
      goto commit_error;
    }
  }

  {
    ChunkIndexEntry *aMerged2 = 0;
    int nMerged2 = 0;
    rc = csMergeIndex(cs, &aMerged2, &nMerged2);
    if( rc == SQLITE_OK ){
      sqlite3_free(cs->aIndex);
      cs->aIndex = aMerged2;
      cs->nIndex = nMerged2;
      cs->nIndexAlloc = nMerged2;
    }
  }

  /* Clear pending state */
  sqlite3_free(cs->pWriteBuf);
  cs->pWriteBuf = 0;
  cs->nWriteBuf = 0;
  cs->nWriteBufAlloc = 0;
  cs->nPending = 0;

  /* Create a zero-byte placeholder at the user's filename so it's
  ** visible in directory listings. The actual data lives in the -wal
  ** file. GC/checkpoint will later write compacted data here. */
  if( cs->pFile==0 && cs->zFilename && !cs->isMemory ){
    int exists = 0;
    sqlite3OsAccess(cs->pVfs, cs->zFilename, SQLITE_ACCESS_EXISTS, &exists);
    if( !exists ){
      int fd = open(cs->zFilename, O_WRONLY | O_CREAT, 0644);
      if( fd >= 0 ) close(fd);
    }
  }

  return SQLITE_OK;

commit_error:
  sqlite3_free(aMerged);
  sqlite3_free(indexBuf);
  return rc;
}

/*
** Discard all pending (uncommitted) chunks and reset the write buffer.
** The root hash is NOT reverted -- the caller is responsible for that
** if desired.
*/
void chunkStoreRollback(ChunkStore *cs){
  cs->nPending = 0;
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
** Return the filename of this chunk store.
*/
const char *chunkStoreFilename(ChunkStore *cs){
  return cs->zFilename;
}


int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged){
  int bMoved = 0;
  int rc;
  *pChanged = 0;
  if( cs->isMemory ){
    return SQLITE_OK;
  }
  if( cs->pFile==0 ){
    int exists = 0;
    rc = sqlite3OsAccess(cs->pVfs, cs->zFilename,
                         SQLITE_ACCESS_EXISTS, &exists);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    /* Treat zero-byte placeholder the same as non-existent */
    if( exists ){
      struct stat mainStat;
      if( stat(cs->zFilename, &mainStat)==0 && mainStat.st_size==0 ){
        exists = 0;
      }
    }
    if( !exists ){
      /* Main file doesn't exist (or is a zero-byte placeholder).
      ** But another connection may have created a WAL file. Check the WAL
      ** file's actual size on disk
      ** vs what this connection has already processed (nWalData).
      ** Note: after this connection commits, nWalData reflects the data
      ** it wrote, so this only triggers for OTHER connections' writes. */
      char *zWal = csWalPath(cs->zFilename);
      if( zWal ){
        struct stat walStat;
        i64 walFileSize = 0;
        if( stat(zWal, &walStat)==0 ) walFileSize = (i64)walStat.st_size;
        if( walFileSize > cs->nWalFileSize ){
          /* WAL has new data from another connection. Free old buffer and
          ** re-read the whole WAL. The new buffer has the same content at
          ** the same byte positions, so existing negative-offset index
          ** entries remain valid. */
          sqlite3_free(cs->pWalData);
          cs->pWalData = 0;
          cs->nWalData = 0;
          cs->nWalFileSize = 0;
          cs->nPending = 0;
          rc = csReplayWal(cs);  /* full replay including manifest */
          if( rc!=SQLITE_OK ){ sqlite3_free(zWal); return rc; }
          *pChanged = 1;
        }
        sqlite3_free(zWal);
      }
      return SQLITE_OK;
    }
    bMoved = 1;
  }else{
    rc = sqlite3OsFileControl(cs->pFile,
                              SQLITE_FCNTL_HAS_MOVED, &bMoved);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
  }
  if( !bMoved ){
    /* Main file didn't move, but another connection may have appended
    ** to the WAL. Check WAL size against our last-known size. */
    char *zWal = csWalPath(cs->zFilename);
    if( zWal ){
      struct stat walStat;
      if( stat(zWal, &walStat)==0 && (i64)walStat.st_size > cs->nWalData ){
        /* WAL grew — re-read entirely. Existing index entries have
        ** negative offsets into pWalData; the new buffer has the same
        ** content at the same positions, so those offsets remain valid
        ** as long as the old content is preserved. csReplayWal reads
        ** the full WAL (including old data) into a new buffer, then
        ** merges any new entries into the index. */
        sqlite3_free(cs->pWalData);
        cs->pWalData = 0;
        cs->nWalData = 0;
          cs->nWalFileSize = 0;
        /* Reset pending so csReplayWalFull can use it for new WAL entries */
        cs->nPending = 0;
        rc = csReplayWal(cs);  /* full replay including manifest */
        sqlite3_free(zWal);
        if( rc!=SQLITE_OK ) return rc;
        *pChanged = 1;
      } else {
        sqlite3_free(zWal);
      }
    }
    return SQLITE_OK;
  }
  if( cs->pFile ){
    csCloseFile(cs->pFile);
    cs->pFile = 0;
  }
  sqlite3_free(cs->aIndex);
  cs->aIndex = 0; cs->nIndex = 0; cs->nIndexAlloc = 0;
  { int i; for(i=0;i<cs->nBranches;i++) sqlite3_free(cs->aBranches[i].zName); }
  sqlite3_free(cs->aBranches);
  cs->aBranches = 0; cs->nBranches = 0;
  { int i; for(i=0;i<cs->nTags;i++) sqlite3_free(cs->aTags[i].zName); }
  sqlite3_free(cs->aTags);
  cs->aTags = 0; cs->nTags = 0;
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
  if( !prollyHashIsEmpty(&cs->refsHash) ){
    u8 *refsData = 0; int nRefsData = 0;
    rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
    if( rc==SQLITE_OK ){
      csDeserializeRefs(cs, refsData, nRefsData);
      sqlite3_free(refsData);
    }
  }
  if( !cs->zDefaultBranch ) cs->zDefaultBranch = sqlite3_mprintf("main");
  if( cs->iIndexOffset > 0 ){
    cs->iAppendOffset = cs->iIndexOffset;
  }else{
    cs->iAppendOffset = CHUNK_MANIFEST_SIZE;
  }
  /* Replay WAL after refreshing from main file */
  rc = csReplayWal(cs);
  if( rc!=SQLITE_OK ) return rc;
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
