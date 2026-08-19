
#ifdef DOLTLITE_PROLLY

#include "chunk_wal.h"
#include "chunk_store.h"
#include "chunk_store_int.h"
#include "chunk_staging.h"
#include "chunk_file.h"
#include "../ext/blake3/blake3.h"
#include <string.h>

i64 walStateGetOffset(const WalState *w){
  assert( w!=0 );
  assert( w->iWalOffset>=0 );
  return w->iWalOffset;
}

i64 walStateGetDataSize(const WalState *w){
  assert( w!=0 );
  assert( w->nWalData>=0 );
  return w->nWalData;
}

void walStateSetOffset(WalState *w, i64 iOffset){
  assert( w!=0 );
  assert( iOffset>=0 );
  w->iWalOffset = iOffset;
  w->iCheckpointOffset = 0;
  w->nCheckpointIndex = 0;
  w->iCheckpointReplay = 0;
}

void walStateSetDataSize(WalState *w, i64 nData){
  assert( w!=0 );
  assert( nData>=0 );
  w->nWalData = nData;
}

static int csReadCheckpointStamp(
  const u8 *aManifest,
  i64 iRootOffset,
  WalState *pWal
){
  i64 iOffset;
  i64 nIndex;
  i64 iReplay;
  i64 iCheckpointRoot;

  if( CS_READ_U32(aManifest + CS_MANIFEST_CHECKPOINT_MAGIC_OFF)
      != CS_WAL_CHECKPOINT_MAGIC ){
    return 0;
  }
  iOffset = CS_READ_I64(aManifest + CS_MANIFEST_CHECKPOINT_OFFSET_OFF);
  nIndex = (i64)CS_READ_U32(
      aManifest + CS_MANIFEST_CHECKPOINT_SIZE_OFF);
  iReplay = CS_READ_I64(
      aManifest + CS_MANIFEST_CHECKPOINT_REPLAY_OFF);
  if( iOffset<pWal->iWalOffset || nIndex<=0
   || nIndex%CHUNK_INDEX_ENTRY_SIZE!=0
   || nIndex>INT_MAX
   || iOffset>LARGEST_INT64-CS_WAL_CHUNK_HDR_SIZE-nIndex ){
    return 0;
  }
  iCheckpointRoot = iOffset + CS_WAL_CHUNK_HDR_SIZE + nIndex;
  if( iCheckpointRoot>iRootOffset
   || iCheckpointRoot>LARGEST_INT64-(1+CHUNK_MANIFEST_SIZE)
   || iReplay<iCheckpointRoot+1+CHUNK_MANIFEST_SIZE
   || iReplay-(iCheckpointRoot+1+CHUNK_MANIFEST_SIZE)>=65536 ){
    return 0;
  }
  pWal->iCheckpointOffset = iOffset;
  pWal->nCheckpointIndex = nIndex;
  pWal->iCheckpointReplay = iReplay;
  return 1;
}

void csStampWalCheckpoint(const ChunkStore *cs, u8 *aManifest){
  if( cs->wal.iCheckpointOffset<=0 || cs->wal.nCheckpointIndex<=0
   || cs->wal.iCheckpointReplay<=0 ){
    return;
  }
  assert( cs->wal.nCheckpointIndex<=0xffffffffu );
  CS_WRITE_U32(aManifest + CS_MANIFEST_CHECKPOINT_MAGIC_OFF,
               CS_WAL_CHECKPOINT_MAGIC);
  CS_WRITE_I64(aManifest + CS_MANIFEST_CHECKPOINT_OFFSET_OFF,
               cs->wal.iCheckpointOffset);
  CS_WRITE_U32(aManifest + CS_MANIFEST_CHECKPOINT_SIZE_OFF,
               (u32)cs->wal.nCheckpointIndex);
  CS_WRITE_I64(aManifest + CS_MANIFEST_CHECKPOINT_REPLAY_OFF,
               cs->wal.iCheckpointReplay);
}

static i64 csWalCheckpointThreshold(void){
  i64 n = 64*1024*1024;
#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
  const char *z = getenv("DOLTLITE_WAL_CHECKPOINT_THRESHOLD");
  if( z && z[0] ){
    i64 v = (i64)strtoll(z, 0, 10);
    if( v>0 ) n = v;
  }
#endif
  return n;
}

int csWalCheckpointDue(const ChunkStore *cs){
  i64 iBase = cs->wal.iCheckpointReplay>0
            ? cs->wal.iCheckpointReplay : cs->wal.iWalOffset;
  /* Ref-less raw stores retain eager verification of every WAL chunk. */
  return !prollyHashIsEmpty(&cs->refs.refsHash)
      && iBase>0 && cs->file.iFileSize>=iBase
      && cs->file.iFileSize-iBase>=csWalCheckpointThreshold();
}

static void csSerializeCheckpointEntry(u8 *aBuf, const ChunkIndexEntry *pEntry){
  memcpy(aBuf, pEntry->hash.data, PROLLY_HASH_SIZE);
  CS_WRITE_I64(aBuf + PROLLY_HASH_SIZE, pEntry->offset);
  CS_WRITE_U32(aBuf + PROLLY_HASH_SIZE + 8, (u32)pEntry->size);
}

static int csDeserializeCheckpointEntry(
  const u8 *aBuf,
  ChunkIndexEntry *pEntry
){
  u32 n = CS_READ_U32(aBuf + PROLLY_HASH_SIZE + 8);
  if( n>0x7fffffffu ) return SQLITE_CORRUPT;
  memcpy(pEntry->hash.data, aBuf, PROLLY_HASH_SIZE);
  pEntry->offset = CS_READ_I64(aBuf + PROLLY_HASH_SIZE);
  pEntry->size = (int)n;
  return SQLITE_OK;
}

static int csBuildCheckpointIndex(
  ChunkStore *cs,
  ChunkIndexEntry **ppIndex,
  int *pnIndex
){
  i64 nAlloc = (i64)cs->index.nIndex + cs->staging.nRecent;
  ChunkIndexEntry *aIndex;
  int i;
  int nOut = 0;

  *ppIndex = 0;
  *pnIndex = 0;
  if( nAlloc<=0 || nAlloc>INT_MAX/(int)sizeof(ChunkIndexEntry) ){
    return nAlloc<=0 ? SQLITE_OK : SQLITE_TOOBIG;
  }
  aIndex = sqlite3_malloc64(
      (sqlite3_uint64)nAlloc * sizeof(ChunkIndexEntry));
  if( !aIndex ) return SQLITE_NOMEM;
  if( cs->index.nIndex>0 ){
    memcpy(aIndex, cs->index.aIndex,
           cs->index.nIndex * sizeof(ChunkIndexEntry));
  }
  if( cs->staging.nRecent>0 ){
    memcpy(aIndex + cs->index.nIndex, cs->staging.aRecent,
           cs->staging.nRecent * sizeof(ChunkIndexEntry));
  }
  qsort(aIndex, (size_t)nAlloc, sizeof(ChunkIndexEntry), csIndexEntryCmp);
  for(i=0; i<(int)nAlloc; i++){
    if( nOut==0
     || prollyHashCompare(&aIndex[nOut-1].hash, &aIndex[i].hash)!=0 ){
      aIndex[nOut++] = aIndex[i];
    }
  }
  *ppIndex = aIndex;
  *pnIndex = nOut;
  return SQLITE_OK;
}

int csWriteWalCheckpoint(ChunkStore *cs, int sectorSize){
  ChunkIndexEntry *aIndex = 0;
  int nIndex = 0;
  i64 nIndexBytes;
  i64 iCheckpoint;
  i64 iRoot;
  i64 iRootEnd;
  i64 iNext;
  u8 aEntry[CHUNK_INDEX_ENTRY_SIZE];
  u8 aBuf[65536];
  u8 aHeader[CS_WAL_CHUNK_HDR_SIZE];
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  ProllyHash snapshotHash;
  blake3_hasher hasher;
  int i;
  int rc;

  rc = csBuildCheckpointIndex(cs, &aIndex, &nIndex);
  if( rc!=SQLITE_OK || nIndex==0 ) goto checkpoint_done;
  nIndexBytes = (i64)nIndex * CHUNK_INDEX_ENTRY_SIZE;
  if( nIndexBytes>INT_MAX
   || cs->file.iFileSize>LARGEST_INT64-CS_WAL_CHUNK_HDR_SIZE-nIndexBytes ){
    rc = SQLITE_TOOBIG;
    goto checkpoint_done;
  }

  blake3_hasher_init(&hasher);
  for(i=0; i<nIndex; i++){
    csSerializeCheckpointEntry(aEntry, &aIndex[i]);
    blake3_hasher_update(&hasher, aEntry, sizeof(aEntry));
  }
  blake3_hasher_finalize(&hasher, snapshotHash.data, PROLLY_HASH_SIZE);

  iCheckpoint = cs->file.iFileSize;
  csFillChunkHdr(aHeader, &snapshotHash, (u32)nIndexBytes);
  rc = sqlite3OsWrite(cs->file.pFile, aHeader, sizeof(aHeader), iCheckpoint);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;
  for(i=0; i<nIndex; ){
    int nEntry = nIndex-i;
    int j;
    if( nEntry>(int)(sizeof(aBuf)/CHUNK_INDEX_ENTRY_SIZE) ){
      nEntry = (int)(sizeof(aBuf)/CHUNK_INDEX_ENTRY_SIZE);
    }
    for(j=0; j<nEntry; j++){
      csSerializeCheckpointEntry(
          aBuf + j*CHUNK_INDEX_ENTRY_SIZE, &aIndex[i+j]);
    }
    rc = sqlite3OsWrite(cs->file.pFile, aBuf,
        nEntry*CHUNK_INDEX_ENTRY_SIZE,
        iCheckpoint + CS_WAL_CHUNK_HDR_SIZE
        + (i64)i * CHUNK_INDEX_ENTRY_SIZE);
    if( rc!=SQLITE_OK ) goto checkpoint_rollback;
    i += nEntry;
  }
  rc = csSyncFile(cs);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;

  iRoot = iCheckpoint + CS_WAL_CHUNK_HDR_SIZE + nIndexBytes;
  iRootEnd = iRoot + (i64)sizeof(aRoot);
  iNext = iRootEnd;
  if( sectorSize>1 ){
    iNext += sectorSize - 1;
    iNext -= iNext % sectorSize;
  }
  aRoot[0] = CS_WAL_TAG_ROOT;
  csSerializeManifest(cs, aRoot + 1);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_DURABLE_TO_OFF, iRoot);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_NEXT_OFF_OFF, iNext);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_BATCH_START_OFF, iRoot);
  CS_WRITE_U32(aRoot + 1 + CS_MANIFEST_CHECKPOINT_MAGIC_OFF,
               CS_WAL_CHECKPOINT_MAGIC);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_CHECKPOINT_OFFSET_OFF, iCheckpoint);
  CS_WRITE_U32(aRoot + 1 + CS_MANIFEST_CHECKPOINT_SIZE_OFF,
               (u32)nIndexBytes);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_CHECKPOINT_REPLAY_OFF, iNext);
  csManifestSeal(aRoot + 1, iRoot);
  rc = sqlite3OsWrite(cs->file.pFile, aRoot, sizeof(aRoot), iRoot);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;
  rc = csSyncFile(cs);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;

  cs->wal.iCheckpointOffset = iCheckpoint;
  cs->wal.nCheckpointIndex = nIndexBytes;
  cs->wal.iCheckpointReplay = iNext;
  cs->file.iFileSize = iNext;
  cs->wal.nWalData = iRootEnd - cs->wal.iWalOffset;
  cs->wal.cleanCloseMarker = 1;
  rc = SQLITE_OK;
  goto checkpoint_done;

checkpoint_rollback:
  (void)sqlite3OsTruncate(cs->file.pFile, cs->file.iFileSize);
  (void)csSyncFile(cs);

checkpoint_done:
  sqlite3_free(aIndex);
  return rc;
}

static void csCaptureReplayState(ChunkStore *cs, ChunkStoreReplayState *pSaved){
  assert( cs!=0 && pSaved!=0 );
  memset(pSaved, 0, sizeof(*pSaved));
  pSaved->aIndex = cs->index.aIndex;
  pSaved->nIndex = cs->index.nIndex;
  pSaved->aIndexMmapBase = cs->index.aIndexMmapBase;
  pSaved->aIndexMmapSize = cs->index.aIndexMmapSize;
  csCaptureSavedRefsState(cs, &pSaved->refs);
}

static void csRestoreReplayState(ChunkStore *cs, const ChunkStoreReplayState *pSaved){
  cs->index.aIndex = pSaved->aIndex;
  cs->index.nIndex = pSaved->nIndex;
  cs->index.aIndexMmapBase = pSaved->aIndexMmapBase;
  cs->index.aIndexMmapSize = pSaved->aIndexMmapSize;
  csRestoreSavedRefsState(cs, &pSaved->refs);
}

void csCaptureReloadState(ChunkStore *cs, ChunkStoreReloadState *pSaved){
  memset(pSaved, 0, sizeof(*pSaved));
  pSaved->pFile = cs->file.pFile;
  pSaved->aIndex = cs->index.aIndex;
  pSaved->aIndexMmapBase = cs->index.aIndexMmapBase;
  pSaved->aIndexMmapSize = cs->index.aIndexMmapSize;
  csCaptureSavedRefsState(cs, &pSaved->refs);
}

static void csReleaseReplayState(
  ChunkStore *cs,
  ChunkStoreReplayState *pSaved
){
  if( cs->index.aIndex!=pSaved->aIndex ){
    csReleaseIndexBuf(pSaved->aIndex, pSaved->aIndexMmapBase,
                       pSaved->aIndexMmapSize);
  }
  csFreeSavedRefsState(&pSaved->refs);
  memset(pSaved, 0, sizeof(*pSaved));
}

static void csRollbackReplayState(
  ChunkStore *cs,
  ChunkStoreReplayState *pSaved,
  int nPendingBefore
){
  if( cs->index.aIndex!=pSaved->aIndex ){
    csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase, cs->index.aIndexMmapSize);
  }
  csRestoreReplayState(cs, pSaved);
  cs->staging.nPending = nPendingBefore;
  csPendHTClear(cs);
}

void csAdoptOpenedStoreState(ChunkStore *pDst, ChunkStore *pSrc){
  sqlite3_free(pDst->staging.aRecent);
  pDst->staging.aRecent = 0;
  pDst->staging.nRecent = 0;
  pDst->staging.nRecentAlloc = 0;
  pDst->staging.nRecentUncommitted = 0;
  pDst->staging.iUncommittedStart = 0;
  csRecentHTClear(pDst);

  pDst->file.pFile = pSrc->file.pFile;
  pDst->readOnly = pSrc->readOnly;
  pDst->refs.refsHash = pSrc->refs.refsHash;
  pDst->refs.committedRefsHash = pSrc->refs.committedRefsHash;
  pDst->index.nChunks = pSrc->index.nChunks;
  pDst->index.iIndexOffset = pSrc->index.iIndexOffset;
  pDst->index.nIndexSize = pSrc->index.nIndexSize;
  pDst->wal.iWalOffset = pSrc->wal.iWalOffset;
  pDst->file.iFileSize = pSrc->file.iFileSize;
  pDst->index.aIndex = pSrc->index.aIndex;
  pDst->index.nIndex = pSrc->index.nIndex;
  pDst->index.aIndexMmapBase = pSrc->index.aIndexMmapBase;
  pDst->index.aIndexMmapSize = pSrc->index.aIndexMmapSize;
  pDst->wal.nWalData = pSrc->wal.nWalData;
  pDst->wal.iCheckpointOffset = pSrc->wal.iCheckpointOffset;
  pDst->wal.nCheckpointIndex = pSrc->wal.nCheckpointIndex;
  pDst->wal.iCheckpointReplay = pSrc->wal.iCheckpointReplay;
  pDst->wal.recoveredMidStream = pSrc->wal.recoveredMidStream;
  pDst->wal.cleanCloseMarker = pSrc->wal.cleanCloseMarker;
  pDst->corruptMidStream = pSrc->corruptMidStream;
  pDst->notADatabase = pSrc->notADatabase;
  REFS_OWNED_COPY(pDst->refs, pSrc->refs);

  pSrc->file.pFile = 0;
  pSrc->index.aIndex = 0;
  pSrc->index.nIndex = 0;
  pSrc->index.aIndexMmapBase = 0;
  pSrc->index.aIndexMmapSize = 0;
  pSrc->wal.nWalData = 0;
  pSrc->wal.iCheckpointOffset = 0;
  pSrc->wal.nCheckpointIndex = 0;
  pSrc->wal.iCheckpointReplay = 0;
  pSrc->wal.cleanCloseMarker = 0;
  REFS_OWNED_CLEAR(pSrc->refs);
}

void csFreeReloadState(ChunkStoreReloadState *pSaved){
  csCloseFile(pSaved->pFile);
  csReleaseIndexBuf(pSaved->aIndex, pSaved->aIndexMmapBase,
                     pSaved->aIndexMmapSize);
  csFreeSavedRefsState(&pSaved->refs);
  memset(pSaved, 0, sizeof(*pSaved));
}

/* Cap the zero-tail probe; valid writer gaps are sector-bounded, so a
** legitimate preallocated tail never hides data past this window. */
#define CS_WAL_SCAN_MAX (64*1024*1024)

/* True if the WAL region from pos to walSize is zero as far as the scan
** cap reaches. */
static int csWalTailIsZero(ChunkStore *cs, i64 pos, i64 walSize){
  u8 buf[4096];
  i64 off = cs->wal.iWalOffset + pos;
  i64 end = cs->wal.iWalOffset + walSize;
  if( end - off > CS_WAL_SCAN_MAX ) end = off + CS_WAL_SCAN_MAX;
  while( off < end ){
    int n = (end - off) > (i64)sizeof(buf) ? (int)sizeof(buf) : (int)(end - off);
    int i;
    if( sqlite3OsRead(cs->file.pFile, buf, n, off) != SQLITE_OK ) return 0;
    for(i=0; i<n; i++){
      if( buf[i] ) return 0;
    }
    off += n;
  }
  return 1;
}

#define CS_DAMAGE_TORN      0
#define CS_DAMAGE_MIDSTREAM 1
#define CS_DAMAGE_RESUME    2

/* Classify WAL damage as committed corruption, declared gap, or torn tail. */
static int csWalResolveDamage(
  ChunkStore *cs,
  i64 damagePos,
  i64 walSize,
  int *pAction,
  i64 *pResume
){
  u8 buf[65536];
  i64 q = damagePos + 1;
  i64 last = walSize - 5;
  i64 damageAbs = cs->wal.iWalOffset + damagePos;
  /* The scan must reach the end of the WAL: a single batch can span far
  ** more than any fixed window (drains plus large chunks), and a sealed
  ** root past a capped window proves the damaged region was committed.
  ** Stopping early would default that case to TORN, silently rewinding
  ** committed batches that the next append then overwrites. */
  *pAction = CS_DAMAGE_TORN;
  *pResume = 0;
  while( q <= last ){
    i64 nAvail = walSize - q;
    int n = nAvail > (i64)sizeof(buf) ? (int)sizeof(buf) : (int)nAvail;
    int i;
    int rc = sqlite3OsRead(cs->file.pFile, buf, n, cs->wal.iWalOffset + q);
    if( rc != SQLITE_OK ) return rc;
    for(i=0; i+5<=n && q+i<=last; i++){
      if( buf[i]==CS_WAL_TAG_ROOT && CS_READ_U32(buf+i+1)==CHUNK_STORE_MAGIC ){
        u8 m[CHUNK_MANIFEST_SIZE];
        i64 cand = q + i;
        int state;
        if( cand + 1 + CHUNK_MANIFEST_SIZE > walSize ) continue;
        rc = sqlite3OsRead(cs->file.pFile, m, CHUNK_MANIFEST_SIZE,
                           cs->wal.iWalOffset + cand + 1);
        if( rc != SQLITE_OK ) return rc;
        state = csManifestHashState(m, cs->wal.iWalOffset + cand);
        if( state==CS_MANIFEST_HASH_OK
         && csValidateWalRootManifest(
              cs, m, cs->wal.iWalOffset+cand)==SQLITE_OK ){
          i64 durableTo = CS_READ_I64(m + CS_MANIFEST_DURABLE_TO_OFF);
          i64 batchStart = CS_READ_I64(m + CS_MANIFEST_BATCH_START_OFF);
          if( durableTo > damageAbs ){
            cs->wal.recoveredMidStream = 1;
            *pAction = CS_DAMAGE_MIDSTREAM;
            return SQLITE_OK;
          }
          if( batchStart > damageAbs ){
            *pAction = CS_DAMAGE_RESUME;
            *pResume = batchStart - cs->wal.iWalOffset;
            return SQLITE_OK;
          }
          /* A root of the damaged batch itself; keep scanning. */
        }
      }
    }
    if( (i64)n >= nAvail ) break;
    q += n - 4;
  }
  return SQLITE_OK;
}

static int csReplayWalFrom(ChunkStore *cs, i64 iStart){
  i64 walSize;
  ChunkStoreReplayState saved;
  i64 pos;
  i64 lastBoundary;
  i64 resumePos = 0;
  int damageAction = 0;
  int sawMidStream = 0;
  int nPendingBefore;
  int nRootedPending;
  int nRootRecordsSeen = 0;
  int sawDamage = 0;
  ChunkStore tmpRefs;
  int haveTmpRefs = 0;
  int rc = SQLITE_OK;

  assert( cs!=0 );
  nPendingBefore = cs->staging.nPending;
  nRootedPending = cs->staging.nPending;
  memset(&tmpRefs, 0, sizeof(tmpRefs));

  if( cs->wal.iWalOffset <= 0 || !cs->file.pFile ) return SQLITE_OK;
  if( iStart<cs->wal.iWalOffset ) return SQLITE_CORRUPT;

  {
    i64 fileSize = 0;
    int rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
    walSize = fileSize - cs->wal.iWalOffset;
    cs->file.iFileSize = fileSize;
  }
  if( walSize <= 0 ){
    /* Without a WAL tail, chunk_count must describe the compacted index. */
    if( cs->index.nIndex > 0 && cs->index.nChunks != cs->index.nIndex ){
      return SQLITE_CORRUPT;
    }
    if( cs->index.nIndex==0 && cs->index.nChunks==0
     && !prollyHashIsEmpty(&cs->refs.refsHash) ){
      memset(cs->refs.refsHash.data, 0, PROLLY_HASH_SIZE);
    }
    return SQLITE_OK;
  }

  csCaptureReplayState(cs, &saved);

  cs->wal.nWalData = walSize;
  lastBoundary = iStart - cs->wal.iWalOffset;
  cs->wal.cleanCloseMarker = lastBoundary>=walSize;

  pos = lastBoundary;
  while( pos < walSize ){
    u8 aPrefix[CS_WAL_CHUNK_HDR_SIZE];
    u8 tag = 0;
    i64 recPos = pos;
    int havePrefix = 0;
    if( walSize - pos < CS_WAL_CHUNK_HDR_SIZE ){
      rc = sqlite3OsRead(cs->file.pFile, &tag, 1, cs->wal.iWalOffset + pos);
      if( rc != SQLITE_OK ) goto replay_error;
      pos++;
    }else{
      rc = sqlite3OsRead(cs->file.pFile, aPrefix, sizeof(aPrefix),
                         cs->wal.iWalOffset + pos);
      if( rc != SQLITE_OK ) goto replay_error;
      tag = aPrefix[0];
      pos += CS_WAL_CHUNK_HDR_SIZE;
      havePrefix = 1;
    }

    if( tag == CS_WAL_TAG_CHUNK ){
      ProllyHash hash;
      u32 len;
      if( !havePrefix ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: WAL chunk header truncated at offset %lld; "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + recPos));
        rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
        if( rc != SQLITE_OK ) goto replay_error;
        sawDamage = 1;
        if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
        sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
        break;
      }
      memcpy(&hash, aPrefix + CS_WAL_CHUNK_HASH_OFF, PROLLY_HASH_SIZE);
      len = CS_READ_U32(aPrefix + CS_WAL_CHUNK_LEN_OFF);
      if( pos < 0 || len > (u32)0x7fffffff
       || (u64)pos + len > (u64)walSize ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: WAL chunk body truncated at offset %lld (declared len=%u); "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + pos - 24 - 1), (unsigned)len);
        rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
        if( rc != SQLITE_OK ) goto replay_error;
        sawDamage = 1;
        if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
        sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
        break;
      }

      {
        blake3_hasher hasher;
        ProllyHash bodyHash;
        u8 chunkBuf[65536];
        u32 nRemaining = len;
        i64 readOff = cs->wal.iWalOffset + pos;
        int hashMismatch = 0;
        blake3_hasher_init(&hasher);
        while( nRemaining > 0 ){
          u32 toRead = nRemaining > sizeof(chunkBuf)
                     ? (u32)sizeof(chunkBuf) : nRemaining;
          rc = sqlite3OsRead(cs->file.pFile, chunkBuf, (int)toRead, readOff);
          if( rc != SQLITE_OK ) goto replay_error;
          blake3_hasher_update(&hasher, chunkBuf, (size_t)toRead);
          readOff += toRead;
          nRemaining -= toRead;
        }
        blake3_hasher_finalize(&hasher, bodyHash.data, PROLLY_HASH_SIZE);
        if( prollyHashCompare(&bodyHash, &hash) != 0 ) hashMismatch = 1;
        if( hashMismatch ){
          sqlite3_log(SQLITE_NOTICE,
            "doltlite: WAL chunk body hash mismatch at offset %lld (declared len=%u); "
            "stopping replay at last commit boundary",
            (long long)(cs->wal.iWalOffset + pos - 24 - 1), (unsigned)len);
          rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
          if( rc != SQLITE_OK ) goto replay_error;
          sawDamage = 1;
          if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
          sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
          break;
        }
      }
      {
        int existing = csSearchIndex(cs->index.aIndex, cs->index.nIndex, &hash);
        ChunkIndexEntry *e = 0;
        if( existing < 0 ){
          rc = csGrowPending(cs);
          if( rc != SQLITE_OK ) goto replay_error;
          e = &cs->staging.aPending[cs->staging.nPending];
          memcpy(&e->hash, &hash, sizeof(ProllyHash));
          e->offset = cs->wal.iWalOffset + (i64)(pos - 4);
          e->size = (int)len;
          cs->staging.nPending++;
        }
      }
      pos += len;

    } else if( tag == CS_WAL_TAG_ROOT ){
      u8 m[CHUNK_MANIFEST_SIZE];
      int hashState;
      if( !havePrefix || recPos + 1 + CHUNK_MANIFEST_SIZE > walSize ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: WAL root manifest truncated at offset %lld; "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + recPos));
        rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
        if( rc != SQLITE_OK ) goto replay_error;
        sawDamage = 1;
        if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
        sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
        break;
      }
      memcpy(m, aPrefix + 1, CS_WAL_CHUNK_HDR_SIZE - 1);
      rc = sqlite3OsRead(cs->file.pFile, m + CS_WAL_CHUNK_HDR_SIZE - 1,
                         CHUNK_MANIFEST_SIZE - (CS_WAL_CHUNK_HDR_SIZE - 1),
                         cs->wal.iWalOffset + pos);
      if( rc != SQLITE_OK ) goto replay_error;
      hashState = csManifestHashState(m, cs->wal.iWalOffset + recPos);
      /* Offsetless v12 seals are safe only at a sequentially parsed boundary;
      ** damage scans must require an offset-bound seal. */
      if( hashState==CS_MANIFEST_HASH_BAD
       && csManifestHashStateOffsetless(m)==CS_MANIFEST_HASH_OK ){
        hashState = CS_MANIFEST_HASH_OK;
      }
      if( CS_READ_U32(m) != CHUNK_STORE_MAGIC
       || hashState == CS_MANIFEST_HASH_BAD ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: damaged WAL root manifest at offset %lld; "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + recPos));
        rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
        if( rc != SQLITE_OK ) goto replay_error;
        sawDamage = 1;
        if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
        sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
        break;
      }
      if( hashState==CS_MANIFEST_HASH_OK
       && csValidateWalRootManifest(
            cs, m, cs->wal.iWalOffset+recPos)!=SQLITE_OK ){
        sqlite3_log(SQLITE_CORRUPT,
          "doltlite: invalid sealed WAL root manifest at offset %lld",
          (long long)(cs->wal.iWalOffset + recPos));
        cs->corruptMidStream = 1;
        rc = SQLITE_CORRUPT;
        goto replay_error;
      }

      cs->index.nChunks = (int)CS_READ_U32(m + CS_MANIFEST_CHUNK_COUNT_OFF);
      memcpy(cs->refs.refsHash.data, m + CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);

      pos = recPos + 1 + CHUNK_MANIFEST_SIZE;
      if( hashState == CS_MANIFEST_HASH_OK ){
        i64 recAbs = cs->wal.iWalOffset + recPos;
        i64 durableTo = CS_READ_I64(m + CS_MANIFEST_DURABLE_TO_OFF);
        i64 batchStart = CS_READ_I64(m + CS_MANIFEST_BATCH_START_OFF);
        i64 nextOff = CS_READ_I64(m + CS_MANIFEST_NEXT_OFF_OFF);
        cs->wal.cleanCloseMarker = durableTo >= recAbs && batchStart == recAbs;
        if( nextOff > cs->wal.iWalOffset + pos ){
          pos = nextOff - cs->wal.iWalOffset;
        }
        (void)csReadCheckpointStamp(m, recAbs, &cs->wal);
      }else{
        cs->wal.cleanCloseMarker = 0;
      }
      lastBoundary = pos;
      nRootedPending = cs->staging.nPending;
      nRootRecordsSeen++;

    } else if( tag == 0 && csWalTailIsZero(cs, recPos, walSize) ){
      /* SQLITE_FCNTL_SIZE_HINT (or filesystem preallocation) zero-extends the
      ** file past the last commit. Treat the all-zero tail as absent: the
      ** post-loop rewind lets the next append reclaim it. */
      break;

    } else {
      sqlite3_log(SQLITE_NOTICE,
        "doltlite: unrecognized WAL record tag 0x%02x at offset %lld; "
        "stopping replay at last commit boundary",
        (int)tag, (long long)(cs->wal.iWalOffset + recPos));
      rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
      if( rc != SQLITE_OK ) goto replay_error;
      sawDamage = 1;
      if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
      /* Foreign bytes at WAL start are corruption; later junk is crash tail. */
      if( damageAction==CS_DAMAGE_TORN && recPos == 0 && tag != 0 ){
        rc = SQLITE_CORRUPT;
        goto replay_error;
      }
      sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
      break;
    }
  }

  /* Do not let damaged initial WAL bytes masquerade as a fresh empty store.
  ** A legitimate preallocated tail reaches the zero-tail case without setting
  ** sawDamage; once non-zero WAL bytes are malformed, the file is corrupt even
  ** if no root manifest can be replayed. */
  if( sawDamage
   && nRootRecordsSeen == 0
   && nPendingBefore == 0 && cs->index.nIndex == 0 ){
    memset(cs->refs.refsHash.data, 0, PROLLY_HASH_SIZE);
    cs->index.nChunks = 0;
    cs->wal.recoveredMidStream = 1;
    sawMidStream = 1;
  }

  /* Reclaim uncommitted tail bytes unless the store is poisoned. */
  if( !sawMidStream ){
    cs->wal.nWalData = lastBoundary < walSize ? lastBoundary : walSize;
    cs->file.iFileSize = cs->wal.iWalOffset + lastBoundary;
  }

  cs->staging.nPending = nRootedPending;

  if( nRootRecordsSeen == 0
   && nPendingBefore == 0 && cs->index.nIndex == 0
   && !sawMidStream && !cs->wal.recoveredMidStream ){
    memset(cs->refs.refsHash.data, 0, PROLLY_HASH_SIZE);
    cs->index.nChunks = 0;
  }

  if( cs->staging.nPending > 0 ){
    ChunkIndexEntry *aMerged = 0;
    int nMerged = 0;
    rc = csMergeIndex(cs, &aMerged, &nMerged);
    if( rc != SQLITE_OK ) goto replay_error;
    cs->index.aIndex = aMerged;
    cs->index.nIndex = nMerged;
    cs->index.aIndexMmapBase = 0;
    cs->index.aIndexMmapSize = 0;
    cs->staging.nPending = 0;
    csPendHTClear(cs);
  }

  if( !prollyHashIsEmpty(&cs->refs.refsHash) ){
    u8 *refsData = 0;
    int nRefsData = 0;
    int rc2 = chunkStoreGet(cs, &cs->refs.refsHash, &refsData, &nRefsData);
    if( rc2==SQLITE_OK && refsData ){
      rc2 = csReplaceRefsStateFromBlob(&tmpRefs, refsData, nRefsData, 0);
      sqlite3_free(refsData);
      if( rc2!=SQLITE_OK ){
        rc = rc2;
        goto replay_error;
      }
      haveTmpRefs = 1;
    }else if( rc2!=SQLITE_OK ){
      rc = rc2;
      goto replay_error;
    }
  }
  if( haveTmpRefs ){
    csFreeRefsState(cs);
    csAdoptRefsState(cs, &tmpRefs);
    haveTmpRefs = 0;
  }
  rc = csEnsureDefaultBranch(cs);
  if( rc!=SQLITE_OK ) goto replay_error;

  csReleaseReplayState(cs, &saved);
  return SQLITE_OK;

replay_error:
  if( haveTmpRefs ) csFreeRefsState(&tmpRefs);
  csRollbackReplayState(cs, &saved, nPendingBefore);
  return rc;
}

int csReplayWal(ChunkStore *cs){
  cs->wal.iCheckpointOffset = 0;
  cs->wal.nCheckpointIndex = 0;
  cs->wal.iCheckpointReplay = 0;
  return csReplayWalFrom(cs, cs->wal.iWalOffset);
}

static int csCheckpointIndexInsert(
  ChunkIndexEntry *aIndex,
  int nIndex,
  const ChunkIndexEntry *pEntry
){
  int lo = 0;
  int hi = nIndex;
  while( lo<hi ){
    int mid = lo + (hi-lo)/2;
    int cmp = prollyHashCompare(&aIndex[mid].hash, &pEntry->hash);
    if( cmp<0 ) lo = mid + 1;
    else hi = mid;
  }
  if( lo<nIndex
   && prollyHashCompare(&aIndex[lo].hash, &pEntry->hash)==0 ){
    return nIndex;
  }
  memmove(aIndex + lo + 1, aIndex + lo,
          (nIndex-lo) * sizeof(ChunkIndexEntry));
  aIndex[lo] = *pEntry;
  return nIndex + 1;
}

int csTryLoadWalCheckpoint(ChunkStore *cs, int *pLoaded){
  u8 aTail[1 + CHUNK_MANIFEST_SIZE];
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  u8 aHeader[CS_WAL_CHUNK_HDR_SIZE];
  u8 aBuf[65536];
  i64 nFile = 0;
  i64 iTail;
  i64 iRoot;
  i64 iRead;
  i64 nRemain;
  WalState stamp;
  WalState rootStamp;
  ChunkIndexEntry *aIndex = 0;
  ChunkIndexEntry snapshotEntry;
  ProllyHash bodyHash;
  blake3_hasher hasher;
  int nIndex;
  int iEntry = 0;
  int rc;

  *pLoaded = 0;
  if( cs->wal.iWalOffset<=0 || !cs->file.pFile ) return SQLITE_OK;
  rc = sqlite3OsFileSize(cs->file.pFile, &nFile);
  if( rc!=SQLITE_OK ) return rc;
  if( nFile<(i64)sizeof(aTail) ) return SQLITE_OK;
  iTail = nFile - (i64)sizeof(aTail);
  rc = sqlite3OsRead(cs->file.pFile, aTail, sizeof(aTail), iTail);
  if( rc!=SQLITE_OK ) return rc;
  if( aTail[0]!=CS_WAL_TAG_ROOT
   || csManifestHashState(aTail+1, iTail)!=CS_MANIFEST_HASH_OK
   || csValidateWalRootManifest(cs, aTail+1, iTail)!=SQLITE_OK ){
    return SQLITE_OK;
  }
  stamp = cs->wal;
  if( !csReadCheckpointStamp(aTail+1, iTail, &stamp) ) return SQLITE_OK;

  iRoot = stamp.iCheckpointOffset + CS_WAL_CHUNK_HDR_SIZE
        + stamp.nCheckpointIndex;
  if( iRoot<0 || iRoot+(i64)sizeof(aRoot)>nFile ) return SQLITE_OK;
  rc = sqlite3OsRead(cs->file.pFile, aRoot, sizeof(aRoot), iRoot);
  if( rc!=SQLITE_OK ) return rc;
  rootStamp = cs->wal;
  if( aRoot[0]!=CS_WAL_TAG_ROOT
   || csManifestHashState(aRoot+1, iRoot)!=CS_MANIFEST_HASH_OK
   || csValidateWalRootManifest(cs, aRoot+1, iRoot)!=SQLITE_OK
   || !csReadCheckpointStamp(aRoot+1, iRoot, &rootStamp)
   || rootStamp.iCheckpointOffset!=stamp.iCheckpointOffset
   || rootStamp.nCheckpointIndex!=stamp.nCheckpointIndex
   || rootStamp.iCheckpointReplay!=stamp.iCheckpointReplay
   || CS_READ_I64(aRoot+1+CS_MANIFEST_DURABLE_TO_OFF)<iRoot
   || CS_READ_I64(aRoot+1+CS_MANIFEST_BATCH_START_OFF)!=iRoot
   || CS_READ_I64(aRoot+1+CS_MANIFEST_NEXT_OFF_OFF)
        !=stamp.iCheckpointReplay ){
    return SQLITE_OK;
  }

  rc = sqlite3OsRead(cs->file.pFile, aHeader, sizeof(aHeader),
                     stamp.iCheckpointOffset);
  if( rc!=SQLITE_OK ) return rc;
  if( aHeader[0]!=CS_WAL_TAG_CHUNK
   || (i64)CS_READ_U32(aHeader+CS_WAL_CHUNK_LEN_OFF)
        !=stamp.nCheckpointIndex ){
    return SQLITE_OK;
  }
  nIndex = (int)(stamp.nCheckpointIndex / CHUNK_INDEX_ENTRY_SIZE);
  if( nIndex>=(INT_MAX/(int)sizeof(ChunkIndexEntry)) ) return SQLITE_TOOBIG;
  aIndex = sqlite3_malloc64(
      (sqlite3_uint64)(nIndex+1) * sizeof(ChunkIndexEntry));
  if( !aIndex ) return SQLITE_NOMEM;

  blake3_hasher_init(&hasher);
  iRead = stamp.iCheckpointOffset + CS_WAL_CHUNK_HDR_SIZE;
  nRemain = stamp.nCheckpointIndex;
  while( nRemain>0 ){
    int n = nRemain>(i64)sizeof(aBuf) ? (int)sizeof(aBuf) : (int)nRemain;
    int i;
    rc = sqlite3OsRead(cs->file.pFile, aBuf, n, iRead);
    if( rc!=SQLITE_OK ) goto checkpoint_invalid;
    blake3_hasher_update(&hasher, aBuf, (size_t)n);
    for(i=0; i<n; i+=CHUNK_INDEX_ENTRY_SIZE){
      rc = csDeserializeCheckpointEntry(aBuf+i, &aIndex[iEntry]);
      if( rc!=SQLITE_OK ) goto checkpoint_invalid;
      if( aIndex[iEntry].offset<CHUNK_MANIFEST_SIZE
       || aIndex[iEntry].offset>stamp.iCheckpointOffset-4
       || (i64)aIndex[iEntry].size
            > stamp.iCheckpointOffset-aIndex[iEntry].offset-4
       || (iEntry>0
           && prollyHashCompare(&aIndex[iEntry-1].hash,
                                &aIndex[iEntry].hash)>=0) ){
        goto checkpoint_invalid;
      }
      iEntry++;
    }
    iRead += n;
    nRemain -= n;
  }
  blake3_hasher_finalize(&hasher, bodyHash.data, PROLLY_HASH_SIZE);
  if( memcmp(bodyHash.data, aHeader+CS_WAL_CHUNK_HASH_OFF,
             PROLLY_HASH_SIZE)!=0 ){
    goto checkpoint_invalid;
  }

  memcpy(snapshotEntry.hash.data, aHeader+CS_WAL_CHUNK_HASH_OFF,
         PROLLY_HASH_SIZE);
  snapshotEntry.offset = stamp.iCheckpointOffset + CS_WAL_CHUNK_LEN_OFF;
  snapshotEntry.size = (int)stamp.nCheckpointIndex;
  nIndex = csCheckpointIndexInsert(aIndex, nIndex, &snapshotEntry);
  csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase,
                    cs->index.aIndexMmapSize);
  cs->index.aIndex = aIndex;
  cs->index.nIndex = nIndex;
  cs->index.aIndexMmapBase = 0;
  cs->index.aIndexMmapSize = 0;
  cs->index.nChunks = (int)CS_READ_U32(
      aRoot+1+CS_MANIFEST_CHUNK_COUNT_OFF);
  memcpy(cs->refs.refsHash.data,
         aRoot+1+CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);
  cs->wal.iCheckpointOffset = stamp.iCheckpointOffset;
  cs->wal.nCheckpointIndex = stamp.nCheckpointIndex;
  cs->wal.iCheckpointReplay = stamp.iCheckpointReplay;
  aIndex = 0;
  rc = csReplayWalFrom(cs, stamp.iCheckpointReplay);
  if( rc!=SQLITE_OK ) return rc;
  *pLoaded = 1;
  return SQLITE_OK;

checkpoint_invalid:
  sqlite3_free(aIndex);
  return rc==SQLITE_NOMEM ? rc : SQLITE_OK;
}

#endif
