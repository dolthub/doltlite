
#ifdef DOLTLITE_PROLLY

#include "chunk_wal.h"
#include "chunk_store.h"
#include "chunk_staging.h"
#include "chunk_file.h"
#include "../ext/blake3/blake3.h"
#include <string.h>

i64 walStateGetOffset(const WalState *w){
  return w->iWalOffset;
}

i64 walStateGetDataSize(const WalState *w){
  return w->nWalData;
}

void walStateSetOffset(WalState *w, i64 iOffset){
  w->iWalOffset = iOffset;
}

void walStateSetDataSize(WalState *w, i64 nData){
  w->nWalData = nData;
}

static void csCaptureReplayState(ChunkStore *cs, ChunkStoreReplayState *pSaved){
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
  pDst->wal.recoveredMidStream = pSrc->wal.recoveredMidStream;
  pDst->corruptMidStream = pSrc->corruptMidStream;
  REFS_OWNED_COPY(pDst->refs, pSrc->refs);

  pSrc->file.pFile = 0;
  pSrc->index.aIndex = 0;
  pSrc->index.nIndex = 0;
  pSrc->index.aIndexMmapBase = 0;
  pSrc->index.aIndexMmapSize = 0;
  pSrc->wal.nWalData = 0;
  REFS_OWNED_CLEAR(pSrc->refs);
}

void csFreeReloadState(ChunkStoreReloadState *pSaved){
  csCloseFile(pSaved->pFile);
  csReleaseIndexBuf(pSaved->aIndex, pSaved->aIndexMmapBase,
                     pSaved->aIndexMmapSize);
  csFreeSavedRefsState(&pSaved->refs);
  memset(pSaved, 0, sizeof(*pSaved));
}

/* Cap recovery scans; valid writer gaps are sector-bounded. */
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
  if( last > damagePos + CS_WAL_SCAN_MAX ) last = damagePos + CS_WAL_SCAN_MAX;
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
        state = csManifestHashState(m);
        if( state==CS_MANIFEST_HASH_OK ){
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
        }else if( state==CS_MANIFEST_HASH_LEGACY
               && cand + 1 + CHUNK_MANIFEST_SIZE < walSize ){
          cs->wal.recoveredMidStream = 1;
          *pAction = CS_DAMAGE_MIDSTREAM;
          return SQLITE_OK;
        }
      }
    }
    if( (i64)n >= nAvail ) break;
    q += n - 4;
  }
  return SQLITE_OK;
}

int csReplayWal(ChunkStore *cs){
  i64 walSize;
  ChunkStoreReplayState saved;
  i64 pos;
  i64 lastBoundary = 0;
  i64 resumePos = 0;
  int damageAction = 0;
  int sawMidStream = 0;
  int nPendingBefore = cs->staging.nPending;
  int nRootedPending = cs->staging.nPending;
  int nRootRecordsSeen = 0;
  ChunkStore tmpRefs;
  int haveTmpRefs = 0;
  int rc = SQLITE_OK;

  memset(&tmpRefs, 0, sizeof(tmpRefs));

  if( cs->wal.iWalOffset <= 0 || !cs->file.pFile ) return SQLITE_OK;

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

  pos = 0;
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
        if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
        sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
        break;
      }
      memcpy(m, aPrefix + 1, CS_WAL_CHUNK_HDR_SIZE - 1);
      rc = sqlite3OsRead(cs->file.pFile, m + CS_WAL_CHUNK_HDR_SIZE - 1,
                         CHUNK_MANIFEST_SIZE - (CS_WAL_CHUNK_HDR_SIZE - 1),
                         cs->wal.iWalOffset + pos);
      if( rc != SQLITE_OK ) goto replay_error;
      hashState = csManifestHashState(m);
      if( CS_READ_U32(m) != CHUNK_STORE_MAGIC
       || hashState == CS_MANIFEST_HASH_BAD ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: damaged WAL root manifest at offset %lld; "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + recPos));
        rc = csWalResolveDamage(cs, recPos, walSize, &damageAction, &resumePos);
        if( rc != SQLITE_OK ) goto replay_error;
        if( damageAction==CS_DAMAGE_RESUME ){ pos = resumePos; continue; }
        sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
        break;
      }

      cs->index.nChunks = (int)CS_READ_U32(m + CS_MANIFEST_CHUNK_COUNT_OFF);
      memcpy(cs->refs.refsHash.data, m + CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);

      pos = recPos + 1 + CHUNK_MANIFEST_SIZE;
      if( hashState == CS_MANIFEST_HASH_OK ){
        i64 nextOff = CS_READ_I64(m + CS_MANIFEST_NEXT_OFF_OFF);
        if( nextOff > cs->wal.iWalOffset + pos ){
          pos = nextOff - cs->wal.iWalOffset;
        }
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

  /* Reclaim uncommitted tail bytes unless the store is poisoned. */
  if( !sawMidStream ){
    cs->wal.nWalData = lastBoundary < walSize ? lastBoundary : walSize;
    cs->file.iFileSize = cs->wal.iWalOffset + lastBoundary;
  }

  cs->staging.nPending = nRootedPending;

  if( nRootRecordsSeen == 0
   && nPendingBefore == 0 && cs->index.nIndex == 0 ){
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

#endif
