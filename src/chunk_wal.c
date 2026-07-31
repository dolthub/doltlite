
#ifdef DOLTLITE_PROLLY

#include "chunk_wal.h"
#include "chunk_store.h"
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
}

void walStateSetDataSize(WalState *w, i64 nData){
  assert( w!=0 );
  assert( nData>=0 );
  w->nWalData = nData;
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
  /* The saved refs pointers are never freed here: either the replay adopted
  ** a new refs state (the old arrays were freed inline before adoption) or
  ** the live store still owns them. Tail replays run on stores with
  ** populated refs, where freeing the snapshot would be a double free. */
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

/* Replay WAL records from startPos (an offset relative to iWalOffset) to
** the end of the file, folding new chunks and root records into the
** current in-memory state. A full recovery replay passes 0 with fresh
** state; a refresh after a peer append passes the committed boundary
** (nWalData) so only the new tail is read and hashed, instead of paying a
** full reopen that replays everything since the last gc. */
int csReplayWalRange(ChunkStore *cs, i64 startPos){
  i64 walSize;
  ChunkStoreReplayState saved;
  ProllyHash refsHashBefore;
  int nChunksBefore;
  i64 nWalDataBefore;
  i64 iFileSizeBefore;
  u8 cleanCloseBefore;
  u8 recoveredMidStreamBefore;
  i64 pos;
  i64 lastBoundary = startPos;
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

  {
    i64 fileSize = 0;
    int rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
    walSize = fileSize - cs->wal.iWalOffset;
    cs->file.iFileSize = fileSize;
  }
  if( startPos>0 && walSize<=startPos ) return SQLITE_OK;
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
  /* A range replay mutates the live store; a failure must put back every
  ** scalar the loop touches, not just the captured pointers (the fresh-open
  ** caller discards the whole store on error and never noticed). */
  refsHashBefore = cs->refs.refsHash;
  nChunksBefore = cs->index.nChunks;
  nWalDataBefore = cs->wal.nWalData;
  iFileSizeBefore = cs->file.iFileSize;
  cleanCloseBefore = cs->wal.cleanCloseMarker;
  recoveredMidStreamBefore = cs->wal.recoveredMidStream;

  cs->wal.nWalData = walSize;
  cs->wal.cleanCloseMarker = 0;

  pos = startPos;
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
        /* A tail replay runs with the committed recent list still live;
        ** admitting a hash it already holds would put two entries for one
        ** chunk into gc's stream and brick the store at the next open. */
        if( existing < 0 && cs->staging.nRecent > 0 ){
          int idxRecent = -1;
          if( csSearchRecent(cs, &hash, &idxRecent)==SQLITE_OK
           && idxRecent>=0 ){
            existing = idxRecent;
          }
        }
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
      hashState = csManifestHashState(m);
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
  ** if no root manifest can be replayed. Full replays only: a tail replay
  ** knows committed data precedes startPos, so tail damage with no sealing
  ** root is an ordinary torn tail. */
  if( startPos == 0
   && sawDamage
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

  if( startPos == 0
   && nRootRecordsSeen == 0
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

  /* A tail replay that saw no new root leaves refsHash untouched, and the
  ** blob is content-addressed, so the in-memory refs are already current. */
  if( !prollyHashIsEmpty(&cs->refs.refsHash)
   && (startPos==0 || nRootRecordsSeen>0) ){
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
    /* Validate the incoming refs before the point of no return: once the
    ** live arrays are freed, the saved snapshot's pointers are dead and
    ** the error path must not restore them. */
    rc = csEnsureDefaultBranch(&tmpRefs);
    if( rc!=SQLITE_OK ) goto replay_error;
    csFreeRefsState(cs);
    csAdoptRefsState(cs, &tmpRefs);
    haveTmpRefs = 0;
  }else{
    rc = csEnsureDefaultBranch(cs);
    if( rc!=SQLITE_OK ) goto replay_error;
  }

  csReleaseReplayState(cs, &saved);
  return SQLITE_OK;

replay_error:
  if( haveTmpRefs ) csFreeRefsState(&tmpRefs);
  csRollbackReplayState(cs, &saved, nPendingBefore);
  cs->refs.refsHash = refsHashBefore;
  cs->index.nChunks = nChunksBefore;
  cs->wal.nWalData = nWalDataBefore;
  cs->file.iFileSize = iFileSizeBefore;
  cs->wal.cleanCloseMarker = cleanCloseBefore;
  cs->wal.recoveredMidStream = recoveredMidStreamBefore;
  return rc;
}

int csReplayWal(ChunkStore *cs){
  return csReplayWalRange(cs, 0);
}

#endif
