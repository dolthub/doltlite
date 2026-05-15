
#ifdef DOLTLITE_PROLLY

#include "chunk_wal.h"
#include "chunk_store.h"
#include "chunk_staging.h"
#include "chunk_file.h"
#include <string.h>

#define CS_READ_U32(p) (             \
  (u32)(((const u8*)(p))[0])       | \
  (u32)(((const u8*)(p))[1]) << 8  | \
  (u32)(((const u8*)(p))[2]) << 16 | \
  (u32)(((const u8*)(p))[3]) << 24   \
)

#define CS_WAL_TAG_CHUNK  0x01
#define CS_WAL_TAG_ROOT   0x02

void walStateInit(WalState *w){
  memset(w, 0, sizeof(*w));
}

void walStateReset(WalState *w){
  w->iWalOffset = 0;
  w->nWalData = 0;
}

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
  pDst->refs.aBranches = pSrc->refs.aBranches;
  pDst->refs.nBranches = pSrc->refs.nBranches;
  pDst->refs.zDefaultBranch = pSrc->refs.zDefaultBranch;
  pDst->refs.aTags = pSrc->refs.aTags;
  pDst->refs.nTags = pSrc->refs.nTags;
  pDst->refs.aRemotes = pSrc->refs.aRemotes;
  pDst->refs.nRemotes = pSrc->refs.nRemotes;
  pDst->refs.aTracking = pSrc->refs.aTracking;
  pDst->refs.nTracking = pSrc->refs.nTracking;

  pSrc->file.pFile = 0;
  pSrc->index.aIndex = 0;
  pSrc->index.nIndex = 0;
  pSrc->index.aIndexMmapBase = 0;
  pSrc->index.aIndexMmapSize = 0;
  pSrc->wal.nWalData = 0;
  pSrc->refs.aBranches = 0;
  pSrc->refs.nBranches = 0;
  pSrc->refs.zDefaultBranch = 0;
  pSrc->refs.aTags = 0;
  pSrc->refs.nTags = 0;
  pSrc->refs.aRemotes = 0;
  pSrc->refs.nRemotes = 0;
  pSrc->refs.aTracking = 0;
  pSrc->refs.nTracking = 0;
}

void csFreeReloadState(ChunkStoreReloadState *pSaved){
  csCloseFile(pSaved->pFile);
  csReleaseIndexBuf(pSaved->aIndex, pSaved->aIndexMmapBase,
                     pSaved->aIndexMmapSize);
  csFreeSavedRefsState(&pSaved->refs);
  memset(pSaved, 0, sizeof(*pSaved));
}

int csReplayWal(ChunkStore *cs){
  i64 walSize;
  ChunkStoreReplayState saved;
  i64 pos;
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
    u8 tag = 0;
    rc = sqlite3OsRead(cs->file.pFile, &tag, 1, cs->wal.iWalOffset + pos);
    if( rc != SQLITE_OK ) goto replay_error;
    pos++;

    if( tag == CS_WAL_TAG_CHUNK ){
      u8 aHdr[24];
      ProllyHash hash;
      u32 len;
      if( pos + 20 + 4 > walSize ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: WAL chunk header truncated at offset %lld; "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + pos - 1));
        break;
      }
      rc = sqlite3OsRead(cs->file.pFile, aHdr, sizeof(aHdr), cs->wal.iWalOffset + pos);
      if( rc != SQLITE_OK ) goto replay_error;
      memcpy(&hash, aHdr, 20);
      len = CS_READ_U32(aHdr + 20);
      pos += 24;
      if( pos < 0 || len > (u32)0x7fffffff
       || (u64)pos + len > (u64)walSize ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: WAL chunk body truncated at offset %lld (declared len=%u); "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + pos - 24 - 1), (unsigned)len);
        break;
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
      if( pos + CHUNK_MANIFEST_SIZE > walSize ){
        sqlite3_log(SQLITE_NOTICE,
          "doltlite: WAL root manifest truncated at offset %lld; "
          "stopping replay at last commit boundary",
          (long long)(cs->wal.iWalOffset + pos - 1));
        break;
      }
      {
        u32 magic;
        rc = sqlite3OsRead(cs->file.pFile, m, sizeof(m), cs->wal.iWalOffset + pos);
        if( rc != SQLITE_OK ) goto replay_error;
        magic = CS_READ_U32(m);
        if( magic != CHUNK_STORE_MAGIC ){
          if( pos + CHUNK_MANIFEST_SIZE < walSize ){
            rc = SQLITE_CORRUPT;
            goto replay_error;
          }
          sqlite3_log(SQLITE_NOTICE,
            "doltlite: WAL root manifest at tail offset %lld has bad magic; "
            "stopping replay at last commit boundary",
            (long long)(cs->wal.iWalOffset + pos - 1));
          break;
        }

        cs->index.nChunks = (int)CS_READ_U32(m + 28);

        memcpy(cs->refs.refsHash.data, m + 104, PROLLY_HASH_SIZE);
      }
      pos += CHUNK_MANIFEST_SIZE;
      nRootedPending = cs->staging.nPending;
      nRootRecordsSeen++;

    } else {
      rc = SQLITE_CORRUPT;
      goto replay_error;
    }
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
