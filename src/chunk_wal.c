
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
  w->iCheckpointDataEnd = 0;
  w->nCheckpointEntries = 0;
  memset(&w->checkpointHash, 0, sizeof(w->checkpointHash));
  w->checkpointMagic = 0;
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
  i64 iDataEnd = 0;
  int nEntries = 0;
  u32 magic;

  magic = CS_READ_U32(aManifest + CS_MANIFEST_CHECKPOINT_MAGIC_OFF);
  if( magic!=CS_WAL_CHECKPOINT_MAGIC_V1
   && magic!=CS_WAL_CHECKPOINT_MAGIC_V2 ){
    return 0;
  }
  iOffset = CS_READ_I64(aManifest + CS_MANIFEST_CHECKPOINT_OFFSET_OFF);
  nIndex = (i64)CS_READ_U32(
      aManifest + CS_MANIFEST_CHECKPOINT_SIZE_OFF);
  iReplay = CS_READ_I64(
      aManifest + CS_MANIFEST_CHECKPOINT_REPLAY_OFF);
  if( iOffset<pWal->iWalOffset || nIndex<=0 || nIndex>INT_MAX
   || iOffset>LARGEST_INT64-CS_WAL_CHUNK_HDR_SIZE-nIndex ){
    return 0;
  }
  if( magic==CS_WAL_CHECKPOINT_MAGIC_V1 ){
    if( nIndex%CHUNK_INDEX_ENTRY_SIZE!=0 ) return 0;
    nEntries = (int)(nIndex/CHUNK_INDEX_ENTRY_SIZE);
    iDataEnd = iOffset;
  }else{
    iDataEnd = CS_READ_I64(
        aManifest + CS_MANIFEST_CHECKPOINT_DATA_END_OFF);
    nEntries = (int)CS_READ_U32(
        aManifest + CS_MANIFEST_CHECKPOINT_COUNT_OFF);
    if( nIndex<CS_INDEX_PAGE_HEADER_SIZE || nIndex>CS_INDEX_PAGE_SIZE
     || iDataEnd<pWal->iWalOffset || iDataEnd>iOffset || nEntries<=0 ){
      return 0;
    }
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
  pWal->iCheckpointDataEnd = iDataEnd;
  pWal->nCheckpointEntries = nEntries;
  pWal->checkpointMagic = magic;
  if( magic==CS_WAL_CHECKPOINT_MAGIC_V2 ){
    memcpy(pWal->checkpointHash.data,
           aManifest + CS_MANIFEST_CHECKPOINT_HASH_OFF,
           PROLLY_HASH_SIZE);
  }else{
    memset(&pWal->checkpointHash, 0, sizeof(pWal->checkpointHash));
  }
  return 1;
}

void csStampWalCheckpoint(const ChunkStore *cs, u8 *aManifest){
  if( cs->wal.iCheckpointOffset<=0 || cs->wal.nCheckpointIndex<=0
   || cs->wal.iCheckpointReplay<=0
   || (cs->wal.checkpointMagic!=CS_WAL_CHECKPOINT_MAGIC_V1
       && cs->wal.checkpointMagic!=CS_WAL_CHECKPOINT_MAGIC_V2) ){
    return;
  }
  assert( cs->wal.nCheckpointIndex<=0xffffffffu );
  CS_WRITE_U32(aManifest + CS_MANIFEST_CHECKPOINT_MAGIC_OFF,
               cs->wal.checkpointMagic);
  CS_WRITE_I64(aManifest + CS_MANIFEST_CHECKPOINT_OFFSET_OFF,
               cs->wal.iCheckpointOffset);
  CS_WRITE_U32(aManifest + CS_MANIFEST_CHECKPOINT_SIZE_OFF,
               (u32)cs->wal.nCheckpointIndex);
  CS_WRITE_I64(aManifest + CS_MANIFEST_CHECKPOINT_REPLAY_OFF,
               cs->wal.iCheckpointReplay);
  if( cs->wal.checkpointMagic==CS_WAL_CHECKPOINT_MAGIC_V2 ){
    CS_WRITE_I64(aManifest + CS_MANIFEST_CHECKPOINT_DATA_END_OFF,
                 cs->wal.iCheckpointDataEnd);
    memcpy(aManifest + CS_MANIFEST_CHECKPOINT_HASH_OFF,
           cs->wal.checkpointHash.data, PROLLY_HASH_SIZE);
    CS_WRITE_U32(aManifest + CS_MANIFEST_CHECKPOINT_COUNT_OFF,
                 (u32)cs->wal.nCheckpointEntries);
  }
}

static i64 csWalCheckpointThreshold(void){
  i64 n = 64*1024*1024;
  const char *z = getenv("DOLTLITE_WAL_CHECKPOINT_THRESHOLD");
  if( z && z[0] ){
    i64 v = (i64)strtoll(z, 0, 10);
    if( v>0 ) n = v;
  }
  return n;
}

int csWalCheckpointDue(const ChunkStore *cs){
  i64 iBase = cs->wal.iCheckpointReplay>0
            ? cs->wal.iCheckpointReplay : cs->wal.iWalOffset;
  /* Ref-less raw stores still verify every WAL chunk. */
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
  i64 nAlloc;
  ChunkIndexEntry *aIndex;
  int i;
  int nOut = 0;
  int rc;

  *ppIndex = 0;
  *pnIndex = 0;
  rc = csMaterializeIndex(cs);
  if( rc!=SQLITE_OK ) return rc;
  nAlloc = (i64)cs->index.nIndex + cs->staging.nRecent;
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

typedef struct CsCheckpointPageRef CsCheckpointPageRef;
struct CsCheckpointPageRef {
  ProllyHash maxHash;
  ProllyHash bodyHash;
  i64 offset;
  int size;
};

static int csWriteCheckpointPage(
  ChunkStore *cs,
  const u8 *aBody,
  int nBody,
  const ProllyHash *pMaxHash,
  i64 *pOffset,
  CsCheckpointPageRef *pRef
){
  u8 aHeader[CS_WAL_CHUNK_HDR_SIZE];
  int rc;
  if( nBody<CS_INDEX_PAGE_HEADER_SIZE || nBody>CS_INDEX_PAGE_SIZE
   || *pOffset>LARGEST_INT64-CS_WAL_CHUNK_HDR_SIZE-nBody ){
    return SQLITE_TOOBIG;
  }
  prollyHashCompute(aBody, nBody, &pRef->bodyHash);
  csFillChunkHdr(aHeader, &pRef->bodyHash, (u32)nBody);
  rc = sqlite3OsWrite(cs->file.pFile, aHeader, sizeof(aHeader), *pOffset);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsWrite(cs->file.pFile, aBody, nBody,
                        *pOffset+CS_WAL_CHUNK_HDR_SIZE);
  }
  if( rc!=SQLITE_OK ) return rc;
  pRef->maxHash = *pMaxHash;
  pRef->offset = *pOffset;
  pRef->size = nBody;
  *pOffset += CS_WAL_CHUNK_HDR_SIZE+nBody;
  return SQLITE_OK;
}

static int csWriteCheckpointLeaves(
  ChunkStore *cs,
  const ChunkIndexEntry *aIndex,
  int nIndex,
  i64 *pOffset,
  CsCheckpointPageRef **ppRef,
  int *pnRef
){
  const int nPerPage =
      (CS_INDEX_PAGE_SIZE-CS_INDEX_PAGE_HEADER_SIZE)/CHUNK_INDEX_ENTRY_SIZE;
  int nRef = (nIndex+nPerPage-1)/nPerPage;
  CsCheckpointPageRef *aRef;
  u8 aBody[CS_INDEX_PAGE_SIZE];
  int i;
  int rc = SQLITE_OK;

  if( nRef<=0 || nRef>INT_MAX/(int)sizeof(CsCheckpointPageRef) ){
    return SQLITE_TOOBIG;
  }
  aRef = sqlite3_malloc64(
      (sqlite3_uint64)nRef*sizeof(CsCheckpointPageRef));
  if( !aRef ) return SQLITE_NOMEM;
  for(i=0; i<nRef; i++){
    int iFirst = i*nPerPage;
    int nCell = nIndex-iFirst;
    int j;
    int nBody;
    if( nCell>nPerPage ) nCell = nPerPage;
    CS_WRITE_U32(aBody, CS_INDEX_PAGE_LEAF_MAGIC);
    CS_WRITE_U32(aBody+4, (u32)nCell);
    for(j=0; j<nCell; j++){
      csSerializeCheckpointEntry(
          aBody+CS_INDEX_PAGE_HEADER_SIZE+j*CHUNK_INDEX_ENTRY_SIZE,
          &aIndex[iFirst+j]);
    }
    nBody = CS_INDEX_PAGE_HEADER_SIZE+nCell*CHUNK_INDEX_ENTRY_SIZE;
    rc = csWriteCheckpointPage(cs, aBody, nBody,
                               &aIndex[iFirst+nCell-1].hash,
                               pOffset, &aRef[i]);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(aRef);
    return rc;
  }
  *ppRef = aRef;
  *pnRef = nRef;
  return SQLITE_OK;
}

static int csWriteCheckpointParents(
  ChunkStore *cs,
  CsCheckpointPageRef *aChild,
  int nChild,
  i64 *pOffset,
  CsCheckpointPageRef **ppParent,
  int *pnParent
){
  const int nPerPage =
      (CS_INDEX_PAGE_SIZE-CS_INDEX_PAGE_HEADER_SIZE)/CS_INDEX_CHILD_SIZE;
  int nParent = (nChild+nPerPage-1)/nPerPage;
  CsCheckpointPageRef *aParent;
  u8 aBody[CS_INDEX_PAGE_SIZE];
  int i;
  int rc = SQLITE_OK;

  aParent = sqlite3_malloc64(
      (sqlite3_uint64)nParent*sizeof(CsCheckpointPageRef));
  if( !aParent ) return SQLITE_NOMEM;
  for(i=0; i<nParent; i++){
    int iFirst = i*nPerPage;
    int nCell = nChild-iFirst;
    int j;
    int nBody;
    if( nCell>nPerPage ) nCell = nPerPage;
    CS_WRITE_U32(aBody, CS_INDEX_PAGE_INTERNAL_MAGIC);
    CS_WRITE_U32(aBody+4, (u32)nCell);
    for(j=0; j<nCell; j++){
      const CsCheckpointPageRef *pChild = &aChild[iFirst+j];
      u8 *p = aBody+CS_INDEX_PAGE_HEADER_SIZE+j*CS_INDEX_CHILD_SIZE;
      memcpy(p, pChild->maxHash.data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      CS_WRITE_I64(p, pChild->offset);
      p += 8;
      CS_WRITE_U32(p, (u32)pChild->size);
      p += 4;
      memcpy(p, pChild->bodyHash.data, PROLLY_HASH_SIZE);
    }
    nBody = CS_INDEX_PAGE_HEADER_SIZE+nCell*CS_INDEX_CHILD_SIZE;
    rc = csWriteCheckpointPage(cs, aBody, nBody,
                               &aChild[iFirst+nCell-1].maxHash,
                               pOffset, &aParent[i]);
    if( rc!=SQLITE_OK ) break;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_free(aParent);
    return rc;
  }
  *ppParent = aParent;
  *pnParent = nParent;
  return SQLITE_OK;
}

int csWriteWalCheckpoint(ChunkStore *cs, int sectorSize, int *pWritten){
  ChunkIndexEntry *aIndex = 0;
  CsCheckpointPageRef *aRef = 0;
  int nIndex = 0;
  i64 iCheckpoint;
  i64 iDataEnd;
  i64 iWrite;
  i64 iRoot;
  i64 iRootEnd;
  i64 iNext;
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  int nRef = 0;
  int rc;

  *pWritten = 0;
  rc = csBuildCheckpointIndex(cs, &aIndex, &nIndex);
  if( rc!=SQLITE_OK || nIndex==0 ) goto checkpoint_done;
  iDataEnd = cs->file.iFileSize;
  iWrite = iDataEnd;
  rc = csWriteCheckpointLeaves(cs, aIndex, nIndex, &iWrite, &aRef, &nRef);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;
  while( nRef>1 ){
    CsCheckpointPageRef *aParent = 0;
    int nParent = 0;
    rc = csWriteCheckpointParents(cs, aRef, nRef, &iWrite,
                                  &aParent, &nParent);
    sqlite3_free(aRef);
    aRef = aParent;
    nRef = nParent;
    if( rc!=SQLITE_OK ) goto checkpoint_rollback;
  }
  rc = csSyncFile(cs);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;

  iCheckpoint = aRef[0].offset;
  iRoot = iWrite;
  iRootEnd = iRoot + (i64)sizeof(aRoot);
  iNext = iRootEnd;
  if( sectorSize>1 ){
    iNext += sectorSize - 1;
    iNext -= iNext % sectorSize;
  }
  aRoot[0] = CS_WAL_TAG_ROOT;
  csSerializeManifest(cs, aRoot + 1);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_DURABLE_TO_OFF, iDataEnd);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_NEXT_OFF_OFF, iNext);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_BATCH_START_OFF, iDataEnd);
  CS_WRITE_U32(aRoot + 1 + CS_MANIFEST_CHECKPOINT_MAGIC_OFF,
               CS_WAL_CHECKPOINT_MAGIC_V2);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_CHECKPOINT_OFFSET_OFF, iCheckpoint);
  CS_WRITE_U32(aRoot + 1 + CS_MANIFEST_CHECKPOINT_SIZE_OFF,
               (u32)aRef[0].size);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_CHECKPOINT_REPLAY_OFF, iNext);
  CS_WRITE_I64(aRoot + 1 + CS_MANIFEST_CHECKPOINT_DATA_END_OFF, iDataEnd);
  memcpy(aRoot + 1 + CS_MANIFEST_CHECKPOINT_HASH_OFF,
         aRef[0].bodyHash.data, PROLLY_HASH_SIZE);
  CS_WRITE_U32(aRoot + 1 + CS_MANIFEST_CHECKPOINT_COUNT_OFF, (u32)nIndex);
  csManifestSeal(aRoot + 1, iRoot);
  rc = sqlite3OsWrite(cs->file.pFile, aRoot, sizeof(aRoot), iRoot);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;
  rc = csSyncFile(cs);
  if( rc!=SQLITE_OK ) goto checkpoint_rollback;

  cs->wal.iCheckpointOffset = iCheckpoint;
  cs->wal.nCheckpointIndex = aRef[0].size;
  cs->wal.iCheckpointReplay = iNext;
  cs->wal.iCheckpointDataEnd = iDataEnd;
  cs->wal.nCheckpointEntries = nIndex;
  cs->wal.checkpointHash = aRef[0].bodyHash;
  cs->wal.checkpointMagic = CS_WAL_CHECKPOINT_MAGIC_V2;
  cs->file.iFileSize = iNext;
  cs->wal.nWalData = iRootEnd - cs->wal.iWalOffset;
  cs->wal.cleanCloseMarker = 1;
  *pWritten = 1;
  rc = SQLITE_OK;
  goto checkpoint_done;

checkpoint_rollback:
  (void)sqlite3OsTruncate(cs->file.pFile, iDataEnd);
  (void)csSyncFile(cs);

checkpoint_done:
  sqlite3_free(aRef);
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
  pSaved->lazy = cs->index.lazy;
  pSaved->refsHash = cs->refs.refsHash;
  pSaved->nChunks = cs->index.nChunks;
  pSaved->iFileSize = cs->file.iFileSize;
  pSaved->wal = cs->wal;
  /* Detach, do not alias: the finalize frees the live refs before
  ** adopting the replayed ones, so an aliasing save would double-free
  ** when replay runs on a store that already holds refs (tail refresh). */
  csDetachSavedRefsState(cs, &pSaved->refs);
}

static void csRestoreReplayState(ChunkStore *cs, const ChunkStoreReplayState *pSaved){
  cs->index.aIndex = pSaved->aIndex;
  cs->index.nIndex = pSaved->nIndex;
  cs->index.aIndexMmapBase = pSaved->aIndexMmapBase;
  cs->index.aIndexMmapSize = pSaved->aIndexMmapSize;
  cs->index.lazy = pSaved->lazy;
  cs->refs.refsHash = pSaved->refsHash;
  cs->index.nChunks = pSaved->nChunks;
  cs->file.iFileSize = pSaved->iFileSize;
  cs->wal = pSaved->wal;
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
  pDst->index.lazy = pSrc->index.lazy;
  pDst->wal.nWalData = pSrc->wal.nWalData;
  pDst->wal.iCheckpointOffset = pSrc->wal.iCheckpointOffset;
  pDst->wal.nCheckpointIndex = pSrc->wal.nCheckpointIndex;
  pDst->wal.iCheckpointReplay = pSrc->wal.iCheckpointReplay;
  pDst->wal.iCheckpointDataEnd = pSrc->wal.iCheckpointDataEnd;
  pDst->wal.nCheckpointEntries = pSrc->wal.nCheckpointEntries;
  pDst->wal.checkpointHash = pSrc->wal.checkpointHash;
  pDst->wal.checkpointMagic = pSrc->wal.checkpointMagic;
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
  memset(&pSrc->index.lazy, 0, sizeof(pSrc->index.lazy));
  pSrc->wal.nWalData = 0;
  pSrc->wal.iCheckpointOffset = 0;
  pSrc->wal.nCheckpointIndex = 0;
  pSrc->wal.iCheckpointReplay = 0;
  pSrc->wal.iCheckpointDataEnd = 0;
  pSrc->wal.nCheckpointEntries = 0;
  memset(&pSrc->wal.checkpointHash, 0, sizeof(pSrc->wal.checkpointHash));
  pSrc->wal.checkpointMagic = 0;
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

/* Zero-tail probe cap; writer gaps are sector-bounded. */
#define CS_WAL_SCAN_MAX (64*1024*1024)

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

/* Classify WAL damage: mid-stream, declared gap, or torn tail. */
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
  /* Scan to WAL end: a sealed root past a capped window proves the
  ** damage was committed; stopping early would rewind it as TORN. */
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
        }
      }
    }
    if( (i64)n >= nAvail ) break;
    q += n - 4;
  }
  return SQLITE_OK;
}

static int csReplayWalFrom(
  ChunkStore *cs,
  i64 iStart,
  i64 iSkipStart,
  i64 iSkipEnd,
  int bTailMode
){
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
    /* No WAL tail: chunk_count must match the compacted index. */
    if( chunkIndexCount(&cs->index)>0
     && cs->index.nChunks!=chunkIndexCount(&cs->index) ){
      return SQLITE_CORRUPT;
    }
    if( chunkIndexCount(&cs->index)==0 && cs->index.nChunks==0
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
    if( iSkipStart>0
     && cs->wal.iWalOffset+pos>=iSkipStart
     && cs->wal.iWalOffset+pos<iSkipEnd ){
      pos = iSkipEnd-cs->wal.iWalOffset;
      continue;
    }
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
        ChunkIndexEntry existingEntry;
        int existing = 0;
        ChunkIndexEntry *e = 0;
        if( bTailMode ){
          /* Fresh commits are almost never duplicates, and the lazy index
          ** verifies whole pages per probe. Duplicate entries are
          ** tolerated everywhere, so probe only the cheap sets. */
          int iRec = -1;
          existing = csSearchIndex(cs->index.aIndex, cs->index.nIndex,
                                   &hash)>=0;
          if( !existing ){
            rc = csSearchRecent(cs, &hash, &iRec);
            if( rc!=SQLITE_OK ) goto replay_error;
            existing = iRec>=0;
          }
        }else{
          rc = csIndexLookup(cs, &hash, &existingEntry, &existing);
          if( rc!=SQLITE_OK ) goto replay_error;
        }
        if( !existing ){
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
      /* Offsetless v12 seals are only safe at a sequential parse boundary. */
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
        if( iSkipStart==0 ){
          (void)csReadCheckpointStamp(m, recAbs, &cs->wal);
        }
      }else{
        cs->wal.cleanCloseMarker = 0;
      }
      lastBoundary = pos;
      nRootedPending = cs->staging.nPending;
      nRootRecordsSeen++;

    } else if( tag == 0 && csWalTailIsZero(cs, recPos, walSize) ){
      /* SIZE_HINT/prealloc zero-extends past commit; treat as absent. */
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
      /* Bytes at WAL start are corruption; later junk is a crash tail. */
      if( damageAction==CS_DAMAGE_TORN && recPos == 0 && tag != 0 ){
        rc = SQLITE_CORRUPT;
        goto replay_error;
      }
      sawMidStream = (damageAction==CS_DAMAGE_MIDSTREAM);
      break;
    }
  }

  /* Damaged WAL start is not an empty store. Prealloc zeros never set
  ** sawDamage; malformed non-zero bytes are corrupt even with no root. */
  if( sawDamage
   && nRootRecordsSeen == 0
   && nPendingBefore == 0 && chunkIndexCount(&cs->index)==0 ){
    memset(cs->refs.refsHash.data, 0, PROLLY_HASH_SIZE);
    cs->index.nChunks = 0;
    cs->wal.recoveredMidStream = 1;
    sawMidStream = 1;
  }

  /* Reclaim uncommitted tail unless poisoned. */
  if( !sawMidStream ){
    cs->wal.nWalData = lastBoundary < walSize ? lastBoundary : walSize;
    cs->file.iFileSize = cs->wal.iWalOffset + lastBoundary;
  }

  cs->staging.nPending = nRootedPending;

  if( nRootRecordsSeen == 0
   && nPendingBefore == 0 && chunkIndexCount(&cs->index)==0
   && !sawMidStream && !cs->wal.recoveredMidStream ){
    memset(cs->refs.refsHash.data, 0, PROLLY_HASH_SIZE);
    cs->index.nChunks = 0;
  }

  if( cs->staging.nPending > 0 ){
    if( bTailMode ){
      /* O(delta): the replayed chunks are committed content, exactly what
      ** a local commit appends to the recent set. Merging into the main
      ** index would copy it whole for every peer commit. */
      int i;
      rc = csGrowRecent(cs, cs->staging.nPending);
      if( rc != SQLITE_OK ) goto replay_error;
      for(i=0; i<cs->staging.nPending; i++){
        cs->staging.aRecent[cs->staging.nRecent] = cs->staging.aPending[i];
        cs->staging.aRecentZeroTail[cs->staging.nRecent] = 0;
        cs->staging.nRecent++;
      }
      cs->staging.nPending = 0;
      csPendHTReset(cs);
    }else{
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
  return csReplayWalSkipping(cs, 0, 0);
}

/* Ingest records appended after iStart into the live store. The caller
** proved the prefix below iStart is the content it already holds. */
int csReplayWalTail(ChunkStore *cs, i64 iStart){
  return csReplayWalFrom(cs, iStart, 0, 0, 1);
}

int csReplayWalSkipping(ChunkStore *cs, i64 iSkipStart, i64 iSkipEnd){
  cs->wal.iCheckpointOffset = 0;
  cs->wal.nCheckpointIndex = 0;
  cs->wal.iCheckpointReplay = 0;
  cs->wal.iCheckpointDataEnd = 0;
  cs->wal.nCheckpointEntries = 0;
  memset(&cs->wal.checkpointHash, 0, sizeof(cs->wal.checkpointHash));
  cs->wal.checkpointMagic = 0;
  if( (iSkipStart==0)!=(iSkipEnd==0)
   || (iSkipStart>0
       && (iSkipStart<cs->wal.iWalOffset || iSkipEnd<=iSkipStart)) ){
    return SQLITE_CORRUPT;
  }
  return csReplayWalFrom(cs, cs->wal.iWalOffset, iSkipStart, iSkipEnd, 0);
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

static int csFindLastCheckpointRoot(
  ChunkStore *cs,
  i64 nFile,
  u8 *aRoot,
  i64 *pRootOffset,
  WalState *pStamp,
  int *pFound
){
  u8 aBuf[65540];
  i64 iEnd = nFile;
  i64 nZeroRun = 0;

  *pFound = 0;
  while( iEnd>cs->wal.iWalOffset ){
    i64 iStart = iEnd-(i64)sizeof(aBuf);
    int n;
    int i;
    int iFirst;
    int rc;
    if( iStart<cs->wal.iWalOffset ) iStart = cs->wal.iWalOffset;
    n = (int)(iEnd-iStart);
    rc = sqlite3OsRead(cs->file.pFile, aBuf, n, iStart);
    if( rc!=SQLITE_OK ) return rc;
    iFirst = iEnd==nFile ? n-1 : n-5;
    for(i=iFirst; i>=0; i--){
      i64 iRoot;
      WalState stamp;
      if( aBuf[i]==0 ){
        nZeroRun++;
        if( nZeroRun>=CS_WAL_SCAN_MAX ) return SQLITE_OK;
      }else{
        nZeroRun = 0;
      }
      if( i>n-5 ) continue;
      if( aBuf[i]!=CS_WAL_TAG_ROOT
       || CS_READ_U32(aBuf+i+1)!=CHUNK_STORE_MAGIC ){
        continue;
      }
      iRoot = iStart+i;
      if( iRoot>nFile-(i64)(1+CHUNK_MANIFEST_SIZE) ) continue;
      rc = sqlite3OsRead(cs->file.pFile, aRoot,
                         1+CHUNK_MANIFEST_SIZE, iRoot);
      if( rc!=SQLITE_OK ) return rc;
      stamp = *pStamp;
      if( aRoot[0]==CS_WAL_TAG_ROOT
       && csManifestHashState(aRoot+1, iRoot)==CS_MANIFEST_HASH_OK
       && csValidateWalRootManifest(cs, aRoot+1, iRoot)==SQLITE_OK ){
        if( csReadCheckpointStamp(aRoot+1, iRoot, &stamp) ){
          *pRootOffset = iRoot;
          *pStamp = stamp;
          *pFound = 1;
          return SQLITE_OK;
        }
        if( CS_READ_I64(aRoot+1+CS_MANIFEST_DURABLE_TO_OFF)==iRoot
         && CS_READ_I64(aRoot+1+CS_MANIFEST_BATCH_START_OFF)==iRoot ){
          return SQLITE_OK;
        }
      }
    }
    if( iStart==cs->wal.iWalOffset ) break;
    iEnd = iStart+4;
  }
  return SQLITE_OK;
}

static void csCheckpointSkipRange(
  const WalState *pStamp,
  i64 *pSkipStart,
  i64 *pSkipEnd
){
  *pSkipStart = pStamp->checkpointMagic==CS_WAL_CHECKPOINT_MAGIC_V2
              ? pStamp->iCheckpointDataEnd : pStamp->iCheckpointOffset;
  *pSkipEnd = pStamp->iCheckpointReplay;
}

int csTryLoadWalCheckpoint(
  ChunkStore *cs,
  int *pLoaded,
  i64 *pSkipStart,
  i64 *pSkipEnd
){
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
  int found = 0;
  int rc;

  *pLoaded = 0;
  *pSkipStart = 0;
  *pSkipEnd = 0;
  if( cs->wal.iWalOffset<=0 || !cs->file.pFile ) return SQLITE_OK;
  rc = sqlite3OsFileSize(cs->file.pFile, &nFile);
  if( rc!=SQLITE_OK ) return rc;
  stamp = cs->wal;
  rc = csFindLastCheckpointRoot(cs, nFile, aTail, &iTail, &stamp, &found);
  if( rc!=SQLITE_OK || !found ) return rc;

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
   || rootStamp.checkpointMagic!=stamp.checkpointMagic
   || (stamp.checkpointMagic==CS_WAL_CHECKPOINT_MAGIC_V2
       && (rootStamp.iCheckpointDataEnd!=stamp.iCheckpointDataEnd
           || rootStamp.nCheckpointEntries!=stamp.nCheckpointEntries
           || prollyHashCompare(&rootStamp.checkpointHash,
                                &stamp.checkpointHash)!=0))
   || (stamp.checkpointMagic==CS_WAL_CHECKPOINT_MAGIC_V2
       ? (CS_READ_I64(aRoot+1+CS_MANIFEST_DURABLE_TO_OFF)
              !=stamp.iCheckpointDataEnd
          || CS_READ_I64(aRoot+1+CS_MANIFEST_BATCH_START_OFF)
              !=stamp.iCheckpointDataEnd)
       : (CS_READ_I64(aRoot+1+CS_MANIFEST_DURABLE_TO_OFF)<iRoot
          || CS_READ_I64(aRoot+1+CS_MANIFEST_BATCH_START_OFF)!=iRoot))
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
    csCheckpointSkipRange(&stamp, pSkipStart, pSkipEnd);
    return SQLITE_OK;
  }
  if( stamp.checkpointMagic==CS_WAL_CHECKPOINT_MAGIC_V2 ){
    u8 *aPage = 0;
    u32 pageMagic;
    u32 nCell;
    int nCellSize;
    if( memcmp(aHeader+CS_WAL_CHUNK_HASH_OFF,
               stamp.checkpointHash.data, PROLLY_HASH_SIZE)!=0 ){
      csCheckpointSkipRange(&stamp, pSkipStart, pSkipEnd);
      return SQLITE_OK;
    }
    aPage = sqlite3_malloc((int)stamp.nCheckpointIndex);
    if( !aPage ) return SQLITE_NOMEM;
    rc = sqlite3OsRead(cs->file.pFile, aPage, (int)stamp.nCheckpointIndex,
                       stamp.iCheckpointOffset+CS_WAL_CHUNK_HDR_SIZE);
    if( rc==SQLITE_OK ){
      prollyHashCompute(aPage, (int)stamp.nCheckpointIndex, &bodyHash);
      if( prollyHashCompare(&bodyHash, &stamp.checkpointHash)!=0 ){
        rc = SQLITE_CORRUPT;
      }
    }
    if( rc==SQLITE_OK ){
      pageMagic = CS_READ_U32(aPage);
      nCell = CS_READ_U32(aPage+4);
      nCellSize = pageMagic==CS_INDEX_PAGE_LEAF_MAGIC
                ? CHUNK_INDEX_ENTRY_SIZE
                : pageMagic==CS_INDEX_PAGE_INTERNAL_MAGIC
                  ? CS_INDEX_CHILD_SIZE : 0;
      if( nCellSize==0 || nCell==0
       || (i64)CS_INDEX_PAGE_HEADER_SIZE+(i64)nCell*nCellSize
            !=stamp.nCheckpointIndex ){
        rc = SQLITE_CORRUPT;
      }
    }
    sqlite3_free(aPage);
    if( rc!=SQLITE_OK ){
      if( rc!=SQLITE_NOMEM ){
        csCheckpointSkipRange(&stamp, pSkipStart, pSkipEnd);
      }
      return rc==SQLITE_NOMEM ? rc : SQLITE_OK;
    }

    csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase,
                      cs->index.aIndexMmapSize);
    cs->index.aIndex = 0;
    cs->index.nIndex = 0;
    cs->index.aIndexMmapBase = 0;
    cs->index.aIndexMmapSize = 0;
    cs->index.lazy.iRootOffset = stamp.iCheckpointOffset;
    cs->index.lazy.iDataEnd = stamp.iCheckpointDataEnd;
    cs->index.lazy.nRootSize = (int)stamp.nCheckpointIndex;
    cs->index.lazy.nEntries = stamp.nCheckpointEntries;
    cs->index.lazy.rootHash = stamp.checkpointHash;
    cs->index.lazy.active = 1;
    cs->index.nChunks = (int)CS_READ_U32(
        aRoot+1+CS_MANIFEST_CHUNK_COUNT_OFF);
    memcpy(cs->refs.refsHash.data,
           aRoot+1+CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);
    cs->wal.iCheckpointOffset = stamp.iCheckpointOffset;
    cs->wal.nCheckpointIndex = stamp.nCheckpointIndex;
    cs->wal.iCheckpointReplay = stamp.iCheckpointReplay;
    cs->wal.iCheckpointDataEnd = stamp.iCheckpointDataEnd;
    cs->wal.nCheckpointEntries = stamp.nCheckpointEntries;
    cs->wal.checkpointHash = stamp.checkpointHash;
    cs->wal.checkpointMagic = stamp.checkpointMagic;
    rc = csReplayWalFrom(cs, stamp.iCheckpointReplay, 0, 0, 0);
    if( rc!=SQLITE_OK ){
      memset(&cs->index.lazy, 0, sizeof(cs->index.lazy));
      cs->wal.iCheckpointOffset = 0;
      cs->wal.nCheckpointIndex = 0;
      cs->wal.iCheckpointReplay = 0;
      cs->wal.iCheckpointDataEnd = 0;
      cs->wal.nCheckpointEntries = 0;
      memset(&cs->wal.checkpointHash, 0, sizeof(cs->wal.checkpointHash));
      cs->wal.checkpointMagic = 0;
      if( rc!=SQLITE_NOMEM ){
        csCheckpointSkipRange(&stamp, pSkipStart, pSkipEnd);
      }
      return rc==SQLITE_NOMEM ? rc : SQLITE_OK;
    }
    *pLoaded = 1;
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
  cs->wal.iCheckpointDataEnd = stamp.iCheckpointDataEnd;
  cs->wal.nCheckpointEntries = stamp.nCheckpointEntries;
  cs->wal.checkpointHash = stamp.checkpointHash;
  cs->wal.checkpointMagic = stamp.checkpointMagic;
  aIndex = 0;
  rc = csReplayWalFrom(cs, stamp.iCheckpointReplay, 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  *pLoaded = 1;
  return SQLITE_OK;

checkpoint_invalid:
  sqlite3_free(aIndex);
  if( rc!=SQLITE_NOMEM ){
    csCheckpointSkipRange(&stamp, pSkipStart, pSkipEnd);
  }
  return rc==SQLITE_NOMEM ? rc : SQLITE_OK;
}

#endif
