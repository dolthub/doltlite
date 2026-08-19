#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "prolly_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef SQLITE_TEST
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif
#endif

/* Chunk-store commit/rollback path. */

static int csFileHasMovedNoFault(ChunkStore *cs){
  int bMoved = 0;
  if( cs->file.pFile==0 ) return 0;
  sqlite3OsFileControlHint(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
  return bMoved;
}

static int csCrashWriteInjectionActive(void){
#ifdef SQLITE_TEST
  const char *zEnv = getenv("DOLTLITE_CRASH_WRITE");
  return zEnv && atoi(zEnv)>0;
#else
  return 0;
#endif
}

#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
static int csReloadInjectionActive(void){
  const char *zEnv = getenv("DOLTLITE_RELOAD_INJECT");
  return zEnv && atoi(zEnv)>0;
}
#endif
static int csRollbackFailedAppend(ChunkStore *cs, i64 origFileSize){
  sqlite3_int64 sizeNow = -1;
  int rc = SQLITE_OK;

  if( !cs->file.pFile ) return SQLITE_IOERR;

  rc = sqlite3OsTruncate(cs->file.pFile, origFileSize);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsFileSize(cs->file.pFile, &sizeNow);
  }
  if( rc==SQLITE_OK && sizeNow==origFileSize ){
    (void)csSyncFile(cs);
    return SQLITE_OK;
  }

  csCloseFile(cs->file.pFile);
  cs->file.pFile = 0;
  rc = csOpenFile(cs->file.pVfs, cs->file.zFilename, &cs->file.pFile,
                  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3OsTruncate(cs->file.pFile, origFileSize);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsFileSize(cs->file.pFile, &sizeNow);
  }
  if( rc==SQLITE_OK && sizeNow==origFileSize ){
    (void)csSyncFile(cs);
    return SQLITE_OK;
  }
  return rc==SQLITE_OK ? SQLITE_IOERR_TRUNCATE : rc;
}

static int csRestoreCommittedRefsState(ChunkStore *cs){
  csRestoreCommittedRefsHash(cs);
  if( prollyHashIsEmpty(&cs->refs.committedRefsHash) ){
    csFreeBranches(cs);
    csFreeTags(cs);
    csFreeRemotes(cs);
    csFreeTracking(cs);
    csFreeSequences(cs);
    return csEnsureDefaultBranch(cs);
  }
  {
    int rc = chunkStoreReloadRefs(cs);
    if( rc!=SQLITE_OK ){
      /* Clear refs without allocating; rollback is discarding staged chunks. */
      csFreeBranches(cs);
      csFreeTags(cs);
      csFreeRemotes(cs);
      csFreeTracking(cs);
      csFreeSequences(cs);
      cs->bRefsStale = 1;
      return rc;
    }
    cs->bRefsStale = 0;
    return SQLITE_OK;
  }
}

static int csCommitToMemory(ChunkStore *cs){
  if( cs->staging.nPending > 0 ){
    ChunkIndexEntry *aMem = 0;
    int nMem = 0;
    int rc = csMergeIndex(cs, &aMem, &nMem);
    if( rc!=SQLITE_OK ) return rc;
    csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase, cs->index.aIndexMmapSize);
    cs->index.aIndex = aMem;
    cs->index.nIndex = nMem;
    cs->index.aIndexMmapBase = 0;
    cs->index.aIndexMmapSize = 0;
    cs->staging.nPending = 0;
    csPendHTReset(cs);
    cs->staging.nCommittedWriteBuf = cs->staging.nWriteBuf;
  }
  csMarkRefsCommitted(cs);
  return SQLITE_OK;
}

/* Open/lock the store file, resolve logical EOF, and sector-align the
** next commit batch. On entry *pLockFd / *pzLockName are zeroed by the
** caller when the graph lock is already held. */
static int csCommitResolveAppendPoint(
  ChunkStore *cs,
  int hadFile,
  int lockHeld,
  CsFileLock *pLockFd,
  char **pzLockName,
  i64 *pFileSize,
  i64 *pOrigFileSize,
  i64 *pDurableTo,
  i64 *pBatchStart,
  int *pSectorSize
){
  int rc = SQLITE_OK;
  i64 fileSize = 0;
  i64 physFileSize = -1;
  int sectorSize = 1;

  if( cs->file.pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->file.pVfs, cs->file.zFilename, &cs->file.pFile, openFlags, 0);
    if( rc != SQLITE_OK ){
      return (rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM) ? rc : SQLITE_CANTOPEN;
    }
  }

  if( !lockHeld ){
    rc = csFileLock(cs->file.pVfs, cs->file.zFilename, pLockFd, pzLockName);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( lockHeld && hadFile ){
    fileSize = cs->file.iFileSize;
  }else{
    rc = cs->file.pFile->pMethods->xFileSize(cs->file.pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
    physFileSize = fileSize;
  }

  if( hadFile && !lockHeld ){
    int bMoved = 0;
    int rc2 = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED,
                                   &bMoved);
    if( rc2==SQLITE_OK && bMoved ){
      rc = csReloadFromDiskPreservingLocalRefs(cs);
      if( rc != SQLITE_OK ) return rc;
      fileSize = cs->file.iFileSize;
    }
  }

  if( fileSize > cs->file.iFileSize && hadFile ){
    rc = csReloadFromDiskPreservingLocalRefs(cs);
    if( rc != SQLITE_OK ) return rc;
    fileSize = cs->file.iFileSize;
  }
#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
  if( hadFile && cs->staging.nRecentUncommitted>0 && csReloadInjectionActive() ){
    static int csReloadInjected = 0;
    if( !csReloadInjected ){
      csReloadInjected = 1;
      rc = csReloadFromDiskPreservingLocalRefs(cs);
      if( rc != SQLITE_OK ) return rc;
      fileSize = cs->file.iFileSize;
    }
  }
#endif
  /* Append at logical EOF and truncate crash garbage beyond it. */
  if( hadFile && cs->file.iFileSize > fileSize ){
    fileSize = cs->file.iFileSize;
  }
  if( physFileSize > fileSize ){
    (void)sqlite3OsTruncate(cs->file.pFile, fileSize);
  }

  /* On non-atomic media, isolate commit batches on fresh sectors. */
  if( (cs->file.pFile->pMethods->xDeviceCharacteristics(cs->file.pFile)
       & (SQLITE_IOCAP_POWERSAFE_OVERWRITE|SQLITE_IOCAP_ATOMIC))==0 ){
    sectorSize = cs->file.pFile->pMethods->xSectorSize(cs->file.pFile);
    if( sectorSize < 512 ) sectorSize = 512;
    if( sectorSize > 65536 ) sectorSize = 65536;
  }
  *pFileSize = fileSize;
  *pOrigFileSize = fileSize;
  *pDurableTo = fileSize > 0 ? fileSize : (i64)CHUNK_MANIFEST_SIZE;
  *pBatchStart = *pDurableTo;
  if( fileSize > 0 && sectorSize > 1 && (*pBatchStart % sectorSize)!=0 ){
    *pBatchStart += sectorSize - 1;
    *pBatchStart -= *pBatchStart % sectorSize;
  }
  *pSectorSize = sectorSize;
  return SQLITE_OK;
}

/* Build the on-disk index plan for pending chunks (offsets + merge/recent). */
static int csCommitPlanPendingIndex(
  ChunkStore *cs,
  i64 batchStart,
  int crashWriteActive,
  ChunkIndexEntry *aSmallCommittedPending,
  int nSmallCommittedPending,
  ChunkIndexEntry **paCommittedPending,
  ChunkIndexEntry **paMergePending,
  ChunkIndexEntry **paMerged,
  int *pnMerged,
  int *pUseRecent
){
  ChunkStore mergeView;
  i64 filePos = batchStart;
  i64 appendBytes = 0;
  int i;

  *paCommittedPending = 0;
  *paMergePending = 0;
  *paMerged = 0;
  *pnMerged = 0;
  *pUseRecent = 0;
  if( cs->staging.nPending <= 0 ) return SQLITE_OK;

  for( i = 0; i < cs->staging.nPending; i++ ){
    i64 recBytes = (i64)CS_WAL_CHUNK_HDR_SIZE
                 + (i64)cs->staging.aPending[i].size;
    if( appendBytes > LARGEST_INT64 - recBytes ) return SQLITE_TOOBIG;
    appendBytes += recBytes;
  }
  if( cs->wal.nWalData > LARGEST_INT64 - appendBytes ) return SQLITE_TOOBIG;

  if( cs->staging.nPending <= nSmallCommittedPending ){
    *paCommittedPending = aSmallCommittedPending;
  }else{
    *paCommittedPending = (ChunkIndexEntry*)sqlite3_malloc(
      cs->staging.nPending * (int)sizeof(ChunkIndexEntry)
    );
    if( !*paCommittedPending ) return SQLITE_NOMEM;
  }

  for( i = 0; i < cs->staging.nPending; i++ ){
    ChunkIndexEntry *pSrc = &cs->staging.aPending[i];
    (*paCommittedPending)[i] = *pSrc;
    (*paCommittedPending)[i].offset = filePos + CS_WAL_CHUNK_LEN_OFF;
    filePos += (i64)CS_WAL_CHUNK_HDR_SIZE + (i64)pSrc->size;
  }

  *pUseRecent = !crashWriteActive
             && cs->staging.nPending <= 32
             && cs->staging.nRecent + cs->staging.nPending
                  <= CS_RECENT_FAST_PATH_MAX;
  if( *pUseRecent ){
    return csGrowRecent(cs, cs->staging.nPending);
  }

  if( cs->staging.nRecent > 0 ){
    *paMergePending = (ChunkIndexEntry*)sqlite3_malloc(
      (cs->staging.nRecent + cs->staging.nPending) * (int)sizeof(ChunkIndexEntry)
    );
    if( !*paMergePending ) return SQLITE_NOMEM;
    memcpy(*paMergePending, cs->staging.aRecent,
           cs->staging.nRecent * sizeof(ChunkIndexEntry));
    memcpy(*paMergePending + cs->staging.nRecent, *paCommittedPending,
           cs->staging.nPending * sizeof(ChunkIndexEntry));
    mergeView = *cs;
    mergeView.staging.aPending = *paMergePending;
    mergeView.staging.nPending = cs->staging.nRecent + cs->staging.nPending;
  }else{
    mergeView = *cs;
    mergeView.staging.aPending = *paCommittedPending;
    mergeView.staging.nPending = cs->staging.nPending;
  }
  return csMergeIndex(&mergeView, paMerged, pnMerged);
}

/* Install the committed index view and clear staging after a successful sync. */
static void csCommitPublishStaging(
  ChunkStore *cs,
  int useRecent,
  ChunkIndexEntry *aCommittedPending,
  ChunkIndexEntry *aMerged,
  int nMerged
){
  int i;

  if( cs->staging.nPending > 0 ){
    if( useRecent ){
      memcpy(cs->staging.aRecent + cs->staging.nRecent, aCommittedPending,
             cs->staging.nPending * sizeof(ChunkIndexEntry));
      for( i=0; i<cs->staging.nPending; i++ ){
        cs->staging.aRecentZeroTail[cs->staging.nRecent+i] =
          cs->staging.aPendingZeroTail[i];
      }
      cs->staging.nRecent += cs->staging.nPending;
      sqlite3_free(aMerged);
    }else{
      csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase, cs->index.aIndexMmapSize);
      cs->index.aIndex = aMerged;
      cs->index.nIndex = nMerged;
      cs->index.aIndexMmapBase = 0;
      cs->index.aIndexMmapSize = 0;
      cs->staging.nRecent = 0;
      if( cs->staging.aRecentZeroTail ){
        memset(cs->staging.aRecentZeroTail, 0,
               cs->staging.nRecentAlloc * sizeof(i64));
      }
      csRecentHTClear(cs);
    }
  }else{
    sqlite3_free(aMerged);
  }

  cs->staging.nRecentUncommitted = 0;
  cs->staging.iUncommittedStart = 0;
  cs->staging.nWriteBuf = 0;
  if( cs->staging.nWriteBufAlloc > CS_WRITEBUF_RETAIN_MAX ){
    sqlite3_free(cs->staging.pWriteBuf);
    cs->staging.pWriteBuf = 0;
    cs->staging.nWriteBufAlloc = 0;
  }
  cs->staging.nPending = 0;
  csPendHTReset(cs);
  csMarkRefsCommitted(cs);
}

static int csCommitToFile(ChunkStore *cs){
  int rc;
  int i;
  i64 fileSize = 0;
  i64 origFileSize = 0;
  i64 writeOff = 0;
  i64 durableTo = 0;
  i64 batchStart = 0;
  i64 contentEnd = 0;
  int sectorSize = 1;
  CsFileLock lockFd = CS_FILE_LOCK_INIT;
  char *lockName = 0;
  int hadFile = (cs->file.pFile != 0);
  int lockHeld = cs->lockDepth>0 || cs->isBuffer;
  ChunkIndexEntry *aCommittedPending = 0;
  ChunkIndexEntry aSmallCommittedPending[32];
  ChunkIndexEntry *aMergePending = 0;
  ChunkIndexEntry *aMerged = 0;
  int nMerged = 0;
  int useRecent = 0;
  int crashWriteActive = csCrashWriteInjectionActive();

  rc = csCommitResolveAppendPoint(
      cs, hadFile, lockHeld, &lockFd, &lockName,
      &fileSize, &origFileSize, &durableTo, &batchStart, &sectorSize);
  if( rc!=SQLITE_OK ){
    /* Open-before-lock failures never held a commit lock; unlock is a no-op
    ** when lockHeld or when the lock was never acquired. */
    if( !lockHeld ) csFileUnlock(lockFd, &lockName);
    return rc;
  }

  rc = csCommitPlanPendingIndex(
      cs, batchStart, crashWriteActive,
      aSmallCommittedPending,
      (int)(sizeof(aSmallCommittedPending)/sizeof(aSmallCommittedPending[0])),
      &aCommittedPending, &aMergePending, &aMerged, &nMerged, &useRecent);
  if( rc!=SQLITE_OK ) goto commit_done;

#ifdef SQLITE_TEST
  {
    static int crashWriteTarget = -2;
    static int crashWriteCount = 0;
    if( crashWriteTarget == -2 ){
      const char *zEnv = getenv("DOLTLITE_CRASH_WRITE");
      crashWriteTarget = zEnv ? atoi(zEnv) : -1;
    }
    if( crashWriteTarget > 0 ) crashWriteCount = 0;
#define CRASH_CHECK_WRITE() do{ \
  if( crashWriteTarget>0 && ++crashWriteCount>=crashWriteTarget ){ \
    _exit(99); \
  } \
}while(0)
#else
#define CRASH_CHECK_WRITE() ((void)0)
#endif

  if( fileSize == 0 ){
    u8 manifest[CHUNK_MANIFEST_SIZE];
    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    csSerializeManifest(cs, manifest);
    csManifestSeal(manifest, 0);
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->file.pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc != SQLITE_OK ) goto commit_done;
  }

  writeOff = batchStart;

  /* Append chunks before the root record. Recovery ignores appended data until
  ** it finds a later valid root record that points at the new manifest. */
  if( cs->staging.nPending > 0 ){
    i64 walBytes = 0;
    int hasSparse = 0;
    for( i = 0; i < cs->staging.nPending; i++ ){
      walBytes += (i64)CS_WAL_CHUNK_HDR_SIZE
                + (i64)cs->staging.aPending[i].size;
      if( cs->staging.aPendingZeroTail[i] ) hasSparse = 1;
    }
    if( !crashWriteActive && !hasSparse ){
      i64 remaining = walBytes;
      const u8 *pSrc = cs->staging.pWriteBuf;
      assert( walBytes == cs->staging.nWriteBuf );
      while( remaining > 0 ){
        int toWrite = remaining > 65536 ? 65536 : (int)remaining;
        CRASH_CHECK_WRITE();
        rc = sqlite3OsWrite(cs->file.pFile, pSrc, toWrite, writeOff);
        if( rc != SQLITE_OK ) goto commit_done;
        pSrc += toWrite;
        writeOff += toWrite;
        remaining -= toWrite;
      }
    }else{
      for( i = 0; i < cs->staging.nPending; i++ ){
        ChunkIndexEntry *pe = &cs->staging.aPending[i];
        u8 recHdr[CS_WAL_CHUNK_HDR_SIZE];
        i64 bufOff;
        csFillChunkHdr(recHdr, &pe->hash, (u32)pe->size);

        bufOff = pe->offset + 4;
        CRASH_CHECK_WRITE();
        rc = sqlite3OsWrite(cs->file.pFile, recHdr, CS_WAL_CHUNK_HDR_SIZE, writeOff);
        if( rc != SQLITE_OK ) goto commit_done;
        writeOff += CS_WAL_CHUNK_HDR_SIZE;

        {
          const u8 *pSrc = cs->staging.pWriteBuf + bufOff;
          i64 zeroTail = cs->staging.aPendingZeroTail[i];
          int remaining = (int)(pe->size - zeroTail);
          while( remaining > 0 && rc==SQLITE_OK ){
            int toWrite = remaining > 65536 ? 65536 : remaining;
            CRASH_CHECK_WRITE();
            rc = sqlite3OsWrite(cs->file.pFile, pSrc, toWrite, writeOff);
            pSrc += toWrite;
            writeOff += toWrite;
            remaining -= toWrite;
          }
          while( zeroTail > 0 && rc==SQLITE_OK ){
            static const u8 aZeroWin[65536];
            int toWrite = zeroTail > (i64)sizeof(aZeroWin)
                        ? (int)sizeof(aZeroWin) : (int)zeroTail;
            CRASH_CHECK_WRITE();
            rc = sqlite3OsWrite(cs->file.pFile, aZeroWin, toWrite, writeOff);
            writeOff += toWrite;
            zeroTail -= toWrite;
          }
        }
        if( rc != SQLITE_OK ) goto commit_done;
      }
    }
  }

  /* Mid-transaction drains appended chunk bodies without a sync, but the
  ** root record below declares everything before this batch durable, and
  ** recovery poisons the store when damage lands below durableTo. The
  ** drained bytes must reach disk before a root record can claim them. */
  if( cs->staging.nRecentUncommitted > 0 ){
    CRASH_CHECK_WRITE();
    rc = csSyncFile(cs);
    if( rc != SQLITE_OK ) goto commit_done;
  }

  /* The root record is the commit point for the append-only chunk store. */
  {
    u8 rootRec[1 + CHUNK_MANIFEST_SIZE];
    i64 rootEnd = writeOff + (i64)sizeof(rootRec);
    i64 nextOff = rootEnd;
    if( sectorSize > 1 ){
      nextOff = rootEnd + (sectorSize - 1);
      nextOff -= nextOff % sectorSize;
    }
    rootRec[0] = CS_WAL_TAG_ROOT;
    csSerializeManifest(cs, rootRec + 1);
    csStampWalCheckpoint(cs, rootRec + 1);
    CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_DURABLE_TO_OFF, durableTo);
    CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_NEXT_OFF_OFF, nextOff);
    CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_BATCH_START_OFF, batchStart);
    csManifestSeal(rootRec + 1, writeOff);
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->file.pFile, rootRec, sizeof(rootRec), writeOff);
    if( rc != SQLITE_OK ) goto commit_done;
    contentEnd = rootEnd;
    writeOff = nextOff;
  }

  CRASH_CHECK_WRITE();
  rc = csSyncFile(cs);

#ifdef SQLITE_TEST
  }
#undef CRASH_CHECK_WRITE
#endif
  if( rc != SQLITE_OK ) goto commit_done;

  /* iFileSize is the logical append cursor (the aligned NEXT_OFF), nWalData
  ** the physically present WAL content. */
  cs->file.iFileSize = writeOff;
  cs->wal.nWalData = contentEnd - cs->wal.iWalOffset;
  cs->wal.cleanCloseMarker = 0;

commit_done:
  csFileUnlock(lockFd, &lockName);

  if( rc != SQLITE_OK ){
    if( cs->file.pFile && writeOff > origFileSize ){
      (void)csRollbackFailedAppend(cs, origFileSize);
    }
    (void)csRestoreCommittedRefsState(cs);
    if( aCommittedPending!=aSmallCommittedPending ){
      sqlite3_free(aCommittedPending);
    }
    sqlite3_free(aMergePending);
    sqlite3_free(aMerged);
    return rc;
  }

  csCommitPublishStaging(cs, useRecent, aCommittedPending, aMerged, nMerged);
  if( aCommittedPending!=aSmallCommittedPending ){
    sqlite3_free(aCommittedPending);
  }
  sqlite3_free(aMergePending);
  return SQLITE_OK;
}

int chunkStoreCommit(ChunkStore *cs){
  int rc;
  int acquiredLock = 0;
  int preserveRefs = 0;
  ProllyHash savedRefsHash;
  SavedRefsState savedRefs;

  memset(&savedRefs, 0, sizeof(savedRefs));
  if( cs->notADatabase ) return SQLITE_NOTADB;
  if( cs->corruptMidStream ) return SQLITE_CORRUPT;
  if( cs->readOnly || cs->movedReadOnly ) return SQLITE_READONLY;
  if( cs->isMemory ) return csCommitToMemory(cs);
  if( !cs->isBuffer && cs->lockDepth<=0 && cs->file.zFilename ){
    ProllyHash baseRefsHash;
    preserveRefs = cs->staging.nPending > 0
                && prollyHashCompare(&cs->refs.refsHash,
                                     &cs->refs.committedRefsHash)!=0;
    if( preserveRefs ){
      savedRefsHash = cs->refs.refsHash;
      baseRefsHash = cs->refs.committedRefsHash;
      csDetachSavedRefsState(cs, &savedRefs);
    }
    rc = chunkStoreLockAndRefresh(cs);
    if( rc!=SQLITE_OK ){
      if( preserveRefs ){
        csRestoreSavedRefsState(cs, &savedRefs);
        cs->refs.refsHash = savedRefsHash;
      }
      return rc;
    }
    if( preserveRefs ){
      rc = csRestoreOrMergeLocalRefs(cs, &savedRefs,
                                     &savedRefsHash, &baseRefsHash);
      if( rc!=SQLITE_OK ){
        chunkStoreUnlock(cs);
        return rc;
      }
    }
    acquiredLock = 1;
  }
  if( cs->movedReadOnly
   || (!cs->isBuffer && !acquiredLock && csFileHasMovedNoFault(cs)) ){
    if( acquiredLock ) chunkStoreUnlock(cs);
    return SQLITE_READONLY;
  }
  rc = csCommitToFile(cs);
  if( acquiredLock ) chunkStoreUnlock(cs);
  return rc;
}

void chunkStoreRollback(ChunkStore *cs){
  cs->staging.nPending = 0;
  csPendHTReset(cs);
  if( cs->isMemory ){

    cs->staging.nWriteBuf = cs->staging.nCommittedWriteBuf;
  }else{
    if( cs->staging.nRecentUncommitted > 0 ){
      assert( cs->staging.nRecent >= cs->staging.nRecentUncommitted );
      cs->staging.nRecent -= cs->staging.nRecentUncommitted;
      cs->staging.nRecentUncommitted = 0;
      csRecentHTClear(cs);
      cs->file.iFileSize = cs->staging.iUncommittedStart;
      if( cs->file.iFileSize >= cs->wal.iWalOffset ){
        cs->wal.nWalData = cs->file.iFileSize - cs->wal.iWalOffset;
      }else{
        cs->wal.nWalData = 0;
      }
      cs->staging.iUncommittedStart = 0;
    }
    cs->staging.nWriteBuf = 0;
  }
  (void)csRestoreCommittedRefsState(cs);
}

#endif
