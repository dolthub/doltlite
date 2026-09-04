#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

int csFileLockHeld(sqlite3_file *pFile){
  return pFile!=0;
}

int csFileLockPromote(sqlite3_file *pFile){
  return sqlite3OsLock(pFile, SQLITE_LOCK_RESERVED);
}

static int csFileRelock(sqlite3_file *pFile){
  int rc;
  if( !pFile ) return SQLITE_ERROR;
  rc = sqlite3OsLock(pFile, SQLITE_LOCK_SHARED);
  if( rc==SQLITE_OK ) rc = csFileLockPromote(pFile);
  if( rc!=SQLITE_OK ) sqlite3OsUnlock(pFile, SQLITE_LOCK_NONE);
  return rc;
}

static char *csLockPath(const char *path){
  const char *zBase = strrchr(path, '/');
  int nDir = 0;

  if( zBase ){
    nDir = (int)(zBase - path) + 1;
    zBase++;
  }else{
    zBase = path;
  }
  return sqlite3_mprintf("%.*s.%s-lock", nDir, path, zBase);
}

/* Missing parent is CANTOPEN, not READONLY_DIRECTORY (file:// clone). */
static int csDirectoryAccess(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int *pExists,
  int *pWritable
){
  const char *zSlash;
  const char *zDirPath = 0;
  char *zDir = 0;
  int exists = 0;
  int canWrite = 0;
  int rc;

  *pExists = 0;
  *pWritable = 0;
  if( !pVfs || !zPath ) return SQLITE_OK;
  zSlash = strrchr(zPath, '/');
  if( !zSlash ){
    zDirPath = ".";
  }else if( zSlash==zPath ){
    zDirPath = "/";
  }else{
    int nDir = (int)(zSlash - zPath);
    zDir = (char*)sqlite3_malloc(nDir + 1);
    if( !zDir ) return SQLITE_NOMEM;
    memcpy(zDir, zPath, (size_t)nDir);
    zDir[nDir] = 0;
    zDirPath = zDir;
  }
  rc = sqlite3OsAccess(pVfs, zDirPath, SQLITE_ACCESS_EXISTS, &exists);
  if( rc==SQLITE_OK && exists ){
    rc = sqlite3OsAccess(pVfs, zDirPath, SQLITE_ACCESS_READWRITE, &canWrite);
  }
  sqlite3_free(zDir);
  if( rc!=SQLITE_OK ) return rc;
  *pExists = exists;
  *pWritable = canWrite;
  return SQLITE_OK;
}

int csFileLock(sqlite3_vfs *pVfs, const char *path,
                      sqlite3_file **ppFile, char **pzName){
  char *zRaw = csLockPath(path);
  char *zLock = 0;
  sqlite3_file *pFile = 0;
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            | SQLITE_OPEN_MAIN_DB;
  int rc;

  *ppFile = 0;
  *pzName = 0;
  if( !zRaw ) return SQLITE_NOMEM;
  /* SQLITE_OPEN_MAIN_DB names need a double-nul; csLockPath is single. */
  rc = chunkStoreDupFilenameDoubleNul(zRaw, &zLock);
  sqlite3_free(zRaw);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3OsOpenMalloc(pVfs, zLock, &pFile, flags, 0);
  if( rc!=SQLITE_OK ){
    int exists = 0;
    int canWrite = 0;
    int rcDir = csDirectoryAccess(pVfs, path, &exists, &canWrite);
    sqlite3_free(zLock);
    /* Sidecar create, like -journal. Existing non-writable dir is
    ** READONLY_DIRECTORY (misc7-23); missing parent keeps the open error. */
    if( rcDir==SQLITE_OK && exists && !canWrite ){
      return SQLITE_READONLY_DIRECTORY;
    }
    return rc;
  }
  /* unix VFS SQLITE_DEBUG lock ladder: NO_LOCK -> SHARED, then escalate. */
  rc = csFileRelock(pFile);
  if( rc!=SQLITE_OK ){
    sqlite3OsCloseFree(pFile);
    sqlite3_free(zLock);
    return rc;
  }
  *ppFile = pFile;
  /* unixFile.zPath aliases this buffer until close; caller frees it. */
  *pzName = zLock;
  return SQLITE_OK;
}

static void csFileUnlockKeepOpen(sqlite3_file *pFile){
  if( pFile ) sqlite3OsUnlock(pFile, SQLITE_LOCK_NONE);
}

void csFileUnlock(sqlite3_file *pFile, char **pzName){
  if( pFile ){
    csFileUnlockKeepOpen(pFile);
    sqlite3OsCloseFree(pFile);
  }
  if( pzName ){
    sqlite3_free(*pzName);
    *pzName = 0;
  }
}

int csFileLockNB(sqlite3_vfs *pVfs, const char *path,
                        sqlite3_file **ppFile, char **pzName){
  return csFileLock(pVfs, path, ppFile, pzName);
}


int chunkStoreEnsureRefsFresh(ChunkStore *cs){
  int rc;
  if( !cs->bRefsStale ) return SQLITE_OK;
  rc = chunkStoreReloadRefs(cs);
  if( rc==SQLITE_OK ) cs->bRefsStale = 0;
  return rc;
}

/* Reapply uncommitted refs after adopting disk. Unchanged disk restores
** wholesale; if peers moved, merge local deltas (same-ref races fail).
** Success consumes pSaved; failure reinstates the local view. */
int csRestoreOrMergeLocalRefs(
  ChunkStore *cs,
  SavedRefsState *pSaved,
  const ProllyHash *pSavedRefsHash,
  const ProllyHash *pBaseRefsHash
){
  ProllyHash diskRefsHash;
  int diskMoved = 0;
  int rc;
  /* committedRefsHash is a proxy. A failed merge can leave it at disk
  ** while arrays are base-derived; also consult the sealed tail. */
  if( csReadDiskRefsHash(cs, &diskRefsHash)
   && prollyHashCompare(&diskRefsHash, pBaseRefsHash)!=0 ){
    diskMoved = 1;
  }
  if( !diskMoved
   && prollyHashCompare(&cs->refs.committedRefsHash, pBaseRefsHash)==0 ){
    csFreeRefsState(cs);
    csRestoreSavedRefsState(cs, pSaved);
    cs->refs.refsHash = *pSavedRefsHash;
    return SQLITE_OK;
  }
  {
    u8 *baseData = 0;
    int nBaseData = 0;
    ChunkStore baseView;
    memset(&baseView, 0, sizeof(baseView));
    rc = chunkStoreGet(cs, pBaseRefsHash, &baseData, &nBaseData);
    if( rc==SQLITE_OK ){
      rc = csDeserializeRefsIntoTemp(&baseView, baseData, nBaseData);
    }
    if( rc==SQLITE_OK ){
      rc = csMergeSavedRefsOntoDisk(cs, pSaved, &baseView.refs);
    }
    sqlite3_free(baseData);
    csFreeRefsState(&baseView);
  }
  if( rc==SQLITE_OK ){
    /* Pre-merge blob is the local view; serialize the merged table. */
    rc = chunkStoreSerializeRefs(cs);
  }
  if( rc==SQLITE_OK ){
    csFreeSavedRefsState(pSaved);
    return SQLITE_OK;
  }
  rc = rc==SQLITE_NOMEM ? SQLITE_NOMEM : SQLITE_BUSY_SNAPSHOT;
  csFreeRefsState(cs);
  csRestoreSavedRefsState(cs, pSaved);
  cs->refs.refsHash = *pSavedRefsHash;
  /* Reinstated tables are base-derived; keep committedRefsHash at base. */
  cs->refs.committedRefsHash = *pBaseRefsHash;
  return rc;
}

int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs){
  int rc;
  int preserveRefs;
  ProllyHash savedRefsHash;
  ProllyHash baseRefsHash;
  SavedRefsState savedRefs;

  /* Reload drops aRecent; refuse while uncommitted refs point at it. */
  if( cs->staging.nRecentUncommitted > 0 ){
    return SQLITE_BUSY_SNAPSHOT;
  }

  preserveRefs = cs->staging.nPending > 0
              && prollyHashCompare(&cs->refs.refsHash,
                                   &cs->refs.committedRefsHash)!=0;
  memset(&savedRefs, 0, sizeof(savedRefs));
  if( preserveRefs ){
    savedRefsHash = cs->refs.refsHash;
    baseRefsHash = cs->refs.committedRefsHash;
    csDetachSavedRefsState(cs, &savedRefs);
  }

  rc = csReloadFromDisk(cs);

  if( preserveRefs ){
    if( rc==SQLITE_OK ){
      return csRestoreOrMergeLocalRefs(cs, &savedRefs,
                                       &savedRefsHash, &baseRefsHash);
    }
    csRestoreSavedRefsState(cs, &savedRefs);
    cs->refs.refsHash = savedRefsHash;
  }
  return rc;
}


static int csMovedFileIsOurs(ChunkStore *cs, int *pIsOurs);

static int csAdoptMatchingCloseMarker(
  ChunkStore *cs,
  i64 fileSize,
  int *pAdopted
){
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  u8 aExpected[CHUNK_MANIFEST_SIZE];
  i64 iMarker = cs->file.iFileSize;
  i64 iNext;
  int rc;

  *pAdopted = 0;
  if( iMarker<=0
   || iMarker>LARGEST_INT64-(i64)sizeof(aRoot)
   || fileSize!=iMarker+(i64)sizeof(aRoot) ){
    return SQLITE_OK;
  }
  rc = sqlite3OsRead(cs->file.pFile, aRoot, (int)sizeof(aRoot), iMarker);
  if( rc!=SQLITE_OK ) return rc;
  if( aRoot[0]!=CS_WAL_TAG_ROOT
   || csManifestHashState(aRoot+1, iMarker)!=CS_MANIFEST_HASH_OK
   || csValidateWalRootManifest(cs, aRoot+1, iMarker)!=SQLITE_OK
   || CS_READ_I64(aRoot+1+CS_MANIFEST_DURABLE_TO_OFF)!=iMarker
   || CS_READ_I64(aRoot+1+CS_MANIFEST_BATCH_START_OFF)!=iMarker ){
    return SQLITE_OK;
  }

  iNext = CS_READ_I64(aRoot+1+CS_MANIFEST_NEXT_OFF_OFF);
  csSerializeManifest(cs, aExpected);
  csStampWalCheckpoint(cs, aExpected);
  CS_WRITE_I64(aExpected+CS_MANIFEST_DURABLE_TO_OFF, iMarker);
  CS_WRITE_I64(aExpected+CS_MANIFEST_NEXT_OFF_OFF, iNext);
  CS_WRITE_I64(aExpected+CS_MANIFEST_BATCH_START_OFF, iMarker);
  csManifestSeal(aExpected, iMarker);
  if( memcmp(aRoot+1, aExpected, sizeof(aExpected))!=0 ) return SQLITE_OK;

  cs->file.iFileSize = iNext;
  cs->wal.nWalData = fileSize - cs->wal.iWalOffset;
  cs->wal.cleanCloseMarker = 1;
  *pAdopted = 1;
  return SQLITE_OK;
}

/* The replaced-file guard must hold at every write-intent moment, not
** ride on a reader statement having statted first: the pinned-snapshot
** early return in refresh skips detection under the lock. One fcntl.
** Real failures surface: absorbing them here hides injected faults. */
static int csNoteMovedUnderLock(ChunkStore *cs){
  int bMoved = 0;
  int rc;
  if( cs->isMemory || cs->isBuffer || cs->file.pFile==0 ) return SQLITE_OK;
  rc = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;
  if( !bMoved ){
    cs->movedReadOnly = 0;
    return SQLITE_OK;
  }
  {
    int bOurs = 0;
    rc = csMovedFileIsOurs(cs, &bOurs);
    if( rc!=SQLITE_OK ) return rc;
    cs->movedReadOnly = bOurs ? 0 : 1;
  }
  return SQLITE_OK;
}

int chunkStoreLockAndRefreshChanged(ChunkStore *cs, int *pChanged){
  int changed = 0;
  int rc;
  if( pChanged ) *pChanged = 0;
  if( cs->isMemory || cs->isBuffer ) return SQLITE_OK;
  if( cs->pLockMutex && sqlite3_mutex_try(cs->pLockMutex)!=SQLITE_OK ){
    return SQLITE_BUSY;
  }
  if( cs->lockDepth > 0 ){
    cs->lockDepth++;
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return SQLITE_OK;
  }
  if( !cs->file.zFilename ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return SQLITE_ERROR;
  }
  if( csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
    rc = csFileRelock(CS_GRAPH_LOCK(cs));
  }else{
    rc = csFileLockNB(cs->file.pVfs, cs->file.zFilename,
                      &CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
  }
  if( rc!=SQLITE_OK ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return rc;
  }
  cs->lockDepth = 1;
  rc = csNoteMovedUnderLock(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreRefreshIfChanged(cs, &changed);
  if( rc!=SQLITE_OK ){
    cs->lockDepth = 0;
    csFileUnlockKeepOpen(CS_GRAPH_LOCK(cs));
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return rc;
  }
  if( pChanged ) *pChanged = changed;
  if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
  return SQLITE_OK;
}

int chunkStoreLockAndRefresh(ChunkStore *cs){
  return chunkStoreLockAndRefreshChanged(cs, 0);
}

void chunkStoreUnlock(ChunkStore *cs){
  if( cs->pLockMutex ) sqlite3_mutex_enter(cs->pLockMutex);
  if( cs->lockDepth > 0 ){
    cs->lockDepth--;
    if( cs->lockDepth == 0 && csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
      csFileUnlockKeepOpen(CS_GRAPH_LOCK(cs));
    }
  }
  if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
}

static int csStoreHasAnyBranchTip(ChunkStore *pCand, const RefsTable *rt,
                                  int *pHas){
  int i;
  *pHas = 0;
  for(i=0; i<rt->nBranches; i++){
    const ProllyHash *pTip = &rt->aBranches[i].commitHash;
    int rc, has = 0;
    if( prollyHashIsEmpty(pTip) ) continue;
    rc = chunkStoreHas(pCand, pTip, &has);
    if( rc!=SQLITE_OK ) return rc;
    if( has ){
      *pHas = 1;
      return SQLITE_OK;
    }
  }
  return SQLITE_OK;
}

/* Proof the path still holds our DB: candidate contains one of our branch
** tips (GC keeps those reachable). Empty files are never moved. Working-set
** roots are not proof — a peer WS advance lets GC sweep the one we remember.
** No proof: upstream read-only. */
static int csMovedFileIsOurs(ChunkStore *cs, int *pIsOurs){
  ChunkStore cand;
  int rc;

  *pIsOurs = 0;
  if( cs->isMemory || cs->isBuffer || cs->file.pFile==0 ) return SQLITE_OK;
  if( cs->file.zFilename==0 || cs->file.zFilename[0]==0 ) return SQLITE_OK;
  if( cs->file.iFileSize<=0 ){
    *pIsOurs = 1;
    return SQLITE_OK;
  }

  rc = chunkStoreOpen(&cand, cs->file.pVfs, cs->file.zFilename,
                      SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  if( rc==SQLITE_OK ){
    rc = csStoreHasAnyBranchTip(&cand, &cs->refs, pIsOurs);
    chunkStoreClose(&cand);
  }
  if( rc!=SQLITE_OK ){
    /* Only OOM is inconclusive; other read failures are a failed proof. */
    *pIsOurs = 0;
    if( rc!=SQLITE_NOMEM && rc!=SQLITE_IOERR_NOMEM ) rc = SQLITE_OK;
  }
  return rc;
}

/* *pMovedAdopt distinguishes "the inode at our path changed and the new
** file proved to be ours" from same-file growth: adoption must reload
** by path, never ingest a tail through the stale handle. */
static int csDetectExternalChanges(
  ChunkStore *cs,
  int *pChanged,
  int *pMovedAdopt
){
  DoltliteFileState fileState;
  int bMoved = 0;
  int haveFileState = 0;
  int rc;

  *pChanged = 0;
  if( pMovedAdopt ) *pMovedAdopt = 0;
  if( cs->isMemory || cs->isBuffer ) return SQLITE_OK;

  if( cs->file.pFile==0 ){
    int exists = 0;
    rc = sqlite3OsAccess(cs->file.pVfs, cs->file.zFilename,
                         SQLITE_ACCESS_EXISTS, &exists);
    if( rc!=SQLITE_OK ) return rc;
    if( exists ){
      i64 mainSize = 0;
      rc = csFileSizeByName(cs->file.pVfs, cs->file.zFilename, &mainSize);
      if( rc!=SQLITE_OK ) return rc;
      if( mainSize > 0 ){
        *pChanged = 1;
      }
    }
    return SQLITE_OK;
  }

  fileState.iFileSize = -1;
  fileState.bMoved = 0;
  rc = sqlite3OsDoltliteFileState(cs->file.pFile, &fileState);
  if( rc==SQLITE_OK ){
    bMoved = fileState.bMoved;
    haveFileState = fileState.iFileSize>=0;
  }else{
    rc = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( bMoved ){
    int bOurs = 0;
    rc = csMovedFileIsOurs(cs, &bOurs);
    if( rc!=SQLITE_OK ) return rc;
    if( !bOurs ){
      /* Do not reload-by-path (would adopt foreign content). Reads stay on
      ** the open handle; restoring the file resumes writes. */
      cs->movedReadOnly = 1;
      *pChanged = 0;
      return SQLITE_OK;
    }
    cs->movedReadOnly = 0;
    *pChanged = 1;
    if( pMovedAdopt ) *pMovedAdopt = 1;
    return SQLITE_OK;
  }
  cs->movedReadOnly = 0;

  {
    i64 fileSize = fileState.iFileSize;
    if( !haveFileState ){
      rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
      if( rc!=SQLITE_OK ) return rc;
    }
    if( fileSize > cs->file.iFileSize ){
      int adopted = 0;
      rc = csAdoptMatchingCloseMarker(cs, fileSize, &adopted);
      if( rc!=SQLITE_OK ) return rc;
      if( !adopted ) *pChanged = 1;
    }
  }
  return SQLITE_OK;
}

int chunkStoreHasExternalChanges(ChunkStore *cs, int *pChanged){
  return csDetectExternalChanges(cs, pChanged, 0);
}

/* The store is append-only between compactions: when the file only grew
** and our sealed tail root is still byte-intact, ingest just the new
** records instead of reopening the whole store. Any doubt falls back to
** the full reload. */
static int csIncrementalTailRefresh(ChunkStore *cs){
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  i64 contentEnd;
  i64 rootOff;
  int hashState;
  int rc;

  if( cs->staging.nPending>0 || cs->staging.nRecentUncommitted>0
   || cs->bRefsStale || cs->movedReadOnly || cs->corruptMidStream
   || cs->notADatabase || cs->file.pFile==0
   || cs->wal.iWalOffset<=0
   || cs->wal.nWalData < (i64)sizeof(aRoot)
   || prollyHashCompare(&cs->refs.refsHash,
                        &cs->refs.committedRefsHash)!=0 ){
    return SQLITE_MISMATCH;
  }

  contentEnd = cs->wal.iWalOffset + cs->wal.nWalData;
  rootOff = contentEnd - (i64)sizeof(aRoot);
  if( rootOff < cs->wal.iWalOffset ) return SQLITE_MISMATCH;
  rc = sqlite3OsRead(cs->file.pFile, aRoot, (int)sizeof(aRoot), rootOff);
  /* A short read means the file shrank (compaction/replacement): not
  ** eligible, reload. Any other IO failure is a real error and must not
  ** be absorbed by the reload retry. */
  if( rc==SQLITE_IOERR_SHORT_READ ) return SQLITE_MISMATCH;
  if( rc!=SQLITE_OK ) return rc;
  if( aRoot[0]!=CS_WAL_TAG_ROOT ) return SQLITE_MISMATCH;
  hashState = csManifestHashState(aRoot+1, rootOff);
  if( hashState!=CS_MANIFEST_HASH_OK ){
    hashState = csManifestHashStateOffsetless(aRoot+1);
  }
  if( hashState!=CS_MANIFEST_HASH_OK
   || memcmp(aRoot + 1 + CS_MANIFEST_REFS_HASH_OFF,
             cs->refs.refsHash.data, PROLLY_HASH_SIZE)!=0 ){
    return SQLITE_MISMATCH;
  }

  rc = csReplayWalTail(cs, cs->file.iFileSize);
  if( rc!=SQLITE_OK ) return rc;
  csMarkRefsCommitted(cs);

  /* Fold a grown recent set into the eager index in memory: a sorted
  ** merge every few thousand commits, instead of reopening the store.
  ** Benign: the refresh already succeeded and a failed fold leaves the
  ** recent set intact for the next attempt. */
  if( cs->staging.nRecent>4096 && cs->staging.nPending==0 ){
    int i;
    sqlite3BeginBenignMalloc();
    rc = SQLITE_OK;
    for(i=0; i<cs->staging.nRecent && rc==SQLITE_OK; i++){
      rc = csGrowPending(cs);
      if( rc==SQLITE_OK ){
        cs->staging.aPending[cs->staging.nPending++] =
            cs->staging.aRecent[i];
      }
    }
    if( rc==SQLITE_OK ){
      ChunkIndexEntry *aMerged = 0;
      int nMerged = 0;
      rc = csMergeIndex(cs, &aMerged, &nMerged);
      if( rc==SQLITE_OK ){
        csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase,
                          cs->index.aIndexMmapSize);
        cs->index.aIndex = aMerged;
        cs->index.nIndex = nMerged;
        cs->index.aIndexMmapBase = 0;
        cs->index.aIndexMmapSize = 0;
        cs->staging.nRecent = 0;
        csRecentHTClear(cs);
      }
    }
    cs->staging.nPending = 0;
    csPendHTReset(cs);
    sqlite3EndBenignMalloc();
  }
  return SQLITE_OK;
}

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged){
  int rc;
  int bChanged = 0;
  int bMovedAdopt = 0;
  if( cs->isMemory || cs->isBuffer ){
    *pChanged = 0;
    return SQLITE_OK;
  }
  *pChanged = 0;
  /* Caller installed this path; adopt it. Heuristic detect cannot bless
  ** foreign content. */
  if( cs->adoptReplacement ){
    cs->adoptReplacement = 0;
    cs->movedReadOnly = 0;
    rc = csReloadFromDisk(cs);
    if( rc!=SQLITE_OK ) return rc;
    *pChanged = 1;
    return SQLITE_OK;
  }
  if( cs->snapshotPinned ) return SQLITE_OK;
  rc = csDetectExternalChanges(cs, &bChanged, &bMovedAdopt);
  if( rc!=SQLITE_OK ) return rc;
  if( !bChanged ) return SQLITE_OK;

  if( cs->lockDepth==0 && !bMovedAdopt ){
    rc = csIncrementalTailRefresh(cs);
    if( rc==SQLITE_OK ){
      *pChanged = 1;
      return SQLITE_OK;
    }
    /* MISMATCH = not eligible, take the full reload. A real failure
    ** (NOMEM, IOERR) already rolled back and must surface: the reload
    ** would absorb an injected fault and report success. */
    if( rc!=SQLITE_MISMATCH ) return rc;
  }

  rc = csReloadFromDisk(cs);
  if( rc!=SQLITE_OK ) return rc;
  *pChanged = 1;
  return SQLITE_OK;
}

int chunkStoreForceRefresh(ChunkStore *cs){
  int rc;
  int wasPinned;
  if( cs->isMemory || cs->isBuffer ) return SQLITE_OK;
  /* Moved-file check declined this: reload-by-path would adopt a foreign
  ** store while the catalog stays ours. GC never latches this. */
  if( cs->movedReadOnly ) return SQLITE_READONLY;
  /* Only the outermost lock holder reloads; inner holders keep the outer view. */
  if( cs->lockDepth != 1 ) return SQLITE_OK;
  /* Skip a no-op WAL replay when the tail root still matches. Uncommitted
  ** appends fall through so reload still reports SQLITE_BUSY_SNAPSHOT. */
  if( cs->staging.nRecentUncommitted==0 && csDiskStateMatchesMemory(cs) ){
    return SQLITE_OK;
  }
  /* VC writes need on-disk tips even under a pinned snapshot. */
  wasPinned = cs->snapshotPinned;
  cs->snapshotPinned = 0;
  rc = csReloadFromDiskPreservingLocalRefs(cs);
  cs->snapshotPinned = wasPinned;
  return rc;
}

int csReloadFromDisk(ChunkStore *cs){
  ChunkStore tmp;
  ChunkStoreReloadState saved;
  char *zOldFilename;
  int rc;
  if( cs->staging.nRecentUncommitted > 0 ){
    return SQLITE_BUSY_SNAPSHOT;
  }
  /* No OPEN_CREATE: a vacated path must fail, not become an empty store. */
  rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;

  /* Deferred NOTADB opens succeed; reload must not adopt a short/garbage file. */
  if( tmp.notADatabase ){
    chunkStoreClose(&tmp);
    return SQLITE_NOTADB;
  }

  /* Reload must not replace a writable store with a fallback read-only one. */
  if( tmp.readOnly && !cs->readOnly ){
    chunkStoreClose(&tmp);
    return SQLITE_BUSY;
  }

  csCaptureReloadState(cs, &saved);
  csAdoptOpenedStoreState(cs, &tmp);

  /* pFile->zPath aliases zFilename; keep each filename with its file. */
  zOldFilename = cs->file.zFilename;
  cs->file.zFilename = tmp.file.zFilename;
  tmp.file.zFilename = 0;

  csFreeReloadState(&saved);
  sqlite3_free(zOldFilename);
  chunkStoreClose(&tmp);
  cs->reloadGen++;

  /* Torn-tail recovery rewinds logical EOF in memory; crash garbage past
  ** it retrips the size check. Under the graph lock, truncate it. Failures
  ** are silent, so allocations are benign. */
  if( !cs->readOnly && !cs->corruptMidStream
   && cs->lockDepth>0 ){
    i64 physSize = 0;
    sqlite3BeginBenignMalloc();
    if( sqlite3OsFileSize(cs->file.pFile, &physSize)==SQLITE_OK
     && physSize > cs->file.iFileSize ){
      (void)sqlite3OsTruncate(cs->file.pFile, cs->file.iFileSize);
    }
    sqlite3EndBenignMalloc();
  }

  return SQLITE_OK;
}


#endif
