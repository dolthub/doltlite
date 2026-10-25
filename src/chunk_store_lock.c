#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#ifdef _MSC_VER
# include <malloc.h>
#endif

static int csReloadFromDisk(ChunkStore *cs);

static int csFileLockHeld(CsFileLock pFile){
  return pFile!=0;
}

int csReadLock(ChunkStore *cs, CsReadLock *pLock){
  int rc;
  if( cs->isBuffer || cs->noReadLock || !cs->file.pFile ) return SQLITE_OK;
  if( strcmp(cs->file.pVfs->zName, "unix-excl")==0 ){
    rc = chunkStoreDupFilenameDoubleNul(cs->file.zFilename, &pLock->zName);
    if( rc!=SQLITE_OK ) return rc;
    rc = csOpenFile(cs->file.pVfs, pLock->zName, &pLock->pFile,
                   SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB, 0);
    if( rc!=SQLITE_OK ) return rc;
  }else{
    pLock->pFile = cs->file.pFile;
  }
  return sqlite3OsLock(pLock->pFile, SQLITE_LOCK_SHARED);
}

int csReadUnlock(ChunkStore *cs, CsReadLock *pLock){
  int rc = SQLITE_OK;
  if( pLock->zName ){
    if( pLock->pFile ){
      rc = sqlite3OsUnlock(pLock->pFile, SQLITE_LOCK_NONE);
      sqlite3OsCloseFree(pLock->pFile);
    }
    sqlite3_free(pLock->zName);
  }else if( pLock->pFile && cs->file.pFile ){
    rc = sqlite3OsUnlock(cs->file.pFile, SQLITE_LOCK_NONE);
  }
  memset(pLock, 0, sizeof(*pLock));
  return rc;
}

int csFileLockPromote(sqlite3_file *pFile){
  return sqlite3OsLock(pFile, SQLITE_LOCK_RESERVED);
}

int csReclaimTail(ChunkStore *cs){
  i64 size = 0;
  int rc, rc2, retries = 0;
  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);
  if( !cs->file.pFile ) return SQLITE_OK;
  rc = sqlite3OsFileSize(cs->file.pFile, &size);
  if( rc!=SQLITE_OK || size<=cs->file.iFileSize ) return rc;
  if( !cs->isBuffer ){
    rc = sqlite3OsLock(cs->file.pFile, SQLITE_LOCK_SHARED);
    if( rc==SQLITE_OK ) rc = sqlite3OsLock(cs->file.pFile, SQLITE_LOCK_RESERVED);
    if( rc==SQLITE_OK ){
      do {
        rc = sqlite3OsLock(cs->file.pFile, SQLITE_LOCK_EXCLUSIVE);
        if( rc!=SQLITE_BUSY || retries++>=100 ) break;
        sqlite3_sleep(1);
      }while( rc==SQLITE_BUSY );
    }
  }
  if( rc==SQLITE_OK ) rc = sqlite3OsTruncate(cs->file.pFile, cs->file.iFileSize);
  if( !cs->isBuffer ){
    rc2 = sqlite3OsUnlock(cs->file.pFile, SQLITE_LOCK_NONE);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  return rc;
}

#if SQLITE_OS_UNIX
static void csGuardUnlock(sqlite3_file *pFile){
  int bLock = 0;
  if( pFile ){
    sqlite3OsFileControlHint(pFile, SQLITE_FCNTL_DOLTLITE_GRAPH_LOCK, &bLock);
  }
}

static int csGuardRelock(sqlite3_file *pFile){
  int bLock = 1;
  int bMoved = 0;
  int rc = sqlite3OsFileControl(pFile, SQLITE_FCNTL_DOLTLITE_GRAPH_LOCK, &bLock);
  if( rc==SQLITE_NOTFOUND ) return SQLITE_OK;
  if( rc==SQLITE_OK ){
    rc = sqlite3OsFileControl(pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc==SQLITE_OK && bMoved ) rc = SQLITE_BUSY;
  }
  if( rc!=SQLITE_OK ) csGuardUnlock(pFile);
  return rc;
}

static int csGuardOpen(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int bCreate,
  sqlite3_file **ppFile,
  char **pzName
){
  int rc;
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
  int outFlags = 0;
  if( bCreate ) flags |= SQLITE_OPEN_CREATE;
  *ppFile = 0;
  *pzName = 0;
  rc = chunkStoreDupFilenameDoubleNul(zPath, pzName);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsOpenMalloc(pVfs, *pzName, ppFile, flags, &outFlags);
  }
  if( rc==SQLITE_OK && (outFlags & SQLITE_OPEN_READONLY) ) rc = SQLITE_READONLY;
  if( rc==SQLITE_OK ) rc = csGuardRelock(*ppFile);
  if( rc!=SQLITE_OK ){
    if( *ppFile ) sqlite3OsCloseFree(*ppFile);
    sqlite3_free(*pzName);
    *ppFile = 0;
    *pzName = 0;
  }
  return rc;
}

#endif

int csFileLockOutput(sqlite3_file *pFile){
#if SQLITE_OS_UNIX
  return csGuardRelock(pFile);
#else
  (void)pFile;
  return SQLITE_OK;
#endif
}

static int csFileRelock(CsFileLock pLock, int bCreate){
  int rc;
  if( !pLock ) return SQLITE_ERROR;
  rc = sqlite3OsDoltliteModeOf(pLock->pFile, pLock->zPath, 1);
  if( rc==SQLITE_OK ) rc = sqlite3OsLock(pLock->pFile, SQLITE_LOCK_SHARED);
  if( rc==SQLITE_OK ) rc = csFileLockPromote(pLock->pFile);
#if SQLITE_OS_UNIX
  if( rc==SQLITE_OK && !pLock->readOnly ){
    int bMoved = 1;
    if( pLock->pGuard ){
      rc = sqlite3OsFileControl(pLock->pGuard, SQLITE_FCNTL_HAS_MOVED, &bMoved);
      if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    }
    if( rc==SQLITE_OK && bMoved ){
      if( pLock->pGuard ) sqlite3OsCloseFree(pLock->pGuard);
      pLock->pGuard = 0;
      sqlite3_free(pLock->zGuard);
      pLock->zGuard = 0;
      rc = csGuardOpen(pLock->pVfs, pLock->zPath, bCreate,
                       &pLock->pGuard, &pLock->zGuard);
    }else if( rc==SQLITE_OK ){
      rc = csGuardRelock(pLock->pGuard);
    }
  }
#else
  (void)bCreate;
#endif
  if( rc!=SQLITE_OK ) sqlite3OsUnlock(pLock->pFile, SQLITE_LOCK_NONE);
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

/* Keep the path on the stack so backup fault injection still reaches the
** delete operation at the same allocation. */
int csLockSidecarDelete(sqlite3_vfs *pVfs, const char *zPath){
  int nPath = (int)strlen(zPath);
  const char *zBase = strrchr(zPath, '/');
  int nDir = zBase ? (int)(zBase - zPath) + 1 : 0;
#ifdef _MSC_VER
  char *zLock = (char*)_alloca(nPath + sizeof(".-lock"));
#else
  char zLock[nPath + sizeof(".-lock")];
#endif
  int rc;

  if( nPath - nDir <= 0 ) return SQLITE_OK;
  memcpy(zLock, zPath, nDir);
  zLock[nDir] = '.';
  memcpy(zLock + nDir + 1, zPath + nDir, nPath - nDir);
  memcpy(zLock + nPath + 1, "-lock", sizeof("-lock"));
  rc = sqlite3OsDelete(pVfs, zLock, 0);
  /* Nothing to remove is not a failure. */
  return rc==SQLITE_IOERR_DELETE_NOENT ? SQLITE_OK : rc;
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

int csFileLock(sqlite3_vfs *pVfs, const char *path, int openFlags,
                      CsFileLock *ppFile, char **pzName){
  char *zRaw = csLockPath(path);
  char *zLock = 0;
  sqlite3_file *pFile = 0;
  CsFileLock pLock = 0;
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
  pLock = sqlite3_malloc(sizeof(*pLock));
  if( !pLock ){
    sqlite3OsCloseFree(pFile);
    sqlite3_free(zLock);
    return SQLITE_NOMEM;
  }
  memset(pLock, 0, sizeof(*pLock));
  pLock->pFile = pFile;
  pLock->pVfs = pVfs;
  pLock->readOnly = (openFlags & SQLITE_OPEN_READONLY)!=0;
  rc = chunkStoreDupFilenameDoubleNul(path, &pLock->zPath);
  if( rc==SQLITE_OK ) rc = csFileRelock(pLock, (openFlags & SQLITE_OPEN_CREATE)!=0);
  if( rc!=SQLITE_OK ){
    sqlite3OsCloseFree(pFile);
    sqlite3_free(pLock->zPath);
    sqlite3_free(pLock);
    sqlite3_free(zLock);
    return rc;
  }
  *ppFile = pLock;
  /* unixFile.zPath aliases this buffer until close; caller frees it. */
  *pzName = zLock;
  return SQLITE_OK;
}

static void csFileUnlockKeepOpen(CsFileLock pLock){
  if( pLock ){
#if SQLITE_OS_UNIX
    csGuardUnlock(pLock->pGuard);
#endif
    sqlite3OsUnlock(pLock->pFile, SQLITE_LOCK_NONE);
  }
}

void csFileUnlock(CsFileLock pLock, char **pzName){
  if( pLock ){
    csFileUnlockKeepOpen(pLock);
    if( pLock->pGuard ) sqlite3OsCloseFree(pLock->pGuard);
    sqlite3_free(pLock->zGuard);
    sqlite3_free(pLock->zPath);
    sqlite3OsCloseFree(pLock->pFile);
    sqlite3_free(pLock);
  }
  if( pzName ){
    sqlite3_free(*pzName);
    *pzName = 0;
  }
}

int chunkStoreReplaceLockedFile(ChunkStore *cs, const char *zTmp, int *pRetainTmp){
  int rc;
#if SQLITE_OS_UNIX
  sqlite3_file *pGuard = 0;
  char *zGuard = 0;
  CsFileLock pLock = CS_GRAPH_LOCK(cs);
  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);
  *pRetainTmp = 0;
  /* Both inodes stay locked across rename, even if the sidecar is unlinked. */
  rc = csGuardOpen(cs->file.pVfs, zTmp, 0, &pGuard, &zGuard);
  if( rc!=SQLITE_OK ) return rc;
#endif
  rc = sqlite3OsReplaceFile(cs->file.pVfs, zTmp, cs->file.zFilename, pRetainTmp);
#if SQLITE_OS_UNIX
  if( !*pRetainTmp ){
    sqlite3OsCloseFree(pLock->pGuard);
    pLock->pGuard = pGuard;
    sqlite3_free(pLock->zGuard);
    pLock->zGuard = zGuard;
  }else{
    sqlite3OsCloseFree(pGuard);
    sqlite3_free(zGuard);
  }
#endif
  return rc;
}

#define CS_REPLACEMENT_PROOF_MAGIC 0x32504c44
#define CS_REPLACEMENT_PROOF_SIZE (4 + 2*PROLLY_HASH_SIZE)

/* Backup changes database identity. The displaced refs and an installed
** commit in the graph-lock sidecar let stale peers follow that replacement. */
static int csReadReplacementProof(
  ChunkStore *cs,
  ProllyHash *pFrom,
  ProllyHash *pTo,
  int *pValid
){
  u8 aBuf[CS_REPLACEMENT_PROOF_SIZE];
  int rc;

  *pValid = 0;
  memset(pFrom, 0, sizeof(*pFrom));
  memset(pTo, 0, sizeof(*pTo));
  if( !CS_GRAPH_LOCK(cs) ) return SQLITE_OK;
  rc = sqlite3OsRead(CS_GRAPH_LOCK(cs)->pFile, aBuf, sizeof(aBuf), 0);
  if( rc==SQLITE_IOERR_SHORT_READ ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;
  if( CS_READ_U32(aBuf)==CS_REPLACEMENT_PROOF_MAGIC ){
    memcpy(pFrom->data, aBuf+4, PROLLY_HASH_SIZE);
    memcpy(pTo->data, aBuf+4+PROLLY_HASH_SIZE, PROLLY_HASH_SIZE);
    *pValid = !prollyHashIsEmpty(pFrom) && !prollyHashIsEmpty(pTo);
  }
  return SQLITE_OK;
}

static int csWriteReplacementProof(
  ChunkStore *cs,
  const ProllyHash *pFrom,
  const ProllyHash *pTo
){
  u8 aBuf[CS_REPLACEMENT_PROOF_SIZE];

  if( prollyHashIsEmpty(pFrom) || prollyHashIsEmpty(pTo) ) return SQLITE_OK;
  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);
  CS_WRITE_U32(aBuf, CS_REPLACEMENT_PROOF_MAGIC);
  memcpy(aBuf+4, pFrom->data, PROLLY_HASH_SIZE);
  memcpy(aBuf+4+PROLLY_HASH_SIZE, pTo->data, PROLLY_HASH_SIZE);
  return sqlite3OsWrite(CS_GRAPH_LOCK(cs)->pFile, aBuf, sizeof(aBuf), 0);
}

int chunkStorePublishPathReplacementProof(
  ChunkStore *cs,
  const ProllyHash *pFrom,
  const ProllyHash *pTo
){
  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);
  return csWriteReplacementProof(cs, pFrom, pTo);
}

static int csFileLockNB(sqlite3_vfs *pVfs, const char *path, int openFlags,
                        CsFileLock *ppFile, char **pzName){
  return csFileLock(pVfs, path, openFlags, ppFile, pzName);
}

static int csLockForReplacementProof(ChunkStore *cs, int *pAcquired){
  int rc;
  *pAcquired = 0;
  if( cs->lockDepth>0 ) return SQLITE_OK;
  if( cs->pLockMutex && sqlite3_mutex_try(cs->pLockMutex)!=SQLITE_OK ){
    return SQLITE_BUSY;
  }
  if( csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
    rc = csFileRelock(CS_GRAPH_LOCK(cs), 0);
  }else{
    rc = csFileLockNB(cs->file.pVfs, cs->file.zFilename,
                      (cs->file.pFile ? 0 : SQLITE_OPEN_CREATE)
                       | (cs->readOnly ? SQLITE_OPEN_READONLY : 0),
                      &CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
  }
  if( rc==SQLITE_OK ){
    cs->lockDepth = 1;
    *pAcquired = 1;
  }
  if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
  return rc;
}


int chunkStoreEnsureRefsFresh(ChunkStore *cs){
  int rc;
  if( cs->openPending ){
    int changed;
    rc = chunkStoreRefreshIfChanged(cs, &changed);
    if( rc!=SQLITE_OK ) return rc;
  }
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
  cs->bReloadAfterRefsConflict = 1;
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

  if( cs->bPublishDeferred ) return SQLITE_OK;
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


static int csMovedFileIsOursAfterTail(ChunkStore *cs, int *pIsOurs);

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
    rc = csMovedFileIsOursAfterTail(cs, &bOurs);
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
  if( cs->bLockProbe && cs->lockDepth==0 ){
    cs->bLockProbeHit = 1;
    return SQLITE_BUSY;
  }
  if( cs->pLockMutex && sqlite3_mutex_try(cs->pLockMutex)!=SQLITE_OK ){
    return SQLITE_BUSY;
  }
  if( cs->lockDepth > 0 ){
    cs->lockDepth++;
    if( cs->nOuterLockHold && cs->lockDepth==1+cs->nOuterLockHold ){
      rc = csNoteMovedUnderLock(cs);
      if( rc==SQLITE_OK ) rc = chunkStoreRefreshIfChanged(cs, &changed);
      if( rc!=SQLITE_OK ){
        cs->lockDepth--;
        if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
        return rc;
      }
      if( pChanged ) *pChanged = changed;
    }
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return SQLITE_OK;
  }
  if( !cs->file.zFilename ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return SQLITE_ERROR;
  }
  if( csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
    rc = csFileRelock(CS_GRAPH_LOCK(cs), 0);
  }else{
    rc = csFileLockNB(cs->file.pVfs, cs->file.zFilename,
                      (cs->file.pFile ? 0 : SQLITE_OPEN_CREATE)
                       | (cs->readOnly ? SQLITE_OPEN_READONLY : 0),
                      &CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
  }
  if( (rc & 0xff)==SQLITE_CANTOPEN && cs->file.pFile ){
    int bMoved = 0;
    int rcMoved = sqlite3OsFileControl(cs->file.pFile,
                                      SQLITE_FCNTL_HAS_MOVED, &bMoved);
    if( rcMoved==SQLITE_OK && bMoved ){
      cs->movedReadOnly = 1;
      rc = SQLITE_READONLY;
    }else if( rcMoved!=SQLITE_OK && rcMoved!=SQLITE_NOTFOUND ){
      rc = rcMoved;
    }
  }
  if( rc!=SQLITE_OK ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return rc;
  }
  cs->lockDepth = 1;
  if( cs->bLockHoldOnly ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return SQLITE_OK;
  }
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

static int csStoreHasAnyBranchTip(ChunkStore *pCand, const BranchRef *aBr,
                                  int nBr, int *pHas){
  int i;
  *pHas = 0;
  for(i=0; i<nBr; i++){
    const ProllyHash *pTip = &aBr[i].commitHash;
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
** tips or the sanctioned replacement proof. Empty files are never moved.
** Working-set roots are not proof — a peer WS advance lets GC sweep the one
** we remember. No proof: upstream read-only. */
static int csMovedFileIsOurs(ChunkStore *cs, int *pIsOurs){
  ChunkStore cand;
  ProllyHash from;
  ProllyHash to;
  int acquired = 0;
  int lockBusy = 0;
  int valid = 0;
  int rc;

  *pIsOurs = 0;
  if( cs->isMemory || cs->isBuffer || cs->file.pFile==0 ) return SQLITE_OK;
  if( cs->file.zFilename==0 || cs->file.zFilename[0]==0 ) return SQLITE_OK;
  if( cs->file.iFileSize<=0 ){
    *pIsOurs = 1;
    return SQLITE_OK;
  }

  rc = csLockForReplacementProof(cs, &acquired);
  /* A peer holding the lock only blocks the replacement-proof record; the
  ** branch-tip check reads the candidate as any fresh open does. */
  if( rc==SQLITE_BUSY ){
    lockBusy = 1;
    rc = SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreOpen(&cand, cs->file.pVfs, cs->file.zFilename,
                        SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB);
  }
  if( rc==SQLITE_OK ){
    /* A commit detaches its local refs across the lock; they are still ours. */
    if( cs->pDetachedRefs ){
      rc = csStoreHasAnyBranchTip(&cand, cs->pDetachedRefs->aBranches,
                                  cs->pDetachedRefs->nBranches, pIsOurs);
    }else{
      rc = csStoreHasAnyBranchTip(&cand, cs->refs.aBranches,
                                  cs->refs.nBranches, pIsOurs);
    }
    if( rc==SQLITE_OK && !*pIsOurs && lockBusy ){
      rc = SQLITE_BUSY;
    }else if( rc==SQLITE_OK && !*pIsOurs ){
      rc = csReadReplacementProof(cs, &from, &to, &valid);
      if( rc==SQLITE_OK && valid
       && prollyHashCompare(&cs->refs.committedRefsHash, &from)==0 ){
        rc = chunkStoreHas(&cand, &to, pIsOurs);
      }
    }
    chunkStoreClose(&cand);
  }
  if( acquired ) chunkStoreUnlock(cs);
  if( rc!=SQLITE_OK ){
    /* OOM and a peer holding the lock are inconclusive; other read failures
    ** are a failed proof. */
    *pIsOurs = 0;
    if( rc!=SQLITE_NOMEM && rc!=SQLITE_IOERR_NOMEM && rc!=SQLITE_BUSY ){
      rc = SQLITE_OK;
    }
  }
  return rc;
}

static int csIncrementalTailRefresh(ChunkStore *cs);

/* A peer's amend or reset followed by GC sweeps every tip we remember.
** The tips it moved to were appended to the file we still hold open, so
** learn them from that tail and retry the proof. */
static int csMovedFileIsOursAfterTail(ChunkStore *cs, int *pIsOurs){
  i64 heldSize = 0;
  int rc = csMovedFileIsOurs(cs, pIsOurs);
  if( rc!=SQLITE_OK || *pIsOurs ) return rc;
  rc = sqlite3OsFileSize(cs->file.pFile, &heldSize);
  if( rc!=SQLITE_OK || heldSize<=cs->file.iFileSize ) return rc;
  rc = csIncrementalTailRefresh(cs);
  if( rc==SQLITE_MISMATCH ) return SQLITE_OK;
  if( rc!=SQLITE_OK ) return rc;
  return csMovedFileIsOurs(cs, pIsOurs);
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
    rc = csMovedFileIsOursAfterTail(cs, &bOurs);
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
    if( fileSize > cs->file.iFileSize && fileSize!=cs->iFailedTailEnd ){
      int adopted = 0;
      rc = csAdoptMatchingCloseMarker(cs, fileSize, &adopted);
      if( rc!=SQLITE_OK ) return rc;
      if( !adopted ) *pChanged = 1;
    }
    if( !*pChanged && csDamageFingerDiffers(cs) ) *pChanged = 1;
  }
  return SQLITE_OK;
}

static int csFileSnapshotUnchanged(ChunkStore *cs){
  DoltliteFileState state;
  int rc;
  if( cs->openPending || cs->bReloadAfterRefsConflict
   || cs->adoptReplacement ) return 0;
  state.iFileSize = -1;
  state.bMoved = 0;
  rc = sqlite3OsDoltliteFileState(cs->file.pFile, &state);
  if( rc!=SQLITE_OK || state.bMoved || state.iFileSize<0
   || state.iFileSize>cs->file.iFileSize ) return 0;
  if( csDamageFingerDiffers(cs) ) return 0;
  cs->movedReadOnly = 0;
  return 1;
}

int chunkStoreHasExternalChanges(ChunkStore *cs, int *pChanged){
  int rc, rc2;
  CsReadLock readLock = {0, 0};
  int locked = cs->file.pFile && !cs->isBuffer
            && !cs->noReadLock && cs->lockDepth==0;
  *pChanged = 0;
  if( locked && csFileSnapshotUnchanged(cs) ) return SQLITE_OK;
  if( locked ){
    rc = csReadLock(cs, &readLock);
    if( rc!=SQLITE_OK ){
      csReadUnlock(cs, &readLock);
      return rc==SQLITE_BUSY && !cs->openPending ? SQLITE_OK : rc;
    }
  }
  if( cs->openPending ){
    *pChanged = 1;
    rc = SQLITE_OK;
  }else{
    rc = csDetectExternalChanges(cs, pChanged, 0);
  }
  if( locked ){
    rc2 = csReadUnlock(cs, &readLock);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  return rc;
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

  rc = csScrubFailedRoot(cs);
  if( rc!=SQLITE_OK ) return rc;

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

static int csRefreshIfChanged(ChunkStore *cs, int *pChanged){
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
  if( cs->openPending || cs->bReloadAfterRefsConflict ){
    rc = csReloadFromDisk(cs);
    if( rc!=SQLITE_OK ) return rc;
    cs->openPending = 0;
    cs->bReloadAfterRefsConflict = 0;
    *pChanged = 1;
    return SQLITE_OK;
  }
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

static int csRefreshIfChangedReadLocked(
  ChunkStore *cs, int *pChanged, int bBusyFails
){
  int rc, rc2;
  CsReadLock readLock = {0, 0};
  int locked = cs->file.pFile && !cs->isBuffer
            && !cs->noReadLock && cs->lockDepth==0;
  *pChanged = 0;
  if( locked && csFileSnapshotUnchanged(cs) ) return SQLITE_OK;
  if( locked ){
    rc = csReadLock(cs, &readLock);
    if( rc!=SQLITE_OK ){
      csReadUnlock(cs, &readLock);
      return rc==SQLITE_BUSY && !cs->openPending && !bBusyFails
             ? SQLITE_OK : rc;
    }
  }
  rc = csRefreshIfChanged(cs, pChanged);
  if( locked ){
    rc2 = csReadUnlock(cs, &readLock);
    if( rc==SQLITE_OK ) rc = rc2;
  }
  return rc;
}

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged){
  return csRefreshIfChangedReadLocked(cs, pChanged, 0);
}

/* A statement that answers from the old snapshot while a publishing writer
** holds the read lock off misses rows already acknowledged to the writer. */
int chunkStoreRefreshIfChangedOrBusy(ChunkStore *cs, int *pChanged){
  return csRefreshIfChangedReadLocked(cs, pChanged, 1);
}

int chunkStoreForceRefresh(ChunkStore *cs){
  int rc;
  int wasPinned;
  if( cs->isMemory || cs->isBuffer ) return SQLITE_OK;
  /* Moved-file check declined this: reload-by-path would adopt a foreign
  ** store while the catalog stays ours. GC never latches this. */
  if( cs->movedReadOnly ) return SQLITE_READONLY;
  /* Only the outermost lock holder reloads; inner holders keep the outer view. */
  if( cs->lockDepth != 1 + cs->nOuterLockHold ) return SQLITE_OK;
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

int chunkStoreAdoptDiskState(ChunkStore *cs){
  int rc;
  int wasPinned;
  if( cs->isMemory || cs->isBuffer ) return SQLITE_OK;
  if( cs->movedReadOnly ) return SQLITE_READONLY;
  /* Only the outermost lock holder reloads. */
  if( cs->lockDepth != 1 + cs->nOuterLockHold ) return SQLITE_OK;
  if( cs->staging.nRecentUncommitted>0 ) return SQLITE_BUSY_SNAPSHOT;
  /* Same sealed tail and no local ref edit: nothing to adopt. */
  if( csDiskStateMatchesMemory(cs)
   && prollyHashCompare(&cs->refs.refsHash,
                        &cs->refs.committedRefsHash)==0 ){
    return SQLITE_OK;
  }
  wasPinned = cs->snapshotPinned;
  cs->snapshotPinned = 0;
  rc = csReloadFromDisk(cs);
  cs->snapshotPinned = wasPinned;
  return rc;
}

static int csReloadFromDisk(ChunkStore *cs){
  ChunkStore tmp;
  ChunkStoreReloadState saved;
  char *zOldFilename;
  int rc;
  /* The command holding the lock is the only writer, so disk is its start
  ** state; reloading would drop what it has staged to publish. */
  if( cs->bPublishDeferred ) return SQLITE_OK;
  rc = csScrubFailedRoot(cs);
  if( rc!=SQLITE_OK ) return rc;
  if( cs->staging.nRecentUncommitted > 0 ){
    return SQLITE_BUSY_SNAPSHOT;
  }
  /* No OPEN_CREATE: a vacated path must fail, not become an empty store.
  ** Match the connection's read-only bit so a RO handle is not reopened RW. */
  {
    int openFlags = SQLITE_OPEN_MAIN_DB;
    if( cs->readOnly ) openFlags |= SQLITE_OPEN_READONLY;
    else openFlags |= SQLITE_OPEN_READWRITE;
    rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename, openFlags);
  }
  if( rc!=SQLITE_OK ) return rc;

  /* Deferred NOTADB and header-CORRUPT opens succeed; reload must not adopt
  ** a short, garbage, or damaged file as an empty store. Mid-stream WAL
  ** damage still reloads: its recovered prefix is what gc needs to name the
  ** missing chunk. */
  if( tmp.notADatabase ){
    chunkStoreClose(&tmp);
    return SQLITE_NOTADB;
  }
  if( tmp.corruptHeader ){
    chunkStoreClose(&tmp);
    return SQLITE_CORRUPT;
  }

  /* Reload must not replace a writable store with a fallback read-only one. */
  if( tmp.readOnly && !cs->readOnly ){
    chunkStoreClose(&tmp);
    return SQLITE_BUSY;
  }

  {
    int keepReadOnly = cs->readOnly;
    csCaptureReloadState(cs, &saved);
    csAdoptOpenedStoreState(cs, &tmp);
    cs->readOnly = keepReadOnly;
  }

  /* pFile->zPath aliases zFilename; keep each filename with its file. */
  zOldFilename = cs->file.zFilename;
  cs->file.zFilename = tmp.file.zFilename;
  tmp.file.zFilename = 0;

  csFreeReloadState(&saved);
  sqlite3_free(zOldFilename);
  chunkStoreClose(&tmp);
  cs->reloadGen++;

  if( !cs->readOnly && !cs->corruptMidStream
   && cs->lockDepth>0 ){
    sqlite3BeginBenignMalloc();
    (void)csReclaimTail(cs);
    sqlite3EndBenignMalloc();
  }

  return SQLITE_OK;
}


#endif
