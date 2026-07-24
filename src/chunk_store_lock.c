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
  /* Opened with SQLITE_OPEN_MAIN_DB, so the name must carry the VFS's
  ** double-nul terminator (csLockPath returns a singly-terminated string). */
  rc = chunkStoreDupFilenameDoubleNul(zRaw, &zLock);
  sqlite3_free(zRaw);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3OsOpenMalloc(pVfs, zLock, &pFile, flags, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zLock);
    return rc;
  }
  /* SQLite's unix VFS asserts the lock ladder under SQLITE_DEBUG: from
  ** NO_LOCK the only legal step is SHARED, then escalate. Jumping straight
  ** to EXCLUSIVE works with NDEBUG but aborts debug builds (e.g. assert
  ** smoke in development-builds). */
  rc = sqlite3OsLock(pFile, SQLITE_LOCK_SHARED);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsLock(pFile, SQLITE_LOCK_EXCLUSIVE);
  }
  if( rc!=SQLITE_OK ){
    sqlite3OsUnlock(pFile, SQLITE_LOCK_NONE);
    sqlite3OsCloseFree(pFile);
    sqlite3_free(zLock);
    return rc;
  }
  *ppFile = pFile;
  /* The unix VFS aliases unixFile.zPath to this buffer (it does not copy),
  ** and SQLite's xOpen contract requires the name to stay valid until close.
  ** Hand ownership to the caller, which frees it via csFileUnlock. */
  *pzName = zLock;
  return SQLITE_OK;
}

void csFileUnlock(sqlite3_file *pFile, char **pzName){
  if( pFile ){
    sqlite3OsUnlock(pFile, SQLITE_LOCK_NONE);
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

int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs){
  int rc;
  int preserveRefs;
  ProllyHash savedRefsHash;
  SavedRefsState savedRefs;

  /* Reloading drops aRecent; never do that with uncommitted refs to it. */
  if( cs->staging.nRecentUncommitted > 0 ){
    return SQLITE_BUSY_SNAPSHOT;
  }

  preserveRefs = cs->staging.nPending > 0
              && prollyHashCompare(&cs->refs.refsHash,
                                   &cs->refs.committedRefsHash)!=0;
  memset(&savedRefs, 0, sizeof(savedRefs));
  if( preserveRefs ){
    savedRefsHash = cs->refs.refsHash;
    csDetachSavedRefsState(cs, &savedRefs);
  }

  rc = csReloadFromDisk(cs);

  if( preserveRefs ){
    if( rc==SQLITE_OK ){
      csFreeRefsState(cs);
    }
    csRestoreSavedRefsState(cs, &savedRefs);
    cs->refs.refsHash = savedRefsHash;
  }
  return rc;
}


int chunkStoreLockAndRefreshChanged(ChunkStore *cs, int *pChanged){
  int changed = 0;
  int rc;
  if( pChanged ) *pChanged = 0;
  if( cs->isMemory ) return SQLITE_OK;
  if( cs->pLockMutex && sqlite3_mutex_try(cs->pLockMutex)!=SQLITE_OK ){
    return SQLITE_BUSY;
  }
  if( cs->lockDepth > 0 ){
    cs->lockDepth++;
    return SQLITE_OK;
  }
  if( !cs->file.zFilename ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return SQLITE_ERROR;
  }
  rc = csFileLockNB(cs->file.pVfs, cs->file.zFilename, &CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
  if( rc!=SQLITE_OK ){
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return rc;
  }
  rc = chunkStoreRefreshIfChanged(cs, &changed);
  if( rc!=SQLITE_OK ){
    csFileUnlock(CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
    CS_GRAPH_LOCK(cs) = CS_FILE_LOCK_INIT;
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return rc;
  }
  cs->lockDepth = 1;
  if( pChanged ) *pChanged = changed;
  return SQLITE_OK;
}

int chunkStoreLockAndRefresh(ChunkStore *cs){
  return chunkStoreLockAndRefreshChanged(cs, 0);
}

void chunkStoreUnlock(ChunkStore *cs){
  if( cs->lockDepth > 0 ){
    cs->lockDepth--;
    if( cs->lockDepth == 0 && csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
      csFileUnlock(CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
      CS_GRAPH_LOCK(cs) = CS_FILE_LOCK_INIT;
    }
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
  }else if( csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
    csFileUnlock(CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
    CS_GRAPH_LOCK(cs) = CS_FILE_LOCK_INIT;
  }
}

static int csDetectExternalChanges(ChunkStore *cs, int *pChanged){
  int bMoved = 0;
  int rc;

  *pChanged = 0;
  if( cs->isMemory ) return SQLITE_OK;

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

  /* Recheck HAS_MOVED on each lock; GC may atomically replace the file. */
  rc = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
  if( rc!=SQLITE_OK ) return rc;
  if( bMoved ){
    *pChanged = 1;
    return SQLITE_OK;
  }

  {
    i64 fileSize = 0;
    rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
    if( rc!=SQLITE_OK ) return rc;
    if( fileSize > cs->file.iFileSize ){
      *pChanged = 1;
    }
  }
  return SQLITE_OK;
}

int chunkStoreHasExternalChanges(ChunkStore *cs, int *pChanged){
  return csDetectExternalChanges(cs, pChanged);
}

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged){
  int rc;
  int bChanged = 0;
  if( cs->isMemory ){
    *pChanged = 0;
    return SQLITE_OK;
  }
  *pChanged = 0;
  if( cs->snapshotPinned ) return SQLITE_OK;
  rc = csDetectExternalChanges(cs, &bChanged);
  if( rc!=SQLITE_OK ) return rc;
  if( !bChanged ) return SQLITE_OK;

  rc = csReloadFromDisk(cs);
  if( rc!=SQLITE_OK ) return rc;
  *pChanged = 1;
  return SQLITE_OK;
}

int chunkStoreForceRefresh(ChunkStore *cs){
  int rc;
  int wasPinned;
  if( cs->isMemory ) return SQLITE_OK;
  /* Only the outermost lock holder reloads; a reentrant (inner) holder runs
  ** under the outer scope's already-current view, so a multi-step VC op that
  ** holds the lock across sub-operations keeps the in-memory state it builds. */
  if( cs->lockDepth != 1 ) return SQLITE_OK;
  /* Fast path: when the tail root record proves the on-disk store still
  ** matches this handle's state, the reload — which replays the entire WAL —
  ** is a no-op and can be skipped. Local uncommitted appends fall through so
  ** the reload path keeps reporting SQLITE_BUSY_SNAPSHOT. */
  if( cs->staging.nRecentUncommitted==0 && csDiskStateMatchesMemory(cs) ){
    return SQLITE_OK;
  }
  /* Reload even under a pinned snapshot — the VC write needs the true on-disk
  ** branch tips. Unpin around it (the heuristic refresh path suppresses reloads
  ** while pinned), then restore the pin. */
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
  /* Direct refresh also reaches the reload path. */
  if( cs->staging.nRecentUncommitted > 0 ){
    return SQLITE_BUSY_SNAPSHOT;
  }
  rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;

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

  return SQLITE_OK;
}


#endif
