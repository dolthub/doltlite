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

/* Classify the directory that will hold the graph-lock sidecar for zPath.
** Sets *pExists and *pWritable. A missing directory must not be treated as
** "not writable" — that would turn CANTOPEN (ENOENT) into READONLY_DIRECTORY
** and break clone of a file:// URL under a nonexistent parent. */
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
  /* Opened with SQLITE_OPEN_MAIN_DB, so the name must carry the VFS's
  ** double-nul terminator (csLockPath returns a singly-terminated string). */
  rc = chunkStoreDupFilenameDoubleNul(zRaw, &zLock);
  sqlite3_free(zRaw);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3OsOpenMalloc(pVfs, zLock, &pFile, flags, 0);
  if( rc!=SQLITE_OK ){
    int exists = 0;
    int canWrite = 0;
    int rcDir = csDirectoryAccess(pVfs, path, &exists, &canWrite);
    sqlite3_free(zLock);
    /* The graph lock is a sidecar create in the DB directory — the same
    ** dependency stock SQLite has on creating -journal/-wal. When the
    ** directory exists but is not writable, report READONLY_DIRECTORY so
    ** the primary code is SQLITE_READONLY with stock's "attempt to write a
    ** readonly database" message (misc7-23). Missing parents stay as the
    ** original open error (typically CANTOPEN), matching stock EACCES vs
    ** ENOENT handling for new journals. */
    if( rcDir==SQLITE_OK && exists && !canWrite ){
      return SQLITE_READONLY_DIRECTORY;
    }
    return rc;
  }
  /* SQLite's unix VFS asserts the lock ladder under SQLITE_DEBUG: from
  ** NO_LOCK the only legal step is SHARED, then escalate. Jumping straight
  ** to EXCLUSIVE works with NDEBUG but aborts debug builds (e.g. assert
  ** smoke in development-builds). */
  rc = csFileRelock(pFile);
  if( rc!=SQLITE_OK ){
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

/* Reinstate a session's uncommitted ref changes after the store adopted
** the on-disk state. When no peer committed refs since the base the
** local arrays were derived from, the wholesale restore is exact. When
** peers moved, restoring wholesale would publish the stale snapshot at
** the next commit, silently reverting every peer change except the one
** tip the branch CAS checks — the lost merged rows and resurrected temp
** branches of the stress failures — so the local delta merges onto the
** peers' table instead, and a ref both sides changed fails the operation
** rather than picking a winner. On success pSaved is consumed; on
** failure the local view is reinstated so the failing operation unwinds
** over known state. */
int csRestoreOrMergeLocalRefs(
  ChunkStore *cs,
  SavedRefsState *pSaved,
  const ProllyHash *pSavedRefsHash,
  const ProllyHash *pBaseRefsHash
){
  ProllyHash diskRefsHash;
  int diskMoved = 0;
  int rc;
  /* committedRefsHash is only a proxy for what disk holds. An operation that
  ** lost this merge reinstates a base-derived view but leaves that field at
  ** the disk hash, and a later lock refresh that finds the file unchanged
  ** never re-adopts it -- so a commit retried without a rollback would read
  ** "base unchanged" and republish the stale table, the very clobber this
  ** merge exists to prevent. Consult the sealed manifest tail as well, and
  ** merge whenever it proves disk has moved past the base. */
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
    /* The commit publishes the blob refsHash names, and the blob staged
    ** before the merge holds only the local view; serialize the merged
    ** table so the commit publishes it. */
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
  /* The reinstated tables derive from the base, so that is what they are
  ** committed against -- leaving this at the disk hash the refresh adopted
  ** would make the next commit capture disk as its base and see nothing to
  ** merge. */
  cs->refs.committedRefsHash = *pBaseRefsHash;
  return rc;
}

int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs){
  int rc;
  int preserveRefs;
  ProllyHash savedRefsHash;
  ProllyHash baseRefsHash;
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
  rc = chunkStoreRefreshIfChanged(cs, &changed);
  if( rc!=SQLITE_OK ){
    cs->lockDepth = 0;
    csFileUnlockKeepOpen(CS_GRAPH_LOCK(cs));
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
    return rc;
  }
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
      csFileUnlockKeepOpen(CS_GRAPH_LOCK(cs));
    }
    if( cs->pLockMutex ) sqlite3_mutex_leave(cs->pLockMutex);
  }
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

/* Whether the file now at this handle's path is provably the database this handle
** opened. Upstream makes any moved file read-only for good; GC is DoltLite's one
** legitimate reason for the path to hold a different file, and a GC replacement,
** a rename and a vacant path are indistinguishable from the outside. So require
** proof: the candidate holds a branch tip of ours. GC preserves every commit
** reachable from the refs it sweeps under and our tip stays an ancestor of
** whatever the branch has since become, so this survives an arbitrarily stale
** handle -- the contract gc_tip_survival_test pins. No proof leaves the upstream
** verdict in place.
**
** Working-set roots cannot be the proof: a peer advancing the working set makes
** GC sweep the one we remember, so a live database fails its own test.
**
** Never written means never moved: the VFS reports moved whenever it cannot
** resolve the path at all, so a store whose file is still to be created -- a
** fresh database, or a backup target before its first write -- is
** indistinguishable from one renamed away. */
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
    /* Only OOM is inconclusive; every other failure to read the candidate --
    ** vacant path, unreadable, not a database -- is simply a failed proof. */
    *pIsOurs = 0;
    if( rc!=SQLITE_NOMEM && rc!=SQLITE_IOERR_NOMEM ) rc = SQLITE_OK;
  }
  return rc;
}

static int csDetectExternalChanges(ChunkStore *cs, int *pChanged){
  DoltliteFileState fileState;
  int bMoved = 0;
  int haveFileState = 0;
  int rc;

  *pChanged = 0;
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
      /* Reporting a change would reload by path and adopt whatever is there.
      ** Reads keep serving the open handle; nothing is latched, so moving the
      ** file back resumes normal operation. */
      cs->movedReadOnly = 1;
      *pChanged = 0;
      return SQLITE_OK;
    }
    cs->movedReadOnly = 0;
    *pChanged = 1;
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
  if( cs->isMemory || cs->isBuffer ){
    *pChanged = 0;
    return SQLITE_OK;
  }
  *pChanged = 0;
  /* The caller installed a database at this path itself, so adopt it whatever it
  ** holds -- external-change detection has no basis to bless foreign content, and
  ** a pinned snapshot has nothing left to pin. */
  if( cs->adoptReplacement ){
    cs->adoptReplacement = 0;
    cs->movedReadOnly = 0;
    rc = csReloadFromDisk(cs);
    if( rc!=SQLITE_OK ) return rc;
    *pChanged = 1;
    return SQLITE_OK;
  }
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
  if( cs->isMemory || cs->isBuffer ) return SQLITE_OK;
  /* The refresh the moved-file check declined, refused for the reason it
  ** declined it: reloading by path here would adopt the foreign store while the
  ** caller's catalog stays ours, and every caller is on a write path. A peer's
  ** gc never latches this -- our tips survive its sweep -- so the only handles
  ** refused are ones whose file genuinely is not at their path. */
  if( cs->movedReadOnly ) return SQLITE_READONLY;
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
  /* No OPEN_CREATE: a reload re-derives state from the path, so allowing it to
  ** create means a vacated path is fabricated as an empty store and then adopted
  ** over the live one. Reopening must fail instead. */
  rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;

  /* Deferred NOTADB opens succeed for stock open timing, but a reload must
  ** not adopt a short/garbage file over a live store. */
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

  /* A torn-tail recovery rewinds the logical EOF in memory only, and the
  ** physical crash garbage past it re-trips the external-change size check
  ** on every lock acquisition, turning each write transaction into another
  ** full reload and replay. Under the graph lock nothing can be appending,
  ** so bytes past the freshly replayed boundary are dead — reclaim them.
  ** The reclaim is a pure optimization and every failure inside it is a
  ** silent no-op, so its allocations must not read as swallowed OOM under
  ** fault injection. */
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
