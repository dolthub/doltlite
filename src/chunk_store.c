

#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"
#include "../ext/blake3/blake3.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#ifdef SQLITE_CRASH_TEST
#include <unistd.h>
#endif

static int csFileLockHeld(sqlite3_file *pFile){
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

static int csFileLock(sqlite3_vfs *pVfs, const char *path,
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
  rc = sqlite3OsLock(pFile, SQLITE_LOCK_EXCLUSIVE);
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

static void csFileUnlock(sqlite3_file *pFile, char **pzName){
  if( pFile ){
    sqlite3OsUnlock(pFile, SQLITE_LOCK_NONE);
    sqlite3OsCloseFree(pFile);
  }
  if( pzName ){
    sqlite3_free(*pzName);
    *pzName = 0;
  }
}

static int csFileLockNB(sqlite3_vfs *pVfs, const char *path,
                        sqlite3_file **ppFile, char **pzName){
  return csFileLock(pVfs, path, ppFile, pzName);
}

typedef sqlite3_file *CsFileLock;
# define CS_FILE_LOCK_INIT 0
# define CS_GRAPH_LOCK(cs) ((cs)->pGraphLockFile)

static int csReloadFromDisk(ChunkStore *cs);

static int csCrashWriteInjectionActive(void){
#ifdef SQLITE_TEST
  const char *zEnv = getenv("DOLTLITE_CRASH_WRITE");
  return zEnv && atoi(zEnv)>0;
#else
  return 0;
#endif
}

#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
/* Test hook: force one mid-commit reload after pending chunks drain. */
static int csReloadInjectionActive(void){
  const char *zEnv = getenv("DOLTLITE_RELOAD_INJECT");
  return zEnv && atoi(zEnv)>0;
}
#endif

#define CS_RECENT_FAST_PATH_MAX 16384
#define CS_WRITEBUF_RETAIN_MAX (64*1024)
#define CS_PENDING_DRAIN_LIMIT (64*1024*1024)

static i64 csPendingDrainLimit(void){
#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
  const char *zEnv = getenv("DOLTLITE_CHUNK_PENDING_DRAIN_LIMIT");
  if( zEnv && zEnv[0] ){
    int n = atoi(zEnv);
    if( n>0 ) return (i64)n;
  }
#endif
  return (i64)CS_PENDING_DRAIN_LIMIT;
}

#if CHUNK_STORE_LE_PACKING
typedef char chunk_index_entry_size_check[
  (sizeof(ChunkIndexEntry) == CHUNK_INDEX_ENTRY_SIZE) ? 1 : -1
];
#endif


static int csOpenFile(
  sqlite3_vfs *pVfs,
  const char *zPath,
  sqlite3_file **ppFile,
  int flags,
  int *pOutFlags
){
  int rc;
  int outFlags = 0;
  rc = sqlite3OsOpenMalloc(pVfs, zPath, ppFile, flags, &outFlags);
  if( pOutFlags ) *pOutFlags = outFlags;
  return rc;
}

static int csFileSizeByName(sqlite3_vfs *pVfs, const char *zPath, i64 *pSize){
  sqlite3_file *pFile = 0;
  int flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB;
  int rc;

  *pSize = 0;
  rc = csOpenFile(pVfs, zPath, &pFile, flags, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3OsFileSize(pFile, pSize);
  sqlite3OsCloseFree(pFile);
  return rc;
}

/* See chunk_store.h. Terminates with two nuls for the VFS xOpen() main-db
** filename contract. */
int chunkStoreDupFilenameDoubleNul(const char *z, char **pzOut){
  int n = (int)strlen(z);
  char *p = (char*)sqlite3_malloc(n + 2);
  if( !p ) return SQLITE_NOMEM;
  memcpy(p, z, n);
  p[n] = '\0';
  p[n + 1] = '\0';
  *pzOut = p;
  return SQLITE_OK;
}

static int csCanonicalFilename(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  char **pzOut
){
  int nPath;
  int rc;
  char *zFull;

  *pzOut = 0;
  assert( pVfs!=0 );

  nPath = pVfs->mxPathname + 1;
  zFull = (char*)sqlite3_malloc(nPath);
  if( !zFull ) return SQLITE_NOMEM;

  rc = sqlite3OsFullPathname(pVfs, zFilename, nPath, zFull);
  if( rc==SQLITE_OK || rc==SQLITE_OK_SYMLINK ){
    rc = chunkStoreDupFilenameDoubleNul(zFull, pzOut);
    sqlite3_free(zFull);
    return rc;
  }
  sqlite3_free(zFull);
  return rc;
}

static int csRollbackFailedAppend(ChunkStore *cs, i64 origFileSize){
  sqlite3_int64 sizeNow = -1;
  int rc = SQLITE_OK;

  if( !cs->file.pFile ) return SQLITE_IOERR;

  rc = sqlite3OsTruncate(cs->file.pFile, origFileSize);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsFileSize(cs->file.pFile, &sizeNow);
  }
  if( rc==SQLITE_OK && sizeNow==origFileSize ){
    (void)sqlite3OsSync(cs->file.pFile, SQLITE_SYNC_NORMAL);
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
    (void)sqlite3OsSync(cs->file.pFile, SQLITE_SYNC_NORMAL);
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

/* Repair a stale refs table (cleared by an OOM rollback) by reloading it
** from the committed refs blob. Called before refs lookups that would
** otherwise read an empty table as a valid empty database. */
int chunkStoreEnsureRefsFresh(ChunkStore *cs){
  int rc;
  if( !cs->bRefsStale ) return SQLITE_OK;
  rc = chunkStoreReloadRefs(cs);
  if( rc==SQLITE_OK ) cs->bRefsStale = 0;
  return rc;
}

static int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs){
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

void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + CS_MANIFEST_MAGIC_OFF, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + CS_MANIFEST_VERSION_OFF, CHUNK_STORE_VERSION);

  CS_WRITE_U32(aBuf + CS_MANIFEST_CHUNK_COUNT_OFF, (u32)cs->index.nChunks);
  CS_WRITE_I64(aBuf + CS_MANIFEST_INDEX_OFFSET_OFF, cs->index.iIndexOffset);
  CS_WRITE_U32(aBuf + CS_MANIFEST_INDEX_SIZE_OFF, (u32)cs->index.nIndexSize);

  CS_WRITE_I64(aBuf + CS_MANIFEST_WAL_OFFSET_OFF, cs->wal.iWalOffset);
  memcpy(aBuf + CS_MANIFEST_REFS_HASH_OFF, cs->refs.refsHash.data, PROLLY_HASH_SIZE);
}

void csManifestSeal(u8 *aBuf){
  blake3_hasher hasher;
  memset(aBuf + CS_MANIFEST_SELF_HASH_OFF, 0, PROLLY_HASH_SIZE);
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, aBuf, CHUNK_MANIFEST_SIZE);
  blake3_hasher_finalize(&hasher, aBuf + CS_MANIFEST_SELF_HASH_OFF,
                         PROLLY_HASH_SIZE);
}

int csManifestHashState(const u8 *aBuf){
  blake3_hasher hasher;
  u8 aCopy[CHUNK_MANIFEST_SIZE];
  u8 aHash[PROLLY_HASH_SIZE];
  int i;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    if( aBuf[CS_MANIFEST_SELF_HASH_OFF+i] ) break;
  }
  if( i==PROLLY_HASH_SIZE ) return CS_MANIFEST_HASH_LEGACY;
  memcpy(aCopy, aBuf, CHUNK_MANIFEST_SIZE);
  memset(aCopy + CS_MANIFEST_SELF_HASH_OFF, 0, PROLLY_HASH_SIZE);
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, aCopy, CHUNK_MANIFEST_SIZE);
  blake3_hasher_finalize(&hasher, aHash, PROLLY_HASH_SIZE);
  return memcmp(aHash, aBuf + CS_MANIFEST_SELF_HASH_OFF, PROLLY_HASH_SIZE)==0
       ? CS_MANIFEST_HASH_OK : CS_MANIFEST_HASH_BAD;
}

static int csReadManifest(ChunkStore *cs){
  u8 aBuf[CHUNK_MANIFEST_SIZE];
  u32 magic, version;
  int rc;

  rc = sqlite3OsRead(cs->file.pFile, aBuf, CHUNK_MANIFEST_SIZE, 0);
  if( rc != SQLITE_OK ) return rc;

  magic = CS_READ_U32(aBuf + CS_MANIFEST_MAGIC_OFF);
  version = CS_READ_U32(aBuf + CS_MANIFEST_VERSION_OFF);
  if( magic != CHUNK_STORE_MAGIC ) return SQLITE_NOTADB;
  if( version != CHUNK_STORE_VERSION ){
    sqlite3_log(SQLITE_NOTADB,
      "doltlite: chunk store format version %u, expected %u "
      "(database written by an incompatible doltlite version; "
      "this build refuses to open it to prevent corruption)",
      version, CHUNK_STORE_VERSION);
    return SQLITE_NOTADB;
  }

  /* Header seals are advisory; WAL root seals are load-bearing. */
  cs->index.nChunks = (int)CS_READ_U32(aBuf + CS_MANIFEST_CHUNK_COUNT_OFF);
  cs->index.iIndexOffset = CS_READ_I64(aBuf + CS_MANIFEST_INDEX_OFFSET_OFF);
  cs->index.nIndexSize = (i64)CS_READ_U32(aBuf + CS_MANIFEST_INDEX_SIZE_OFF);

  cs->wal.iWalOffset = CS_READ_I64(aBuf + CS_MANIFEST_WAL_OFFSET_OFF);
  memcpy(cs->refs.refsHash.data, aBuf + CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);

  return SQLITE_OK;
}

/* Classify unreadable headers as crashed creation or damaged committed DB. */
static int csScanForCommittedRoot(ChunkStore *cs, int *pEverCommitted,
                                  int *pCreationRoot){
  u8 buf[65536];
  i64 fileSize = 0;
  i64 q = CHUNK_MANIFEST_SIZE;
  int rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
  *pEverCommitted = 0;
  *pCreationRoot = 0;
  if( rc != SQLITE_OK ) return rc;
  while( q + 5 <= fileSize ){
    i64 nAvail = fileSize - q;
    int n = nAvail > (i64)sizeof(buf) ? (int)sizeof(buf) : (int)nAvail;
    int i;
    rc = sqlite3OsRead(cs->file.pFile, buf, n, q);
    if( rc != SQLITE_OK ) return rc;
    for(i=0; i+5<=n; i++){
      if( buf[i]==CS_WAL_TAG_ROOT && CS_READ_U32(buf+i+1)==CHUNK_STORE_MAGIC ){
        u8 m[CHUNK_MANIFEST_SIZE];
        int state;
        if( q + i + 1 + CHUNK_MANIFEST_SIZE > fileSize ) continue;
        rc = sqlite3OsRead(cs->file.pFile, m, CHUNK_MANIFEST_SIZE, q + i + 1);
        if( rc != SQLITE_OK ) return rc;
        state = csManifestHashState(m);
        if( state==CS_MANIFEST_HASH_LEGACY ){
          *pEverCommitted = 1;
          return SQLITE_OK;
        }
        if( state==CS_MANIFEST_HASH_OK ){
          if( CS_READ_I64(m + CS_MANIFEST_DURABLE_TO_OFF) > CHUNK_MANIFEST_SIZE ){
            *pEverCommitted = 1;
            return SQLITE_OK;
          }
          *pCreationRoot = 1;
        }
      }
    }
    if( (i64)n >= nAvail ) break;
    q += n - 4;
  }
  return SQLITE_OK;
}


int chunkStoreOpen(
  ChunkStore *cs,
  sqlite3_vfs *pVfs,
  const char *zFilename,
  int flags
){
  int rc;
  int exists = 0;

  memset(cs, 0, sizeof(*cs));
  if( !pVfs ){
    pVfs = sqlite3_vfs_find(0);
    if( !pVfs ) return SQLITE_CANTOPEN;
  }
  cs->file.pVfs = pVfs;
  CS_GRAPH_LOCK(cs) = CS_FILE_LOCK_INIT;
  cs->pGraphLockName = 0;
  cs->pLockMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
  if( cs->pLockMutex==0 && sqlite3GlobalConfig.bCoreMutex ){
    chunkStoreClose(cs);
    return SQLITE_NOMEM;
  }
  cs->lockDepth = 0;

  if( zFilename==0 || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0
   || (flags & SQLITE_OPEN_MEMORY)!=0 ){
    cs->isMemory = 1;
    /* Honor SQLITE_OPEN_READONLY for in-memory stores too; otherwise a
    ** read-only :memory: connection silently accepts writes. */
    cs->readOnly = (flags & SQLITE_OPEN_READONLY)!=0;
    cs->file.zFilename = sqlite3_mprintf(":memory:");
    if( cs->file.zFilename==0 ){
      chunkStoreClose(cs);
      return SQLITE_NOMEM;
    }
    cs->index.nChunks = 0;
    cs->index.iIndexOffset = 0;
    cs->index.nIndexSize = 0;
    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->file.pFile = 0;
    return SQLITE_OK;
  }

  /* File-backed stores need xDelete and HAS_MOVED file controls. */
  if( pVfs->xDelete==0 ){
    chunkStoreClose(cs);
    return SQLITE_CANTOPEN;
  }

  rc = csCanonicalFilename(pVfs, zFilename, &cs->file.zFilename);
  if( rc!=SQLITE_OK ){
    chunkStoreClose(cs);
    return rc;
  }

  rc = sqlite3OsAccess(pVfs, cs->file.zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc != SQLITE_OK ){
    chunkStoreClose(cs);
    return rc;
  }

  if( exists ){
    i64 mainSize = 0;
    rc = csFileSizeByName(pVfs, cs->file.zFilename, &mainSize);
    if( rc!=SQLITE_OK ){
      chunkStoreClose(cs);
      return rc;
    }
    if( mainSize==0 ){
      exists = 0;
    }
  }

  if( exists ){
    /* Honor SQLITE_OPEN_READONLY even when the file is writable. */
    int wantReadOnly = (flags & SQLITE_OPEN_READONLY)!=0;
    int openFlags = (wantReadOnly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE)
                  | SQLITE_OPEN_MAIN_DB;
    int outFlags = 0;
    rc = csOpenFile(pVfs, cs->file.zFilename, &cs->file.pFile, openFlags, &outFlags);
    if( rc != SQLITE_OK ){
      /* Do not downgrade OOM-failed writable opens to read-only. */
      if( wantReadOnly || rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
        chunkStoreClose(cs);
        return rc;
      }
      openFlags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB;
      rc = csOpenFile(pVfs, cs->file.zFilename, &cs->file.pFile, openFlags, 0);
      if( rc != SQLITE_OK ){
        chunkStoreClose(cs);
        return rc;
      }
      /* Probe fallback handles so directories fail at open time. */
      {
        u8 probe;
        if( sqlite3OsRead(cs->file.pFile, &probe, 1, 0)==SQLITE_IOERR_READ ){
          chunkStoreClose(cs);
          return SQLITE_CANTOPEN;
        }
      }
      cs->readOnly = 1;
    }else if( wantReadOnly || (outFlags & SQLITE_OPEN_READONLY) ){
      /* Either the caller asked for read-only, or a read-write open silently
      ** downgraded to read-only (VFS returned READONLY in the out-flags). Mark
      ** the store read-only so writes fail with SQLITE_READONLY. */
      cs->readOnly = 1;
    }

    rc = csReadManifest(cs);
    if( (rc==SQLITE_NOTADB || rc==SQLITE_CORRUPT)
     && (flags & SQLITE_OPEN_CREATE)!=0 && !cs->readOnly ){
      /* Recover only proven first-commit crashes to an empty database. */
      int everCommitted = 1;
      int creationRoot = 0;
      int rc2 = csScanForCommittedRoot(cs, &everCommitted, &creationRoot);
      if( rc2==SQLITE_OK && !everCommitted && !creationRoot ){
        u8 aHdr[CHUNK_MANIFEST_SIZE];
        rc2 = sqlite3OsRead(cs->file.pFile, aHdr, CHUNK_MANIFEST_SIZE, 0);
        if( rc2==SQLITE_OK ){
          int i;
          for(i=0; i<CHUNK_MANIFEST_SIZE && aHdr[i]==0; i++){}
          creationRoot = (i==CHUNK_MANIFEST_SIZE);
        }
      }
      if( rc2==SQLITE_OK && !everCommitted && creationRoot ){
        cs->index.nChunks = 0;
        cs->index.iIndexOffset = 0;
        cs->index.nIndexSize = 0;
        cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
        cs->file.iFileSize = 0;
        rc = sqlite3OsTruncate(cs->file.pFile, 0);
        if( rc==SQLITE_OK ){
          csMarkRefsCommitted(cs);
          return SQLITE_OK;
        }
      }
    }
    if( rc != SQLITE_OK ){
      chunkStoreClose(cs);
      return rc;
    }

    rc = csReadIndex(cs);
    if( rc != SQLITE_OK ){
      chunkStoreClose(cs);
      return rc;
    }

    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      chunkStoreClose(cs);
      return rc;
    }

    if( prollyHashIsEmpty(&cs->refs.refsHash) && cs->index.nIndexSize>0 ){
      chunkStoreClose(cs);
      return SQLITE_CORRUPT;
    }

    if( !prollyHashIsEmpty(&cs->refs.refsHash) ){
      u8 *refsData = 0; int nRefsData = 0;
      rc = chunkStoreGet(cs, &cs->refs.refsHash, &refsData, &nRefsData);
      if( rc==SQLITE_OK ){
        rc = csDeserializeRefs(cs, refsData, nRefsData);
        sqlite3_free(refsData);
      }
      if( rc!=SQLITE_OK ){
        chunkStoreClose(cs);
        return rc;
      }
    }
    rc = csEnsureDefaultBranch(cs);
    if( rc!=SQLITE_OK ){
      chunkStoreClose(cs);
      return rc;
    }
  }else{
    if( !(flags & SQLITE_OPEN_CREATE) ){
      chunkStoreClose(cs);
      return SQLITE_CANTOPEN;
    }
    cs->index.nChunks = 0;
    cs->index.iIndexOffset = 0;
    cs->index.nIndexSize = 0;

    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->file.iFileSize = 0;
    cs->file.pFile = 0;
  }

  /* Poison only after the open's own refs/branch reads are done. */
  if( cs->wal.recoveredMidStream ) cs->corruptMidStream = 1;

  csMarkRefsCommitted(cs);
  return SQLITE_OK;
}

static void csWriteCleanCloseMarker(ChunkStore *cs){
  u8 rootRec[1 + CHUNK_MANIFEST_SIZE];
  i64 markerStart;
  i64 markerEnd;
  i64 markerNext;
  int sectorSize = 1;
  int rc;
  CsFileLock lockFd = CS_FILE_LOCK_INIT;
  char *lockName = 0;
  int lockHeld;

  if( cs->isMemory || cs->readOnly || cs->corruptMidStream ){
    return;
  }
  if( !cs->file.pFile || !cs->file.zFilename || cs->wal.cleanCloseMarker ){
    return;
  }
  if( cs->wal.iWalOffset<=0 || cs->wal.nWalData<=0 ){
    return;
  }
  if( cs->staging.nPending>0 || cs->staging.nRecentUncommitted>0 ){
    return;
  }
  if( prollyHashCompare(&cs->refs.refsHash, &cs->refs.committedRefsHash)!=0 ){
    return;
  }

  lockHeld = csFileLockHeld(CS_GRAPH_LOCK(cs));
  if( !lockHeld ){
    rc = csFileLock(cs->file.pVfs, cs->file.zFilename, &lockFd, &lockName);
    if( rc!=SQLITE_OK ) return;
  }

  if( !lockHeld ){
    i64 fileSize = 0;
    rc = cs->file.pFile->pMethods->xFileSize(cs->file.pFile, &fileSize);
    if( rc!=SQLITE_OK ) goto done;
    if( fileSize!=cs->file.iFileSize ) goto done;
  }

  if( (cs->file.pFile->pMethods->xDeviceCharacteristics(cs->file.pFile)
       & (SQLITE_IOCAP_POWERSAFE_OVERWRITE|SQLITE_IOCAP_ATOMIC))==0 ){
    sectorSize = cs->file.pFile->pMethods->xSectorSize(cs->file.pFile);
    if( sectorSize < 512 ) sectorSize = 512;
    if( sectorSize > 65536 ) sectorSize = 65536;
  }

  markerStart = cs->file.iFileSize;
  if( markerStart <= 0 ) goto done;
  markerEnd = markerStart + (i64)sizeof(rootRec);
  markerNext = markerEnd;
  if( sectorSize > 1 ){
    markerNext = markerEnd + (sectorSize - 1);
    markerNext -= markerNext % sectorSize;
  }

  rootRec[0] = CS_WAL_TAG_ROOT;
  csSerializeManifest(cs, rootRec + 1);
  CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_DURABLE_TO_OFF, markerStart);
  CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_NEXT_OFF_OFF, markerNext);
  CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_BATCH_START_OFF, markerStart);
  csManifestSeal(rootRec + 1);

  rc = sqlite3OsWrite(cs->file.pFile, rootRec, sizeof(rootRec), markerStart);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsSync(cs->file.pFile, SQLITE_SYNC_NORMAL);
  }
  if( rc==SQLITE_OK ){
    cs->file.iFileSize = markerNext;
    cs->wal.nWalData = markerEnd - cs->wal.iWalOffset;
    cs->wal.cleanCloseMarker = 1;
  }else{
    sqlite3_log(SQLITE_NOTICE,
      "doltlite: unable to append clean-close WAL marker: %d", rc);
  }

done:
  if( !lockHeld ) csFileUnlock(lockFd, &lockName);
}

int chunkStoreClose(ChunkStore *cs){
  /* The marker is a pure optimization (skips recovery on the next open) and
  ** every failure inside it is already a silent no-op, so its allocations
  ** must not read as swallowed OOM under fault injection. */
  sqlite3BeginBenignMalloc();
  csWriteCleanCloseMarker(cs);
  sqlite3EndBenignMalloc();
  chunkStoreUnlock(cs);
  if( cs->file.pFile ){
    csCloseFile(cs->file.pFile);
    cs->file.pFile = 0;
  }
  sqlite3_free(cs->file.zFilename);
  csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase, cs->index.aIndexMmapSize);
  cs->index.aIndex = 0;
  cs->index.aIndexMmapBase = 0;
  cs->index.aIndexMmapSize = 0;
  sqlite3_free(cs->staging.aPending);
  sqlite3_free(cs->staging.aPendingZeroTail);
  sqlite3_free(cs->staging.aRecent);
  sqlite3_free(cs->staging.aRecentZeroTail);
  csPendHTClear(cs);
  csRecentHTClear(cs);
  sqlite3_free(cs->staging.pWriteBuf);
  sqlite3_free(cs->refs.zDefaultBranch);
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  csFreeSequences(cs);
  sqlite3_mutex_free(cs->pLockMutex);
  memset(cs, 0, sizeof(*cs));
  return SQLITE_OK;
}

const char *chunkStoreGetDefaultBranch(ChunkStore *cs){
  return cs->refs.zDefaultBranch ? cs->refs.zDefaultBranch : "main";
}

int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName){
  char *zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  sqlite3_free(cs->refs.zDefaultBranch);
  cs->refs.zDefaultBranch = zCopy;
  return SQLITE_OK;
}

static int findBranchIdx(ChunkStore *cs, const char *zName){
  return csFindNamedRef(cs->refs.aBranches, cs->refs.nBranches,
                        (int)sizeof(struct BranchRef), zName);
}

static int findTagIdx(ChunkStore *cs, const char *zName){
  return csFindNamedRef(cs->refs.aTags, cs->refs.nTags,
                        (int)sizeof(struct TagRef), zName);
}

static int findRemoteIdx(ChunkStore *cs, const char *zName){
  return csFindNamedRef(cs->refs.aRemotes, cs->refs.nRemotes,
                        (int)sizeof(struct RemoteRef), zName);
}

static int findTrackingIdx(ChunkStore *cs, const char *zRemote, const char *zBranch){
  int i;
  for(i=0; i<cs->refs.nTracking; i++){
    if( strcmp(cs->refs.aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->refs.aTracking[i].zBranch, zBranch)==0 ) return i;
  }
  return -1;
}

int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i = findBranchIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  if( pCommit ) memcpy(pCommit, &cs->refs.aBranches[i].commitHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

/* True when the on-disk store provably matches this handle's in-memory
** state: same inode (a peer gc replaces the file), physical size equal to
** the in-memory committed extent (the store is append-only, so any peer
** commit or crash garbage grows it), and a sealed tail root record carrying
** this handle's live refs hash (so uncommitted local ref changes, or any
** size-coincident rewrite, force the slow path). */
static int csDiskStateMatchesMemory(ChunkStore *cs){
  int bMoved = 0;
  i64 contentEnd;
  i64 physSize = 0;
  u8 aRoot[1 + CHUNK_MANIFEST_SIZE];
  if( cs->file.pFile==0 ) return 0;
  if( cs->wal.nWalData < 1 + CHUNK_MANIFEST_SIZE ) return 0;
  if( sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED,
                           &bMoved)!=SQLITE_OK || bMoved ){
    return 0;
  }
  contentEnd = cs->wal.iWalOffset + cs->wal.nWalData;
  if( sqlite3OsFileSize(cs->file.pFile, &physSize)!=SQLITE_OK
   || physSize!=contentEnd ){
    return 0;
  }
  if( sqlite3OsRead(cs->file.pFile, aRoot, (int)sizeof(aRoot),
                    contentEnd - (i64)sizeof(aRoot))!=SQLITE_OK ){
    return 0;
  }
  return aRoot[0]==CS_WAL_TAG_ROOT
      && csManifestHashState(aRoot+1)==CS_MANIFEST_HASH_OK
      && memcmp(aRoot + 1 + CS_MANIFEST_REFS_HASH_OFF,
                cs->refs.refsHash.data, PROLLY_HASH_SIZE)==0;
}

/* Read zName's committed tip straight from disk into *pTip, leaving this
** store's in-memory state untouched. Opens a throwaway view of the file (as
** csReloadFromDisk does) so a commit/merge head-CAS sees a peer's advance even
** when its force-refresh is suppressed (a reentrant lock holder, or a WAL-reuse
** commit the file-size heuristic misses). When the tail root record proves the
** disk still matches this handle's state, the in-memory tip IS the disk tip
** and the throwaway open is skipped. *pFound is 0 if the branch is absent.
** Caller must hold the chunk-store lock. */
int chunkStoreReadDiskBranchTip(ChunkStore *cs, const char *zName,
                                ProllyHash *pTip, int *pFound){
  ChunkStore tmp;
  int rc;

  *pFound = 0;
  if( cs->isMemory || !cs->file.zFilename ){
    if( chunkStoreFindBranch(cs, zName, pTip)==SQLITE_OK ) *pFound = 1;
    return SQLITE_OK;
  }

  if( csDiskStateMatchesMemory(cs) ){
    if( chunkStoreFindBranch(cs, zName, pTip)==SQLITE_OK ) *pFound = 1;
    return SQLITE_OK;
  }

  rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;
  if( chunkStoreFindBranch(&tmp, zName, pTip)==SQLITE_OK ) *pFound = 1;
  chunkStoreClose(&tmp);
  return SQLITE_OK;
}

int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int n = cs->refs.nBranches;
  if( chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  if( csRefArrayGrow((void**)&cs->refs.aBranches, n, (int)sizeof(struct BranchRef)) ) return SQLITE_NOMEM;
  cs->refs.aBranches[n].zName = sqlite3_mprintf("%s", zName);
  if( !cs->refs.aBranches[n].zName ) return SQLITE_NOMEM;
  memcpy(&cs->refs.aBranches[n].commitHash, pCommit, sizeof(ProllyHash));
  cs->refs.nBranches++;
  return SQLITE_OK;
}

int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int i = findBranchIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  memcpy(&cs->refs.aBranches[i].commitHash, pCommit, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName){
  int i = findBranchIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  sqlite3_free(cs->refs.aBranches[i].zName);
  cs->refs.aBranches[i] = cs->refs.aBranches[cs->refs.nBranches-1];
  cs->refs.nBranches--;
  return SQLITE_OK;
}

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash){
  int i = findBranchIdx(cs, zBranch);
  if( i<0 ){
    memset(pHash, 0, sizeof(ProllyHash));
    return SQLITE_NOTFOUND;
  }
  memcpy(pHash, &cs->refs.aBranches[i].workingSetHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash){
  int i = findBranchIdx(cs, zBranch);
  if( i<0 ) return SQLITE_NOTFOUND;
  memcpy(&cs->refs.aBranches[i].workingSetHash, pHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i = findTagIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  if( pCommit ) memcpy(pCommit, &cs->refs.aTags[i].commitHash, sizeof(ProllyHash));
  return SQLITE_OK;
}

int chunkStoreAddTag(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  return chunkStoreAddTagFull(cs, zName, pCommit, 0, 0, 0, 0);
}

int chunkStoreAddTagFull(
  ChunkStore *cs,
  const char *zName,
  const ProllyHash *pCommit,
  const char *zTagger,
  const char *zEmail,
  i64 timestamp,
  const char *zMessage
){
  struct TagRef *t;
  int n = cs->refs.nTags;
  if( chunkStoreFindTag(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  if( csRefArrayGrow((void**)&cs->refs.aTags, n, (int)sizeof(struct TagRef)) ) return SQLITE_NOMEM;
  t = &cs->refs.aTags[n];
  t->zName = sqlite3_mprintf("%s", zName);
  if( !t->zName ) return SQLITE_NOMEM;
  memcpy(&t->commitHash, pCommit, sizeof(ProllyHash));
  t->zTagger  = sqlite3_mprintf("%s", zTagger  ? zTagger  : "");
  t->zEmail   = sqlite3_mprintf("%s", zEmail   ? zEmail   : "");
  t->zMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
  if( !t->zTagger || !t->zEmail || !t->zMessage ){
    sqlite3_free(t->zName);
    sqlite3_free(t->zTagger);
    sqlite3_free(t->zEmail);
    sqlite3_free(t->zMessage);
    memset(t, 0, sizeof(struct TagRef));
    return SQLITE_NOMEM;
  }
  t->timestamp = timestamp;
  cs->refs.nTags++;
  return SQLITE_OK;
}

int chunkStoreDeleteTag(ChunkStore *cs, const char *zName){
  int i = findTagIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  sqlite3_free(cs->refs.aTags[i].zName);
  cs->refs.aTags[i] = cs->refs.aTags[cs->refs.nTags-1];
  cs->refs.nTags--;
  return SQLITE_OK;
}

int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl){
  int i = findRemoteIdx(cs, zName);
  if( i<0 ) return SQLITE_NOTFOUND;
  if( pzUrl ) *pzUrl = cs->refs.aRemotes[i].zUrl;
  return SQLITE_OK;
}

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl){
  int n = cs->refs.nRemotes;
  if( chunkStoreFindRemote(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  if( csRefArrayGrow((void**)&cs->refs.aRemotes, n, (int)sizeof(struct RemoteRef)) ) return SQLITE_NOMEM;
  cs->refs.aRemotes[n].zName = sqlite3_mprintf("%s", zName);
  if( !cs->refs.aRemotes[n].zName ) return SQLITE_NOMEM;
  cs->refs.aRemotes[n].zUrl = sqlite3_mprintf("%s", zUrl);
  if( !cs->refs.aRemotes[n].zUrl ){
    sqlite3_free(cs->refs.aRemotes[n].zName);
    return SQLITE_NOMEM;
  }
  cs->refs.nRemotes++;
  return SQLITE_OK;
}

int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName){
  int i = findRemoteIdx(cs, zName);
  int j;
  if( i<0 ) return SQLITE_NOTFOUND;
  sqlite3_free(cs->refs.aRemotes[i].zName);
  sqlite3_free(cs->refs.aRemotes[i].zUrl);
  cs->refs.aRemotes[i] = cs->refs.aRemotes[cs->refs.nRemotes-1];
  cs->refs.nRemotes--;

  for(j=cs->refs.nTracking-1; j>=0; j--){
    if( strcmp(cs->refs.aTracking[j].zRemote, zName)==0 ){
      sqlite3_free(cs->refs.aTracking[j].zRemote);
      sqlite3_free(cs->refs.aTracking[j].zBranch);
      cs->refs.aTracking[j] = cs->refs.aTracking[cs->refs.nTracking-1];
      cs->refs.nTracking--;
    }
  }
  return SQLITE_OK;
}

int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit){
  int i = findTrackingIdx(cs, zRemote, zBranch);
  if( i>=0 ){
    if( pCommit ) memcpy(pCommit, &cs->refs.aTracking[i].commitHash, sizeof(ProllyHash));
    return SQLITE_OK;
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit){
  int i = findTrackingIdx(cs, zRemote, zBranch);
  if( i>=0 ){
    memcpy(&cs->refs.aTracking[i].commitHash, pCommit, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  {
    int n = cs->refs.nTracking;
    if( csRefArrayGrow((void**)&cs->refs.aTracking, n, (int)sizeof(struct TrackingBranch)) ) return SQLITE_NOMEM;
    cs->refs.aTracking[n].zRemote = sqlite3_mprintf("%s", zRemote);
    if( !cs->refs.aTracking[n].zRemote ) return SQLITE_NOMEM;
    cs->refs.aTracking[n].zBranch = sqlite3_mprintf("%s", zBranch);
    if( !cs->refs.aTracking[n].zBranch ){
      sqlite3_free(cs->refs.aTracking[n].zRemote);
      return SQLITE_NOMEM;
    }
    memcpy(&cs->refs.aTracking[n].commitHash, pCommit, sizeof(ProllyHash));
    cs->refs.nTracking++;
  }
  return SQLITE_OK;
}

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult){
  int i;
  for(i=0; i<nHash; i++){
    int has = 0;
    int rc = chunkStoreHas(cs, &aHash[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    aResult[i] = has ? 1 : 0;
  }
  return SQLITE_OK;
}

static int csSerializeRefsBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  const char *def = cs->refs.zDefaultBranch ? cs->refs.zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  int sz = 1 + 4 + defLen + 4 + 4 + 4 + 4;
  int i;
  u8 *buf, *bufCur;

  *ppOut = 0;
  *pnOut = 0;

  for(i=0; i<cs->refs.nBranches; i++){
    int inc = 4 + (int)strlen(cs->refs.aBranches[i].zName) + PROLLY_HASH_SIZE*2;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->refs.nTags; i++){
    int taggerLen  = cs->refs.aTags[i].zTagger  ? (int)strlen(cs->refs.aTags[i].zTagger)  : 0;
    int emailLen   = cs->refs.aTags[i].zEmail   ? (int)strlen(cs->refs.aTags[i].zEmail)   : 0;
    int messageLen = cs->refs.aTags[i].zMessage ? (int)strlen(cs->refs.aTags[i].zMessage) : 0;
    int inc = 4 + (int)strlen(cs->refs.aTags[i].zName) + PROLLY_HASH_SIZE
            + 4 + taggerLen
            + 4 + emailLen
            + 8
            + 4 + messageLen;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->refs.nRemotes; i++){
    int inc = 4 + (int)strlen(cs->refs.aRemotes[i].zName) + 4 + (int)strlen(cs->refs.aRemotes[i].zUrl);
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->refs.nTracking; i++){
    int inc = 4 + (int)strlen(cs->refs.aTracking[i].zRemote) + 4 + (int)strlen(cs->refs.aTracking[i].zBranch) + PROLLY_HASH_SIZE;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  /* SequenceRef section: u32 count, then for each: u32 nameLen, name, i64 seq. */
  sz += 4;
  for(i=0; i<cs->refs.nSequences; i++){
    int nameLen = cs->refs.aSequences[i].zTableName
                ? (int)strlen(cs->refs.aSequences[i].zTableName) : 0;
    int inc = 4 + nameLen + 8;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  bufCur = buf;
  *bufCur++ = 7;
  CS_WRITE_U32(bufCur,defLen); bufCur+=4;
  memcpy(bufCur, def, defLen); bufCur+=defLen;
  CS_WRITE_U32(bufCur,cs->refs.nBranches); bufCur+=4;
  for(i=0; i<cs->refs.nBranches; i++){
    int nameLen = (int)strlen(cs->refs.aBranches[i].zName);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aBranches[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->refs.aBranches[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    memcpy(bufCur, cs->refs.aBranches[i].workingSetHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->refs.nTags); bufCur+=4;
  for(i=0; i<cs->refs.nTags; i++){
    int nameLen    = (int)strlen(cs->refs.aTags[i].zName);
    int taggerLen  = cs->refs.aTags[i].zTagger  ? (int)strlen(cs->refs.aTags[i].zTagger)  : 0;
    int emailLen   = cs->refs.aTags[i].zEmail   ? (int)strlen(cs->refs.aTags[i].zEmail)   : 0;
    int messageLen = cs->refs.aTags[i].zMessage ? (int)strlen(cs->refs.aTags[i].zMessage) : 0;
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aTags[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->refs.aTags[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    CS_WRITE_U32(bufCur,taggerLen); bufCur+=4;
    if( taggerLen ) memcpy(bufCur, cs->refs.aTags[i].zTagger, taggerLen);
    bufCur+=taggerLen;
    CS_WRITE_U32(bufCur,emailLen); bufCur+=4;
    if( emailLen ) memcpy(bufCur, cs->refs.aTags[i].zEmail, emailLen);
    bufCur+=emailLen;
    CS_WRITE_I64(bufCur,cs->refs.aTags[i].timestamp); bufCur+=8;
    CS_WRITE_U32(bufCur,messageLen); bufCur+=4;
    if( messageLen ) memcpy(bufCur, cs->refs.aTags[i].zMessage, messageLen);
    bufCur+=messageLen;
  }
  CS_WRITE_U32(bufCur,cs->refs.nRemotes); bufCur+=4;
  for(i=0; i<cs->refs.nRemotes; i++){
    int nameLen = (int)strlen(cs->refs.aRemotes[i].zName);
    int urlLen = (int)strlen(cs->refs.aRemotes[i].zUrl);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aRemotes[i].zName, nameLen); bufCur+=nameLen;
    CS_WRITE_U32(bufCur,urlLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aRemotes[i].zUrl, urlLen); bufCur+=urlLen;
  }
  CS_WRITE_U32(bufCur,cs->refs.nTracking); bufCur+=4;
  for(i=0; i<cs->refs.nTracking; i++){
    int remoteLen = (int)strlen(cs->refs.aTracking[i].zRemote);
    int branchLen = (int)strlen(cs->refs.aTracking[i].zBranch);
    CS_WRITE_U32(bufCur,remoteLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aTracking[i].zRemote, remoteLen); bufCur+=remoteLen;
    CS_WRITE_U32(bufCur,branchLen); bufCur+=4;
    memcpy(bufCur, cs->refs.aTracking[i].zBranch, branchLen); bufCur+=branchLen;
    memcpy(bufCur, cs->refs.aTracking[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->refs.nSequences); bufCur+=4;
  for(i=0; i<cs->refs.nSequences; i++){
    int nameLen = cs->refs.aSequences[i].zTableName
                ? (int)strlen(cs->refs.aSequences[i].zTableName) : 0;
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    if( nameLen ){
      memcpy(bufCur, cs->refs.aSequences[i].zTableName, nameLen);
      bufCur+=nameLen;
    }
    CS_WRITE_I64(bufCur, cs->refs.aSequences[i].iSeq); bufCur+=8;
  }
  *ppOut = buf;
  *pnOut = sz;
  return SQLITE_OK;
}

int chunkStoreSerializeRefs(ChunkStore *cs){
  int rc;
  u8 *buf = 0;
  int sz = 0;
  ProllyHash refsHash;

  if( cs->refs.nBranches==1
   && cs->refs.nTags==0
   && cs->refs.nRemotes==0
   && cs->refs.nTracking==0
   && cs->refs.nSequences==0
   && (!cs->refs.zDefaultBranch || strcmp(cs->refs.zDefaultBranch, "main")==0)
   && strcmp(cs->refs.aBranches[0].zName, "main")==0 ){
    u8 aBuf[77];
    u8 *p = aBuf;
    *p++ = 7;
    CS_WRITE_U32(p,4); p+=4;
    memcpy(p, "main", 4); p+=4;
    CS_WRITE_U32(p,1); p+=4;
    CS_WRITE_U32(p,4); p+=4;
    memcpy(p, "main", 4); p+=4;
    memcpy(p, cs->refs.aBranches[0].commitHash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    memcpy(p, cs->refs.aBranches[0].workingSetHash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    CS_WRITE_U32(p,0); p+=4;     /* nTags */
    CS_WRITE_U32(p,0); p+=4;     /* nRemotes */
    CS_WRITE_U32(p,0); p+=4;     /* nTracking */
    CS_WRITE_U32(p,0); p+=4;     /* nSequences */
    assert( p==aBuf+sizeof(aBuf) );
    rc = chunkStorePut(cs, aBuf, (int)sizeof(aBuf), &refsHash);
    if( rc==SQLITE_OK ) memcpy(&cs->refs.refsHash, &refsHash, sizeof(ProllyHash));
    return rc;
  }

  rc = csSerializeRefsBlob(cs, &buf, &sz);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, buf, sz, &refsHash);
  sqlite3_free(buf);
  if( rc==SQLITE_OK ) memcpy(&cs->refs.refsHash, &refsHash, sizeof(ProllyHash));
  return rc;
}


int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData){
  return csReplaceRefsStateFromBlob(cs, data, nData, 1);
}

int chunkStoreInstallRefsBlob(ChunkStore *cs, const u8 *data, int nData){
  ChunkStore refsView;
  ProllyHash refsHash;
  int rc;

  if( !data || nData<=0 ) return SQLITE_ERROR;
  rc = csDeserializeRefsIntoTemp(&refsView, data, nData);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&refsView);
    return rc;
  }

  rc = chunkStorePut(cs, data, nData, &refsHash);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&refsView);
    return rc;
  }

  csFreeRefsState(cs);
  csAdoptRefsState(cs, &refsView);
  refsTableSetHash(&cs->refs, &refsHash);
  cs->bRefsStale = 0;
  return SQLITE_OK;
}

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut){
  return csSerializeRefsBlob(cs, ppOut, pnOut);
}

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash, int *pHas){
  int idx = -1;
  int rc;
  *pHas = 0;
  if( csSearchIndex(cs->index.aIndex, cs->index.nIndex, hash) >= 0 ){
    *pHas = 1;
    return SQLITE_OK;
  }
  rc = csSearchRecent(cs, hash, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx >= 0 ){
    *pHas = 1;
    return SQLITE_OK;
  }
  rc = csSearchPending(cs, hash, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx >= 0 ) *pHas = 1;
  return SQLITE_OK;
}

int chunkStoreGet(
  ChunkStore *cs,
  const ProllyHash *hash,
  u8 **ppData,
  int *pnData
){
  int idx;
  int rc;

  *ppData = 0;
  *pnData = 0;

  if( cs->corruptMidStream ) return SQLITE_CORRUPT;

  rc = csSearchPending(cs, hash, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx >= 0 ){
    ChunkIndexEntry *e = &cs->staging.aPending[idx];
    i64 off = e->offset;
    int sz = e->size;
    i64 nZ = cs->staging.aPendingZeroTail[idx];
    u8 *pCopy = (u8 *)sqlite3_malloc(sz);
    if( pCopy == 0 ) return SQLITE_NOMEM;

    memcpy(pCopy, cs->staging.pWriteBuf + off + 4, (size_t)(sz - nZ));
    if( nZ>0 ) memset(pCopy + (sz - nZ), 0, (size_t)nZ);
    *ppData = pCopy;
    *pnData = sz;
    return SQLITE_OK;
  }

  {
    ChunkIndexEntry *e;
    rc = csSearchRecent(cs, hash, &idx);
    if( rc!=SQLITE_OK ) return rc;
    if( idx >= 0 ){
      e = &cs->staging.aRecent[idx];
    }else{
      idx = csSearchIndex(cs->index.aIndex, cs->index.nIndex, hash);
      if( idx < 0 ){
        return SQLITE_NOTFOUND;
      }
      e = &cs->index.aIndex[idx];
    }

    if( cs->file.pFile == 0 ){
      if( cs->staging.pWriteBuf && e->offset >= 0
       && (e->offset + 4 + e->size) <= cs->staging.nWriteBuf ){
        u8 *pCopy = (u8 *)sqlite3_malloc(e->size);
        if( pCopy == 0 ) return SQLITE_NOMEM;
        memcpy(pCopy, cs->staging.pWriteBuf + e->offset + 4, e->size);
        *ppData = pCopy;
        *pnData = e->size;
        return SQLITE_OK;
      }
      return SQLITE_CORRUPT;
    }

    {
      i64 fileOff = e->offset;
      int sz = e->size;
      u8 *pBuf;
      u32 storedLen;

      if( sz > INT_MAX - 4 ) return SQLITE_TOOBIG;
      pBuf = (u8 *)sqlite3_malloc(sz + 4);
      if( pBuf == 0 ) return SQLITE_NOMEM;

      rc = sqlite3OsRead(cs->file.pFile, pBuf, sz + 4, fileOff);
      if( rc != SQLITE_OK ){
        sqlite3_free(pBuf);
        return rc;
      }

      storedLen = CS_READ_U32(pBuf);
      if( (int)storedLen != sz ){
        sqlite3_free(pBuf);
        return SQLITE_CORRUPT;
      }

      memmove(pBuf, pBuf + 4, sz);
      *ppData = pBuf;
      *pnData = sz;
    }
  }

  {
    ProllyHash h;
    prollyHashCompute(*ppData, *pnData, &h);
    if( memcmp(&h, hash, sizeof(ProllyHash)) != 0 ){
      sqlite3_free(*ppData);
      *ppData = 0;
      *pnData = 0;
      return SQLITE_CORRUPT;
    }
  }
  return SQLITE_OK;
}

int chunkStoreGetSparse(
  ChunkStore *cs,
  const ProllyHash *hash,
  u8 **ppData,
  int *pnData,
  int *pnDataPhys
){
  int idx;
  int rc;

  *ppData = 0;
  *pnData = 0;
  *pnDataPhys = 0;

  if( cs->corruptMidStream ) return SQLITE_CORRUPT;

  rc = csSearchPending(cs, hash, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx>=0 && cs->staging.aPendingZeroTail
   && cs->staging.aPendingZeroTail[idx]>0 ){
    ChunkIndexEntry *e = &cs->staging.aPending[idx];
    i64 zeroTail = cs->staging.aPendingZeroTail[idx];
    int nPhys;
    u8 *pBuf;

    if( zeroTail<0 || zeroTail>(i64)e->size ) return SQLITE_CORRUPT;
    nPhys = e->size - (int)zeroTail;
    pBuf = (u8*)sqlite3_malloc(nPhys>0 ? nPhys : 1);
    if( !pBuf ) return SQLITE_NOMEM;
    if( nPhys>0 ){
      memcpy(pBuf, cs->staging.pWriteBuf + e->offset + 4, nPhys);
    }
    *ppData = pBuf;
    *pnData = e->size;
    *pnDataPhys = nPhys;
    return SQLITE_OK;
  }

  rc = csSearchRecent(cs, hash, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx>=0 && cs->staging.aRecentZeroTail
   && cs->staging.aRecentZeroTail[idx]>0 ){
    ChunkIndexEntry *e = &cs->staging.aRecent[idx];
    i64 zeroTail = cs->staging.aRecentZeroTail[idx];
    int nPhys;
    u8 *pBuf;
    u32 storedLen;

    if( zeroTail<0 || zeroTail>(i64)e->size ) return SQLITE_CORRUPT;
    nPhys = e->size - (int)zeroTail;
    pBuf = (u8*)sqlite3_malloc(nPhys + 4);
    if( !pBuf ) return SQLITE_NOMEM;
    rc = sqlite3OsRead(cs->file.pFile, pBuf, nPhys + 4, e->offset);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pBuf);
      return rc;
    }
    storedLen = CS_READ_U32(pBuf);
    if( (int)storedLen != e->size ){
      sqlite3_free(pBuf);
      return SQLITE_CORRUPT;
    }
    memmove(pBuf, pBuf + 4, nPhys);
    {
      ProllyHash h;
      prollyHashComputeZeroTail(pBuf, nPhys, zeroTail, &h);
      if( memcmp(&h, hash, sizeof(ProllyHash))!=0 ){
        sqlite3_free(pBuf);
        return SQLITE_CORRUPT;
      }
    }
    *ppData = pBuf;
    *pnData = e->size;
    *pnDataPhys = nPhys;
    return SQLITE_OK;
  }

  rc = chunkStoreGet(cs, hash, ppData, pnData);
  if( rc==SQLITE_OK ) *pnDataPhys = *pnData;
  return rc;
}

static void csFillChunkHdr(u8 *p, const ProllyHash *pHash, u32 size){
  p[0] = CS_WAL_TAG_CHUNK;
  memcpy(p + CS_WAL_CHUNK_HASH_OFF, pHash, PROLLY_HASH_SIZE);
  CS_WRITE_U32(p + CS_WAL_CHUNK_LEN_OFF, size);
}

static int csDrainPendingToWal(ChunkStore *cs){
  int rc;
  int i;
  i64 writeOff;

  if( cs->isMemory || cs->readOnly || cs->corruptMidStream ){
    return SQLITE_OK;
  }
  if( cs->staging.nPending==0 ){
    return SQLITE_OK;
  }
  if( !csFileLockHeld(CS_GRAPH_LOCK(cs)) ){
    return SQLITE_OK;
  }

  if( cs->file.pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->file.pVfs, cs->file.zFilename, &cs->file.pFile,
                    openFlags, 0);
    if( rc != SQLITE_OK ){
      return (rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM) ? rc : SQLITE_CANTOPEN;
    }
  }

  if( cs->staging.nRecentUncommitted==0 ){
    cs->staging.iUncommittedStart = cs->file.iFileSize;
  }

  writeOff = cs->file.iFileSize;
  if( writeOff == 0 ){
    u8 manifest[CHUNK_MANIFEST_SIZE];
    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    csSerializeManifest(cs, manifest);
    csManifestSeal(manifest);
    rc = sqlite3OsWrite(cs->file.pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc!=SQLITE_OK ) return rc;
    writeOff = CHUNK_MANIFEST_SIZE;
  }

  rc = csGrowRecent(cs, cs->staging.nPending);
  if( rc!=SQLITE_OK ) return rc;

  for( i=0; i<cs->staging.nPending; i++ ){
    ChunkIndexEntry *pe = &cs->staging.aPending[i];
    ChunkIndexEntry *pr = &cs->staging.aRecent[cs->staging.nRecent+i];
    u8 recHdr[CS_WAL_CHUNK_HDR_SIZE];
    const u8 *pSrc = cs->staging.pWriteBuf + pe->offset + 4;
    i64 zeroTail = cs->staging.aPendingZeroTail[i];
    int remaining = (int)(pe->size - zeroTail);

    if( writeOff > LARGEST_INT64 - (i64)CS_WAL_CHUNK_HDR_SIZE
     || writeOff + (i64)CS_WAL_CHUNK_HDR_SIZE > LARGEST_INT64 - (i64)pe->size ){
      return SQLITE_TOOBIG;
    }

    *pr = *pe;
    pr->offset = writeOff + CS_WAL_CHUNK_LEN_OFF;
    cs->staging.aRecentZeroTail[cs->staging.nRecent+i] =
      cs->staging.aPendingZeroTail[i];

    csFillChunkHdr(recHdr, &pe->hash, (u32)pe->size);

    rc = sqlite3OsWrite(cs->file.pFile, recHdr, CS_WAL_CHUNK_HDR_SIZE, writeOff);
    if( rc!=SQLITE_OK ) return rc;
    writeOff += CS_WAL_CHUNK_HDR_SIZE;

    while( remaining > 0 ){
      int toWrite = remaining > 65536 ? 65536 : remaining;
      rc = sqlite3OsWrite(cs->file.pFile, pSrc, toWrite, writeOff);
      if( rc!=SQLITE_OK ) return rc;
      pSrc += toWrite;
      writeOff += toWrite;
      remaining -= toWrite;
    }
    while( zeroTail > 0 ){
      static const u8 aZeroWin[65536];
      int toWrite = zeroTail > (i64)sizeof(aZeroWin)
                  ? (int)sizeof(aZeroWin) : (int)zeroTail;
      rc = sqlite3OsWrite(cs->file.pFile, aZeroWin, toWrite, writeOff);
      if( rc!=SQLITE_OK ) return rc;
      writeOff += toWrite;
      zeroTail -= toWrite;
    }
  }

  cs->staging.nRecent += cs->staging.nPending;
  cs->staging.nRecentUncommitted += cs->staging.nPending;
  cs->file.iFileSize = writeOff;
  if( writeOff > cs->wal.iWalOffset ){
    cs->wal.nWalData = writeOff - cs->wal.iWalOffset;
  }

  cs->staging.nPending = 0;
  csPendHTReset(cs);
  cs->staging.nWriteBuf = 0;
  if( cs->staging.nWriteBufAlloc > CS_WRITEBUF_RETAIN_MAX ){
    sqlite3_free(cs->staging.pWriteBuf);
    cs->staging.pWriteBuf = 0;
    cs->staging.nWriteBufAlloc = 0;
  }
  return SQLITE_OK;
}

int chunkStorePut(
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  ProllyHash *pHash
){
  int rc;
  ProllyHash h;

  prollyHashCompute(pData, nData, &h);
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  {
    int bHas = 0;
    int hasRc = chunkStoreHas(cs, &h, &bHas);
    if( hasRc==SQLITE_OK && bHas ){
      return SQLITE_OK;
    }
    if( hasRc!=SQLITE_OK ) return hasRc;
  }

  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;

  rc = csGrowWriteBuf(cs, 4 + nData);
  if( rc != SQLITE_OK ) return rc;

  {
    ChunkIndexEntry *e = &cs->staging.aPending[cs->staging.nPending];
    e->hash = h;
    e->offset = (i64)cs->staging.nWriteBuf;
    e->size = nData;
    cs->staging.aPendingZeroTail[cs->staging.nPending] = 0;
    cs->staging.nPending++;
  }

  CS_WRITE_U32(cs->staging.pWriteBuf + cs->staging.nWriteBuf, (u32)nData);
  cs->staging.nWriteBuf += 4;
  memcpy(cs->staging.pWriteBuf + cs->staging.nWriteBuf, pData, nData);
  cs->staging.nWriteBuf += nData;

  if( cs->staging.nWriteBuf >= csPendingDrainLimit() ){
    rc = csDrainPendingToWal(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
}

/* Stage pPrefix plus a symbolic zero tail without materializing the zeros. */
int chunkStorePutSparse(
  ChunkStore *cs,
  const u8 *pPrefix,
  int nPrefix,
  i64 nZeroTail,
  ProllyHash *pHash
){
  int rc;
  ProllyHash h;
  i64 nTotal64 = (i64)nPrefix + nZeroTail;
  int nData;

  if( nPrefix<0 || nZeroTail<0 || nTotal64 > (i64)0x7fffffff ){
    return SQLITE_TOOBIG;
  }
  nData = (int)nTotal64;
  if( nZeroTail==0 ){
    return chunkStorePut(cs, pPrefix, nData, pHash);
  }
  if( cs->isMemory ){
    u8 *pFlat = (u8*)sqlite3_malloc(nData>0 ? nData : 1);
    if( !pFlat ) return SQLITE_NOMEM;
    if( nPrefix>0 ) memcpy(pFlat, pPrefix, nPrefix);
    memset(pFlat + nPrefix, 0, (size_t)nZeroTail);
    rc = chunkStorePut(cs, pFlat, nData, pHash);
    sqlite3_free(pFlat);
    return rc;
  }

  prollyHashComputeZeroTail(pPrefix, nPrefix, nZeroTail, &h);
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  {
    int bHas = 0;
    int hasRc = chunkStoreHas(cs, &h, &bHas);
    if( hasRc==SQLITE_OK && bHas ){
      return SQLITE_OK;
    }
    if( hasRc!=SQLITE_OK ) return hasRc;
  }

  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;
  rc = csGrowWriteBuf(cs, 4 + nPrefix);
  if( rc != SQLITE_OK ) return rc;

  {
    ChunkIndexEntry *e = &cs->staging.aPending[cs->staging.nPending];
    e->hash = h;
    e->offset = (i64)cs->staging.nWriteBuf;
    e->size = nData;
    cs->staging.aPendingZeroTail[cs->staging.nPending] = nZeroTail;
    cs->staging.nPending++;
  }

  CS_WRITE_U32(cs->staging.pWriteBuf + cs->staging.nWriteBuf, (u32)nData);
  cs->staging.nWriteBuf += 4;
  if( nPrefix>0 ){
    memcpy(cs->staging.pWriteBuf + cs->staging.nWriteBuf, pPrefix, nPrefix);
    cs->staging.nWriteBuf += nPrefix;
  }

  if( cs->staging.nWriteBuf >= csPendingDrainLimit() ){
    rc = csDrainPendingToWal(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  return SQLITE_OK;
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

static int csCommitToFile(ChunkStore *cs){
  int rc;
  int i;
  i64 fileSize = 0;
  i64 physFileSize = -1;
  i64 origFileSize = 0;
  i64 writeOff = 0;
  i64 durableTo = 0;
  i64 batchStart = 0;
  i64 contentEnd = 0;
  int sectorSize = 1;
  CsFileLock lockFd = CS_FILE_LOCK_INIT;
  char *lockName = 0;
  int hadFile = (cs->file.pFile != 0);
  int lockHeld = csFileLockHeld(CS_GRAPH_LOCK(cs));
  ChunkIndexEntry *aCommittedPending = 0;
  ChunkIndexEntry aSmallCommittedPending[32];
  ChunkIndexEntry *aMergePending = 0;
  ChunkIndexEntry *aMerged = 0;
  int nMerged = 0;
  int useRecent = 0;
  int crashWriteActive = csCrashWriteInjectionActive();

  if( cs->file.pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->file.pVfs, cs->file.zFilename, &cs->file.pFile, openFlags, 0);
    if( rc != SQLITE_OK ){
      return (rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM) ? rc : SQLITE_CANTOPEN;
    }
  }

  if( lockHeld ){
    lockFd = CS_FILE_LOCK_INIT;
  }else{
    rc = csFileLock(cs->file.pVfs, cs->file.zFilename, &lockFd, &lockName);
    if( rc!=SQLITE_OK ) return rc;
  }

  if( lockHeld && hadFile ){
    fileSize = cs->file.iFileSize;
  }else{
    rc = cs->file.pFile->pMethods->xFileSize(cs->file.pFile, &fileSize);
    if( rc != SQLITE_OK ) goto commit_done;
    physFileSize = fileSize;
  }

  if( hadFile && !lockHeld ){
    int bMoved = 0;
    int rc2 = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED,
                                   &bMoved);
    if( rc2==SQLITE_OK && bMoved ){
      rc = csReloadFromDiskPreservingLocalRefs(cs);
      if( rc != SQLITE_OK ) goto commit_done;
      fileSize = cs->file.iFileSize;
    }
  }

  if( fileSize > cs->file.iFileSize && hadFile ){
    rc = csReloadFromDiskPreservingLocalRefs(cs);
    if( rc != SQLITE_OK ) goto commit_done;
    fileSize = cs->file.iFileSize;
  }
#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
  if( hadFile && cs->staging.nRecentUncommitted>0 && csReloadInjectionActive() ){
    static int csReloadInjected = 0;
    if( !csReloadInjected ){
      csReloadInjected = 1;
      rc = csReloadFromDiskPreservingLocalRefs(cs);
      if( rc != SQLITE_OK ) goto commit_done;
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
  origFileSize = fileSize;

  /* On non-atomic media, isolate commit batches on fresh sectors. */
  if( (cs->file.pFile->pMethods->xDeviceCharacteristics(cs->file.pFile)
       & (SQLITE_IOCAP_POWERSAFE_OVERWRITE|SQLITE_IOCAP_ATOMIC))==0 ){
    sectorSize = cs->file.pFile->pMethods->xSectorSize(cs->file.pFile);
    if( sectorSize < 512 ) sectorSize = 512;
    if( sectorSize > 65536 ) sectorSize = 65536;
  }
  durableTo = fileSize > 0 ? fileSize : (i64)CHUNK_MANIFEST_SIZE;
  batchStart = durableTo;
  if( fileSize > 0 && sectorSize > 1 && (batchStart % sectorSize)!=0 ){
    batchStart += sectorSize - 1;
    batchStart -= batchStart % sectorSize;
  }

  if( cs->staging.nPending > 0 ){
    ChunkStore mergeView;
    i64 filePos = batchStart;
    i64 appendBytes = 0;

    for( i = 0; i < cs->staging.nPending; i++ ){
      i64 recBytes = (i64)CS_WAL_CHUNK_HDR_SIZE
                   + (i64)cs->staging.aPending[i].size;
      if( appendBytes > LARGEST_INT64 - recBytes ){
        rc = SQLITE_TOOBIG;
        goto commit_done;
      }
      appendBytes += recBytes;
    }
    if( cs->wal.nWalData > LARGEST_INT64 - appendBytes ){
      rc = SQLITE_TOOBIG;
      goto commit_done;
    }

    if( cs->staging.nPending <= (int)(sizeof(aSmallCommittedPending)
                                    / sizeof(aSmallCommittedPending[0])) ){
      aCommittedPending = aSmallCommittedPending;
    }else{
      aCommittedPending = (ChunkIndexEntry*)sqlite3_malloc(
        cs->staging.nPending * (int)sizeof(ChunkIndexEntry)
      );
      if( !aCommittedPending ){
        rc = SQLITE_NOMEM;
        goto commit_done;
      }
    }

    for( i = 0; i < cs->staging.nPending; i++ ){
      ChunkIndexEntry *pSrc = &cs->staging.aPending[i];
      aCommittedPending[i] = *pSrc;
      aCommittedPending[i].offset = filePos + CS_WAL_CHUNK_LEN_OFF;
      filePos += (i64)CS_WAL_CHUNK_HDR_SIZE + (i64)pSrc->size;
    }

    useRecent = !crashWriteActive
             && cs->staging.nPending <= 32
             && cs->staging.nRecent + cs->staging.nPending
                  <= CS_RECENT_FAST_PATH_MAX;
    if( useRecent ){
      rc = csGrowRecent(cs, cs->staging.nPending);
      if( rc!=SQLITE_OK ) goto commit_done;
    }else{
      if( cs->staging.nRecent > 0 ){
        aMergePending = (ChunkIndexEntry*)sqlite3_malloc(
          (cs->staging.nRecent + cs->staging.nPending) * (int)sizeof(ChunkIndexEntry)
        );
        if( !aMergePending ){
          rc = SQLITE_NOMEM;
          goto commit_done;
        }
        memcpy(aMergePending, cs->staging.aRecent,
               cs->staging.nRecent * sizeof(ChunkIndexEntry));
        memcpy(aMergePending + cs->staging.nRecent, aCommittedPending,
               cs->staging.nPending * sizeof(ChunkIndexEntry));
        mergeView = *cs;
        mergeView.staging.aPending = aMergePending;
        mergeView.staging.nPending = cs->staging.nRecent + cs->staging.nPending;
      }else{
        mergeView = *cs;
        mergeView.staging.aPending = aCommittedPending;
        mergeView.staging.nPending = cs->staging.nPending;
      }
      rc = csMergeIndex(&mergeView, &aMerged, &nMerged);
      if( rc!=SQLITE_OK ) goto commit_done;
    }
  }

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
    csManifestSeal(manifest);
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->file.pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc != SQLITE_OK ) goto commit_done;
  }

  writeOff = batchStart;

  /* Append chunks before the root record. Recovery ignores appended data until
  ** it finds a later valid root record that points at the new manifest. */
  if( cs->staging.nPending > 0 ){
    i64 walBytes = 0;
    u8 *pWalBatch = 0;
    u8 aSmallWalBatch[4096];
    u8 *pOut = 0;
    int hasSparse = 0;
    for( i = 0; i < cs->staging.nPending; i++ ){
      walBytes += (i64)CS_WAL_CHUNK_HDR_SIZE
                + (i64)cs->staging.aPending[i].size;
      if( cs->staging.aPendingZeroTail[i] ) hasSparse = 1;
    }
    if( !crashWriteActive && !hasSparse && walBytes <= 64*1024 ){
      if( walBytes <= (i64)sizeof(aSmallWalBatch) ){
        pWalBatch = aSmallWalBatch;
      }else{
        pWalBatch = (u8*)sqlite3_malloc64((sqlite3_uint64)walBytes);
        if( !pWalBatch ){
          rc = SQLITE_NOMEM;
          goto commit_done;
        }
      }
      pOut = pWalBatch;
      for( i = 0; i < cs->staging.nPending; i++ ){
        ChunkIndexEntry *pe = &cs->staging.aPending[i];
        i64 bufOff = pe->offset + 4;
        csFillChunkHdr(pOut, &pe->hash, (u32)pe->size);
        pOut += CS_WAL_CHUNK_HDR_SIZE;
        memcpy(pOut, cs->staging.pWriteBuf + bufOff, pe->size);
        pOut += pe->size;
      }
      CRASH_CHECK_WRITE();
      rc = sqlite3OsWrite(cs->file.pFile, pWalBatch, (int)walBytes, writeOff);
      if( pWalBatch != aSmallWalBatch ) sqlite3_free(pWalBatch);
      if( rc != SQLITE_OK ) goto commit_done;
      writeOff += walBytes;
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
    CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_DURABLE_TO_OFF, durableTo);
    CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_NEXT_OFF_OFF, nextOff);
    CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_BATCH_START_OFF, batchStart);
    csManifestSeal(rootRec + 1);
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->file.pFile, rootRec, sizeof(rootRec), writeOff);
    if( rc != SQLITE_OK ) goto commit_done;
    contentEnd = rootEnd;
    writeOff = nextOff;
  }

  CRASH_CHECK_WRITE();
  rc = sqlite3OsSync(cs->file.pFile, SQLITE_SYNC_NORMAL);

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
  if( aCommittedPending!=aSmallCommittedPending ){
    sqlite3_free(aCommittedPending);
  }
  sqlite3_free(aMergePending);

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

  return SQLITE_OK;
}

int chunkStoreCommit(ChunkStore *cs){
  int rc;
  int acquiredLock = 0;
  int preserveRefs = 0;
  ProllyHash savedRefsHash;
  SavedRefsState savedRefs;

  memset(&savedRefs, 0, sizeof(savedRefs));
  if( cs->corruptMidStream ) return SQLITE_CORRUPT;
  if( cs->readOnly ) return SQLITE_READONLY;
  if( cs->isMemory ) return csCommitToMemory(cs);
  if( !csFileLockHeld(CS_GRAPH_LOCK(cs)) && cs->file.zFilename ){
    preserveRefs = cs->staging.nPending > 0
                && prollyHashCompare(&cs->refs.refsHash,
                                     &cs->refs.committedRefsHash)!=0;
    if( preserveRefs ){
      savedRefsHash = cs->refs.refsHash;
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
      csFreeRefsState(cs);
      csRestoreSavedRefsState(cs, &savedRefs);
      cs->refs.refsHash = savedRefsHash;
    }
    acquiredLock = 1;
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

int chunkStoreIsEmpty(ChunkStore *cs){
  return cs->refs.nBranches == 0 && prollyHashIsEmpty(&cs->refs.refsHash);
}

void chunkStoreClearRefs(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  csFreeSequences(cs);
  memset(&cs->refs.refsHash, 0, sizeof(cs->refs.refsHash));
}

int chunkStoreReloadRefs(ChunkStore *cs){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;

  if( prollyHashIsEmpty(&cs->refs.refsHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &cs->refs.refsHash, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = csReplaceRefsStateFromBlob(cs, refsData, nRefsData, 0);
  sqlite3_free(refsData);
  return rc;
}

const char *chunkStoreFilename(ChunkStore *cs){
  return cs->file.zFilename;
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

static int csReloadFromDisk(ChunkStore *cs){
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
