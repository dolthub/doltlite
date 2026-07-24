

#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "../ext/blake3/blake3.h"
#ifdef SQLITE_CRASH_TEST
#include <unistd.h>
#endif


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

int csFileSizeByName(sqlite3_vfs *pVfs, const char *zPath, i64 *pSize){
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
#if SQLITE_OS_WIN
  if( rc==SQLITE_OK || rc==SQLITE_OK_SYMLINK ){
    if( !(zFull[0]=='\\' && zFull[1]=='\\'
       && (zFull[2]=='?' || zFull[2]=='.') && zFull[3]=='\\') ){
      char *z = zFull;
      while( *z ){
        if( *z=='\\' ) *z = '/';
        z++;
      }
    }
  }
#endif
  if( rc==SQLITE_OK || rc==SQLITE_OK_SYMLINK ){
    rc = chunkStoreDupFilenameDoubleNul(zFull, pzOut);
    sqlite3_free(zFull);
    return rc;
  }
  sqlite3_free(zFull);
  return rc;
}

static int csSyncFile(ChunkStore *cs){
  if( cs->noSync ) return SQLITE_OK;
  return sqlite3OsSync(cs->file.pFile,
                       cs->fullFsync ? SQLITE_SYNC_FULL : SQLITE_SYNC_NORMAL);
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

/* Repair a stale refs table (cleared by an OOM rollback) by reloading it
** from the committed refs blob. Called before refs lookups that would
** otherwise read an empty table as a valid empty database. */
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + CS_MANIFEST_MAGIC_OFF, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + CS_MANIFEST_VERSION_OFF, CHUNK_STORE_VERSION);

  /* Chunk count and index size are stored as u32, so the on-disk format caps
  ** the store at ~4 billion chunks and a 4 GiB index. Truncating either here
  ** would silently corrupt the manifest, so assert the values fit. */
  assert( cs->index.nChunks>=0 && (u64)cs->index.nChunks<=0xffffffffu );
  assert( cs->index.nIndexSize>=0 && (u64)cs->index.nIndexSize<=0xffffffffu );
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
    rc = csSyncFile(cs);
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
#ifdef SQLITE_DEBUG
    {
      ProllyHash h;
      prollyHashCompute(pCopy, sz, &h);
      if( memcmp(&h, hash, sizeof(ProllyHash))!=0 ){
        sqlite3_free(pCopy);
        return SQLITE_CORRUPT;
      }
    }
#endif
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
#ifdef SQLITE_DEBUG
        {
          ProllyHash h;
          prollyHashCompute(pCopy, e->size, &h);
          if( memcmp(&h, hash, sizeof(ProllyHash))!=0 ){
            sqlite3_free(pCopy);
            return SQLITE_CORRUPT;
          }
        }
#endif
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
#ifdef SQLITE_DEBUG
    {
      ProllyHash h;
      prollyHashComputeZeroTail(pBuf, nPhys, zeroTail, &h);
      if( memcmp(&h, hash, sizeof(ProllyHash))!=0 ){
        sqlite3_free(pBuf);
        return SQLITE_CORRUPT;
      }
    }
#endif
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

#endif
