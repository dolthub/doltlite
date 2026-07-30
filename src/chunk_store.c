

#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "../ext/blake3/blake3.h"
#ifdef SQLITE_CRASH_TEST
#endif


#if defined(SQLITE_TEST) || defined(DOLTLITE_MECH_REPRO)
/* Test hook: force one mid-commit reload after pending chunks drain. */
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


int csOpenFile(
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
  char **pzOut,
  int flags
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
  /* Match stock pagerOpen: FullPathname returns OK_SYMLINK when the path
  ** traverses a symlink; SQLITE_OPEN_NOFOLLOW must refuse that open. */
  if( rc==SQLITE_OK_SYMLINK ){
    if( flags & SQLITE_OPEN_NOFOLLOW ){
      sqlite3_free(zFull);
      return SQLITE_CANTOPEN_SYMLINK;
    }
    rc = SQLITE_OK;
  }
  if( rc==SQLITE_OK ){
    rc = chunkStoreDupFilenameDoubleNul(zFull, pzOut);
    sqlite3_free(zFull);
    return rc;
  }
  sqlite3_free(zFull);
  return rc;
}

int csSyncFile(ChunkStore *cs){
  if( cs->noSync ) return SQLITE_OK;
  return sqlite3OsSync(cs->file.pFile,
                       cs->fullFsync ? SQLITE_SYNC_FULL : SQLITE_SYNC_NORMAL);
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
  /* A short file cannot hold a chunk-store header. Stock SQLite maps the same
  ** class (garbage / truncated open) to SQLITE_NOTADB ("file is not a
  ** database") rather than a bare IOERR_SHORT_READ / "disk I/O error" — see
  ** misc5-4.1 / ticket #1370. */
  if( rc==SQLITE_IOERR_SHORT_READ ) return SQLITE_NOTADB;
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

  rc = csCanonicalFilename(pVfs, zFilename, &cs->file.zFilename, flags);
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
                  | SQLITE_OPEN_MAIN_DB
                  | (flags & SQLITE_OPEN_NOFOLLOW);
    int outFlags = 0;
    rc = csOpenFile(pVfs, cs->file.zFilename, &cs->file.pFile, openFlags, &outFlags);
    if( rc != SQLITE_OK ){
      /* Do not downgrade OOM-failed writable opens to read-only. */
      if( wantReadOnly || rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
        chunkStoreClose(cs);
        return rc;
      }
      openFlags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB
                | (flags & SQLITE_OPEN_NOFOLLOW);
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
    /* Stock defers NOTADB until first use (sqlite3_open succeeds; CREATE
    ** fails with "file is not a database"). Keep the garbage bytes intact. */
    if( rc==SQLITE_NOTADB ){
      cs->notADatabase = 1;
      cs->index.nChunks = 0;
      cs->index.iIndexOffset = 0;
      cs->index.nIndexSize = 0;
      cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
      csMarkRefsCommitted(cs);
      return SQLITE_OK;
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
    /* Fail early when the parent directory is missing so sqlite3_open
    ** returns SQLITE_CANTOPEN like stock SQLite, instead of succeeding
    ** with an empty connection that only fails on first write. Probe the
    ** parent rather than creating the file: WASM/node VFSes can reject
    ** eager CREATE while still supporting the usual first-write open.
    **
    ** Parent is OK if either ACCESS_READWRITE or ACCESS_EXISTS reports
    ** success. Neither alone is portable:
    **   - Windows ACCESS_EXISTS treats zero-length objects as missing, and
    **     directories report size zero — so EXISTS alone CANTOPEN's every
    **     new DB under a real parent.
    **   - Some WASM/node VFSes reject ACCESS_READWRITE on directories that
    **     still pass EXISTS (and reject eager CREATE), so READWRITE alone
    **     breaks opens that only EXISTS can green-light.
    **
    ** Access failures (IOERR from faultsim, etc.) are treated as
    ** inconclusive — fall through to deferred open rather than
    ** promoting a probe error into open failure. Only a definitive
    ** "parent is absent" answer becomes CANTOPEN. */
    {
      const char *zPath = cs->file.zFilename;
      const char *zSlash = strrchr(zPath, '/');
      int parentOk = 1;
#ifdef _WIN32
      {
        const char *zB = strrchr(zPath, '\\');
        if( zB && (!zSlash || zB>zSlash) ) zSlash = zB;
      }
#endif
      if( zSlash && zSlash!=zPath ){
        int nDir = (int)(zSlash - zPath);
        int canWrite = 0;
        int exists = 0;
        char *zDir = (char*)sqlite3_malloc(nDir + 1);
        if( !zDir ){
          chunkStoreClose(cs);
          return SQLITE_NOMEM;
        }
        memcpy(zDir, zPath, (size_t)nDir);
        zDir[nDir] = 0;
        rc = sqlite3OsAccess(pVfs, zDir, SQLITE_ACCESS_READWRITE, &canWrite);
        if( rc==SQLITE_OK && !canWrite ){
          rc = sqlite3OsAccess(pVfs, zDir, SQLITE_ACCESS_EXISTS, &exists);
        }
        sqlite3_free(zDir);
        if( rc==SQLITE_OK ){
          parentOk = canWrite || exists;
          if( !parentOk ){
            /* Parent is missing: still attempt OsOpen so the host VFS logs the
            ** create-path failure like stock. oserror-1.3.2 expects a VFS log
            ** line naming the failed create-path syscall. Access alone never
            ** hits that log path. Create cannot succeed without a parent. */
            sqlite3_file *pProbe = 0;
            int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                          | SQLITE_OPEN_MAIN_DB
                          | (flags & SQLITE_OPEN_NOFOLLOW);
            (void)csOpenFile(pVfs, cs->file.zFilename, &pProbe, openFlags, 0);
            if( pProbe ) sqlite3OsCloseFree(pProbe);
            chunkStoreClose(cs);
            return SQLITE_CANTOPEN;
          }
        }
        /* rc!=OK: probe inconclusive (e.g. ioerr fault injection) — defer. */
      }
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
  if( cs->notADatabase ) return SQLITE_NOTADB;
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

  if( cs->notADatabase ) return SQLITE_NOTADB;
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
  if( cs->notADatabase ) return SQLITE_NOTADB;

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

void csFillChunkHdr(u8 *p, const ProllyHash *pHash, u32 size){
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

  if( cs->notADatabase ) return SQLITE_NOTADB;
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

  if( cs->notADatabase ) return SQLITE_NOTADB;
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
