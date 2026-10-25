

#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"
#include "../ext/blake3/blake3.h"

/* unix VFS asserts a 128KiB read cap; writers already slice. */
#define CS_IO_SLICE 65536
int csReadSliced(ChunkStore *cs, void *pBuf, i64 nByte, i64 iOff){
  u8 *p = (u8*)pBuf;
  while( nByte > 0 ){
    int n = nByte > CS_IO_SLICE ? CS_IO_SLICE : (int)nByte;
    int rc = sqlite3OsRead(cs->file.pFile, p, n, iOff);
    if( rc != SQLITE_OK ) return rc;
    p += n;
    iOff += n;
    nByte -= n;
  }
  return SQLITE_OK;
}
#ifdef SQLITE_CRASH_TEST
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

int chunkStoreStagingExceedsDrain(ChunkStore *cs){
  if( !cs ) return 0;
  return cs->staging.nWriteBuf >= csPendingDrainLimit();
}

#if CHUNK_STORE_LE_PACKING
typedef char chunk_index_entry_size_check[
  (sizeof(ChunkIndexEntry) == CHUNK_INDEX_ENTRY_SIZE) ? 1 : -1
];
#endif

int chunkStoreValidateWorkingSetBlob(const u8 *data, int nData){
  int nExpected;
  if( !data || nData<1 ) return SQLITE_CORRUPT;
  switch( data[0] ){
    case WS_FORMAT_VERSION_V2: nExpected = WS_TOTAL_SIZE_V2; break;
    case WS_FORMAT_VERSION_V3: nExpected = WS_TOTAL_SIZE_V3; break;
    case WS_FORMAT_VERSION_V4: nExpected = WS_TOTAL_SIZE_V4; break;
    case WS_FORMAT_VERSION_V5: nExpected = WS_TOTAL_SIZE; break;
    default: return SQLITE_CORRUPT;
  }
  return nData==nExpected ? SQLITE_OK : SQLITE_CORRUPT;
}


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

/* xAccess reports a 0-byte regular file as absent. Distinguish that from a
** missing path without xOpen: a failed open is logged by the VFS, including
** wrapper VFSes, even when this connection goes on to create the database. */
static int csHostFileIsZero(const char *zPath){
  FILE *p;
  long sz;

  p = fopen(zPath, "rb");
  if( !p ) return 0;
  sz = -1;
  if( fseek(p, 0, SEEK_END)==0 ) sz = ftell(p);
  fclose(p);
  return sz==0;
}

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
  /* pagerOpen: OK_SYMLINK means a symlink; SQLITE_OPEN_NOFOLLOW refuses. */
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

int chunkStoreReadFileHeader(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  u8 *aBuf,
  int nBuf,
  int *pRead
){
  sqlite3_file *pFile = 0;
  int exists = 0;
  int outFlags = 0;
  int nFull;
  char *zFull = 0;
  char *zProbe = 0;
  int rc;

  *pRead = 0;
  if( !zFilename || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0 ){
    return SQLITE_OK;
  }
  if( !pVfs ){
    pVfs = sqlite3_vfs_find(0);
    if( !pVfs ) return SQLITE_OK;
  }
  rc = sqlite3OsAccess(pVfs, zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
  if( rc!=SQLITE_OK || !exists ) return SQLITE_OK;

  nFull = pVfs->mxPathname + 1;
  zFull = sqlite3_malloc(nFull);
  if( !zFull ) return SQLITE_NOMEM;
  rc = sqlite3OsFullPathname(pVfs, zFilename, nFull, zFull);
  if( rc==SQLITE_OK_SYMLINK ) rc = SQLITE_OK;
  if( rc!=SQLITE_OK ){
    sqlite3_free(zFull);
    return rc;
  }
  rc = chunkStoreDupFilenameDoubleNul(zFull, &zProbe);
  sqlite3_free(zFull);
  if( rc!=SQLITE_OK ) return rc;

  /* zProbe must outlive close: unixClose logs pFile->zPath. */
  rc = sqlite3OsOpenMalloc(pVfs, zProbe, &pFile,
                           SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB,
                           &outFlags);
  if( rc!=SQLITE_OK ){
    if( pFile ) sqlite3OsCloseFree(pFile);
    sqlite3_free(zProbe);
    if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
    return SQLITE_OK;
  }
  rc = sqlite3OsRead(pFile, aBuf, nBuf, 0);
  sqlite3OsCloseFree(pFile);
  sqlite3_free(zProbe);
  if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ) return rc;
  if( rc==SQLITE_OK ) *pRead = 1;
  return SQLITE_OK;
}

int csSyncFile(ChunkStore *cs){
  if( cs->noSync ) return SQLITE_OK;
  return sqlite3OsSync(cs->file.pFile,
                       cs->fullFsync ? SQLITE_SYNC_FULL : SQLITE_SYNC_NORMAL);
}

void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + CS_MANIFEST_MAGIC_OFF, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + CS_MANIFEST_VERSION_OFF, CHUNK_STORE_VERSION);

  /* nChunks and nIndexSize are u32 on disk; truncating would corrupt. */
  assert( cs->index.nChunks>=0 && (u64)cs->index.nChunks<=0xffffffffu );
  assert( cs->index.nIndexSize>=0 && (u64)cs->index.nIndexSize<=0xffffffffu );
  CS_WRITE_U32(aBuf + CS_MANIFEST_CHUNK_COUNT_OFF, (u32)cs->index.nChunks);
  CS_WRITE_I64(aBuf + CS_MANIFEST_INDEX_OFFSET_OFF, cs->index.iIndexOffset);
  CS_WRITE_U32(aBuf + CS_MANIFEST_INDEX_SIZE_OFF, (u32)cs->index.nIndexSize);

  CS_WRITE_I64(aBuf + CS_MANIFEST_WAL_OFFSET_OFF, cs->wal.iWalOffset);
  memcpy(aBuf + CS_MANIFEST_REFS_HASH_OFF, cs->refs.refsHash.data, PROLLY_HASH_SIZE);
}

static void csManifestHashOffset(blake3_hasher *pHasher, i64 iOffset){
  u8 aOffset[8];
  CS_WRITE_I64(aOffset, iOffset);
  blake3_hasher_update(pHasher, aOffset, sizeof(aOffset));
}

void csManifestSeal(u8 *aBuf, i64 iOffset){
  blake3_hasher hasher;
  memset(aBuf + CS_MANIFEST_SELF_HASH_OFF, 0, PROLLY_HASH_SIZE);
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, aBuf, CHUNK_MANIFEST_SIZE);
  csManifestHashOffset(&hasher, iOffset);
  blake3_hasher_finalize(&hasher, aBuf + CS_MANIFEST_SELF_HASH_OFF,
                         PROLLY_HASH_SIZE);
}

static int csManifestHashStateImpl(
  const u8 *aBuf,
  i64 iOffset,
  int bindOffset
){
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
  if( bindOffset ) csManifestHashOffset(&hasher, iOffset);
  blake3_hasher_finalize(&hasher, aHash, PROLLY_HASH_SIZE);
  return memcmp(aHash, aBuf + CS_MANIFEST_SELF_HASH_OFF, PROLLY_HASH_SIZE)==0
       ? CS_MANIFEST_HASH_OK : CS_MANIFEST_HASH_BAD;
}

int csManifestHashState(const u8 *aBuf, i64 iOffset){
  return csManifestHashStateImpl(aBuf, iOffset, 1);
}

int csManifestHashStateOffsetless(const u8 *aBuf){
  return csManifestHashStateImpl(aBuf, 0, 0);
}

int csValidateWalRootManifest(
  const ChunkStore *cs,
  const u8 *aBuf,
  i64 iRootOffset
){
  u32 nChunks = CS_READ_U32(aBuf + CS_MANIFEST_CHUNK_COUNT_OFF);
  u32 nIndexSize = CS_READ_U32(aBuf + CS_MANIFEST_INDEX_SIZE_OFF);
  i64 iIndexOffset = CS_READ_I64(aBuf + CS_MANIFEST_INDEX_OFFSET_OFF);
  i64 iWalOffset = CS_READ_I64(aBuf + CS_MANIFEST_WAL_OFFSET_OFF);
  i64 durableTo = CS_READ_I64(aBuf + CS_MANIFEST_DURABLE_TO_OFF);
  i64 batchStart = CS_READ_I64(aBuf + CS_MANIFEST_BATCH_START_OFF);
  i64 rootEnd;
  i64 nextOff = CS_READ_I64(aBuf + CS_MANIFEST_NEXT_OFF_OFF);

  if( CS_READ_U32(aBuf + CS_MANIFEST_MAGIC_OFF)!=CHUNK_STORE_MAGIC
   || CS_READ_U32(aBuf + CS_MANIFEST_VERSION_OFF)!=CHUNK_STORE_VERSION
   || nChunks>(u32)INT_MAX || nIndexSize>(u32)INT_MAX
   || nIndexSize%CHUNK_INDEX_ENTRY_SIZE!=0
   || nIndexSize/CHUNK_INDEX_ENTRY_SIZE>nChunks
   || iIndexOffset<0 || iWalOffset<CHUNK_MANIFEST_SIZE
   || (nIndexSize>0 && iIndexOffset<CHUNK_MANIFEST_SIZE) ){
    return SQLITE_CORRUPT;
  }
  if( iIndexOffset>0
   && (iWalOffset<iIndexOffset
       || iWalOffset-iIndexOffset<(i64)nIndexSize) ){
    return SQLITE_CORRUPT;
  }
  if( cs
   && (iWalOffset!=cs->wal.iWalOffset
       || iIndexOffset!=cs->index.iIndexOffset
       || (i64)nIndexSize!=cs->index.nIndexSize) ){
    return SQLITE_CORRUPT;
  }
  if( iRootOffset<iWalOffset
   || iRootOffset>LARGEST_INT64-(1+CHUNK_MANIFEST_SIZE) ){
    return SQLITE_CORRUPT;
  }
  rootEnd = iRootOffset + 1 + CHUNK_MANIFEST_SIZE;
  /* Gaps are sector padding; writers cap sectors at 64KiB. */
  if( durableTo<iWalOffset || durableTo>batchStart
   || batchStart>iRootOffset || batchStart-durableTo>=65536
   || nextOff<rootEnd || nextOff-rootEnd>=65536 ){
    return SQLITE_CORRUPT;
  }
  return SQLITE_OK;
}

static int csReadManifest(ChunkStore *cs){
  u8 aBuf[CHUNK_MANIFEST_SIZE];
  u32 magic, version;
  int rc;

  rc = sqlite3OsRead(cs->file.pFile, aBuf, CHUNK_MANIFEST_SIZE, 0);
  /* Short/garbage open is SQLITE_NOTADB, not IOERR_SHORT_READ (misc5-4.1). */
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

  /* Every writer seals the header. Files sealed before seals were bound to
  ** their offset verify offsetless; an all-zero seal is legacy. */
  if( csManifestHashState(aBuf, 0)==CS_MANIFEST_HASH_BAD
   && csManifestHashStateOffsetless(aBuf)!=CS_MANIFEST_HASH_OK ){
    return SQLITE_CORRUPT;
  }

  cs->index.nChunks = (int)CS_READ_U32(aBuf + CS_MANIFEST_CHUNK_COUNT_OFF);
  cs->index.iIndexOffset = CS_READ_I64(aBuf + CS_MANIFEST_INDEX_OFFSET_OFF);
  cs->index.nIndexSize = (i64)CS_READ_U32(aBuf + CS_MANIFEST_INDEX_SIZE_OFF);

  cs->wal.iWalOffset = CS_READ_I64(aBuf + CS_MANIFEST_WAL_OFFSET_OFF);
  memcpy(cs->refs.refsHash.data, aBuf + CS_MANIFEST_REFS_HASH_OFF, PROLLY_HASH_SIZE);

  /* A WAL offset inside live data makes replay rewind and reclaim rows.
  ** Replay bounds it against the file size when it stats the file. */
  if( cs->index.iIndexOffset<0 || cs->index.nIndexSize<0 ) return SQLITE_CORRUPT;
  if( cs->wal.iWalOffset < CHUNK_MANIFEST_SIZE ) return SQLITE_CORRUPT;
  /* Do not sum: iIndexOffset + nIndexSize can wrap i64. */
  if( cs->index.iIndexOffset>0
   && ( cs->wal.iWalOffset < cs->index.iIndexOffset
     || cs->wal.iWalOffset - cs->index.iIndexOffset < cs->index.nIndexSize ) ){
    return SQLITE_CORRUPT;
  }

  return SQLITE_OK;
}

/* Distinguish crashed first-commit from a damaged committed DB. */
static int csScanForCommittedRoot(ChunkStore *cs, int *pEverCommitted,
                                  int *pCreationRoot){
  return csScanForCommittedRootEx(cs, pEverCommitted, pCreationRoot, 0);
}

int csScanForCommittedRootEx(ChunkStore *cs, int *pEverCommitted,
                             int *pCreationRoot, int *pLaterBatch){
  u8 *buf;
  i64 fileSize = 0;
  i64 q = CHUNK_MANIFEST_SIZE;
  int rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
  *pEverCommitted = 0;
  *pCreationRoot = 0;
  if( pLaterBatch ) *pLaterBatch = 0;
  if( rc != SQLITE_OK ) return rc;
  buf = sqlite3_malloc(CS_SCAN_WINDOW);
  if( !buf ) return SQLITE_NOMEM;
  while( q + 5 <= fileSize ){
    i64 nAvail = fileSize - q;
    int n = nAvail > (i64)CS_SCAN_WINDOW ? CS_SCAN_WINDOW : (int)nAvail;
    int i;
    rc = sqlite3OsRead(cs->file.pFile, buf, n, q);
    if( rc != SQLITE_OK ) goto scan_done;
    for(i=0; i+5<=n; i++){
      if( buf[i]==CS_WAL_TAG_ROOT && CS_READ_U32(buf+i+1)==CHUNK_STORE_MAGIC ){
        u8 m[CHUNK_MANIFEST_SIZE];
        int state;
        if( q + i + 1 + CHUNK_MANIFEST_SIZE > fileSize ) continue;
        rc = sqlite3OsRead(cs->file.pFile, m, CHUNK_MANIFEST_SIZE, q + i + 1);
        if( rc != SQLITE_OK ) goto scan_done;
        state = csManifestHashState(m, q + i);
        if( state==CS_MANIFEST_HASH_OK ){
          if( csValidateWalRootManifest(0, m, q+i)!=SQLITE_OK ){
            *pEverCommitted = 1;
            goto scan_done;
          }
          if( CS_READ_I64(m + CS_MANIFEST_DURABLE_TO_OFF) > CHUNK_MANIFEST_SIZE ){
            *pEverCommitted = 1;
            goto scan_done;
          }
          if( pLaterBatch
           && CS_READ_I64(m + CS_MANIFEST_BATCH_START_OFF) > CHUNK_MANIFEST_SIZE ){
            *pLaterBatch = 1;
          }
          *pCreationRoot = 1;
        }
      }
    }
    if( (i64)n >= nAvail ) break;
    q += n - 4;
  }

scan_done:
  sqlite3_free(buf);
  return rc;
}


static int csOpen(
  ChunkStore *cs,
  sqlite3_vfs *pVfs,
  const char *zFilename,
  int flags,
  int deferBusy,
  CsReadLock *pReadLock
){
  int rc;
  int exists = 0;
  int emptyFile = 0;

  memset(cs, 0, sizeof(*cs));
  csIndexCacheSetBudget(cs, 200*1024);
  if( !pVfs ){
    pVfs = sqlite3_vfs_find(0);
    if( !pVfs ) return SQLITE_CANTOPEN;
  }
  cs->file.pVfs = pVfs;
  cs->isBuffer = sqlite3IsDoltliteMemdb(pVfs);
  cs->isSharedMemory = sqlite3IsDoltliteSharedMemVfs(pVfs);
  CS_GRAPH_LOCK(cs) = CS_FILE_LOCK_INIT;
  cs->pGraphLockName = 0;
  cs->pLockMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  if( cs->pLockMutex==0 && sqlite3GlobalConfig.bCoreMutex ){
    chunkStoreClose(cs);
    return SQLITE_NOMEM;
  }
  cs->lockDepth = 0;
  cs->noReadLock = (flags & SQLITE_OPEN_URI)!=0
               && (flags & SQLITE_OPEN_READONLY)!=0
               && sqlite3_uri_boolean(zFilename, "nolock", 0);

  if( zFilename==0 || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0
   || (flags & SQLITE_OPEN_MEMORY)!=0 ){
    cs->isMemory = 1;
    /* SQLITE_OPEN_READONLY applies to :memory: too. */
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
    /* Some VFSes report a 0-byte file as present. Stock opens it empty. */
    if( mainSize==0 ){
      exists = 0;
      emptyFile = 1;
      if( (flags & SQLITE_OPEN_READONLY)!=0 ) cs->readOnly = 1;
    }
  }else if( csHostFileIsZero(cs->file.zFilename) ){
    /* unix and win xAccess treat a 0-byte regular file as absent. Stock
    ** opens that file as an empty database. A missing path stays absent. */
    emptyFile = 1;
    if( (flags & SQLITE_OPEN_READONLY)!=0 ) cs->readOnly = 1;
  }

  if( exists ){
    int wantReadOnly = (flags & SQLITE_OPEN_READONLY)!=0;
    int openFlags = (wantReadOnly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE)
                  | SQLITE_OPEN_MAIN_DB
                  | (flags & SQLITE_OPEN_NOFOLLOW);
    int outFlags = 0;
    rc = csOpenFile(pVfs, cs->file.zFilename, &cs->file.pFile, openFlags, &outFlags);
    if( rc != SQLITE_OK ){
      /* Do not fall back to read-only after an OOM on the writable open. */
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
      /* Probe the fallback handle so a directory fails at open. */
      {
        u8 probe;
        if( sqlite3OsRead(cs->file.pFile, &probe, 1, 0)==SQLITE_IOERR_READ ){
          chunkStoreClose(cs);
          return SQLITE_CANTOPEN;
        }
      }
      cs->readOnly = 1;
    }else if( wantReadOnly || (outFlags & SQLITE_OPEN_READONLY) ){
      /* Caller asked, or the VFS silently opened read-only. */
      cs->readOnly = 1;
    }else if( sqlite3OsDoltliteHasMultipleLinks(cs->file.pFile) ){
      /* The graph lock is keyed by path and gc replaces the file by rename,
      ** so writers through another hard link are neither serialized nor
      ** kept on the same file. */
      cs->readOnly = 1;
    }

    if( !cs->isBuffer && !cs->noReadLock ){
      int retry = 0;
      do {
        rc = csReadLock(cs, pReadLock);
        if( rc!=SQLITE_BUSY || retry++>=100 ) break;
        csReadUnlock(cs, pReadLock);
        sqlite3_sleep(1);
      }while( rc==SQLITE_BUSY );
      if( rc==SQLITE_BUSY && deferBusy ){
        cs->openPending = 1;
        return SQLITE_OK;
      }
      if( rc!=SQLITE_OK ){
        chunkStoreClose(cs);
        return rc;
      }
    }
    rc = csReadManifest(cs);
    /* Truncate-to-empty only for NOTADB, not for a damaged-but-identified
    ** header. SQLITE_OPEN_CREATE keeps the full scan. Other modes recover
    ** only an all-zero header sector, so a foreign file is not read through.
    ** Read-only presents that empty store and does not rewrite the file. */
    if( rc==SQLITE_NOTADB ){
      u8 aHdr[CHUNK_MANIFEST_SIZE];
      int allZero = 0;
      int everCommitted = 1;
      int creationRoot = 0;
      int rc2;
      int i;
      rc2 = sqlite3OsRead(cs->file.pFile, aHdr, CHUNK_MANIFEST_SIZE, 0);
      if( rc2==SQLITE_OK ){
        for(i=0; i<CHUNK_MANIFEST_SIZE && aHdr[i]==0; i++){}
        allZero = (i==CHUNK_MANIFEST_SIZE);
      }
      if( ((flags & SQLITE_OPEN_CREATE)!=0 && !cs->readOnly) || allZero ){
        rc2 = csScanForCommittedRoot(cs, &everCommitted, &creationRoot);
        if( rc2==SQLITE_OK && !everCommitted && !creationRoot && allZero ){
          creationRoot = 1;
        }
        if( rc2==SQLITE_OK && !everCommitted && creationRoot ){
          cs->index.nChunks = 0;
          cs->index.iIndexOffset = 0;
          cs->index.nIndexSize = 0;
          cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
          if( cs->readOnly ){
            i64 sz = 0;
            rc2 = sqlite3OsFileSize(cs->file.pFile, &sz);
            if( rc2==SQLITE_OK ){
              cs->file.iFileSize = sz;
              csMarkRefsCommitted(cs);
              return SQLITE_OK;
            }
          }else{
            cs->file.iFileSize = 0;
            rc = sqlite3OsTruncate(cs->file.pFile, 0);
            if( rc==SQLITE_OK ){
              csMarkRefsCommitted(cs);
              return SQLITE_OK;
            }
          }
        }
      }
    }
    /* Stock defers NOTADB until first use; keep the garbage bytes. */
    if( rc==SQLITE_NOTADB ){
      cs->notADatabase = 1;
      cs->index.nChunks = 0;
      cs->index.iIndexOffset = 0;
      cs->index.nIndexSize = 0;
      cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
      csMarkRefsCommitted(cs);
      return SQLITE_OK;
    }
    /* A damaged header is likewise surfaced on first use, never served. */
    if( rc==SQLITE_CORRUPT ){
      cs->corruptHeader = 1;
      cs->corruptMidStream = 1;
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

    {
      int loadedCheckpoint = 0;
      i64 iSkipStart = 0;
      i64 iSkipEnd = 0;
      rc = csTryLoadWalCheckpoint(cs, &loadedCheckpoint,
                                  &iSkipStart, &iSkipEnd);
      if( rc==SQLITE_OK && !loadedCheckpoint ) rc = csReadIndex(cs);
      if( rc==SQLITE_OK && !loadedCheckpoint ){
        rc = csReplayWalSkipping(cs, iSkipStart, iSkipEnd);
      }
    }
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
    if( !emptyFile && !(flags & SQLITE_OPEN_CREATE) ){
      chunkStoreClose(cs);
      return SQLITE_CANTOPEN;
    }
    /* Missing parent -> CANTOPEN at open, not on first write. Probe the
    ** parent rather than CREATE (WASM/node VFSes may reject eager CREATE).
    ** Parent is OK if ACCESS_READWRITE or ACCESS_EXISTS succeeds: Windows
    ** EXISTS treats size-0 dirs as missing; some WASM VFSes reject
    ** READWRITE on dirs that still pass EXISTS. Probe IOERR is
    ** inconclusive — only a definitive absent parent is CANTOPEN. */
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
        if( rc==SQLITE_NOMEM || rc==SQLITE_IOERR_NOMEM ){
          chunkStoreClose(cs);
          return rc;
        }
        if( rc==SQLITE_OK ){
          parentOk = canWrite || exists;
          if( !parentOk ){
            /* OsOpen so the VFS logs the create-path failure (oserror-1.3.2). */
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
        /* Probe IOERR: defer the open. */
      }
    }
    cs->index.nChunks = 0;
    cs->index.iIndexOffset = 0;
    cs->index.nIndexSize = 0;

    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->file.iFileSize = 0;
    cs->file.pFile = 0;
  }

  /* Poison after this open's refs/branch reads. */
  if( cs->wal.recoveredMidStream ) cs->corruptMidStream = 1;

  csMarkRefsCommitted(cs);
  return SQLITE_OK;
}

static int csOpenUnlocked(
  ChunkStore *cs,
  sqlite3_vfs *pVfs,
  const char *zFilename,
  int flags,
  int deferBusy
){
  CsReadLock readLock = {0, 0};
  int rc = csOpen(cs, pVfs, zFilename, flags, deferBusy, &readLock);
  int rc2 = csReadUnlock(cs, &readLock);
  if( rc==SQLITE_OK && rc2!=SQLITE_OK ){
    rc = rc2;
    chunkStoreClose(cs);
  }
  return rc;
}

int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags){
  return csOpenUnlocked(cs, pVfs, zFilename, flags, 0);
}

int chunkStoreOpenDeferred(ChunkStore *cs, sqlite3_vfs *pVfs,
                           const char *zFilename, int flags){
  return csOpenUnlocked(cs, pVfs, zFilename, flags, 1);
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

  if( cs->isMemory || cs->isBuffer || cs->isSharedMemory || cs->readOnly
   || cs->movedReadOnly || cs->corruptMidStream ){
    return;
  }
  if( !cs->file.pFile || !cs->file.zFilename ){
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
  if( cs->wal.cleanCloseMarker && !csWalCheckpointDue(cs) ) return;

  lockHeld = cs->lockDepth>0;
  if( !lockHeld ){
    rc = csFileLock(cs->file.pVfs, cs->file.zFilename, 0, &lockFd, &lockName);
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

  if( csWalCheckpointDue(cs) ){
    int wroteCheckpoint = 0;
    rc = csWriteWalCheckpoint(cs, sectorSize, &wroteCheckpoint);
    if( rc!=SQLITE_OK ){
      sqlite3_log(SQLITE_NOTICE,
        "doltlite: unable to checkpoint chunk WAL on close: %d", rc);
    }
    if( wroteCheckpoint ) goto done;
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
  csStampWalCheckpoint(cs, rootRec + 1);
  CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_DURABLE_TO_OFF, markerStart);
  CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_NEXT_OFF_OFF, markerNext);
  CS_WRITE_I64(rootRec + 1 + CS_MANIFEST_BATCH_START_OFF, markerStart);
  csManifestSeal(rootRec + 1, markerStart);

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
  chunkStoreSourceClose(cs);
  /* Clean-close marker is optional; failures are silent, so malloc is benign. */
  sqlite3BeginBenignMalloc();
  (void)csScrubFailedRoot(cs);
  csWriteCleanCloseMarker(cs);
  sqlite3EndBenignMalloc();
  chunkStoreUnlock(cs);
  csFileUnlock(CS_GRAPH_LOCK(cs), &cs->pGraphLockName);
  CS_GRAPH_LOCK(cs) = CS_FILE_LOCK_INIT;
  if( cs->file.pFile ){
    csCloseFile(cs->file.pFile);
    cs->file.pFile = 0;
  }
  if( cs->pOwnedVfs ){
    sqlite3MemdbDestroyPrivateVfs(cs->pOwnedVfs);
    cs->pOwnedVfs = 0;
  }
  sqlite3_free(cs->file.zFilename);
  csIndexCacheFree(cs);
  csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase, cs->index.aIndexMmapSize);
  cs->index.aIndex = 0;
  cs->index.aIndexMmapBase = 0;
  cs->index.aIndexMmapSize = 0;
  sqlite3_free(cs->aStoredHit);
  cs->aStoredHit = 0;
  cs->nStoredHit = cs->nStoredHitAlloc = 0;
  sqlite3_free(cs->staging.aPending);
  sqlite3_free(cs->staging.aPendingZeroTail);
  sqlite3_free(cs->staging.aRecent);
  sqlite3_free(cs->staging.aRecentZeroTail);
  csPendHTClear(cs);
  csRecentHTClear(cs);
  sqlite3_free(cs->staging.pWriteBuf);
  csMemSegFreeAll(cs);
  sqlite3_free(cs->refs.zDefaultBranch);
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  csFreeSequences(cs);
  csDropTxnSequences(cs);
  chunkStoreFreeWsBranches(cs);
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
  ChunkIndexEntry entry;
  *pHas = 0;
  if( cs->notADatabase ) return SQLITE_NOTADB;
  rc = csIndexLookup(cs, hash, &entry, pHas);
  if( rc!=SQLITE_OK ) return rc;
  if( *pHas ){
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
  if( idx >= 0 ){
    *pHas = 1;
    return SQLITE_OK;
  }
  return chunkStoreSourceHas(cs, hash, pHas);
}

int chunkStoreVerifyChunk(
  const ProllyHash *hash,
  u8 **ppData,
  int *pnData,
  ProllyStats *pStats
){
  ProllyHash h;
  int nData;
  nData = *pnData;
  prollyHashCompute(*ppData, nData, &h);
  if( memcmp(&h, hash, sizeof(ProllyHash)) != 0 ){
    sqlite3_free(*ppData);
    *ppData = 0;
    *pnData = 0;
    return SQLITE_CORRUPT;
  }
  prollyStatAdd(pStats, nChunkRead, 1);
  prollyStatAdd(pStats, nVerifyBytes, (u64)nData);
  return SQLITE_OK;
}

static int csReadChunk(
  ChunkStore *cs,
  const ChunkIndexEntry *e,
  const ProllyHash *pHash,
  u8 **ppData,
  int *pnData
){
  u8 *pBuf;
  int rc;
  int sz = e->size;

  if( sz > INT_MAX - 4 ) return SQLITE_TOOBIG;
  pBuf = (u8*)sqlite3_malloc(sz + 4);
  if( !pBuf ) return SQLITE_NOMEM;
  rc = csReadSliced(cs, pBuf, (i64)sz + 4, e->offset);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pBuf);
    return rc;
  }
  if( CS_READ_U32(pBuf)!=(u32)sz ){
    sqlite3_free(pBuf);
    return SQLITE_CORRUPT;
  }
  memmove(pBuf, pBuf + 4, sz);
  *ppData = pBuf;
  *pnData = sz;
  return chunkStoreVerifyChunk(pHash, ppData, pnData, cs->pStats);
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
  if( cs->iFailedRootOff ){
    rc = csScrubFailedRoot(cs);
    if( rc!=SQLITE_OK ) return rc;
  }
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

    memcpy(pCopy, cs->staging.pWriteBuf + (off - cs->staging.iMemBase) + 4,
           (size_t)(sz - nZ));
    if( nZ>0 ) memset(pCopy + (sz - nZ), 0, (size_t)nZ);
    *ppData = pCopy;
    *pnData = sz;
    /* Memory-store bytes are private copies hashed by chunkStorePut(). */
    if( cs->isMemory ){
      prollyStatAdd(cs->pStats, nChunkRead, 1);
      return SQLITE_OK;
    }
    return chunkStoreVerifyChunk(hash, ppData, pnData, cs->pStats);
  }

  {
    ChunkIndexEntry *e;
    ChunkIndexEntry indexEntry;
    int found = 0;
    rc = csSearchRecent(cs, hash, &idx);
    if( rc!=SQLITE_OK ) return rc;
    if( idx >= 0 ){
      e = &cs->staging.aRecent[idx];
    }else{
      rc = csIndexLookup(cs, hash, &indexEntry, &found);
      if( rc!=SQLITE_OK ) return rc;
      if( !found ){
        return chunkStoreSourceGet(cs, hash, ppData, pnData);
      }
      e = &indexEntry;
    }

    if( cs->file.pFile == 0 ){
      ChunkMemSeg *pSeg;
      const u8 *pChunk = csMemChunk(cs, e->offset, e->size, &pSeg);
      if( pChunk ){
        u8 *pCopy = (u8 *)sqlite3_malloc(e->size);
        if( pCopy == 0 ) return SQLITE_NOMEM;
        memcpy(pCopy, pChunk, e->size);
        *ppData = pCopy;
        *pnData = e->size;
        if( cs->isMemory ) return SQLITE_OK;
        return chunkStoreVerifyChunk(hash, ppData, pnData, cs->pStats);
      }
      return SQLITE_CORRUPT;
    }

    return csReadChunk(cs, e, hash, ppData, pnData);
  }
}

typedef struct CsBatchRead CsBatchRead;
struct CsBatchRead {
  ChunkIndexEntry entry;
  int iOut;
};

static int csBatchReadCmp(const void *a, const void *b){
  const CsBatchRead *p = (const CsBatchRead*)a;
  const CsBatchRead *q = (const CsBatchRead*)b;
  if( p->entry.offset<q->entry.offset ) return -1;
  if( p->entry.offset>q->entry.offset ) return 1;
  return p->iOut - q->iOut;
}

static int csReadBatchFits(i64 iStart, i64 iEnd, const ChunkIndexEntry *e){
  return e->offset>=iEnd && e->offset-iEnd<=CS_WAL_CHUNK_HDR_SIZE
      && e->offset-iStart<=CHUNK_READ_BATCH_BYTES-e->size-4;
}

int chunkStoreReadBatch(
  ChunkStore *cs,
  const ProllyHash *aHash,
  int nHash,
  u8 **apData,
  int *anData,
  int *pnRead
){
  CsBatchRead aRead[CHUNK_READ_BATCH_MAX];
  u8 *pBuf = 0;
  int nRead = 0;
  int nMax = MIN(nHash, CHUNK_READ_BATCH_MAX);
  int rc = SQLITE_OK;
  int i;

  *pnRead = 0;
  if( nHash<0 || nHash>CHUNK_READ_BATCH_MAX ) return SQLITE_MISUSE;
  if( nHash==0 ) return SQLITE_OK;
  memset(apData, 0, nMax*sizeof(*apData));
  memset(anData, 0, nMax*sizeof(*anData));
  if( cs->notADatabase ) return SQLITE_NOTADB;
  if( cs->iFailedRootOff ){
    rc = csScrubFailedRoot(cs);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( cs->corruptMidStream ) return SQLITE_CORRUPT;
  for(i=0; cs->file.pFile && i<nMax; i++){
    ChunkIndexEntry e;
    int idx;
    int found = 0;
    rc = csSearchPending(cs, &aHash[i], &idx);
    if( rc!=SQLITE_OK ) return rc;
    if( idx>=0 ) break;
    rc = csSearchRecent(cs, &aHash[i], &idx);
    if( rc!=SQLITE_OK ) return rc;
    if( idx>=0 ){
      e = cs->staging.aRecent[idx];
    }else{
      rc = csIndexLookup(cs, &aHash[i], &e, &found);
      if( rc!=SQLITE_OK ) return rc;
      if( !found ) break;
    }
    if( e.size<0 || e.size>CHUNK_READ_BATCH_BYTES-4
     || e.offset<CHUNK_MANIFEST_SIZE
     || e.offset>cs->file.iFileSize-e.size-4 ) break;
    aRead[nRead].entry = e;
    aRead[nRead++].iOut = i;
  }
  if( nRead==0 ){
    rc = chunkStoreGet(cs, aHash, apData, anData);
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
    if( rc==SQLITE_OK ) *pnRead = 1;
    return rc;
  }
  qsort(aRead, nRead, sizeof(*aRead), csBatchReadCmp);
  for(i=0; i<nRead && rc==SQLITE_OK; ){
    i64 iStart = aRead[i].entry.offset;
    i64 iEnd = iStart+aRead[i].entry.size+4;
    int j = i+1;
    int k;
    while( j<nRead && csReadBatchFits(iStart, iEnd, &aRead[j].entry) ){
      iEnd = aRead[j].entry.offset+aRead[j].entry.size+4;
      j++;
    }
    if( j==i+1 ){
      int out = aRead[i].iOut;
      rc = csReadChunk(cs, &aRead[i].entry, &aHash[out],
                       &apData[out], &anData[out]);
      i = j;
      continue;
    }
    if( !pBuf ){
      pBuf = sqlite3_malloc(CHUNK_READ_BATCH_BYTES);
      if( !pBuf ){
        rc = SQLITE_NOMEM;
        break;
      }
    }
    rc = csReadSliced(cs, pBuf, iEnd-iStart, iStart);
    for(k=i; k<j && rc==SQLITE_OK; k++){
      ChunkIndexEntry *e = &aRead[k].entry;
      const u8 *p = pBuf+(e->offset-iStart);
      int out = aRead[k].iOut;
      if( CS_READ_U32(p)!=(u32)e->size ){
        rc = SQLITE_CORRUPT;
        break;
      }
      apData[out] = sqlite3_malloc(e->size+4);
      if( !apData[out] ){
        rc = SQLITE_NOMEM;
        break;
      }
      memcpy(apData[out], p+4, e->size);
      anData[out] = e->size;
      rc = chunkStoreVerifyChunk(&aHash[out], &apData[out], &anData[out],
                                 cs->pStats);
    }
    i = j;
  }
  sqlite3_free(pBuf);
  if( rc==SQLITE_OK ){
    *pnRead = nRead;
  }else{
    for(i=0; i<nMax; i++){
      sqlite3_free(apData[i]);
      apData[i] = 0;
      anData[i] = 0;
    }
  }
  return rc;
}

int chunkStoreBorrow(
  ChunkStore *cs,
  const ProllyHash *hash,
  const u8 **ppData,
  int *pnData,
  void **ppSeg
){
  ChunkIndexEntry e;
  ChunkMemSeg *pSeg = 0;
  const u8 *pChunk;
  int found = 0;
  int rc;

  *ppData = 0;
  *pnData = 0;
  *ppSeg = 0;
  if( !cs->isMemory || cs->notADatabase || cs->corruptMidStream ){
    return SQLITE_NOTFOUND;
  }
  rc = csIndexLookup(cs, hash, &e, &found);
  if( rc!=SQLITE_OK ) return rc;
  if( !found || e.offset>=cs->staging.iMemBase ) return SQLITE_NOTFOUND;
  pChunk = csMemChunk(cs, e.offset, e.size, &pSeg);
  if( !pChunk || !pSeg ) return SQLITE_CORRUPT;
  pSeg->nRef++;
  *ppData = pChunk;
  *pnData = e.size;
  *ppSeg = pSeg;
  return SQLITE_OK;
}

void chunkStoreBorrowRelease(void *pSeg){
  csMemSegRelease((ChunkMemSeg*)pSeg);
}

int chunkStoreReadAhead(
  ChunkStore *cs,
  const ProllyHash *aHash,
  int nHash,
  int (*xCached)(void*, const ProllyHash*),
  int (*xRead)(void*, const ProllyHash*, const u8*, int),
  void *pCtx
){
  ChunkIndexEntry aEntry[CHUNK_READ_AHEAD_MAX];
  i64 iStart = 0;
  i64 iEnd = 0;
  u8 *aData;
  int nEntry = 0;
  int i;
  int rc;

  if( cs->notADatabase ) return SQLITE_NOTADB;
  if( cs->iFailedRootOff ){
    rc = csScrubFailedRoot(cs);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( cs->corruptMidStream ) return SQLITE_CORRUPT;
  if( !cs->file.pFile ) return SQLITE_OK;
  for(i=0; i<nHash && i<CHUNK_READ_AHEAD_MAX; i++){
    ChunkIndexEntry e;
    int found = 0;
    rc = csIndexLookup(cs, &aHash[i], &e, &found);
    if( rc!=SQLITE_OK ) return rc;
    if( !found || e.size<=0 || e.size>CHUNK_READ_BATCH_BYTES-4
     || e.offset<CHUNK_MANIFEST_SIZE
     || e.offset>cs->file.iFileSize-e.size-4 ) break;
    if( nEntry && !csReadBatchFits(iStart, iEnd, &e) ) break;
    if( nEntry==0 ) iStart = e.offset;
    if( e.offset-iStart>CHUNK_READ_BATCH_BYTES-e.size-4 ) break;
    iEnd = e.offset+e.size+4;
    aEntry[nEntry++] = e;
  }
  if( nEntry<2 ) return SQLITE_OK;
  aData = sqlite3_malloc((int)(iEnd-iStart));
  if( !aData ) return SQLITE_NOMEM;
  rc = csReadSliced(cs, aData, iEnd-iStart, iStart);
  for(i=0; rc==SQLITE_OK && i<nEntry; i++){
    ChunkIndexEntry *e = &aEntry[i];
    const u8 *p = aData+(e->offset-iStart);
    ProllyHash actual;
    if( xCached(pCtx, &e->hash) ) continue;
    if( CS_READ_U32(p)!=(u32)e->size ){
      rc = SQLITE_CORRUPT;
      break;
    }
    prollyHashCompute(p+4, e->size, &actual);
    if( prollyHashCompare(&actual, &e->hash)!=0 ){
      rc = SQLITE_CORRUPT;
      break;
    }
    rc = xRead(pCtx, &e->hash, p+4, e->size);
  }
  sqlite3_free(aData);
  return rc;
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
      memcpy(pBuf, cs->staging.pWriteBuf + (e->offset - cs->staging.iMemBase) + 4,
             nPhys);
    }
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
    rc = csReadSliced(cs, pBuf, (i64)nPhys + 4, e->offset);
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
  int dense;
  i64 nDense = 0;
  i64 writeOff;

  if( cs->isMemory || cs->readOnly || cs->corruptMidStream ){
    return SQLITE_OK;
  }
  if( cs->staging.nPending==0 ){
    return SQLITE_OK;
  }
  if( cs->lockDepth<=0 && !cs->isBuffer ){
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
    if( sqlite3OsDoltliteHasMultipleLinks(cs->file.pFile) ){
      sqlite3OsCloseFree(cs->file.pFile);
      cs->file.pFile = 0;
      return SQLITE_READONLY;
    }
    sqlite3OsDoltliteRequestDirSync(cs->file.pFile);
  }

  if( cs->staging.nRecentUncommitted==0 ){
    rc = csReclaimTail(cs);
    if( rc!=SQLITE_OK ) return rc;
    cs->staging.iUncommittedStart = cs->file.iFileSize;
  }

  writeOff = cs->file.iFileSize;
  if( writeOff == 0 ){
    u8 manifest[CHUNK_MANIFEST_SIZE];
    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    csSerializeManifest(cs, manifest);
    csManifestSeal(manifest, 0);
    rc = sqlite3OsWrite(cs->file.pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc!=SQLITE_OK ) return rc;
    writeOff = CHUNK_MANIFEST_SIZE;
  }

  rc = csGrowRecent(cs, cs->staging.nPending);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<cs->staging.nPending; i++){
    ChunkIndexEntry *pe = &cs->staging.aPending[i];
    if( cs->staging.aPendingZeroTail[i]!=0
     || pe->offset!=nDense+CS_WAL_CHUNK_LEN_OFF ) break;
    nDense += CS_WAL_CHUNK_HDR_SIZE+(i64)pe->size;
  }
  dense = i==cs->staging.nPending && nDense==cs->staging.nWriteBuf;
  if( dense ){
    int pos = 0;
    if( writeOff>LARGEST_INT64-nDense ) return SQLITE_TOOBIG;
    while( pos<cs->staging.nWriteBuf ){
      int n = cs->staging.nWriteBuf-pos;
      if( n>65536 ) n = 65536;
      rc = sqlite3OsWrite(cs->file.pFile, cs->staging.pWriteBuf+pos,
                          n, writeOff+pos);
      if( rc!=SQLITE_OK ) return rc;
      pos += n;
    }
  }

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

    if( dense ){
      writeOff += CS_WAL_CHUNK_HDR_SIZE+(i64)pe->size;
      continue;
    }

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

static void csNoteStoredHit(ChunkStore *cs, const ProllyHash *h){
  int idx = -1;
  if( cs->isMemory || cs->isBuffer ) return;
  if( csSearchPending(cs, h, &idx)==SQLITE_OK && idx>=0 ) return;
  if( cs->nStoredHit==0 && !cs->storedHitLost ){
    cs->storedHitReloadGen = cs->reloadGen;
  }
  if( cs->storedHitLost ) return;
  if( cs->nStoredHit>=cs->nStoredHitAlloc ){
    int nNew = cs->nStoredHitAlloc ? cs->nStoredHitAlloc*2 : 16;
    ProllyHash *aNew;
    sqlite3BeginBenignMalloc();
    aNew = sqlite3_realloc64(cs->aStoredHit, (u64)nNew*sizeof(ProllyHash));
    sqlite3EndBenignMalloc();
    if( !aNew ){
      cs->storedHitLost = 1;
      return;
    }
    cs->aStoredHit = aNew;
    cs->nStoredHitAlloc = nNew;
  }
  cs->aStoredHit[cs->nStoredHit++] = *h;
}

/* Every chunk format stores child hashes raw, so a pending chunk that
** references h carries its bytes. */
static int csPendingMentions(ChunkStore *cs, const ProllyHash *h){
  const u8 *p = cs->staging.pWriteBuf;
  i64 n = cs->staging.nWriteBuf;
  i64 i;
  for(i=0; i+PROLLY_HASH_SIZE<=n; i++){
    const u8 *q = memchr(p+i, h->data[0], (size_t)(n-PROLLY_HASH_SIZE+1-i));
    if( !q ) return 0;
    i = q - p;
    if( memcmp(q, h->data, PROLLY_HASH_SIZE)==0 ) return 1;
  }
  return 0;
}

void chunkStoreNoteSourceCommit(ChunkStore *cs, const ProllyHash *h){
  int i;
  if( cs->isMemory || cs->isBuffer ) return;
  if( cs->nSourceHit==0 && !cs->sourceHitLost ){
    cs->sourceHitReloadGen = cs->reloadGen;
  }
  for(i=0; i<cs->nSourceHit; i++){
    if( prollyHashCompare(&cs->aSourceHit[i], h)==0 ) return;
  }
  if( cs->nSourceHit>=(int)(sizeof(cs->aSourceHit)/sizeof(cs->aSourceHit[0])) ){
    cs->sourceHitLost = 1;
    return;
  }
  cs->aSourceHit[cs->nSourceHit++] = *h;
}

void chunkStoreClearSourceCommits(ChunkStore *cs){
  cs->nSourceHit = 0;
  cs->sourceHitLost = 0;
}

static int csSourceCommitSwept(ChunkStore *cs, int *pSwept){
  int i;
  *pSwept = 0;
  if( cs->nSourceHit==0 && !cs->sourceHitLost ) return SQLITE_OK;
  if( cs->reloadGen==cs->sourceHitReloadGen ) return SQLITE_OK;
  if( cs->sourceHitLost ){
    *pSwept = 1;
    return SQLITE_OK;
  }
  for(i=0; i<cs->nSourceHit; i++){
    int has = 0;
    int rc = chunkStoreHas(cs, &cs->aSourceHit[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    if( !has ){
      *pSwept = 1;
      return SQLITE_OK;
    }
  }
  return SQLITE_OK;
}

/* Chunks leave the store only when a gc replaces the file, which forces a
** full reload; without one since the first hit, every hit still holds. A
** swept hit matters only if this batch references it; one left by abandoned
** work does not. */
int csVerifyStoredHits(ChunkStore *cs){
  int i;
  int bSwept = 0;
  int rc = csSourceCommitSwept(cs, &bSwept);
  if( rc!=SQLITE_OK ) return rc;
  if( bSwept ) return SQLITE_BUSY;
  if( cs->nStoredHit==0 && !cs->storedHitLost ) return SQLITE_OK;
  if( cs->reloadGen==cs->storedHitReloadGen ) return SQLITE_OK;
  if( cs->storedHitLost ) return SQLITE_BUSY;
  for(i=0; i<cs->nStoredHit; i++){
    int has = 0;
    rc = chunkStoreHas(cs, &cs->aStoredHit[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    if( !has && csPendingMentions(cs, &cs->aStoredHit[i]) ) return SQLITE_BUSY;
  }
  return SQLITE_OK;
}

/* For a command about to read back what it built: any swept hit may be
** part of it. */
int chunkStoreStoredHitSwept(ChunkStore *cs, int *pSwept){
  int i;
  int rc = csSourceCommitSwept(cs, pSwept);
  if( rc!=SQLITE_OK || *pSwept ) return rc;
  if( cs->nStoredHit==0 && !cs->storedHitLost ) return SQLITE_OK;
  if( cs->reloadGen==cs->storedHitReloadGen ) return SQLITE_OK;
  if( cs->storedHitLost ){
    *pSwept = 1;
    return SQLITE_OK;
  }
  for(i=0; i<cs->nStoredHit; i++){
    int has = 0;
    rc = chunkStoreHas(cs, &cs->aStoredHit[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    if( !has ){
      *pSwept = 1;
      return SQLITE_OK;
    }
  }
  return SQLITE_OK;
}

void chunkStoreClearStoredHits(ChunkStore *cs){
  cs->nStoredHit = 0;
  cs->storedHitLost = 0;
}

static int csPutChunk(
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  ProllyHash *pHash,
  const ProllyHash *pExpected
){
  int rc;
  ProllyHash h;

  if( cs->notADatabase ) return SQLITE_NOTADB;
  if( nData<0 ) return SQLITE_TOOBIG;
  prollyHashCompute(pData, nData, &h);
  prollyStatAdd(cs->pStats, nHashBytes, (u64)nData);
  if( pExpected && prollyHashCompare(&h, pExpected)!=0 ) return SQLITE_CORRUPT;
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  {
    int bHas = 0;
    int hasRc = chunkStoreHas(cs, &h, &bHas);
    if( hasRc==SQLITE_OK && bHas ){
      csNoteStoredHit(cs, &h);
      return SQLITE_OK;
    }
    if( hasRc!=SQLITE_OK ) return hasRc;
  }

  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;

  if( cs->isMemory ){
    rc = csGrowWriteBuf(cs, 4 + nData + CHUNK_STORE_BORROW_PAD);
  }else{
    rc = csGrowWriteBuf(cs, CS_WAL_CHUNK_HDR_SIZE + nData);
  }
  if( rc != SQLITE_OK ) return rc;

  {
    ChunkIndexEntry *e = &cs->staging.aPending[cs->staging.nPending];
    e->hash = h;
    e->offset = cs->staging.iMemBase + cs->staging.nWriteBuf;
    if( !cs->isMemory ) e->offset += CS_WAL_CHUNK_LEN_OFF;
    e->size = nData;
    cs->staging.aPendingZeroTail[cs->staging.nPending] = 0;
    cs->staging.nPending++;
  }

  if( cs->isMemory ){
    CS_WRITE_U32(cs->staging.pWriteBuf + cs->staging.nWriteBuf, (u32)nData);
    cs->staging.nWriteBuf += 4;
  }else{
    csFillChunkHdr(cs->staging.pWriteBuf + cs->staging.nWriteBuf,
                   &h, (u32)nData);
    cs->staging.nWriteBuf += CS_WAL_CHUNK_HDR_SIZE;
  }
  memcpy(cs->staging.pWriteBuf + cs->staging.nWriteBuf, pData, nData);
  cs->staging.nWriteBuf += nData;
  if( cs->isMemory ){
    memset(cs->staging.pWriteBuf + cs->staging.nWriteBuf, 0,
           CHUNK_STORE_BORROW_PAD);
    cs->staging.nWriteBuf += CHUNK_STORE_BORROW_PAD;
  }

  if( cs->staging.nWriteBuf >= csPendingDrainLimit() ){
    rc = csDrainPendingToWal(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  prollyStatAdd(cs->pStats, nChunkWrite, 1);
  return SQLITE_OK;
}

int chunkStorePut(
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  ProllyHash *pHash
){
  return csPutChunk(cs, pData, nData, pHash, 0);
}

int chunkStorePutChecked(
  ChunkStore *cs,
  const ProllyHash *pExpected,
  const u8 *pData,
  int nData
){
  if( !pExpected ) return SQLITE_MISUSE;
  return csPutChunk(cs, pData, nData, 0, pExpected);
}

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
  prollyStatAdd(cs->pStats, nHashBytes, (u64)nData);
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  {
    int bHas = 0;
    int hasRc = chunkStoreHas(cs, &h, &bHas);
    if( hasRc==SQLITE_OK && bHas ){
      csNoteStoredHit(cs, &h);
      return SQLITE_OK;
    }
    if( hasRc!=SQLITE_OK ) return hasRc;
  }

  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;
  rc = csGrowWriteBuf(cs, CS_WAL_CHUNK_HDR_SIZE + nPrefix);
  if( rc != SQLITE_OK ) return rc;

  {
    ChunkIndexEntry *e = &cs->staging.aPending[cs->staging.nPending];
    e->hash = h;
    e->offset = (i64)cs->staging.nWriteBuf + CS_WAL_CHUNK_LEN_OFF;
    e->size = nData;
    cs->staging.aPendingZeroTail[cs->staging.nPending] = nZeroTail;
    cs->staging.nPending++;
  }

  csFillChunkHdr(cs->staging.pWriteBuf + cs->staging.nWriteBuf,
                 &h, (u32)nData);
  cs->staging.nWriteBuf += CS_WAL_CHUNK_HDR_SIZE;
  if( nPrefix>0 ){
    memcpy(cs->staging.pWriteBuf + cs->staging.nWriteBuf, pPrefix, nPrefix);
    cs->staging.nWriteBuf += nPrefix;
  }

  if( cs->staging.nWriteBuf >= csPendingDrainLimit() ){
    rc = csDrainPendingToWal(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  prollyStatAdd(cs->pStats, nChunkWrite, 1);
  return SQLITE_OK;
}

static int csCopyEntries(
  ChunkStore *pSrc,
  ChunkStore *pDest,
  const ChunkIndexEntry *aEntry,
  int nEntry
){
  int i;
  for(i=0; i<nEntry; i++){
    u8 *pData = 0;
    int nData = 0;
    int nPhys = 0;
    int rc = chunkStoreGetSparse(pSrc, &aEntry[i].hash,
                                 &pData, &nData, &nPhys);
    if( rc==SQLITE_OK ){
      rc = chunkStorePutSparse(pDest, pData, nPhys,
                               (i64)nData - nPhys, 0);
    }
    sqlite3_free(pData);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

int chunkStoreCopyIntoEmptyNoCommit(ChunkStore *pSrc, ChunkStore *pDest){
  const ChunkIndexEntry *aEntry = 0;
  u8 *pRefs = 0;
  int nEntry = 0;
  int nRefs = 0;
  int rc;

  assert( chunkStoreIsEmpty(pDest) );
  rc = csMaterializeIndex(pSrc);
  if( rc!=SQLITE_OK ) return rc;
  chunkIndexGetEntries(&pSrc->index, &nEntry, &aEntry);
  rc = csCopyEntries(pSrc, pDest, aEntry, nEntry);
  if( rc!=SQLITE_OK ) return rc;

  chunkStagingGetRecent(&pSrc->staging, &nEntry, &aEntry);
  rc = csCopyEntries(pSrc, pDest, aEntry, nEntry);
  if( rc!=SQLITE_OK ) return rc;

  chunkStagingGetPending(&pSrc->staging, &nEntry, &aEntry);
  rc = csCopyEntries(pSrc, pDest, aEntry, nEntry);
  if( rc!=SQLITE_OK ) return rc;

  rc = chunkStoreSerializeRefsToBlob(pSrc, &pRefs, &nRefs);
  if( rc==SQLITE_OK ){
    rc = chunkStoreInstallRefsBlob(pDest, pRefs, nRefs);
  }
  sqlite3_free(pRefs);
  return rc;
}

int chunkStoreCopyIntoEmpty(ChunkStore *pSrc, ChunkStore *pDest){
  int rc = chunkStoreCopyIntoEmptyNoCommit(pSrc, pDest);
  if( rc!=SQLITE_OK ) return rc;
  return chunkStoreCommit(pDest);
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
  csDropTxnSequences(cs);
  memset(&cs->refs.refsHash, 0, sizeof(cs->refs.refsHash));
}

int chunkStoreReloadRefs(ChunkStore *cs){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;

  if( prollyHashIsEmpty(&cs->refs.refsHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &cs->refs.refsHash, &refsData, &nRefsData);
  if( rc==SQLITE_OK ){
    rc = csReplaceRefsStateFromBlob(cs, refsData, nRefsData, 0);
  }
  sqlite3_free(refsData);
  cs->bRefsStale = rc!=SQLITE_OK;
  return rc;
}

const char *chunkStoreFilename(ChunkStore *cs){
  return cs->file.zFilename;
}

#endif
