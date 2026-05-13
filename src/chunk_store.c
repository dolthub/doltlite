

#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>

#ifdef _WIN32
# include <io.h>
# include <windows.h>
  static int csFileLock(const char *path, int *pFd){
    int fd = _open(path, _O_BINARY | _O_RDWR | _O_CREAT, 0644);
    if( fd < 0 ) return -1;
    {
      HANDLE h = (HANDLE)_get_osfhandle(fd);
      OVERLAPPED ov = {0};
      if( !LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK, 0, MAXDWORD, MAXDWORD, &ov) ){
        _close(fd);
        return -1;
      }
    }
    *pFd = fd;
    return 0;
  }
  static void csFileUnlock(int fd){
    if( fd >= 0 ){
      HANDLE h = (HANDLE)_get_osfhandle(fd);
      OVERLAPPED ov = {0};
      UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov);
      _close(fd);
    }
  }
  static int csFileLockNB(const char *path, int *pFd){
    int fd = _open(path, _O_BINARY | _O_RDWR | _O_CREAT, 0644);
    if( fd < 0 ) return -1;
    {
      HANDLE h = (HANDLE)_get_osfhandle(fd);
      OVERLAPPED ov = {0};
      if( !LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                       0, MAXDWORD, MAXDWORD, &ov) ){
        _close(fd);
        return -1;
      }
    }
    *pFd = fd;
    return 0;
  }
#else
# include <unistd.h>
# include <sys/file.h>
  static int csFileLock(const char *path, int *pFd){
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if( fd < 0 ) return -1;
    if( flock(fd, LOCK_EX) != 0 ){
      close(fd);
      return -1;
    }
    *pFd = fd;
    return 0;
  }
  static void csFileUnlock(int fd){
    if( fd >= 0 ) close(fd);
  }
  static int csFileLockNB(const char *path, int *pFd){
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if( fd < 0 ) return -1;
    if( flock(fd, LOCK_EX | LOCK_NB) != 0 ){
      close(fd);
      return -1;
    }
    *pFd = fd;
    return 0;
  }
#endif

#define CS_READ_U32(p) (             \
  (u32)(((const u8*)(p))[0])       | \
  (u32)(((const u8*)(p))[1]) << 8  | \
  (u32)(((const u8*)(p))[2]) << 16 | \
  (u32)(((const u8*)(p))[3]) << 24   \
)

#define CS_WRITE_U32(p, v) do {      \
  ((u8*)(p))[0] = (u8)((v));        \
  ((u8*)(p))[1] = (u8)((v) >> 8);   \
  ((u8*)(p))[2] = (u8)((v) >> 16);  \
  ((u8*)(p))[3] = (u8)((v) >> 24);  \
} while(0)

#define CS_READ_I64(p) (                  \
  (i64)((u64)(((const u8*)(p))[0])      ) | \
  (i64)((u64)(((const u8*)(p))[1]) << 8 ) | \
  (i64)((u64)(((const u8*)(p))[2]) << 16) | \
  (i64)((u64)(((const u8*)(p))[3]) << 24) | \
  (i64)((u64)(((const u8*)(p))[4]) << 32) | \
  (i64)((u64)(((const u8*)(p))[5]) << 40) | \
  (i64)((u64)(((const u8*)(p))[6]) << 48) | \
  (i64)((u64)(((const u8*)(p))[7]) << 56)   \
)

#define CS_WRITE_I64(p, v) do {            \
  ((u8*)(p))[0] = (u8)((u64)(v));         \
  ((u8*)(p))[1] = (u8)((u64)(v) >> 8);    \
  ((u8*)(p))[2] = (u8)((u64)(v) >> 16);   \
  ((u8*)(p))[3] = (u8)((u64)(v) >> 24);   \
  ((u8*)(p))[4] = (u8)((u64)(v) >> 32);   \
  ((u8*)(p))[5] = (u8)((u64)(v) >> 40);   \
  ((u8*)(p))[6] = (u8)((u64)(v) >> 48);   \
  ((u8*)(p))[7] = (u8)((u64)(v) >> 56);   \
} while(0)

static int csOpenFile(sqlite3_vfs *pVfs, const char *zPath,
                      sqlite3_file **ppFile, int flags);
static int csRollbackFailedAppend(ChunkStore *cs, i64 origFileSize);
static int csRestoreCommittedRefsState(ChunkStore *cs);
static int csReadManifest(ChunkStore *cs);
static int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData);
static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash, int *pIdx);
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
static int csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e);
static int csGrowRecent(ChunkStore *cs, int nAdd);
static int csGrowWriteBuf(ChunkStore *cs, int nNeeded);

static int csDeserializeRefsIntoTemp(ChunkStore *pTmp, const u8 *data, int nData);
static int csReloadFromDisk(ChunkStore *cs);
static int csDetectExternalChanges(ChunkStore *cs, int *pChanged);

static int csCrashWriteInjectionActive(void){
#ifdef SQLITE_TEST
  const char *zEnv = getenv("DOLTLITE_CRASH_WRITE");
  return zEnv && atoi(zEnv)>0;
#else
  return 0;
#endif
}

#define CS_WAL_TAG_CHUNK  0x01
#define CS_WAL_TAG_ROOT   0x02

#if CHUNK_STORE_LE_PACKING
typedef char chunk_index_entry_size_check[
  (sizeof(ChunkIndexEntry) == CHUNK_INDEX_ENTRY_SIZE) ? 1 : -1
];
#endif

static void csFreeBranches(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nBranches; k++) sqlite3_free(cs->refs.aBranches[k].zName);
  sqlite3_free(cs->refs.aBranches);
  cs->refs.aBranches = 0;
  cs->refs.nBranches = 0;
}
static void csFreeTags(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nTags; k++){
    sqlite3_free(cs->refs.aTags[k].zName);
    sqlite3_free(cs->refs.aTags[k].zTagger);
    sqlite3_free(cs->refs.aTags[k].zEmail);
    sqlite3_free(cs->refs.aTags[k].zMessage);
  }
  sqlite3_free(cs->refs.aTags);
  cs->refs.aTags = 0;
  cs->refs.nTags = 0;
}
static void csFreeRemotes(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nRemotes; k++){
    sqlite3_free(cs->refs.aRemotes[k].zName);
    sqlite3_free(cs->refs.aRemotes[k].zUrl);
  }
  sqlite3_free(cs->refs.aRemotes);
  cs->refs.aRemotes = 0;
  cs->refs.nRemotes = 0;
}
static void csFreeTracking(ChunkStore *cs){
  int k;
  for(k=0; k<cs->refs.nTracking; k++){
    sqlite3_free(cs->refs.aTracking[k].zRemote);
    sqlite3_free(cs->refs.aTracking[k].zBranch);
  }
  sqlite3_free(cs->refs.aTracking);
  cs->refs.aTracking = 0;
  cs->refs.nTracking = 0;
}

static void csMarkRefsCommitted(ChunkStore *cs){
  cs->refs.committedRefsHash = cs->refs.refsHash;
}

static void csRestoreCommittedRefsHash(ChunkStore *cs){
  cs->refs.refsHash = cs->refs.committedRefsHash;
}

void csCaptureSavedRefsState(ChunkStore *cs, SavedRefsState *pSaved){
  memset(pSaved, 0, sizeof(*pSaved));
  pSaved->zDefaultBranch = cs->refs.zDefaultBranch;
  pSaved->aBranches = cs->refs.aBranches;
  pSaved->nBranches = cs->refs.nBranches;
  pSaved->aTags = cs->refs.aTags;
  pSaved->nTags = cs->refs.nTags;
  pSaved->aRemotes = cs->refs.aRemotes;
  pSaved->nRemotes = cs->refs.nRemotes;
  pSaved->aTracking = cs->refs.aTracking;
  pSaved->nTracking = cs->refs.nTracking;
}

static void csDetachSavedRefsState(ChunkStore *cs, SavedRefsState *pSaved){
  csCaptureSavedRefsState(cs, pSaved);
  cs->refs.zDefaultBranch = 0;
  cs->refs.aBranches = 0;
  cs->refs.nBranches = 0;
  cs->refs.aTags = 0;
  cs->refs.nTags = 0;
  cs->refs.aRemotes = 0;
  cs->refs.nRemotes = 0;
  cs->refs.aTracking = 0;
  cs->refs.nTracking = 0;
}

void csRestoreSavedRefsState(ChunkStore *cs, const SavedRefsState *pSaved){
  cs->refs.zDefaultBranch = pSaved->zDefaultBranch;
  cs->refs.aBranches = pSaved->aBranches;
  cs->refs.nBranches = pSaved->nBranches;
  cs->refs.aTags = pSaved->aTags;
  cs->refs.nTags = pSaved->nTags;
  cs->refs.aRemotes = pSaved->aRemotes;
  cs->refs.nRemotes = pSaved->nRemotes;
  cs->refs.aTracking = pSaved->aTracking;
  cs->refs.nTracking = pSaved->nTracking;
}

void csFreeSavedRefsState(SavedRefsState *pSaved){
  ChunkStore refsStore;
  memset(&refsStore, 0, sizeof(refsStore));
  refsStore.refs.zDefaultBranch = pSaved->zDefaultBranch;
  refsStore.refs.aBranches = pSaved->aBranches;
  refsStore.refs.nBranches = pSaved->nBranches;
  refsStore.refs.aTags = pSaved->aTags;
  refsStore.refs.nTags = pSaved->nTags;
  refsStore.refs.aRemotes = pSaved->aRemotes;
  refsStore.refs.nRemotes = pSaved->nRemotes;
  refsStore.refs.aTracking = pSaved->aTracking;
  refsStore.refs.nTracking = pSaved->nTracking;
  csFreeRefsState(&refsStore);
  memset(pSaved, 0, sizeof(*pSaved));
}

void csFreeRefsState(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  sqlite3_free(cs->refs.zDefaultBranch);
  cs->refs.zDefaultBranch = 0;
}

int csEnsureDefaultBranch(ChunkStore *cs){
  if( !cs->refs.zDefaultBranch ){
    cs->refs.zDefaultBranch = sqlite3_mprintf("main");
    if( !cs->refs.zDefaultBranch ) return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

#define CS_INIT_PENDING_ALLOC 16
#define CS_INIT_WRITEBUF_SIZE 4096

static int csOpenFile(
  sqlite3_vfs *pVfs,
  const char *zPath,
  sqlite3_file **ppFile,
  int flags
){
  int rc;
  int outFlags = 0;
  rc = sqlite3OsOpenMalloc(pVfs, zPath, ppFile, flags, &outFlags);
  return rc;
}

void csCloseFile(sqlite3_file *pFile){
  if( pFile ){
    sqlite3OsCloseFree(pFile);
  }
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
                  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
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
    return csEnsureDefaultBranch(cs);
  }
  return chunkStoreReloadRefs(cs);
}

static int csReloadFromDiskPreservingLocalRefs(ChunkStore *cs){
  int rc;
  int preserveRefs = cs->staging.nPending > 0
                  && prollyHashCompare(&cs->refs.refsHash,
                                       &cs->refs.committedRefsHash)!=0;
  ProllyHash savedRefsHash;
  SavedRefsState savedRefs;

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

#define CS_PEND_HT_INIT_BITS 12
#define CS_PEND_HT_MAX_LOAD  4

static u32 csPendBucket(const ProllyHash *h, int nHTMask){
  return ((u32)h->data[0] | ((u32)h->data[1]<<8)
        | ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24)) & (u32)nHTMask;
}

void csPendHTClear(ChunkStore *cs){
  sqlite3_free(cs->staging.aPendingHT);
  sqlite3_free(cs->staging.aPendingHTNext);
  cs->staging.aPendingHT = 0;
  cs->staging.aPendingHTNext = 0;
  cs->staging.nPendingHTBuilt = 0;
  cs->staging.nPendingHTSize = 0;
}

void csRecentHTClear(ChunkStore *cs){
  sqlite3_free(cs->staging.aRecentHT);
  sqlite3_free(cs->staging.aRecentHTNext);
  cs->staging.aRecentHT = 0;
  cs->staging.aRecentHTNext = 0;
  cs->staging.nRecentHTBuilt = 0;
  cs->staging.nRecentHTSize = 0;
  cs->staging.nRecentHTNextAlloc = 0;
}

static int csPendHTRebuild(ChunkStore *cs){
  int i;
  memset(cs->staging.aPendingHT, 0xff, cs->staging.nPendingHTSize * sizeof(int));
  for(i=0; i<cs->staging.nPending; i++){
    u32 b = csPendBucket(&cs->staging.aPending[i].hash, cs->staging.nPendingHTSize - 1);
    cs->staging.aPendingHTNext[i] = cs->staging.aPendingHT[b];
    cs->staging.aPendingHT[b] = i;
  }
  cs->staging.nPendingHTBuilt = cs->staging.nPending;
  return SQLITE_OK;
}

static int csPendHTEnsure(ChunkStore *cs){
  int i;
  if( cs->staging.nPending==0 ) return SQLITE_OK;
  if( !cs->staging.aPendingHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->staging.aPendingHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->staging.aPendingHT ) return SQLITE_NOMEM;
    memset(cs->staging.aPendingHT, 0xff, initSize * sizeof(int));
    cs->staging.nPendingHTSize = initSize;
    cs->staging.nPendingHTBuilt = 0;
  }
  if( cs->staging.nPending > cs->staging.nPendingHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->staging.nPendingHTSize * 4;
    int *aNew = sqlite3_realloc(cs->staging.aPendingHT, newSize * (int)sizeof(int));
    if( aNew ){
      cs->staging.aPendingHT = aNew;
      cs->staging.nPendingHTSize = newSize;
      if( !cs->staging.aPendingHTNext || cs->staging.nPendingAlloc > cs->staging.nPendingHTNextAlloc ){
        int nAlloc = cs->staging.nPendingAlloc > 0 ? cs->staging.nPendingAlloc : 64;
        int *aNew2 = sqlite3_realloc(cs->staging.aPendingHTNext, nAlloc*(int)sizeof(int));
        if( !aNew2 ) return SQLITE_NOMEM;
        cs->staging.aPendingHTNext = aNew2;
        cs->staging.nPendingHTNextAlloc = nAlloc;
      }
      return csPendHTRebuild(cs);
    }

  }
  if( !cs->staging.aPendingHTNext || cs->staging.nPendingAlloc > cs->staging.nPendingHTNextAlloc ){
    int nAlloc = cs->staging.nPendingAlloc > 0 ? cs->staging.nPendingAlloc : 64;
    int *aNew = sqlite3_realloc(cs->staging.aPendingHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->staging.aPendingHTNext = aNew;
    cs->staging.nPendingHTNextAlloc = nAlloc;
  }

  for(i=cs->staging.nPendingHTBuilt; i<cs->staging.nPending; i++){
    u32 b = csPendBucket(&cs->staging.aPending[i].hash, cs->staging.nPendingHTSize - 1);
    cs->staging.aPendingHTNext[i] = cs->staging.aPendingHT[b];
    cs->staging.aPendingHT[b] = i;
  }
  cs->staging.nPendingHTBuilt = cs->staging.nPending;
  return SQLITE_OK;
}

static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash, int *pIdx){
  int i; u32 b; int rc;
  *pIdx = -1;
  if( cs->staging.nPending==0 ) return SQLITE_OK;
  rc = csPendHTEnsure(cs);
  if( rc!=SQLITE_OK ){

    for(i=0; i<cs->staging.nPending; i++){
      if( prollyHashCompare(&cs->staging.aPending[i].hash, pHash)==0 ){
        *pIdx = i;
        return SQLITE_OK;
      }
    }
    return rc;
  }
  b = csPendBucket(pHash, cs->staging.nPendingHTSize - 1);
  i = cs->staging.aPendingHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->staging.aPending[i].hash, pHash)==0 ){
      *pIdx = i;
      return SQLITE_OK;
    }
    i = cs->staging.aPendingHTNext[i];
  }
  return SQLITE_OK;
}

static int csRecentHTEnsure(ChunkStore *cs){
  int i;
  if( cs->staging.nRecent==0 ) return SQLITE_OK;
  if( !cs->staging.aRecentHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->staging.aRecentHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->staging.aRecentHT ) return SQLITE_NOMEM;
    memset(cs->staging.aRecentHT, 0xff, initSize * sizeof(int));
    cs->staging.nRecentHTSize = initSize;
    cs->staging.nRecentHTBuilt = 0;
  }
  if( cs->staging.nRecent > cs->staging.nRecentHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->staging.nRecentHTSize * 4;
    int *aNew = sqlite3_realloc(cs->staging.aRecentHT, newSize * (int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->staging.aRecentHT = aNew;
    cs->staging.nRecentHTSize = newSize;
    memset(cs->staging.aRecentHT, 0xff, cs->staging.nRecentHTSize * sizeof(int));
    cs->staging.nRecentHTBuilt = 0;
  }
  if( !cs->staging.aRecentHTNext || cs->staging.nRecentAlloc > cs->staging.nRecentHTNextAlloc ){
    int nAlloc = cs->staging.nRecentAlloc > 0 ? cs->staging.nRecentAlloc : 64;
    int *aNew = sqlite3_realloc(cs->staging.aRecentHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->staging.aRecentHTNext = aNew;
    cs->staging.nRecentHTNextAlloc = nAlloc;
  }
  for(i=cs->staging.nRecentHTBuilt; i<cs->staging.nRecent; i++){
    u32 b = csPendBucket(&cs->staging.aRecent[i].hash, cs->staging.nRecentHTSize - 1);
    cs->staging.aRecentHTNext[i] = cs->staging.aRecentHT[b];
    cs->staging.aRecentHT[b] = i;
  }
  cs->staging.nRecentHTBuilt = cs->staging.nRecent;
  return SQLITE_OK;
}

static int csSearchRecent(ChunkStore *cs, const ProllyHash *pHash, int *pIdx){
  int i; u32 b; int rc;
  *pIdx = -1;
  if( cs->staging.nRecent==0 ) return SQLITE_OK;
  rc = csRecentHTEnsure(cs);
  if( rc!=SQLITE_OK ){
    for(i=cs->staging.nRecent-1; i>=0; i--){
      if( prollyHashCompare(&cs->staging.aRecent[i].hash, pHash)==0 ){
        *pIdx = i;
        return SQLITE_OK;
      }
    }
    return rc;
  }
  b = csPendBucket(pHash, cs->staging.nRecentHTSize - 1);
  i = cs->staging.aRecentHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->staging.aRecent[i].hash, pHash)==0 ){
      *pIdx = i;
      return SQLITE_OK;
    }
    i = cs->staging.aRecentHTNext[i];
  }
  return SQLITE_OK;
}

void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + 0, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + 4, CHUNK_STORE_VERSION);

  CS_WRITE_U32(aBuf + 28, (u32)cs->index.nChunks);
  CS_WRITE_I64(aBuf + 32, cs->index.iIndexOffset);
  CS_WRITE_U32(aBuf + 40, (u32)cs->index.nIndexSize);

  CS_WRITE_I64(aBuf + 84, cs->wal.iWalOffset);
  memcpy(aBuf + 104, cs->refs.refsHash.data, PROLLY_HASH_SIZE);
}

static int csReadManifest(ChunkStore *cs){
  u8 aBuf[CHUNK_MANIFEST_SIZE];
  u32 magic, version;
  int rc;

  rc = sqlite3OsRead(cs->file.pFile, aBuf, CHUNK_MANIFEST_SIZE, 0);
  if( rc != SQLITE_OK ) return rc;

  magic = CS_READ_U32(aBuf + 0);
  version = CS_READ_U32(aBuf + 4);
  if( magic != CHUNK_STORE_MAGIC ) return SQLITE_NOTADB;
  if( version != CHUNK_STORE_VERSION ){
    sqlite3_log(SQLITE_NOTADB,
      "doltlite: chunk store format version %u, expected %u "
      "(database written by an incompatible doltlite version; "
      "this build refuses to open it to prevent corruption)",
      version, CHUNK_STORE_VERSION);
    return SQLITE_NOTADB;
  }

  cs->index.nChunks = (int)CS_READ_U32(aBuf + 28);
  cs->index.iIndexOffset = CS_READ_I64(aBuf + 32);
  cs->index.nIndexSize = (i64)CS_READ_U32(aBuf + 40);

  cs->wal.iWalOffset = CS_READ_I64(aBuf + 84);
  memcpy(cs->refs.refsHash.data, aBuf + 104, PROLLY_HASH_SIZE);

  return SQLITE_OK;
}

int csGrowPending(ChunkStore *cs){
  if( cs->staging.nPending >= cs->staging.nPendingAlloc ){
    int nNew = cs->staging.nPendingAlloc ? cs->staging.nPendingAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->staging.aPending, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->staging.aPending = aNew;
    cs->staging.nPendingAlloc = nNew;
  }
  return SQLITE_OK;
}

static int csGrowRecent(ChunkStore *cs, int nAdd){
  int nNeed = cs->staging.nRecent + nAdd;
  if( nNeed > cs->staging.nRecentAlloc ){
    int nNew = cs->staging.nRecentAlloc ? cs->staging.nRecentAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew;
    while( nNew < nNeed ) nNew *= 2;
    aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->staging.aRecent, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->staging.aRecent = aNew;
    cs->staging.nRecentAlloc = nNew;
  }
  return SQLITE_OK;
}

static int csGrowWriteBuf(ChunkStore *cs, int nNeeded){
  i64 nRequired = cs->staging.nWriteBuf + (i64)nNeeded;
  if( nRequired > cs->staging.nWriteBufAlloc ){
    i64 nNew = cs->staging.nWriteBufAlloc ? cs->staging.nWriteBufAlloc : CS_INIT_WRITEBUF_SIZE;
    u8 *pNew;

    while( nNew < nRequired ){
      if( nNew < 64*1024*1024 ){
        nNew *= 2;
      }else{
        nNew += nNew / 2;
      }
    }
    pNew = (u8 *)sqlite3_realloc64(cs->staging.pWriteBuf, (sqlite3_uint64)nNew);
    if( pNew == 0 ) return SQLITE_NOMEM;
    cs->staging.pWriteBuf = pNew;
    cs->staging.nWriteBufAlloc = nNew;
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
  int n;

  memset(cs, 0, sizeof(*cs));
  cs->file.pVfs = pVfs;
  cs->graphLockFd = -1;

  if( zFilename==0 || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0 ){
    cs->isMemory = 1;
    cs->file.zFilename = sqlite3_mprintf(":memory:");
    if( cs->file.zFilename==0 ) return SQLITE_NOMEM;
    cs->index.nChunks = 0;
    cs->index.iIndexOffset = 0;
    cs->index.nIndexSize = 0;
    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->file.pFile = 0;
    return SQLITE_OK;
  }

  n = (int)strlen(zFilename);
  cs->file.zFilename = (char *)sqlite3_malloc(n + 1);
  if( cs->file.zFilename == 0 ) return SQLITE_NOMEM;
  memcpy(cs->file.zFilename, zFilename, n + 1);

  rc = sqlite3OsAccess(pVfs, cs->file.zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc != SQLITE_OK ){
    sqlite3_free(cs->file.zFilename);
    cs->file.zFilename = 0;
    return rc;
  }

  if( exists ){
    struct stat mainStat;
    if( stat(cs->file.zFilename, &mainStat)==0 && mainStat.st_size==0 ){
      exists = 0;
    }
  }

  if( exists ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(pVfs, cs->file.zFilename, &cs->file.pFile, openFlags);
    if( rc != SQLITE_OK ){
      openFlags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB;
      rc = csOpenFile(pVfs, cs->file.zFilename, &cs->file.pFile, openFlags);
      if( rc != SQLITE_OK ){
        sqlite3_free(cs->file.zFilename);
        cs->file.zFilename = 0;
        return rc;
      }
      cs->readOnly = 1;
    }

    rc = csReadManifest(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->file.pFile);
      cs->file.pFile = 0;
      sqlite3_free(cs->file.zFilename);
      cs->file.zFilename = 0;
      return rc;
    }

    rc = csReadIndex(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->file.pFile);
      cs->file.pFile = 0;
      sqlite3_free(cs->file.zFilename);
      cs->file.zFilename = 0;
      return rc;
    }

    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->file.pFile);
      cs->file.pFile = 0;
      sqlite3_free(cs->file.zFilename);
      cs->file.zFilename = 0;
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
      sqlite3_free(cs->file.zFilename);
      cs->file.zFilename = 0;
      return SQLITE_CANTOPEN;
    }
    cs->index.nChunks = 0;
    cs->index.iIndexOffset = 0;
    cs->index.nIndexSize = 0;

    cs->wal.iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->file.iFileSize = 0;
    cs->file.pFile = 0;
  }

  csMarkRefsCommitted(cs);
  return SQLITE_OK;
}

int chunkStoreClose(ChunkStore *cs){
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
  sqlite3_free(cs->staging.aRecent);
  csPendHTClear(cs);
  csRecentHTClear(cs);
  sqlite3_free(cs->staging.pWriteBuf);
  sqlite3_free(cs->refs.zDefaultBranch);
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
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

int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->refs.nBranches; i++){
    if( strcmp(cs->refs.aBranches[i].zName, zName)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->refs.aBranches[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  struct BranchRef *aNew;
  if( chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->refs.aBranches, (cs->refs.nBranches+1)*(int)sizeof(struct BranchRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->refs.aBranches = aNew;
  memset(&aNew[cs->refs.nBranches], 0, sizeof(struct BranchRef));
  aNew[cs->refs.nBranches].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->refs.nBranches].zName ) return SQLITE_NOMEM;
  memcpy(&aNew[cs->refs.nBranches].commitHash, pCommit, sizeof(ProllyHash));
  cs->refs.nBranches++;
  return SQLITE_OK;
}

int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->refs.nBranches; i++){
    if( strcmp(cs->refs.aBranches[i].zName, zName)==0 ){
      memcpy(&cs->refs.aBranches[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName){
  int i;
  for(i=0; i<cs->refs.nBranches; i++){
    if( strcmp(cs->refs.aBranches[i].zName, zName)==0 ){
      sqlite3_free(cs->refs.aBranches[i].zName);
      cs->refs.aBranches[i] = cs->refs.aBranches[cs->refs.nBranches-1];
      cs->refs.nBranches--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash){
  int i;
  for(i=0; i<cs->refs.nBranches; i++){
    if( strcmp(cs->refs.aBranches[i].zName, zBranch)==0 ){
      memcpy(pHash, &cs->refs.aBranches[i].workingSetHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  memset(pHash, 0, sizeof(ProllyHash));
  return SQLITE_NOTFOUND;
}

int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash){
  int i;
  for(i=0; i<cs->refs.nBranches; i++){
    if( strcmp(cs->refs.aBranches[i].zName, zBranch)==0 ){
      memcpy(&cs->refs.aBranches[i].workingSetHash, pHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->refs.nTags; i++){
    if( strcmp(cs->refs.aTags[i].zName, zName)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->refs.aTags[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
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
  struct TagRef *aNew;
  if( chunkStoreFindTag(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->refs.aTags, (cs->refs.nTags+1)*(int)sizeof(struct TagRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->refs.aTags = aNew;
  memset(&aNew[cs->refs.nTags], 0, sizeof(struct TagRef));
  aNew[cs->refs.nTags].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->refs.nTags].zName ) return SQLITE_NOMEM;
  memcpy(&aNew[cs->refs.nTags].commitHash, pCommit, sizeof(ProllyHash));
  aNew[cs->refs.nTags].zTagger  = sqlite3_mprintf("%s", zTagger  ? zTagger  : "");
  aNew[cs->refs.nTags].zEmail   = sqlite3_mprintf("%s", zEmail   ? zEmail   : "");
  aNew[cs->refs.nTags].zMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
  if( !aNew[cs->refs.nTags].zTagger || !aNew[cs->refs.nTags].zEmail || !aNew[cs->refs.nTags].zMessage ){
    sqlite3_free(aNew[cs->refs.nTags].zName);
    sqlite3_free(aNew[cs->refs.nTags].zTagger);
    sqlite3_free(aNew[cs->refs.nTags].zEmail);
    sqlite3_free(aNew[cs->refs.nTags].zMessage);
    memset(&aNew[cs->refs.nTags], 0, sizeof(struct TagRef));
    return SQLITE_NOMEM;
  }
  aNew[cs->refs.nTags].timestamp = timestamp;
  cs->refs.nTags++;
  return SQLITE_OK;
}

int chunkStoreDeleteTag(ChunkStore *cs, const char *zName){
  int i;
  for(i=0; i<cs->refs.nTags; i++){
    if( strcmp(cs->refs.aTags[i].zName, zName)==0 ){
      sqlite3_free(cs->refs.aTags[i].zName);
      cs->refs.aTags[i] = cs->refs.aTags[cs->refs.nTags-1];
      cs->refs.nTags--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl){
  int i;
  for(i=0; i<cs->refs.nRemotes; i++){
    if( strcmp(cs->refs.aRemotes[i].zName, zName)==0 ){
      if( pzUrl ) *pzUrl = cs->refs.aRemotes[i].zUrl;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl){
  struct RemoteRef *aNew;
  if( chunkStoreFindRemote(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->refs.aRemotes, (cs->refs.nRemotes+1)*(int)sizeof(struct RemoteRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->refs.aRemotes = aNew;
  aNew[cs->refs.nRemotes].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->refs.nRemotes].zName ) return SQLITE_NOMEM;
  aNew[cs->refs.nRemotes].zUrl = sqlite3_mprintf("%s", zUrl);
  if( !aNew[cs->refs.nRemotes].zUrl ){
    sqlite3_free(aNew[cs->refs.nRemotes].zName);
    return SQLITE_NOMEM;
  }
  cs->refs.nRemotes++;
  return SQLITE_OK;
}

int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName){
  int i, j;
  for(i=0; i<cs->refs.nRemotes; i++){
    if( strcmp(cs->refs.aRemotes[i].zName, zName)==0 ){
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
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->refs.nTracking; i++){
    if( strcmp(cs->refs.aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->refs.aTracking[i].zBranch, zBranch)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->refs.aTracking[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->refs.nTracking; i++){
    if( strcmp(cs->refs.aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->refs.aTracking[i].zBranch, zBranch)==0 ){
      memcpy(&cs->refs.aTracking[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }

  {
    struct TrackingBranch *aNew;
    aNew = sqlite3_realloc(cs->refs.aTracking, (cs->refs.nTracking+1)*(int)sizeof(struct TrackingBranch));
    if( !aNew ) return SQLITE_NOMEM;
    cs->refs.aTracking = aNew;
    aNew[cs->refs.nTracking].zRemote = sqlite3_mprintf("%s", zRemote);
    if( !aNew[cs->refs.nTracking].zRemote ) return SQLITE_NOMEM;
    aNew[cs->refs.nTracking].zBranch = sqlite3_mprintf("%s", zBranch);
    if( !aNew[cs->refs.nTracking].zBranch ){
      sqlite3_free(aNew[cs->refs.nTracking].zRemote);
      return SQLITE_NOMEM;
    }
    memcpy(&aNew[cs->refs.nTracking].commitHash, pCommit, sizeof(ProllyHash));
    cs->refs.nTracking++;
  }
  return SQLITE_OK;
}

int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch){
  int i;
  for(i=0; i<cs->refs.nTracking; i++){
    if( strcmp(cs->refs.aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->refs.aTracking[i].zBranch, zBranch)==0 ){
      sqlite3_free(cs->refs.aTracking[i].zRemote);
      sqlite3_free(cs->refs.aTracking[i].zBranch);
      cs->refs.aTracking[i] = cs->refs.aTracking[cs->refs.nTracking-1];
      cs->refs.nTracking--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
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
  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  bufCur = buf;
  *bufCur++ = 6;
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
  *ppOut = buf;
  *pnOut = sz;
  return SQLITE_OK;
}

int chunkStoreSerializeRefs(ChunkStore *cs){
  int rc;
  u8 *buf = 0;
  int sz = 0;
  ProllyHash refsHash;

  rc = csSerializeRefsBlob(cs, &buf, &sz);
  if( rc!=SQLITE_OK ) return rc;
  rc = chunkStorePut(cs, buf, sz, &refsHash);
  sqlite3_free(buf);
  if( rc==SQLITE_OK ) memcpy(&cs->refs.refsHash, &refsHash, sizeof(ProllyHash));
  return rc;
}

static int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData){
  const u8 *bufCur = data;
  int defLen, nBranches, nTags, i;
  u8 version;
  if( nData<5 ) return SQLITE_CORRUPT;
  version = *bufCur++;
  if( version!=6 ) return SQLITE_CORRUPT;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  defLen = (int)CS_READ_U32(bufCur); bufCur+=4;
  if( bufCur+defLen>data+nData ) return SQLITE_CORRUPT;
  sqlite3_free(cs->refs.zDefaultBranch);
  cs->refs.zDefaultBranch = sqlite3_malloc(defLen+1);
  if(!cs->refs.zDefaultBranch) return SQLITE_NOMEM;
  memcpy(cs->refs.zDefaultBranch, bufCur, defLen); cs->refs.zDefaultBranch[defLen]=0; bufCur+=defLen;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  nBranches = (int)CS_READ_U32(bufCur); bufCur+=4;
  csFreeBranches(cs);
  if( nBranches>0 ){
    cs->refs.aBranches = sqlite3_malloc(nBranches*(int)sizeof(struct BranchRef));
    if(!cs->refs.aBranches) return SQLITE_NOMEM;
    for(i=0;i<nBranches;i++){
      int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
      nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
      if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
      memset(&cs->refs.aBranches[i], 0, sizeof(struct BranchRef));
      cs->refs.aBranches[i].zName=sqlite3_malloc(nameLen+1);
      if(!cs->refs.aBranches[i].zName) return SQLITE_NOMEM;
      memcpy(cs->refs.aBranches[i].zName,bufCur,nameLen); cs->refs.aBranches[i].zName[nameLen]=0; bufCur+=nameLen;
      memcpy(cs->refs.aBranches[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      if( bufCur+PROLLY_HASH_SIZE<=data+nData ){
        memcpy(cs->refs.aBranches[i].workingSetHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      }
      cs->refs.nBranches++;
    }
  }

  csFreeTags(cs);
  if( bufCur+4<=data+nData ){
    nTags = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nTags>0 ){
      cs->refs.aTags = sqlite3_malloc(nTags*(int)sizeof(struct TagRef));
      if(!cs->refs.aTags) return SQLITE_NOMEM;
      memset(cs->refs.aTags, 0, nTags*(int)sizeof(struct TagRef));
      for(i=0;i<nTags;i++){
        int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
        cs->refs.aTags[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->refs.aTags[i].zName) return SQLITE_NOMEM;
        memcpy(cs->refs.aTags[i].zName,bufCur,nameLen); cs->refs.aTags[i].zName[nameLen]=0; bufCur+=nameLen;
        memcpy(cs->refs.aTags[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
        if( version>=6 ){
          int taggerLen, emailLen, messageLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          taggerLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+taggerLen>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].zTagger=sqlite3_malloc(taggerLen+1);
          if(!cs->refs.aTags[i].zTagger) return SQLITE_NOMEM;
          memcpy(cs->refs.aTags[i].zTagger,bufCur,taggerLen); cs->refs.aTags[i].zTagger[taggerLen]=0; bufCur+=taggerLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          emailLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+emailLen>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].zEmail=sqlite3_malloc(emailLen+1);
          if(!cs->refs.aTags[i].zEmail) return SQLITE_NOMEM;
          memcpy(cs->refs.aTags[i].zEmail,bufCur,emailLen); cs->refs.aTags[i].zEmail[emailLen]=0; bufCur+=emailLen;
          if(bufCur+8>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].timestamp=CS_READ_I64(bufCur); bufCur+=8;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          messageLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+messageLen>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTags[i].zMessage=sqlite3_malloc(messageLen+1);
          if(!cs->refs.aTags[i].zMessage) return SQLITE_NOMEM;
          memcpy(cs->refs.aTags[i].zMessage,bufCur,messageLen); cs->refs.aTags[i].zMessage[messageLen]=0; bufCur+=messageLen;
        }else{
          cs->refs.aTags[i].zTagger  = sqlite3_mprintf("");
          cs->refs.aTags[i].zEmail   = sqlite3_mprintf("");
          cs->refs.aTags[i].zMessage = sqlite3_mprintf("");
          if( !cs->refs.aTags[i].zTagger || !cs->refs.aTags[i].zEmail || !cs->refs.aTags[i].zMessage ){
            return SQLITE_NOMEM;
          }
        }
        cs->refs.nTags++;
      }
    }
  }

  csFreeRemotes(cs);
  csFreeTracking(cs);
  if( bufCur+4<=data+nData ){
    int nRemotes = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nRemotes>0 ){
      cs->refs.aRemotes = sqlite3_malloc(nRemotes*(int)sizeof(struct RemoteRef));
      if(!cs->refs.aRemotes) return SQLITE_NOMEM;
      memset(cs->refs.aRemotes, 0, nRemotes*(int)sizeof(struct RemoteRef));
      for(i=0;i<nRemotes;i++){
        int nameLen, urlLen;
        if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+nameLen+4>data+nData) return SQLITE_CORRUPT;
        cs->refs.aRemotes[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->refs.aRemotes[i].zName) return SQLITE_NOMEM;
        memcpy(cs->refs.aRemotes[i].zName,bufCur,nameLen); cs->refs.aRemotes[i].zName[nameLen]=0; bufCur+=nameLen;
        urlLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+urlLen>data+nData) return SQLITE_CORRUPT;
        cs->refs.aRemotes[i].zUrl=sqlite3_malloc(urlLen+1);
        if(!cs->refs.aRemotes[i].zUrl) return SQLITE_NOMEM;
        memcpy(cs->refs.aRemotes[i].zUrl,bufCur,urlLen); cs->refs.aRemotes[i].zUrl[urlLen]=0; bufCur+=urlLen;
        cs->refs.nRemotes++;
      }
    }
    if( bufCur+4<=data+nData ){
      int nTracking = (int)CS_READ_U32(bufCur); bufCur+=4;
      if( nTracking>0 ){
        cs->refs.aTracking = sqlite3_malloc(nTracking*(int)sizeof(struct TrackingBranch));
        if(!cs->refs.aTracking) return SQLITE_NOMEM;
        memset(cs->refs.aTracking, 0, nTracking*(int)sizeof(struct TrackingBranch));
        for(i=0;i<nTracking;i++){
          int remoteLen, branchLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          remoteLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+remoteLen+4>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTracking[i].zRemote=sqlite3_malloc(remoteLen+1);
          if(!cs->refs.aTracking[i].zRemote) return SQLITE_NOMEM;
          memcpy(cs->refs.aTracking[i].zRemote,bufCur,remoteLen); cs->refs.aTracking[i].zRemote[remoteLen]=0; bufCur+=remoteLen;
          branchLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+branchLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
          cs->refs.aTracking[i].zBranch=sqlite3_malloc(branchLen+1);
          if(!cs->refs.aTracking[i].zBranch) return SQLITE_NOMEM;
          memcpy(cs->refs.aTracking[i].zBranch,bufCur,branchLen); cs->refs.aTracking[i].zBranch[branchLen]=0; bufCur+=branchLen;
          memcpy(cs->refs.aTracking[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
          cs->refs.nTracking++;
        }
      }
    }
  }

  return SQLITE_OK;
}

static int csDeserializeRefsIntoTemp(ChunkStore *pTmp, const u8 *data, int nData){
  memset(pTmp, 0, sizeof(*pTmp));
  return csDeserializeRefs(pTmp, data, nData);
}

void csAdoptRefsState(ChunkStore *pDst, ChunkStore *pSrc){
  pDst->refs.aBranches = pSrc->refs.aBranches;
  pDst->refs.nBranches = pSrc->refs.nBranches;
  pDst->refs.zDefaultBranch = pSrc->refs.zDefaultBranch;
  pDst->refs.aTags = pSrc->refs.aTags;
  pDst->refs.nTags = pSrc->refs.nTags;
  pDst->refs.aRemotes = pSrc->refs.aRemotes;
  pDst->refs.nRemotes = pSrc->refs.nRemotes;
  pDst->refs.aTracking = pSrc->refs.aTracking;
  pDst->refs.nTracking = pSrc->refs.nTracking;

  pSrc->refs.aBranches = 0;
  pSrc->refs.nBranches = 0;
  pSrc->refs.zDefaultBranch = 0;
  pSrc->refs.aTags = 0;
  pSrc->refs.nTags = 0;
  pSrc->refs.aRemotes = 0;
  pSrc->refs.nRemotes = 0;
  pSrc->refs.aTracking = 0;
  pSrc->refs.nTracking = 0;
}

int csReplaceRefsStateFromBlob(
  ChunkStore *cs,
  const u8 *data,
  int nData,
  int markCommitted
){
  ChunkStore tmp;
  int rc = csDeserializeRefsIntoTemp(&tmp, data, nData);
  if( rc!=SQLITE_OK ){
    csFreeRefsState(&tmp);
    return rc;
  }
  csFreeRefsState(cs);
  csAdoptRefsState(cs, &tmp);
  if( markCommitted ){
    csMarkRefsCommitted(cs);
  }
  return SQLITE_OK;
}

int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData){
  return csReplaceRefsStateFromBlob(cs, data, nData, 1);
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

  rc = csSearchPending(cs, hash, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx >= 0 ){
    ChunkIndexEntry *e = &cs->staging.aPending[idx];
    i64 off = e->offset;
    int sz = e->size;
    u8 *pCopy = (u8 *)sqlite3_malloc(sz);
    if( pCopy == 0 ) return SQLITE_NOMEM;

    memcpy(pCopy, cs->staging.pWriteBuf + off + 4, sz);
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
      u8 lenBuf[4];
      u8 *pBuf;
      u32 storedLen;

      rc = sqlite3OsRead(cs->file.pFile, lenBuf, 4, fileOff);
      if( rc != SQLITE_OK ) return rc;

      storedLen = CS_READ_U32(lenBuf);
      if( (int)storedLen != sz ){
        return SQLITE_CORRUPT;
      }

      pBuf = (u8 *)sqlite3_malloc(sz);
      if( pBuf == 0 ) return SQLITE_NOMEM;

      rc = sqlite3OsRead(cs->file.pFile, pBuf, sz, fileOff + 4);
      if( rc != SQLITE_OK ){
        sqlite3_free(pBuf);
        return rc;
      }

      *ppData = pBuf;
      *pnData = sz;
    }
  }

verify:
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

int chunkStorePut(
  ChunkStore *cs,
  const u8 *pData,
  int nData,
  ProllyHash *pHash
){
  int rc;
  int idx = -1;
  ProllyHash h;

  prollyHashCompute(pData, nData, &h);
  if( pHash ) memcpy(pHash, &h, sizeof(ProllyHash));

  if( csSearchIndex(cs->index.aIndex, cs->index.nIndex, &h) >= 0 ) return SQLITE_OK;
  rc = csSearchRecent(cs, &h, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx >= 0 ) return SQLITE_OK;
  idx = -1;
  rc = csSearchPending(cs, &h, &idx);
  if( rc!=SQLITE_OK ) return rc;
  if( idx >= 0 ) return SQLITE_OK;

  rc = csGrowPending(cs);
  if( rc != SQLITE_OK ) return rc;

  rc = csGrowWriteBuf(cs, 4 + nData);
  if( rc != SQLITE_OK ) return rc;

  {
    ChunkIndexEntry *e = &cs->staging.aPending[cs->staging.nPending];
    e->hash = h;
    e->offset = (i64)cs->staging.nWriteBuf;
    e->size = nData;
    cs->staging.nPending++;
  }

  CS_WRITE_U32(cs->staging.pWriteBuf + cs->staging.nWriteBuf, (u32)nData);
  cs->staging.nWriteBuf += 4;
  memcpy(cs->staging.pWriteBuf + cs->staging.nWriteBuf, pData, nData);
  cs->staging.nWriteBuf += nData;

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
    csPendHTClear(cs);
    cs->staging.nCommittedWriteBuf = cs->staging.nWriteBuf;
  }
  csMarkRefsCommitted(cs);
  return SQLITE_OK;
}

static int csCommitToFile(ChunkStore *cs){
  int rc;
  int i;
  i64 fileSize = 0;
  i64 origFileSize = 0;
  i64 writeOff = 0;
  int lockFd = -1;
  int hadFile = (cs->file.pFile != 0);
  int lockHeld = (cs->graphLockFd >= 0);
  i64 newWalSize = cs->wal.nWalData;
  ChunkIndexEntry *aCommittedPending = 0;
  ChunkIndexEntry *aMergePending = 0;
  ChunkIndexEntry *aMerged = 0;
  int nMerged = 0;
  int useRecent = 0;
  int crashWriteActive = csCrashWriteInjectionActive();

  if( cs->file.pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->file.pVfs, cs->file.zFilename, &cs->file.pFile, openFlags);
    if( rc != SQLITE_OK ) return SQLITE_CANTOPEN;
  }

  if( lockHeld ){
    lockFd = -1;
  }else{
    if( csFileLock(cs->file.zFilename, &lockFd) != 0 ){
      return SQLITE_BUSY;
    }
  }

  rc = cs->file.pFile->pMethods->xFileSize(cs->file.pFile, &fileSize);
  if( rc != SQLITE_OK ) goto commit_done;

  if( hadFile && !cs->hasMovedChecked ){
    int bMoved = 0;
    int rc2 = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED,
                                   &bMoved);
    if( rc2==SQLITE_OK && bMoved ){
      rc = csReloadFromDiskPreservingLocalRefs(cs);
      if( rc != SQLITE_OK ) goto commit_done;
      fileSize = cs->file.iFileSize;
    }
    cs->hasMovedChecked = 1;
  }

  if( fileSize > cs->file.iFileSize && hadFile ){
    rc = csReloadFromDiskPreservingLocalRefs(cs);
    if( rc != SQLITE_OK ) goto commit_done;
    fileSize = cs->file.iFileSize;
  }
  origFileSize = fileSize;

  if( cs->staging.nPending > 0 ){
    ChunkStore mergeView;
    i64 filePos = fileSize > 0 ? fileSize : (i64)CHUNK_MANIFEST_SIZE;
    i64 appendBytes = 0;

    for( i = 0; i < cs->staging.nPending; i++ ){
      i64 recBytes = (i64)25 + (i64)cs->staging.aPending[i].size;
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
    newWalSize = cs->wal.nWalData + appendBytes;

    aCommittedPending = (ChunkIndexEntry*)sqlite3_malloc(
      cs->staging.nPending * (int)sizeof(ChunkIndexEntry)
    );
    if( !aCommittedPending ){
      rc = SQLITE_NOMEM;
      goto commit_done;
    }

    for( i = 0; i < cs->staging.nPending; i++ ){
      ChunkIndexEntry *pSrc = &cs->staging.aPending[i];
      aCommittedPending[i] = *pSrc;
      aCommittedPending[i].offset = filePos + 21;
      filePos += (i64)25 + (i64)pSrc->size;
    }

    useRecent = !crashWriteActive
             && cs->staging.nPending <= 32 && cs->staging.nRecent + cs->staging.nPending <= 8192;
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
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->file.pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc != SQLITE_OK ) goto commit_done;
    fileSize = CHUNK_MANIFEST_SIZE;
  }

  writeOff = fileSize;

  /* Append chunks before the root record. Recovery ignores appended data until
  ** it finds a later valid root record that points at the new manifest. */
  if( cs->staging.nPending > 0 ){
    i64 walBytes = 0;
    u8 *pWalBatch = 0;
    u8 aSmallWalBatch[4096];
    u8 *pOut = 0;
    for( i = 0; i < cs->staging.nPending; i++ ){
      walBytes += (i64)25 + (i64)cs->staging.aPending[i].size;
    }
    if( !crashWriteActive && walBytes <= 64*1024 ){
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
        pOut[0] = CS_WAL_TAG_CHUNK;
        memcpy(pOut + 1, &pe->hash, 20);
        CS_WRITE_U32(pOut + 21, (u32)pe->size);
        pOut += 25;
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
        u8 recHdr[25];
        i64 bufOff;
        recHdr[0] = CS_WAL_TAG_CHUNK;
        memcpy(recHdr + 1, &pe->hash, 20);
        CS_WRITE_U32(recHdr + 21, (u32)pe->size);

        bufOff = pe->offset + 4;
        CRASH_CHECK_WRITE();
        rc = sqlite3OsWrite(cs->file.pFile, recHdr, 25, writeOff);
        if( rc != SQLITE_OK ) goto commit_done;
        writeOff += 25;

        {
          const u8 *pSrc = cs->staging.pWriteBuf + bufOff;
          int remaining = pe->size;
          while( remaining > 0 && rc==SQLITE_OK ){
            int toWrite = remaining > 65536 ? 65536 : remaining;
            CRASH_CHECK_WRITE();
            rc = sqlite3OsWrite(cs->file.pFile, pSrc, toWrite, writeOff);
            pSrc += toWrite;
            writeOff += toWrite;
            remaining -= toWrite;
          }
        }
        if( rc != SQLITE_OK ) goto commit_done;
      }
    }
  }

  /* The root record is the commit point for the append-only chunk store. */
  {
    u8 rootRec[1 + CHUNK_MANIFEST_SIZE];
    rootRec[0] = CS_WAL_TAG_ROOT;
    csSerializeManifest(cs, rootRec + 1);
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->file.pFile, rootRec, sizeof(rootRec), writeOff);
    if( rc != SQLITE_OK ) goto commit_done;
    writeOff += sizeof(rootRec);
  }

  CRASH_CHECK_WRITE();
  rc = sqlite3OsSync(cs->file.pFile, SQLITE_SYNC_NORMAL);

#ifdef SQLITE_TEST
  }
#undef CRASH_CHECK_WRITE
#endif
  if( rc != SQLITE_OK ) goto commit_done;

  cs->file.iFileSize = writeOff;

commit_done:
  csFileUnlock(lockFd);

  if( rc != SQLITE_OK ){
    if( cs->file.pFile && writeOff > origFileSize ){
      (void)csRollbackFailedAppend(cs, origFileSize);
    }
    (void)csRestoreCommittedRefsState(cs);
    sqlite3_free(aCommittedPending);
    sqlite3_free(aMergePending);
    sqlite3_free(aMerged);
    return rc;
  }

  if( cs->staging.nPending > 0 ){
    if( useRecent ){
      memcpy(cs->staging.aRecent + cs->staging.nRecent, aCommittedPending,
             cs->staging.nPending * sizeof(ChunkIndexEntry));
      cs->staging.nRecent += cs->staging.nPending;
      sqlite3_free(aMerged);
    }else{
      csReleaseIndexBuf(cs->index.aIndex, cs->index.aIndexMmapBase, cs->index.aIndexMmapSize);
      cs->index.aIndex = aMerged;
      cs->index.nIndex = nMerged;
      cs->index.aIndexMmapBase = 0;
      cs->index.aIndexMmapSize = 0;
      cs->staging.nRecent = 0;
      csRecentHTClear(cs);
    }
    cs->wal.nWalData = newWalSize;
  }else{
    sqlite3_free(aMerged);
  }
  sqlite3_free(aCommittedPending);
  sqlite3_free(aMergePending);

  sqlite3_free(cs->staging.pWriteBuf);
  cs->staging.pWriteBuf = 0;
  cs->staging.nWriteBuf = 0;
  cs->staging.nWriteBufAlloc = 0;
  cs->staging.nPending = 0;
  csPendHTClear(cs);
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
  if( cs->readOnly ) return SQLITE_READONLY;
  if( cs->isMemory ) return csCommitToMemory(cs);
  if( cs->graphLockFd < 0 && cs->file.zFilename ){
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
  csPendHTClear(cs);
  if( cs->isMemory ){

    cs->staging.nWriteBuf = cs->staging.nCommittedWriteBuf;
  }else{
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

int chunkStoreLockAndRefresh(ChunkStore *cs){
  int changed = 0;
  int rc;
  if( cs->isMemory ) return SQLITE_OK;
  if( cs->graphLockFd >= 0 ) return SQLITE_OK;
  if( !cs->file.zFilename ) return SQLITE_ERROR;
  if( csFileLockNB(cs->file.zFilename, &cs->graphLockFd) != 0 ){
    return SQLITE_BUSY;
  }
  rc = chunkStoreRefreshIfChanged(cs, &changed);
  if( rc!=SQLITE_OK ){
    csFileUnlock(cs->graphLockFd);
    cs->graphLockFd = -1;
  }
  return rc;
}

void chunkStoreUnlock(ChunkStore *cs){
  if( cs->graphLockFd >= 0 ){
    csFileUnlock(cs->graphLockFd);
    cs->graphLockFd = -1;
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
      struct stat mainStat;
      if( stat(cs->file.zFilename, &mainStat)==0 ){
        if( mainStat.st_size > 0 ){
          *pChanged = 1;
        }
      }else{
        return SQLITE_IOERR;
      }
    }
    return SQLITE_OK;
  }

  if( !cs->hasMovedChecked ){
    rc = sqlite3OsFileControl(cs->file.pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
    if( rc!=SQLITE_OK ) return rc;
    if( bMoved ){
      *pChanged = 1;
      return SQLITE_OK;
    }
    cs->hasMovedChecked = 1;
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

static int csReloadFromDisk(ChunkStore *cs){
  ChunkStore tmp;
  ChunkStoreReloadState saved;
  int rc = chunkStoreOpen(&tmp, cs->file.pVfs, cs->file.zFilename,
                          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;

  csCaptureReloadState(cs, &saved);
  csAdoptOpenedStoreState(cs, &tmp);
  chunkStoreClose(&tmp);

  cs->hasMovedChecked = 0;

  csFreeReloadState(&saved);
  return SQLITE_OK;
}

#endif
