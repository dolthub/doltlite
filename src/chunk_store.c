

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
static void csCloseFile(sqlite3_file *pFile);
static int csRollbackFailedAppend(ChunkStore *cs, i64 origFileSize);
static int csRestoreCommittedRefsState(ChunkStore *cs);
static int csReadManifest(ChunkStore *cs);
static int csReadIndex(ChunkStore *cs);
static int csDeserializeRefs(ChunkStore *cs, const u8 *data, int nData);
static int csSearchIndex(const ChunkIndexEntry *aIdx, int nIdx,
                         const ProllyHash *pHash);
static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash, int *pIdx);
static int csIndexEntryCmp(const void *a, const void *b);
void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
static void csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e);
static int csMergeIndex(ChunkStore *cs, ChunkIndexEntry **ppMerged,
                        int *pnMerged);
static int csGrowPending(ChunkStore *cs);
static int csGrowRecent(ChunkStore *cs, int nAdd);
static int csGrowWriteBuf(ChunkStore *cs, int nNeeded);
static void csPendHTClear(ChunkStore *cs);
static void csRecentHTClear(ChunkStore *cs);

static int csReplayWal(ChunkStore *cs);
static void csFreeRefsState(ChunkStore *cs);
static int csDeserializeRefsIntoTemp(ChunkStore *pTmp, const u8 *data, int nData);
static void csAdoptRefsState(ChunkStore *pDst, ChunkStore *pSrc);
static int csReplaceRefsStateFromBlob(ChunkStore *cs, const u8 *data, int nData,
                                      int markCommitted);
static int csEnsureDefaultBranch(ChunkStore *cs);
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

typedef struct SavedRefsState SavedRefsState;
typedef struct ChunkStoreReplayState ChunkStoreReplayState;
typedef struct ChunkStoreReloadState ChunkStoreReloadState;

struct SavedRefsState {
  char *zDefaultBranch;
  struct BranchRef *aBranches;
  int nBranches;
  struct TagRef *aTags;
  int nTags;
  struct RemoteRef *aRemotes;
  int nRemotes;
  struct TrackingBranch *aTracking;
  int nTracking;
};

struct ChunkStoreReplayState {
  ChunkIndexEntry *aIndex;
  int nIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  SavedRefsState refs;
};

struct ChunkStoreReloadState {
  sqlite3_file *pFile;
  ChunkIndexEntry *aIndex;
  void *aIndexMmapBase;
  i64 aIndexMmapSize;
  SavedRefsState refs;
};

#if CHUNK_STORE_LE_PACKING
typedef char chunk_index_entry_size_check[
  (sizeof(ChunkIndexEntry) == CHUNK_INDEX_ENTRY_SIZE) ? 1 : -1
];
#endif

#if CHUNK_STORE_LE_PACKING
#  if defined(_WIN32)
#    include <windows.h>
static int csMapIndex(const char *zPath, i64 offset, i64 nBytes,
                      void **ppMapBase, i64 *pnMapSize,
                      const u8 **ppData){
  HANDLE hFile = CreateFileA(zPath, GENERIC_READ, FILE_SHARE_READ,
                              NULL, OPEN_EXISTING,
                              FILE_ATTRIBUTE_NORMAL, NULL);
  HANDLE hMap;
  void *pMap;
  SYSTEM_INFO si;
  i64 alignOffset, alignPad, mapSize;

  if( hFile==INVALID_HANDLE_VALUE ) return SQLITE_CANTOPEN;

  GetSystemInfo(&si);
  alignOffset = (offset / si.dwAllocationGranularity)
              * si.dwAllocationGranularity;
  alignPad = offset - alignOffset;
  mapSize = nBytes + alignPad;

  hMap = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
  CloseHandle(hFile);
  if( hMap==NULL ) return SQLITE_IOERR;

  pMap = MapViewOfFile(hMap, FILE_MAP_READ,
                        (DWORD)(alignOffset >> 32),
                        (DWORD)(alignOffset & 0xFFFFFFFF),
                        (SIZE_T)mapSize);
  CloseHandle(hMap);
  if( pMap==NULL ) return SQLITE_IOERR;

  *ppMapBase = pMap;
  *pnMapSize = mapSize;
  *ppData = (const u8 *)pMap + alignPad;
  return SQLITE_OK;
}

static void csUnmapIndex(void *pMapBase, i64 nMapSize){
  (void)nMapSize;
  if( pMapBase ) UnmapViewOfFile(pMapBase);
}
#  else
#    include <sys/mman.h>
#    include <fcntl.h>
#    include <unistd.h>
static int csMapIndex(const char *zPath, i64 offset, i64 nBytes,
                      void **ppMapBase, i64 *pnMapSize,
                      const u8 **ppData){
  int fd;
  long pageSize;
  i64 alignOffset, alignPad, mapSize;
  void *pMap;

  fd = open(zPath, O_RDONLY);
  if( fd < 0 ) return SQLITE_CANTOPEN;

  pageSize = sysconf(_SC_PAGESIZE);
  if( pageSize <= 0 ) pageSize = 4096;

  alignOffset = (offset / pageSize) * pageSize;
  alignPad = offset - alignOffset;
  mapSize = nBytes + alignPad;

  pMap = mmap(NULL, (size_t)mapSize, PROT_READ, MAP_PRIVATE, fd,
              (off_t)alignOffset);
  close(fd);
  if( pMap == MAP_FAILED ) return SQLITE_IOERR;

  *ppMapBase = pMap;
  *pnMapSize = mapSize;
  *ppData = (const u8 *)pMap + alignPad;
  return SQLITE_OK;
}

static void csUnmapIndex(void *pMapBase, i64 nMapSize){
  if( pMapBase ) munmap(pMapBase, (size_t)nMapSize);
}
#  endif
#else
static int csMapIndex(const char *zPath, i64 offset, i64 nBytes,
                      void **ppMapBase, i64 *pnMapSize,
                      const u8 **ppData){
  (void)zPath; (void)offset; (void)nBytes;
  (void)ppMapBase; (void)pnMapSize; (void)ppData;
  return SQLITE_NOTFOUND;
}
static void csUnmapIndex(void *pMapBase, i64 nMapSize){
  (void)pMapBase; (void)nMapSize;
}
#endif

static void csReleaseIndexBuf(ChunkIndexEntry *aIndex,
                              void *mmapBase, i64 mmapSize){
  if( mmapBase ){
    csUnmapIndex(mmapBase, mmapSize);
  }else{
    sqlite3_free(aIndex);
  }
}

static void csFreeBranches(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nBranches; k++) sqlite3_free(cs->aBranches[k].zName);
  sqlite3_free(cs->aBranches);
  cs->aBranches = 0;
  cs->nBranches = 0;
}
static void csFreeTags(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nTags; k++){
    sqlite3_free(cs->aTags[k].zName);
    sqlite3_free(cs->aTags[k].zTagger);
    sqlite3_free(cs->aTags[k].zEmail);
    sqlite3_free(cs->aTags[k].zMessage);
  }
  sqlite3_free(cs->aTags);
  cs->aTags = 0;
  cs->nTags = 0;
}
static void csFreeRemotes(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nRemotes; k++){
    sqlite3_free(cs->aRemotes[k].zName);
    sqlite3_free(cs->aRemotes[k].zUrl);
  }
  sqlite3_free(cs->aRemotes);
  cs->aRemotes = 0;
  cs->nRemotes = 0;
}
static void csFreeTracking(ChunkStore *cs){
  int k;
  for(k=0; k<cs->nTracking; k++){
    sqlite3_free(cs->aTracking[k].zRemote);
    sqlite3_free(cs->aTracking[k].zBranch);
  }
  sqlite3_free(cs->aTracking);
  cs->aTracking = 0;
  cs->nTracking = 0;
}

static void csMarkRefsCommitted(ChunkStore *cs){
  cs->committedRefsHash = cs->refsHash;
}

static void csRestoreCommittedRefsHash(ChunkStore *cs){
  cs->refsHash = cs->committedRefsHash;
}

static void csCaptureSavedRefsState(ChunkStore *cs, SavedRefsState *pSaved){
  memset(pSaved, 0, sizeof(*pSaved));
  pSaved->zDefaultBranch = cs->zDefaultBranch;
  pSaved->aBranches = cs->aBranches;
  pSaved->nBranches = cs->nBranches;
  pSaved->aTags = cs->aTags;
  pSaved->nTags = cs->nTags;
  pSaved->aRemotes = cs->aRemotes;
  pSaved->nRemotes = cs->nRemotes;
  pSaved->aTracking = cs->aTracking;
  pSaved->nTracking = cs->nTracking;
}

static void csRestoreSavedRefsState(ChunkStore *cs, const SavedRefsState *pSaved){
  cs->zDefaultBranch = pSaved->zDefaultBranch;
  cs->aBranches = pSaved->aBranches;
  cs->nBranches = pSaved->nBranches;
  cs->aTags = pSaved->aTags;
  cs->nTags = pSaved->nTags;
  cs->aRemotes = pSaved->aRemotes;
  cs->nRemotes = pSaved->nRemotes;
  cs->aTracking = pSaved->aTracking;
  cs->nTracking = pSaved->nTracking;
}

static void csFreeSavedRefsState(SavedRefsState *pSaved){
  ChunkStore refsStore;
  memset(&refsStore, 0, sizeof(refsStore));
  refsStore.zDefaultBranch = pSaved->zDefaultBranch;
  refsStore.aBranches = pSaved->aBranches;
  refsStore.nBranches = pSaved->nBranches;
  refsStore.aTags = pSaved->aTags;
  refsStore.nTags = pSaved->nTags;
  refsStore.aRemotes = pSaved->aRemotes;
  refsStore.nRemotes = pSaved->nRemotes;
  refsStore.aTracking = pSaved->aTracking;
  refsStore.nTracking = pSaved->nTracking;
  csFreeRefsState(&refsStore);
  memset(pSaved, 0, sizeof(*pSaved));
}

static void csCaptureReplayState(ChunkStore *cs, ChunkStoreReplayState *pSaved){
  memset(pSaved, 0, sizeof(*pSaved));
  pSaved->aIndex = cs->aIndex;
  pSaved->nIndex = cs->nIndex;
  pSaved->aIndexMmapBase = cs->aIndexMmapBase;
  pSaved->aIndexMmapSize = cs->aIndexMmapSize;
  csCaptureSavedRefsState(cs, &pSaved->refs);
}

static void csRestoreReplayState(ChunkStore *cs, const ChunkStoreReplayState *pSaved){
  cs->aIndex = pSaved->aIndex;
  cs->nIndex = pSaved->nIndex;
  cs->aIndexMmapBase = pSaved->aIndexMmapBase;
  cs->aIndexMmapSize = pSaved->aIndexMmapSize;
  csRestoreSavedRefsState(cs, &pSaved->refs);
}

static void csCaptureReloadState(ChunkStore *cs, ChunkStoreReloadState *pSaved){
  memset(pSaved, 0, sizeof(*pSaved));
  pSaved->pFile = cs->pFile;
  pSaved->aIndex = cs->aIndex;
  pSaved->aIndexMmapBase = cs->aIndexMmapBase;
  pSaved->aIndexMmapSize = cs->aIndexMmapSize;
  csCaptureSavedRefsState(cs, &pSaved->refs);
}

static void csReleaseReplayState(
  ChunkStore *cs,
  ChunkStoreReplayState *pSaved
){
  if( cs->aIndex!=pSaved->aIndex ){
    csReleaseIndexBuf(pSaved->aIndex, pSaved->aIndexMmapBase,
                       pSaved->aIndexMmapSize);
  }
  csFreeSavedRefsState(&pSaved->refs);
  memset(pSaved, 0, sizeof(*pSaved));
}

static void csRollbackReplayState(
  ChunkStore *cs,
  ChunkStoreReplayState *pSaved,
  int nPendingBefore
){
  if( cs->aIndex!=pSaved->aIndex ){
    csReleaseIndexBuf(cs->aIndex, cs->aIndexMmapBase, cs->aIndexMmapSize);
  }
  csRestoreReplayState(cs, pSaved);
  cs->nPending = nPendingBefore;
  csPendHTClear(cs);
}

static void csAdoptOpenedStoreState(ChunkStore *pDst, ChunkStore *pSrc){
  sqlite3_free(pDst->aRecent);
  pDst->aRecent = 0;
  pDst->nRecent = 0;
  pDst->nRecentAlloc = 0;
  csRecentHTClear(pDst);

  pDst->pFile = pSrc->pFile;
  pDst->readOnly = pSrc->readOnly;
  pDst->refsHash = pSrc->refsHash;
  pDst->committedRefsHash = pSrc->committedRefsHash;
  pDst->nChunks = pSrc->nChunks;
  pDst->iIndexOffset = pSrc->iIndexOffset;
  pDst->nIndexSize = pSrc->nIndexSize;
  pDst->iWalOffset = pSrc->iWalOffset;
  pDst->iFileSize = pSrc->iFileSize;
  pDst->aIndex = pSrc->aIndex;
  pDst->nIndex = pSrc->nIndex;
  pDst->aIndexMmapBase = pSrc->aIndexMmapBase;
  pDst->aIndexMmapSize = pSrc->aIndexMmapSize;
  pDst->nWalData = pSrc->nWalData;
  pDst->aBranches = pSrc->aBranches;
  pDst->nBranches = pSrc->nBranches;
  pDst->zDefaultBranch = pSrc->zDefaultBranch;
  pDst->aTags = pSrc->aTags;
  pDst->nTags = pSrc->nTags;
  pDst->aRemotes = pSrc->aRemotes;
  pDst->nRemotes = pSrc->nRemotes;
  pDst->aTracking = pSrc->aTracking;
  pDst->nTracking = pSrc->nTracking;

  pSrc->pFile = 0;
  pSrc->aIndex = 0;
  pSrc->nIndex = 0;
  pSrc->aIndexMmapBase = 0;
  pSrc->aIndexMmapSize = 0;
  pSrc->nWalData = 0;
  pSrc->aBranches = 0;
  pSrc->nBranches = 0;
  pSrc->zDefaultBranch = 0;
  pSrc->aTags = 0;
  pSrc->nTags = 0;
  pSrc->aRemotes = 0;
  pSrc->nRemotes = 0;
  pSrc->aTracking = 0;
  pSrc->nTracking = 0;
}

static void csFreeReloadState(ChunkStoreReloadState *pSaved){
  csCloseFile(pSaved->pFile);
  csReleaseIndexBuf(pSaved->aIndex, pSaved->aIndexMmapBase,
                     pSaved->aIndexMmapSize);
  csFreeSavedRefsState(&pSaved->refs);
  memset(pSaved, 0, sizeof(*pSaved));
}
static void csFreeRefsState(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = 0;
}

static int csEnsureDefaultBranch(ChunkStore *cs){
  if( !cs->zDefaultBranch ){
    cs->zDefaultBranch = sqlite3_mprintf("main");
    if( !cs->zDefaultBranch ) return SQLITE_NOMEM;
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

static void csCloseFile(sqlite3_file *pFile){
  if( pFile ){
    sqlite3OsCloseFree(pFile);
  }
}

static int csRollbackFailedAppend(ChunkStore *cs, i64 origFileSize){
  sqlite3_int64 sizeNow = -1;
  int rc = SQLITE_OK;

  if( !cs->pFile ) return SQLITE_IOERR;

  rc = sqlite3OsTruncate(cs->pFile, origFileSize);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsFileSize(cs->pFile, &sizeNow);
  }
  if( rc==SQLITE_OK && sizeNow==origFileSize ){
    (void)sqlite3OsSync(cs->pFile, SQLITE_SYNC_NORMAL);
    return SQLITE_OK;
  }

  csCloseFile(cs->pFile);
  cs->pFile = 0;
  rc = csOpenFile(cs->pVfs, cs->zFilename, &cs->pFile,
                  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3OsTruncate(cs->pFile, origFileSize);
  if( rc==SQLITE_OK ){
    rc = sqlite3OsFileSize(cs->pFile, &sizeNow);
  }
  if( rc==SQLITE_OK && sizeNow==origFileSize ){
    (void)sqlite3OsSync(cs->pFile, SQLITE_SYNC_NORMAL);
    return SQLITE_OK;
  }
  return rc==SQLITE_OK ? SQLITE_IOERR_TRUNCATE : rc;
}

static int csRestoreCommittedRefsState(ChunkStore *cs){
  csRestoreCommittedRefsHash(cs);
  if( prollyHashIsEmpty(&cs->committedRefsHash) ){
    csFreeBranches(cs);
    csFreeTags(cs);
    csFreeRemotes(cs);
    csFreeTracking(cs);
    return csEnsureDefaultBranch(cs);
  }
  return chunkStoreReloadRefs(cs);
}

static int csSearchIndex(
  const ChunkIndexEntry *aIdx,
  int nIdx,
  const ProllyHash *pHash
){
  int lo = 0;
  int hi = nIdx - 1;
  while( lo <= hi ){
    int mid = lo + (hi - lo) / 2;
    int cmp = prollyHashCompare(&aIdx[mid].hash, pHash);
    if( cmp == 0 ) return mid;
    if( cmp < 0 ){
      lo = mid + 1;
    }else{
      hi = mid - 1;
    }
  }
  return -1;
}

#define CS_PEND_HT_INIT_BITS 12
#define CS_PEND_HT_MAX_LOAD  4

static u32 csPendBucket(const ProllyHash *h, int nHTMask){
  return ((u32)h->data[0] | ((u32)h->data[1]<<8)
        | ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24)) & (u32)nHTMask;
}

static void csPendHTClear(ChunkStore *cs){
  sqlite3_free(cs->aPendingHT);
  sqlite3_free(cs->aPendingHTNext);
  cs->aPendingHT = 0;
  cs->aPendingHTNext = 0;
  cs->nPendingHTBuilt = 0;
  cs->nPendingHTSize = 0;
}

static void csRecentHTClear(ChunkStore *cs){
  sqlite3_free(cs->aRecentHT);
  sqlite3_free(cs->aRecentHTNext);
  cs->aRecentHT = 0;
  cs->aRecentHTNext = 0;
  cs->nRecentHTBuilt = 0;
  cs->nRecentHTSize = 0;
  cs->nRecentHTNextAlloc = 0;
}

static int csPendHTRebuild(ChunkStore *cs){
  int i;
  memset(cs->aPendingHT, 0xff, cs->nPendingHTSize * sizeof(int));
  for(i=0; i<cs->nPending; i++){
    u32 b = csPendBucket(&cs->aPending[i].hash, cs->nPendingHTSize - 1);
    cs->aPendingHTNext[i] = cs->aPendingHT[b];
    cs->aPendingHT[b] = i;
  }
  cs->nPendingHTBuilt = cs->nPending;
  return SQLITE_OK;
}

static int csPendHTEnsure(ChunkStore *cs){
  int i;
  if( cs->nPending==0 ) return SQLITE_OK;
  if( !cs->aPendingHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->aPendingHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->aPendingHT ) return SQLITE_NOMEM;
    memset(cs->aPendingHT, 0xff, initSize * sizeof(int));
    cs->nPendingHTSize = initSize;
    cs->nPendingHTBuilt = 0;
  }
  if( cs->nPending > cs->nPendingHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->nPendingHTSize * 4;
    int *aNew = sqlite3_realloc(cs->aPendingHT, newSize * (int)sizeof(int));
    if( aNew ){
      cs->aPendingHT = aNew;
      cs->nPendingHTSize = newSize;
      if( !cs->aPendingHTNext || cs->nPendingAlloc > cs->nPendingHTNextAlloc ){
        int nAlloc = cs->nPendingAlloc > 0 ? cs->nPendingAlloc : 64;
        int *aNew2 = sqlite3_realloc(cs->aPendingHTNext, nAlloc*(int)sizeof(int));
        if( !aNew2 ) return SQLITE_NOMEM;
        cs->aPendingHTNext = aNew2;
        cs->nPendingHTNextAlloc = nAlloc;
      }
      return csPendHTRebuild(cs);
    }

  }
  if( !cs->aPendingHTNext || cs->nPendingAlloc > cs->nPendingHTNextAlloc ){
    int nAlloc = cs->nPendingAlloc > 0 ? cs->nPendingAlloc : 64;
    int *aNew = sqlite3_realloc(cs->aPendingHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aPendingHTNext = aNew;
    cs->nPendingHTNextAlloc = nAlloc;
  }

  for(i=cs->nPendingHTBuilt; i<cs->nPending; i++){
    u32 b = csPendBucket(&cs->aPending[i].hash, cs->nPendingHTSize - 1);
    cs->aPendingHTNext[i] = cs->aPendingHT[b];
    cs->aPendingHT[b] = i;
  }
  cs->nPendingHTBuilt = cs->nPending;
  return SQLITE_OK;
}

static int csSearchPending(ChunkStore *cs, const ProllyHash *pHash, int *pIdx){
  int i; u32 b; int rc;
  *pIdx = -1;
  if( cs->nPending==0 ) return SQLITE_OK;
  rc = csPendHTEnsure(cs);
  if( rc!=SQLITE_OK ){

    for(i=0; i<cs->nPending; i++){
      if( prollyHashCompare(&cs->aPending[i].hash, pHash)==0 ){
        *pIdx = i;
        return SQLITE_OK;
      }
    }
    return rc;
  }
  b = csPendBucket(pHash, cs->nPendingHTSize - 1);
  i = cs->aPendingHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->aPending[i].hash, pHash)==0 ){
      *pIdx = i;
      return SQLITE_OK;
    }
    i = cs->aPendingHTNext[i];
  }
  return SQLITE_OK;
}

static int csRecentHTEnsure(ChunkStore *cs){
  int i;
  if( cs->nRecent==0 ) return SQLITE_OK;
  if( !cs->aRecentHT ){
    int initSize = 1 << CS_PEND_HT_INIT_BITS;
    cs->aRecentHT = sqlite3_malloc(initSize * (int)sizeof(int));
    if( !cs->aRecentHT ) return SQLITE_NOMEM;
    memset(cs->aRecentHT, 0xff, initSize * sizeof(int));
    cs->nRecentHTSize = initSize;
    cs->nRecentHTBuilt = 0;
  }
  if( cs->nRecent > cs->nRecentHTSize * CS_PEND_HT_MAX_LOAD ){
    int newSize = cs->nRecentHTSize * 4;
    int *aNew = sqlite3_realloc(cs->aRecentHT, newSize * (int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aRecentHT = aNew;
    cs->nRecentHTSize = newSize;
    memset(cs->aRecentHT, 0xff, cs->nRecentHTSize * sizeof(int));
    cs->nRecentHTBuilt = 0;
  }
  if( !cs->aRecentHTNext || cs->nRecentAlloc > cs->nRecentHTNextAlloc ){
    int nAlloc = cs->nRecentAlloc > 0 ? cs->nRecentAlloc : 64;
    int *aNew = sqlite3_realloc(cs->aRecentHTNext, nAlloc*(int)sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aRecentHTNext = aNew;
    cs->nRecentHTNextAlloc = nAlloc;
  }
  for(i=cs->nRecentHTBuilt; i<cs->nRecent; i++){
    u32 b = csPendBucket(&cs->aRecent[i].hash, cs->nRecentHTSize - 1);
    cs->aRecentHTNext[i] = cs->aRecentHT[b];
    cs->aRecentHT[b] = i;
  }
  cs->nRecentHTBuilt = cs->nRecent;
  return SQLITE_OK;
}

static int csSearchRecent(ChunkStore *cs, const ProllyHash *pHash, int *pIdx){
  int i; u32 b; int rc;
  *pIdx = -1;
  if( cs->nRecent==0 ) return SQLITE_OK;
  rc = csRecentHTEnsure(cs);
  if( rc!=SQLITE_OK ){
    for(i=cs->nRecent-1; i>=0; i--){
      if( prollyHashCompare(&cs->aRecent[i].hash, pHash)==0 ){
        *pIdx = i;
        return SQLITE_OK;
      }
    }
    return rc;
  }
  b = csPendBucket(pHash, cs->nRecentHTSize - 1);
  i = cs->aRecentHT[b];
  while( i>=0 ){
    if( prollyHashCompare(&cs->aRecent[i].hash, pHash)==0 ){
      *pIdx = i;
      return SQLITE_OK;
    }
    i = cs->aRecentHTNext[i];
  }
  return SQLITE_OK;
}

void csSerializeManifest(const ChunkStore *cs, u8 *aBuf){
  memset(aBuf, 0, CHUNK_MANIFEST_SIZE);
  CS_WRITE_U32(aBuf + 0, CHUNK_STORE_MAGIC);
  CS_WRITE_U32(aBuf + 4, CHUNK_STORE_VERSION);

  CS_WRITE_U32(aBuf + 28, (u32)cs->nChunks);
  CS_WRITE_I64(aBuf + 32, cs->iIndexOffset);
  CS_WRITE_U32(aBuf + 40, (u32)cs->nIndexSize);

  CS_WRITE_I64(aBuf + 84, cs->iWalOffset);
  memcpy(aBuf + 104, cs->refsHash.data, PROLLY_HASH_SIZE);
}

static void csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e){
  memcpy(e->hash.data, aBuf, PROLLY_HASH_SIZE);
  e->offset = CS_READ_I64(aBuf + PROLLY_HASH_SIZE);
  e->size = (int)CS_READ_U32(aBuf + PROLLY_HASH_SIZE + 8);
}

static int csReadManifest(ChunkStore *cs){
  u8 aBuf[CHUNK_MANIFEST_SIZE];
  u32 magic, version;
  int rc;

  rc = sqlite3OsRead(cs->pFile, aBuf, CHUNK_MANIFEST_SIZE, 0);
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

  cs->nChunks = (int)CS_READ_U32(aBuf + 28);
  cs->iIndexOffset = CS_READ_I64(aBuf + 32);
  cs->nIndexSize = (i64)CS_READ_U32(aBuf + 40);

  cs->iWalOffset = CS_READ_I64(aBuf + 84);
  memcpy(cs->refsHash.data, aBuf + 104, PROLLY_HASH_SIZE);

  return SQLITE_OK;
}

static int csReadIndex(ChunkStore *cs){
  int rc;
  i64 nEntries64;
  int nEntries;
  u8 *aBuf;
  int i;
  void *pMapBase = 0;
  i64 nMapSize = 0;
  const u8 *pMapData = 0;

  if( cs->nIndexSize == 0 || cs->nChunks == 0 ){
    cs->nIndex = 0;
    return SQLITE_OK;
  }

  nEntries64 = cs->nIndexSize / CHUNK_INDEX_ENTRY_SIZE;
  if( nEntries64 * CHUNK_INDEX_ENTRY_SIZE != cs->nIndexSize ){
    return SQLITE_CORRUPT;
  }
  if( nEntries64 > INT_MAX ){
    return SQLITE_TOOBIG;
  }
  nEntries = (int)nEntries64;

  if( cs->zFilename
   && csMapIndex(cs->zFilename, cs->iIndexOffset, cs->nIndexSize,
                  &pMapBase, &nMapSize, &pMapData) == SQLITE_OK ){
    cs->aIndex = (ChunkIndexEntry *)pMapData;
    cs->nIndex = nEntries;
    cs->aIndexMmapBase = pMapBase;
    cs->aIndexMmapSize = nMapSize;
    return SQLITE_OK;
  }

  cs->aIndex = (ChunkIndexEntry *)sqlite3_malloc(
    nEntries * (int)sizeof(ChunkIndexEntry)
  );
  if( cs->aIndex == 0 ) return SQLITE_NOMEM;
  cs->nIndex = nEntries;
  cs->aIndexMmapBase = 0;
  cs->aIndexMmapSize = 0;

  aBuf = (u8 *)sqlite3_malloc64(cs->nIndexSize);
  if( aBuf == 0 ){
    sqlite3_free(cs->aIndex);
    cs->aIndex = 0;
    cs->nIndex = 0;
    return SQLITE_NOMEM;
  }

  rc = sqlite3OsRead(cs->pFile, aBuf, cs->nIndexSize, cs->iIndexOffset);
  if( rc != SQLITE_OK ){
    sqlite3_free(aBuf);
    sqlite3_free(cs->aIndex);
    cs->aIndex = 0;
    cs->nIndex = 0;
    return rc;
  }

  for( i = 0; i < nEntries; i++ ){
    csDeserializeIndexEntry(aBuf + i * CHUNK_INDEX_ENTRY_SIZE, &cs->aIndex[i]);
  }

  sqlite3_free(aBuf);
  return SQLITE_OK;
}

static int csGrowPending(ChunkStore *cs){
  if( cs->nPending >= cs->nPendingAlloc ){
    int nNew = cs->nPendingAlloc ? cs->nPendingAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->aPending, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->aPending = aNew;
    cs->nPendingAlloc = nNew;
  }
  return SQLITE_OK;
}

static int csGrowRecent(ChunkStore *cs, int nAdd){
  int nNeed = cs->nRecent + nAdd;
  if( nNeed > cs->nRecentAlloc ){
    int nNew = cs->nRecentAlloc ? cs->nRecentAlloc * 2 : CS_INIT_PENDING_ALLOC;
    ChunkIndexEntry *aNew;
    while( nNew < nNeed ) nNew *= 2;
    aNew = (ChunkIndexEntry *)sqlite3_realloc(
      cs->aRecent, nNew * (int)sizeof(ChunkIndexEntry)
    );
    if( aNew == 0 ) return SQLITE_NOMEM;
    cs->aRecent = aNew;
    cs->nRecentAlloc = nNew;
  }
  return SQLITE_OK;
}

static int csGrowWriteBuf(ChunkStore *cs, int nNeeded){
  i64 nRequired = cs->nWriteBuf + (i64)nNeeded;
  if( nRequired > cs->nWriteBufAlloc ){
    i64 nNew = cs->nWriteBufAlloc ? cs->nWriteBufAlloc : CS_INIT_WRITEBUF_SIZE;
    u8 *pNew;

    while( nNew < nRequired ){
      if( nNew < 64*1024*1024 ){
        nNew *= 2;
      }else{
        nNew += nNew / 2;
      }
    }
    pNew = (u8 *)sqlite3_realloc64(cs->pWriteBuf, (sqlite3_uint64)nNew);
    if( pNew == 0 ) return SQLITE_NOMEM;
    cs->pWriteBuf = pNew;
    cs->nWriteBufAlloc = nNew;
  }
  return SQLITE_OK;
}

static int csReplayWal(ChunkStore *cs){
  i64 walSize;
  ChunkStoreReplayState saved;
  i64 pos;
  int nPendingBefore = cs->nPending;
  int nRootedPending = cs->nPending;
  int nRootRecordsSeen = 0;
  ChunkStore tmpRefs;
  int haveTmpRefs = 0;
  int rc = SQLITE_OK;

  memset(&tmpRefs, 0, sizeof(tmpRefs));

  csCaptureReplayState(cs, &saved);

  if( cs->iWalOffset <= 0 || !cs->pFile ) return SQLITE_OK;

  {
    i64 fileSize = 0;
    int rc = sqlite3OsFileSize(cs->pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
    walSize = fileSize - cs->iWalOffset;
    cs->iFileSize = fileSize;
  }
  if( walSize <= 0 ){
    if( cs->nIndex==0 && cs->nChunks==0
     && !prollyHashIsEmpty(&cs->refsHash) ){
      memset(cs->refsHash.data, 0, PROLLY_HASH_SIZE);
    }
    return SQLITE_OK;
  }

  cs->nWalData = walSize;

  pos = 0;
  while( pos < walSize ){
    u8 tag = 0;
    rc = sqlite3OsRead(cs->pFile, &tag, 1, cs->iWalOffset + pos);
    if( rc != SQLITE_OK ) goto replay_error;
    pos++;

    if( tag == CS_WAL_TAG_CHUNK ){
      u8 aHdr[24];
      ProllyHash hash;
      u32 len;
      if( pos + 20 + 4 > walSize ){
        break;
      }
      rc = sqlite3OsRead(cs->pFile, aHdr, sizeof(aHdr), cs->iWalOffset + pos);
      if( rc != SQLITE_OK ) goto replay_error;
      memcpy(&hash, aHdr, 20);
      len = CS_READ_U32(aHdr + 20);
      pos += 24;
      if( pos < 0 || (u64)pos + len > (u64)walSize ){
        break;
      }

      {
        int existing = csSearchIndex(cs->aIndex, cs->nIndex, &hash);
        ChunkIndexEntry *e = 0;
        if( existing < 0 ){
          rc = csGrowPending(cs);
          if( rc != SQLITE_OK ) goto replay_error;
          e = &cs->aPending[cs->nPending];
          memcpy(&e->hash, &hash, sizeof(ProllyHash));
          e->offset = cs->iWalOffset + (i64)(pos - 4);
          e->size = (int)len;
          cs->nPending++;
        }
      }
      pos += len;

    } else if( tag == CS_WAL_TAG_ROOT ){
      u8 m[CHUNK_MANIFEST_SIZE];
      if( pos + CHUNK_MANIFEST_SIZE > walSize ){
        break;
      }
      {
        u32 magic;
        rc = sqlite3OsRead(cs->pFile, m, sizeof(m), cs->iWalOffset + pos);
        if( rc != SQLITE_OK ) goto replay_error;
        magic = CS_READ_U32(m);
        if( magic != CHUNK_STORE_MAGIC ){
          break;
        }

        cs->nChunks = (int)CS_READ_U32(m + 28);

        memcpy(cs->refsHash.data, m + 104, PROLLY_HASH_SIZE);
      }
      pos += CHUNK_MANIFEST_SIZE;
      nRootedPending = cs->nPending;
      nRootRecordsSeen++;

    } else {
      rc = SQLITE_CORRUPT;
      goto replay_error;
    }
  }

  cs->nPending = nRootedPending;

  if( nRootRecordsSeen == 0
   && nPendingBefore == 0 && cs->nIndex == 0 ){
    memset(cs->refsHash.data, 0, PROLLY_HASH_SIZE);
    cs->nChunks = 0;
  }

  if( cs->nPending > 0 ){
    ChunkIndexEntry *aMerged = 0;
    int nMerged = 0;
    rc = csMergeIndex(cs, &aMerged, &nMerged);
    if( rc != SQLITE_OK ) goto replay_error;
    cs->aIndex = aMerged;
    cs->nIndex = nMerged;
    cs->aIndexMmapBase = 0;
    cs->aIndexMmapSize = 0;
    cs->nPending = 0;
    csPendHTClear(cs);
  }

  if( !prollyHashIsEmpty(&cs->refsHash) ){
    u8 *refsData = 0;
    int nRefsData = 0;
    int rc2 = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
    if( rc2==SQLITE_OK && refsData ){
      rc2 = csReplaceRefsStateFromBlob(&tmpRefs, refsData, nRefsData, 0);
      sqlite3_free(refsData);
      if( rc2!=SQLITE_OK ){
        rc = rc2;
        goto replay_error;
      }
      haveTmpRefs = 1;
    }else if( rc2!=SQLITE_OK ){
      rc = rc2;
      goto replay_error;
    }
  }
  if( haveTmpRefs ){
    csFreeRefsState(cs);
    csAdoptRefsState(cs, &tmpRefs);
    haveTmpRefs = 0;
  }
  rc = csEnsureDefaultBranch(cs);
  if( rc!=SQLITE_OK ) goto replay_error;

  csReleaseReplayState(cs, &saved);
  return SQLITE_OK;

replay_error:
  if( haveTmpRefs ) csFreeRefsState(&tmpRefs);
  csRollbackReplayState(cs, &saved, nPendingBefore);
  return rc;
}

static int csIndexEntryCmp(const void *a, const void *b){
  const ChunkIndexEntry *ea = (const ChunkIndexEntry *)a;
  const ChunkIndexEntry *eb = (const ChunkIndexEntry *)b;
  return prollyHashCompare(&ea->hash, &eb->hash);
}

static int csIndexLowerBound(
  const ChunkIndexEntry *aIndex,
  int nIndex,
  int lo,
  const ProllyHash *pHash
){
  int hi = nIndex;
  while( lo < hi ){
    int mid = lo + ((hi - lo) >> 1);
    if( prollyHashCompare(&aIndex[mid].hash, pHash) < 0 ){
      lo = mid + 1;
    }else{
      hi = mid;
    }
  }
  return lo;
}

static int csMergeIndex(
  ChunkStore *cs,
  ChunkIndexEntry **ppMerged,
  int *pnMerged
){
  int nTotal = cs->nIndex + cs->nPending;
  ChunkIndexEntry *aMerged;
  int idxPos, pendPos, outPos;

  *ppMerged = 0;
  *pnMerged = 0;
  if( nTotal == 0 ) return SQLITE_OK;

  aMerged = (ChunkIndexEntry *)sqlite3_malloc(
    nTotal * (int)sizeof(ChunkIndexEntry)
  );
  if( aMerged == 0 ) return SQLITE_NOMEM;

  if( cs->nPending > 1 ){
    qsort(cs->aPending, cs->nPending, sizeof(ChunkIndexEntry),
          csIndexEntryCmp);
  }

  if( cs->nPending > 0 && cs->nPending <= 32 ){
    idxPos = 0;
    outPos = 0;
    for( pendPos = 0; pendPos < cs->nPending; pendPos++ ){
      ChunkIndexEntry *pPending = &cs->aPending[pendPos];
      int found;
      int nCopy;
      int pos = csIndexLowerBound(cs->aIndex, cs->nIndex, idxPos,
                                  &pPending->hash);
      nCopy = pos - idxPos;
      if( nCopy > 0 ){
        memcpy(&aMerged[outPos], &cs->aIndex[idxPos],
               nCopy * sizeof(ChunkIndexEntry));
        outPos += nCopy;
      }
      found = pos < cs->nIndex
           && prollyHashCompare(&cs->aIndex[pos].hash, &pPending->hash)==0;
      aMerged[outPos++] = *pPending;
      idxPos = found ? pos + 1 : pos;
    }
    if( idxPos < cs->nIndex ){
      int nCopy = cs->nIndex - idxPos;
      memcpy(&aMerged[outPos], &cs->aIndex[idxPos],
             nCopy * sizeof(ChunkIndexEntry));
      outPos += nCopy;
    }
    *ppMerged = aMerged;
    *pnMerged = outPos;
    return SQLITE_OK;
  }

  idxPos = 0;
  pendPos = 0;
  outPos = 0;
  while( idxPos < cs->nIndex && pendPos < cs->nPending ){
    int cmp = prollyHashCompare(&cs->aIndex[idxPos].hash, &cs->aPending[pendPos].hash);
    if( cmp < 0 ){
      aMerged[outPos++] = cs->aIndex[idxPos++];
    }else if( cmp > 0 ){
      aMerged[outPos++] = cs->aPending[pendPos++];
    }else{

      aMerged[outPos++] = cs->aPending[pendPos++];
      idxPos++;
    }
  }
  while( idxPos < cs->nIndex ) aMerged[outPos++] = cs->aIndex[idxPos++];
  while( pendPos < cs->nPending ) aMerged[outPos++] = cs->aPending[pendPos++];

  *ppMerged = aMerged;
  *pnMerged = outPos;
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
  cs->pVfs = pVfs;
  cs->graphLockFd = -1;

  if( zFilename==0 || zFilename[0]=='\0'
   || strcmp(zFilename, ":memory:")==0 ){
    cs->isMemory = 1;
    cs->zFilename = sqlite3_mprintf(":memory:");
    if( cs->zFilename==0 ) return SQLITE_NOMEM;
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->pFile = 0;
    return SQLITE_OK;
  }

  n = (int)strlen(zFilename);
  cs->zFilename = (char *)sqlite3_malloc(n + 1);
  if( cs->zFilename == 0 ) return SQLITE_NOMEM;
  memcpy(cs->zFilename, zFilename, n + 1);

  rc = sqlite3OsAccess(pVfs, cs->zFilename, SQLITE_ACCESS_EXISTS, &exists);
  if( rc != SQLITE_OK ){
    sqlite3_free(cs->zFilename);
    cs->zFilename = 0;
    return rc;
  }

  if( exists ){
    struct stat mainStat;
    if( stat(cs->zFilename, &mainStat)==0 && mainStat.st_size==0 ){
      exists = 0;
    }
  }

  if( exists ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc != SQLITE_OK ){
      openFlags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB;
      rc = csOpenFile(pVfs, cs->zFilename, &cs->pFile, openFlags);
      if( rc != SQLITE_OK ){
        sqlite3_free(cs->zFilename);
        cs->zFilename = 0;
        return rc;
      }
      cs->readOnly = 1;
    }

    rc = csReadManifest(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    rc = csReadIndex(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    rc = csReplayWal(cs);
    if( rc != SQLITE_OK ){
      csCloseFile(cs->pFile);
      cs->pFile = 0;
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return rc;
    }

    if( !prollyHashIsEmpty(&cs->refsHash) ){
      u8 *refsData = 0; int nRefsData = 0;
      rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
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
      sqlite3_free(cs->zFilename);
      cs->zFilename = 0;
      return SQLITE_CANTOPEN;
    }
    cs->nChunks = 0;
    cs->iIndexOffset = 0;
    cs->nIndexSize = 0;

    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    cs->iFileSize = 0;
    cs->pFile = 0;
  }

  csMarkRefsCommitted(cs);
  return SQLITE_OK;
}

int chunkStoreClose(ChunkStore *cs){
  chunkStoreUnlock(cs);
  if( cs->pFile ){
    csCloseFile(cs->pFile);
    cs->pFile = 0;
  }
  sqlite3_free(cs->zFilename);
  csReleaseIndexBuf(cs->aIndex, cs->aIndexMmapBase, cs->aIndexMmapSize);
  cs->aIndex = 0;
  cs->aIndexMmapBase = 0;
  cs->aIndexMmapSize = 0;
  sqlite3_free(cs->aPending);
  sqlite3_free(cs->aRecent);
  csPendHTClear(cs);
  csRecentHTClear(cs);
  sqlite3_free(cs->pWriteBuf);
  sqlite3_free(cs->zDefaultBranch);
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  memset(cs, 0, sizeof(*cs));
  return SQLITE_OK;
}

const char *chunkStoreGetDefaultBranch(ChunkStore *cs){
  return cs->zDefaultBranch ? cs->zDefaultBranch : "main";
}

int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName){
  char *zCopy = sqlite3_mprintf("%s", zName);
  if( !zCopy ) return SQLITE_NOMEM;
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = zCopy;
  return SQLITE_OK;
}

int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zName)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->aBranches[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  struct BranchRef *aNew;
  if( chunkStoreFindBranch(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->aBranches, (cs->nBranches+1)*(int)sizeof(struct BranchRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->aBranches = aNew;
  memset(&aNew[cs->nBranches], 0, sizeof(struct BranchRef));
  aNew[cs->nBranches].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->nBranches].zName ) return SQLITE_NOMEM;
  memcpy(&aNew[cs->nBranches].commitHash, pCommit, sizeof(ProllyHash));
  cs->nBranches++;
  return SQLITE_OK;
}

int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zName)==0 ){
      memcpy(&cs->aBranches[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zName)==0 ){
      sqlite3_free(cs->aBranches[i].zName);
      cs->aBranches[i] = cs->aBranches[cs->nBranches-1];
      cs->nBranches--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zBranch)==0 ){
      memcpy(pHash, &cs->aBranches[i].workingSetHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  memset(pHash, 0, sizeof(ProllyHash));
  return SQLITE_NOTFOUND;
}

int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash){
  int i;
  for(i=0; i<cs->nBranches; i++){
    if( strcmp(cs->aBranches[i].zName, zBranch)==0 ){
      memcpy(&cs->aBranches[i].workingSetHash, pHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nTags; i++){
    if( strcmp(cs->aTags[i].zName, zName)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->aTags[i].commitHash, sizeof(ProllyHash));
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
  aNew = sqlite3_realloc(cs->aTags, (cs->nTags+1)*(int)sizeof(struct TagRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->aTags = aNew;
  memset(&aNew[cs->nTags], 0, sizeof(struct TagRef));
  aNew[cs->nTags].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->nTags].zName ) return SQLITE_NOMEM;
  memcpy(&aNew[cs->nTags].commitHash, pCommit, sizeof(ProllyHash));
  aNew[cs->nTags].zTagger  = sqlite3_mprintf("%s", zTagger  ? zTagger  : "");
  aNew[cs->nTags].zEmail   = sqlite3_mprintf("%s", zEmail   ? zEmail   : "");
  aNew[cs->nTags].zMessage = sqlite3_mprintf("%s", zMessage ? zMessage : "");
  if( !aNew[cs->nTags].zTagger || !aNew[cs->nTags].zEmail || !aNew[cs->nTags].zMessage ){
    return SQLITE_NOMEM;
  }
  aNew[cs->nTags].timestamp = timestamp;
  cs->nTags++;
  return SQLITE_OK;
}

int chunkStoreDeleteTag(ChunkStore *cs, const char *zName){
  int i;
  for(i=0; i<cs->nTags; i++){
    if( strcmp(cs->aTags[i].zName, zName)==0 ){
      sqlite3_free(cs->aTags[i].zName);
      cs->aTags[i] = cs->aTags[cs->nTags-1];
      cs->nTags--;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl){
  int i;
  for(i=0; i<cs->nRemotes; i++){
    if( strcmp(cs->aRemotes[i].zName, zName)==0 ){
      if( pzUrl ) *pzUrl = cs->aRemotes[i].zUrl;
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl){
  struct RemoteRef *aNew;
  if( chunkStoreFindRemote(cs, zName, 0)==SQLITE_OK ) return SQLITE_ERROR;
  aNew = sqlite3_realloc(cs->aRemotes, (cs->nRemotes+1)*(int)sizeof(struct RemoteRef));
  if( !aNew ) return SQLITE_NOMEM;
  cs->aRemotes = aNew;
  aNew[cs->nRemotes].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[cs->nRemotes].zName ) return SQLITE_NOMEM;
  aNew[cs->nRemotes].zUrl = sqlite3_mprintf("%s", zUrl);
  if( !aNew[cs->nRemotes].zUrl ){
    sqlite3_free(aNew[cs->nRemotes].zName);
    return SQLITE_NOMEM;
  }
  cs->nRemotes++;
  return SQLITE_OK;
}

int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName){
  int i, j;
  for(i=0; i<cs->nRemotes; i++){
    if( strcmp(cs->aRemotes[i].zName, zName)==0 ){
      sqlite3_free(cs->aRemotes[i].zName);
      sqlite3_free(cs->aRemotes[i].zUrl);
      cs->aRemotes[i] = cs->aRemotes[cs->nRemotes-1];
      cs->nRemotes--;

      for(j=cs->nTracking-1; j>=0; j--){
        if( strcmp(cs->aTracking[j].zRemote, zName)==0 ){
          sqlite3_free(cs->aTracking[j].zRemote);
          sqlite3_free(cs->aTracking[j].zBranch);
          cs->aTracking[j] = cs->aTracking[cs->nTracking-1];
          cs->nTracking--;
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
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      if( pCommit ) memcpy(pCommit, &cs->aTracking[i].commitHash, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
}

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit){
  int i;
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      memcpy(&cs->aTracking[i].commitHash, pCommit, sizeof(ProllyHash));
      return SQLITE_OK;
    }
  }

  {
    struct TrackingBranch *aNew;
    aNew = sqlite3_realloc(cs->aTracking, (cs->nTracking+1)*(int)sizeof(struct TrackingBranch));
    if( !aNew ) return SQLITE_NOMEM;
    cs->aTracking = aNew;
    aNew[cs->nTracking].zRemote = sqlite3_mprintf("%s", zRemote);
    if( !aNew[cs->nTracking].zRemote ) return SQLITE_NOMEM;
    aNew[cs->nTracking].zBranch = sqlite3_mprintf("%s", zBranch);
    if( !aNew[cs->nTracking].zBranch ){
      sqlite3_free(aNew[cs->nTracking].zRemote);
      return SQLITE_NOMEM;
    }
    memcpy(&aNew[cs->nTracking].commitHash, pCommit, sizeof(ProllyHash));
    cs->nTracking++;
  }
  return SQLITE_OK;
}

int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch){
  int i;
  for(i=0; i<cs->nTracking; i++){
    if( strcmp(cs->aTracking[i].zRemote, zRemote)==0
     && strcmp(cs->aTracking[i].zBranch, zBranch)==0 ){
      sqlite3_free(cs->aTracking[i].zRemote);
      sqlite3_free(cs->aTracking[i].zBranch);
      cs->aTracking[i] = cs->aTracking[cs->nTracking-1];
      cs->nTracking--;
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
  const char *def = cs->zDefaultBranch ? cs->zDefaultBranch : "main";
  int defLen = (int)strlen(def);
  int sz = 1 + 4 + defLen + 4 + 4 + 4 + 4;
  int i;
  u8 *buf, *bufCur;

  *ppOut = 0;
  *pnOut = 0;

  for(i=0; i<cs->nBranches; i++){
    int inc = 4 + (int)strlen(cs->aBranches[i].zName) + PROLLY_HASH_SIZE*2;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nTags; i++){
    int taggerLen  = cs->aTags[i].zTagger  ? (int)strlen(cs->aTags[i].zTagger)  : 0;
    int emailLen   = cs->aTags[i].zEmail   ? (int)strlen(cs->aTags[i].zEmail)   : 0;
    int messageLen = cs->aTags[i].zMessage ? (int)strlen(cs->aTags[i].zMessage) : 0;
    int inc = 4 + (int)strlen(cs->aTags[i].zName) + PROLLY_HASH_SIZE
            + 4 + taggerLen
            + 4 + emailLen
            + 8
            + 4 + messageLen;
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nRemotes; i++){
    int inc = 4 + (int)strlen(cs->aRemotes[i].zName) + 4 + (int)strlen(cs->aRemotes[i].zUrl);
    if( sz > INT_MAX - inc ){
      return SQLITE_TOOBIG;
    }
    sz += inc;
  }
  for(i=0; i<cs->nTracking; i++){
    int inc = 4 + (int)strlen(cs->aTracking[i].zRemote) + 4 + (int)strlen(cs->aTracking[i].zBranch) + PROLLY_HASH_SIZE;
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
  CS_WRITE_U32(bufCur,cs->nBranches); bufCur+=4;
  for(i=0; i<cs->nBranches; i++){
    int nameLen = (int)strlen(cs->aBranches[i].zName);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->aBranches[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aBranches[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    memcpy(bufCur, cs->aBranches[i].workingSetHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
  }
  CS_WRITE_U32(bufCur,cs->nTags); bufCur+=4;
  for(i=0; i<cs->nTags; i++){
    int nameLen    = (int)strlen(cs->aTags[i].zName);
    int taggerLen  = cs->aTags[i].zTagger  ? (int)strlen(cs->aTags[i].zTagger)  : 0;
    int emailLen   = cs->aTags[i].zEmail   ? (int)strlen(cs->aTags[i].zEmail)   : 0;
    int messageLen = cs->aTags[i].zMessage ? (int)strlen(cs->aTags[i].zMessage) : 0;
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->aTags[i].zName, nameLen); bufCur+=nameLen;
    memcpy(bufCur, cs->aTags[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
    CS_WRITE_U32(bufCur,taggerLen); bufCur+=4;
    if( taggerLen ) memcpy(bufCur, cs->aTags[i].zTagger, taggerLen);
    bufCur+=taggerLen;
    CS_WRITE_U32(bufCur,emailLen); bufCur+=4;
    if( emailLen ) memcpy(bufCur, cs->aTags[i].zEmail, emailLen);
    bufCur+=emailLen;
    CS_WRITE_I64(bufCur,cs->aTags[i].timestamp); bufCur+=8;
    CS_WRITE_U32(bufCur,messageLen); bufCur+=4;
    if( messageLen ) memcpy(bufCur, cs->aTags[i].zMessage, messageLen);
    bufCur+=messageLen;
  }
  CS_WRITE_U32(bufCur,cs->nRemotes); bufCur+=4;
  for(i=0; i<cs->nRemotes; i++){
    int nameLen = (int)strlen(cs->aRemotes[i].zName);
    int urlLen = (int)strlen(cs->aRemotes[i].zUrl);
    CS_WRITE_U32(bufCur,nameLen); bufCur+=4;
    memcpy(bufCur, cs->aRemotes[i].zName, nameLen); bufCur+=nameLen;
    CS_WRITE_U32(bufCur,urlLen); bufCur+=4;
    memcpy(bufCur, cs->aRemotes[i].zUrl, urlLen); bufCur+=urlLen;
  }
  CS_WRITE_U32(bufCur,cs->nTracking); bufCur+=4;
  for(i=0; i<cs->nTracking; i++){
    int remoteLen = (int)strlen(cs->aTracking[i].zRemote);
    int branchLen = (int)strlen(cs->aTracking[i].zBranch);
    CS_WRITE_U32(bufCur,remoteLen); bufCur+=4;
    memcpy(bufCur, cs->aTracking[i].zRemote, remoteLen); bufCur+=remoteLen;
    CS_WRITE_U32(bufCur,branchLen); bufCur+=4;
    memcpy(bufCur, cs->aTracking[i].zBranch, branchLen); bufCur+=branchLen;
    memcpy(bufCur, cs->aTracking[i].commitHash.data, PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
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
  if( rc==SQLITE_OK ) memcpy(&cs->refsHash, &refsHash, sizeof(ProllyHash));
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
  sqlite3_free(cs->zDefaultBranch);
  cs->zDefaultBranch = sqlite3_malloc(defLen+1);
  if(!cs->zDefaultBranch) return SQLITE_NOMEM;
  memcpy(cs->zDefaultBranch, bufCur, defLen); cs->zDefaultBranch[defLen]=0; bufCur+=defLen;
  if( bufCur+4>data+nData ) return SQLITE_CORRUPT;
  nBranches = (int)CS_READ_U32(bufCur); bufCur+=4;
  csFreeBranches(cs);
  if( nBranches>0 ){
    cs->aBranches = sqlite3_malloc(nBranches*(int)sizeof(struct BranchRef));
    if(!cs->aBranches) return SQLITE_NOMEM;
    for(i=0;i<nBranches;i++){
      int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
      nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
      if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
      memset(&cs->aBranches[i], 0, sizeof(struct BranchRef));
      cs->aBranches[i].zName=sqlite3_malloc(nameLen+1);
      if(!cs->aBranches[i].zName) return SQLITE_NOMEM;
      memcpy(cs->aBranches[i].zName,bufCur,nameLen); cs->aBranches[i].zName[nameLen]=0; bufCur+=nameLen;
      memcpy(cs->aBranches[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      if( bufCur+PROLLY_HASH_SIZE<=data+nData ){
        memcpy(cs->aBranches[i].workingSetHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
      }
      cs->nBranches++;
    }
  }

  csFreeTags(cs);
  if( bufCur+4<=data+nData ){
    nTags = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nTags>0 ){
      cs->aTags = sqlite3_malloc(nTags*(int)sizeof(struct TagRef));
      if(!cs->aTags) return SQLITE_NOMEM;
      memset(cs->aTags, 0, nTags*(int)sizeof(struct TagRef));
      for(i=0;i<nTags;i++){
        int nameLen; if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+nameLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
        cs->aTags[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->aTags[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aTags[i].zName,bufCur,nameLen); cs->aTags[i].zName[nameLen]=0; bufCur+=nameLen;
        memcpy(cs->aTags[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
        if( version>=6 ){
          int taggerLen, emailLen, messageLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          taggerLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+taggerLen>data+nData) return SQLITE_CORRUPT;
          cs->aTags[i].zTagger=sqlite3_malloc(taggerLen+1);
          if(!cs->aTags[i].zTagger) return SQLITE_NOMEM;
          memcpy(cs->aTags[i].zTagger,bufCur,taggerLen); cs->aTags[i].zTagger[taggerLen]=0; bufCur+=taggerLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          emailLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+emailLen>data+nData) return SQLITE_CORRUPT;
          cs->aTags[i].zEmail=sqlite3_malloc(emailLen+1);
          if(!cs->aTags[i].zEmail) return SQLITE_NOMEM;
          memcpy(cs->aTags[i].zEmail,bufCur,emailLen); cs->aTags[i].zEmail[emailLen]=0; bufCur+=emailLen;
          if(bufCur+8>data+nData) return SQLITE_CORRUPT;
          cs->aTags[i].timestamp=CS_READ_I64(bufCur); bufCur+=8;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          messageLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+messageLen>data+nData) return SQLITE_CORRUPT;
          cs->aTags[i].zMessage=sqlite3_malloc(messageLen+1);
          if(!cs->aTags[i].zMessage) return SQLITE_NOMEM;
          memcpy(cs->aTags[i].zMessage,bufCur,messageLen); cs->aTags[i].zMessage[messageLen]=0; bufCur+=messageLen;
        }else{
          cs->aTags[i].zTagger  = sqlite3_mprintf("");
          cs->aTags[i].zEmail   = sqlite3_mprintf("");
          cs->aTags[i].zMessage = sqlite3_mprintf("");
          if( !cs->aTags[i].zTagger || !cs->aTags[i].zEmail || !cs->aTags[i].zMessage ){
            return SQLITE_NOMEM;
          }
        }
        cs->nTags++;
      }
    }
  }

  csFreeRemotes(cs);
  csFreeTracking(cs);
  if( bufCur+4<=data+nData ){
    int nRemotes = (int)CS_READ_U32(bufCur); bufCur+=4;
    if( nRemotes>0 ){
      cs->aRemotes = sqlite3_malloc(nRemotes*(int)sizeof(struct RemoteRef));
      if(!cs->aRemotes) return SQLITE_NOMEM;
      memset(cs->aRemotes, 0, nRemotes*(int)sizeof(struct RemoteRef));
      for(i=0;i<nRemotes;i++){
        int nameLen, urlLen;
        if(bufCur+4>data+nData) return SQLITE_CORRUPT;
        nameLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+nameLen+4>data+nData) return SQLITE_CORRUPT;
        cs->aRemotes[i].zName=sqlite3_malloc(nameLen+1);
        if(!cs->aRemotes[i].zName) return SQLITE_NOMEM;
        memcpy(cs->aRemotes[i].zName,bufCur,nameLen); cs->aRemotes[i].zName[nameLen]=0; bufCur+=nameLen;
        urlLen=(int)CS_READ_U32(bufCur); bufCur+=4;
        if(bufCur+urlLen>data+nData) return SQLITE_CORRUPT;
        cs->aRemotes[i].zUrl=sqlite3_malloc(urlLen+1);
        if(!cs->aRemotes[i].zUrl) return SQLITE_NOMEM;
        memcpy(cs->aRemotes[i].zUrl,bufCur,urlLen); cs->aRemotes[i].zUrl[urlLen]=0; bufCur+=urlLen;
        cs->nRemotes++;
      }
    }
    if( bufCur+4<=data+nData ){
      int nTracking = (int)CS_READ_U32(bufCur); bufCur+=4;
      if( nTracking>0 ){
        cs->aTracking = sqlite3_malloc(nTracking*(int)sizeof(struct TrackingBranch));
        if(!cs->aTracking) return SQLITE_NOMEM;
        memset(cs->aTracking, 0, nTracking*(int)sizeof(struct TrackingBranch));
        for(i=0;i<nTracking;i++){
          int remoteLen, branchLen;
          if(bufCur+4>data+nData) return SQLITE_CORRUPT;
          remoteLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+remoteLen+4>data+nData) return SQLITE_CORRUPT;
          cs->aTracking[i].zRemote=sqlite3_malloc(remoteLen+1);
          if(!cs->aTracking[i].zRemote) return SQLITE_NOMEM;
          memcpy(cs->aTracking[i].zRemote,bufCur,remoteLen); cs->aTracking[i].zRemote[remoteLen]=0; bufCur+=remoteLen;
          branchLen=(int)CS_READ_U32(bufCur); bufCur+=4;
          if(bufCur+branchLen+PROLLY_HASH_SIZE>data+nData) return SQLITE_CORRUPT;
          cs->aTracking[i].zBranch=sqlite3_malloc(branchLen+1);
          if(!cs->aTracking[i].zBranch) return SQLITE_NOMEM;
          memcpy(cs->aTracking[i].zBranch,bufCur,branchLen); cs->aTracking[i].zBranch[branchLen]=0; bufCur+=branchLen;
          memcpy(cs->aTracking[i].commitHash.data,bufCur,PROLLY_HASH_SIZE); bufCur+=PROLLY_HASH_SIZE;
          cs->nTracking++;
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

static void csAdoptRefsState(ChunkStore *pDst, ChunkStore *pSrc){
  pDst->aBranches = pSrc->aBranches;
  pDst->nBranches = pSrc->nBranches;
  pDst->zDefaultBranch = pSrc->zDefaultBranch;
  pDst->aTags = pSrc->aTags;
  pDst->nTags = pSrc->nTags;
  pDst->aRemotes = pSrc->aRemotes;
  pDst->nRemotes = pSrc->nRemotes;
  pDst->aTracking = pSrc->aTracking;
  pDst->nTracking = pSrc->nTracking;

  pSrc->aBranches = 0;
  pSrc->nBranches = 0;
  pSrc->zDefaultBranch = 0;
  pSrc->aTags = 0;
  pSrc->nTags = 0;
  pSrc->aRemotes = 0;
  pSrc->nRemotes = 0;
  pSrc->aTracking = 0;
  pSrc->nTracking = 0;
}

static int csReplaceRefsStateFromBlob(
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
  if( csSearchIndex(cs->aIndex, cs->nIndex, hash) >= 0 ){
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
    ChunkIndexEntry *e = &cs->aPending[idx];
    i64 off = e->offset;
    int sz = e->size;
    u8 *pCopy = (u8 *)sqlite3_malloc(sz);
    if( pCopy == 0 ) return SQLITE_NOMEM;

    memcpy(pCopy, cs->pWriteBuf + off + 4, sz);
    *ppData = pCopy;
    *pnData = sz;
    return SQLITE_OK;
  }

  {
    ChunkIndexEntry *e;
    rc = csSearchRecent(cs, hash, &idx);
    if( rc!=SQLITE_OK ) return rc;
    if( idx >= 0 ){
      e = &cs->aRecent[idx];
    }else{
      idx = csSearchIndex(cs->aIndex, cs->nIndex, hash);
      if( idx < 0 ){
        return SQLITE_NOTFOUND;
      }
      e = &cs->aIndex[idx];
    }

    if( cs->pFile == 0 ){
      if( cs->pWriteBuf && e->offset >= 0
       && (e->offset + 4 + e->size) <= cs->nWriteBuf ){
        u8 *pCopy = (u8 *)sqlite3_malloc(e->size);
        if( pCopy == 0 ) return SQLITE_NOMEM;
        memcpy(pCopy, cs->pWriteBuf + e->offset + 4, e->size);
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

      rc = sqlite3OsRead(cs->pFile, lenBuf, 4, fileOff);
      if( rc != SQLITE_OK ) return rc;

      storedLen = CS_READ_U32(lenBuf);
      if( (int)storedLen != sz ){
        return SQLITE_CORRUPT;
      }

      pBuf = (u8 *)sqlite3_malloc(sz);
      if( pBuf == 0 ) return SQLITE_NOMEM;

      rc = sqlite3OsRead(cs->pFile, pBuf, sz, fileOff + 4);
      if( rc != SQLITE_OK ){
        sqlite3_free(pBuf);
        return rc;
      }

      *ppData = pBuf;
      *pnData = sz;
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

  if( csSearchIndex(cs->aIndex, cs->nIndex, &h) >= 0 ) return SQLITE_OK;
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
    ChunkIndexEntry *e = &cs->aPending[cs->nPending];
    e->hash = h;
    e->offset = (i64)cs->nWriteBuf;
    e->size = nData;
    cs->nPending++;
  }

  CS_WRITE_U32(cs->pWriteBuf + cs->nWriteBuf, (u32)nData);
  cs->nWriteBuf += 4;
  memcpy(cs->pWriteBuf + cs->nWriteBuf, pData, nData);
  cs->nWriteBuf += nData;

  return SQLITE_OK;
}

static int csCommitToMemory(ChunkStore *cs){
  if( cs->nPending > 0 ){
    ChunkIndexEntry *aMem = 0;
    int nMem = 0;
    int rc = csMergeIndex(cs, &aMem, &nMem);
    if( rc!=SQLITE_OK ) return rc;
    csReleaseIndexBuf(cs->aIndex, cs->aIndexMmapBase, cs->aIndexMmapSize);
    cs->aIndex = aMem;
    cs->nIndex = nMem;
    cs->aIndexMmapBase = 0;
    cs->aIndexMmapSize = 0;
    cs->nPending = 0;
    csPendHTClear(cs);
    cs->nCommittedWriteBuf = cs->nWriteBuf;
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
  int hadFile = (cs->pFile != 0);
  int lockHeld = (cs->graphLockFd >= 0);
  i64 newWalSize = cs->nWalData;
  ChunkIndexEntry *aCommittedPending = 0;
  ChunkIndexEntry *aMergePending = 0;
  ChunkIndexEntry *aMerged = 0;
  int nMerged = 0;
  int useRecent = 0;
  int crashWriteActive = csCrashWriteInjectionActive();

  if( cs->pFile == 0 ){
    int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_MAIN_DB;
    rc = csOpenFile(cs->pVfs, cs->zFilename, &cs->pFile, openFlags);
    if( rc != SQLITE_OK ) return SQLITE_CANTOPEN;
  }

  if( lockHeld ){
    lockFd = -1;
  }else{
    if( csFileLock(cs->zFilename, &lockFd) != 0 ){
      return SQLITE_BUSY;
    }
  }

  rc = cs->pFile->pMethods->xFileSize(cs->pFile, &fileSize);
  if( rc != SQLITE_OK ) goto commit_done;

  if( hadFile && !cs->hasMovedChecked ){
    int bMoved = 0;
    int rc2 = sqlite3OsFileControl(cs->pFile, SQLITE_FCNTL_HAS_MOVED,
                                   &bMoved);
    if( rc2==SQLITE_OK && bMoved ){
      rc = csReloadFromDisk(cs);
      if( rc != SQLITE_OK ) goto commit_done;
      fileSize = cs->iFileSize;
    }
    cs->hasMovedChecked = 1;
  }

  if( fileSize > cs->iFileSize && hadFile ){
    rc = csReloadFromDisk(cs);
    if( rc != SQLITE_OK ) goto commit_done;
    fileSize = cs->iFileSize;
  }
  origFileSize = fileSize;

  if( cs->nPending > 0 ){
    ChunkStore mergeView;
    i64 filePos = fileSize > 0 ? fileSize : (i64)CHUNK_MANIFEST_SIZE;
    i64 appendBytes = 0;

    for( i = 0; i < cs->nPending; i++ ){
      i64 recBytes = (i64)25 + (i64)cs->aPending[i].size;
      if( appendBytes > LARGEST_INT64 - recBytes ){
        rc = SQLITE_TOOBIG;
        goto commit_done;
      }
      appendBytes += recBytes;
    }
    if( cs->nWalData > LARGEST_INT64 - appendBytes ){
      rc = SQLITE_TOOBIG;
      goto commit_done;
    }
    newWalSize = cs->nWalData + appendBytes;

    aCommittedPending = (ChunkIndexEntry*)sqlite3_malloc(
      cs->nPending * (int)sizeof(ChunkIndexEntry)
    );
    if( !aCommittedPending ){
      rc = SQLITE_NOMEM;
      goto commit_done;
    }

    for( i = 0; i < cs->nPending; i++ ){
      ChunkIndexEntry *pSrc = &cs->aPending[i];
      aCommittedPending[i] = *pSrc;
      aCommittedPending[i].offset = filePos + 21;
      filePos += (i64)25 + (i64)pSrc->size;
    }

    useRecent = !crashWriteActive
             && cs->nPending <= 32 && cs->nRecent + cs->nPending <= 8192;
    if( useRecent ){
      rc = csGrowRecent(cs, cs->nPending);
      if( rc!=SQLITE_OK ) goto commit_done;
    }else{
      if( cs->nRecent > 0 ){
        aMergePending = (ChunkIndexEntry*)sqlite3_malloc(
          (cs->nRecent + cs->nPending) * (int)sizeof(ChunkIndexEntry)
        );
        if( !aMergePending ){
          rc = SQLITE_NOMEM;
          goto commit_done;
        }
        memcpy(aMergePending, cs->aRecent,
               cs->nRecent * sizeof(ChunkIndexEntry));
        memcpy(aMergePending + cs->nRecent, aCommittedPending,
               cs->nPending * sizeof(ChunkIndexEntry));
        mergeView = *cs;
        mergeView.aPending = aMergePending;
        mergeView.nPending = cs->nRecent + cs->nPending;
      }else{
        mergeView = *cs;
        mergeView.aPending = aCommittedPending;
        mergeView.nPending = cs->nPending;
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
    cs->iWalOffset = CHUNK_MANIFEST_SIZE;
    csSerializeManifest(cs, manifest);
    CRASH_CHECK_WRITE();
    rc = sqlite3OsWrite(cs->pFile, manifest, CHUNK_MANIFEST_SIZE, 0);
    if( rc != SQLITE_OK ) goto commit_done;
    fileSize = CHUNK_MANIFEST_SIZE;
  }

  writeOff = fileSize;

  /* Append chunks before the root record. Recovery ignores appended data until
  ** it finds a later valid root record that points at the new manifest. */
  if( cs->nPending > 0 ){
    i64 walBytes = 0;
    u8 *pWalBatch = 0;
    u8 aSmallWalBatch[4096];
    u8 *pOut = 0;
    for( i = 0; i < cs->nPending; i++ ){
      walBytes += (i64)25 + (i64)cs->aPending[i].size;
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
      for( i = 0; i < cs->nPending; i++ ){
        ChunkIndexEntry *pe = &cs->aPending[i];
        i64 bufOff = pe->offset + 4;
        pOut[0] = CS_WAL_TAG_CHUNK;
        memcpy(pOut + 1, &pe->hash, 20);
        CS_WRITE_U32(pOut + 21, (u32)pe->size);
        pOut += 25;
        memcpy(pOut, cs->pWriteBuf + bufOff, pe->size);
        pOut += pe->size;
      }
      CRASH_CHECK_WRITE();
      rc = sqlite3OsWrite(cs->pFile, pWalBatch, (int)walBytes, writeOff);
      if( pWalBatch != aSmallWalBatch ) sqlite3_free(pWalBatch);
      if( rc != SQLITE_OK ) goto commit_done;
      writeOff += walBytes;
    }else{
      for( i = 0; i < cs->nPending; i++ ){
        ChunkIndexEntry *pe = &cs->aPending[i];
        u8 recHdr[25];
        i64 bufOff;
        recHdr[0] = CS_WAL_TAG_CHUNK;
        memcpy(recHdr + 1, &pe->hash, 20);
        CS_WRITE_U32(recHdr + 21, (u32)pe->size);

        bufOff = pe->offset + 4;
        CRASH_CHECK_WRITE();
        rc = sqlite3OsWrite(cs->pFile, recHdr, 25, writeOff);
        if( rc != SQLITE_OK ) goto commit_done;
        writeOff += 25;

        {
          const u8 *pSrc = cs->pWriteBuf + bufOff;
          int remaining = pe->size;
          while( remaining > 0 && rc==SQLITE_OK ){
            int toWrite = remaining > 65536 ? 65536 : remaining;
            CRASH_CHECK_WRITE();
            rc = sqlite3OsWrite(cs->pFile, pSrc, toWrite, writeOff);
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
    rc = sqlite3OsWrite(cs->pFile, rootRec, sizeof(rootRec), writeOff);
    if( rc != SQLITE_OK ) goto commit_done;
    writeOff += sizeof(rootRec);
  }

  CRASH_CHECK_WRITE();
  rc = sqlite3OsSync(cs->pFile, SQLITE_SYNC_NORMAL);

#ifdef SQLITE_TEST
  }
#undef CRASH_CHECK_WRITE
#endif
  if( rc != SQLITE_OK ) goto commit_done;

  cs->iFileSize = writeOff;

commit_done:
  csFileUnlock(lockFd);

  if( rc != SQLITE_OK ){
    if( cs->pFile && writeOff > origFileSize ){
      (void)csRollbackFailedAppend(cs, origFileSize);
    }
    (void)csRestoreCommittedRefsState(cs);
    sqlite3_free(aCommittedPending);
    sqlite3_free(aMergePending);
    sqlite3_free(aMerged);
    return rc;
  }

  if( cs->nPending > 0 ){
    if( useRecent ){
      memcpy(cs->aRecent + cs->nRecent, aCommittedPending,
             cs->nPending * sizeof(ChunkIndexEntry));
      cs->nRecent += cs->nPending;
      sqlite3_free(aMerged);
    }else{
      csReleaseIndexBuf(cs->aIndex, cs->aIndexMmapBase, cs->aIndexMmapSize);
      cs->aIndex = aMerged;
      cs->nIndex = nMerged;
      cs->aIndexMmapBase = 0;
      cs->aIndexMmapSize = 0;
      cs->nRecent = 0;
      csRecentHTClear(cs);
    }
    cs->nWalData = newWalSize;
  }else{
    sqlite3_free(aMerged);
  }
  sqlite3_free(aCommittedPending);
  sqlite3_free(aMergePending);

  sqlite3_free(cs->pWriteBuf);
  cs->pWriteBuf = 0;
  cs->nWriteBuf = 0;
  cs->nWriteBufAlloc = 0;
  cs->nPending = 0;
  csPendHTClear(cs);
  csMarkRefsCommitted(cs);

  return SQLITE_OK;
}

int chunkStoreCommit(ChunkStore *cs){
  int rc;
  int acquiredLock = 0;
  if( cs->readOnly ) return SQLITE_READONLY;
  if( cs->isMemory ) return csCommitToMemory(cs);

  if( cs->graphLockFd < 0 && cs->zFilename ){
    rc = chunkStoreLockAndRefresh(cs);
    if( rc!=SQLITE_OK ) return rc;
    acquiredLock = 1;
  }
  rc = csCommitToFile(cs);
  if( acquiredLock ) chunkStoreUnlock(cs);
  return rc;
}

void chunkStoreRollback(ChunkStore *cs){
  cs->nPending = 0;
  csPendHTClear(cs);
  if( cs->isMemory ){

    cs->nWriteBuf = cs->nCommittedWriteBuf;
  }else{
    cs->nWriteBuf = 0;
  }
  (void)csRestoreCommittedRefsState(cs);
}

int chunkStoreIsEmpty(ChunkStore *cs){
  return cs->nBranches == 0 && prollyHashIsEmpty(&cs->refsHash);
}

void chunkStoreClearRefs(ChunkStore *cs){
  csFreeBranches(cs);
  csFreeTags(cs);
  csFreeRemotes(cs);
  csFreeTracking(cs);
  memset(&cs->refsHash, 0, sizeof(cs->refsHash));
}

int chunkStoreReloadRefs(ChunkStore *cs){
  u8 *refsData = 0;
  int nRefsData = 0;
  int rc;

  if( prollyHashIsEmpty(&cs->refsHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, &cs->refsHash, &refsData, &nRefsData);
  if( rc!=SQLITE_OK ) return rc;

  rc = csReplaceRefsStateFromBlob(cs, refsData, nRefsData, 0);
  sqlite3_free(refsData);
  return rc;
}

const char *chunkStoreFilename(ChunkStore *cs){
  return cs->zFilename;
}

int chunkStoreLockAndRefresh(ChunkStore *cs){
  int changed = 0;
  int rc;
  if( cs->isMemory ) return SQLITE_OK;
  if( cs->graphLockFd >= 0 ) return SQLITE_OK;
  if( !cs->zFilename ) return SQLITE_ERROR;
  if( csFileLockNB(cs->zFilename, &cs->graphLockFd) != 0 ){
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

  if( cs->pFile==0 ){
    int exists = 0;
    rc = sqlite3OsAccess(cs->pVfs, cs->zFilename,
                         SQLITE_ACCESS_EXISTS, &exists);
    if( rc!=SQLITE_OK ) return rc;
    if( exists ){
      struct stat mainStat;
      if( stat(cs->zFilename, &mainStat)==0 ){
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
    rc = sqlite3OsFileControl(cs->pFile, SQLITE_FCNTL_HAS_MOVED, &bMoved);
    if( rc!=SQLITE_OK ) return rc;
    if( bMoved ){
      *pChanged = 1;
      return SQLITE_OK;
    }
    cs->hasMovedChecked = 1;
  }

  {
    i64 fileSize = 0;
    rc = sqlite3OsFileSize(cs->pFile, &fileSize);
    if( rc!=SQLITE_OK ) return rc;
    if( fileSize > cs->iFileSize ){
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
  int rc = chunkStoreOpen(&tmp, cs->pVfs, cs->zFilename,
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
