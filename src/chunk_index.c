
#ifdef DOLTLITE_PROLLY

#include "chunk_store.h"
#include "chunk_index.h"
#include "prolly_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>

#define CS_READ_U32(p) ( \
    (u32)((p)[0]) | ((u32)((p)[1])<<8) \
  | ((u32)((p)[2])<<16) | ((u32)((p)[3])<<24))
#define CS_READ_I64(p) ( \
    (i64)((u64)(p)[0]) | ((i64)((u64)(p)[1])<<8) \
  | ((i64)((u64)(p)[2])<<16) | ((i64)((u64)(p)[3])<<24) \
  | ((i64)((u64)(p)[4])<<32) | ((i64)((u64)(p)[5])<<40) \
  | ((i64)((u64)(p)[6])<<48) | ((i64)((u64)(p)[7])<<56))

void chunkIndexInit(ChunkIndex *idx){
  memset(idx, 0, sizeof(*idx));
}

void chunkIndexReset(ChunkIndex *idx){
  memset(idx, 0, sizeof(*idx));
}

void chunkIndexGetEntries(const ChunkIndex *idx, int *pn, const ChunkIndexEntry **par){
  *pn = idx->nIndex;
  *par = idx->aIndex;
}

int chunkIndexCount(const ChunkIndex *idx){
  return idx->nIndex;
}

int chunkIndexNChunks(const ChunkIndex *idx){
  return idx->nChunks;
}

i64 chunkIndexOffset(const ChunkIndex *idx){
  return idx->iIndexOffset;
}

i64 chunkIndexSize(const ChunkIndex *idx){
  return idx->nIndexSize;
}

void chunkIndexSetMetadata(ChunkIndex *idx, int nChunks, i64 iOffset, i64 nSize){
  idx->nChunks = nChunks;
  idx->iIndexOffset = iOffset;
  idx->nIndexSize = nSize;
}

void chunkIndexReplaceEntries(ChunkIndex *idx, ChunkIndexEntry *aNew, int nNew){
  sqlite3_free(idx->aIndex);
  idx->aIndex = aNew;
  idx->nIndex = nNew;
  idx->aIndexMmapBase = 0;
  idx->aIndexMmapSize = 0;
}

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

void csReleaseIndexBuf(ChunkIndexEntry *aIndex,
                       void *mmapBase, i64 mmapSize){
  if( mmapBase ){
    csUnmapIndex(mmapBase, mmapSize);
  }else{
    sqlite3_free(aIndex);
  }
}

int csSearchIndex(
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

static int csDeserializeIndexEntry(const u8 *aBuf, ChunkIndexEntry *e){
  u32 sz = CS_READ_U32(aBuf + PROLLY_HASH_SIZE + 8);
  if( sz > (u32)0x7fffffff ) return SQLITE_CORRUPT;
  memcpy(e->hash.data, aBuf, PROLLY_HASH_SIZE);
  e->offset = CS_READ_I64(aBuf + PROLLY_HASH_SIZE);
  e->size = (int)sz;
  return SQLITE_OK;
}

int csReadIndex(ChunkStore *cs){
  int rc;
  i64 nEntries64;
  int nEntries;
  u8 *aBuf;
  int i;
  void *pMapBase = 0;
  i64 nMapSize = 0;
  const u8 *pMapData = 0;
  i64 fileSize = 0;

  if( cs->index.nIndexSize == 0 || cs->index.nChunks == 0 ){
    cs->index.nIndex = 0;
    return SQLITE_OK;
  }

  nEntries64 = cs->index.nIndexSize / CHUNK_INDEX_ENTRY_SIZE;
  if( nEntries64 * CHUNK_INDEX_ENTRY_SIZE != cs->index.nIndexSize ){
    return SQLITE_CORRUPT;
  }
  if( nEntries64 > INT_MAX ){
    return SQLITE_TOOBIG;
  }
  nEntries = (int)nEntries64;

  if( cs->index.iIndexOffset < 0 || cs->index.nIndexSize < 0 ){
    return SQLITE_CORRUPT;
  }
  if( cs->file.pFile ){
    rc = sqlite3OsFileSize(cs->file.pFile, &fileSize);
    if( rc != SQLITE_OK ) return rc;
  }else if( cs->file.zFilename ){
    struct stat st;
    if( stat(cs->file.zFilename, &st) != 0 ) return SQLITE_IOERR;
    fileSize = (i64)st.st_size;
  }
  if( fileSize > 0
   && (cs->index.iIndexOffset > fileSize
       || cs->index.nIndexSize > fileSize - cs->index.iIndexOffset) ){
    return SQLITE_CORRUPT;
  }

  if( cs->file.zFilename
   && csMapIndex(cs->file.zFilename, cs->index.iIndexOffset, cs->index.nIndexSize,
                  &pMapBase, &nMapSize, &pMapData) == SQLITE_OK ){
    cs->index.aIndex = (ChunkIndexEntry *)pMapData;
    cs->index.nIndex = nEntries;
    cs->index.aIndexMmapBase = pMapBase;
    cs->index.aIndexMmapSize = nMapSize;
    return SQLITE_OK;
  }

  cs->index.aIndex = (ChunkIndexEntry *)sqlite3_malloc(
    nEntries * (int)sizeof(ChunkIndexEntry)
  );
  if( cs->index.aIndex == 0 ) return SQLITE_NOMEM;
  cs->index.nIndex = nEntries;
  cs->index.aIndexMmapBase = 0;
  cs->index.aIndexMmapSize = 0;

  aBuf = (u8 *)sqlite3_malloc64(cs->index.nIndexSize);
  if( aBuf == 0 ){
    sqlite3_free(cs->index.aIndex);
    cs->index.aIndex = 0;
    cs->index.nIndex = 0;
    return SQLITE_NOMEM;
  }

  rc = sqlite3OsRead(cs->file.pFile, aBuf, cs->index.nIndexSize, cs->index.iIndexOffset);
  if( rc != SQLITE_OK ){
    sqlite3_free(aBuf);
    sqlite3_free(cs->index.aIndex);
    cs->index.aIndex = 0;
    cs->index.nIndex = 0;
    return rc;
  }

  for( i = 0; i < nEntries; i++ ){
    rc = csDeserializeIndexEntry(aBuf + i * CHUNK_INDEX_ENTRY_SIZE,
                                 &cs->index.aIndex[i]);
    if( rc != SQLITE_OK ){
      sqlite3_free(aBuf);
      sqlite3_free(cs->index.aIndex);
      cs->index.aIndex = 0;
      cs->index.nIndex = 0;
      return rc;
    }
  }

  sqlite3_free(aBuf);
  return SQLITE_OK;
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

int csMergeIndex(
  ChunkStore *cs,
  ChunkIndexEntry **ppMerged,
  int *pnMerged
){
  int nTotal = cs->index.nIndex + cs->staging.nPending;
  ChunkIndexEntry *aMerged;
  int idxPos, pendPos, outPos;

  *ppMerged = 0;
  *pnMerged = 0;
  if( nTotal == 0 ) return SQLITE_OK;

  aMerged = (ChunkIndexEntry *)sqlite3_malloc(
    nTotal * (int)sizeof(ChunkIndexEntry)
  );
  if( aMerged == 0 ) return SQLITE_NOMEM;

  if( cs->staging.nPending > 1 ){
    qsort(cs->staging.aPending, cs->staging.nPending, sizeof(ChunkIndexEntry),
          csIndexEntryCmp);
  }

  if( cs->staging.nPending > 0 && cs->staging.nPending <= 32 ){
    idxPos = 0;
    outPos = 0;
    for( pendPos = 0; pendPos < cs->staging.nPending; pendPos++ ){
      ChunkIndexEntry *pPending = &cs->staging.aPending[pendPos];
      int found;
      int nCopy;
      int pos = csIndexLowerBound(cs->index.aIndex, cs->index.nIndex, idxPos,
                                  &pPending->hash);
      nCopy = pos - idxPos;
      if( nCopy > 0 ){
        memcpy(&aMerged[outPos], &cs->index.aIndex[idxPos],
               nCopy * sizeof(ChunkIndexEntry));
        outPos += nCopy;
      }
      found = pos < cs->index.nIndex
           && prollyHashCompare(&cs->index.aIndex[pos].hash, &pPending->hash)==0;
      aMerged[outPos++] = *pPending;
      idxPos = found ? pos + 1 : pos;
    }
    if( idxPos < cs->index.nIndex ){
      int nCopy = cs->index.nIndex - idxPos;
      memcpy(&aMerged[outPos], &cs->index.aIndex[idxPos],
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
  while( idxPos < cs->index.nIndex && pendPos < cs->staging.nPending ){
    int cmp = prollyHashCompare(&cs->index.aIndex[idxPos].hash, &cs->staging.aPending[pendPos].hash);
    if( cmp < 0 ){
      aMerged[outPos++] = cs->index.aIndex[idxPos++];
    }else if( cmp > 0 ){
      aMerged[outPos++] = cs->staging.aPending[pendPos++];
    }else{

      aMerged[outPos++] = cs->staging.aPending[pendPos++];
      idxPos++;
    }
  }
  while( idxPos < cs->index.nIndex ) aMerged[outPos++] = cs->index.aIndex[idxPos++];
  while( pendPos < cs->staging.nPending ) aMerged[outPos++] = cs->staging.aPending[pendPos++];

  *ppMerged = aMerged;
  *pnMerged = outPos;
  return SQLITE_OK;
}

#endif
