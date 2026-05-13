
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_chunk_walk.h"

#include <string.h>
#include <stdio.h>
#if !defined(_WIN32) && !defined(WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

extern void csSerializeManifest(const ChunkStore *cs, u8 *aBuf);
#include "doltlite_internal.h"

typedef struct GcQueue GcQueue;
struct GcQueue {
  ProllyHash *aItems;
  int nItems;
  int nAlloc;
  int iHead;
};

static int gcQueueInit(GcQueue *q){
  q->nAlloc = 256;
  q->aItems = sqlite3_malloc(q->nAlloc * sizeof(ProllyHash));
  if( !q->aItems ) return SQLITE_NOMEM;
  q->nItems = 0;
  q->iHead = 0;
  return SQLITE_OK;
}

static void gcQueueFree(GcQueue *q){
  sqlite3_free(q->aItems);
  memset(q, 0, sizeof(*q));
}

static int gcQueuePush(GcQueue *q, const ProllyHash *h){
  if( prollyHashIsEmpty(h) ) return SQLITE_OK;
  if( q->nItems >= q->nAlloc ){
    int newAlloc = q->nAlloc * 2;
    ProllyHash *aNew = sqlite3_realloc(q->aItems, newAlloc * sizeof(ProllyHash));
    if( !aNew ) return SQLITE_NOMEM;
    q->aItems = aNew;
    q->nAlloc = newAlloc;
  }
  memcpy(&q->aItems[q->nItems], h, sizeof(ProllyHash));
  q->nItems++;
  return SQLITE_OK;
}

static int gcQueuePop(GcQueue *q, ProllyHash *h){
  if( q->iHead >= q->nItems ) return 0;
  memcpy(h, &q->aItems[q->iHead], sizeof(ProllyHash));
  q->iHead++;
  return 1;
}

static int gcChildCb(void *ctx, const ProllyHash *pHash){
  GcQueue *q = (GcQueue*)ctx;
  return gcQueuePush(q, pHash);
}

static int gcMarkReachable(
  ChunkStore *cs,
  ProllyHashSet *marked
){
  GcQueue queue;
  ProllyHash current;
  int rc, i;

  rc = gcQueueInit(&queue);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcQueuePush(&queue, refsTableGetHash(&cs->refs));

  {
    int nBr; const BranchRef *aBr;
    refsTableGetBranches(&cs->refs, &nBr, &aBr);
    for(i=0; rc==SQLITE_OK && i<nBr; i++){
      rc = gcQueuePush(&queue, &aBr[i].commitHash);
      if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &aBr[i].workingSetHash);
    }
  }

  {
    int nTg; const TagRef *aTg;
    refsTableGetTags(&cs->refs, &nTg, &aTg);
    for(i=0; rc==SQLITE_OK && i<nTg; i++){
      rc = gcQueuePush(&queue, &aTg[i].commitHash);
    }
  }
  if( rc!=SQLITE_OK ){
    gcQueueFree(&queue);
    return rc;
  }

  while( gcQueuePop(&queue, &current) ){
    u8 *data = 0;
    int nData = 0;

    if( prollyHashIsEmpty(&current) ) continue;
    if( prollyHashSetContains(marked, &current) ) continue;

    rc = prollyHashSetAdd(marked, &current);
    if( rc!=SQLITE_OK ) break;

    rc = chunkStoreGet(cs, &current, &data, &nData);
    if( rc!=SQLITE_OK ){
      break;
    }

    rc = doltliteEnumerateChunkChildren(data, nData, gcChildCb, &queue);

    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;
  }

  gcQueueFree(&queue);
  return rc;
}

static int gcAppendMarkedChunk(
  ChunkStore *cs,
  const ProllyHash *pHash,
  ProllyHashSet *marked,
  u8 **ppBuf,
  int *pnBuf,
  int *pnBufAlloc,
  i64 dataOffset,
  ChunkIndexEntry *aNewIndex,
  int *pnNewIndex
){
  u8 *chunkData = 0;
  int nChunkData = 0;
  i64 need;
  int rc;

  if( !prollyHashSetContains(marked, pHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, pHash, &chunkData, &nChunkData);
  if( rc!=SQLITE_OK ) return rc;

  if( nChunkData < 0 ){
    sqlite3_free(chunkData);
    return SQLITE_CORRUPT;
  }
  need = (i64)*pnBuf + 4 + (i64)nChunkData;
  if( need > (i64)0x7fffffff ){
    sqlite3_free(chunkData);
    return SQLITE_NOMEM;
  }
  if( need > (i64)*pnBufAlloc ){
    i64 newAlloc = *pnBufAlloc ? (i64)*pnBufAlloc * 2 : (i64)65536;
    u8 *pNew;
    while( newAlloc < need ){
      if( newAlloc > (i64)0x7fffffff/2 ){
        newAlloc = (i64)0x7fffffff;
        break;
      }
      newAlloc *= 2;
    }
    if( newAlloc < need || newAlloc > (i64)0x7fffffff ){
      sqlite3_free(chunkData);
      return SQLITE_NOMEM;
    }
    pNew = sqlite3_realloc(*ppBuf, (int)newAlloc);
    if( !pNew ){
      sqlite3_free(chunkData);
      return SQLITE_NOMEM;
    }
    *ppBuf = pNew;
    *pnBufAlloc = (int)newAlloc;
  }

  (*ppBuf)[*pnBuf]   = (u8)(nChunkData);
  (*ppBuf)[*pnBuf+1] = (u8)(nChunkData>>8);
  (*ppBuf)[*pnBuf+2] = (u8)(nChunkData>>16);
  (*ppBuf)[*pnBuf+3] = (u8)(nChunkData>>24);

  memcpy(&aNewIndex[*pnNewIndex].hash, pHash, sizeof(ProllyHash));
  aNewIndex[*pnNewIndex].offset = dataOffset + *pnBuf;
  aNewIndex[*pnNewIndex].size = nChunkData;
  (*pnNewIndex)++;

  memcpy(*ppBuf + *pnBuf + 4, chunkData, nChunkData);
  *pnBuf += 4 + nChunkData;

  sqlite3_free(chunkData);
  return SQLITE_OK;
}

static int gcBuildCompactedData(
  ChunkStore *cs,
  ProllyHashSet *marked,
  u8 **ppNewData,
  int *pnNewData,
  ChunkIndexEntry **ppNewIndex,
  int *pnNewIndex
){
  int i, j;
  int kept = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  u8 *buf = 0;
  int nBuf = 0, nBufAlloc = 0;
  i64 dataOffset = CHUNK_MANIFEST_SIZE;
  int rc = SQLITE_OK;

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      if( prollyHashSetContains(marked, &aIdx[i].hash) ) kept++;
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      if( prollyHashSetContains(marked, &aRec[i].hash) ) kept++;
    }
  }

  aNewIndex = sqlite3_malloc((kept ? kept : 1) * (int)sizeof(ChunkIndexEntry));
  if( !aNewIndex ) return SQLITE_NOMEM;

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      rc = gcAppendMarkedChunk(cs, &aIdx[i].hash, marked, &buf, &nBuf,
                               &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aNewIndex);
        sqlite3_free(buf);
        return rc;
      }
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      rc = gcAppendMarkedChunk(cs, &aRec[i].hash, marked, &buf, &nBuf,
                               &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
      if( rc!=SQLITE_OK ){
        sqlite3_free(aNewIndex);
        sqlite3_free(buf);
        return rc;
      }
    }
  }

  for(i=1; i<nNewIndex; i++){
    ChunkIndexEntry tmp = aNewIndex[i];
    j = i-1;
    while( j>=0 && memcmp(aNewIndex[j].hash.data, tmp.hash.data, PROLLY_HASH_SIZE)>0 ){
      aNewIndex[j+1] = aNewIndex[j];
      j--;
    }
    aNewIndex[j+1] = tmp;
  }

  *ppNewData = buf;
  *pnNewData = nBuf;
  *ppNewIndex = aNewIndex;
  *pnNewIndex = nNewIndex;
  return SQLITE_OK;
}

static int gcRewriteFile(
  ChunkStore *cs,
  const u8 *pNewData,
  int nNewData,
  const ChunkIndexEntry *pNewIndex,
  int nNewIndex
){
  int i;
  int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
  i64 indexOffset = CHUNK_MANIFEST_SIZE + nNewData;
  u8 *indexBuf = 0;
  u8 manifest[CHUNK_MANIFEST_SIZE];
  ChunkStore manifestCs;
  int rc = SQLITE_OK;

#ifdef SQLITE_TEST
  {
    static int crashGcTarget = -2;
    static int crashGcCount = 0;
    if( crashGcTarget == -2 ){
      const char *zEnv = getenv("DOLTLITE_CRASH_GC_WRITE");
      crashGcTarget = zEnv ? atoi(zEnv) : -1;
    }
    if( crashGcTarget > 0 ) crashGcCount = 0;
#define GC_CRASH_CHECK() do{ \
  if( crashGcTarget>0 && ++crashGcCount>=crashGcTarget ){ \
    _exit(99); \
  } \
}while(0)
#else
#define GC_CRASH_CHECK() ((void)0)
#endif

  indexBuf = sqlite3_malloc(indexSize);
  if( !indexBuf ) return SQLITE_NOMEM;
  for(i=0; i<nNewIndex; i++){
    u8 *p = indexBuf + i * CHUNK_INDEX_ENTRY_SIZE;
    memcpy(p, pNewIndex[i].hash.data, PROLLY_HASH_SIZE);
    p += PROLLY_HASH_SIZE;
    {
      i64 off = pNewIndex[i].offset;
      p[0] = (u8)off; p[1] = (u8)(off>>8);
      p[2] = (u8)(off>>16); p[3] = (u8)(off>>24);
      p[4] = (u8)(off>>32); p[5] = (u8)(off>>40);
      p[6] = (u8)(off>>48); p[7] = (u8)(off>>56);
    }
    p += 8;
    {
      u32 sz = (u32)pNewIndex[i].size;
      p[0] = (u8)sz; p[1] = (u8)(sz>>8);
      p[2] = (u8)(sz>>16); p[3] = (u8)(sz>>24);
    }
  }

  manifestCs = *cs;
  chunkIndexSetMetadata(&manifestCs.index, nNewIndex, indexOffset, indexSize);
  walStateSetOffset(&manifestCs.wal, indexOffset + indexSize);

  csSerializeManifest(&manifestCs, manifest);

  if( chunkFileGetFilename(&cs->file) && strcmp(chunkFileGetFilename(&cs->file), ":memory:")!=0 ){
    char *zTmp = sqlite3_mprintf("%s-gc-tmp", chunkFileGetFilename(&cs->file));
    if( !zTmp ){
      sqlite3_free(indexBuf);
      return SQLITE_NOMEM;
    }

    {
      sqlite3_file *pTmpFile = 0;
      int tmpFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                   | SQLITE_OPEN_MAIN_DB;
      i64 writeOff = 0;

      chunkFileGetVfs(&cs->file)->xDelete(chunkFileGetVfs(&cs->file), zTmp, 0);

      rc = sqlite3OsOpenMalloc(chunkFileGetVfs(&cs->file), zTmp, &pTmpFile, tmpFlags, 0);
      if( rc != SQLITE_OK ){
        sqlite3_free(zTmp); sqlite3_free(indexBuf);
        return SQLITE_CANTOPEN;
      }

      GC_CRASH_CHECK();
      rc = sqlite3OsWrite(pTmpFile, manifest, CHUNK_MANIFEST_SIZE, writeOff);
      writeOff += CHUNK_MANIFEST_SIZE;

      if( rc==SQLITE_OK && nNewData>0 ){
        const u8 *p = pNewData;
        int remaining = nNewData;
        while( remaining > 0 && rc==SQLITE_OK ){
          int toWrite = remaining > 65536 ? 65536 : remaining;
          GC_CRASH_CHECK();
          rc = sqlite3OsWrite(pTmpFile, p, toWrite, writeOff);
          p += toWrite;
          writeOff += toWrite;
          remaining -= toWrite;
        }
      }

      if( rc==SQLITE_OK && indexSize>0 ){
        const u8 *p = indexBuf;
        int remaining = indexSize;
        while( remaining > 0 && rc==SQLITE_OK ){
          int toWrite = remaining > 65536 ? 65536 : remaining;
          GC_CRASH_CHECK();
          rc = sqlite3OsWrite(pTmpFile, p, toWrite, writeOff);
          p += toWrite;
          writeOff += toWrite;
          remaining -= toWrite;
        }
      }

      if( rc==SQLITE_OK ){
        GC_CRASH_CHECK();
        rc = sqlite3OsSync(pTmpFile, SQLITE_SYNC_NORMAL);
      }
      sqlite3OsCloseFree(pTmpFile);

      if( rc==SQLITE_OK ){
        sqlite3_file *pOldFile = chunkFileGetHandle(&cs->file);
        sqlite3_file *pNewFile = 0;

        GC_CRASH_CHECK();
        if( rename(zTmp, chunkFileGetFilename(&cs->file))!=0 ){
          rc = SQLITE_IOERR;
        }

#if !defined(_WIN32) && !defined(WIN32)
        if( rc==SQLITE_OK ){
          char *zDir = sqlite3_mprintf("%s", chunkFileGetFilename(&cs->file));
          if( zDir ){
            int k = (int)strlen(zDir);
            while( k>0 && zDir[k-1]!='/' ) k--;
            if( k>0 ) zDir[k-1] = 0; else{ zDir[0]='.'; zDir[1]=0; }
            {
              int dfd = open(zDir, O_RDONLY);
              if( dfd>=0 ){
                GC_CRASH_CHECK();
                fsync(dfd);
                close(dfd);
              }
            }
            sqlite3_free(zDir);
          }
        }
#endif

        if( rc==SQLITE_OK ){
          int reopenFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
          int reopenAttempt;
          for(reopenAttempt=0; reopenAttempt<3; reopenAttempt++){
            pNewFile = 0;
            rc = sqlite3OsOpenMalloc(chunkFileGetVfs(&cs->file), chunkFileGetFilename(&cs->file), &pNewFile,
                                     reopenFlags, 0);
            if( rc==SQLITE_OK ) break;
            if( pNewFile ){
              sqlite3OsCloseFree(pNewFile);
              pNewFile = 0;
            }
          }
        }

        if( rc==SQLITE_OK ){
          chunkFileSetHandle(&cs->file, pNewFile);
          walStateSetDataSize(&cs->wal, 0);
          if( pOldFile ){
            sqlite3OsCloseFree(pOldFile);
          }
        }
      }else{
        chunkFileGetVfs(&cs->file)->xDelete(chunkFileGetVfs(&cs->file), zTmp, 0);
      }
    }
    sqlite3_free(zTmp);
  }

  sqlite3_free(indexBuf);
#ifdef SQLITE_TEST
  }
#undef GC_CRASH_CHECK
#endif
  return rc;
}

static int gcSweep(
  ChunkStore *cs,
  ProllyHashSet *marked,
  int *pKept,
  int *pRemoved
){
  int i, kept = 0, removed = 0;
  ChunkIndexEntry *aNewIndex = 0;
  int nNewIndex = 0;
  u8 *buf = 0;
  int nBuf = 0;
  int rc = SQLITE_OK;

  {
    int nIdx; const ChunkIndexEntry *aIdx;
    chunkIndexGetEntries(&cs->index, &nIdx, &aIdx);
    for(i=0; i<nIdx; i++){
      if( prollyHashSetContains(marked, &aIdx[i].hash) ){
        kept++;
      }else{
        removed++;
      }
    }
  }
  {
    int nPend; const ChunkIndexEntry *aPend;
    chunkStagingGetPending(&cs->staging, &nPend, &aPend);
    for(i=0; i<nPend; i++){
      if( prollyHashSetContains(marked, &aPend[i].hash) ){
        kept++;
      }
    }
  }
  {
    int nRec; const ChunkIndexEntry *aRec;
    chunkStagingGetRecent(&cs->staging, &nRec, &aRec);
    for(i=0; i<nRec; i++){
      if( prollyHashSetContains(marked, &aRec[i].hash) ){
        kept++;
      }
    }
  }

  if( removed==0 && chunkStagingRecentCount(&cs->staging)==0 ){
    *pKept = kept;
    *pRemoved = 0;
    return SQLITE_OK;
  }

  rc = gcBuildCompactedData(cs, marked, &buf, &nBuf, &aNewIndex, &nNewIndex);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcRewriteFile(cs, buf, nBuf, aNewIndex, nNewIndex);

  if( rc==SQLITE_OK ){
    int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
    chunkIndexReplaceEntries(&cs->index, aNewIndex, nNewIndex);
    chunkIndexSetMetadata(&cs->index, nNewIndex,
                          CHUNK_MANIFEST_SIZE + nBuf, indexSize);
    walStateSetOffset(&cs->wal, CHUNK_MANIFEST_SIZE + nBuf + indexSize);
    aNewIndex = 0;

    chunkStagingResetAfterSweep(&cs->staging);
  }

  sqlite3_free(aNewIndex);
  sqlite3_free(buf);

  *pKept = kept;
  *pRemoved = removed;
  return rc;
}

static void doltliteGcFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHashSet marked;
  int nKept = 0, nRemoved = 0;
  int rc;
  char result[128];

  (void)argc;
  (void)argv;

  if( !cs ){
    sqlite3_result_error(context, "no database", -1);
    return;
  }

  if( !chunkFileGetFilename(&cs->file) || strcmp(chunkFileGetFilename(&cs->file), ":memory:")==0 ){
    sqlite3_result_text(context, "0 chunks removed, 0 chunks kept (in-memory)", -1, SQLITE_TRANSIENT);
    return;
  }

  rc = chunkStoreLockAndRefresh(cs);
  if( rc==SQLITE_BUSY ){
    sqlite3_result_error(context,
      "database is locked by another connection", -1);
    return;
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to acquire lock for gc", -1);
    return;
  }

  rc = prollyHashSetInit(&marked, chunkIndexCount(&cs->index) > 64 ? chunkIndexCount(&cs->index) : 64);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    sqlite3_result_error(context, "out of memory", -1);
    return;
  }

  rc = gcMarkReachable(cs, &marked);
  if( rc!=SQLITE_OK ){
    prollyHashSetFree(&marked);
    chunkStoreUnlock(cs);
    sqlite3_result_error(context, "gc mark phase failed", -1);
    return;
  }

  rc = gcSweep(cs, &marked, &nKept, &nRemoved);
  prollyHashSetFree(&marked);
  chunkStoreUnlock(cs);

  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "gc sweep phase failed", -1);
    return;
  }

  sqlite3_snprintf(sizeof(result), result,
    "%d chunks removed, %d chunks kept", nRemoved, nKept);
  sqlite3_result_text(context, result, -1, SQLITE_TRANSIENT);
}

int doltliteGcCompact(sqlite3 *db){
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHashSet marked;
  int nKept = 0, nRemoved = 0;
  int rc;

  if( !cs ) return SQLITE_OK;
  if( !chunkFileGetFilename(&cs->file) || strcmp(chunkFileGetFilename(&cs->file), ":memory:")==0 ){
    return SQLITE_OK;
  }

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyHashSetInit(&marked, chunkIndexCount(&cs->index) > 64 ? chunkIndexCount(&cs->index) : 64);
  if( rc!=SQLITE_OK ){
    chunkStoreUnlock(cs);
    return rc;
  }

  rc = gcMarkReachable(cs, &marked);
  if( rc==SQLITE_OK ){
    rc = gcSweep(cs, &marked, &nKept, &nRemoved);
  }
  prollyHashSetFree(&marked);
  chunkStoreUnlock(cs);
  return rc;
}

int doltliteGcRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_gc", 0, SQLITE_UTF8, 0,
                                  doltliteGcFunc, 0, 0);
}

#endif
