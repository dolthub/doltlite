
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

  rc = gcQueuePush(&queue, &cs->refsHash);

  for(i=0; rc==SQLITE_OK && i<cs->nBranches; i++){
    rc = gcQueuePush(&queue, &cs->aBranches[i].commitHash);
    if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &cs->aBranches[i].workingSetHash);
  }

  for(i=0; rc==SQLITE_OK && i<cs->nTags; i++){
    rc = gcQueuePush(&queue, &cs->aTags[i].commitHash);
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
  int need;
  int rc;

  if( !prollyHashSetContains(marked, pHash) ) return SQLITE_OK;

  rc = chunkStoreGet(cs, pHash, &chunkData, &nChunkData);
  if( rc!=SQLITE_OK ) return rc;

  need = *pnBuf + 4 + nChunkData;
  if( need > *pnBufAlloc ){
    int newAlloc = *pnBufAlloc ? *pnBufAlloc * 2 : 65536;
    u8 *pNew;
    while( newAlloc < need ) newAlloc *= 2;
    pNew = sqlite3_realloc(*ppBuf, newAlloc);
    if( !pNew ){
      sqlite3_free(chunkData);
      return SQLITE_NOMEM;
    }
    *ppBuf = pNew;
    *pnBufAlloc = newAlloc;
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

  for(i=0; i<cs->nIndex; i++){
    if( prollyHashSetContains(marked, &cs->aIndex[i].hash) ) kept++;
  }
  for(i=0; i<cs->nRecent; i++){
    if( prollyHashSetContains(marked, &cs->aRecent[i].hash) ) kept++;
  }

  aNewIndex = sqlite3_malloc((kept ? kept : 1) * (int)sizeof(ChunkIndexEntry));
  if( !aNewIndex ) return SQLITE_NOMEM;

  for(i=0; i<cs->nIndex; i++){
    rc = gcAppendMarkedChunk(cs, &cs->aIndex[i].hash, marked, &buf, &nBuf,
                             &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aNewIndex);
      sqlite3_free(buf);
      return rc;
    }
  }
  for(i=0; i<cs->nRecent; i++){
    rc = gcAppendMarkedChunk(cs, &cs->aRecent[i].hash, marked, &buf, &nBuf,
                             &nBufAlloc, dataOffset, aNewIndex, &nNewIndex);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aNewIndex);
      sqlite3_free(buf);
      return rc;
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
  manifestCs.nChunks = nNewIndex;
  manifestCs.iIndexOffset = indexOffset;
  manifestCs.nIndexSize = indexSize;
  manifestCs.iWalOffset = indexOffset + indexSize;

  csSerializeManifest(&manifestCs, manifest);

  if( cs->zFilename && strcmp(cs->zFilename, ":memory:")!=0 ){
    char *zTmp = sqlite3_mprintf("%s-gc-tmp", cs->zFilename);
    if( !zTmp ){
      sqlite3_free(indexBuf);
      return SQLITE_NOMEM;
    }

    {
      sqlite3_file *pTmpFile = 0;
      int tmpFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                   | SQLITE_OPEN_MAIN_DB;
      i64 writeOff = 0;

      cs->pVfs->xDelete(cs->pVfs, zTmp, 0);

      rc = sqlite3OsOpenMalloc(cs->pVfs, zTmp, &pTmpFile, tmpFlags, 0);
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

        if( cs->pFile ){
          sqlite3OsCloseFree(cs->pFile);
          cs->pFile = 0;
        }

        GC_CRASH_CHECK();
        if( rename(zTmp, cs->zFilename)!=0 ){
          int reopenFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
          (void)sqlite3OsOpenMalloc(cs->pVfs, cs->zFilename,
                                    &cs->pFile, reopenFlags, 0);
          rc = SQLITE_IOERR;
        }

#if !defined(_WIN32) && !defined(WIN32)
        if( rc==SQLITE_OK ){
          char *zDir = sqlite3_mprintf("%s", cs->zFilename);
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
          cs->nWalData = 0;
        }

        if( rc==SQLITE_OK ){
          int reopenFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
          rc = sqlite3OsOpenMalloc(cs->pVfs, cs->zFilename, &cs->pFile,
                                   reopenFlags, 0);
        }
      }else{
        cs->pVfs->xDelete(cs->pVfs, zTmp, 0);
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

  for(i=0; i<cs->nIndex; i++){
    if( prollyHashSetContains(marked, &cs->aIndex[i].hash) ){
      kept++;
    }else{
      removed++;
    }
  }
  for(i=0; i<cs->nPending; i++){
    if( prollyHashSetContains(marked, &cs->aPending[i].hash) ){
      kept++;
    }
  }
  for(i=0; i<cs->nRecent; i++){
    if( prollyHashSetContains(marked, &cs->aRecent[i].hash) ){
      kept++;
    }
  }

  if( removed==0 && cs->nRecent==0 ){
    *pKept = kept;
    *pRemoved = 0;
    return SQLITE_OK;
  }

  rc = gcBuildCompactedData(cs, marked, &buf, &nBuf, &aNewIndex, &nNewIndex);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcRewriteFile(cs, buf, nBuf, aNewIndex, nNewIndex);

  if( rc==SQLITE_OK ){
    int indexSize = nNewIndex * CHUNK_INDEX_ENTRY_SIZE;
    sqlite3_free(cs->aIndex);
    cs->aIndex = aNewIndex;
    cs->nIndex = nNewIndex;
    cs->nChunks = nNewIndex;
    cs->iIndexOffset = CHUNK_MANIFEST_SIZE + nBuf;
    cs->nIndexSize = indexSize;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE + nBuf + indexSize;
    aNewIndex = 0;

    cs->nPending = 0;
    cs->nRecent = 0;
    sqlite3_free(cs->aRecentHT);
    sqlite3_free(cs->aRecentHTNext);
    cs->aRecentHT = 0;
    cs->aRecentHTNext = 0;
    cs->nRecentHTBuilt = 0;
    cs->nRecentHTNextAlloc = 0;
    cs->nRecentHTSize = 0;
    cs->nWriteBuf = 0;
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

  if( !cs->zFilename || strcmp(cs->zFilename, ":memory:")==0 ){
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

  rc = prollyHashSetInit(&marked, cs->nIndex > 64 ? cs->nIndex : 64);
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
  if( !cs->zFilename || strcmp(cs->zFilename, ":memory:")==0 ){
    return SQLITE_OK;
  }

  rc = chunkStoreLockAndRefresh(cs);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyHashSetInit(&marked, cs->nIndex > 64 ? cs->nIndex : 64);
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
