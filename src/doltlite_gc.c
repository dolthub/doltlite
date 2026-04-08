
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>
#include <stdio.h>

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

#define PROLLY_NODE_MAGIC_VAL 0x504E4F44

static int isCommitChunk(const u8 *data, int nData){
  
  if( nData < 30 ) return 0;
  if( data[0] != DOLTLITE_COMMIT_V2 ) return 0;
  
  if( nData >= 4 ){
    u32 m = (u32)data[0] | ((u32)data[1]<<8) |
            ((u32)data[2]<<16) | ((u32)data[3]<<24);
    if( m == PROLLY_NODE_MAGIC_VAL ) return 0;
  }
  return 1;
}

static int isProllyNodeChunk(const u8 *data, int nData){
  u32 m;
  if( nData < 8 ) return 0;
  m = (u32)data[0] | ((u32)data[1]<<8) |
      ((u32)data[2]<<16) | ((u32)data[3]<<24);
  return m == PROLLY_NODE_MAGIC_VAL;
}

/* BFS from all roots (manifest hashes + all branch/tag refs + working sets)
** to mark every reachable chunk. Understands three chunk types: prolly nodes
** (recurse into children), commits (follow parents + catalog), and catalog
** blobs (follow table root hashes). */
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

  /* Mark the per-branch working state chunk and all catalog hashes within it. */
  if( rc==SQLITE_OK && !prollyHashIsEmpty(&cs->workingState) ){
    rc = gcQueuePush(&queue, &cs->workingState);
    if( rc==SQLITE_OK ){
      u8 *wsData = 0; int nWsData = 0;
      if( chunkStoreGet(cs, &cs->workingState, &wsData, &nWsData)==SQLITE_OK
       && wsData && nWsData >= 4 ){
        int nBr = (int)((u32)wsData[0] | ((u32)wsData[1]<<8) | ((u32)wsData[2]<<16) | ((u32)wsData[3]<<24));
        const u8 *pp = wsData + 4;
        int j;
        for(j=0; j<nBr && rc==SQLITE_OK; j++){
          int nl;
          if( pp + 4 > wsData + nWsData ) break;
          nl = (int)((u32)pp[0] | ((u32)pp[1]<<8) | ((u32)pp[2]<<16) | ((u32)pp[3]<<24));
          pp += 4;
          if( pp + nl + PROLLY_HASH_SIZE > wsData + nWsData ) break;
          {
            ProllyHash brCat;
            memcpy(brCat.data, pp + nl, PROLLY_HASH_SIZE);
            rc = gcQueuePush(&queue, &brCat);
          }
          pp += nl + PROLLY_HASH_SIZE;
        }
        sqlite3_free(wsData);
      }
    }
  }

  
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
      
      continue;
    }

    if( isProllyNodeChunk(data, nData) ){
      
      ProllyNode node;
      int parseRc = prollyNodeParse(&node, data, nData);
      if( parseRc==SQLITE_OK && node.level > 0 ){
        for(i=0; i<(int)node.nItems; i++){
          ProllyHash childHash;
          prollyNodeChildHash(&node, i, &childHash);
          rc = gcQueuePush(&queue, &childHash);
          if( rc!=SQLITE_OK ) break;
        }
      }
    }else if( isCommitChunk(data, nData) ){

      DoltliteCommit commit;
      int drc;
      memset(&commit, 0, sizeof(commit));
      drc = doltliteCommitDeserialize(data, nData, &commit);
      if( drc==SQLITE_OK ){
        int pi;
        for(pi=0; pi<commit.nParents; pi++){
          rc = gcQueuePush(&queue, &commit.aParents[pi]);
          if( rc!=SQLITE_OK ) break;
        }
        if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &commit.catalogHash);
        doltliteCommitClear(&commit);
      }
    }else{
      
      
      if( nData == WS_TOTAL_SIZE && data[0] == 1 ){
        ProllyHash stagedCat, conflictsCat;
        memcpy(stagedCat.data, data + WS_STAGED_OFF, PROLLY_HASH_SIZE);
        memcpy(conflictsCat.data, data + WS_CONFLICTS_OFF, PROLLY_HASH_SIZE);
        rc = gcQueuePush(&queue, &stagedCat);
        if( rc==SQLITE_OK ) rc = gcQueuePush(&queue, &conflictsCat);
        if( rc==SQLITE_OK && data[WS_MERGING_OFF] ){  
          ProllyHash mergeCommit;
          memcpy(mergeCommit.data, data + WS_MERGE_COMMIT_OFF, PROLLY_HASH_SIZE);
          rc = gcQueuePush(&queue, &mergeCommit);
        }
      }
      
      if( nData >= CAT_HEADER_SIZE && data[0] == CATALOG_FORMAT_V2 ){
        int nTables = (int)(data[CAT_NUM_TABLES_OFF]
                          | (data[CAT_NUM_TABLES_OFF+1]<<8)
                          | (data[CAT_NUM_TABLES_OFF+2]<<16)
                          | (data[CAT_NUM_TABLES_OFF+3]<<24));
        if( nTables >= 0 && nTables < 10000 ){
          const u8 *p = data + CAT_HEADER_SIZE;
          for(i=0; i<nTables; i++){
            if( p + CAT_ENTRY_FIXED_SIZE > data + nData ) break;
            {
              ProllyHash tableRoot;
              memcpy(tableRoot.data, p + CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE,
                     PROLLY_HASH_SIZE);
              rc = gcQueuePush(&queue, &tableRoot);
              if( rc!=SQLITE_OK ) break;
            }
            {
              int nameLen;
              p += CAT_ENTRY_ITABLE_SIZE + CAT_ENTRY_FLAGS_SIZE
                 + PROLLY_HASH_SIZE + PROLLY_HASH_SIZE;
              nameLen = p[0] | (p[1]<<8);
              p += 2 + nameLen;
            }
          }
        }
      }
    }

    sqlite3_free(data);
    if( rc!=SQLITE_OK ) break;
  }

  gcQueueFree(&queue);
  return rc;
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

  aNewIndex = sqlite3_malloc(kept * (int)sizeof(ChunkIndexEntry));
  if( !aNewIndex ) return SQLITE_NOMEM;

  for(i=0; i<cs->nIndex; i++){
    u8 *chunkData = 0;
    int nChunkData = 0;

    if( !prollyHashSetContains(marked, &cs->aIndex[i].hash) ) continue;

    rc = chunkStoreGet(cs, &cs->aIndex[i].hash, &chunkData, &nChunkData);
    if( rc!=SQLITE_OK ){
      sqlite3_free(aNewIndex);
      sqlite3_free(buf);
      return rc;
    }

    
    {
      int need = nBuf + 4 + nChunkData;
      if( need > nBufAlloc ){
        int newAlloc = nBufAlloc ? nBufAlloc * 2 : 65536;
        while( newAlloc < need ) newAlloc *= 2;
        buf = sqlite3_realloc(buf, newAlloc);
        if( !buf ){
          sqlite3_free(chunkData);
          sqlite3_free(aNewIndex);
          return SQLITE_NOMEM;
        }
        nBufAlloc = newAlloc;
      }
    }

    
    buf[nBuf]   = (u8)(nChunkData);
    buf[nBuf+1] = (u8)(nChunkData>>8);
    buf[nBuf+2] = (u8)(nChunkData>>16);
    buf[nBuf+3] = (u8)(nChunkData>>24);

    memcpy(&aNewIndex[nNewIndex].hash, &cs->aIndex[i].hash, sizeof(ProllyHash));
    aNewIndex[nNewIndex].offset = dataOffset + nBuf;
    aNewIndex[nNewIndex].size = nChunkData;
    nNewIndex++;

    memcpy(buf + nBuf + 4, chunkData, nChunkData);
    nBuf += 4 + nChunkData;

    sqlite3_free(chunkData);
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
  int rc = SQLITE_OK;

  
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

  
  cs->nChunks = nNewIndex;
  cs->iIndexOffset = indexOffset;
  cs->nIndexSize = indexSize;
  cs->iWalOffset = indexOffset + indexSize;

  csSerializeManifest(cs, manifest);

  
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

      
      rc = sqlite3OsWrite(pTmpFile, manifest, CHUNK_MANIFEST_SIZE, writeOff);
      writeOff += CHUNK_MANIFEST_SIZE;

      
      if( rc==SQLITE_OK && nNewData>0 ){
        const u8 *p = pNewData;
        int remaining = nNewData;
        while( remaining > 0 && rc==SQLITE_OK ){
          int toWrite = remaining > 65536 ? 65536 : remaining;
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
          rc = sqlite3OsWrite(pTmpFile, p, toWrite, writeOff);
          p += toWrite;
          writeOff += toWrite;
          remaining -= toWrite;
        }
      }

      
      if( rc==SQLITE_OK ){
        rc = sqlite3OsSync(pTmpFile, SQLITE_SYNC_NORMAL);
      }
      sqlite3OsCloseFree(pTmpFile);

      if( rc==SQLITE_OK ){
        
        /* Close the old file before rename. The file handle must be released
        ** first or rename() will fail on Windows. After rename, reopen. */
        if( cs->pFile ){
          sqlite3OsCloseFree(cs->pFile);
          cs->pFile = 0;
        }

        if( rename(zTmp, cs->zFilename)!=0 ){
          rc = SQLITE_IOERR;
        }

        if( rc==SQLITE_OK ){
          sqlite3_free(cs->pWalData);
          cs->pWalData = 0;
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

  if( removed==0 ){
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
    cs->nIndexAlloc = nNewIndex;
    cs->nChunks = nNewIndex;
    cs->iIndexOffset = CHUNK_MANIFEST_SIZE + nBuf;
    cs->nIndexSize = indexSize;
    cs->iWalOffset = CHUNK_MANIFEST_SIZE + nBuf + indexSize;
    aNewIndex = 0;  

    cs->nPending = 0;
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

  /* Acquire exclusive write lock for the entire GC operation.
  ** GC rewrites the file — no other connection or process may be
  ** reading or writing while this happens. */
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

  rc = prollyHashSetInit(&marked, cs->nIndex > 64 ? cs->nIndex : 64);
  if( rc!=SQLITE_OK ) return rc;

  rc = gcMarkReachable(cs, &marked);
  if( rc==SQLITE_OK ){
    rc = gcSweep(cs, &marked, &nKept, &nRemoved);
  }
  prollyHashSetFree(&marked);
  return rc;
}

int doltliteGcRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_gc", 0, SQLITE_UTF8, 0,
                                  doltliteGcFunc, 0, 0);
}

#endif 
