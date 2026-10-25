#ifdef DOLTLITE_PROLLY

#include "doltlite_remote.h"
#include <stdio.h>
#include <string.h>

static int nPass;
static int nFail;
static const sqlite3_io_methods *pMethods;
static i64 iFirst, iLast, iFault = -1;
static int nReadCall, nMaxRead, bIoError;
static sqlite3_mem_methods allocator;
static int failAfter = -1;

static void *testMalloc(int n){
  if( failAfter==0 ) return 0;
  if( failAfter>0 ) failAfter--;
  return allocator.xMalloc(n);
}

static void *testRealloc(void *p, int n){
  if( failAfter==0 ) return 0;
  if( failAfter>0 ) failAfter--;
  return allocator.xRealloc(p, n);
}

static void check(const char *zName, int ok){
  if( ok ) nPass++;
  else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", zName);
  }
}

static int countedRead(sqlite3_file *pFile, void *pBuf, int n, i64 off){
  int rc;
  if( off>=iFirst && off<iLast ){
    nReadCall++;
    if( n>nMaxRead ) nMaxRead = n;
    if( bIoError ) return SQLITE_IOERR_READ;
  }
  rc = pMethods->xRead(pFile, pBuf, n, off);
  if( rc==SQLITE_OK && iFault>=off && iFault-off<n ){
    ((u8*)pBuf)[iFault-off] ^= 1;
  }
  return rc;
}

static void fill(u8 *p, int n, int value){
  memset(p, value, n);
  if( n>=sizeof(value) ) memcpy(p, &value, sizeof(value));
}

static int readAll(ChunkStore *cs, const ProllyHash *aHash, int nHash,
                   u8 **apData, int *anData){
  int i = 0;
  while( i<nHash ){
    int nRead = 0;
    int rc = chunkStoreReadBatch(cs, &aHash[i],
        MIN(nHash-i, CHUNK_READ_BATCH_MAX), &apData[i], &anData[i], &nRead);
    if( rc!=SQLITE_OK ) return rc;
    if( nRead<=0 ) return SQLITE_ERROR;
    i += nRead;
  }
  return SQLITE_OK;
}

static void freeResults(u8 **apData, int n){
  int i;
  for(i=0; i<n; i++){
    sqlite3_free(apData[i]);
    apData[i] = 0;
  }
}

static void testFile(void){
  const char *zPath = "test_chunk_store_read_batch.db";
  ChunkStore cs;
  ChunkIndexEntry aEntry[40];
  ProllyHash aHash[40], aRequest[40], large, empty, missing, pending;
  u8 *apData[40] = {0};
  int anData[40] = {0};
  u8 *data = 0;
  sqlite3_io_methods methods;
  int i, nRead, nScalar, rc;

  remove(zPath);
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), zPath,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
  check("file open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  data = sqlite3_malloc(70000);
  check("allocate test payload", data!=0);
  if( !data ){
    chunkStoreClose(&cs);
    return;
  }
  for(i=0; i<40; i++){
    fill(data, 4096, i);
    check("file put", chunkStorePut(&cs, data, 4096, &aHash[i])==SQLITE_OK);
  }
  fill(data, 70000, 41);
  check("large put", chunkStorePut(&cs, data, 70000, &large)==SQLITE_OK);
  check("empty put", chunkStorePut(&cs, (const u8*)"", 0, &empty)==SQLITE_OK);
  check("file commit", chunkStoreCommit(&cs)==SQLITE_OK);
  chunkStoreClose(&cs);
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), zPath, SQLITE_OPEN_READWRITE);
  check("file reopen", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ){ sqlite3_free(data); return; }
  iFirst = LARGEST_INT64;
  iLast = 0;
  for(i=0; i<40; i++){
    int found = 0;
    rc = csIndexLookup(&cs, &aHash[i], &aEntry[i], &found);
    check("locate chunk", rc==SQLITE_OK && found);
    if( aEntry[i].offset<iFirst ) iFirst = aEntry[i].offset;
    if( aEntry[i].offset+4100>iLast ) iLast = aEntry[i].offset+4100;
    aRequest[i] = aHash[39-i];
  }
  pMethods = cs.file.pFile->pMethods;
  methods = *pMethods;
  methods.xRead = countedRead;
  cs.file.pFile->pMethods = &methods;
  nReadCall = nMaxRead = 0;
  for(i=0; i<40; i++){
    rc = chunkStoreGet(&cs, &aRequest[i], &apData[i], &anData[i]);
    check("scalar control", rc==SQLITE_OK);
  }
  nScalar = nReadCall;
  freeResults(apData, 40);
  nReadCall = nMaxRead = 0;
  rc = readAll(&cs, aRequest, 40, apData, anData);
  check("batch reverse reads", rc==SQLITE_OK);
  for(i=0; i<40; i++){
    fill(data, 4096, 39-i);
    check("batch request order", apData[i] && anData[i]==4096
        && memcmp(apData[i], data, 4096)==0);
  }
  check("fewer data reads", nScalar==40 && nReadCall<nScalar/2);
  check("bounded reads", nMaxRead<=CHUNK_READ_BATCH_BYTES);
  printf("data reads: scalar=%d batch=%d max_bytes=%d\n",
         nScalar, nReadCall, nMaxRead);
  freeResults(apData, 40);
  aRequest[0] = aHash[0];
  aRequest[1] = aHash[2];
  nReadCall = nMaxRead = 0;
  rc = readAll(&cs, aRequest, 2, apData, anData);
  check("skip unrequested chunk gap", rc==SQLITE_OK && nReadCall==2
        && nMaxRead==4100);
  freeResults(apData, 2);

  for(i=0; i<3; i++){
    int j;
    for(j=0; j<4; j++) aRequest[j] = aHash[j];
    nRead = 99;
    iFault = i==0 ? aEntry[2].offset : aEntry[2].offset+4;
    bIoError = i==2;
    rc = chunkStoreReadBatch(&cs, aRequest, 4, apData, anData, &nRead);
    check("batch rejects corrupt length/data or I/O error",
          rc==(i==2 ? SQLITE_IOERR_READ : SQLITE_CORRUPT) && nRead==0);
    for(j=0; j<4; j++){
      check("error clears all outputs", apData[j]==0 && anData[j]==0);
    }
    iFault = -1;
    bIoError = 0;
  }

  for(i=0; i<24; i++){
    sqlite3_int64 nBefore = sqlite3_memory_used();
    int j;
    failAfter = i;
    rc = chunkStoreReadBatch(&cs, aRequest, 4, apData, anData, &nRead);
    failAfter = -1;
    check("allocation failure result", rc==SQLITE_OK || rc==SQLITE_NOMEM);
    if( rc!=SQLITE_OK ){
      check("allocation failure consumes no input", nRead==0);
      for(j=0; j<4; j++){
        check("allocation failure clears outputs", !apData[j] && anData[j]==0);
      }
    }
    freeResults(apData, 4);
    check("allocation failure frees buffers", sqlite3_memory_used()==nBefore);
  }

  aRequest[0] = aHash[0];
  aRequest[1] = aHash[0];
  aRequest[2] = aHash[1];
  rc = readAll(&cs, aRequest, 3, apData, anData);
  check("duplicate reads", rc==SQLITE_OK && apData[0] && apData[1]
        && apData[0]!=apData[1] && memcmp(apData[0], apData[1], 4096)==0);
  freeResults(apData, 3);

  prollyHashCompute("missing", 7, &missing);
  aRequest[0] = aHash[0];
  aRequest[1] = missing;
  aRequest[2] = large;
  aRequest[3] = empty;
  aRequest[4] = aHash[39];
  nReadCall = nMaxRead = 0;
  rc = readAll(&cs, aRequest, 5, apData, anData);
  fill(data, 70000, 41);
  check("mixed missing, large and empty chunks", rc==SQLITE_OK
        && apData[0] && !apData[1] && anData[1]==0
        && apData[2] && anData[2]==70000
        && memcmp(apData[2], data, 70000)==0
        && apData[3] && anData[3]==0 && apData[4]);
  check("large gap is not read", nReadCall==2);
  freeResults(apData, 5);

  fill(data, 4096, 90);
  check("pending put", chunkStorePut(&cs, data, 4096, &pending)==SQLITE_OK);
  aRequest[0] = aHash[0];
  aRequest[1] = pending;
  aRequest[2] = aHash[1];
  rc = readAll(&cs, aRequest, 3, apData, anData);
  check("pending chunk fallback", rc==SQLITE_OK && apData[1]
        && anData[1]==4096 && memcmp(apData[1], data, 4096)==0);
  freeResults(apData, 3);
  chunkStoreRollback(&cs);
  cs.file.pFile->pMethods = pMethods;
  {
    DoltliteRemote *pRemote = doltliteFsRemoteOpen(sqlite3_vfs_find(0), zPath);
    check("file remote exposes batch reads", pRemote && pRemote->xGetChunks);
    if( pRemote && pRemote->xGetChunks ){
      check("empty remote batch",
            pRemote->xGetChunks(pRemote, 0, 0, 0, 0)==SQLITE_OK);
      aRequest[0] = aHash[0];
      aRequest[1] = missing;
      aRequest[2] = aHash[0];
      rc = pRemote->xGetChunks(pRemote, aRequest, 3, apData, anData);
      check("remote batch preserves order and missing slots", rc==SQLITE_OK
          && apData[0] && !apData[1] && apData[2]
          && anData[0]==4096 && anData[1]==0 && anData[2]==4096
          && apData[0]!=apData[2]
          && memcmp(apData[0], apData[2], 4096)==0);
      freeResults(apData, 3);
    }
    if( pRemote ) pRemote->xClose(pRemote);
  }
  chunkStoreClose(&cs);
  sqlite3_free(data);
  remove(zPath);
}

static void testMemory(void){
  ChunkStore cs;
  ProllyHash aHash[2];
  u8 *apData[2] = {0};
  int anData[2], nRead, i, rc;
  rc = chunkStoreOpen(&cs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("memory open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  check("memory put", chunkStorePut(&cs, (const u8*)"abc", 3,
                                    &aHash[0])==SQLITE_OK);
  check("memory sparse put", chunkStorePutSparse(&cs, (const u8*)"abc",
      3, 4093, &aHash[1])==SQLITE_OK);
  for(i=0; i<2; i++){
    rc = readAll(&cs, aHash, 2, apData, anData);
    check("memory pending and committed reads", rc==SQLITE_OK
          && anData[0]==3 && anData[1]==4096
          && apData[0] && memcmp(apData[0], "abc", 3)==0
          && apData[1] && memcmp(apData[1], "abc", 3)==0
          && apData[1][4095]==0);
    freeResults(apData, 2);
    check("memory commit", chunkStoreCommit(&cs)==SQLITE_OK);
  }
  check("empty batch", chunkStoreReadBatch(&cs, 0, 0, 0, 0,
                                            &nRead)==SQLITE_OK && nRead==0);
  check("negative count", chunkStoreReadBatch(&cs, 0, -1, 0, 0,
                                               &nRead)==SQLITE_MISUSE);
  check("oversized count", chunkStoreReadBatch(&cs, 0,
      CHUNK_READ_BATCH_MAX+1, 0, 0, &nRead)==SQLITE_MISUSE);
  cs.notADatabase = 1;
  check("invalid store rejected", chunkStoreReadBatch(&cs, aHash, 2,
      apData, anData, &nRead)==SQLITE_NOTADB && nRead==0
      && !apData[0] && !apData[1] && anData[0]==0 && anData[1]==0);
  cs.notADatabase = 0;
  cs.corruptMidStream = 1;
  check("corrupt store rejected", chunkStoreReadBatch(&cs, aHash, 2,
      apData, anData, &nRead)==SQLITE_CORRUPT && nRead==0
      && !apData[0] && !apData[1] && anData[0]==0 && anData[1]==0);
  cs.corruptMidStream = 0;
  chunkStoreClose(&cs);
}

int main(void){
  sqlite3_mem_methods methods;
  if( sqlite3_config(SQLITE_CONFIG_GETMALLOC, &allocator)!=SQLITE_OK ) return 1;
  methods = allocator;
  methods.xMalloc = testMalloc;
  methods.xRealloc = testRealloc;
  if( sqlite3_config(SQLITE_CONFIG_MALLOC, &methods)!=SQLITE_OK ) return 1;
  sqlite3_initialize();
  testFile();
  testMemory();
  printf("%d passed, %d failed\n", nPass, nFail);
  sqlite3_shutdown();
  return nFail ? 1 : 0;
}

#else
int main(void){ return 0; }
#endif
