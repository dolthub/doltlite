#ifdef DOLTLITE_PROLLY

#include "doltlite_commit.h"
#include "doltlite_remote.h"
#include <stdio.h>
#include <string.h>

typedef struct FakeRemote FakeRemote;
struct FakeRemote {
  DoltliteRemote base;
  const u8 *pData;
  int nData;
  int nPut;
};

static int failures;

int doltliteRemoteSrvStoreChunkBatchForTest(
  ChunkStore *pStore, const u8 *pBody, int nBody
);

static void check(const char *zName, int ok){
  if( !ok ){
    fprintf(stderr, "FAIL: %s\n", zName);
    failures++;
  }
}

static int fakeGetChunk(
  DoltliteRemote *pRemote,
  const ProllyHash *pHash,
  u8 **ppData,
  int *pnData
){
  FakeRemote *p = (FakeRemote*)pRemote;
  (void)pHash;
  *ppData = sqlite3_malloc(p->nData);
  if( !*ppData ) return SQLITE_NOMEM;
  memcpy(*ppData, p->pData, p->nData);
  *pnData = p->nData;
  return SQLITE_OK;
}

static int fakeGetChunks(
  DoltliteRemote *pRemote,
  const ProllyHash *aHash,
  int nHash,
  u8 **apData,
  int *anData
){
  int i;
  for(i=0; i<nHash; i++){
    int rc = fakeGetChunk(pRemote, &aHash[i], &apData[i], &anData[i]);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int fakeHasChunks(
  DoltliteRemote *pRemote,
  const ProllyHash *aHash,
  int nHash,
  u8 *aResult
){
  (void)pRemote;
  (void)aHash;
  memset(aResult, 0, nHash);
  return SQLITE_OK;
}

static int fakePutChunk(
  DoltliteRemote *pRemote,
  const ProllyHash *pHash,
  const u8 *pData,
  int nData
){
  FakeRemote *p = (FakeRemote*)pRemote;
  (void)pHash;
  (void)pData;
  (void)nData;
  p->nPut++;
  return SQLITE_OK;
}

static int syncOnce(
  const u8 *pData,
  int nData,
  const ProllyHash *pRoot,
  int bBatch,
  int *pnPut
){
  FakeRemote src;
  FakeRemote dst;
  ProllyHash root = *pRoot;
  int rc;

  memset(&src, 0, sizeof(src));
  memset(&dst, 0, sizeof(dst));
  src.pData = pData;
  src.nData = nData;
  src.base.xGetChunk = fakeGetChunk;
  if( bBatch ) src.base.xGetChunks = fakeGetChunks;
  dst.base.xHasChunks = fakeHasChunks;
  dst.base.xPutChunk = fakePutChunk;
  rc = doltliteSyncChunks(&src.base, &dst.base, &root, 1);
  *pnPut = dst.nPut;
  return rc;
}

static u8 *makeRefsBlob(const ProllyHash *pCommit, int *pnBlob){
  ChunkStore refs;
  u8 *pBlob = 0;
  memset(&refs, 0, sizeof(refs));
  if( chunkStoreSetDefaultBranch(&refs, "main")!=SQLITE_OK ) return 0;
  if( chunkStoreAddBranch(&refs, "main", pCommit)!=SQLITE_OK ){
    chunkStoreClose(&refs);
    return 0;
  }
  if( chunkStoreSerializeRefsToBlob(&refs, &pBlob, pnBlob)!=SQLITE_OK ){
    pBlob = 0;
  }
  chunkStoreClose(&refs);
  return pBlob;
}

static void putLength(u8 *p, int n){
  p[0] = (u8)(n & 0xff);
  p[1] = (u8)((n >> 8) & 0xff);
  p[2] = (u8)((n >> 16) & 0xff);
  p[3] = (u8)((n >> 24) & 0xff);
}

int main(void){
  DoltliteCommit commit;
  ChunkStore store;
  ProllyHash actual;
  ProllyHash wrong;
  u8 *pCommit = 0;
  u8 *pBody = 0;
  u8 *pRefs = 0;
  int nCommit = 0;
  int nRecord;
  int nRefs = 0;
  int nPut = 0;
  int has = 0;
  int rc;

  sqlite3_initialize();
  memset(&commit, 0, sizeof(commit));
  commit.timestamp = 1;
  commit.zName = "n";
  commit.zEmail = "e";
  commit.zMessage = "m";
  rc = doltliteCommitSerialize(&commit, &pCommit, &nCommit);
  check("serialize commit", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return 1;
  prollyHashCompute(pCommit, nCommit, &actual);
  wrong = actual;
  wrong.data[0] ^= 0xff;

  rc = syncOnce(pCommit, nCommit, &wrong, 0, &nPut);
  check("single fetch rejects mismatched hash", rc==SQLITE_CORRUPT);
  check("single fetch stores nothing", nPut==0);
  rc = syncOnce(pCommit, nCommit, &wrong, 1, &nPut);
  check("batch fetch rejects mismatched hash", rc==SQLITE_CORRUPT);
  check("batch fetch stores nothing", nPut==0);
  rc = syncOnce(pCommit, nCommit, &actual, 1, &nPut);
  check("batch fetch accepts matching hash", rc==SQLITE_OK);
  check("matching batch stores chunk", nPut==1);

  memset(&store, 0, sizeof(store));
  rc = chunkStoreOpen(&store, sqlite3_vfs_find(0), ":memory:",
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB);
  check("open chunk store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) goto done;

  nRecord = PROLLY_HASH_SIZE + 4 + nCommit;
  pBody = sqlite3_malloc64((sqlite3_uint64)nRecord * 2);
  check("allocate upload body", pBody!=0);
  if( !pBody ) goto close_store;
  memcpy(pBody, actual.data, PROLLY_HASH_SIZE);
  putLength(pBody+PROLLY_HASH_SIZE, nCommit);
  memcpy(pBody+PROLLY_HASH_SIZE+4, pCommit, nCommit);
  memcpy(pBody+nRecord, wrong.data, PROLLY_HASH_SIZE);
  putLength(pBody+nRecord+PROLLY_HASH_SIZE, nCommit);
  memcpy(pBody+nRecord+PROLLY_HASH_SIZE+4, pCommit, nCommit);

  rc = doltliteRemoteSrvStoreChunkBatchForTest(&store, pBody, nRecord*2);
  check("upload rejects mismatched declared hash", rc==SQLITE_PROTOCOL);
  rc = chunkStoreHas(&store, &actual, &has);
  check("rejected upload rolls back whole batch", rc==SQLITE_OK && !has);
  rc = doltliteRemoteSrvStoreChunkBatchForTest(&store, pBody, nRecord);
  check("upload accepts matching declared hash", rc==SQLITE_OK);
  rc = chunkStoreHas(&store, &actual, &has);
  check("accepted upload stores chunk", rc==SQLITE_OK && has);
  rc = doltliteRemoteSrvStoreChunkBatchForTest(&store, pBody, nRecord-1);
  check("upload rejects truncated record", rc==SQLITE_PROTOCOL);

  pRefs = makeRefsBlob(&wrong, &nRefs);
  check("serialize refs", pRefs!=0);
  if( pRefs ){
    rc = doltliteValidateRefsTargetGraph(&store, pRefs, nRefs, "main");
    check("refs reject missing reachable graph", rc==SQLITE_CORRUPT);
    sqlite3_free(pRefs);
    pRefs = makeRefsBlob(&actual, &nRefs);
    check("serialize valid refs", pRefs!=0);
    if( pRefs ){
      rc = doltliteValidateRefsTargetGraph(&store, pRefs, nRefs, "main");
      check("refs accept complete reachable graph", rc==SQLITE_OK);
    }
  }

close_store:
  chunkStoreClose(&store);
done:
  sqlite3_free(pRefs);
  sqlite3_free(pBody);
  sqlite3_free(pCommit);
  if( failures ){
    fprintf(stderr, "remote_chunk_integrity_test: %d failure(s)\n", failures);
    return 1;
  }
  printf("remote_chunk_integrity_test: all passed\n");
  return 0;
}

#else
int main(void){ return 0; }
#endif
