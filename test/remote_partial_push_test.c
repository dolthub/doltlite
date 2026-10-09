#ifdef DOLTLITE_PROLLY

#include "sqlite3.h"
#include "doltlite_commit.h"
#include "doltlite_remote.h"
#include "lib/test_tmpdir.h"
#include <stdio.h>
#include <string.h>

int doltliteRemoteSrvApplyScopedRefsForTest(
  ChunkStore *pStore, const char *zBranch, int bForce,
  const u8 *pBody, int nBody
);

static int failures;

static void check(const char *zName, int ok){
  if( !ok ){
    fprintf(stderr, "FAIL: %s\n", zName);
    failures++;
  }
}

typedef struct StoreRemote StoreRemote;
struct StoreRemote {
  DoltliteRemote base;
  ChunkStore *pStore;
};

static int srGetChunk(DoltliteRemote *p, const ProllyHash *pHash,
                      u8 **ppData, int *pnData){
  return chunkStoreGet(((StoreRemote*)p)->pStore, pHash, ppData, pnData);
}

static int srPutChunk(DoltliteRemote *p, const ProllyHash *pHash,
                      const u8 *pData, int nData){
  (void)pHash;
  return chunkStorePut(((StoreRemote*)p)->pStore, pData, nData, 0);
}

static int srHasChunks(DoltliteRemote *p, const ProllyHash *aHash, int nHash,
                       u8 *aResult){
  int i;
  for(i=0; i<nHash; i++){
    int has = 0;
    int rc = chunkStoreHas(((StoreRemote*)p)->pStore, &aHash[i], &has);
    if( rc!=SQLITE_OK ) return rc;
    aResult[i] = (u8)has;
  }
  return SQLITE_OK;
}

static void initRemote(StoreRemote *p, ChunkStore *pStore){
  memset(p, 0, sizeof(*p));
  p->pStore = pStore;
  p->base.xGetChunk = srGetChunk;
  p->base.xPutChunk = srPutChunk;
  p->base.xHasChunks = srHasChunks;
}

static int syncInto(ChunkStore *pSrc, ChunkStore *pDst, const ProllyHash *pRoot,
                    int bForce, int *pbFlagAfter){
  StoreRemote src, dst;
  ProllyHash root = *pRoot;
  int rc;
  initRemote(&src, pSrc);
  initRemote(&dst, pDst);
  dst.base.bForceResumeScan = bForce;
  rc = chunkStoreLockAndRefresh(pDst);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteSyncChunks(&src.base, &dst.base, &root, 1);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(pDst);
  chunkStoreUnlock(pDst);
  if( pbFlagAfter ) *pbFlagAfter = dst.base.bForceResumeScan;
  return rc;
}

static int putOne(ChunkStore *pSrc, ChunkStore *pDst, const ProllyHash *pHash){
  u8 *pData = 0;
  int nData = 0;
  int rc = chunkStoreGet(pSrc, pHash, &pData, &nData);
  if( rc==SQLITE_OK ) rc = chunkStoreLockAndRefresh(pDst);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(pDst, pData, nData, 0);
    if( rc==SQLITE_OK ) rc = chunkStoreCommit(pDst);
    chunkStoreUnlock(pDst);
  }
  sqlite3_free(pData);
  return rc;
}

static u8 *refsBlobFor(const ProllyHash *pCommit, int *pnBlob){
  ChunkStore refs;
  u8 *pBlob = 0;
  memset(&refs, 0, sizeof(refs));
  if( chunkStoreSetDefaultBranch(&refs, "main")==SQLITE_OK
   && chunkStoreAddBranch(&refs, "main", pCommit)==SQLITE_OK
   && chunkStoreSerializeRefsToBlob(&refs, &pBlob, pnBlob)!=SQLITE_OK ){
    pBlob = 0;
  }
  chunkStoreClose(&refs);
  return pBlob;
}

static int headHash(sqlite3 *db, ProllyHash *pOut){
  sqlite3_stmt *p = 0;
  int rc = sqlite3_prepare_v2(db, "SELECT dolt_hashof('HEAD')", -1, &p, 0);
  if( rc==SQLITE_OK && sqlite3_step(p)==SQLITE_ROW ){
    rc = doltliteHexToHash((const char*)sqlite3_column_text(p, 0), pOut);
  }else if( rc==SQLITE_OK ){
    rc = SQLITE_ERROR;
  }
  sqlite3_finalize(p);
  return rc;
}

int main(void){
  const char *zLocal = DOLTLITE_TEST_TMPDIR "/partial_push_local.db";
  const char *zServer = DOLTLITE_TEST_TMPDIR "/partial_push_server.db";
  char zLock[512];
  sqlite3 *db = 0;
  ChunkStore local, server;
  ProllyHash c1, c2;
  u8 *pBlob = 0;
  int nBlob = 0;
  int flagAfter = -1;
  int rc;

  remove(zLocal);
  remove(zServer);
  snprintf(zLock, sizeof(zLock), DOLTLITE_TEST_TMPDIR "/.partial_push_local.db-lock");
  remove(zLock);
  snprintf(zLock, sizeof(zLock), DOLTLITE_TEST_TMPDIR "/.partial_push_server.db-lock");
  remove(zLock);

  rc = sqlite3_open(zLocal, &db);
  check("open local", rc==SQLITE_OK);
  rc = sqlite3_exec(db,
      "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);"
      "INSERT INTO t VALUES(1,'base');"
      "SELECT dolt_commit('-Am','c1');", 0, 0, 0);
  check("commit c1", rc==SQLITE_OK && headHash(db, &c1)==SQLITE_OK);
  rc = sqlite3_exec(db,
      "WITH RECURSIVE n(i) AS (SELECT 2 UNION ALL SELECT i+1 FROM n WHERE i<3000)"
      " INSERT INTO t SELECT i, randomblob(100) FROM n;"
      "SELECT dolt_commit('-am','c2');", 0, 0, 0);
  check("commit c2", rc==SQLITE_OK && headHash(db, &c2)==SQLITE_OK);
  sqlite3_close(db);

  memset(&local, 0, sizeof(local));
  memset(&server, 0, sizeof(server));
  rc = chunkStoreOpen(&local, sqlite3_vfs_find(0), zLocal,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB);
  check("open local store", rc==SQLITE_OK);
  rc = chunkStoreOpen(&server, sqlite3_vfs_find(0), zServer,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                      | SQLITE_OPEN_MAIN_DB);
  check("open server store", rc==SQLITE_OK);

  /* Server holds c1 and main -> c1. */
  check("sync c1", syncInto(&local, &server, &c1, 0, 0)==SQLITE_OK);
  pBlob = refsBlobFor(&c1, &nBlob);
  check("install main -> c1",
        pBlob && doltliteRemoteSrvApplyScopedRefsForTest(
            &server, "main", 0, pBlob, nBlob)==SQLITE_OK);
  sqlite3_free(pBlob);

  /* c2 not held at all, as after a gc swept an unreferenced upload: the
  ** ancestry walk cannot prove a fast-forward, which is not a refusal. */
  pBlob = refsBlobFor(&c2, &nBlob);
  check("absent commit is retryable, not non-ff",
        pBlob && doltliteRemoteSrvApplyScopedRefsForTest(
            &server, "main", 0, pBlob, nBlob)==SQLITE_BUSY_SNAPSHOT);

  /* Only c2's commit chunk landed, as after an interrupted push. */
  check("put c2 commit only", putOne(&local, &server, &c2)==SQLITE_OK);
  check("partial graph is retryable, not corrupt",
        doltliteRemoteSrvApplyScopedRefsForTest(
            &server, "main", 0, pBlob, nBlob)==SQLITE_BUSY_SNAPSHOT);

  /* A sync that trusts present chunks leaves the graph incomplete; the
  ** forced resume scan uploads what is missing below them. */
  check("plain sync", syncInto(&local, &server, &c2, 0, 0)==SQLITE_OK);
  check("plain sync skips the present commit's descendants",
        doltliteRemoteSrvApplyScopedRefsForTest(
            &server, "main", 0, pBlob, nBlob)==SQLITE_BUSY_SNAPSHOT);
  check("forced resume sync",
        syncInto(&local, &server, &c2, 1, &flagAfter)==SQLITE_OK);
  check("forced resume flag is consumed", flagAfter==0);
  check("completed graph installs",
        doltliteRemoteSrvApplyScopedRefsForTest(
            &server, "main", 0, pBlob, nBlob)==SQLITE_OK);
  sqlite3_free(pBlob);

  chunkStoreClose(&server);
  chunkStoreClose(&local);
  remove(zLocal);
  remove(zServer);

  if( failures ){
    fprintf(stderr, "%d failure(s)\n", failures);
    return 1;
  }
  printf("remote_partial_push_test: ok\n");
  return 0;
}

#else
int main(void){ return 0; }
#endif
