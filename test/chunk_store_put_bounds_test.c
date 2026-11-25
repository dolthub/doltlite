#ifdef DOLTLITE_PROLLY

#include "sqlite3.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include "lib/test_tmpdir.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int cond){
  if( cond ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

static sqlite3_mem_methods gReal;
static int gFailReallocAtLeast;

static void *wrapMalloc(int n){ return gReal.xMalloc(n); }
static void wrapFree(void *p){ if( p ) gReal.xFree(p); }
static void *wrapRealloc(void *p, int n){
  if( gFailReallocAtLeast>0 && n>=gFailReallocAtLeast ) return 0;
  return gReal.xRealloc(p, n);
}
static int wrapSize(void *p){ return gReal.xSize(p); }
static int wrapRoundup(int n){ return gReal.xRoundup(n); }
static int wrapInit(void *p){ return gReal.xInit ? gReal.xInit(p) : SQLITE_OK; }
static void wrapShutdown(void *p){ if( gReal.xShutdown ) gReal.xShutdown(p); }

static sqlite3_mem_methods gWrap = {
  wrapMalloc, wrapFree, wrapRealloc, wrapSize, wrapRoundup,
  wrapInit, wrapShutdown, 0
};

static int putI(ChunkStore *cs, int i, ProllyHash *pHash){
  u8 a[4];
  a[0] = (u8)i; a[1] = (u8)(i>>8); a[2] = (u8)(i>>16); a[3] = (u8)(i>>24);
  return chunkStorePut(cs, a, 4, pHash);
}

static void testNegativePut(void){
  ChunkStore cs;
  ProllyHash h;
  int has = 0;
  u8 body = 1;
  int rc;

  memset(&cs, 0, sizeof(cs));
  rc = chunkStoreOpen(&cs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("neg_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  rc = chunkStorePut(&cs, &body, -1, &h);
  check("neg_put_toobig", rc==SQLITE_TOOBIG);

  rc = chunkStorePut(&cs, 0, -100, 0);
  check("neg_put_null_toobig", rc==SQLITE_TOOBIG);

  rc = chunkStoreHas(&cs, &h, &has);
  check("neg_has_ok", rc==SQLITE_OK);
  check("neg_not_inserted", has==0);

  rc = chunkStorePut(&cs, &body, 1, &h);
  check("pos_put_ok", rc==SQLITE_OK);
  rc = chunkStoreHas(&cs, &h, &has);
  check("pos_has", rc==SQLITE_OK && has==1);

  rc = chunkStorePut(&cs, (const u8*)"", 0, &h);
  check("empty_put_ok", rc==SQLITE_OK);

  chunkStoreClose(&cs);
}

static void testPendingHtNomemKeepsHits(void){
  ChunkStore cs;
  ProllyHash first;
  ProllyHash extra;
  int has = 0;
  int i;
  int rc;

  memset(&cs, 0, sizeof(cs));
  rc = chunkStoreOpen(&cs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("ht_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  /* Init HT is 4096 buckets, max load 4: grow on the 16385th pending entry.
  ** Put that many so the next lookup reallocs the table and the next-index
  ** array (the latter is the larger realloc). */
  for(i=0; i<16385; i++){
    rc = putI(&cs, i, i==0 ? &first : 0);
    if( rc!=SQLITE_OK ){
      check("ht_warmup_put", 0);
      chunkStoreClose(&cs);
      return;
    }
  }
  check("ht_warmup_put", 1);

  gFailReallocAtLeast = 100000;
  rc = putI(&cs, 200000, &extra);
  gFailReallocAtLeast = 0;
  check("ht_grow_nomem", rc==SQLITE_NOMEM);

  rc = chunkStoreHas(&cs, &first, &has);
  check("ht_first_has_rc", rc==SQLITE_OK);
  check("ht_first_still_present", has==1);

  rc = putI(&cs, 200001, &extra);
  check("ht_put_after_nomem", rc==SQLITE_OK);
  rc = chunkStoreHas(&cs, &extra, &has);
  check("ht_new_present", rc==SQLITE_OK && has==1);
  rc = chunkStoreHas(&cs, &first, &has);
  check("ht_first_after_recover", rc==SQLITE_OK && has==1);

  chunkStoreClose(&cs);
}

static void testMemoryCopies(void){
  ChunkStore cs;
  ProllyHash hash;
  ProllyHash sparse;
  u8 data[4096];
  u8 *pRead = 0;
  int nRead = 0;
  int nPhys = 0;
  int rc;
  int i;
  int has = 0;

  rc = chunkStoreOpen(&cs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("copies_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  memset(data, 0x5a, sizeof(data));
  rc = chunkStorePut(&cs, data, sizeof(data), &hash);
  check("copies_put", rc==SQLITE_OK);
  memset(data, 0, sizeof(data));
  for(i=0; i<4; i++){
    if( i==2 ){
      rc = chunkStoreCommit(&cs);
      check("copies_commit", rc==SQLITE_OK);
    }
    rc = chunkStoreGet(&cs, &hash, &pRead, &nRead);
    check("copies_get", rc==SQLITE_OK && nRead==sizeof(data));
    if( rc==SQLITE_OK && nRead==sizeof(data) ){
      memset(data, 0x5a, sizeof(data));
      check("copies_unchanged", memcmp(pRead, data, sizeof(data))==0);
      memset(pRead, 0, nRead);
    }
    sqlite3_free(pRead);
  }
  rc = chunkStorePutSparse(&cs, (const u8*)"abc", 3, 4093, &sparse);
  check("copies_sparse_put", rc==SQLITE_OK);
  rc = chunkStoreGetSparse(&cs, &sparse, &pRead, &nRead, &nPhys);
  check("copies_sparse_get", rc==SQLITE_OK && nRead==4096
        && nPhys==4096);
  if( rc==SQLITE_OK && nPhys==4096 ){
    memset(data, 0, sizeof(data));
    memcpy(data, "abc", 3);
    check("copies_sparse_content", memcmp(pRead, data, sizeof(data))==0);
  }
  sqlite3_free(pRead);
  chunkStoreRollback(&cs);
  rc = chunkStoreHas(&cs, &sparse, &has);
  check("copies_rollback_pending", rc==SQLITE_OK && !has);
  rc = chunkStoreGet(&cs, &hash, &pRead, &nRead);
  check("copies_rollback_committed", rc==SQLITE_OK && nRead==sizeof(data));
  if( rc==SQLITE_OK && nRead==sizeof(data) ){
    memset(data, 0x5a, sizeof(data));
    check("copies_rollback_content", memcmp(pRead, data, sizeof(data))==0);
  }
  sqlite3_free(pRead);
  chunkStoreClose(&cs);
}

static void testCheckedPut(void){
  ChunkStore cs;
  ProllyHash hash, wrong, other;
  u8 data[4096];
  u8 *pRead = 0;
  int nRead = 0;
  int has = 0;
  int rc;

  rc = chunkStoreOpen(&cs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("checked_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  memset(data, 0x5a, sizeof(data));
  prollyHashCompute(data, sizeof(data), &hash);
  wrong = hash;
  wrong.data[0] ^= 1;
  rc = chunkStorePutChecked(&cs, &wrong, data, sizeof(data));
  check("checked_mismatch", rc==SQLITE_CORRUPT);
  check("checked_mismatch_not_staged", cs.staging.nPending==0
        && cs.staging.nWriteBuf==0);
  check("checked_mismatch_not_present",
        chunkStoreHas(&cs, &hash, &has)==SQLITE_OK && !has);
  rc = chunkStorePutChecked(&cs, &hash, data, sizeof(data));
  check("checked_put", rc==SQLITE_OK);
  rc = chunkStorePutChecked(&cs, &hash, data, sizeof(data));
  check("checked_duplicate", rc==SQLITE_OK && cs.staging.nPending==1);
  data[0] ^= 1;
  rc = chunkStorePutChecked(&cs, &hash, data, sizeof(data));
  check("checked_existing_hash_still_verified", rc==SQLITE_CORRUPT
        && cs.staging.nPending==1);
  data[0] ^= 1;
  rc = chunkStoreGet(&cs, &hash, &pRead, &nRead);
  check("checked_original_preserved", rc==SQLITE_OK && nRead==sizeof(data)
        && memcmp(pRead, data, sizeof(data))==0);
  sqlite3_free(pRead);
  rc = chunkStorePutChecked(&cs, &hash, 0, -1);
  check("checked_negative_size", rc==SQLITE_TOOBIG);
  rc = chunkStorePutChecked(&cs, 0, data, sizeof(data));
  check("checked_missing_hash", rc==SQLITE_MISUSE);
  check("checked_commit", chunkStoreCommit(&cs)==SQLITE_OK);
  data[0] ^= 1;
  prollyHashCompute(data, sizeof(data), &other);
  check("checked_second_put",
        chunkStorePutChecked(&cs, &other, data, sizeof(data))==SQLITE_OK);
  chunkStoreRollback(&cs);
  check("checked_rollback_pending",
        chunkStoreHas(&cs, &other, &has)==SQLITE_OK && !has);
  check("checked_rollback_committed",
        chunkStoreHas(&cs, &hash, &has)==SQLITE_OK && has);
  chunkStoreClose(&cs);
}

static void testPendingHtResetAfterSweep(void){
  ChunkStore cs;
  ProllyHash oldHashes[32];
  ProllyHash newHashes[24];
  u8 *pRead = 0;
  int nRead = 0;
  int has = 0;
  int i;
  int rc;

  memset(&cs, 0, sizeof(cs));
  rc = chunkStoreOpen(&cs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("sweep_ht_open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  for(i=0; i<32; i++){
    rc = putI(&cs, 1000 + i, &oldHashes[i]);
    if( rc!=SQLITE_OK ){
      check("sweep_ht_pre_put", 0);
      chunkStoreClose(&cs);
      return;
    }
  }
  rc = chunkStoreHas(&cs, &oldHashes[0], &has);
  check("sweep_ht_pre_has", rc==SQLITE_OK && has==1);
  check("sweep_ht_pre_built", cs.staging.aPendingHT!=0
        && cs.staging.nPendingHTBuilt==32);

  chunkStagingResetAfterSweep(&cs.staging);
  check("sweep_ht_cleared", cs.staging.aPendingHT==0
        && cs.staging.aPendingHTNext==0
        && cs.staging.nPendingHTBuilt==0
        && cs.staging.nPendingHTSize==0
        && cs.staging.nPendingHTNextAlloc==0);

  for(i=0; i<24; i++){
    rc = putI(&cs, 2000 + i, &newHashes[i]);
    if( rc!=SQLITE_OK ){
      check("sweep_ht_post_put", 0);
      chunkStoreClose(&cs);
      return;
    }
  }

  for(i=0; i<24; i++){
    u8 expect[4];
    int v = 2000 + i;
    expect[0] = (u8)v; expect[1] = (u8)(v>>8);
    expect[2] = (u8)(v>>16); expect[3] = (u8)(v>>24);
    rc = chunkStoreHas(&cs, &newHashes[i], &has);
    if( rc!=SQLITE_OK || has!=1 ){
      check("sweep_ht_post_has", 0);
      break;
    }
    pRead = 0;
    nRead = 0;
    rc = chunkStoreGet(&cs, &newHashes[i], &pRead, &nRead);
    if( rc!=SQLITE_OK || nRead!=4 || memcmp(pRead, expect, 4)!=0 ){
      sqlite3_free(pRead);
      check("sweep_ht_post_get", 0);
      break;
    }
    sqlite3_free(pRead);
  }
  if( i==24 ){
    check("sweep_ht_post_has", 1);
    check("sweep_ht_post_get", 1);
  }

  for(i=0; i<32; i++){
    rc = chunkStoreHas(&cs, &oldHashes[i], &has);
    if( rc!=SQLITE_OK || has!=0 ){
      check("sweep_ht_old_absent", 0);
      break;
    }
  }
  if( i==32 ){
    check("sweep_ht_old_absent", 1);
  }

  rc = putI(&cs, 1000 + 28, &oldHashes[28]);
  check("sweep_ht_restage_old_put", rc==SQLITE_OK);
  pRead = 0;
  nRead = 0;
  rc = chunkStoreGet(&cs, &oldHashes[28], &pRead, &nRead);
  check("sweep_ht_restage_old_get", rc==SQLITE_OK && nRead==4);
  sqlite3_free(pRead);

  chunkStoreClose(&cs);
}

static void testZeroByteChunkGet(void){
  const char *zPath = DOLTLITE_TEST_TMPDIR "/test_zero_byte_chunk_get.db";
  char zLock[512];
  ChunkStore memCs;
  ChunkStore *cs;
  sqlite3 *db = 0;
  ProllyHash hEmpty, hPut, hNonEmpty;
  ProllyHash aBatch[2];
  u8 *apData[2] = {0, 0};
  int anData[2] = {-1, -1};
  u8 *pRead = 0;
  const u8 *pBorrow = 0;
  void *pSeg = 0;
  int nRead = -1;
  int nPhys = -1;
  int nBorrow = -1;
  int nBatchRead = 0;
  int rc;

  prollyHashCompute((const u8*)"", 0, &hEmpty);

  /* :memory: store (pending, committed, borrow, sparse, PutHashed) */
  memset(&memCs, 0, sizeof(memCs));
  rc = chunkStoreOpen(&memCs, 0, ":memory:", SQLITE_OPEN_CREATE);
  check("zero_mem_open", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(&memCs, (const u8*)"", 0, &hPut);
    check("zero_mem_put", rc==SQLITE_OK && prollyHashCompare(&hPut, &hEmpty)==0);
    pRead = 0; nRead = -1;
    rc = chunkStoreGet(&memCs, &hPut, &pRead, &nRead);
    check("zero_mem_pending_get", rc==SQLITE_OK && pRead!=0 && nRead==0);
    sqlite3_free(pRead);

    chunkStoreRollback(&memCs);
    rc = chunkStorePutHashed(&memCs, 0, 0, &hEmpty);
    check("zero_mem_put_hashed", rc==SQLITE_OK);
    pRead = 0; nRead = -1;
    rc = chunkStoreGet(&memCs, &hEmpty, &pRead, &nRead);
    check("zero_mem_put_hashed_get", rc==SQLITE_OK && pRead!=0 && nRead==0);
    sqlite3_free(pRead);

    rc = chunkStoreCommit(&memCs);
    check("zero_mem_commit", rc==SQLITE_OK);

    pRead = 0; nRead = -1;
    rc = chunkStoreGet(&memCs, &hEmpty, &pRead, &nRead);
    check("zero_mem_committed_get", rc==SQLITE_OK && pRead!=0 && nRead==0);
    sqlite3_free(pRead);

    pRead = 0; nRead = -1; nPhys = -1;
    rc = chunkStoreGetSparse(&memCs, &hEmpty, &pRead, &nRead, &nPhys);
    check("zero_mem_committed_sparse",
          rc==SQLITE_OK && pRead!=0 && nRead==0 && nPhys==0);
    sqlite3_free(pRead);

    pBorrow = 0; nBorrow = -1; pSeg = 0;
    rc = chunkStoreBorrow(&memCs, &hEmpty, &pBorrow, &nBorrow, &pSeg);
    check("zero_mem_borrow",
          rc==SQLITE_OK && pBorrow!=0 && nBorrow==0 && pSeg!=0);
    if( pSeg ) chunkStoreBorrowRelease(pSeg);

    chunkStoreClose(&memCs);
  }

  /* Disk store (pending, committed/recent, reopened, readBatch) */
  remove(zPath);
  snprintf(zLock, sizeof(zLock), "%s-lock", zPath);
  remove(zLock);

  rc = sqlite3_open(zPath, &db);
  check("zero_disk_open", rc==SQLITE_OK && db!=0);
  if( rc!=SQLITE_OK || !db ){
    sqlite3_close(db);
    return;
  }
  cs = doltliteGetChunkStore(db);
  check("zero_disk_cs", cs!=0);
  if( cs ){
    rc = chunkStorePut(cs, (const u8*)"", 0, &hPut);
    check("zero_disk_put", rc==SQLITE_OK && prollyHashCompare(&hPut, &hEmpty)==0);
    pRead = 0; nRead = -1;
    rc = chunkStoreGet(cs, &hPut, &pRead, &nRead);
    check("zero_disk_pending_get", rc==SQLITE_OK && pRead!=0 && nRead==0);
    sqlite3_free(pRead);

    rc = chunkStorePut(cs, (const u8*)"abc", 3, &hNonEmpty);
    check("zero_disk_put_nonempty", rc==SQLITE_OK);

    rc = chunkStoreCommit(cs);
    check("zero_disk_commit", rc==SQLITE_OK);

    pRead = 0; nRead = -1;
    rc = chunkStoreGet(cs, &hPut, &pRead, &nRead);
    check("zero_disk_recent_get", rc==SQLITE_OK && pRead!=0 && nRead==0);
    sqlite3_free(pRead);
  }
  sqlite3_close(db);
  db = 0;

  rc = sqlite3_open(zPath, &db);
  check("zero_disk_reopen", rc==SQLITE_OK && db!=0);
  if( rc==SQLITE_OK && db ){
    cs = doltliteGetChunkStore(db);
    check("zero_disk_reopen_cs", cs!=0);
    if( cs ){
      pRead = 0; nRead = -1;
      rc = chunkStoreGet(cs, &hEmpty, &pRead, &nRead);
      check("zero_disk_reopen_get", rc==SQLITE_OK && pRead!=0 && nRead==0);
      sqlite3_free(pRead);

      aBatch[0] = hNonEmpty;
      aBatch[1] = hEmpty;
      rc = chunkStoreReadBatch(cs, aBatch, 2, apData, anData, &nBatchRead);
      check("zero_disk_batch_get",
            rc==SQLITE_OK && nBatchRead==2
            && apData[0]!=0 && anData[0]==3 && memcmp(apData[0], "abc", 3)==0
            && apData[1]!=0 && anData[1]==0);
      sqlite3_free(apData[0]);
      sqlite3_free(apData[1]);
    }
  }
  sqlite3_close(db);
  remove(zPath);
  remove(zLock);
}

int main(void){
  int rc;

  sqlite3_shutdown();
  rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &gReal);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "FAIL: getmalloc %d\n", rc);
    return 1;
  }
  rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &gWrap);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "FAIL: setmalloc %d\n", rc);
    return 1;
  }
  rc = sqlite3_initialize();
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "FAIL: initialize %d\n", rc);
    return 1;
  }

  testNegativePut();
  testPendingHtNomemKeepsHits();
  testMemoryCopies();
  testCheckedPut();
  testPendingHtResetAfterSweep();
  testZeroByteChunkGet();

  printf("%d passed, %d failed\n", nPass, nFail);
  sqlite3_shutdown();
  return nFail ? 1 : 0;
}

#else
int main(void){ return 0; }
#endif
