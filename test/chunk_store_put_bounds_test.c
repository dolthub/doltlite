#ifdef DOLTLITE_PROLLY

#include "sqlite3.h"
#include "chunk_store.h"
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

  printf("%d passed, %d failed\n", nPass, nFail);
  sqlite3_shutdown();
  return nFail ? 1 : 0;
}

#else
int main(void){ return 0; }
#endif
