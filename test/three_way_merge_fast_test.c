#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cache.h"
#include "prolly_mutate.h"
#include "prolly_three_way_merge.h"
#include "chunk_store.h"

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

static int openStore(ChunkStore *cs, ProllyCache *cache, const char *path){
  int rc;
  remove(path);
  memset(cs, 0, sizeof(*cs));
  rc = chunkStoreOpen(cs, sqlite3_vfs_find(0), path, SQLITE_OPEN_CREATE);
  if( rc!=SQLITE_OK ) return rc;
  prollyCacheInit(cache, 64);
  return SQLITE_OK;
}

static void closeStore(ChunkStore *cs, ProllyCache *cache, const char *path){
  chunkStoreClose(cs);
  prollyCacheFree(cache);
  if( path ) remove(path);
}

static int insertInt(
  ChunkStore *cs, ProllyCache *cache,
  const ProllyHash *pRoot, i64 key,
  const u8 *pVal, int nVal, ProllyHash *pNew
){
  int rc = prollyMutateInsert(cs, cache, pRoot, PROLLY_NODE_INTKEY,
                              0, 0, key, pVal, nVal, pNew);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  return rc;
}

static int insertBlob(
  ChunkStore *cs, ProllyCache *cache,
  const ProllyHash *pRoot, const u8 *pKey, int nKey,
  const u8 *pVal, int nVal, ProllyHash *pNew
){
  int rc = prollyMutateInsert(cs, cache, pRoot, PROLLY_NODE_BLOBKEY,
                              pKey, nKey, 0, pVal, nVal, pNew);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  return rc;
}

static void test_same_shape_still_handles(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, anc, ours, theirs, merged;
  int rc, handled = 0;
  u8 val = 1;
  const char *path = "/tmp/test_3wm_same";

  rc = openStore(&cs, &cache, path);
  check("same_shape: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, &val, 1, &anc);
  check("same_shape: anc", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &anc, 2, &val, 1, &ours);
  check("same_shape: ours", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &anc, 3, &val, 1, &theirs);
  check("same_shape: theirs", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  rc = prollyThreeWayMergeFast(&cs, &cache, &anc, &ours, &theirs,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("same_shape: rc", rc==SQLITE_OK);
  check("same_shape: handled", handled==1);
  check("same_shape: root set", !prollyHashIsEmpty(&merged));

  closeStore(&cs, &cache, path);
}

static void test_mixed_shape_does_not_handle(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, anc, ours, theirs, merged;
  int rc, handled = 0;
  u8 val = 1;
  u8 blobKey[3] = {'a','b','c'};
  const char *path = "/tmp/test_3wm_mixed";

  rc = openStore(&cs, &cache, path);
  check("mixed: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, &val, 1, &anc);
  check("mixed: anc", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &anc, 2, &val, 1, &ours);
  check("mixed: ours", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &empty, blobKey, 3, &val, 1, &theirs);
  check("mixed: theirs blob", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  rc = prollyThreeWayMergeFast(&cs, &cache, &anc, &ours, &theirs,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("mixed: rc", rc==SQLITE_OK);
  check("mixed: not handled", handled==0);
  check("mixed: no merged root", prollyHashIsEmpty(&merged));

  closeStore(&cs, &cache, path);
}

static void test_flags_disagree_with_nodes(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, anc, ours, theirs, merged;
  int rc, handled = 0;
  u8 val = 1;
  u8 blobA[3] = {'a','a','a'};
  u8 blobB[3] = {'b','b','b'};
  u8 blobC[3] = {'c','c','c'};
  const char *path = "/tmp/test_3wm_flags";

  rc = openStore(&cs, &cache, path);
  check("flags: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertBlob(&cs, &cache, &empty, blobA, 3, &val, 1, &anc);
  check("flags: anc", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &anc, blobB, 3, &val, 1, &ours);
  check("flags: ours", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &anc, blobC, 3, &val, 1, &theirs);
  check("flags: theirs", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  rc = prollyThreeWayMergeFast(&cs, &cache, &anc, &ours, &theirs,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("flags: rc", rc==SQLITE_OK);
  check("flags: not handled", handled==0);

  closeStore(&cs, &cache, path);
}

int main(void){
  sqlite3_initialize();
  test_same_shape_still_handles();
  test_mixed_shape_does_not_handle();
  test_flags_disagree_with_nodes();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
