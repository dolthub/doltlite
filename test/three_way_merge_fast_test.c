#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cache.h"
#include "prolly_node.h"
#include "prolly_diff.h"
#include "prolly_mutate.h"
#include "prolly_three_way_merge.h"
#include "chunk_store.h"
#include "lib/test_tmpdir.h"

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
  prollyCacheInit(cache, 64*4096);
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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_same";

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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_mixed";

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
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_flags";

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

static void test_corrupt_value_error(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, anc, ours, theirs, tmp, merged;
  u8 validA[] = {2, 15, 'a'};
  u8 validO[] = {2, 15, 'o'};
  u8 validT[] = {2, 15, 't'};
  u8 corrupt[] = {2, 15};
  int rc, handled = 0;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_corrupt";

  rc = openStore(&cs, &cache, path);
  check("corrupt: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, validA, sizeof(validA), &anc);
  check("corrupt: anc", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &anc, 2, validO, sizeof(validO), &ours);
  check("corrupt: ours", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &anc, 1, corrupt, sizeof(corrupt), &theirs);
  check("corrupt: theirs", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &anc, &ours, &theirs,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt: modify returns SQLITE_CORRUPT", rc==SQLITE_CORRUPT);
  check("corrupt: modify not handled", handled==0);

  rc = insertInt(&cs, &cache, &anc, 2, corrupt, sizeof(corrupt), &tmp);
  check("corrupt: ours add corrupt", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &tmp, 3, validO, sizeof(validO), &ours);
  check("corrupt: ours add valid", rc==SQLITE_OK);

  rc = insertInt(&cs, &cache, &anc, 2, corrupt, sizeof(corrupt), &tmp);
  check("corrupt: theirs add corrupt", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &tmp, 4, validT, sizeof(validT), &theirs);
  check("corrupt: theirs add valid", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &anc, &ours, &theirs,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt: convergent add returns SQLITE_CORRUPT", rc==SQLITE_CORRUPT);
  check("corrupt: convergent add not handled", handled==0);

  closeStore(&cs, &cache, path);
}

static int putSingleEntryNode(
  ChunkStore *cs,
  u8 level,
  i64 key,
  const u8 *pVal,
  int nVal,
  ProllyHash *pOut
){
  ProllyNodeBuilder b;
  u8 keyBuf[8];
  u8 *pData = 0;
  int nData = 0;
  int rc;

  prollyEncodeIntKey(key, keyBuf);
  prollyNodeBuilderInit(&b, level, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAdd(&b, keyBuf, 8, pVal, nVal);
  if( rc==SQLITE_OK ){
    rc = prollyNodeBuilderFinish(&b, &pData, &nData);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(cs, pData, nData, pOut);
  }
  sqlite3_free(pData);
  prollyNodeBuilderFree(&b);
  return rc;
}

static int putTwoChildNode(
  ChunkStore *cs,
  u8 level,
  i64 k0, const ProllyHash *h0,
  i64 k1, const ProllyHash *h1,
  ProllyHash *pOut
){
  ProllyNodeBuilder b;
  u8 keyBuf0[8], keyBuf1[8];
  u8 *pData = 0;
  int nData = 0;
  int rc;

  prollyEncodeIntKey(k0, keyBuf0);
  prollyEncodeIntKey(k1, keyBuf1);
  prollyNodeBuilderInit(&b, level, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAdd(&b, keyBuf0, 8, h0->data, PROLLY_HASH_SIZE);
  if( rc==SQLITE_OK ){
    rc = prollyNodeBuilderAdd(&b, keyBuf1, 8, h1->data, PROLLY_HASH_SIZE);
  }
  if( rc==SQLITE_OK ){
    rc = prollyNodeBuilderFinish(&b, &pData, &nData);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(cs, pData, nData, pOut);
  }
  sqlite3_free(pData);
  prollyNodeBuilderFree(&b);
  return rc;
}

static void test_corrupt_child_level_and_depth(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leafA, leafO, leafT;
  ProllyHash badChildA, badChildO, badChildT;
  ProllyHash ancRoot, oursRoot, theirsRoot, merged;
  ProllyHash deepAnc, deepOurs, deepTheirs;
  ProllyDiffIter iter;
  ProllyDiffChange *pCh = 0;
  u8 recA[] = {2, 15, 'a'};
  u8 recO[] = {2, 15, 'o'};
  u8 recT[] = {2, 15, 't'};
  int rc, handled = 0;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_corrupt_level";

  rc = openStore(&cs, &cache, path);
  check("corrupt_level: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, recA, sizeof(recA), &leafA);
  check("corrupt_level: leafA", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &leafA, 2, recO, sizeof(recO), &leafO);
  check("corrupt_level: leafO", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &leafA, 3, recT, sizeof(recT), &leafT);
  check("corrupt_level: leafT", rc==SQLITE_OK);

  /* Level-1 children referenced by level-1 parents (child.level == parent.level). */
  rc = putSingleEntryNode(&cs, 1, 10, leafA.data, PROLLY_HASH_SIZE, &badChildA);
  check("corrupt_level: badChildA", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, leafO.data, PROLLY_HASH_SIZE, &badChildO);
  check("corrupt_level: badChildO", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, leafT.data, PROLLY_HASH_SIZE, &badChildT);
  check("corrupt_level: badChildT", rc==SQLITE_OK);

  rc = putSingleEntryNode(&cs, 1, 10, badChildA.data, PROLLY_HASH_SIZE, &ancRoot);
  check("corrupt_level: ancRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, badChildO.data, PROLLY_HASH_SIZE, &oursRoot);
  check("corrupt_level: oursRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, badChildT.data, PROLLY_HASH_SIZE, &theirsRoot);
  check("corrupt_level: theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_level: merge fmEmitChild returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_level: merge fmEmitChild not handled", handled==0);

  /* Exercise fmEmitSubtreeRows: child 0 merges leaves so chunker is non-empty,
  ** child 1 splices badChildO (level 1 under a level-1 parent). */
  rc = putTwoChildNode(&cs, 1, 3, &leafA, 10, &badChildA, &ancRoot);
  check("corrupt_level: splice ancRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 3, &leafO, 10, &badChildO, &oursRoot);
  check("corrupt_level: splice oursRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 3, &leafT, 10, &badChildO, &theirsRoot);
  check("corrupt_level: splice theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_level: merge fmEmitSubtreeRows returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_level: merge fmEmitSubtreeRows not handled", handled==0);

  /* 2-way diff: level-1 parents pointing to level-1 children. */
  rc = putSingleEntryNode(&cs, 1, 10, badChildA.data, PROLLY_HASH_SIZE, &ancRoot);
  check("corrupt_level: diff oldRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, badChildO.data, PROLLY_HASH_SIZE, &oursRoot);
  check("corrupt_level: diff newRoot", rc==SQLITE_OK);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &ancRoot, &oursRoot,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("corrupt_level: diff open ok", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = prollyDiffIterStep(&iter, &pCh);
    check("corrupt_level: diff step returns SQLITE_CORRUPT", rc==SQLITE_CORRUPT);
    prollyDiffIterClose(&iter);
  }

  /* Excessive root level >= PROLLY_CURSOR_MAX_DEPTH. */
  rc = putSingleEntryNode(&cs, PROLLY_CURSOR_MAX_DEPTH, 1,
                          leafA.data, PROLLY_HASH_SIZE, &deepAnc);
  check("corrupt_level: deepAnc", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, PROLLY_CURSOR_MAX_DEPTH, 1,
                          leafO.data, PROLLY_HASH_SIZE, &deepOurs);
  check("corrupt_level: deepOurs", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, PROLLY_CURSOR_MAX_DEPTH, 1,
                          leafT.data, PROLLY_HASH_SIZE, &deepTheirs);
  check("corrupt_level: deepTheirs", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &deepAnc, &deepOurs, &deepTheirs,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_level: merge excessive depth returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_level: merge excessive depth not handled", handled==0);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &deepAnc, &deepOurs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("corrupt_level: diff excessive depth returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  prollyDiffIterClose(&iter);

  closeStore(&cs, &cache, path);
}

int main(void){
  sqlite3_initialize();
  test_same_shape_still_handles();
  test_mixed_shape_does_not_handle();
  test_flags_disagree_with_nodes();
  test_corrupt_value_error();
  test_corrupt_child_level_and_depth();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
