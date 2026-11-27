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

static int dummySourceGet(
  void *pCtx,
  const unsigned char aHash[20],
  unsigned char **ppBytes,
  int *pnBytes
){
  (void)pCtx;
  (void)aHash;
  (void)ppBytes;
  (void)pnBytes;
  return DOLTLITE_SOURCE_NOTFOUND;
}

static int dummySourceGetMany(
  void *pCtx,
  int nHash,
  const unsigned char *aHash,
  unsigned char **apBytes,
  int *anBytes
){
  (void)pCtx;
  (void)nHash;
  (void)aHash;
  (void)apBytes;
  (void)anBytes;
  return DOLTLITE_SOURCE_NOTFOUND;
}

static void test_diff_open_error_frees_prefetch_buf(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leafA, leafO, deepOld, deepNew;
  ProllyDiffIter iter;
  doltlite_chunk_source src;
  u8 recA[] = {2, 15, 'a'};
  u8 recO[] = {2, 15, 'o'};
  sqlite3_int64 memBefore;
  int rc;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_diff_prefetch_leak";

  rc = openStore(&cs, &cache, path);
  check("diff_prefetch_leak: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, recA, sizeof(recA), &leafA);
  check("diff_prefetch_leak: leafA", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &leafA, 2, recO, sizeof(recO), &leafO);
  check("diff_prefetch_leak: leafO", rc==SQLITE_OK);

  rc = putSingleEntryNode(&cs, PROLLY_CURSOR_MAX_DEPTH, 1,
                          leafA.data, PROLLY_HASH_SIZE, &deepOld);
  check("diff_prefetch_leak: deepOld", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, PROLLY_CURSOR_MAX_DEPTH, 1,
                          leafO.data, PROLLY_HASH_SIZE, &deepNew);
  check("diff_prefetch_leak: deepNew", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);
  check("diff_prefetch_leak: commit", rc==SQLITE_OK);

  memset(&src, 0, sizeof(src));
  src.iVersion = 1;
  src.xGet = dummySourceGet;
  src.xGetMany = dummySourceGetMany;
  rc = chunkStoreSourceSet(&cs, 0, &src, 0);
  check("diff_prefetch_leak: set source", rc==SQLITE_OK);
  check("diff_prefetch_leak: source attached", cs.pChunkSource!=0);

  /* Error during diffIterPrefetchPairs after pPrefetchPairs is allocated. */
  memBefore = sqlite3_memory_used();
  rc = prollyDiffIterOpen(&iter, &cs, &cache, &deepOld, &deepNew,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("diff_prefetch_leak: prefetch error rc", rc==SQLITE_CORRUPT);
  check("diff_prefetch_leak: prefetch error iter.rc", iter.rc==SQLITE_CORRUPT);
  check("diff_prefetch_leak: prefetch error iter.eof", iter.eof==1);
  check("diff_prefetch_leak: prefetch error pPrefetchPairs NULL",
        iter.pPrefetchPairs==0);
  check("diff_prefetch_leak: prefetch error pPrefetchHashes NULL",
        iter.pPrefetchHashes==0);
  check("diff_prefetch_leak: prefetch error no leak without close",
        sqlite3_memory_used()==memBefore);
  prollyDiffIterClose(&iter);
  check("diff_prefetch_leak: prefetch error idempotent close",
        sqlite3_memory_used()==memBefore);

  /* Error during diffIterActivateRange after diffIterPrefetchPairs succeeds. */
  prollyCacheFree(&cache);
  memBefore = sqlite3_memory_used();
  rc = prollyCacheInit(&cache, 64*4096);
  check("diff_prefetch_leak: reinit cache", rc==SQLITE_OK);
  rc = prollyDiffIterOpen(&iter, &cs, &cache, &deepOld, &deepNew,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_BLOBKEY);
  check("diff_prefetch_leak: activate error rc", rc!=SQLITE_OK);
  check("diff_prefetch_leak: activate error iter.rc", iter.rc==rc);
  check("diff_prefetch_leak: activate error iter.eof", iter.eof==1);
  check("diff_prefetch_leak: activate error pPrefetchPairs NULL",
        iter.pPrefetchPairs==0);
  check("diff_prefetch_leak: activate error pPrefetchHashes NULL",
        iter.pPrefetchHashes==0);
  prollyCacheFree(&cache);
  check("diff_prefetch_leak: activate error no leak without close",
        sqlite3_memory_used()==memBefore);

  chunkStoreSourceSet(&cs, 0, 0, 0);
  chunkStoreClose(&cs);
  remove(path);
}

static int putTwoBlobChildNode(
  ChunkStore *cs,
  u8 level,
  const u8 *k0, int nK0, const ProllyHash *h0,
  const u8 *k1, int nK1, const ProllyHash *h1,
  ProllyHash *pOut
){
  ProllyNodeBuilder b;
  u8 *pData = 0;
  int nData = 0;
  int rc;

  prollyNodeBuilderInit(&b, level, PROLLY_NODE_BLOBKEY);
  rc = prollyNodeBuilderAdd(&b, k0, nK0, h0->data, PROLLY_HASH_SIZE);
  if( rc==SQLITE_OK ){
    rc = prollyNodeBuilderAdd(&b, k1, nK1, h1->data, PROLLY_HASH_SIZE);
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

static void test_diff_prefetch_frame_no_overlap(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leaf1A, leaf1B, leaf2;
  ProllyHash midOld, midNew, rootOld, rootNew;
  ProllyDiffIter iter;
  ProllyDiffChange *pCh = 0;
  doltlite_chunk_source src;
  u8 recA[] = {2, 15, 'a'};
  u8 recB[] = {2, 15, 'b'};
  int rc;
  int nChanges = 0;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_diff_prefetch_frame";

  rc = openStore(&cs, &cache, path);
  check("diff_prefetch_frame: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, recA, sizeof(recA), &leaf1A);
  check("diff_prefetch_frame: leaf1A", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 1, recB, sizeof(recB), &leaf1B);
  check("diff_prefetch_frame: leaf1B", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 2, recA, sizeof(recA), &leaf2);
  check("diff_prefetch_frame: leaf2", rc==SQLITE_OK);

  rc = putTwoChildNode(&cs, 1, 1, &leaf1A, 2, &leaf2, &midOld);
  check("diff_prefetch_frame: midOld", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 1, &leaf1B, 2, &leaf2, &midNew);
  check("diff_prefetch_frame: midNew", rc==SQLITE_OK);

  rc = putSingleEntryNode(&cs, 2, 2, midOld.data, PROLLY_HASH_SIZE, &rootOld);
  check("diff_prefetch_frame: rootOld", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 2, 2, midNew.data, PROLLY_HASH_SIZE, &rootNew);
  check("diff_prefetch_frame: rootNew", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);
  check("diff_prefetch_frame: commit", rc==SQLITE_OK);

  memset(&src, 0, sizeof(src));
  src.iVersion = 1;
  src.xGet = dummySourceGet;
  src.xGetMany = dummySourceGetMany;
  rc = chunkStoreSourceSet(&cs, 0, &src, 0);
  check("diff_prefetch_frame: set source", rc==SQLITE_OK);
  check("diff_prefetch_frame: source attached", cs.pChunkSource!=0);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &rootOld, &rootNew,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("diff_prefetch_frame: open diff", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    while( (rc = prollyDiffIterStep(&iter, &pCh))==SQLITE_ROW ){
      nChanges++;
      check("diff_prefetch_frame: change is modify",
            pCh->type==PROLLY_DIFF_MODIFY);
      check("diff_prefetch_frame: change key is 1",
            pCh->keyIsIntKey && pCh->intKey==1);
    }
    check("diff_prefetch_frame: step done", rc==SQLITE_DONE);
    check("diff_prefetch_frame: one change", nChanges==1);
    prollyDiffIterClose(&iter);
  }

  chunkStoreSourceSet(&cs, 0, 0, 0);
  closeStore(&cs, &cache, path);
}

static void test_diff_corrupt_child_flags(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leafA, leafO;
  ProllyHash badOld, badNew, rootOld, rootNew;
  ProllyDiffIter iter;
  ProllyDiffChange *pCh = 0;
  doltlite_chunk_source src;
  u8 recA[] = {2, 15, 'a'};
  u8 recO[] = {2, 15, 'o'};
  u8 kA[1] = {'a'};
  u8 kB[1] = {'b'};
  u8 kC[1] = {'c'};
  int rc;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_diff_corrupt_flags";

  rc = openStore(&cs, &cache, path);
  check("diff_corrupt_flags: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, recA, sizeof(recA), &leafA);
  check("diff_corrupt_flags: leafA", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 2, recO, sizeof(recO), &leafO);
  check("diff_corrupt_flags: leafO", rc==SQLITE_OK);

  /* Corrupt level-1 BLOBKEY internal children with 1-byte keys under INTKEY roots. */
  rc = putTwoBlobChildNode(&cs, 1, kA, 1, &leafA, kB, 1, &leafO, &badOld);
  check("diff_corrupt_flags: badOld", rc==SQLITE_OK);
  rc = putTwoBlobChildNode(&cs, 1, kA, 1, &leafA, kC, 1, &leafO, &badNew);
  check("diff_corrupt_flags: badNew", rc==SQLITE_OK);

  rc = putSingleEntryNode(&cs, 2, 10, badOld.data, PROLLY_HASH_SIZE, &rootOld);
  check("diff_corrupt_flags: rootOld", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 2, 10, badNew.data, PROLLY_HASH_SIZE, &rootNew);
  check("diff_corrupt_flags: rootNew", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);
  check("diff_corrupt_flags: commit", rc==SQLITE_OK);

  /* Corrupt child flags caught in diffIterDescendPair during Step. */
  rc = prollyDiffIterOpen(&iter, &cs, &cache, &rootOld, &rootNew,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("diff_corrupt_flags: open ok for corrupt child", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = prollyDiffIterStep(&iter, &pCh);
    check("diff_corrupt_flags: step returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
    prollyDiffIterClose(&iter);
  }

  /* Corrupt root flags caught directly in diffIterDescendPair during Open. */
  rc = prollyDiffIterOpen(&iter, &cs, &cache, &badOld, &badNew,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("diff_corrupt_flags: corrupt root open returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  prollyDiffIterClose(&iter);

  /* Corrupt child flags caught in diffIterPrefetchPairs when chunk source is set. */
  memset(&src, 0, sizeof(src));
  src.iVersion = 1;
  src.xGet = dummySourceGet;
  src.xGetMany = dummySourceGetMany;
  rc = chunkStoreSourceSet(&cs, 0, &src, 0);
  check("diff_corrupt_flags: set source", rc==SQLITE_OK);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &rootOld, &rootNew,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("diff_corrupt_flags: prefetch returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  prollyDiffIterClose(&iter);

  chunkStoreSourceSet(&cs, 0, 0, 0);
  closeStore(&cs, &cache, path);
}

int main(void){
  sqlite3_initialize();
  test_same_shape_still_handles();
  test_mixed_shape_does_not_handle();
  test_flags_disagree_with_nodes();
  test_corrupt_value_error();
  test_corrupt_child_level_and_depth();
  test_diff_open_error_frees_prefetch_buf();
  test_diff_prefetch_frame_no_overlap();
  test_diff_corrupt_child_flags();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
