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

static int putBlobKeyNode(
  ChunkStore *cs,
  u8 level,
  const u8 *pKey, int nKey,
  const u8 *pVal, int nVal,
  ProllyHash *pOut
){
  ProllyNodeBuilder b;
  u8 *pData = 0;
  int nData = 0;
  int rc;

  prollyNodeBuilderInit(&b, level, PROLLY_NODE_BLOBKEY);
  rc = prollyNodeBuilderAdd(&b, pKey, nKey, pVal, nVal);
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

static void test_corrupt_child_key_flags(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leafA, leafO, leafT;
  ProllyHash blobLeaf, blobInt;
  ProllyHash intChildA, intChildO;
  ProllyHash ancRoot, oursRoot, theirsRoot, merged;
  u8 recA[] = {2, 15, 'a'};
  u8 recO[] = {2, 15, 'o'};
  u8 recT[] = {2, 15, 't'};
  u8 shortKey[1] = {'x'};
  int rc, handled = 0;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_corrupt_flags";

  rc = openStore(&cs, &cache, path);
  check("corrupt_flags: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, recA, sizeof(recA), &leafA);
  check("corrupt_flags: leafA", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &leafA, 2, recO, sizeof(recO), &leafO);
  check("corrupt_flags: leafO", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &leafA, 3, recT, sizeof(recT), &leafT);
  check("corrupt_flags: leafT", rc==SQLITE_OK);

  /* Corrupt BLOBKEY leaf with a 1-byte key under an INTKEY parent. */
  rc = putBlobKeyNode(&cs, 0, shortKey, 1, recT, sizeof(recT), &blobLeaf);
  check("corrupt_flags: blobLeaf", rc==SQLITE_OK);

  /* fmEmitChild with corrupt theirs leaf child. */
  rc = putSingleEntryNode(&cs, 1, 10, leafA.data, PROLLY_HASH_SIZE, &ancRoot);
  check("corrupt_flags: ancRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, leafO.data, PROLLY_HASH_SIZE, &oursRoot);
  check("corrupt_flags: oursRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, blobLeaf.data, PROLLY_HASH_SIZE, &theirsRoot);
  check("corrupt_flags: theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_flags: fmEmitChild theirs blob leaf returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_flags: fmEmitChild theirs blob leaf not handled", handled==0);

  /* fmEmitChild with corrupt anc leaf child. */
  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &theirsRoot, &ancRoot, &oursRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_flags: fmEmitChild anc blob leaf returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_flags: fmEmitChild anc blob leaf not handled", handled==0);

  /* fmEmitChild with corrupt ours leaf child. */
  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &theirsRoot, &oursRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_flags: fmEmitChild ours blob leaf returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_flags: fmEmitChild ours blob leaf not handled", handled==0);

  /* fmEmitChild with corrupt level-1 BLOBKEY interior child under level-2 INTKEY parents. */
  rc = putSingleEntryNode(&cs, 1, 10, leafA.data, PROLLY_HASH_SIZE, &intChildA);
  check("corrupt_flags: intChildA", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 1, 10, leafO.data, PROLLY_HASH_SIZE, &intChildO);
  check("corrupt_flags: intChildO", rc==SQLITE_OK);
  rc = putBlobKeyNode(&cs, 1, shortKey, 1, leafT.data, PROLLY_HASH_SIZE, &blobInt);
  check("corrupt_flags: blobInt", rc==SQLITE_OK);

  rc = putSingleEntryNode(&cs, 2, 10, intChildA.data, PROLLY_HASH_SIZE, &ancRoot);
  check("corrupt_flags: l2 ancRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 2, 10, intChildO.data, PROLLY_HASH_SIZE, &oursRoot);
  check("corrupt_flags: l2 oursRoot", rc==SQLITE_OK);
  rc = putSingleEntryNode(&cs, 2, 10, blobInt.data, PROLLY_HASH_SIZE, &theirsRoot);
  check("corrupt_flags: l2 theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_flags: fmEmitChild blob interior returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_flags: fmEmitChild blob interior not handled", handled==0);

  /* fmEmitSubtreeRows: child 0 merges leaves so chunker is non-empty,
  ** child 1 splices blobLeaf (BLOBKEY leaf under an INTKEY parent). */
  rc = putTwoChildNode(&cs, 1, 3, &leafA, 10, &leafA, &ancRoot);
  check("corrupt_flags: splice ancRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 3, &leafO, 10, &blobLeaf, &oursRoot);
  check("corrupt_flags: splice oursRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 3, &leafT, 10, &blobLeaf, &theirsRoot);
  check("corrupt_flags: splice theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_flags: fmEmitSubtreeRows blob leaf returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_flags: fmEmitSubtreeRows blob leaf not handled", handled==0);

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

static void test_corrupt_spliced_child(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leafA, leaf20A, leaf20O, leaf20T;
  ProllyHash badLevelChild, badFlagsChild;
  ProllyHash ancRoot, oursRoot, theirsRoot, merged;
  u8 recA[] = {2, 15, 'a'};
  u8 recO[] = {2, 15, 'o'};
  u8 recT[] = {2, 15, 't'};
  u8 shortKey[1] = {'x'};
  int rc, handled = 0;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_corrupt_splice";

  rc = openStore(&cs, &cache, path);
  check("corrupt_splice: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  rc = insertInt(&cs, &cache, &empty, 1, recA, sizeof(recA), &leafA);
  check("corrupt_splice: leafA", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 20, recA, sizeof(recA), &leaf20A);
  check("corrupt_splice: leaf20A", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 20, recO, sizeof(recO), &leaf20O);
  check("corrupt_splice: leaf20O", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 20, recT, sizeof(recT), &leaf20T);
  check("corrupt_splice: leaf20T", rc==SQLITE_OK);

  /* Case 1: spliced child 0 has wrong level (level 1 under a level-1 parent)
  ** while chunker is empty below parentLevel so the splice fast path runs. */
  rc = putSingleEntryNode(&cs, 1, 10, leafA.data, PROLLY_HASH_SIZE,
                          &badLevelChild);
  check("corrupt_splice: badLevelChild", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &leafA, 20, &leaf20A, &ancRoot);
  check("corrupt_splice: level ancRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &badLevelChild, 20, &leaf20O, &oursRoot);
  check("corrupt_splice: level oursRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &badLevelChild, 20, &leaf20T, &theirsRoot);
  check("corrupt_splice: level theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_splice: wrong level returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_splice: wrong level not handled", handled==0);

  /* Case 2: spliced child 0 has wrong key flags (BLOBKEY under INTKEY parent)
  ** while chunker is empty below parentLevel so the splice fast path runs. */
  rc = putBlobKeyNode(&cs, 0, shortKey, 1, recT, sizeof(recT), &badFlagsChild);
  check("corrupt_splice: badFlagsChild", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &leafA, 20, &leaf20A, &ancRoot);
  check("corrupt_splice: flags ancRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &badFlagsChild, 20, &leaf20O, &oursRoot);
  check("corrupt_splice: flags oursRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &badFlagsChild, 20, &leaf20T, &theirsRoot);
  check("corrupt_splice: flags theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = -1;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_splice: wrong flags returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);
  check("corrupt_splice: wrong flags not handled", handled==0);

  closeStore(&cs, &cache, path);
}

static void test_cursor_recover_corrupt_boundary(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash empty, leaf5A, leaf5B, leaf20A, leaf20O, leaf25T;
  ProllyHash oldRoot, newRoot, ancRoot, oursRoot, theirsRoot, merged;
  ProllyHash bLeafA, bLeafB, bLeafCA, bLeafCO, bLeafDT;
  ProllyHash bOldRoot, bNewRoot, bAncRoot, bOursRoot, bTheirsRoot, bMerged;
  ProllyDiffIter iter;
  ProllyDiffChange *pCh = 0;
  ProllyCursor cur;
  u8 recA[] = {2, 15, 'a'};
  u8 recB[] = {2, 15, 'b'};
  u8 recO[] = {2, 15, 'o'};
  u8 recT[] = {2, 15, 't'};
  u8 kAA[2] = {'a','a'};
  u8 kBB[2] = {'b','b'};
  u8 kCC[2] = {'c','c'};
  u8 kDD[2] = {'d','d'};
  int rc, res = 0, handled = 0;
  int nChanges, nRows;
  i64 seenKeys[4];
  const char *path = DOLTLITE_TEST_TMPDIR "/test_3wm_corrupt_boundary";

  rc = openStore(&cs, &cache, path);
  check("corrupt_boundary: open", rc==SQLITE_OK);
  memset(&empty, 0, sizeof(empty));

  /* INTKEY: child 0 has shared boundary key 10, exceeding its subtree max key 5. */
  rc = insertInt(&cs, &cache, &empty, 5, recA, sizeof(recA), &leaf5A);
  check("corrupt_boundary: leaf5A", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 5, recB, sizeof(recB), &leaf5B);
  check("corrupt_boundary: leaf5B", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 20, recA, sizeof(recA), &leaf20A);
  check("corrupt_boundary: leaf20A", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &empty, 20, recO, sizeof(recO), &leaf20O);
  check("corrupt_boundary: leaf20O", rc==SQLITE_OK);
  rc = insertInt(&cs, &cache, &leaf20A, 25, recT, sizeof(recT), &leaf25T);
  check("corrupt_boundary: leaf25T", rc==SQLITE_OK);

  /* Diff INTKEY: child 0 emits MODIFY 5, then child 1 diverges (20 vs 25). */
  rc = putTwoChildNode(&cs, 1, 10, &leaf5A, 20, &leaf20A, &oldRoot);
  check("corrupt_boundary: diff int oldRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &leaf5B, 25, &leaf25T, &newRoot);
  check("corrupt_boundary: diff int newRoot", rc==SQLITE_OK);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &oldRoot, &newRoot,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY);
  check("corrupt_boundary: diff int open", rc==SQLITE_OK);
  nChanges = 0;
  if( rc==SQLITE_OK ){
    while( (rc = prollyDiffIterStep(&iter, &pCh))==SQLITE_ROW ){
      if( nChanges<4 ) seenKeys[nChanges] = pCh->intKey;
      nChanges++;
    }
    check("corrupt_boundary: diff int done", rc==SQLITE_DONE);
    check("corrupt_boundary: diff int no duplicate key 5", nChanges==2);
    check("corrupt_boundary: diff int keys [5,25]",
          nChanges==2 && seenKeys[0]==5 && seenKeys[1]==25);
    prollyDiffIterClose(&iter);
  }

  /* Three-way merge INTKEY: child 0 splices leaf5A, then child 1 diverges. */
  rc = putTwoChildNode(&cs, 1, 10, &leaf5A, 20, &leaf20A, &ancRoot);
  check("corrupt_boundary: merge int ancRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &leaf5A, 20, &leaf20O, &oursRoot);
  check("corrupt_boundary: merge int oursRoot", rc==SQLITE_OK);
  rc = putTwoChildNode(&cs, 1, 10, &leaf5A, 25, &leaf25T, &theirsRoot);
  check("corrupt_boundary: merge int theirsRoot", rc==SQLITE_OK);

  memset(&merged, 0, sizeof(merged));
  handled = 0;
  rc = prollyThreeWayMergeFast(&cs, &cache, &ancRoot, &oursRoot, &theirsRoot,
                               PROLLY_NODE_INTKEY, &merged, &handled);
  check("corrupt_boundary: merge int rc", rc==SQLITE_OK);
  check("corrupt_boundary: merge int handled", handled==1);
  if( rc==SQLITE_OK && handled==1 ){
    prollyCursorInit(&cur, &cs, &cache, &merged, PROLLY_NODE_INTKEY);
    rc = prollyCursorFirst(&cur, &res);
    nRows = 0;
    while( rc==SQLITE_OK && prollyCursorIsValid(&cur) ){
      if( nRows<4 ) seenKeys[nRows] = prollyCursorIntKey(&cur);
      nRows++;
      rc = prollyCursorNext(&cur);
    }
    prollyCursorClose(&cur);
    check("corrupt_boundary: merge int scan ok", rc==SQLITE_OK);
    check("corrupt_boundary: merge int no duplicate key 5", nRows==3);
    check("corrupt_boundary: merge int keys [5,20,25]",
          nRows==3 && seenKeys[0]==5 && seenKeys[1]==20 && seenKeys[2]==25);
  }

  /* BLOBKEY: child 0 has shared boundary "bb", exceeding subtree max key "aa". */
  rc = insertBlob(&cs, &cache, &empty, kAA, 2, recA, sizeof(recA), &bLeafA);
  check("corrupt_boundary: bLeafA", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &empty, kAA, 2, recB, sizeof(recB), &bLeafB);
  check("corrupt_boundary: bLeafB", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &empty, kCC, 2, recA, sizeof(recA), &bLeafCA);
  check("corrupt_boundary: bLeafCA", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &empty, kCC, 2, recO, sizeof(recO), &bLeafCO);
  check("corrupt_boundary: bLeafCO", rc==SQLITE_OK);
  rc = insertBlob(&cs, &cache, &bLeafCA, kDD, 2, recT, sizeof(recT), &bLeafDT);
  check("corrupt_boundary: bLeafDT", rc==SQLITE_OK);

  rc = putTwoBlobChildNode(&cs, 1, kBB, 2, &bLeafA, kCC, 2, &bLeafCA, &bOldRoot);
  check("corrupt_boundary: diff blob oldRoot", rc==SQLITE_OK);
  rc = putTwoBlobChildNode(&cs, 1, kBB, 2, &bLeafB, kDD, 2, &bLeafDT, &bNewRoot);
  check("corrupt_boundary: diff blob newRoot", rc==SQLITE_OK);

  rc = prollyDiffIterOpen(&iter, &cs, &cache, &bOldRoot, &bNewRoot,
                          PROLLY_NODE_BLOBKEY, PROLLY_NODE_BLOBKEY);
  check("corrupt_boundary: diff blob open", rc==SQLITE_OK);
  nChanges = 0;
  if( rc==SQLITE_OK ){
    while( (rc = prollyDiffIterStep(&iter, &pCh))==SQLITE_ROW ){
      nChanges++;
    }
    check("corrupt_boundary: diff blob done", rc==SQLITE_DONE);
    check("corrupt_boundary: diff blob no duplicate key aa", nChanges==2);
    prollyDiffIterClose(&iter);
  }

  rc = putTwoBlobChildNode(&cs, 1, kBB, 2, &bLeafA, kCC, 2, &bLeafCA, &bAncRoot);
  check("corrupt_boundary: merge blob ancRoot", rc==SQLITE_OK);
  rc = putTwoBlobChildNode(&cs, 1, kBB, 2, &bLeafA, kCC, 2, &bLeafCO, &bOursRoot);
  check("corrupt_boundary: merge blob oursRoot", rc==SQLITE_OK);
  rc = putTwoBlobChildNode(&cs, 1, kBB, 2, &bLeafA, kDD, 2, &bLeafDT, &bTheirsRoot);
  check("corrupt_boundary: merge blob theirsRoot", rc==SQLITE_OK);

  memset(&bMerged, 0, sizeof(bMerged));
  handled = 0;
  rc = prollyThreeWayMergeFast(&cs, &cache, &bAncRoot, &bOursRoot, &bTheirsRoot,
                               PROLLY_NODE_BLOBKEY, &bMerged, &handled);
  check("corrupt_boundary: merge blob rc", rc==SQLITE_OK);
  check("corrupt_boundary: merge blob handled", handled==1);
  if( rc==SQLITE_OK && handled==1 ){
    prollyCursorInit(&cur, &cs, &cache, &bMerged, PROLLY_NODE_BLOBKEY);
    rc = prollyCursorFirst(&cur, &res);
    nRows = 0;
    while( rc==SQLITE_OK && prollyCursorIsValid(&cur) ){
      nRows++;
      rc = prollyCursorNext(&cur);
    }
    prollyCursorClose(&cur);
    check("corrupt_boundary: merge blob scan ok", rc==SQLITE_OK);
    check("corrupt_boundary: merge blob no duplicate key aa", nRows==3);
  }

  closeStore(&cs, &cache, path);
}

int main(void){
  sqlite3_initialize();
  test_same_shape_still_handles();
  test_mixed_shape_does_not_handle();
  test_flags_disagree_with_nodes();
  test_corrupt_value_error();
  test_corrupt_child_level_and_depth();
  test_corrupt_child_key_flags();
  test_diff_open_error_frees_prefetch_buf();
  test_diff_prefetch_frame_no_overlap();
  test_diff_corrupt_child_flags();
  test_corrupt_spliced_child();
  test_cursor_recover_corrupt_boundary();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
