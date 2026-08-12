#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "prolly_mutate.h"
#include "prolly_three_way_diff.h"

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

#define MAX_CHANGES 64
static ThreeWayChange aChanges[MAX_CHANGES];
static int nChanges = 0;

static u8 *copyBlob(const u8 *p, int n){
  u8 *c;
  if( !p || n<=0 ) return 0;
  c = sqlite3_malloc(n);
  if( c ) memcpy(c, p, n);
  return c;
}

static u8 *encodeTextRecord(const char *zText, int *pnRec){
  int nText = (int)strlen(zText);
  int serialType = nText*2 + 13;     /* TEXT serial type for length nText */
  int nRec;
  u8 *pRec;

  if( serialType >= 0x80 ){
    fprintf(stderr, "encodeTextRecord: text too long (%d)\n", nText);
    *pnRec = 0;
    return 0;
  }

  nRec = 2 + nText;
  pRec = (u8*)sqlite3_malloc(nRec);
  if( !pRec ){ *pnRec = 0; return 0; }
  pRec[0] = 2;                       /* header size varint = 2 */
  pRec[1] = (u8)serialType;
  if( nText>0 ) memcpy(pRec + 2, zText, nText);
  *pnRec = nRec;
  return pRec;
}

static int recordHasText(const u8 *pRec, int nRec, const char *zText){
  int nText = (int)strlen(zText);
  if( pRec==0 ) return 0;
  if( nRec != nText + 2 ) return 0;
  if( pRec[0] != 2 ) return 0;
  if( pRec[1] != (u8)(nText*2 + 13) ) return 0;
  return nText==0 || memcmp(pRec+2, zText, nText)==0;
}

static int collectCallback(void *pCtx, const ThreeWayChange *pChange){
  ThreeWayChange *dst;
  (void)pCtx;
  if( nChanges>=MAX_CHANGES ) return SQLITE_ERROR;
  dst = &aChanges[nChanges++];
  dst->type = pChange->type;
  dst->pKey = copyBlob(pChange->pKey, pChange->nKey);
  dst->nKey = pChange->nKey;
  dst->intKey = pChange->intKey;
  dst->pBaseVal = copyBlob(pChange->pBaseVal, pChange->nBaseVal);
  dst->nBaseVal = pChange->nBaseVal;
  dst->pOurVal = copyBlob(pChange->pOurVal, pChange->nOurVal);
  dst->nOurVal = pChange->nOurVal;
  dst->pTheirVal = copyBlob(pChange->pTheirVal, pChange->nTheirVal);
  dst->nTheirVal = pChange->nTheirVal;
  return SQLITE_OK;
}

static void resetChanges(void){
  int i;
  for(i=0; i<nChanges; i++){
    sqlite3_free((void*)aChanges[i].pKey);
    sqlite3_free((void*)aChanges[i].pBaseVal);
    sqlite3_free((void*)aChanges[i].pOurVal);
    sqlite3_free((void*)aChanges[i].pTheirVal);
  }
  nChanges = 0;
}

static int treeInsertInt(
  ChunkStore *cs, ProllyCache *cache,
  const ProllyHash *pRoot, i64 key,
  const char *val, ProllyHash *pNewRoot
){
  int nRec = 0;
  u8 *pRec = encodeTextRecord(val, &nRec);
  int rc;
  if( !pRec ) return SQLITE_NOMEM;
  rc = prollyMutateInsert(cs, cache, pRoot, PROLLY_NODE_INTKEY,
                          0, 0, key,
                          pRec, nRec,
                          pNewRoot);
  sqlite3_free(pRec);
  return rc;
}

static int treeDeleteInt(
  ChunkStore *cs, ProllyCache *cache,
  const ProllyHash *pRoot, i64 key,
  ProllyHash *pNewRoot
){
  return prollyMutateDelete(cs, cache, pRoot, PROLLY_NODE_INTKEY,
                            0, 0, key, pNewRoot);
}

static int treeInsertBlob(
  ChunkStore *cs, ProllyCache *cache,
  const ProllyHash *pRoot, const u8 *pKey, int nKey,
  const char *val, ProllyHash *pNewRoot
){
  int nRec = 0;
  u8 *pRec = encodeTextRecord(val, &nRec);
  int rc;
  if( !pRec ) return SQLITE_NOMEM;
  rc = prollyMutateInsert(cs, cache, pRoot, PROLLY_NODE_BLOBKEY,
                          pKey, nKey, 0, pRec, nRec, pNewRoot);
  sqlite3_free(pRec);
  return rc;
}

static int openTestStore(ChunkStore *cs, ProllyCache *cache, const char *path){
  int rc;
  char chunks[512];
  remove(path);
  snprintf(chunks, sizeof(chunks), "%s-chunks", path);
  remove(chunks);

  memset(cs, 0, sizeof(*cs));
  rc = chunkStoreOpen(cs, sqlite3_vfs_find(0), chunks, SQLITE_OPEN_CREATE);
  if( rc!=SQLITE_OK ) return rc;

  prollyCacheInit(cache, 64);
  return SQLITE_OK;
}

static void closeTestStore(ChunkStore *cs, ProllyCache *cache, const char *path){
  char chunks[512];
  chunkStoreClose(cs);
  prollyCacheFree(cache);
  if( path ){
    remove(path);
    snprintf(chunks, sizeof(chunks), "%s-chunks", path);
    remove(chunks);
  }
}

static int buildTree(
  ChunkStore *cs, ProllyCache *cache,
  i64 *aKeys, const char **aVals, int n,
  ProllyHash *pRoot
){
  ProllyHash cur, next;
  int i, rc;

  memset(&cur, 0, sizeof(cur));

  for(i=0; i<n; i++){
    rc = treeInsertInt(cs, cache, &cur, aKeys[i], aVals[i], &next);
    if( rc!=SQLITE_OK ) return rc;
    cur = next;
    rc = chunkStoreCommit(cs);
    if( rc!=SQLITE_OK ) return rc;
  }

  *pRoot = cur;
  return SQLITE_OK;
}

static void test_identical(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash root;
  int rc;
  const char *path = "/tmp/test_3wd_identical";

  rc = openTestStore(&cs, &cache, path);
  check("identical: open store", rc==SQLITE_OK);

  {
    i64 keys[] = {1, 2, 3};
    const char *vals[] = {"a", "b", "c"};
    rc = buildTree(&cs, &cache, keys, vals, 3, &root);
    check("identical: build tree", rc==SQLITE_OK);
  }

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &root, &root, &root,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("identical: diff ok", rc==SQLITE_OK);
  check("identical: no changes", nChanges==0);

  closeTestStore(&cs, &cache, path);
  printf("  test_identical passed\n");
}

static void test_left_add(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours;
  int rc;
  const char *path = "/tmp/test_3wd_ladd";

  rc = openTestStore(&cs, &cache, path);
  check("left_add: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"base"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("left_add: build ancestor", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 2, "added", &ours);
  check("left_add: insert", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &ancestor,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("left_add: diff ok", rc==SQLITE_OK);
  check("left_add: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("left_add: type LEFT_ADD", aChanges[0].type==THREE_WAY_LEFT_ADD);
    check("left_add: key 2", aChanges[0].intKey==2);
    check("left_add: val 'added'",
          recordHasText(aChanges[0].pOurVal, aChanges[0].nOurVal, "added"));
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_left_add passed\n");
}

static void test_right_add(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_radd";

  rc = openTestStore(&cs, &cache, path);
  check("right_add: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"base"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("right_add: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 3, "theirs-added", &theirs);
  check("right_add: insert", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ancestor, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("right_add: diff ok", rc==SQLITE_OK);
  check("right_add: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("right_add: type RIGHT_ADD", aChanges[0].type==THREE_WAY_RIGHT_ADD);
    check("right_add: key 3", aChanges[0].intKey==3);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_right_add passed\n");
}

static void test_left_delete(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours;
  int rc;
  const char *path = "/tmp/test_3wd_ldel";

  rc = openTestStore(&cs, &cache, path);
  check("left_delete: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1, 2};
    const char *vals[] = {"keep", "delete-me"};
    rc = buildTree(&cs, &cache, keys, vals, 2, &ancestor);
    check("left_delete: build", rc==SQLITE_OK);
  }

  rc = treeDeleteInt(&cs, &cache, &ancestor, 2, &ours);
  check("left_delete: delete", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &ancestor,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("left_delete: diff ok", rc==SQLITE_OK);
  check("left_delete: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("left_delete: type LEFT_DELETE",
          aChanges[0].type==THREE_WAY_LEFT_DELETE);
    check("left_delete: key 2", aChanges[0].intKey==2);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_left_delete passed\n");
}

static void test_right_delete(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_rdel";

  rc = openTestStore(&cs, &cache, path);
  check("right_delete: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1, 2};
    const char *vals[] = {"keep", "delete-me"};
    rc = buildTree(&cs, &cache, keys, vals, 2, &ancestor);
    check("right_delete: build", rc==SQLITE_OK);
  }

  rc = treeDeleteInt(&cs, &cache, &ancestor, 2, &theirs);
  check("right_delete: delete", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ancestor, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("right_delete: diff ok", rc==SQLITE_OK);
  check("right_delete: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("right_delete: type RIGHT_DELETE",
          aChanges[0].type==THREE_WAY_RIGHT_DELETE);
    check("right_delete: key 2", aChanges[0].intKey==2);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_right_delete passed\n");
}

static void test_left_modify(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours;
  int rc;
  const char *path = "/tmp/test_3wd_lmod";

  rc = openTestStore(&cs, &cache, path);
  check("left_modify: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"original"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("left_modify: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "modified", &ours);
  check("left_modify: modify", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &ancestor,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("left_modify: diff ok", rc==SQLITE_OK);
  check("left_modify: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("left_modify: type LEFT_MODIFY",
          aChanges[0].type==THREE_WAY_LEFT_MODIFY);
    check("left_modify: key 1", aChanges[0].intKey==1);
    check("left_modify: base val 'original'",
          recordHasText(aChanges[0].pBaseVal, aChanges[0].nBaseVal, "original"));
    check("left_modify: our val 'modified'",
          recordHasText(aChanges[0].pOurVal, aChanges[0].nOurVal, "modified"));
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_left_modify passed\n");
}

static void test_right_modify(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_rmod";

  rc = openTestStore(&cs, &cache, path);
  check("right_modify: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"original"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("right_modify: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "theirs-mod", &theirs);
  check("right_modify: modify", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ancestor, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("right_modify: diff ok", rc==SQLITE_OK);
  check("right_modify: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("right_modify: type RIGHT_MODIFY",
          aChanges[0].type==THREE_WAY_RIGHT_MODIFY);
    check("right_modify: key 1", aChanges[0].intKey==1);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_right_modify passed\n");
}

static void test_convergent_modify(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conv_mod";

  rc = openTestStore(&cs, &cache, path);
  check("conv_modify: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"original"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("conv_modify: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "same", &ours);
  check("conv_modify: ours", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "same", &theirs);
  check("conv_modify: theirs", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conv_modify: diff ok", rc==SQLITE_OK);
  check("conv_modify: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conv_modify: type CONVERGENT",
          aChanges[0].type==THREE_WAY_CONVERGENT);
    check("conv_modify: key 1", aChanges[0].intKey==1);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_convergent_modify passed\n");
}

static void test_convergent_delete(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conv_del";

  rc = openTestStore(&cs, &cache, path);
  check("conv_delete: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1, 2};
    const char *vals[] = {"keep", "both-delete"};
    rc = buildTree(&cs, &cache, keys, vals, 2, &ancestor);
    check("conv_delete: build", rc==SQLITE_OK);
  }

  rc = treeDeleteInt(&cs, &cache, &ancestor, 2, &ours);
  check("conv_delete: ours del", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeDeleteInt(&cs, &cache, &ancestor, 2, &theirs);
  check("conv_delete: theirs del", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conv_delete: diff ok", rc==SQLITE_OK);
  check("conv_delete: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conv_delete: type CONVERGENT",
          aChanges[0].type==THREE_WAY_CONVERGENT);
    check("conv_delete: key 2", aChanges[0].intKey==2);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_convergent_delete passed\n");
}

static void test_convergent_add(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conv_add";

  rc = openTestStore(&cs, &cache, path);
  check("conv_add: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"base"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("conv_add: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 5, "same-val", &ours);
  check("conv_add: ours add", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeInsertInt(&cs, &cache, &ancestor, 5, "same-val", &theirs);
  check("conv_add: theirs add", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conv_add: diff ok", rc==SQLITE_OK);
  check("conv_add: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conv_add: type CONVERGENT",
          aChanges[0].type==THREE_WAY_CONVERGENT);
    check("conv_add: key 5", aChanges[0].intKey==5);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_convergent_add passed\n");
}

static void test_conflict_mm(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conf_mm";

  rc = openTestStore(&cs, &cache, path);
  check("conflict_mm: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"original"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("conflict_mm: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "ours-val", &ours);
  check("conflict_mm: ours mod", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "theirs-val", &theirs);
  check("conflict_mm: theirs mod", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conflict_mm: diff ok", rc==SQLITE_OK);
  check("conflict_mm: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conflict_mm: type CONFLICT_MM",
          aChanges[0].type==THREE_WAY_CONFLICT_MM);
    check("conflict_mm: key 1", aChanges[0].intKey==1);
    check("conflict_mm: our val 'ours-val'",
          recordHasText(aChanges[0].pOurVal, aChanges[0].nOurVal, "ours-val"));
    check("conflict_mm: their val 'theirs-val'",
          recordHasText(aChanges[0].pTheirVal, aChanges[0].nTheirVal, "theirs-val"));
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_conflict_mm passed\n");
}

static void test_conflict_dm(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conf_dm";

  rc = openTestStore(&cs, &cache, path);
  check("conflict_dm: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"original"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("conflict_dm: build", rc==SQLITE_OK);
  }

  rc = treeDeleteInt(&cs, &cache, &ancestor, 1, &ours);
  check("conflict_dm: ours del", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "theirs-mod", &theirs);
  check("conflict_dm: theirs mod", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conflict_dm: diff ok", rc==SQLITE_OK);
  check("conflict_dm: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conflict_dm: type CONFLICT_DM",
          aChanges[0].type==THREE_WAY_CONFLICT_DM);
    check("conflict_dm: key 1", aChanges[0].intKey==1);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_conflict_dm passed\n");
}

static void test_conflict_dm_reversed(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conf_dm_r";

  rc = openTestStore(&cs, &cache, path);
  check("conflict_dm_r: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"original"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("conflict_dm_r: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "ours-mod", &ours);
  check("conflict_dm_r: ours mod", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeDeleteInt(&cs, &cache, &ancestor, 1, &theirs);
  check("conflict_dm_r: theirs del", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conflict_dm_r: diff ok", rc==SQLITE_OK);
  check("conflict_dm_r: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conflict_dm_r: type CONFLICT_DM",
          aChanges[0].type==THREE_WAY_CONFLICT_DM);
    check("conflict_dm_r: key 1", aChanges[0].intKey==1);
    check("conflict_dm_r: our val present", aChanges[0].pOurVal!=0);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_conflict_dm_reversed passed\n");
}

static void test_mixed(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs, tmp;
  int rc;
  const char *path = "/tmp/test_3wd_mixed";
  int foundLeftAdd=0, foundRightDel=0, foundConflict=0;
  int i;

  rc = openTestStore(&cs, &cache, path);
  check("mixed: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1, 2, 3};
    const char *vals[] = {"base-1", "base-2", "base-3"};
    rc = buildTree(&cs, &cache, keys, vals, 3, &ancestor);
    check("mixed: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 4, "ours-added", &tmp);
  check("mixed: ours add", rc==SQLITE_OK);
  rc = treeInsertInt(&cs, &cache, &tmp, 1, "ours-mod", &ours);
  check("mixed: ours mod", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeDeleteInt(&cs, &cache, &ancestor, 3, &tmp);
  check("mixed: theirs del", rc==SQLITE_OK);
  rc = treeInsertInt(&cs, &cache, &tmp, 1, "theirs-mod", &theirs);
  check("mixed: theirs mod", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("mixed: diff ok", rc==SQLITE_OK);
  check("mixed: 3 changes", nChanges==3);

  for(i=0; i<nChanges; i++){
    if( aChanges[i].intKey==1 && aChanges[i].type==THREE_WAY_CONFLICT_MM ){
      foundConflict = 1;
    }
    if( aChanges[i].intKey==3 && aChanges[i].type==THREE_WAY_RIGHT_DELETE ){
      foundRightDel = 1;
    }
    if( aChanges[i].intKey==4 && aChanges[i].type==THREE_WAY_LEFT_ADD ){
      foundLeftAdd = 1;
    }
  }
  check("mixed: found conflict on key 1", foundConflict);
  check("mixed: found right delete on key 3", foundRightDel);
  check("mixed: found left add on key 4", foundLeftAdd);

  closeTestStore(&cs, &cache, path);
  printf("  test_mixed passed\n");
}

static void test_empty_ancestor(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_empty_anc";

  rc = openTestStore(&cs, &cache, path);
  check("empty_anc: open", rc==SQLITE_OK);

  memset(&ancestor, 0, sizeof(ancestor));

  {
    i64 keys[] = {1, 2};
    const char *vals[] = {"a", "b"};
    rc = buildTree(&cs, &cache, keys, vals, 2, &ours);
    check("empty_anc: build ours", rc==SQLITE_OK);
  }

  {
    i64 keys[] = {3, 4};
    const char *vals[] = {"c", "d"};
    rc = buildTree(&cs, &cache, keys, vals, 2, &theirs);
    check("empty_anc: build theirs", rc==SQLITE_OK);
  }

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("empty_anc: diff ok", rc==SQLITE_OK);
  check("empty_anc: 4 changes", nChanges==4);

  {
    int leftAdds=0, rightAdds=0;
    int i;
    for(i=0; i<nChanges; i++){
      if( aChanges[i].type==THREE_WAY_LEFT_ADD ) leftAdds++;
      if( aChanges[i].type==THREE_WAY_RIGHT_ADD ) rightAdds++;
    }
    check("empty_anc: 2 left adds", leftAdds==2);
    check("empty_anc: 2 right adds", rightAdds==2);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_empty_ancestor passed\n");
}

static void test_mixed_shape_empty_ancestor(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  u8 blobKey[8];
  int rc;
  int i;
  int leftAdds = 0, rightAdds = 0, paired = 0;
  const char *path = "/tmp/test_3wd_mixed_shape";

  rc = openTestStore(&cs, &cache, path);
  check("mixed_shape: open", rc==SQLITE_OK);
  memset(&ancestor, 0, sizeof(ancestor));
  prollyEncodeIntKey(1, blobKey);

  rc = treeInsertInt(&cs, &cache, &ancestor, 1, "ours", &ours);
  check("mixed_shape: ours", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);
  rc = treeInsertBlob(&cs, &cache, &ancestor, blobKey, 8, "theirs", &theirs);
  check("mixed_shape: theirs", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          0, PROLLY_NODE_INTKEY, PROLLY_NODE_BLOBKEY,
                          collectCallback, 0);
  check("mixed_shape: diff ok", rc==SQLITE_OK);
  for(i=0; i<nChanges; i++){
    if( aChanges[i].type==THREE_WAY_LEFT_ADD ) leftAdds++;
    if( aChanges[i].type==THREE_WAY_RIGHT_ADD ) rightAdds++;
    if( aChanges[i].type==THREE_WAY_CONFLICT_MM
     || aChanges[i].type==THREE_WAY_CONVERGENT ){
      paired++;
    }
  }
  check("mixed_shape: 1 left add", leftAdds==1);
  check("mixed_shape: 1 right add", rightAdds==1);
  check("mixed_shape: not paired", paired==0);
  check("mixed_shape: 2 changes", nChanges==2);
  if( nChanges>=2 ){
    check("mixed_shape: right key is blob",
          aChanges[0].type==THREE_WAY_RIGHT_ADD
            ? aChanges[0].nKey==8
            : aChanges[1].nKey==8);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_mixed_shape_empty_ancestor passed\n");
}

static void test_conflict_add(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_conf_add";

  rc = openTestStore(&cs, &cache, path);
  check("conflict_add: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"base"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &ancestor);
    check("conflict_add: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 5, "ours-5", &ours);
  check("conflict_add: ours", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeInsertInt(&cs, &cache, &ancestor, 5, "theirs-5", &theirs);
  check("conflict_add: theirs", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("conflict_add: diff ok", rc==SQLITE_OK);
  check("conflict_add: 1 change", nChanges==1);
  if( nChanges>=1 ){
    check("conflict_add: type CONFLICT_MM",
          aChanges[0].type==THREE_WAY_CONFLICT_MM);
    check("conflict_add: key 5", aChanges[0].intKey==5);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_conflict_add passed\n");
}

static void test_many_rows(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs, tmp;
  int rc;
  const char *path = "/tmp/test_3wd_many";
  int i;

  rc = openTestStore(&cs, &cache, path);
  check("many_rows: open", rc==SQLITE_OK);

  {
    ProllyHash cur;
    memset(&cur, 0, sizeof(cur));
    for(i=1; i<=20; i++){
      char val[32];
      snprintf(val, sizeof(val), "val-%d", i);
      rc = treeInsertInt(&cs, &cache, &cur, (i64)i, val, &tmp);
      if( rc!=SQLITE_OK ) break;
      cur = tmp;
      chunkStoreCommit(&cs);
    }
    ancestor = cur;
    check("many_rows: build ancestor", rc==SQLITE_OK);
  }

  {
    ProllyHash cur = ancestor;
    for(i=1; i<=5; i++){
      char val[32];
      snprintf(val, sizeof(val), "ours-%d", i);
      rc = treeInsertInt(&cs, &cache, &cur, (i64)i, val, &tmp);
      if( rc!=SQLITE_OK ) break;
      cur = tmp;
      chunkStoreCommit(&cs);
    }
    ours = cur;
    check("many_rows: build ours", rc==SQLITE_OK);
  }

  {
    ProllyHash cur = ancestor;
    for(i=16; i<=20; i++){
      char val[32];
      snprintf(val, sizeof(val), "theirs-%d", i);
      rc = treeInsertInt(&cs, &cache, &cur, (i64)i, val, &tmp);
      if( rc!=SQLITE_OK ) break;
      cur = tmp;
      chunkStoreCommit(&cs);
    }
    theirs = cur;
    check("many_rows: build theirs", rc==SQLITE_OK);
  }

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("many_rows: diff ok", rc==SQLITE_OK);
  check("many_rows: 10 changes", nChanges==10);

  {
    int lm=0, rm=0;
    for(i=0; i<nChanges; i++){
      if( aChanges[i].type==THREE_WAY_LEFT_MODIFY ) lm++;
      if( aChanges[i].type==THREE_WAY_RIGHT_MODIFY ) rm++;
    }
    check("many_rows: 5 left modifies", lm==5);
    check("many_rows: 5 right modifies", rm==5);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_many_rows passed\n");
}

static void test_sorted_output(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash ancestor, ours, theirs, tmp;
  int rc;
  const char *path = "/tmp/test_3wd_sorted";
  int i;

  rc = openTestStore(&cs, &cache, path);
  check("sorted: open", rc==SQLITE_OK);

  {
    i64 keys[] = {10, 20, 30};
    const char *vals[] = {"a", "b", "c"};
    rc = buildTree(&cs, &cache, keys, vals, 3, &ancestor);
    check("sorted: build", rc==SQLITE_OK);
  }

  rc = treeInsertInt(&cs, &cache, &ancestor, 5, "new-5", &tmp);
  rc = treeInsertInt(&cs, &cache, &tmp, 20, "mod-20", &ours);
  check("sorted: ours", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  rc = treeInsertInt(&cs, &cache, &ancestor, 25, "new-25", &tmp);
  rc = treeDeleteInt(&cs, &cache, &tmp, 30, &theirs);
  check("sorted: theirs", rc==SQLITE_OK);
  rc = chunkStoreCommit(&cs);

  resetChanges();
  rc = prollyThreeWayDiff(&cs, &cache, &ancestor, &ours, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("sorted: diff ok", rc==SQLITE_OK);
  check("sorted: 4 changes", nChanges==4);

  if( nChanges==4 ){
    check("sorted: key[0]=5", aChanges[0].intKey==5);
    check("sorted: key[1]=20", aChanges[1].intKey==20);
    check("sorted: key[2]=25", aChanges[2].intKey==25);
    check("sorted: key[3]=30", aChanges[3].intKey==30);
  }

  closeTestStore(&cs, &cache, path);
  printf("  test_sorted_output passed\n");
}

static void poisonStack(void){
  volatile u8 buf[8192];
  memset((void*)buf, 0xEF, sizeof(buf));
}

static void test_open_failure_cleanup(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyHash bogus, theirs;
  int rc;
  const char *path = "/tmp/test_3wd_openfail";

  rc = openTestStore(&cs, &cache, path);
  check("open_fail: open", rc==SQLITE_OK);

  {
    i64 keys[] = {1};
    const char *vals[] = {"x"};
    rc = buildTree(&cs, &cache, keys, vals, 1, &theirs);
    check("open_fail: build theirs", rc==SQLITE_OK);
  }

  memset(&bogus, 0xAB, sizeof(bogus));

  resetChanges();
  poisonStack();
  rc = prollyThreeWayDiff(&cs, &cache, &bogus, &bogus, &theirs,
                          PROLLY_NODE_INTKEY, PROLLY_NODE_INTKEY,
                          PROLLY_NODE_INTKEY, collectCallback, 0);
  check("open_fail: diff returns error", rc!=SQLITE_OK);
  check("open_fail: no changes emitted", nChanges==0);

  closeTestStore(&cs, &cache, path);
  printf("  test_open_failure_cleanup passed\n");
}

int main(void){
  sqlite3_initialize();
  printf("Three-way diff engine unit tests\n");
  printf("=================================\n\n");

  test_identical();
  test_left_add();
  test_right_add();
  test_left_delete();
  test_right_delete();
  test_left_modify();
  test_right_modify();
  test_convergent_modify();
  test_convergent_delete();
  test_convergent_add();
  test_conflict_mm();
  test_conflict_dm();
  test_conflict_dm_reversed();
  test_mixed();
  test_empty_ancestor();
  test_mixed_shape_empty_ancestor();
  test_conflict_add();
  test_many_rows();
  test_sorted_output();
  test_open_failure_cleanup();

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
