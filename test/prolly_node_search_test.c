#include "prolly_node.h"
#include <stdio.h>
#include <string.h>

static int nPass;
static int nFail;

static void check(const char *zName, int ok){
  if( ok ){
    nPass++;
  }else{
    fprintf(stderr, "FAIL: %s\n", zName);
    nFail++;
  }
}

static void search(const ProllyNode *pNode, const u8 *pKey, int nKey){
  int expected = 0;
  int expectedRes = -1;
  int actualRes;
  int actual;
  int i;
  for(i=0; i<pNode->nItems; i++){
    const u8 *pStored;
    int nStored;
    int c;
    prollyNodeKey(pNode, i, &pStored, &nStored);
    c = memcmp(pKey, pStored, MIN(nKey, nStored));
    if( c==0 ) c = nKey-nStored;
    expected = i;
    expectedRes = c<0 ? -1 : c>0;
    if( c<=0 ) break;
  }
  actual = prollyNodeSearchBlob(pNode, pKey, nKey, &actualRes, 0);
  check("search matches linear comparison",
        actual==expected && actualRes==expectedRes);
}

static void testKeys(int nPrefix, int nItems){
  ProllyNodeBuilder builder;
  ProllyNode node;
  u8 key[1100];
  u8 *pData = 0;
  int nData = 0;
  int rc;
  int i, j, k;
  static const u8 bytes[] = {0, 0x7f, 0x80, 0xff};

  if( nItems==0 ){
    memset(key, 0, 8);
    PROLLY_PUT_U32(key, PROLLY_NODE_MAGIC);
    key[7] = PROLLY_NODE_BLOBKEY;
    check("parse empty node", prollyNodeParse(&node, key, 8)==SQLITE_OK);
    search(&node, (const u8*)"", 0);
    search(&node, (const u8*)"key", 3);
    return;
  }
  prollyNodeBuilderInit(&builder, 0, PROLLY_NODE_BLOBKEY);
  check("add empty key", prollyNodeBuilderAdd(&builder,
        (const u8*)"", 0, (const u8*)"v", 1)==SQLITE_OK);
  for(i=0; i<nItems; i++){
    memset(key, 0x80, sizeof(key));
    key[nPrefix] = (u8)(i*2);
    rc = prollyNodeBuilderAdd(&builder, key, nPrefix+1+i%9,
                             (const u8*)"v", 1);
    check("add key", rc==SQLITE_OK);
  }
  rc = prollyNodeBuilderFinish(&builder, &pData, &nData);
  check("finish node", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    rc = prollyNodeParse(&node, pData, nData);
    check("parse node", rc==SQLITE_OK);
    if( rc==SQLITE_OK ){
      search(&node, (const u8*)"", 0);
      for(i=0; i<node.nItems; i++){
        const u8 *pStored;
        int nStored;
        prollyNodeKey(&node, i, &pStored, &nStored);
        memcpy(key, pStored, nStored);
        key[nStored] = 0;
        for(j=0; j<=nStored+1; j++) search(&node, key, j);
        for(j=0; j<nStored; j++){
          for(k=0; k<(int)sizeof(bytes); k++){
            key[j] = bytes[k];
            search(&node, key, nStored);
          }
          key[j] = pStored[j];
        }
      }
    }
  }
  sqlite3_free(pData);
  prollyNodeBuilderFree(&builder);
}

static void testFinishSparseOverflow(void){
  ProllyNodeBuilder builder;
  ProllyNode node;
  u8 key[8];
  u8 val[1] = {0x42};
  u8 *pData = 0;
  int nData = 0;
  i64 nZero = 0;
  int rc;
  const i64 maxAllowedZeroTail = (i64)0x7fffffff - 8 - 16 - 8 - 1;

  memset(key, 0x11, sizeof(key));

  prollyNodeBuilderInit(&builder, 0, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAddZeroTail(&builder, key, 8, val, 1,
                                    (i64)0x7fffffff - 1);
  check("add_zero_tail_large", rc==SQLITE_OK);
  rc = prollyNodeBuilderFinishSparse(&builder, &pData, &nData, &nZero);
  check("finish_sparse_overflow_toobig",
        rc==SQLITE_TOOBIG && pData==0 && nData==0 && nZero==0);
  sqlite3_free(pData);
  prollyNodeBuilderFree(&builder);

  pData = 0;
  nData = 0;
  nZero = 0;
  prollyNodeBuilderInit(&builder, 0, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAddZeroTail(&builder, key, 8, val, 1,
                                    maxAllowedZeroTail + 1);
  check("add_zero_tail_max_plus_one", rc==SQLITE_OK);
  rc = prollyNodeBuilderFinishSparse(&builder, &pData, &nData, &nZero);
  check("finish_sparse_max_plus_one_toobig",
        rc==SQLITE_TOOBIG && pData==0 && nData==0 && nZero==0);
  sqlite3_free(pData);
  prollyNodeBuilderFree(&builder);

  pData = 0;
  nData = 0;
  nZero = 0;
  prollyNodeBuilderInit(&builder, 0, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAddZeroTail(&builder, key, 8, val, 1,
                                    maxAllowedZeroTail);
  check("add_zero_tail_exact_max", rc==SQLITE_OK);
  rc = prollyNodeBuilderFinishSparse(&builder, &pData, &nData, &nZero);
  check("finish_sparse_exact_max_ok",
        rc==SQLITE_OK && pData!=0 && nData==0x7fffffff
        && nZero==maxAllowedZeroTail);
  if( rc==SQLITE_OK ){
    rc = prollyNodeParseSparse(&node, pData, nData, nData - (int)nZero);
    check("parse_sparse_exact_max_ok", rc==SQLITE_OK && node.nItems==1);
  }
  sqlite3_free(pData);
  prollyNodeBuilderFree(&builder);

  prollyNodeBuilderInit(&builder, 0, PROLLY_NODE_INTKEY);
  rc = prollyNodeBuilderAddZeroTail(&builder, key, 8, val, 1,
                                    (i64)0x7fffffffffffffffLL);
  check("add_zero_tail_i64_max_nomem", rc==SQLITE_NOMEM);
  rc = prollyNodeBuilderAddZeroTail(&builder, key, 8, val, -1, 16);
  check("add_zero_tail_neg_prefix_nomem", rc==SQLITE_NOMEM);
  prollyNodeBuilderFree(&builder);
}

int main(void){
  int i;
  sqlite3_initialize();
  testKeys(0, 0);
  testKeys(0, 1);
  for(i=0; i<=40; i++) testKeys(i, 64);
  for(i=63; i<=73; i++) testKeys(i, 64);
  testKeys(255, 64);
  testKeys(1024, 64);
  testFinishSparseOverflow();
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
