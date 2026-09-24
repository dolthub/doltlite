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
  actual = prollyNodeSearchBlob(pNode, pKey, nKey, &actualRes);
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

int main(void){
  int i;
  sqlite3_initialize();
  testKeys(0, 0);
  testKeys(0, 1);
  for(i=0; i<=40; i++) testKeys(i, 64);
  testKeys(64, 64);
  testKeys(255, 64);
  testKeys(1024, 64);
  printf("%d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
