#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct TestKey {
  u8 key[256];
  int nKey;
  int value;
} TestKey;

static int nPass;
static int nFail;

static int check(const char *zName, int ok){
  if( ok ){
    nPass++;
  }else{
    fprintf(stderr, "FAIL: %s\n", zName);
    nFail++;
  }
  return ok;
}

static int keyCmp(const void *a, const void *b){
  const TestKey *ka = a;
  const TestKey *kb = b;
  int c = memcmp(ka->key, kb->key, MIN(ka->nKey, kb->nKey));
  return c ? c : ka->nKey-kb->nKey;
}

static i64 intKey(ProllyMutMap *mm, const TestKey *k){
  return mm->isIntKey ? prollyDecodeIntKey(k->key) : 0;
}

static void verify(ProllyMutMap *mm, TestKey *keys, int n, int edited){
  ProllyMutMapIter it;
  int i;
  if( !check("sort map", prollyMutMapIterFirst(&it, mm)==SQLITE_OK) ) return;
  for(i=0; i<n; i++){
    ProllyMutMapEntry *e;
    int deleted = edited && i%3==0;
    int value = edited && i%3!=0 && i%5==0 ? -1 : keys[i].value;
    if( !check("iterator valid", prollyMutMapIterValid(&it)) ) break;
    e = prollyMutMapIterEntry(&it);
    check("iteration matches bytewise oracle",
          e->nKey==keys[i].nKey
          && (e->nKey==0 || memcmp(e->pKey, keys[i].key, e->nKey)==0));
    check("operation survives sorting",
          e->op==(deleted ? PROLLY_EDIT_DELETE : PROLLY_EDIT_INSERT));
    if( !deleted ){
      check("value survives sorting",
            e->nVal==sizeof(value) && memcmp(e->pVal, &value, sizeof(value))==0);
    }
    e = 0;
    check("lookup succeeds", prollyMutMapFindRc(mm, keys[i].key,
          keys[i].nKey, intKey(mm, &keys[i]), &e)==SQLITE_OK);
    check("lookup matches iteration", e==prollyMutMapIterEntry(&it));
    prollyMutMapIterNext(&it);
  }
  check("iterator exhausted", !prollyMutMapIterValid(&it));
}

static void testOrder(int nPrefix, int nKeys, int wide, int isInt){
  TestKey keys[1024];
  ProllyMutMap mm;
  int order[1024];
  int n = nKeys;
  unsigned state = 42;
  int i;
  memset(keys, 0, sizeof(keys));
  prollyMutMapInitMode(&mm, isInt, 0);
  for(i=0; i<nKeys; i++){
    TestKey *k = &keys[i];
    u32 x = wide ? (u32)(i+1)*2654435761u : (u32)(i+1);
    k->value = i;
    if( isInt ){
      i64 v = ((i64)(x & 0x7fffffff) << 32) | (u32)(x*97u);
      if( i%2 ) v = -v;
      if( i==0 ) v = SMALLEST_INT64;
      if( i==1 ) v = LARGEST_INT64;
      if( i==2 ) v = 0;
      prollyEncodeIntKey(v, k->key);
      k->nKey = 8;
    }else{
      k->key[nPrefix] = (u8)(x >> 24);
      k->key[nPrefix+1] = (u8)(x >> 16);
      k->key[nPrefix+2] = (u8)(x >> 8);
      k->key[nPrefix+3] = (u8)x;
      k->nKey = nPrefix==128 ? sizeof(k->key) : nPrefix+4+i%3;
    }
  }
  if( !isInt ){
    for(i=0; i<nPrefix+4; i++){
      keys[n].nKey = i;
      keys[n].value = n;
      n++;
    }
  }
  for(i=0; i<n; i++) order[i] = i;
  for(i=n-1; i>0; i--){
    int j, tmp;
    state = state*1664525u + 1013904223u;
    j = (int)(state % (unsigned)(i+1));
    tmp = order[i]; order[i] = order[j]; order[j] = tmp;
  }
  for(i=0; i<n; i++){
    TestKey *k = &keys[order[i]];
    check("insert shuffled key", prollyMutMapInsert(&mm, k->key, k->nKey,
          intKey(&mm, k), (const u8*)&k->value, sizeof(k->value))==SQLITE_OK);
  }
  qsort(keys, n, sizeof(keys[0]), keyCmp);
  check("distinct keys retained", prollyMutMapCount(&mm)==n);
  verify(&mm, keys, n, 0);
  prollyMutMapPushSavepoint(&mm, 1);
  for(i=n-1; i>=0; i--){
    TestKey *k = &keys[i];
    if( i%3==0 ){
      check("delete key", prollyMutMapDelete(&mm, k->key, k->nKey,
            intKey(&mm, k))==SQLITE_OK);
    }else if( i%5==0 ){
      int value = -1;
      check("replace key", prollyMutMapInsert(&mm, k->key, k->nKey,
            intKey(&mm, k), (const u8*)&value, sizeof(value))==SQLITE_OK);
    }
  }
  verify(&mm, keys, n, 1);
  check("rollback edits", prollyMutMapRollbackToSavepoint(&mm, 1)==SQLITE_OK);
  verify(&mm, keys, n, 0);
  prollyMutMapFree(&mm);
}

int main(void){
  static const int prefixes[] = {0, 7, 8, 15, 16, 23, 24, 31, 32, 64, 128};
  int i, wide;
  sqlite3_initialize();
  for(wide=0; wide<=1; wide++){
    for(i=0; i<(int)(sizeof(prefixes)/sizeof(prefixes[0])); i++){
      testOrder(prefixes[i], 512, wide, 0);
    }
    testOrder(0, 512, wide, 1);
    testOrder(0, 16, wide, 0);
    testOrder(0, 123, wide, 0);
    testOrder(0, 124, wide, 0);
  }
  printf("%d passed, %d failed\n", nPass, nFail);
  sqlite3_shutdown();
  return nFail ? 1 : 0;
}
