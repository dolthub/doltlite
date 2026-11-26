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

static sqlite3_mem_methods oomOrigAlloc;
static int oomFailCountdown;

static int oomShouldFail(void){
  if( oomFailCountdown<=0 ) return 0;
  oomFailCountdown--;
  return oomFailCountdown==0;
}

static void *oomMalloc(int n){
  if( oomShouldFail() ) return 0;
  return oomOrigAlloc.xMalloc(n);
}

static void *oomRealloc(void *p, int n){
  if( oomShouldFail() ) return 0;
  return oomOrigAlloc.xRealloc(p, n);
}

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
    ProllyMutMapEntry *e = 0;
    check("insert shuffled key", prollyMutMapInsertGetEntry(&mm, k->key, k->nKey,
          intKey(&mm, k), (const u8*)&k->value, sizeof(k->value), &e)==SQLITE_OK);
    check("insert returns stored entry", e && e->nKey==k->nKey
          && (e->nKey==0 || memcmp(e->pKey, k->key, e->nKey)==0)
          && e->nVal==sizeof(k->value)
          && memcmp(e->pVal, &k->value, sizeof(k->value))==0);
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
      ProllyMutMapEntry *e = 0;
      check("replace key", prollyMutMapInsertGetEntry(&mm, k->key, k->nKey,
            intKey(&mm, k), (const u8*)&value, sizeof(value), &e)==SQLITE_OK);
      check("replacement returns stored entry", e && e->op==PROLLY_EDIT_INSERT
            && e->nVal==sizeof(value) && memcmp(e->pVal, &value, sizeof(value))==0);
    }
  }
  verify(&mm, keys, n, 1);
  check("rollback edits", prollyMutMapRollbackToSavepoint(&mm, 1)==SQLITE_OK);
  verify(&mm, keys, n, 0);
  prollyMutMapFree(&mm);
}

static void testWritePhases(int isInt){
  ProllyMutMap mm;
  TestKey keys[768];
  int phase, i;
  prollyMutMapInitMode(&mm, isInt, 0);
  for(phase=0; phase<3; phase++){
    if( phase==1 ) prollyMutMapPushSavepoint(&mm, 1);
    for(i=0; i<256; i++){
      int value = phase*256 + (i%2 ? 255-i/2 : i/2);
      TestKey *k = &keys[phase*256+i];
      memset(k, 0, sizeof(*k));
      prollyEncodeIntKey(value, k->key);
      k->nKey = 8;
      k->value = value;
      check("insert after ordered scan", prollyMutMapInsert(&mm,
            k->key, k->nKey, intKey(&mm, k),
            (const u8*)&value, sizeof(value))==SQLITE_OK);
    }
    qsort(keys, (phase+1)*256, sizeof(keys[0]), keyCmp);
    verify(&mm, keys, (phase+1)*256, 0);
  }
  check("rollback write phases", prollyMutMapRollbackToSavepoint(&mm, 1)==SQLITE_OK);
  verify(&mm, keys, 256, 0);
  prollyMutMapFree(&mm);
}

static void testReinsertDeleteOom(void){
  sqlite3_mem_methods methods;
  sqlite3_int64 nBefore;
  u8 bigVal[128];
  u8 smallVal[4] = {1, 2, 3, 4};
  u8 otherVal[4] = {9, 8, 7, 6};
  ProllyMutMap mm;
  ProllyMutMapEntry *e;
  ProllyMutMapEntry *out;
  int rc;
  int bornBefore;
  int undoBefore;

  memset(bigVal, 0xab, sizeof(bigVal));
  check("get oom allocator",
        sqlite3_config(SQLITE_CONFIG_GETMALLOC, &oomOrigAlloc)==SQLITE_OK);
  methods = oomOrigAlloc;
  methods.xMalloc = oomMalloc;
  methods.xRealloc = oomRealloc;
  check("install oom allocator",
        sqlite3_config(SQLITE_CONFIG_MALLOC, &methods)==SQLITE_OK);
  check("init oom allocator", sqlite3_initialize()==SQLITE_OK);
  nBefore = sqlite3_memory_used();

  prollyMutMapInit(&mm, 1);

  check("delete key 1 without savepoint",
        prollyMutMapDelete(&mm, 0, 0, 1)==SQLITE_OK);
  check("delete key 2 without savepoint",
        prollyMutMapDelete(&mm, 0, 0, 2)==SQLITE_OK);
  check("delete key 3 without savepoint",
        prollyMutMapDelete(&mm, 0, 0, 3)==SQLITE_OK);

  e = 0;
  check("find deleted key 1", prollyMutMapFindRc(&mm, 0, 0, 1, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  out = (ProllyMutMapEntry*)1;
  oomFailCountdown = 1;
  rc = prollyMutMapInsertGetEntry(&mm, 0, 0, 1, bigVal, sizeof(bigVal), &out);
  oomFailCountdown = 0;
  check("reinsert InsertGetEntry OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("reinsert InsertGetEntry OOM clears ppEntry", out==0);
  check("reinsert InsertGetEntry OOM keeps DELETE",
        e->op==PROLLY_EDIT_DELETE && e->nVal==0 && e->nZeroTail==0
        && e->bornAt==bornBefore);

  e = 0;
  check("find deleted key 2", prollyMutMapFindRc(&mm, 0, 0, 2, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  oomFailCountdown = 1;
  rc = prollyMutMapInsertZeroTail(&mm, 2, bigVal, sizeof(bigVal), 64);
  oomFailCountdown = 0;
  check("reinsert InsertZeroTail OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("reinsert InsertZeroTail OOM keeps DELETE",
        e->op==PROLLY_EDIT_DELETE && e->nVal==0 && e->nZeroTail==0
        && e->bornAt==bornBefore);

  e = 0;
  check("find deleted key 3", prollyMutMapFindRc(&mm, 0, 0, 3, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  oomFailCountdown = 1;
  rc = prollyMutMapReplaceEntry(&mm, e, bigVal, sizeof(bigVal));
  oomFailCountdown = 0;
  check("reinsert ReplaceEntry OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("reinsert ReplaceEntry OOM keeps DELETE",
        e->op==PROLLY_EDIT_DELETE && e->nVal==0 && e->nZeroTail==0
        && e->bornAt==bornBefore);

  check("insert key 4 for savepoint undo warm-up",
        prollyMutMapInsert(&mm, 0, 0, 4, smallVal, sizeof(smallVal))==SQLITE_OK);
  check("insert key 5 for live replace OOM",
        prollyMutMapInsert(&mm, 0, 0, 5, smallVal, sizeof(smallVal))==SQLITE_OK);

  prollyMutMapPushSavepoint(&mm, 1);
  check("warm undo array via key 4",
        prollyMutMapInsert(&mm, 0, 0, 4, otherVal, sizeof(otherVal))==SQLITE_OK);
  undoBefore = mm.nUndo;
  check("undo array warmed", undoBefore==1 && mm.nUndoAlloc>undoBefore);

  e = 0;
  check("find deleted key 1 in savepoint",
        prollyMutMapFindRc(&mm, 0, 0, 1, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  out = (ProllyMutMapEntry*)1;
  oomFailCountdown = 1;
  rc = prollyMutMapInsertGetEntry(&mm, 0, 0, 1, bigVal, sizeof(bigVal), &out);
  oomFailCountdown = 0;
  check("sp reinsert InsertGetEntry OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("sp reinsert InsertGetEntry OOM clears ppEntry", out==0);
  check("sp reinsert InsertGetEntry OOM keeps DELETE and undo count",
        e->op==PROLLY_EDIT_DELETE && e->nVal==0 && e->nZeroTail==0
        && e->bornAt==bornBefore && mm.nUndo==undoBefore);

  e = 0;
  check("find deleted key 2 in savepoint",
        prollyMutMapFindRc(&mm, 0, 0, 2, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  oomFailCountdown = 1;
  rc = prollyMutMapInsertZeroTail(&mm, 2, bigVal, sizeof(bigVal), 64);
  oomFailCountdown = 0;
  check("sp reinsert InsertZeroTail OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("sp reinsert InsertZeroTail OOM keeps DELETE and undo count",
        e->op==PROLLY_EDIT_DELETE && e->nVal==0 && e->nZeroTail==0
        && e->bornAt==bornBefore && mm.nUndo==undoBefore);

  e = 0;
  check("find deleted key 3 in savepoint",
        prollyMutMapFindRc(&mm, 0, 0, 3, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  oomFailCountdown = 1;
  rc = prollyMutMapReplaceEntry(&mm, e, bigVal, sizeof(bigVal));
  oomFailCountdown = 0;
  check("sp reinsert ReplaceEntry OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("sp reinsert ReplaceEntry OOM keeps DELETE and undo count",
        e->op==PROLLY_EDIT_DELETE && e->nVal==0 && e->nZeroTail==0
        && e->bornAt==bornBefore && mm.nUndo==undoBefore);

  e = 0;
  check("find live key 5 in savepoint",
        prollyMutMapFindRc(&mm, 0, 0, 5, &e)==SQLITE_OK && e);
  bornBefore = e->bornAt;
  oomFailCountdown = 2;
  rc = prollyMutMapInsertGetEntry(&mm, 0, 0, 5, bigVal, sizeof(bigVal), &out);
  oomFailCountdown = 0;
  check("sp replace live key OOM returns NOMEM", rc==SQLITE_NOMEM);
  check("sp replace live key OOM preserves value and pops undo",
        e->op==PROLLY_EDIT_INSERT && e->nVal==sizeof(smallVal)
        && memcmp(e->pVal, smallVal, sizeof(smallVal))==0
        && e->bornAt==bornBefore && mm.nUndo==undoBefore);

  check("rollback savepoint after OOM",
        prollyMutMapRollbackToSavepoint(&mm, 1)==SQLITE_OK);
  check("undo log empty after rollback", mm.nUndo==0);

  e = 0;
  check("key 1 still DELETE after rollback",
        prollyMutMapFindRc(&mm, 0, 0, 1, &e)==SQLITE_OK
        && e && e->op==PROLLY_EDIT_DELETE && e->nVal==0);
  check("key 4 restored after rollback",
        prollyMutMapFindRc(&mm, 0, 0, 4, &e)==SQLITE_OK
        && e && e->op==PROLLY_EDIT_INSERT && e->nVal==sizeof(smallVal)
        && memcmp(e->pVal, smallVal, sizeof(smallVal))==0);

  check("reinsert key 1 succeeds after OOM clears",
        prollyMutMapInsert(&mm, 0, 0, 1, bigVal, sizeof(bigVal))==SQLITE_OK);
  check("key 1 is INSERT with full value",
        prollyMutMapFindRc(&mm, 0, 0, 1, &e)==SQLITE_OK
        && e && e->op==PROLLY_EDIT_INSERT && e->nVal==sizeof(bigVal)
        && memcmp(e->pVal, bigVal, sizeof(bigVal))==0);

  prollyMutMapFree(&mm);
  check("oom test releases all allocations", sqlite3_memory_used()==nBefore);
  check("shutdown oom allocator", sqlite3_shutdown()==SQLITE_OK);
  check("restore oom allocator",
        sqlite3_config(SQLITE_CONFIG_MALLOC, &oomOrigAlloc)==SQLITE_OK);
}

int main(void){
  static const int prefixes[] = {0, 7, 8, 15, 16, 23, 24, 31, 32, 64, 128};
  int i, wide;
  testReinsertDeleteOom();
  sqlite3_initialize();
  testWritePhases(0);
  testWritePhases(1);
  check("empty keys compare equal", prollyKeyCmp(0, 0, 0, 0)==0);
  check("empty key sorts first", prollyKeyCmp(0, 0, (const u8*)"x", 1)<0);
  check("nonempty key sorts last", prollyKeyCmp((const u8*)"x", 1, 0, 0)>0);
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
