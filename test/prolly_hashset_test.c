#include <stdio.h>
#include <string.h>
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"

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

static void hashFromInt(ProllyHash *h, u32 v){
  memset(h, 0, sizeof(*h));
  h->data[0] = (u8)(v);
  h->data[1] = (u8)(v>>8);
  h->data[2] = (u8)(v>>16);
  h->data[3] = (u8)(v>>24);
}

static void test_basic(void){
  ProllyHashSet hs;
  ProllyHash a, b;
  int rc, i;

  rc = prollyHashSetInit(&hs, 64);
  check("basic: init", rc==SQLITE_OK);

  hashFromInt(&a, 1);
  hashFromInt(&b, 2);
  check("basic: absent before add", !prollyHashSetContains(&hs, &a));
  check("basic: add a", prollyHashSetAdd(&hs, &a)==SQLITE_OK);
  check("basic: present after add", prollyHashSetContains(&hs, &a));
  check("basic: b still absent", !prollyHashSetContains(&hs, &b));
  check("basic: add a again idempotent", prollyHashSetAdd(&hs, &a)==SQLITE_OK);

  for(i=0; i<1000; i++){
    ProllyHash h;
    hashFromInt(&h, (u32)(100+i));
    if( prollyHashSetAdd(&hs, &h)!=SQLITE_OK ){ check("basic: bulk add", 0); break; }
  }
  check("basic: grown member present", prollyHashSetContains(&hs, &a));
  prollyHashSetFree(&hs);
}

/* Reject capacity where rounded slots * sizeof(ProllyHash) exceeds INT_MAX; 1e8 -> 2^28*20 wraps 32-bit size. */
static void test_capacity_overflow_rejected(void){
  ProllyHashSet hs;
  int rc;
  memset(&hs, 0xA5, sizeof(hs));
  rc = prollyHashSetInit(&hs, 100000000);
  check("overflow: rejected with NOMEM", rc==SQLITE_NOMEM);
  check("overflow: zeroes struct on error",
      hs.aSlots==0 && hs.aUsed==0 && hs.nSlots==0 && hs.nUsed==0);
  prollyHashSetFree(&hs);
}

static sqlite3_mem_methods gRealMem;
static int gFailAtAlloc = 0;
static int gFailAll = 0;
static int gAllocCount = 0;
static int gNonNullFrees = 0;

static void *faultMalloc(int n){
  gAllocCount++;
  if( gFailAll || (gFailAtAlloc>0 && gAllocCount==gFailAtAlloc) ){
    return 0;
  }
  return gRealMem.xMalloc(n);
}

static void faultFree(void *p){
  if( p!=0 ) gNonNullFrees++;
  gRealMem.xFree(p);
}

static void test_init_partial_oom(void){
  sqlite3_mem_methods wrapMem;
  ProllyHashSet hs;
  ProllyHash h;
  sqlite3_int64 nBefore;
  int rc, i, freesBefore, cap;

  check("oom: shutdown before config", sqlite3_shutdown()==SQLITE_OK);
  check("oom: get default allocator",
      sqlite3_config(SQLITE_CONFIG_GETMALLOC, &gRealMem)==SQLITE_OK);
  wrapMem = gRealMem;
  wrapMem.xMalloc = faultMalloc;
  wrapMem.xFree = faultFree;
  check("oom: install fault allocator",
      sqlite3_config(SQLITE_CONFIG_MALLOC, &wrapMem)==SQLITE_OK);
  check("oom: initialize with fault allocator", sqlite3_initialize()==SQLITE_OK);

  nBefore = sqlite3_memory_used();

  /* Fail 2nd allocation (aSlots succeeds, aUsed fails). */
  memset(&hs, 0xA5, sizeof(hs));
  gAllocCount = 0;
  gFailAtAlloc = 2;
  gFailAll = 0;
  rc = prollyHashSetInit(&hs, 64);
  gFailAtAlloc = 0;
  check("oom aUsed: returns NOMEM", rc==SQLITE_NOMEM);
  check("oom aUsed: zeroes struct",
      hs.aSlots==0 && hs.aUsed==0 && hs.nSlots==0 && hs.nUsed==0);
  freesBefore = gNonNullFrees;
  prollyHashSetFree(&hs);
  check("oom aUsed: free does not double-free", gNonNullFrees==freesBefore);
  check("oom aUsed: no memory leak", sqlite3_memory_used()==nBefore);

  /* Fail 1st allocation (aSlots fails, aUsed succeeds). */
  memset(&hs, 0xA5, sizeof(hs));
  gAllocCount = 0;
  gFailAtAlloc = 1;
  gFailAll = 0;
  rc = prollyHashSetInit(&hs, 64);
  gFailAtAlloc = 0;
  check("oom aSlots: returns NOMEM", rc==SQLITE_NOMEM);
  check("oom aSlots: zeroes struct",
      hs.aSlots==0 && hs.aUsed==0 && hs.nSlots==0 && hs.nUsed==0);
  freesBefore = gNonNullFrees;
  prollyHashSetFree(&hs);
  check("oom aSlots: free does not double-free", gNonNullFrees==freesBefore);
  check("oom aSlots: no memory leak", sqlite3_memory_used()==nBefore);

  /* Fail both allocations. */
  memset(&hs, 0xA5, sizeof(hs));
  gAllocCount = 0;
  gFailAtAlloc = 0;
  gFailAll = 1;
  rc = prollyHashSetInit(&hs, 64);
  gFailAll = 0;
  check("oom both: returns NOMEM", rc==SQLITE_NOMEM);
  check("oom both: zeroes struct",
      hs.aSlots==0 && hs.aUsed==0 && hs.nSlots==0 && hs.nUsed==0);
  freesBefore = gNonNullFrees;
  prollyHashSetFree(&hs);
  check("oom both: free does not double-free", gNonNullFrees==freesBefore);
  check("oom both: no memory leak", sqlite3_memory_used()==nBefore);

  /* Partial OOM during grow preserves existing set and frees cleanly. */
  rc = prollyHashSetInit(&hs, 64);
  check("oom grow: init", rc==SQLITE_OK);
  if( rc==SQLITE_OK ){
    cap = hs.nSlots / 2;
    for(i=0; i<cap; i++){
      hashFromInt(&h, (u32)(i + 1));
      rc = prollyHashSetAdd(&hs, &h);
      if( rc!=SQLITE_OK ) break;
    }
    check("oom grow: fill to threshold", rc==SQLITE_OK);
    hashFromInt(&h, (u32)(cap + 1));
    gAllocCount = 0;
    gFailAtAlloc = 2;
    rc = prollyHashSetAdd(&hs, &h);
    gFailAtAlloc = 0;
    check("oom grow: returns NOMEM", rc==SQLITE_NOMEM);
    hashFromInt(&h, 1);
    check("oom grow: existing member preserved", prollyHashSetContains(&hs, &h));
    prollyHashSetFree(&hs);
    check("oom grow: no memory leak", sqlite3_memory_used()==nBefore);
  }

  check("oom: shutdown after test", sqlite3_shutdown()==SQLITE_OK);
  check("oom: restore default allocator",
      sqlite3_config(SQLITE_CONFIG_MALLOC, &gRealMem)==SQLITE_OK);
  check("oom: reinitialize default allocator", sqlite3_initialize()==SQLITE_OK);
}

int main(void){
  sqlite3_initialize();
  printf("Prolly hashset unit tests\n");
  printf("=========================\n\n");

  test_basic();
  test_capacity_overflow_rejected();
  test_init_partial_oom();

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
