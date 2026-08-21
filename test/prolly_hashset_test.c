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
  memset(&hs, 0, sizeof(hs));
  rc = prollyHashSetInit(&hs, 100000000);
  check("overflow: rejected with NOMEM", rc==SQLITE_NOMEM);
  if( rc==SQLITE_OK ) prollyHashSetFree(&hs);
}

int main(void){
  sqlite3_initialize();
  printf("Prolly hashset unit tests\n");
  printf("=========================\n\n");

  test_basic();
  test_capacity_overflow_rejected();

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
