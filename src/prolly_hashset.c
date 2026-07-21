
#ifdef DOLTLITE_PROLLY

#include "prolly_hashset.h"
#include <string.h>

static u32 prollyHashSetSlotIndex(const ProllyHash *h, int nSlots){
  u32 v = (u32)h->data[0] | ((u32)h->data[1]<<8) |
          ((u32)h->data[2]<<16) | ((u32)h->data[3]<<24);
  return v & (nSlots - 1);
}

int prollyHashSetInit(ProllyHashSet *hs, int nCapacity){
  i64 n = 256;
  i64 want = (i64)nCapacity * 2;
  while( n < want ) n *= 2;
  if( n > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ) return SQLITE_NOMEM;
  hs->aSlots = sqlite3_malloc64((sqlite3_uint64)n * sizeof(ProllyHash));
  hs->aUsed = sqlite3_malloc64((sqlite3_uint64)n);
  if( !hs->aSlots || !hs->aUsed ){
    sqlite3_free(hs->aSlots);
    sqlite3_free(hs->aUsed);
    return SQLITE_NOMEM;
  }
  memset(hs->aUsed, 0, (size_t)n);
  hs->nSlots = (int)n;
  hs->nUsed = 0;
  return SQLITE_OK;
}

void prollyHashSetFree(ProllyHashSet *hs){
  sqlite3_free(hs->aSlots);
  sqlite3_free(hs->aUsed);
  memset(hs, 0, sizeof(*hs));
}

int prollyHashSetContains(ProllyHashSet *hs, const ProllyHash *h){
  u32 idx = prollyHashSetSlotIndex(h, hs->nSlots);
  int i;
  for(i=0; i<hs->nSlots; i++){
    u32 slot = (idx + i) & (hs->nSlots - 1);
    if( !hs->aUsed[slot] ) return 0;
    if( memcmp(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE)==0 ) return 1;
  }
  return 0;
}

static int prollyHashSetGrow(ProllyHashSet *hs);

int prollyHashSetAdd(ProllyHashSet *hs, const ProllyHash *h){
  u32 idx;
  int i;
  if( hs->nUsed >= hs->nSlots / 2 ){
    int rc = prollyHashSetGrow(hs);
    if( rc!=SQLITE_OK ) return rc;
  }
  idx = prollyHashSetSlotIndex(h, hs->nSlots);
  for(i=0; i<hs->nSlots; i++){
    u32 slot = (idx + i) & (hs->nSlots - 1);
    if( !hs->aUsed[slot] ){
      memcpy(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE);
      hs->aUsed[slot] = 1;
      hs->nUsed++;
      return SQLITE_OK;
    }
    if( memcmp(hs->aSlots[slot].data, h->data, PROLLY_HASH_SIZE)==0 ){
      return SQLITE_OK;
    }
  }
  return SQLITE_FULL;
}

static int prollyHashSetGrow(ProllyHashSet *hs){
  ProllyHashSet newHs;
  int i, rc;
  i64 newSize = (i64)hs->nSlots * 2;
  if( newSize > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ) return SQLITE_NOMEM;
  newHs.aSlots = sqlite3_malloc64((sqlite3_uint64)newSize * sizeof(ProllyHash));
  newHs.aUsed = sqlite3_malloc64((sqlite3_uint64)newSize);
  if( !newHs.aSlots || !newHs.aUsed ){
    sqlite3_free(newHs.aSlots);
    sqlite3_free(newHs.aUsed);
    return SQLITE_NOMEM;
  }
  memset(newHs.aUsed, 0, (size_t)newSize);
  newHs.nSlots = (int)newSize;
  newHs.nUsed = 0;
  for(i=0; i<hs->nSlots; i++){
    if( hs->aUsed[i] ){
      rc = prollyHashSetAdd(&newHs, &hs->aSlots[i]);
      if( rc!=SQLITE_OK ){
        sqlite3_free(newHs.aSlots);
        sqlite3_free(newHs.aUsed);
        return rc;
      }
    }
  }
  sqlite3_free(hs->aSlots);
  sqlite3_free(hs->aUsed);
  *hs = newHs;
  return SQLITE_OK;
}

#endif
