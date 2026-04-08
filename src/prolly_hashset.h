
#ifndef SQLITE_PROLLY_HASHSET_H
#define SQLITE_PROLLY_HASHSET_H

#include "prolly_hash.h"

typedef struct ProllyHashSet ProllyHashSet;
struct ProllyHashSet {
  ProllyHash *aSlots;   
  u8 *aUsed;            
  int nSlots;           
  int nUsed;            
};

int prollyHashSetInit(ProllyHashSet *hs, int nCapacity);
void prollyHashSetFree(ProllyHashSet *hs);
int prollyHashSetContains(ProllyHashSet *hs, const ProllyHash *h);
int prollyHashSetAdd(ProllyHashSet *hs, const ProllyHash *h);

#endif 
