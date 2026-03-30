/*
** Open-addressing hash set keyed by ProllyHash.
** Shared by GC mark phase and sync engine.
*/
#ifndef SQLITE_PROLLY_HASHSET_H
#define SQLITE_PROLLY_HASHSET_H

#include "prolly_hash.h"

typedef struct ProllyHashSet ProllyHashSet;
struct ProllyHashSet {
  ProllyHash *aSlots;   /* Hash slot array */
  u8 *aUsed;            /* 1 if slot is occupied */
  int nSlots;           /* Total slots (power of 2) */
  int nUsed;            /* Number of entries */
};

int prollyHashSetInit(ProllyHashSet *hs, int nCapacity);
void prollyHashSetFree(ProllyHashSet *hs);
int prollyHashSetContains(ProllyHashSet *hs, const ProllyHash *h);
int prollyHashSetAdd(ProllyHashSet *hs, const ProllyHash *h);

#endif /* SQLITE_PROLLY_HASHSET_H */
