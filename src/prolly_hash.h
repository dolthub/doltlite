
#ifndef SQLITE_PROLLY_HASH_H
#define SQLITE_PROLLY_HASH_H

#include "sqliteInt.h"

#define PROLLY_HASH_SIZE 20

typedef struct ProllyHash ProllyHash;
struct ProllyHash {
  u8 data[PROLLY_HASH_SIZE];
};

void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut);

int prollyHashCompare(const ProllyHash *a, const ProllyHash *b);

int prollyHashIsEmpty(const ProllyHash *h);

typedef struct ProllyRollingHash ProllyRollingHash;
struct ProllyRollingHash {
  u32 hash;
  int windowSize;
  int pos;
  int filled;
  u8 *window;
};

int prollyRollingHashInit(ProllyRollingHash *rh, int windowSize);

u32 prollyRollingHashUpdate(ProllyRollingHash *rh, u8 byte);

int prollyRollingHashAtBoundary(ProllyRollingHash *rh, u32 pattern);

void prollyRollingHashReset(ProllyRollingHash *rh);

void prollyRollingHashFree(ProllyRollingHash *rh);

#endif
