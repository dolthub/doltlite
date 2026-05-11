
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

int prollyWeibullCheck(u32 size, u32 thisSize, u32 hash);

#endif
