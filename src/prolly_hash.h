
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

/* Weibull-distribution chunk-boundary check.
**
** Returns 1 if a chunk should split at the current record. Treats
** `hash` as a uniform random number in [0, 2^32). The probability of
** splitting rises as `size` approaches the target chunk size, biasing
** the resulting chunk-size distribution to a Weibull (K=4, L=4096)
** rather than a geometric distribution. This tightens the cluster
** around the target size and reduces upward boundary cascades on
** edits.
**
** Ported from Dolt's keySplitter (go/store/prolly/tree/node_splitter.go). */
int prollyWeibullCheck(u32 size, u32 thisSize, u32 hash);

#endif
