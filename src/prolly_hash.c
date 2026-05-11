#ifdef DOLTLITE_PROLLY

#include "prolly_hash.h"
#include "blake3.h"
#include <string.h>

void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut){
  blake3_hasher h;
  u8 digest[BLAKE3_OUT_LEN];
  blake3_hasher_init(&h);
  blake3_hasher_update(&h, pData, (size_t)nData);
  blake3_hasher_finalize(&h, digest, BLAKE3_OUT_LEN);
  memcpy(pOut->data, digest, PROLLY_HASH_SIZE);
}

int prollyHashCompare(const ProllyHash *a, const ProllyHash *b){
  return memcmp(a->data, b->data, PROLLY_HASH_SIZE);
}

int prollyHashIsEmpty(const ProllyHash *h){
  int i;
  for(i = 0; i < PROLLY_HASH_SIZE; i++){
    if( h->data[i] != 0 ) return 0;
  }
  return 1;
}

#include <math.h>

#define PROLLY_WEIBULL_L  4096.0
#define PROLLY_MAX_U32    4294967295.0

/* Boundary predicate for content-defined chunking. `size` is the candidate
** chunk size after adding the current item; `thisSize` is that item size. */
int prollyWeibullCheck(u32 size, u32 thisSize, u32 hash){
  double pow;
  double start, end;
  double p, d, target;

  pow = (double)(size - thisSize) / PROLLY_WEIBULL_L;
  start = -expm1(-(pow * pow * pow * pow));

  pow = (double)size / PROLLY_WEIBULL_L;
  end = -expm1(-(pow * pow * pow * pow));

  p = (double)hash / PROLLY_MAX_U32;
  d = 1.0 - start;
  if( d <= 0.0 ) return 1;

  target = (end - start) / d;
  return p < target;
}

#endif
