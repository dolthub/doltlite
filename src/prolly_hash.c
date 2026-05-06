#ifdef DOLTLITE_PROLLY

#include "prolly_hash.h"
#include "blake3.h"
#include <string.h>

void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut){
  /* BLAKE3 with the high 12 bytes of the 32-byte digest dropped to
  ** match PROLLY_HASH_SIZE (20). BLAKE3's portable C path measures
  ** ~3-5x faster than the SHA-512 truncation it replaces, with the
  ** same collision properties for content addressing. The truncation
  ** preserves bit-uniformity since BLAKE3's output is itself a
  ** uniform digest. */
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

/* Weibull-distribution chunk-boundary check.
**
** Ported from Dolt's keySplitter.weibullCheck
** (go/store/prolly/tree/node_splitter.go). Given that we have not
** split on any record up through |size - thisSize|, the conditional
** probability that we should split on the current record is
**
**     (CDF(size) - CDF(size - thisSize)) / (1 - CDF(size - thisSize))
**
** where CDF is the Weibull CDF with shape K=4, scale L=4096:
**
**     CDF(x) = 1 - exp(-(x/L)^K)
**
** We split if the uniform sample |hash|/MAX_U32 falls within that
** conditional probability. Result is a chunk-size distribution that
** clusters around L (the target) with a much shorter tail than the
** geometric distribution a static pattern produces.
**
** K is hand-unrolled (pow*pow*pow*pow) to avoid pow()/Gamma() calls
** in the hot path, matching the Dolt implementation. */
#define PROLLY_WEIBULL_L  4096.0
#define PROLLY_MAX_U32    4294967295.0

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
