#ifdef DOLTLITE_PROLLY

#include "prolly_hash.h"
#include "prolly_chunker.h"
#include "blake3.h"
#include <string.h>

void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut){
  blake3_hasher h;
  blake3_hasher_init(&h);
  blake3_hasher_update(&h, pData, (size_t)nData);
  blake3_hasher_finalize(&h, pOut->data, PROLLY_HASH_SIZE);
}

/* Hash of pData[0..nData) followed by nZeroTail zero bytes, without
** materializing the zeros. */
void prollyHashComputeZeroTail(const void *pData, int nData, sqlite3_int64 nZeroTail,
                               ProllyHash *pOut){
  blake3_hasher h;
  static const u8 aZero[8192];
  blake3_hasher_init(&h);
  if( nData>0 ) blake3_hasher_update(&h, pData, (size_t)nData);
  while( nZeroTail>0 ){
    size_t n = nZeroTail > (sqlite3_int64)sizeof(aZero)
             ? sizeof(aZero) : (size_t)nZeroTail;
    blake3_hasher_update(&h, aZero, n);
    nZeroTail -= (sqlite3_int64)n;
  }
  blake3_hasher_finalize(&h, pOut->data, PROLLY_HASH_SIZE);
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
#if defined(__GNUC__) || defined(__clang__)
  static double aWeibull[PROLLY_CHUNK_MAX + 1];
  static volatile int initState = 0;
  double start;
  double end;
  double p;
  double d;
  double target;

  if( initState!=2 ){
    if( __sync_bool_compare_and_swap(&initState, 0, 1) ){
      u32 i;
      for(i=0; i<=PROLLY_CHUNK_MAX; i++){
        double pow = (double)i / PROLLY_WEIBULL_L;
        aWeibull[i] = -expm1(-(pow * pow * pow * pow));
      }
      __sync_synchronize();
      initState = 2;
    }else{
      while( initState!=2 ){
        /* Another thread is initializing the immutable lookup table. */
      }
      __sync_synchronize();
    }
  }

  assert( size <= PROLLY_CHUNK_MAX );
  assert( thisSize <= size );
  start = aWeibull[size - thisSize];
  end = aWeibull[size];

  p = (double)hash / PROLLY_MAX_U32;
  d = 1.0 - start;
  if( d <= 0.0 ) return 1;

  target = (end - start) / d;
  return p < target;
#else
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
#endif
}

#endif
