#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "blake3.h"

typedef struct ProllyHash { uint8_t data[20]; } ProllyHash;
extern void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut);
extern void prollyHashComputeMany(const uint8_t *const *apData,
                                  const int *anData, int n, ProllyHash *aOut);

static void print_hex(const uint8_t *p, int n){
  int i;
  for(i=0; i<n; i++) printf("%02x", p[i]);
}

int main(int argc, char **argv){
  const char *test;
  if( argc<2 ){
    fprintf(stderr, "usage: %s <test>\n", argv[0]);
    return 2;
  }
  test = argv[1];

  if( !strcmp(test, "empty-blake3-32") ){
    blake3_hasher h;
    uint8_t out[32];
    blake3_hasher_init(&h);
    blake3_hasher_update(&h, "", 0);
    blake3_hasher_finalize(&h, out, 32);
    print_hex(out, 32);
    printf("\n");
    return 0;
  }

  if( !strcmp(test, "prolly-empty") ){
    ProllyHash h;
    prollyHashCompute("", 0, &h);
    print_hex(h.data, 20);
    printf("\n");
    return 0;
  }
  if( !strcmp(test, "prolly-abc") ){
    ProllyHash h;
    prollyHashCompute("abc", 3, &h);
    print_hex(h.data, 20);
    printf("\n");
    return 0;
  }
  if( !strcmp(test, "prolly-1024") ){
    uint8_t buf[1024];
    int i;
    ProllyHash h;
    for(i=0; i<1024; i++) buf[i] = (uint8_t)(i % 256);
    prollyHashCompute(buf, 1024, &h);
    print_hex(h.data, 20);
    printf("\n");
    return 0;
  }
  if( !strcmp(test, "prolly-4096") ){
    uint8_t *buf = malloc(4096);
    int i;
    ProllyHash h;
    if( !buf ) return 1;
    for(i=0; i<4096; i++) buf[i] = (uint8_t)(i % 256);
    prollyHashCompute(buf, 4096, &h);
    free(buf);
    print_hex(h.data, 20);
    printf("\n");
    return 0;
  }
  if( !strcmp(test, "prolly-16384") ){
    uint8_t *buf = malloc(16384);
    int i;
    ProllyHash h;
    if( !buf ) return 1;
    for(i=0; i<16384; i++) buf[i] = (uint8_t)(i % 256);
    prollyHashCompute(buf, 16384, &h);
    free(buf);
    print_hex(h.data, 20);
    printf("\n");
    return 0;
  }
  /* Arbitrary length, so the suite can pin sizes whose chunk count does not
  ** fill a SIMD batch -- those take a different path through hash_many(). */
  if( !strcmp(test, "prolly-size") ){
    uint8_t *buf;
    int n;
    int i;
    ProllyHash h;
    if( argc<3 ) return 2;
    n = atoi(argv[2]);
    if( n<0 ) return 2;
    buf = malloc(n ? (size_t)n : 1);
    if( !buf ) return 1;
    for(i=0; i<n; i++) buf[i] = (uint8_t)(i % 256);
    prollyHashCompute(buf, n, &h);
    free(buf);
    print_hex(h.data, 20);
    printf("\n");
    return 0;
  }

  /* Batched hashing must match one-at-a-time hashing for every length,
  ** including chunk boundaries and buffers sharing a batch with others. */
  if( !strcmp(test, "prolly-many") ){
    static uint8_t buf[40000];
    const uint8_t *ap[40];
    int an[40];
    ProllyHash aMany[40], one;
    uint32_t r = 1;
    int nCase = 0, nBad = 0;
    int L, k, i;
    for(i=0; i<(int)sizeof(buf); i++) buf[i] = (uint8_t)(i*131 + 7);
    for(L=0; L<=34000; L++){
      int n, me;
      if( L>4200 && (L%1024)>1 && (L%1024)<1023 ) continue;
      r = r*1103515245u + 12345u; n = 1 + (int)((r>>16) % 40);
      r = r*1103515245u + 12345u; me = (int)((r>>16) % (uint32_t)n);
      for(k=0; k<n; k++){
        r = r*1103515245u + 12345u;
        an[k] = k==me ? L : (int)((r>>8) % 8000);
        ap[k] = buf + (r>>20) % 5000;
      }
      prollyHashComputeMany(ap, an, n, aMany);
      for(k=0; k<n; k++){
        prollyHashCompute(ap[k], an[k], &one);
        nCase++;
        if( memcmp(one.data, aMany[k].data, 20) ){
          if( !nBad ) printf("mismatch at length %d\n", an[k]);
          nBad++;
        }
      }
    }
    if( nBad ) return 1;
    printf("ok\n");
    return nCase>0 ? 0 : 1;
  }

  fprintf(stderr, "unknown test: %s\n", test);
  return 2;
}
