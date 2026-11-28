#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#if !defined(_WIN32)
#include <pthread.h>
#endif
#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_xxhash.h"
#include "prolly_chunker.h"
#include "prolly_cursor.h"
#include "prolly_mutate.h"
#include "prolly_node.h"
#include "chunk_store.h"
#include "lib/test_tmpdir.h"

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int cond){
  if( cond ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", name);
  }
}

static void encodeIntKey(i64 v, u8 key[8]){
  int j;
  for(j=7; j>=0; j--){
    key[j] = (u8)(v & 0xff);
    v >>= 8;
  }
}

static int parseHexHash(const char *z, ProllyHash *pOut){
  int i;
  if( !z || (int)strlen(z)!=PROLLY_HASH_SIZE*2 ) return 0;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    unsigned int b = 0;
    if( sscanf(z + 2*i, "%2x", &b)!=1 ) return 0;
    pOut->data[i] = (u8)b;
  }
  return 1;
}

/* XXH32(seed=0), BE int keys 0..31. */
static const struct { int key; u32 h; } aXxh[] = {
  {0, 0xdeb39513u},
  {1, 0xf414a945u},
  {2, 0xd53c8ecbu},
  {3, 0xea0bee70u},
  {4, 0xf8cdd998u},
  {5, 0x17d5f46au},
  {6, 0xb2edcb43u},
  {7, 0x87a9925eu},
  {8, 0xa3301e1du},
  {9, 0x4c537cb4u},
  {10, 0x8b4174ddu},
  {11, 0x5ece03f1u},
  {12, 0xbb28819cu},
  {13, 0x52f7a644u},
  {14, 0x867ce427u},
  {15, 0xfd624c39u},
  {16, 0x15d91f09u},
  {17, 0x9efb9836u},
  {18, 0xd2b74cf3u},
  {19, 0x7d456185u},
  {20, 0xeaabb43du},
  {21, 0xa3f424a0u},
  {22, 0xff4b9b11u},
  {23, 0x0817d47au},
  {24, 0x10d28df6u},
  {25, 0x08ab1330u},
  {26, 0x14c039bdu},
  {27, 0x04d7fa0eu},
  {28, 0xe76e2dbdu},
  {29, 0x74e636d9u},
  {30, 0x3119a83eu},
  {31, 0xf5b8a0e0u},
};

/* Weibull grid, float bit-for-bit. */
static const struct {
  u32 size;
  u32 thisSize;
  u32 hash;
  int want;
} aWeibull[] = {
  {512u, 8u, 0x00000000u, 1},
  {512u, 8u, 0x00000001u, 1},
  {512u, 8u, 0x00000100u, 1},
  {512u, 8u, 0x7fffffffu, 0},
  {512u, 8u, 0x80000000u, 0},
  {512u, 8u, 0xffffffffu, 0},
  {512u, 8u, 0x12345678u, 0},
  {512u, 8u, 0xabcdef01u, 0},
  {512u, 8u, 0x55aa55aau, 0},
  {512u, 8u, 0x0f0f0f0fu, 0},
  {512u, 16u, 0x00000000u, 1},
  {512u, 16u, 0x00000001u, 1},
  {512u, 16u, 0x00000100u, 1},
  {512u, 16u, 0x7fffffffu, 0},
  {512u, 16u, 0x80000000u, 0},
  {512u, 16u, 0xffffffffu, 0},
  {512u, 16u, 0x12345678u, 0},
  {512u, 16u, 0xabcdef01u, 0},
  {512u, 16u, 0x55aa55aau, 0},
  {512u, 16u, 0x0f0f0f0fu, 0},
  {512u, 24u, 0x00000000u, 1},
  {512u, 24u, 0x00000001u, 1},
  {512u, 24u, 0x00000100u, 1},
  {512u, 24u, 0x7fffffffu, 0},
  {512u, 24u, 0x80000000u, 0},
  {512u, 24u, 0xffffffffu, 0},
  {512u, 24u, 0x12345678u, 0},
  {512u, 24u, 0xabcdef01u, 0},
  {512u, 24u, 0x55aa55aau, 0},
  {512u, 24u, 0x0f0f0f0fu, 0},
  {512u, 32u, 0x00000000u, 1},
  {512u, 32u, 0x00000001u, 1},
  {512u, 32u, 0x00000100u, 1},
  {512u, 32u, 0x7fffffffu, 0},
  {512u, 32u, 0x80000000u, 0},
  {512u, 32u, 0xffffffffu, 0},
  {512u, 32u, 0x12345678u, 0},
  {512u, 32u, 0xabcdef01u, 0},
  {512u, 32u, 0x55aa55aau, 0},
  {512u, 32u, 0x0f0f0f0fu, 0},
  {512u, 64u, 0x00000000u, 1},
  {512u, 64u, 0x00000001u, 1},
  {512u, 64u, 0x00000100u, 1},
  {512u, 64u, 0x7fffffffu, 0},
  {512u, 64u, 0x80000000u, 0},
  {512u, 64u, 0xffffffffu, 0},
  {512u, 64u, 0x12345678u, 0},
  {512u, 64u, 0xabcdef01u, 0},
  {512u, 64u, 0x55aa55aau, 0},
  {512u, 64u, 0x0f0f0f0fu, 0},
  {512u, 100u, 0x00000000u, 1},
  {512u, 100u, 0x00000001u, 1},
  {512u, 100u, 0x00000100u, 1},
  {512u, 100u, 0x7fffffffu, 0},
  {512u, 100u, 0x80000000u, 0},
  {512u, 100u, 0xffffffffu, 0},
  {512u, 100u, 0x12345678u, 0},
  {512u, 100u, 0xabcdef01u, 0},
  {512u, 100u, 0x55aa55aau, 0},
  {512u, 100u, 0x0f0f0f0fu, 0},
  {512u, 128u, 0x00000000u, 1},
  {512u, 128u, 0x00000001u, 1},
  {512u, 128u, 0x00000100u, 1},
  {512u, 128u, 0x7fffffffu, 0},
  {512u, 128u, 0x80000000u, 0},
  {512u, 128u, 0xffffffffu, 0},
  {512u, 128u, 0x12345678u, 0},
  {512u, 128u, 0xabcdef01u, 0},
  {512u, 128u, 0x55aa55aau, 0},
  {512u, 128u, 0x0f0f0f0fu, 0},
  {512u, 256u, 0x00000000u, 1},
  {512u, 256u, 0x00000001u, 1},
  {512u, 256u, 0x00000100u, 1},
  {512u, 256u, 0x7fffffffu, 0},
  {512u, 256u, 0x80000000u, 0},
  {512u, 256u, 0xffffffffu, 0},
  {512u, 256u, 0x12345678u, 0},
  {512u, 256u, 0xabcdef01u, 0},
  {512u, 256u, 0x55aa55aau, 0},
  {512u, 256u, 0x0f0f0f0fu, 0},
  {512u, 400u, 0x00000000u, 1},
  {512u, 400u, 0x00000001u, 1},
  {512u, 400u, 0x00000100u, 1},
  {512u, 400u, 0x7fffffffu, 0},
  {512u, 400u, 0x80000000u, 0},
  {512u, 400u, 0xffffffffu, 0},
  {512u, 400u, 0x12345678u, 0},
  {512u, 400u, 0xabcdef01u, 0},
  {512u, 400u, 0x55aa55aau, 0},
  {512u, 400u, 0x0f0f0f0fu, 0},
  {512u, 512u, 0x00000000u, 1},
  {512u, 512u, 0x00000001u, 1},
  {512u, 512u, 0x00000100u, 1},
  {512u, 512u, 0x7fffffffu, 0},
  {512u, 512u, 0x80000000u, 0},
  {512u, 512u, 0xffffffffu, 0},
  {512u, 512u, 0x12345678u, 0},
  {512u, 512u, 0xabcdef01u, 0},
  {512u, 512u, 0x55aa55aau, 0},
  {512u, 512u, 0x0f0f0f0fu, 0},
  {513u, 8u, 0x00000000u, 1},
  {513u, 8u, 0x00000001u, 1},
  {513u, 8u, 0x00000100u, 1},
  {513u, 8u, 0x7fffffffu, 0},
  {513u, 8u, 0x80000000u, 0},
  {513u, 8u, 0xffffffffu, 0},
  {513u, 8u, 0x12345678u, 0},
  {513u, 8u, 0xabcdef01u, 0},
  {513u, 8u, 0x55aa55aau, 0},
  {513u, 8u, 0x0f0f0f0fu, 0},
  {513u, 16u, 0x00000000u, 1},
  {513u, 16u, 0x00000001u, 1},
  {513u, 16u, 0x00000100u, 1},
  {513u, 16u, 0x7fffffffu, 0},
  {513u, 16u, 0x80000000u, 0},
  {513u, 16u, 0xffffffffu, 0},
  {513u, 16u, 0x12345678u, 0},
  {513u, 16u, 0xabcdef01u, 0},
  {513u, 16u, 0x55aa55aau, 0},
  {513u, 16u, 0x0f0f0f0fu, 0},
  {513u, 24u, 0x00000000u, 1},
  {513u, 24u, 0x00000001u, 1},
  {513u, 24u, 0x00000100u, 1},
  {513u, 24u, 0x7fffffffu, 0},
  {513u, 24u, 0x80000000u, 0},
  {513u, 24u, 0xffffffffu, 0},
  {513u, 24u, 0x12345678u, 0},
  {513u, 24u, 0xabcdef01u, 0},
  {513u, 24u, 0x55aa55aau, 0},
  {513u, 24u, 0x0f0f0f0fu, 0},
  {513u, 32u, 0x00000000u, 1},
  {513u, 32u, 0x00000001u, 1},
  {513u, 32u, 0x00000100u, 1},
  {513u, 32u, 0x7fffffffu, 0},
  {513u, 32u, 0x80000000u, 0},
  {513u, 32u, 0xffffffffu, 0},
  {513u, 32u, 0x12345678u, 0},
  {513u, 32u, 0xabcdef01u, 0},
  {513u, 32u, 0x55aa55aau, 0},
  {513u, 32u, 0x0f0f0f0fu, 0},
  {513u, 64u, 0x00000000u, 1},
  {513u, 64u, 0x00000001u, 1},
  {513u, 64u, 0x00000100u, 1},
  {513u, 64u, 0x7fffffffu, 0},
  {513u, 64u, 0x80000000u, 0},
  {513u, 64u, 0xffffffffu, 0},
  {513u, 64u, 0x12345678u, 0},
  {513u, 64u, 0xabcdef01u, 0},
  {513u, 64u, 0x55aa55aau, 0},
  {513u, 64u, 0x0f0f0f0fu, 0},
  {513u, 100u, 0x00000000u, 1},
  {513u, 100u, 0x00000001u, 1},
  {513u, 100u, 0x00000100u, 1},
  {513u, 100u, 0x7fffffffu, 0},
  {513u, 100u, 0x80000000u, 0},
  {513u, 100u, 0xffffffffu, 0},
  {513u, 100u, 0x12345678u, 0},
  {513u, 100u, 0xabcdef01u, 0},
  {513u, 100u, 0x55aa55aau, 0},
  {513u, 100u, 0x0f0f0f0fu, 0},
  {513u, 128u, 0x00000000u, 1},
  {513u, 128u, 0x00000001u, 1},
  {513u, 128u, 0x00000100u, 1},
  {513u, 128u, 0x7fffffffu, 0},
  {513u, 128u, 0x80000000u, 0},
  {513u, 128u, 0xffffffffu, 0},
  {513u, 128u, 0x12345678u, 0},
  {513u, 128u, 0xabcdef01u, 0},
  {513u, 128u, 0x55aa55aau, 0},
  {513u, 128u, 0x0f0f0f0fu, 0},
  {513u, 256u, 0x00000000u, 1},
  {513u, 256u, 0x00000001u, 1},
  {513u, 256u, 0x00000100u, 1},
  {513u, 256u, 0x7fffffffu, 0},
  {513u, 256u, 0x80000000u, 0},
  {513u, 256u, 0xffffffffu, 0},
  {513u, 256u, 0x12345678u, 0},
  {513u, 256u, 0xabcdef01u, 0},
  {513u, 256u, 0x55aa55aau, 0},
  {513u, 256u, 0x0f0f0f0fu, 0},
  {513u, 400u, 0x00000000u, 1},
  {513u, 400u, 0x00000001u, 1},
  {513u, 400u, 0x00000100u, 1},
  {513u, 400u, 0x7fffffffu, 0},
  {513u, 400u, 0x80000000u, 0},
  {513u, 400u, 0xffffffffu, 0},
  {513u, 400u, 0x12345678u, 0},
  {513u, 400u, 0xabcdef01u, 0},
  {513u, 400u, 0x55aa55aau, 0},
  {513u, 400u, 0x0f0f0f0fu, 0},
  {513u, 512u, 0x00000000u, 1},
  {513u, 512u, 0x00000001u, 1},
  {513u, 512u, 0x00000100u, 1},
  {513u, 512u, 0x7fffffffu, 0},
  {513u, 512u, 0x80000000u, 0},
  {513u, 512u, 0xffffffffu, 0},
  {513u, 512u, 0x12345678u, 0},
  {513u, 512u, 0xabcdef01u, 0},
  {513u, 512u, 0x55aa55aau, 0},
  {513u, 512u, 0x0f0f0f0fu, 0},
  {600u, 8u, 0x00000000u, 1},
  {600u, 8u, 0x00000001u, 1},
  {600u, 8u, 0x00000100u, 1},
  {600u, 8u, 0x7fffffffu, 0},
  {600u, 8u, 0x80000000u, 0},
  {600u, 8u, 0xffffffffu, 0},
  {600u, 8u, 0x12345678u, 0},
  {600u, 8u, 0xabcdef01u, 0},
  {600u, 8u, 0x55aa55aau, 0},
  {600u, 8u, 0x0f0f0f0fu, 0},
  {600u, 16u, 0x00000000u, 1},
  {600u, 16u, 0x00000001u, 1},
  {600u, 16u, 0x00000100u, 1},
  {600u, 16u, 0x7fffffffu, 0},
  {600u, 16u, 0x80000000u, 0},
  {600u, 16u, 0xffffffffu, 0},
  {600u, 16u, 0x12345678u, 0},
  {600u, 16u, 0xabcdef01u, 0},
  {600u, 16u, 0x55aa55aau, 0},
  {600u, 16u, 0x0f0f0f0fu, 0},
  {600u, 24u, 0x00000000u, 1},
  {600u, 24u, 0x00000001u, 1},
  {600u, 24u, 0x00000100u, 1},
  {600u, 24u, 0x7fffffffu, 0},
  {600u, 24u, 0x80000000u, 0},
  {600u, 24u, 0xffffffffu, 0},
  {600u, 24u, 0x12345678u, 0},
  {600u, 24u, 0xabcdef01u, 0},
  {600u, 24u, 0x55aa55aau, 0},
  {600u, 24u, 0x0f0f0f0fu, 0},
  {600u, 32u, 0x00000000u, 1},
  {600u, 32u, 0x00000001u, 1},
  {600u, 32u, 0x00000100u, 1},
  {600u, 32u, 0x7fffffffu, 0},
  {600u, 32u, 0x80000000u, 0},
  {600u, 32u, 0xffffffffu, 0},
  {600u, 32u, 0x12345678u, 0},
  {600u, 32u, 0xabcdef01u, 0},
  {600u, 32u, 0x55aa55aau, 0},
  {600u, 32u, 0x0f0f0f0fu, 0},
  {600u, 64u, 0x00000000u, 1},
  {600u, 64u, 0x00000001u, 1},
  {600u, 64u, 0x00000100u, 1},
  {600u, 64u, 0x7fffffffu, 0},
  {600u, 64u, 0x80000000u, 0},
  {600u, 64u, 0xffffffffu, 0},
  {600u, 64u, 0x12345678u, 0},
  {600u, 64u, 0xabcdef01u, 0},
  {600u, 64u, 0x55aa55aau, 0},
  {600u, 64u, 0x0f0f0f0fu, 0},
  {600u, 100u, 0x00000000u, 1},
  {600u, 100u, 0x00000001u, 1},
  {600u, 100u, 0x00000100u, 1},
  {600u, 100u, 0x7fffffffu, 0},
  {600u, 100u, 0x80000000u, 0},
  {600u, 100u, 0xffffffffu, 0},
  {600u, 100u, 0x12345678u, 0},
  {600u, 100u, 0xabcdef01u, 0},
  {600u, 100u, 0x55aa55aau, 0},
  {600u, 100u, 0x0f0f0f0fu, 0},
  {600u, 128u, 0x00000000u, 1},
  {600u, 128u, 0x00000001u, 1},
  {600u, 128u, 0x00000100u, 1},
  {600u, 128u, 0x7fffffffu, 0},
  {600u, 128u, 0x80000000u, 0},
  {600u, 128u, 0xffffffffu, 0},
  {600u, 128u, 0x12345678u, 0},
  {600u, 128u, 0xabcdef01u, 0},
  {600u, 128u, 0x55aa55aau, 0},
  {600u, 128u, 0x0f0f0f0fu, 0},
  {600u, 256u, 0x00000000u, 1},
  {600u, 256u, 0x00000001u, 1},
  {600u, 256u, 0x00000100u, 1},
  {600u, 256u, 0x7fffffffu, 0},
  {600u, 256u, 0x80000000u, 0},
  {600u, 256u, 0xffffffffu, 0},
  {600u, 256u, 0x12345678u, 0},
  {600u, 256u, 0xabcdef01u, 0},
  {600u, 256u, 0x55aa55aau, 0},
  {600u, 256u, 0x0f0f0f0fu, 0},
  {600u, 400u, 0x00000000u, 1},
  {600u, 400u, 0x00000001u, 1},
  {600u, 400u, 0x00000100u, 1},
  {600u, 400u, 0x7fffffffu, 0},
  {600u, 400u, 0x80000000u, 0},
  {600u, 400u, 0xffffffffu, 0},
  {600u, 400u, 0x12345678u, 0},
  {600u, 400u, 0xabcdef01u, 0},
  {600u, 400u, 0x55aa55aau, 0},
  {600u, 400u, 0x0f0f0f0fu, 0},
  {600u, 512u, 0x00000000u, 1},
  {600u, 512u, 0x00000001u, 1},
  {600u, 512u, 0x00000100u, 1},
  {600u, 512u, 0x7fffffffu, 0},
  {600u, 512u, 0x80000000u, 0},
  {600u, 512u, 0xffffffffu, 0},
  {600u, 512u, 0x12345678u, 0},
  {600u, 512u, 0xabcdef01u, 0},
  {600u, 512u, 0x55aa55aau, 0},
  {600u, 512u, 0x0f0f0f0fu, 0},
  {1024u, 8u, 0x00000000u, 1},
  {1024u, 8u, 0x00000001u, 1},
  {1024u, 8u, 0x00000100u, 1},
  {1024u, 8u, 0x7fffffffu, 0},
  {1024u, 8u, 0x80000000u, 0},
  {1024u, 8u, 0xffffffffu, 0},
  {1024u, 8u, 0x12345678u, 0},
  {1024u, 8u, 0xabcdef01u, 0},
  {1024u, 8u, 0x55aa55aau, 0},
  {1024u, 8u, 0x0f0f0f0fu, 0},
  {1024u, 16u, 0x00000000u, 1},
  {1024u, 16u, 0x00000001u, 1},
  {1024u, 16u, 0x00000100u, 1},
  {1024u, 16u, 0x7fffffffu, 0},
  {1024u, 16u, 0x80000000u, 0},
  {1024u, 16u, 0xffffffffu, 0},
  {1024u, 16u, 0x12345678u, 0},
  {1024u, 16u, 0xabcdef01u, 0},
  {1024u, 16u, 0x55aa55aau, 0},
  {1024u, 16u, 0x0f0f0f0fu, 0},
  {1024u, 24u, 0x00000000u, 1},
  {1024u, 24u, 0x00000001u, 1},
  {1024u, 24u, 0x00000100u, 1},
  {1024u, 24u, 0x7fffffffu, 0},
  {1024u, 24u, 0x80000000u, 0},
  {1024u, 24u, 0xffffffffu, 0},
  {1024u, 24u, 0x12345678u, 0},
  {1024u, 24u, 0xabcdef01u, 0},
  {1024u, 24u, 0x55aa55aau, 0},
  {1024u, 24u, 0x0f0f0f0fu, 0},
  {1024u, 32u, 0x00000000u, 1},
  {1024u, 32u, 0x00000001u, 1},
  {1024u, 32u, 0x00000100u, 1},
  {1024u, 32u, 0x7fffffffu, 0},
  {1024u, 32u, 0x80000000u, 0},
  {1024u, 32u, 0xffffffffu, 0},
  {1024u, 32u, 0x12345678u, 0},
  {1024u, 32u, 0xabcdef01u, 0},
  {1024u, 32u, 0x55aa55aau, 0},
  {1024u, 32u, 0x0f0f0f0fu, 0},
  {1024u, 64u, 0x00000000u, 1},
  {1024u, 64u, 0x00000001u, 1},
  {1024u, 64u, 0x00000100u, 1},
  {1024u, 64u, 0x7fffffffu, 0},
  {1024u, 64u, 0x80000000u, 0},
  {1024u, 64u, 0xffffffffu, 0},
  {1024u, 64u, 0x12345678u, 0},
  {1024u, 64u, 0xabcdef01u, 0},
  {1024u, 64u, 0x55aa55aau, 0},
  {1024u, 64u, 0x0f0f0f0fu, 0},
  {1024u, 100u, 0x00000000u, 1},
  {1024u, 100u, 0x00000001u, 1},
  {1024u, 100u, 0x00000100u, 1},
  {1024u, 100u, 0x7fffffffu, 0},
  {1024u, 100u, 0x80000000u, 0},
  {1024u, 100u, 0xffffffffu, 0},
  {1024u, 100u, 0x12345678u, 0},
  {1024u, 100u, 0xabcdef01u, 0},
  {1024u, 100u, 0x55aa55aau, 0},
  {1024u, 100u, 0x0f0f0f0fu, 0},
  {1024u, 128u, 0x00000000u, 1},
  {1024u, 128u, 0x00000001u, 1},
  {1024u, 128u, 0x00000100u, 1},
  {1024u, 128u, 0x7fffffffu, 0},
  {1024u, 128u, 0x80000000u, 0},
  {1024u, 128u, 0xffffffffu, 0},
  {1024u, 128u, 0x12345678u, 0},
  {1024u, 128u, 0xabcdef01u, 0},
  {1024u, 128u, 0x55aa55aau, 0},
  {1024u, 128u, 0x0f0f0f0fu, 0},
  {1024u, 256u, 0x00000000u, 1},
  {1024u, 256u, 0x00000001u, 1},
  {1024u, 256u, 0x00000100u, 1},
  {1024u, 256u, 0x7fffffffu, 0},
  {1024u, 256u, 0x80000000u, 0},
  {1024u, 256u, 0xffffffffu, 0},
  {1024u, 256u, 0x12345678u, 0},
  {1024u, 256u, 0xabcdef01u, 0},
  {1024u, 256u, 0x55aa55aau, 0},
  {1024u, 256u, 0x0f0f0f0fu, 0},
  {1024u, 400u, 0x00000000u, 1},
  {1024u, 400u, 0x00000001u, 1},
  {1024u, 400u, 0x00000100u, 1},
  {1024u, 400u, 0x7fffffffu, 0},
  {1024u, 400u, 0x80000000u, 0},
  {1024u, 400u, 0xffffffffu, 0},
  {1024u, 400u, 0x12345678u, 0},
  {1024u, 400u, 0xabcdef01u, 0},
  {1024u, 400u, 0x55aa55aau, 0},
  {1024u, 400u, 0x0f0f0f0fu, 0},
  {1024u, 512u, 0x00000000u, 1},
  {1024u, 512u, 0x00000001u, 1},
  {1024u, 512u, 0x00000100u, 1},
  {1024u, 512u, 0x7fffffffu, 0},
  {1024u, 512u, 0x80000000u, 0},
  {1024u, 512u, 0xffffffffu, 0},
  {1024u, 512u, 0x12345678u, 0},
  {1024u, 512u, 0xabcdef01u, 0},
  {1024u, 512u, 0x55aa55aau, 0},
  {1024u, 512u, 0x0f0f0f0fu, 0},
  {2048u, 8u, 0x00000000u, 1},
  {2048u, 8u, 0x00000001u, 1},
  {2048u, 8u, 0x00000100u, 1},
  {2048u, 8u, 0x7fffffffu, 0},
  {2048u, 8u, 0x80000000u, 0},
  {2048u, 8u, 0xffffffffu, 0},
  {2048u, 8u, 0x12345678u, 0},
  {2048u, 8u, 0xabcdef01u, 0},
  {2048u, 8u, 0x55aa55aau, 0},
  {2048u, 8u, 0x0f0f0f0fu, 0},
  {2048u, 16u, 0x00000000u, 1},
  {2048u, 16u, 0x00000001u, 1},
  {2048u, 16u, 0x00000100u, 1},
  {2048u, 16u, 0x7fffffffu, 0},
  {2048u, 16u, 0x80000000u, 0},
  {2048u, 16u, 0xffffffffu, 0},
  {2048u, 16u, 0x12345678u, 0},
  {2048u, 16u, 0xabcdef01u, 0},
  {2048u, 16u, 0x55aa55aau, 0},
  {2048u, 16u, 0x0f0f0f0fu, 0},
  {2048u, 24u, 0x00000000u, 1},
  {2048u, 24u, 0x00000001u, 1},
  {2048u, 24u, 0x00000100u, 1},
  {2048u, 24u, 0x7fffffffu, 0},
  {2048u, 24u, 0x80000000u, 0},
  {2048u, 24u, 0xffffffffu, 0},
  {2048u, 24u, 0x12345678u, 0},
  {2048u, 24u, 0xabcdef01u, 0},
  {2048u, 24u, 0x55aa55aau, 0},
  {2048u, 24u, 0x0f0f0f0fu, 0},
  {2048u, 32u, 0x00000000u, 1},
  {2048u, 32u, 0x00000001u, 1},
  {2048u, 32u, 0x00000100u, 1},
  {2048u, 32u, 0x7fffffffu, 0},
  {2048u, 32u, 0x80000000u, 0},
  {2048u, 32u, 0xffffffffu, 0},
  {2048u, 32u, 0x12345678u, 0},
  {2048u, 32u, 0xabcdef01u, 0},
  {2048u, 32u, 0x55aa55aau, 0},
  {2048u, 32u, 0x0f0f0f0fu, 0},
  {2048u, 64u, 0x00000000u, 1},
  {2048u, 64u, 0x00000001u, 1},
  {2048u, 64u, 0x00000100u, 1},
  {2048u, 64u, 0x7fffffffu, 0},
  {2048u, 64u, 0x80000000u, 0},
  {2048u, 64u, 0xffffffffu, 0},
  {2048u, 64u, 0x12345678u, 0},
  {2048u, 64u, 0xabcdef01u, 0},
  {2048u, 64u, 0x55aa55aau, 0},
  {2048u, 64u, 0x0f0f0f0fu, 0},
  {2048u, 100u, 0x00000000u, 1},
  {2048u, 100u, 0x00000001u, 1},
  {2048u, 100u, 0x00000100u, 1},
  {2048u, 100u, 0x7fffffffu, 0},
  {2048u, 100u, 0x80000000u, 0},
  {2048u, 100u, 0xffffffffu, 0},
  {2048u, 100u, 0x12345678u, 0},
  {2048u, 100u, 0xabcdef01u, 0},
  {2048u, 100u, 0x55aa55aau, 0},
  {2048u, 100u, 0x0f0f0f0fu, 0},
  {2048u, 128u, 0x00000000u, 1},
  {2048u, 128u, 0x00000001u, 1},
  {2048u, 128u, 0x00000100u, 1},
  {2048u, 128u, 0x7fffffffu, 0},
  {2048u, 128u, 0x80000000u, 0},
  {2048u, 128u, 0xffffffffu, 0},
  {2048u, 128u, 0x12345678u, 0},
  {2048u, 128u, 0xabcdef01u, 0},
  {2048u, 128u, 0x55aa55aau, 0},
  {2048u, 128u, 0x0f0f0f0fu, 0},
  {2048u, 256u, 0x00000000u, 1},
  {2048u, 256u, 0x00000001u, 1},
  {2048u, 256u, 0x00000100u, 1},
  {2048u, 256u, 0x7fffffffu, 0},
  {2048u, 256u, 0x80000000u, 0},
  {2048u, 256u, 0xffffffffu, 0},
  {2048u, 256u, 0x12345678u, 0},
  {2048u, 256u, 0xabcdef01u, 0},
  {2048u, 256u, 0x55aa55aau, 0},
  {2048u, 256u, 0x0f0f0f0fu, 0},
  {2048u, 400u, 0x00000000u, 1},
  {2048u, 400u, 0x00000001u, 1},
  {2048u, 400u, 0x00000100u, 1},
  {2048u, 400u, 0x7fffffffu, 0},
  {2048u, 400u, 0x80000000u, 0},
  {2048u, 400u, 0xffffffffu, 0},
  {2048u, 400u, 0x12345678u, 0},
  {2048u, 400u, 0xabcdef01u, 0},
  {2048u, 400u, 0x55aa55aau, 0},
  {2048u, 400u, 0x0f0f0f0fu, 0},
  {2048u, 512u, 0x00000000u, 1},
  {2048u, 512u, 0x00000001u, 1},
  {2048u, 512u, 0x00000100u, 1},
  {2048u, 512u, 0x7fffffffu, 0},
  {2048u, 512u, 0x80000000u, 0},
  {2048u, 512u, 0xffffffffu, 0},
  {2048u, 512u, 0x12345678u, 0},
  {2048u, 512u, 0xabcdef01u, 0},
  {2048u, 512u, 0x55aa55aau, 0},
  {2048u, 512u, 0x0f0f0f0fu, 0},
  {4096u, 8u, 0x00000000u, 1},
  {4096u, 8u, 0x00000001u, 1},
  {4096u, 8u, 0x00000100u, 1},
  {4096u, 8u, 0x7fffffffu, 0},
  {4096u, 8u, 0x80000000u, 0},
  {4096u, 8u, 0xffffffffu, 0},
  {4096u, 8u, 0x12345678u, 0},
  {4096u, 8u, 0xabcdef01u, 0},
  {4096u, 8u, 0x55aa55aau, 0},
  {4096u, 8u, 0x0f0f0f0fu, 0},
  {4096u, 16u, 0x00000000u, 1},
  {4096u, 16u, 0x00000001u, 1},
  {4096u, 16u, 0x00000100u, 1},
  {4096u, 16u, 0x7fffffffu, 0},
  {4096u, 16u, 0x80000000u, 0},
  {4096u, 16u, 0xffffffffu, 0},
  {4096u, 16u, 0x12345678u, 0},
  {4096u, 16u, 0xabcdef01u, 0},
  {4096u, 16u, 0x55aa55aau, 0},
  {4096u, 16u, 0x0f0f0f0fu, 0},
  {4096u, 24u, 0x00000000u, 1},
  {4096u, 24u, 0x00000001u, 1},
  {4096u, 24u, 0x00000100u, 1},
  {4096u, 24u, 0x7fffffffu, 0},
  {4096u, 24u, 0x80000000u, 0},
  {4096u, 24u, 0xffffffffu, 0},
  {4096u, 24u, 0x12345678u, 0},
  {4096u, 24u, 0xabcdef01u, 0},
  {4096u, 24u, 0x55aa55aau, 0},
  {4096u, 24u, 0x0f0f0f0fu, 0},
  {4096u, 32u, 0x00000000u, 1},
  {4096u, 32u, 0x00000001u, 1},
  {4096u, 32u, 0x00000100u, 1},
  {4096u, 32u, 0x7fffffffu, 0},
  {4096u, 32u, 0x80000000u, 0},
  {4096u, 32u, 0xffffffffu, 0},
  {4096u, 32u, 0x12345678u, 0},
  {4096u, 32u, 0xabcdef01u, 0},
  {4096u, 32u, 0x55aa55aau, 0},
  {4096u, 32u, 0x0f0f0f0fu, 0},
  {4096u, 64u, 0x00000000u, 1},
  {4096u, 64u, 0x00000001u, 1},
  {4096u, 64u, 0x00000100u, 1},
  {4096u, 64u, 0x7fffffffu, 0},
  {4096u, 64u, 0x80000000u, 0},
  {4096u, 64u, 0xffffffffu, 0},
  {4096u, 64u, 0x12345678u, 0},
  {4096u, 64u, 0xabcdef01u, 0},
  {4096u, 64u, 0x55aa55aau, 0},
  {4096u, 64u, 0x0f0f0f0fu, 1},
  {4096u, 100u, 0x00000000u, 1},
  {4096u, 100u, 0x00000001u, 1},
  {4096u, 100u, 0x00000100u, 1},
  {4096u, 100u, 0x7fffffffu, 0},
  {4096u, 100u, 0x80000000u, 0},
  {4096u, 100u, 0xffffffffu, 0},
  {4096u, 100u, 0x12345678u, 1},
  {4096u, 100u, 0xabcdef01u, 0},
  {4096u, 100u, 0x55aa55aau, 0},
  {4096u, 100u, 0x0f0f0f0fu, 1},
  {4096u, 128u, 0x00000000u, 1},
  {4096u, 128u, 0x00000001u, 1},
  {4096u, 128u, 0x00000100u, 1},
  {4096u, 128u, 0x7fffffffu, 0},
  {4096u, 128u, 0x80000000u, 0},
  {4096u, 128u, 0xffffffffu, 0},
  {4096u, 128u, 0x12345678u, 1},
  {4096u, 128u, 0xabcdef01u, 0},
  {4096u, 128u, 0x55aa55aau, 0},
  {4096u, 128u, 0x0f0f0f0fu, 1},
  {4096u, 256u, 0x00000000u, 1},
  {4096u, 256u, 0x00000001u, 1},
  {4096u, 256u, 0x00000100u, 1},
  {4096u, 256u, 0x7fffffffu, 0},
  {4096u, 256u, 0x80000000u, 0},
  {4096u, 256u, 0xffffffffu, 0},
  {4096u, 256u, 0x12345678u, 1},
  {4096u, 256u, 0xabcdef01u, 0},
  {4096u, 256u, 0x55aa55aau, 0},
  {4096u, 256u, 0x0f0f0f0fu, 1},
  {4096u, 400u, 0x00000000u, 1},
  {4096u, 400u, 0x00000001u, 1},
  {4096u, 400u, 0x00000100u, 1},
  {4096u, 400u, 0x7fffffffu, 0},
  {4096u, 400u, 0x80000000u, 0},
  {4096u, 400u, 0xffffffffu, 0},
  {4096u, 400u, 0x12345678u, 1},
  {4096u, 400u, 0xabcdef01u, 0},
  {4096u, 400u, 0x55aa55aau, 0},
  {4096u, 400u, 0x0f0f0f0fu, 1},
  {4096u, 512u, 0x00000000u, 1},
  {4096u, 512u, 0x00000001u, 1},
  {4096u, 512u, 0x00000100u, 1},
  {4096u, 512u, 0x7fffffffu, 0},
  {4096u, 512u, 0x80000000u, 0},
  {4096u, 512u, 0xffffffffu, 0},
  {4096u, 512u, 0x12345678u, 1},
  {4096u, 512u, 0xabcdef01u, 0},
  {4096u, 512u, 0x55aa55aau, 1},
  {4096u, 512u, 0x0f0f0f0fu, 1},
  {8192u, 8u, 0x00000000u, 1},
  {8192u, 8u, 0x00000001u, 1},
  {8192u, 8u, 0x00000100u, 1},
  {8192u, 8u, 0x7fffffffu, 0},
  {8192u, 8u, 0x80000000u, 0},
  {8192u, 8u, 0xffffffffu, 0},
  {8192u, 8u, 0x12345678u, 0},
  {8192u, 8u, 0xabcdef01u, 0},
  {8192u, 8u, 0x55aa55aau, 0},
  {8192u, 8u, 0x0f0f0f0fu, 1},
  {8192u, 16u, 0x00000000u, 1},
  {8192u, 16u, 0x00000001u, 1},
  {8192u, 16u, 0x00000100u, 1},
  {8192u, 16u, 0x7fffffffu, 0},
  {8192u, 16u, 0x80000000u, 0},
  {8192u, 16u, 0xffffffffu, 0},
  {8192u, 16u, 0x12345678u, 1},
  {8192u, 16u, 0xabcdef01u, 0},
  {8192u, 16u, 0x55aa55aau, 0},
  {8192u, 16u, 0x0f0f0f0fu, 1},
  {8192u, 24u, 0x00000000u, 1},
  {8192u, 24u, 0x00000001u, 1},
  {8192u, 24u, 0x00000100u, 1},
  {8192u, 24u, 0x7fffffffu, 0},
  {8192u, 24u, 0x80000000u, 0},
  {8192u, 24u, 0xffffffffu, 0},
  {8192u, 24u, 0x12345678u, 1},
  {8192u, 24u, 0xabcdef01u, 0},
  {8192u, 24u, 0x55aa55aau, 0},
  {8192u, 24u, 0x0f0f0f0fu, 1},
  {8192u, 32u, 0x00000000u, 1},
  {8192u, 32u, 0x00000001u, 1},
  {8192u, 32u, 0x00000100u, 1},
  {8192u, 32u, 0x7fffffffu, 0},
  {8192u, 32u, 0x80000000u, 0},
  {8192u, 32u, 0xffffffffu, 0},
  {8192u, 32u, 0x12345678u, 1},
  {8192u, 32u, 0xabcdef01u, 0},
  {8192u, 32u, 0x55aa55aau, 0},
  {8192u, 32u, 0x0f0f0f0fu, 1},
  {8192u, 64u, 0x00000000u, 1},
  {8192u, 64u, 0x00000001u, 1},
  {8192u, 64u, 0x00000100u, 1},
  {8192u, 64u, 0x7fffffffu, 0},
  {8192u, 64u, 0x80000000u, 0},
  {8192u, 64u, 0xffffffffu, 0},
  {8192u, 64u, 0x12345678u, 1},
  {8192u, 64u, 0xabcdef01u, 0},
  {8192u, 64u, 0x55aa55aau, 1},
  {8192u, 64u, 0x0f0f0f0fu, 1},
  {8192u, 100u, 0x00000000u, 1},
  {8192u, 100u, 0x00000001u, 1},
  {8192u, 100u, 0x00000100u, 1},
  {8192u, 100u, 0x7fffffffu, 1},
  {8192u, 100u, 0x80000000u, 1},
  {8192u, 100u, 0xffffffffu, 0},
  {8192u, 100u, 0x12345678u, 1},
  {8192u, 100u, 0xabcdef01u, 0},
  {8192u, 100u, 0x55aa55aau, 1},
  {8192u, 100u, 0x0f0f0f0fu, 1},
  {8192u, 128u, 0x00000000u, 1},
  {8192u, 128u, 0x00000001u, 1},
  {8192u, 128u, 0x00000100u, 1},
  {8192u, 128u, 0x7fffffffu, 1},
  {8192u, 128u, 0x80000000u, 1},
  {8192u, 128u, 0xffffffffu, 0},
  {8192u, 128u, 0x12345678u, 1},
  {8192u, 128u, 0xabcdef01u, 0},
  {8192u, 128u, 0x55aa55aau, 1},
  {8192u, 128u, 0x0f0f0f0fu, 1},
  {8192u, 256u, 0x00000000u, 1},
  {8192u, 256u, 0x00000001u, 1},
  {8192u, 256u, 0x00000100u, 1},
  {8192u, 256u, 0x7fffffffu, 1},
  {8192u, 256u, 0x80000000u, 1},
  {8192u, 256u, 0xffffffffu, 0},
  {8192u, 256u, 0x12345678u, 1},
  {8192u, 256u, 0xabcdef01u, 1},
  {8192u, 256u, 0x55aa55aau, 1},
  {8192u, 256u, 0x0f0f0f0fu, 1},
  {8192u, 400u, 0x00000000u, 1},
  {8192u, 400u, 0x00000001u, 1},
  {8192u, 400u, 0x00000100u, 1},
  {8192u, 400u, 0x7fffffffu, 1},
  {8192u, 400u, 0x80000000u, 1},
  {8192u, 400u, 0xffffffffu, 0},
  {8192u, 400u, 0x12345678u, 1},
  {8192u, 400u, 0xabcdef01u, 1},
  {8192u, 400u, 0x55aa55aau, 1},
  {8192u, 400u, 0x0f0f0f0fu, 1},
  {8192u, 512u, 0x00000000u, 1},
  {8192u, 512u, 0x00000001u, 1},
  {8192u, 512u, 0x00000100u, 1},
  {8192u, 512u, 0x7fffffffu, 1},
  {8192u, 512u, 0x80000000u, 1},
  {8192u, 512u, 0xffffffffu, 0},
  {8192u, 512u, 0x12345678u, 1},
  {8192u, 512u, 0xabcdef01u, 1},
  {8192u, 512u, 0x55aa55aau, 1},
  {8192u, 512u, 0x0f0f0f0fu, 1},
  {12000u, 8u, 0x00000000u, 1},
  {12000u, 8u, 0x00000001u, 1},
  {12000u, 8u, 0x00000100u, 1},
  {12000u, 8u, 0x7fffffffu, 1},
  {12000u, 8u, 0x80000000u, 1},
  {12000u, 8u, 0xffffffffu, 1},
  {12000u, 8u, 0x12345678u, 1},
  {12000u, 8u, 0xabcdef01u, 1},
  {12000u, 8u, 0x55aa55aau, 1},
  {12000u, 8u, 0x0f0f0f0fu, 1},
  {12000u, 16u, 0x00000000u, 1},
  {12000u, 16u, 0x00000001u, 1},
  {12000u, 16u, 0x00000100u, 1},
  {12000u, 16u, 0x7fffffffu, 1},
  {12000u, 16u, 0x80000000u, 1},
  {12000u, 16u, 0xffffffffu, 1},
  {12000u, 16u, 0x12345678u, 1},
  {12000u, 16u, 0xabcdef01u, 1},
  {12000u, 16u, 0x55aa55aau, 1},
  {12000u, 16u, 0x0f0f0f0fu, 1},
  {12000u, 24u, 0x00000000u, 1},
  {12000u, 24u, 0x00000001u, 1},
  {12000u, 24u, 0x00000100u, 1},
  {12000u, 24u, 0x7fffffffu, 1},
  {12000u, 24u, 0x80000000u, 1},
  {12000u, 24u, 0xffffffffu, 1},
  {12000u, 24u, 0x12345678u, 1},
  {12000u, 24u, 0xabcdef01u, 1},
  {12000u, 24u, 0x55aa55aau, 1},
  {12000u, 24u, 0x0f0f0f0fu, 1},
  {12000u, 32u, 0x00000000u, 1},
  {12000u, 32u, 0x00000001u, 1},
  {12000u, 32u, 0x00000100u, 1},
  {12000u, 32u, 0x7fffffffu, 1},
  {12000u, 32u, 0x80000000u, 1},
  {12000u, 32u, 0xffffffffu, 1},
  {12000u, 32u, 0x12345678u, 1},
  {12000u, 32u, 0xabcdef01u, 1},
  {12000u, 32u, 0x55aa55aau, 1},
  {12000u, 32u, 0x0f0f0f0fu, 1},
  {12000u, 64u, 0x00000000u, 1},
  {12000u, 64u, 0x00000001u, 1},
  {12000u, 64u, 0x00000100u, 1},
  {12000u, 64u, 0x7fffffffu, 1},
  {12000u, 64u, 0x80000000u, 1},
  {12000u, 64u, 0xffffffffu, 1},
  {12000u, 64u, 0x12345678u, 1},
  {12000u, 64u, 0xabcdef01u, 1},
  {12000u, 64u, 0x55aa55aau, 1},
  {12000u, 64u, 0x0f0f0f0fu, 1},
  {12000u, 100u, 0x00000000u, 1},
  {12000u, 100u, 0x00000001u, 1},
  {12000u, 100u, 0x00000100u, 1},
  {12000u, 100u, 0x7fffffffu, 1},
  {12000u, 100u, 0x80000000u, 1},
  {12000u, 100u, 0xffffffffu, 1},
  {12000u, 100u, 0x12345678u, 1},
  {12000u, 100u, 0xabcdef01u, 1},
  {12000u, 100u, 0x55aa55aau, 1},
  {12000u, 100u, 0x0f0f0f0fu, 1},
  {12000u, 128u, 0x00000000u, 1},
  {12000u, 128u, 0x00000001u, 1},
  {12000u, 128u, 0x00000100u, 1},
  {12000u, 128u, 0x7fffffffu, 1},
  {12000u, 128u, 0x80000000u, 1},
  {12000u, 128u, 0xffffffffu, 1},
  {12000u, 128u, 0x12345678u, 1},
  {12000u, 128u, 0xabcdef01u, 1},
  {12000u, 128u, 0x55aa55aau, 1},
  {12000u, 128u, 0x0f0f0f0fu, 1},
  {12000u, 256u, 0x00000000u, 1},
  {12000u, 256u, 0x00000001u, 1},
  {12000u, 256u, 0x00000100u, 1},
  {12000u, 256u, 0x7fffffffu, 1},
  {12000u, 256u, 0x80000000u, 1},
  {12000u, 256u, 0xffffffffu, 1},
  {12000u, 256u, 0x12345678u, 1},
  {12000u, 256u, 0xabcdef01u, 1},
  {12000u, 256u, 0x55aa55aau, 1},
  {12000u, 256u, 0x0f0f0f0fu, 1},
  {12000u, 400u, 0x00000000u, 1},
  {12000u, 400u, 0x00000001u, 1},
  {12000u, 400u, 0x00000100u, 1},
  {12000u, 400u, 0x7fffffffu, 1},
  {12000u, 400u, 0x80000000u, 1},
  {12000u, 400u, 0xffffffffu, 1},
  {12000u, 400u, 0x12345678u, 1},
  {12000u, 400u, 0xabcdef01u, 1},
  {12000u, 400u, 0x55aa55aau, 1},
  {12000u, 400u, 0x0f0f0f0fu, 1},
  {12000u, 512u, 0x00000000u, 1},
  {12000u, 512u, 0x00000001u, 1},
  {12000u, 512u, 0x00000100u, 1},
  {12000u, 512u, 0x7fffffffu, 1},
  {12000u, 512u, 0x80000000u, 1},
  {12000u, 512u, 0xffffffffu, 1},
  {12000u, 512u, 0x12345678u, 1},
  {12000u, 512u, 0xabcdef01u, 1},
  {12000u, 512u, 0x55aa55aau, 1},
  {12000u, 512u, 0x0f0f0f0fu, 1},
  {16383u, 8u, 0x00000000u, 1},
  {16383u, 8u, 0x00000001u, 1},
  {16383u, 8u, 0x00000100u, 1},
  {16383u, 8u, 0x7fffffffu, 1},
  {16383u, 8u, 0x80000000u, 1},
  {16383u, 8u, 0xffffffffu, 1},
  {16383u, 8u, 0x12345678u, 1},
  {16383u, 8u, 0xabcdef01u, 1},
  {16383u, 8u, 0x55aa55aau, 1},
  {16383u, 8u, 0x0f0f0f0fu, 1},
  {16383u, 16u, 0x00000000u, 1},
  {16383u, 16u, 0x00000001u, 1},
  {16383u, 16u, 0x00000100u, 1},
  {16383u, 16u, 0x7fffffffu, 1},
  {16383u, 16u, 0x80000000u, 1},
  {16383u, 16u, 0xffffffffu, 1},
  {16383u, 16u, 0x12345678u, 1},
  {16383u, 16u, 0xabcdef01u, 1},
  {16383u, 16u, 0x55aa55aau, 1},
  {16383u, 16u, 0x0f0f0f0fu, 1},
  {16383u, 24u, 0x00000000u, 1},
  {16383u, 24u, 0x00000001u, 1},
  {16383u, 24u, 0x00000100u, 1},
  {16383u, 24u, 0x7fffffffu, 1},
  {16383u, 24u, 0x80000000u, 1},
  {16383u, 24u, 0xffffffffu, 1},
  {16383u, 24u, 0x12345678u, 1},
  {16383u, 24u, 0xabcdef01u, 1},
  {16383u, 24u, 0x55aa55aau, 1},
  {16383u, 24u, 0x0f0f0f0fu, 1},
  {16383u, 32u, 0x00000000u, 1},
  {16383u, 32u, 0x00000001u, 1},
  {16383u, 32u, 0x00000100u, 1},
  {16383u, 32u, 0x7fffffffu, 1},
  {16383u, 32u, 0x80000000u, 1},
  {16383u, 32u, 0xffffffffu, 1},
  {16383u, 32u, 0x12345678u, 1},
  {16383u, 32u, 0xabcdef01u, 1},
  {16383u, 32u, 0x55aa55aau, 1},
  {16383u, 32u, 0x0f0f0f0fu, 1},
  {16383u, 64u, 0x00000000u, 1},
  {16383u, 64u, 0x00000001u, 1},
  {16383u, 64u, 0x00000100u, 1},
  {16383u, 64u, 0x7fffffffu, 1},
  {16383u, 64u, 0x80000000u, 1},
  {16383u, 64u, 0xffffffffu, 1},
  {16383u, 64u, 0x12345678u, 1},
  {16383u, 64u, 0xabcdef01u, 1},
  {16383u, 64u, 0x55aa55aau, 1},
  {16383u, 64u, 0x0f0f0f0fu, 1},
  {16383u, 100u, 0x00000000u, 1},
  {16383u, 100u, 0x00000001u, 1},
  {16383u, 100u, 0x00000100u, 1},
  {16383u, 100u, 0x7fffffffu, 1},
  {16383u, 100u, 0x80000000u, 1},
  {16383u, 100u, 0xffffffffu, 1},
  {16383u, 100u, 0x12345678u, 1},
  {16383u, 100u, 0xabcdef01u, 1},
  {16383u, 100u, 0x55aa55aau, 1},
  {16383u, 100u, 0x0f0f0f0fu, 1},
  {16383u, 128u, 0x00000000u, 1},
  {16383u, 128u, 0x00000001u, 1},
  {16383u, 128u, 0x00000100u, 1},
  {16383u, 128u, 0x7fffffffu, 1},
  {16383u, 128u, 0x80000000u, 1},
  {16383u, 128u, 0xffffffffu, 1},
  {16383u, 128u, 0x12345678u, 1},
  {16383u, 128u, 0xabcdef01u, 1},
  {16383u, 128u, 0x55aa55aau, 1},
  {16383u, 128u, 0x0f0f0f0fu, 1},
  {16383u, 256u, 0x00000000u, 1},
  {16383u, 256u, 0x00000001u, 1},
  {16383u, 256u, 0x00000100u, 1},
  {16383u, 256u, 0x7fffffffu, 1},
  {16383u, 256u, 0x80000000u, 1},
  {16383u, 256u, 0xffffffffu, 1},
  {16383u, 256u, 0x12345678u, 1},
  {16383u, 256u, 0xabcdef01u, 1},
  {16383u, 256u, 0x55aa55aau, 1},
  {16383u, 256u, 0x0f0f0f0fu, 1},
  {16383u, 400u, 0x00000000u, 1},
  {16383u, 400u, 0x00000001u, 1},
  {16383u, 400u, 0x00000100u, 1},
  {16383u, 400u, 0x7fffffffu, 1},
  {16383u, 400u, 0x80000000u, 1},
  {16383u, 400u, 0xffffffffu, 1},
  {16383u, 400u, 0x12345678u, 1},
  {16383u, 400u, 0xabcdef01u, 1},
  {16383u, 400u, 0x55aa55aau, 1},
  {16383u, 400u, 0x0f0f0f0fu, 1},
  {16383u, 512u, 0x00000000u, 1},
  {16383u, 512u, 0x00000001u, 1},
  {16383u, 512u, 0x00000100u, 1},
  {16383u, 512u, 0x7fffffffu, 1},
  {16383u, 512u, 0x80000000u, 1},
  {16383u, 512u, 0xffffffffu, 1},
  {16383u, 512u, 0x12345678u, 1},
  {16383u, 512u, 0xabcdef01u, 1},
  {16383u, 512u, 0x55aa55aau, 1},
  {16383u, 512u, 0x0f0f0f0fu, 1},
  {16384u, 8u, 0x00000000u, 1},
  {16384u, 8u, 0x00000001u, 1},
  {16384u, 8u, 0x00000100u, 1},
  {16384u, 8u, 0x7fffffffu, 1},
  {16384u, 8u, 0x80000000u, 1},
  {16384u, 8u, 0xffffffffu, 1},
  {16384u, 8u, 0x12345678u, 1},
  {16384u, 8u, 0xabcdef01u, 1},
  {16384u, 8u, 0x55aa55aau, 1},
  {16384u, 8u, 0x0f0f0f0fu, 1},
  {16384u, 16u, 0x00000000u, 1},
  {16384u, 16u, 0x00000001u, 1},
  {16384u, 16u, 0x00000100u, 1},
  {16384u, 16u, 0x7fffffffu, 1},
  {16384u, 16u, 0x80000000u, 1},
  {16384u, 16u, 0xffffffffu, 1},
  {16384u, 16u, 0x12345678u, 1},
  {16384u, 16u, 0xabcdef01u, 1},
  {16384u, 16u, 0x55aa55aau, 1},
  {16384u, 16u, 0x0f0f0f0fu, 1},
  {16384u, 24u, 0x00000000u, 1},
  {16384u, 24u, 0x00000001u, 1},
  {16384u, 24u, 0x00000100u, 1},
  {16384u, 24u, 0x7fffffffu, 1},
  {16384u, 24u, 0x80000000u, 1},
  {16384u, 24u, 0xffffffffu, 1},
  {16384u, 24u, 0x12345678u, 1},
  {16384u, 24u, 0xabcdef01u, 1},
  {16384u, 24u, 0x55aa55aau, 1},
  {16384u, 24u, 0x0f0f0f0fu, 1},
  {16384u, 32u, 0x00000000u, 1},
  {16384u, 32u, 0x00000001u, 1},
  {16384u, 32u, 0x00000100u, 1},
  {16384u, 32u, 0x7fffffffu, 1},
  {16384u, 32u, 0x80000000u, 1},
  {16384u, 32u, 0xffffffffu, 1},
  {16384u, 32u, 0x12345678u, 1},
  {16384u, 32u, 0xabcdef01u, 1},
  {16384u, 32u, 0x55aa55aau, 1},
  {16384u, 32u, 0x0f0f0f0fu, 1},
  {16384u, 64u, 0x00000000u, 1},
  {16384u, 64u, 0x00000001u, 1},
  {16384u, 64u, 0x00000100u, 1},
  {16384u, 64u, 0x7fffffffu, 1},
  {16384u, 64u, 0x80000000u, 1},
  {16384u, 64u, 0xffffffffu, 1},
  {16384u, 64u, 0x12345678u, 1},
  {16384u, 64u, 0xabcdef01u, 1},
  {16384u, 64u, 0x55aa55aau, 1},
  {16384u, 64u, 0x0f0f0f0fu, 1},
  {16384u, 100u, 0x00000000u, 1},
  {16384u, 100u, 0x00000001u, 1},
  {16384u, 100u, 0x00000100u, 1},
  {16384u, 100u, 0x7fffffffu, 1},
  {16384u, 100u, 0x80000000u, 1},
  {16384u, 100u, 0xffffffffu, 1},
  {16384u, 100u, 0x12345678u, 1},
  {16384u, 100u, 0xabcdef01u, 1},
  {16384u, 100u, 0x55aa55aau, 1},
  {16384u, 100u, 0x0f0f0f0fu, 1},
  {16384u, 128u, 0x00000000u, 1},
  {16384u, 128u, 0x00000001u, 1},
  {16384u, 128u, 0x00000100u, 1},
  {16384u, 128u, 0x7fffffffu, 1},
  {16384u, 128u, 0x80000000u, 1},
  {16384u, 128u, 0xffffffffu, 1},
  {16384u, 128u, 0x12345678u, 1},
  {16384u, 128u, 0xabcdef01u, 1},
  {16384u, 128u, 0x55aa55aau, 1},
  {16384u, 128u, 0x0f0f0f0fu, 1},
  {16384u, 256u, 0x00000000u, 1},
  {16384u, 256u, 0x00000001u, 1},
  {16384u, 256u, 0x00000100u, 1},
  {16384u, 256u, 0x7fffffffu, 1},
  {16384u, 256u, 0x80000000u, 1},
  {16384u, 256u, 0xffffffffu, 1},
  {16384u, 256u, 0x12345678u, 1},
  {16384u, 256u, 0xabcdef01u, 1},
  {16384u, 256u, 0x55aa55aau, 1},
  {16384u, 256u, 0x0f0f0f0fu, 1},
  {16384u, 400u, 0x00000000u, 1},
  {16384u, 400u, 0x00000001u, 1},
  {16384u, 400u, 0x00000100u, 1},
  {16384u, 400u, 0x7fffffffu, 1},
  {16384u, 400u, 0x80000000u, 1},
  {16384u, 400u, 0xffffffffu, 1},
  {16384u, 400u, 0x12345678u, 1},
  {16384u, 400u, 0xabcdef01u, 1},
  {16384u, 400u, 0x55aa55aau, 1},
  {16384u, 400u, 0x0f0f0f0fu, 1},
  {16384u, 512u, 0x00000000u, 1},
  {16384u, 512u, 0x00000001u, 1},
  {16384u, 512u, 0x00000100u, 1},
  {16384u, 512u, 0x7fffffffu, 1},
  {16384u, 512u, 0x80000000u, 1},
  {16384u, 512u, 0xffffffffu, 1},
  {16384u, 512u, 0x12345678u, 1},
  {16384u, 512u, 0xabcdef01u, 1},
  {16384u, 512u, 0x55aa55aau, 1},
  {16384u, 512u, 0x0f0f0f0fu, 1},
};

/* Chunker roots, N intkey rows. */
static const struct { int n; const char *zRoot; } aRoots[] = {
  {0, "0000000000000000000000000000000000000000"},
  {1, "98f292686c25a82ec24688a20fc9b9a7b63483f8"},
  {2, "799d4a707e8e69102252ce978f539f8484f29b2b"},
  {10, "82690a7d1a73a1d699ab8b6c253eccf95a4fa1b2"},
  {50, "9caeb31eaa26dcf7db5dda7644a7e64aa9b9e934"},
  {100, "c63aa536ce523cbd01f0019df5765d27e3fd4482"},
  {200, "270633c83111cc1734581ebf2e8015fc740fe8b4"},
  {500, "ec910c2cb4ed3e437c64742e8d77dad0ee9ebcb1"},
  {1000, "1473220361cee2c16f077ec4a7976ba5c50f719a"},
  {2000, "2322be9bc59995147ae8c69e62155cf7dca65f2e"},
  {5000, "8710988fa386978595f3c9faa9d8324866957acf"},
};

static void test_xxh32_golden(void){
  int i;
  char zName[64];
  for(i=0; i<(int)(sizeof(aXxh)/sizeof(aXxh[0])); i++){
    u8 key[8];
    u32 got;
    encodeIntKey(aXxh[i].key, key);
    got = prollyXXH32(key, 8, 0);
    sqlite3_snprintf(sizeof(zName), zName, "xxh32 key=%d", aXxh[i].key);
    check(zName, got==aXxh[i].h);
  }
}

static void test_weibull_golden(void){
  int i;
  char zName[96];
  for(i=0; i<(int)(sizeof(aWeibull)/sizeof(aWeibull[0])); i++){
    int got = prollyWeibullCheck(
      aWeibull[i].size, aWeibull[i].thisSize, aWeibull[i].hash
    );
    sqlite3_snprintf(sizeof(zName), zName, "weibull %u/%u/0x%x",
                     aWeibull[i].size, aWeibull[i].thisSize, aWeibull[i].hash);
    check(zName, got==aWeibull[i].want);
  }
}

static void test_chunker_root_golden(void){
  ChunkStore cs;
  int i, rc;
  char zName[64];

  memset(&cs, 0, sizeof(cs));
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:",
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                      | SQLITE_OPEN_MAIN_DB);
  check("chunker: open memory store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  for(i=0; i<(int)(sizeof(aRoots)/sizeof(aRoots[0])); i++){
    ProllyChunker ch;
    ProllyHash root, want;
    int k, n = aRoots[i].n;
    int ok;

    if( !parseHexHash(aRoots[i].zRoot, &want) ){
      check("chunker: parse golden root", 0);
      continue;
    }
    prollyChunkerInit(&ch, &cs, PROLLY_NODE_INTKEY | PROLLY_NODE_SUBTREE_COUNTS);
    ok = 1;
    for(k=0; k<n; k++){
      u8 key[8];
      u8 val[4] = {1,2,3,4};
      encodeIntKey(k, key);
      if( prollyChunkerAdd(&ch, key, 8, val, 4)!=SQLITE_OK ){
        ok = 0;
        break;
      }
    }
    if( ok && prollyChunkerFinish(&ch)!=SQLITE_OK ) ok = 0;
    prollyChunkerGetRoot(&ch, &root);
    sqlite3_snprintf(sizeof(zName), zName, "chunker root n=%d", n);
    check(zName, ok && memcmp(root.data, want.data, PROLLY_HASH_SIZE)==0);
    prollyChunkerFree(&ch);
    chunkStoreRollback(&cs);
  }
  chunkStoreClose(&cs);
}

static void test_chunker_repeatable(void){
  ChunkStore cs;
  ProllyHash a, b;
  ProllyChunker ch;
  int k, rc;

  memset(&cs, 0, sizeof(cs));
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:",
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                      | SQLITE_OPEN_MAIN_DB);
  check("repeatable: open", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;

  prollyChunkerInit(&ch, &cs, PROLLY_NODE_INTKEY | PROLLY_NODE_SUBTREE_COUNTS);
  for(k=0; k<777; k++){
    u8 key[8];
    u8 val[8];
    encodeIntKey(k, key);
    encodeIntKey(k*3+1, val);
    if( prollyChunkerAdd(&ch, key, 8, val, 8)!=SQLITE_OK ){
      check("repeatable: add1", 0);
      prollyChunkerFree(&ch);
      chunkStoreClose(&cs);
      return;
    }
  }
  check("repeatable: finish1", prollyChunkerFinish(&ch)==SQLITE_OK);
  prollyChunkerGetRoot(&ch, &a);
  prollyChunkerFree(&ch);
  chunkStoreRollback(&cs);

  prollyChunkerInit(&ch, &cs, PROLLY_NODE_INTKEY | PROLLY_NODE_SUBTREE_COUNTS);
  for(k=0; k<777; k++){
    u8 key[8];
    u8 val[8];
    encodeIntKey(k, key);
    encodeIntKey(k*3+1, val);
    if( prollyChunkerAdd(&ch, key, 8, val, 8)!=SQLITE_OK ){
      check("repeatable: add2", 0);
      prollyChunkerFree(&ch);
      chunkStoreClose(&cs);
      return;
    }
  }
  check("repeatable: finish2", prollyChunkerFinish(&ch)==SQLITE_OK);
  prollyChunkerGetRoot(&ch, &b);
  prollyChunkerFree(&ch);
  chunkStoreClose(&cs);

  check("repeatable: same root twice",
        memcmp(a.data, b.data, PROLLY_HASH_SIZE)==0);
}

static int storeBuiltNode(ChunkStore *cs, ProllyNodeBuilder *b, ProllyHash *hash){
  u8 *pData = 0;
  int nData = 0;
  int rc = prollyNodeBuilderFinish(b, &pData, &nData);
  if( rc==SQLITE_OK ) rc = chunkStorePut(cs, pData, nData, hash);
  sqlite3_free(pData);
  prollyNodeBuilderFree(b);
  return rc;
}

static void test_canonical_root_chain(void){
  int useCache, internal;
  for(useCache=0; useCache<2; useCache++){
    for(internal=0; internal<2; internal++){
      ChunkStore cs;
      ProllyCache cache;
      ProllyChunker ch;
      ProllyCursor cur;
      ProllyNodeBuilder b;
      ProllyHash leaves[2], want, wrapped, root;
      u8 key[8];
      u8 flags = PROLLY_NODE_INTKEY | PROLLY_NODE_SUBTREE_COUNTS;
      int rc, i, level, res = 1, nRows = 0;

      memset(&cs, 0, sizeof(cs));
      rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:",
                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                         | SQLITE_OPEN_MAIN_DB);
      check("root chain: open", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ) continue;
      rc = prollyCacheInit(&cache, 1024*1024);
      check("root chain: cache init", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&cs);
        continue;
      }
      for(i=0; i<2 && rc==SQLITE_OK; i++){
        prollyNodeBuilderInit(&b, 0, flags);
        encodeIntKey(i+1, key);
        rc = prollyNodeBuilderAdd(&b, key, 8, key, 8);
        if( rc==SQLITE_OK ) rc = storeBuiltNode(&cs, &b, &leaves[i]);
        else prollyNodeBuilderFree(&b);
      }
      if( rc==SQLITE_OK ){
        want = leaves[0];
        if( internal ){
          prollyNodeBuilderInit(&b, 1, flags);
          for(i=0; i<2 && rc==SQLITE_OK; i++){
            encodeIntKey(i+1, key);
            rc = prollyNodeBuilderAddWithCount(&b, key, 8,
                                              leaves[i].data, PROLLY_HASH_SIZE, 1);
          }
          if( rc==SQLITE_OK ) rc = storeBuiltNode(&cs, &b, &want);
          else prollyNodeBuilderFree(&b);
        }
        wrapped = want;
        encodeIntKey(internal+1, key);
        for(level=internal+1; level<=internal+3 && rc==SQLITE_OK; level++){
          prollyNodeBuilderInit(&b, (u8)level, flags);
          rc = prollyNodeBuilderAddWithCount(&b, key, 8, wrapped.data,
                                            PROLLY_HASH_SIZE, internal+1);
          if( rc==SQLITE_OK ) rc = storeBuiltNode(&cs, &b, &wrapped);
          else prollyNodeBuilderFree(&b);
        }
      }
      check("root chain: build legacy wrappers", rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        prollyCursorInit(&cur, &cs, &cache, &wrapped, flags);
        rc = prollyCursorFirst(&cur, &res);
        while( rc==SQLITE_OK && prollyCursorIsValid(&cur) ){
          nRows++;
          rc = prollyCursorNext(&cur);
        }
        prollyCursorClose(&cur);
        check("root chain: legacy root remains readable",
              rc==SQLITE_OK && res==0 && nRows==internal+1);
        prollyChunkerInitWithCache(&ch, &cs, useCache ? &cache : 0, flags);
        rc = prollyChunkerAddAtLevelWithCount(&ch, internal+4, key, 8,
                                             wrapped.data, PROLLY_HASH_SIZE,
                                             internal+1);
        if( rc==SQLITE_OK ) rc = prollyChunkerFinish(&ch);
        prollyChunkerGetRoot(&ch, &root);
        check("root chain: stop at leaf or branching node",
              rc==SQLITE_OK && memcmp(root.data, want.data, PROLLY_HASH_SIZE)==0);
        prollyChunkerFree(&ch);
      }
      prollyCacheFree(&cache);
      chunkStoreClose(&cs);
    }
  }
}

static void test_sparse_canonical_root(void){
  int useCache;
  for(useCache=0; useCache<2; useCache++){
    ChunkStore cs;
    ProllyCache cache;
    ProllyChunker ch;
    ProllyHash want, root;
    ProllyNode node;
    u8 key[8], *pData = 0;
    char zPath[256];
    sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
    int rc, nData = 0, nDataPhys = 0;
    sqlite3_int64 oldLimit;

    memset(&cs, 0, sizeof(cs));
    sqlite3_snprintf(sizeof(zPath), zPath,
                     DOLTLITE_TEST_TMPDIR "/chunker-sparse-%d.db", useCache);
    pVfs->xDelete(pVfs, zPath, 0);
    rc = chunkStoreOpen(&cs, pVfs, zPath,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                       | SQLITE_OPEN_MAIN_DB);
    check("sparse root: open", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ) continue;
    rc = prollyCacheInit(&cache, 1024*1024);
    check("sparse root: cache init", rc==SQLITE_OK);
    if( rc!=SQLITE_OK ){
      chunkStoreClose(&cs);
      continue;
    }
    prollyChunkerInitWithCache(&ch, &cs, useCache ? &cache : 0,
                              PROLLY_NODE_INTKEY | PROLLY_NODE_SUBTREE_COUNTS);
    encodeIntKey(1, key);
    rc = prollyChunkerAdd(&ch, key, 8, (const u8*)"x", 1);
    if( rc==SQLITE_OK ){
      encodeIntKey(2, key);
      rc = prollyChunkerAddZeroTail(&ch, key, 8, 0, 0, 64*1024*1024);
    }
    check("sparse root: add", rc==SQLITE_OK);
    check("sparse root: last row flushed leaf",
          rc==SQLITE_OK && ch.nLevels==2
          && ch.aLevel[0].builder.nItems==0
          && ch.aLevel[1].builder.nItems==1);
    if( rc==SQLITE_OK && ch.nLevels==2
        && ch.aLevel[1].builder.nItems==1 ){
      memcpy(want.data, ch.aLevel[1].builder.pValBuf, PROLLY_HASH_SIZE);
      oldLimit = sqlite3_hard_heap_limit64(sqlite3_memory_used() + 1024*1024);
      rc = prollyChunkerFinish(&ch);
      sqlite3_hard_heap_limit64(oldLimit);
      check("sparse root: finish within memory limit", rc==SQLITE_OK);
      if( rc==SQLITE_OK ){
        prollyChunkerGetRoot(&ch, &root);
        check("sparse root: collapsed to leaf",
              memcmp(root.data, want.data, PROLLY_HASH_SIZE)==0);
        rc = chunkStoreGetSparse(&cs, &root, &pData, &nData, &nDataPhys);
        if( rc==SQLITE_OK ){
          rc = prollyNodeParseSparse(&node, pData, nData, nDataPhys);
        }
        check("sparse root: leaf remains sparse",
              rc==SQLITE_OK && node.level==0 && node.nItems==2
              && nData>64*1024*1024 && nDataPhys<1024);
        sqlite3_free(pData);
      }
    }
    prollyChunkerFree(&ch);
    prollyCacheFree(&cache);
    chunkStoreClose(&cs);
    pVfs->xDelete(pVfs, zPath, 0);
  }
}

static void test_append_history(void){
  int largeCache, intKey;
  for(largeCache=0; largeCache<2; largeCache++){
    for(intKey=0; intKey<2; intKey++){
      ChunkStore cs;
      ProllyCache cache;
      ProllyHash root = {{0}};
      u8 key[2048], val[1004];
      u8 flags = PROLLY_NODE_SUBTREE_COUNTS;
      int nKey = intKey ? 8 : (int)sizeof(key);
      int n, rc;
      flags |= intKey ? PROLLY_NODE_INTKEY : PROLLY_NODE_BLOBKEY;
      memset(&cs, 0, sizeof(cs));
      memset(key, 'k', sizeof(key));
      memset(val, 'x', sizeof(val));
      val[0] = 4;
      val[1] = 0;
      val[2] = 0x8f;
      val[3] = 0x5d;
      rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:",
                          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                          | SQLITE_OPEN_MAIN_DB);
      check("append history: open", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ) continue;
      rc = prollyCacheInit(&cache, largeCache ? 1024*1024 : 4096);
      check("append history: cache", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&cs);
        continue;
      }
      for(n=1; n<=600; n++){
        ProllyChunker ch;
        ProllyHash want;
        char zTest[100];
        int k;
        encodeIntKey(n, key+nKey-8);
        key[nKey-8] ^= 0x80;
        rc = prollyMutateInsert(&cs, &cache, &root, flags,
                                key, nKey, n, val, sizeof(val), &root);
        prollyChunkerInit(&ch, &cs, flags);
        for(k=1; rc==SQLITE_OK && k<=n; k++){
          encodeIntKey(k, key+nKey-8);
          key[nKey-8] ^= 0x80;
          rc = prollyChunkerAdd(&ch, key, nKey, val, sizeof(val));
        }
        if( rc==SQLITE_OK ) rc = prollyChunkerFinish(&ch);
        prollyChunkerGetRoot(&ch, &want);
        prollyChunkerFree(&ch);
        sqlite3_snprintf(sizeof(zTest), zTest,
                         "append history: large-cache=%d intkey=%d prefix=%d",
                         largeCache, intKey, n);
        check(zTest, rc==SQLITE_OK && prollyHashCompare(&root, &want)==0);
        if( rc!=SQLITE_OK ) break;
      }
      prollyCacheFree(&cache);
      chunkStoreClose(&cs);
    }
  }
}

static void test_mixed_merge_history(void){
  int intKey, earlyEdits;
  for(intKey=0; intKey<2; intKey++){
    for(earlyEdits=0; earlyEdits<2; earlyEdits++){
      ChunkStore cs;
      ProllyCache cache;
      ProllyChunker ch;
      ProllyMutMap edits;
      ProllyMutator mut;
      ProllyHash root = {{0}}, want = {{0}};
      u8 key[17], val[96];
      u8 flags = PROLLY_NODE_SUBTREE_COUNTS
               | (intKey ? PROLLY_NODE_INTKEY : PROLLY_NODE_BLOBKEY);
      int nKey = intKey ? 8 : (int)sizeof(key);
      int phase, i, rc;
      char zTest[100];

      memset(&cs, 0, sizeof(cs));
      rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), ":memory:",
                          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                          | SQLITE_OPEN_MAIN_DB);
      check("mixed merge: open", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ) continue;
      rc = prollyCacheInit(&cache, earlyEdits ? 4096 : 1024*1024);
      check("mixed merge: cache", rc==SQLITE_OK);
      if( rc!=SQLITE_OK ){
        chunkStoreClose(&cs);
        continue;
      }
      rc = prollyMutMapInit(&edits, (u8)intKey);
      for(phase=0; phase<3 && rc==SQLITE_OK; phase++){
        prollyChunkerInit(&ch, &cs, flags);
        for(i=0; i<=4099 && rc==SQLITE_OK; i++){
          int k = phase==1 ? 4099-i : i;
          int present = k>0 && k<=4096 && k%2==0;
          int inRange = !earlyEdits || k<2048;
          int deleted = inRange && k%10==0;
          int inserted = inRange && !present && (k%5==1 || k==4099);
          int changed = inRange && present && k%7==0;
          int nVal = changed && phase ? 17+k%64 : 32;
          memset(key, 'k', sizeof(key));
          encodeIntKey(k, key+nKey-8);
          key[nKey-8] ^= 0x80;
          memset(val, changed && phase ? 'u' : 'v', sizeof(val));
          if( phase==1 ){
            if( deleted ){
              rc = prollyMutMapDelete(&edits, key, nKey, k);
            }else if( inserted || changed ){
              rc = prollyMutMapInsert(&edits, key, nKey, k, val, nVal);
            }
          }else if( phase==0 ? present : (!deleted && (present || inserted)) ){
            rc = prollyChunkerAdd(&ch, key, nKey, val, nVal);
          }
        }
        if( phase!=1 && rc==SQLITE_OK ) rc = prollyChunkerFinish(&ch);
        if( phase==0 ) prollyChunkerGetRoot(&ch, &root);
        if( phase==2 ) prollyChunkerGetRoot(&ch, &want);
        prollyChunkerFree(&ch);
      }
      memset(&mut, 0, sizeof(mut));
      mut.pStore = &cs;
      mut.pCache = &cache;
      mut.oldRoot = root;
      mut.pEdits = &edits;
      mut.flags = flags;
      if( rc==SQLITE_OK ) rc = prollyMutateFlush(&mut);
      sqlite3_snprintf(sizeof(zTest), zTest,
                      "mixed merge: intkey=%d early-edits=%d", intKey, earlyEdits);
      check(zTest, rc==SQLITE_OK && prollyHashCompare(&mut.newRoot, &want)==0);
      prollyMutMapFree(&edits);
      prollyCacheFree(&cache);
      chunkStoreClose(&cs);
    }
  }
}

static void test_weibull_large_sizes(void){
  static const struct {
    const char *zName;
    i64 size;
    i64 thisSize;
    u32 hash;
    int want;
  } aCases[] = {
    { "weibull_above_max",           PROLLY_CHUNK_MAX + 1,           8,                       0xffffffffu, 1 },
    { "weibull_over_int32_max",      (i64)0x7fffffffLL + 1,          (i64)0x7fffffffLL + 1,   0xffffffffu, 1 },
    { "weibull_over_int32_max_2",    (i64)0x7fffffffLL + 100000,     100000,                  0x80000000u, 1 },
    { "weibull_over_uint32_max",     ((i64)1 << 32) + 512,           512,                     0xffffffffu, 1 },
    { "weibull_huge_i64",            (i64)1 << 40,                   (i64)1 << 39,            0xffffffffu, 1 },
    { "weibull_int64_max",           (i64)0x7fffffffffffffffLL,      (i64)0x7fffffffffffffffLL, 0xffffffffu, 1 },
    { "weibull_size_zero",           0,                              0,                       0x00000000u, 0 },
    { "weibull_negative_size",       -1,                             1,                       0x00000000u, 0 },
    { "weibull_negative_this_size",  1024,                           -8,                      0x00000000u, 0 },
    { "weibull_this_gt_size",        512,                            1024,                    0x00000000u, 1 },
  };
  int i;
  for(i=0; i<(int)(sizeof(aCases)/sizeof(aCases[0])); i++){
    int got = prollyWeibullCheck(aCases[i].size, aCases[i].thisSize, aCases[i].hash);
    check(aCases[i].zName, got==aCases[i].want);
  }
  check("entry_bytes_no_i32_overflow",
        PROLLY_NODE_ENTRY_BYTES(0, 8, 0x7fffffff) == (i64)0x7fffffffLL + 16);
  check("chunker_level_nbytes_64bit",
        sizeof(((ProllyChunkerLevel*)0)->nBytes) == sizeof(i64));
}

#if !defined(_WIN32)
typedef struct WeibullThreadGate WeibullThreadGate;
struct WeibullThreadGate {
  pthread_mutex_t mu;
  pthread_cond_t cv;
  int nReady;
  int nTotal;
  int go;
};

typedef struct WeibullThreadArg WeibullThreadArg;
struct WeibullThreadArg {
  WeibullThreadGate *pGate;
  int id;
  int nMismatches;
};

static void *weibullConcurrentWorker(void *pArg){
  WeibullThreadArg *p = (WeibullThreadArg*)pArg;
  WeibullThreadGate *pGate = p->pGate;
  int nVec = (int)(sizeof(aWeibull)/sizeof(aWeibull[0]));
  int round, k;

  pthread_mutex_lock(&pGate->mu);
  pGate->nReady++;
  if( pGate->nReady==pGate->nTotal ){
    pthread_cond_broadcast(&pGate->cv);
  }
  while( !pGate->go ){
    pthread_cond_wait(&pGate->cv, &pGate->mu);
  }
  pthread_mutex_unlock(&pGate->mu);

  for(round=0; round<4; round++){
    for(k=0; k<nVec; k++){
      int idx = (k + p->id * 97) % nVec;
      int got = prollyWeibullCheck(
        aWeibull[idx].size, aWeibull[idx].thisSize, aWeibull[idx].hash
      );
      if( got!=aWeibull[idx].want ){
        p->nMismatches++;
      }
    }
  }
  return 0;
}

static void test_weibull_concurrent_init(void){
  enum { N_THREADS = 8 };
  pthread_t aThread[N_THREADS];
  WeibullThreadArg aArg[N_THREADS];
  WeibullThreadGate gate;
  int i, nCreated = 0, nMismatches = 0;

  memset(&gate, 0, sizeof(gate));
  pthread_mutex_init(&gate.mu, 0);
  pthread_cond_init(&gate.cv, 0);
  gate.nTotal = N_THREADS;

  for(i=0; i<N_THREADS; i++){
    aArg[i].pGate = &gate;
    aArg[i].id = i;
    aArg[i].nMismatches = 0;
    if( pthread_create(&aThread[i], 0, weibullConcurrentWorker, &aArg[i])==0 ){
      nCreated++;
    }
  }

  pthread_mutex_lock(&gate.mu);
  gate.nTotal = nCreated;
  while( gate.nReady < nCreated ){
    pthread_cond_wait(&gate.cv, &gate.mu);
  }
  gate.go = 1;
  pthread_cond_broadcast(&gate.cv);
  pthread_mutex_unlock(&gate.mu);

  for(i=0; i<nCreated; i++){
    pthread_join(aThread[i], 0);
    nMismatches += aArg[i].nMismatches;
  }

  pthread_cond_destroy(&gate.cv);
  pthread_mutex_destroy(&gate.mu);

  check("weibull_concurrent_init_threads", nCreated==N_THREADS);
  check("weibull_concurrent_init_deterministic", nMismatches==0);
}
#endif

static int putRawNode(
  ChunkStore *pStore,
  u8 level,
  u8 flags,
  int nItems,
  const i64 *aIntKey,
  const u8 *const *apKey,
  const int *anKey,
  const u8 *const *apVal,
  const int *anVal,
  const u64 *aCount,
  ProllyHash *pOut
){
  ProllyNodeBuilder b;
  u8 *pData = 0;
  int nData = 0;
  int i, rc = SQLITE_OK;

  prollyNodeBuilderInit(&b, level, flags);
  for(i=0; i<nItems && rc==SQLITE_OK; i++){
    u8 kbuf[8];
    const u8 *pK;
    int nK;
    if( flags & PROLLY_NODE_INTKEY ){
      prollyEncodeIntKey(aIntKey[i], kbuf);
      pK = kbuf;
      nK = 8;
    }else{
      pK = apKey[i];
      nK = anKey[i];
    }
    if( level>0 && aCount ){
      rc = prollyNodeBuilderAddWithCount(&b, pK, nK, apVal[i], anVal[i], aCount[i]);
    }else{
      rc = prollyNodeBuilderAdd(&b, pK, nK, apVal[i], anVal[i]);
    }
  }
  if( rc==SQLITE_OK ){
    rc = prollyNodeBuilderFinish(&b, &pData, &nData);
  }
  if( rc==SQLITE_OK ){
    rc = chunkStorePut(pStore, pData, nData, pOut);
  }
  sqlite3_free(pData);
  prollyNodeBuilderFree(&b);
  return rc;
}

static int runMutateEdits(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  ProllyMutMap *pEdits
){
  ProllyMutator mut;
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  mut.oldRoot = *pRoot;
  mut.pEdits = pEdits;
  mut.flags = flags;
  return prollyMutateFlush(&mut);
}

static void test_mutate_corrupt_validation(void){
  ChunkStore cs;
  ProllyCache cache;
  ProllyMutMap delEdits, batchEdits;
  ProllyHash validLeaf, blobLeaf, deepRoot, badSkipRoot, badSameLvlRoot;
  ProllyHash badFlagChildRoot, batchBadLvlRoot, batchBadFlagRoot;
  i64 aKeys16[16];
  u8 aVals16[16][4];
  const u8 *apVals16[16];
  int anVals16[16];
  u8 blobKey[1] = {'x'};
  const u8 *pBlobKey = blobKey;
  int nBlobKey = 1;
  u8 val4[4] = {'a', 'b', 'c', 'd'};
  const u8 *pVal4 = val4;
  int nVal4 = 4;
  i64 k1 = 1;
  int i, rc;
  const char *path = DOLTLITE_TEST_TMPDIR "/test_prolly_mutate_corrupt";

  remove(path);
  rc = chunkStoreOpen(&cs, sqlite3_vfs_find(0), path,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
  check("mutate_corrupt: open store", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return;
  rc = prollyCacheInit(&cache, 4 * 1024 * 1024);
  check("mutate_corrupt: init cache", rc==SQLITE_OK);

  for(i=0; i<16; i++){
    aKeys16[i] = i + 1;
    memset(aVals16[i], 'v', 4);
    apVals16[i] = aVals16[i];
    anVals16[i] = 4;
  }
  rc = putRawNode(&cs, 0, PROLLY_NODE_INTKEY, 16, aKeys16, 0, 0,
                  apVals16, anVals16, 0, &validLeaf);
  check("mutate_corrupt: validLeaf", rc==SQLITE_OK);

  rc = putRawNode(&cs, 0, PROLLY_NODE_BLOBKEY, 1, 0, &pBlobKey, &nBlobKey,
                  &pVal4, &nVal4, 0, &blobLeaf);
  check("mutate_corrupt: blobLeaf", rc==SQLITE_OK);

  /* Two deletes bypass single/pair/batch-insert fast paths into streamingMerge. */
  rc = prollyMutMapInit(&delEdits, 1);
  check("mutate_corrupt: init delEdits", rc==SQLITE_OK);
  rc = prollyMutMapDelete(&delEdits, 0, 0, 1);
  check("mutate_corrupt: delEdits 1", rc==SQLITE_OK);
  rc = prollyMutMapDelete(&delEdits, 0, 0, 2);
  check("mutate_corrupt: delEdits 2", rc==SQLITE_OK);

  /* 1. streamingMerge: root level >= PROLLY_CURSOR_MAX_DEPTH */
  {
    const u8 *pChild = validLeaf.data;
    int nChild = PROLLY_HASH_SIZE;
    u64 cnt = 16;
    rc = putRawNode(&cs, PROLLY_CURSOR_MAX_DEPTH, PROLLY_NODE_INTKEY, 1,
                    &k1, 0, 0, &pChild, &nChild, &cnt, &deepRoot);
    check("mutate_corrupt: build deepRoot", rc==SQLITE_OK);
    rc = runMutateEdits(&cs, &cache, &deepRoot, PROLLY_NODE_INTKEY, &delEdits);
    check("mutate_corrupt: streaming deep root returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
  }

  /* 2. streamingMerge: root key flags mismatch */
  rc = runMutateEdits(&cs, &cache, &blobLeaf, PROLLY_NODE_INTKEY, &delEdits);
  check("mutate_corrupt: streaming root key flags mismatch returns SQLITE_CORRUPT",
        rc==SQLITE_CORRUPT);

  /* 3. streamingMergeNode: skipped child level (level 2 -> level 0) */
  {
    const u8 *pChild = validLeaf.data;
    int nChild = PROLLY_HASH_SIZE;
    u64 cnt = 16;
    i64 bound = 16;
    rc = putRawNode(&cs, 2, PROLLY_NODE_INTKEY, 1,
                    &bound, 0, 0, &pChild, &nChild, &cnt, &badSkipRoot);
    check("mutate_corrupt: build badSkipRoot", rc==SQLITE_OK);
    rc = runMutateEdits(&cs, &cache, &badSkipRoot, PROLLY_NODE_INTKEY, &delEdits);
    check("mutate_corrupt: streaming skipped child level returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
  }

  /* 4. streamingMergeNode: same-level / non-decreasing child (level 1 -> level 1) */
  {
    ProllyHash lvl1Child;
    const u8 *pChild = validLeaf.data;
    int nChild = PROLLY_HASH_SIZE;
    u64 cnt = 16;
    i64 bound = 16;
    rc = putRawNode(&cs, 1, PROLLY_NODE_INTKEY, 1,
                    &bound, 0, 0, &pChild, &nChild, &cnt, &lvl1Child);
    check("mutate_corrupt: build lvl1Child", rc==SQLITE_OK);
    pChild = lvl1Child.data;
    rc = putRawNode(&cs, 1, PROLLY_NODE_INTKEY, 1,
                    &bound, 0, 0, &pChild, &nChild, &cnt, &badSameLvlRoot);
    check("mutate_corrupt: build badSameLvlRoot", rc==SQLITE_OK);
    rc = runMutateEdits(&cs, &cache, &badSameLvlRoot, PROLLY_NODE_INTKEY, &delEdits);
    check("mutate_corrupt: streaming same-level child returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
  }

  /* 5. streamingMergeNode: child key flags mismatch (INTKEY parent -> BLOBKEY leaf) */
  {
    const u8 *pChild = blobLeaf.data;
    int nChild = PROLLY_HASH_SIZE;
    u64 cnt = 1;
    i64 bound = 16;
    rc = putRawNode(&cs, 1, PROLLY_NODE_INTKEY, 1,
                    &bound, 0, 0, &pChild, &nChild, &cnt, &badFlagChildRoot);
    check("mutate_corrupt: build badFlagChildRoot", rc==SQLITE_OK);
    rc = runMutateEdits(&cs, &cache, &badFlagChildRoot, PROLLY_NODE_INTKEY, &delEdits);
    check("mutate_corrupt: streaming child flags mismatch returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
  }

  /* 17 same-size replacements: batchReplacementProbe checks indices 0..15
  ** (all in child 0 = validLeaf), then replaceBatchNodeNoRechunk descends
  ** into child 1 (corrupt). */
  rc = prollyMutMapInit(&batchEdits, 1);
  check("mutate_corrupt: init batchEdits", rc==SQLITE_OK);
  for(i=0; i<17 && rc==SQLITE_OK; i++){
    u8 newVal[4] = {'w', 'x', 'y', 'z'};
    rc = prollyMutMapInsert(&batchEdits, 0, 0, i + 1, newVal, 4);
  }
  check("mutate_corrupt: fill batchEdits", rc==SQLITE_OK);

  /* 6. replaceBatchNodeNoRechunk: child wrong level (miss + cache hit) */
  {
    ProllyHash lvl1Child;
    const u8 *pChild = validLeaf.data;
    int nChild = PROLLY_HASH_SIZE;
    u64 cnt1 = 16;
    i64 b1 = 16;
    i64 aBounds[2] = {16, 17};
    const u8 *apChildren[2];
    int anChildren[2] = {PROLLY_HASH_SIZE, PROLLY_HASH_SIZE};
    u64 aCounts[2] = {16, 1};

    rc = putRawNode(&cs, 1, PROLLY_NODE_INTKEY, 1,
                    &b1, 0, 0, &pChild, &nChild, &cnt1, &lvl1Child);
    check("mutate_corrupt: batch lvl1Child", rc==SQLITE_OK);
    apChildren[0] = validLeaf.data;
    apChildren[1] = lvl1Child.data;
    rc = putRawNode(&cs, 1, PROLLY_NODE_INTKEY, 2,
                    aBounds, 0, 0, apChildren, anChildren, aCounts,
                    &batchBadLvlRoot);
    check("mutate_corrupt: build batchBadLvlRoot", rc==SQLITE_OK);

    rc = runMutateEdits(&cs, &cache, &batchBadLvlRoot, PROLLY_NODE_INTKEY, &batchEdits);
    check("mutate_corrupt: batch child wrong level (cold) returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
    rc = runMutateEdits(&cs, &cache, &batchBadLvlRoot, PROLLY_NODE_INTKEY, &batchEdits);
    check("mutate_corrupt: batch child wrong level (cached) returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
  }

  /* 7. replaceBatchNodeNoRechunk: child key flags mismatch (miss + cache hit) */
  {
    i64 aBounds[2] = {16, 17};
    const u8 *apChildren[2] = {validLeaf.data, blobLeaf.data};
    int anChildren[2] = {PROLLY_HASH_SIZE, PROLLY_HASH_SIZE};
    u64 aCounts[2] = {16, 1};

    rc = putRawNode(&cs, 1, PROLLY_NODE_INTKEY, 2,
                    aBounds, 0, 0, apChildren, anChildren, aCounts,
                    &batchBadFlagRoot);
    check("mutate_corrupt: build batchBadFlagRoot", rc==SQLITE_OK);

    rc = runMutateEdits(&cs, &cache, &batchBadFlagRoot, PROLLY_NODE_INTKEY, &batchEdits);
    check("mutate_corrupt: batch child flags mismatch (cold) returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
    rc = runMutateEdits(&cs, &cache, &batchBadFlagRoot, PROLLY_NODE_INTKEY, &batchEdits);
    check("mutate_corrupt: batch child flags mismatch (cached) returns SQLITE_CORRUPT",
          rc==SQLITE_CORRUPT);
  }

  prollyMutMapFree(&batchEdits);
  prollyMutMapFree(&delEdits);
  prollyCacheFree(&cache);
  chunkStoreClose(&cs);
  remove(path);
}

int main(void){
  sqlite3_initialize();
  printf("Prolly chunker boundary golden tests\n");
  printf("====================================\n\n");

#if !defined(_WIN32)
  test_weibull_concurrent_init();
#endif
  test_xxh32_golden();
  test_weibull_golden();
  test_weibull_large_sizes();
  test_chunker_root_golden();
  test_mixed_merge_history();
  test_chunker_repeatable();
  test_canonical_root_chain();
  test_sparse_canonical_root();
  test_append_history();
  test_mutate_corrupt_validation();

  printf("\n%d passed, %d failed\n", nPass, nFail);
  return nFail>0 ? 1 : 0;
}
