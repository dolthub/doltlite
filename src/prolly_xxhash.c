
#ifdef DOLTLITE_PROLLY

#include "prolly_xxhash.h"

#define XXH_PRIME32_1  0x9E3779B1U
#define XXH_PRIME32_2  0x85EBCA77U
#define XXH_PRIME32_3  0xC2B2AE3DU
#define XXH_PRIME32_4  0x27D4EB2FU
#define XXH_PRIME32_5  0x165667B1U

static u32 xxh_read32_le(const u8 *p){
  return (u32)p[0]
       | ((u32)p[1] << 8)
       | ((u32)p[2] << 16)
       | ((u32)p[3] << 24);
}

static u32 xxh_rotl32(u32 x, int r){
  return (x << r) | (x >> (32 - r));
}

static u32 xxh_round(u32 acc, u32 input){
  acc += input * XXH_PRIME32_2;
  acc = xxh_rotl32(acc, 13);
  acc *= XXH_PRIME32_1;
  return acc;
}

u32 prollyXXH32(const u8 *p, int n, u32 seed){
  const u8 *end = p + n;
  u32 h32;

  if( n >= 16 ){
    const u8 *limit = end - 16;
    u32 v1 = seed + XXH_PRIME32_1 + XXH_PRIME32_2;
    u32 v2 = seed + XXH_PRIME32_2;
    u32 v3 = seed + 0;
    u32 v4 = seed - XXH_PRIME32_1;

    do {
      v1 = xxh_round(v1, xxh_read32_le(p)); p += 4;
      v2 = xxh_round(v2, xxh_read32_le(p)); p += 4;
      v3 = xxh_round(v3, xxh_read32_le(p)); p += 4;
      v4 = xxh_round(v4, xxh_read32_le(p)); p += 4;
    } while( p <= limit );

    h32 = xxh_rotl32(v1, 1)  + xxh_rotl32(v2, 7)
        + xxh_rotl32(v3, 12) + xxh_rotl32(v4, 18);
  } else {
    h32 = seed + XXH_PRIME32_5;
  }

  h32 += (u32)n;

  while( p + 4 <= end ){
    h32 += xxh_read32_le(p) * XXH_PRIME32_3;
    h32 = xxh_rotl32(h32, 17) * XXH_PRIME32_4;
    p += 4;
  }

  while( p < end ){
    h32 += (u32)(*p) * XXH_PRIME32_5;
    h32 = xxh_rotl32(h32, 11) * XXH_PRIME32_1;
    p++;
  }

  h32 ^= h32 >> 15;
  h32 *= XXH_PRIME32_2;
  h32 ^= h32 >> 13;
  h32 *= XXH_PRIME32_3;
  h32 ^= h32 >> 16;
  return h32;
}

#endif
