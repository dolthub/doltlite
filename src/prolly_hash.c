#ifdef DOLTLITE_PROLLY
/*
** Prolly tree hashing implementation.
** Content-addressed hashing (xxHash32-based) and Buzhash rolling hash splitter.
*/
#include "prolly_hash.h"
#include <string.h>

/*****************************************************************************
** xxHash32 implementation
*****************************************************************************/

#define PRIME32_1  0x9E3779B1U
#define PRIME32_2  0x85EBCA77U
#define PRIME32_3  0xC2B2AE3DU
#define PRIME32_4  0x27D4EB2FU
#define PRIME32_5  0x165667B1U

static u32 xxh32_rotl(u32 x, int r){
  return (x << r) | (x >> (32 - r));
}

static u32 xxh32_read32(const u8 *p){
  return (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24);
}

static u32 xxh32_round(u32 acc, u32 input){
  acc += input * PRIME32_2;
  acc = xxh32_rotl(acc, 13);
  acc *= PRIME32_1;
  return acc;
}

u32 prollyXXHash32(const void *pData, int nData, u32 seed){
  const u8 *p = (const u8 *)pData;
  const u8 *pEnd = p + nData;
  u32 h32;

  if( nData >= 16 ){
    const u8 *limit = pEnd - 16;
    u32 v1 = seed + PRIME32_1 + PRIME32_2;
    u32 v2 = seed + PRIME32_2;
    u32 v3 = seed + 0;
    u32 v4 = seed - PRIME32_1;
    do {
      v1 = xxh32_round(v1, xxh32_read32(p));      p += 4;
      v2 = xxh32_round(v2, xxh32_read32(p));      p += 4;
      v3 = xxh32_round(v3, xxh32_read32(p));      p += 4;
      v4 = xxh32_round(v4, xxh32_read32(p));      p += 4;
    } while( p <= limit );
    h32 = xxh32_rotl(v1, 1) + xxh32_rotl(v2, 7)
        + xxh32_rotl(v3, 12) + xxh32_rotl(v4, 18);
  }else{
    h32 = seed + PRIME32_5;
  }

  h32 += (u32)nData;

  while( p + 4 <= pEnd ){
    h32 += xxh32_read32(p) * PRIME32_3;
    h32 = xxh32_rotl(h32, 17) * PRIME32_4;
    p += 4;
  }

  while( p < pEnd ){
    h32 += (*p++) * PRIME32_5;
    h32 = xxh32_rotl(h32, 11) * PRIME32_1;
  }

  h32 ^= h32 >> 15;
  h32 *= PRIME32_2;
  h32 ^= h32 >> 13;
  h32 *= PRIME32_3;
  h32 ^= h32 >> 16;

  return h32;
}

/*****************************************************************************
** Content hash: compute a 20-byte hash using xxHash32 with multiple seeds
*****************************************************************************/

void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut){
  u32 h0 = prollyXXHash32(pData, nData, 0x00000000U);
  u32 h1 = prollyXXHash32(pData, nData, 0x9E3779B9U);
  u32 h2 = prollyXXHash32(pData, nData, 0x517CC1B7U);
  u32 h3 = prollyXXHash32(pData, nData, 0x6C62272EU);
  u32 h4 = prollyXXHash32(pData, nData, 0xDEADBEEFU);

  /* Pack 5 x 4-byte hashes into 20 bytes, little-endian */
  pOut->data[0]  = (u8)(h0);
  pOut->data[1]  = (u8)(h0 >> 8);
  pOut->data[2]  = (u8)(h0 >> 16);
  pOut->data[3]  = (u8)(h0 >> 24);
  pOut->data[4]  = (u8)(h1);
  pOut->data[5]  = (u8)(h1 >> 8);
  pOut->data[6]  = (u8)(h1 >> 16);
  pOut->data[7]  = (u8)(h1 >> 24);
  pOut->data[8]  = (u8)(h2);
  pOut->data[9]  = (u8)(h2 >> 8);
  pOut->data[10] = (u8)(h2 >> 16);
  pOut->data[11] = (u8)(h2 >> 24);
  pOut->data[12] = (u8)(h3);
  pOut->data[13] = (u8)(h3 >> 8);
  pOut->data[14] = (u8)(h3 >> 16);
  pOut->data[15] = (u8)(h3 >> 24);
  pOut->data[16] = (u8)(h4);
  pOut->data[17] = (u8)(h4 >> 8);
  pOut->data[18] = (u8)(h4 >> 16);
  pOut->data[19] = (u8)(h4 >> 24);
}

/*****************************************************************************
** Hash comparison and utility
*****************************************************************************/

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

/*****************************************************************************
** Buzhash rolling hash
**
** Uses a 256-entry table of pseudo-random u32 values and a circular buffer
** of size windowSize. On each update the outgoing byte is rotated out and
** the incoming byte is XOR'd in.
*****************************************************************************/

static const u32 buzHashTable[256] = {
  0x6b326ac4U, 0x13f8e1a8U, 0xb240d8d2U, 0x9a7c5e3fU,
  0x4e5d6c8aU, 0xd8f1b42eU, 0x2a93e670U, 0xf7d40bc1U,
  0x81c6f09bU, 0x5419d7a3U, 0xc3a82546U, 0x67eb9315U,
  0xa5f64d89U, 0x3b0e81f4U, 0xe9c23a67U, 0x0d74f6dbU,
  0x7685ca12U, 0xc03b27e5U, 0x1ea9de90U, 0x8f57634cU,
  0x42d8b1f7U, 0xfb6e4523U, 0x350c9a6eU, 0xa8e13fb8U,
  0x5c92d074U, 0xe4ab87c9U, 0x097d5e31U, 0xb6c4a2f6U,
  0x71384d5aU, 0xcd19e8bfU, 0x28f57603U, 0x946a3bc7U,
  0xdf81c962U, 0x43b7e015U, 0xfa2d54a9U, 0x1696bf7cU,
  0x8b4e13d8U, 0x6fd3a846U, 0xc5027f9bU, 0x32e9d160U,
  0xa41b6523U, 0x587cfa97U, 0xed954e0bU, 0x01a8b3ceU,
  0xbf632792U, 0x74d69c57U, 0xc8154fdbU, 0x234ae81eU,
  0x9e8d7ba4U, 0x4cf13069U, 0xd72bc62dU, 0x6a9e51f0U,
  0xb5040db4U, 0x18e7a279U, 0x8c73693dU, 0xf03cde00U,
  0x53b814c4U, 0xa76f9b89U, 0x0b92574dU, 0xce26e310U,
  0x62d18cd5U, 0xd90b4899U, 0x3d67a75cU, 0x81fc3e20U,
  0xf4a5d1e5U, 0x48396fa9U, 0xbc8e036cU, 0x10c2b830U,
  0x6517e4f5U, 0xd964a2b9U, 0x2dab5f7cU, 0x91f01b40U,
  0xe53dc605U, 0x497b8dc9U, 0xbda6428cU, 0x01e3f950U,
  0x562cae15U, 0xaa706bd9U, 0xfe8d209cU, 0x42c1d760U,
  0x97148c25U, 0xeb5843e9U, 0x3f95f8acU, 0x83d2af70U,
  0xd81f6435U, 0x2c5c1bf9U, 0x7098c9bcU, 0xc4d58080U,
  0x19123745U, 0x6d4fee09U, 0xb18ca4ccU, 0x05c95b90U,
  0x5a061255U, 0xae43c119U, 0xf28078dcU, 0x46bd2fa0U,
  0x9afae665U, 0xef379d29U, 0x337454ecU, 0x87b10bb0U,
  0xdbedc275U, 0x202a7939U, 0x746730fcU, 0xc8a4e7c0U,
  0x1ce19e85U, 0x611e5549U, 0xb55b0c0cU, 0x0998c3d0U,
  0x5dd57a95U, 0xa2123159U, 0xf64fe81cU, 0x4a8c9fe0U,
  0x9ec956a5U, 0xe3060d69U, 0x3743c42cU, 0x8b807bf0U,
  0xdfbd32b5U, 0x23fae979U, 0x7837a03cU, 0xcc745700U,
  0x10b10ec5U, 0x64edc589U, 0xb92a7c4cU, 0x0d673310U,
  0x51a4ead5U, 0xa5e1a199U, 0xfa1e585cU, 0x4e5b0f20U,
  0xc19783acU, 0x15d43a70U, 0x6a11f135U, 0xbe4ea8f9U,
  0x028b5fbcU, 0x56c81680U, 0xab05cd45U, 0xff428409U,
  0x437f3bccU, 0x97bcf290U, 0xebf9a955U, 0x30366019U,
  0x847317dcU, 0xd8b0cea0U, 0x2ced8565U, 0x712a3c29U,
  0xc567f3ecU, 0x19a4aab0U, 0x6de16175U, 0xb21e1839U,
  0x065bcffcU, 0x5a9886c0U, 0xaed53d85U, 0xf312f449U,
  0x474fab0cU, 0x9b8c62d0U, 0xefc91995U, 0x3406d059U,
  0x8843871cU, 0xdc803ee0U, 0x20bdf5a5U, 0x74faac69U,
  0xc937632cU, 0x1d741af0U, 0x61b1d1b5U, 0xb5ee8879U,
  0x0a2b3f3cU, 0x5e68f600U, 0xa2a5adc5U, 0xf6e26489U,
  0x4b1f1b4cU, 0x9f5cd210U, 0xe39989d5U, 0x37d64099U,
  0x8c13f75cU, 0xd050ae20U, 0x248d65e5U, 0x78ca1ca9U,
  0xcd07d36cU, 0x11448a30U, 0x658140f5U, 0xb9bef7b9U,
  0x0dfbae7cU, 0x52386540U, 0xa6751c05U, 0xfab2d3c9U,
  0x4eef8a8cU, 0x932c4150U, 0xe769f815U, 0x3ba6afd9U,
  0x8fe3669cU, 0xd4201d60U, 0x285dd425U, 0x7c9a8be9U,
  0xc0d742acU, 0x1514f970U, 0x6951b035U, 0xbd8e67f9U,
  0x01cb1ebcU, 0x5608d580U, 0xaa458c45U, 0xfe824309U,
  0x42bffaccU, 0x96fcb190U, 0xeb396855U, 0x3f761f19U,
  0x83b3d6dcU, 0xd7f08da0U, 0x2c2d4465U, 0x706afb29U,
  0xc4a7b2ecU, 0x18e469b0U, 0x6d212075U, 0xb15ed739U,
  0x059b8efcU, 0x59d845c0U, 0xae15fc85U, 0xf252b349U,
  0x468f6a0cU, 0x9acc21d0U, 0xef09d895U, 0x33468f59U,
  0x8783461cU, 0xdbc0fde0U, 0x2ffdb4a5U, 0x743a6b69U,
  0xc877222cU, 0x1cb4d9f0U, 0x60f190b5U, 0xb52e4779U,
  0x096bfe3cU, 0x5da8b500U, 0xa1e56cc5U, 0xf6222389U,
  0x4a5fda4cU, 0x9e9c9110U, 0xe2d948d5U, 0x3716ff99U,
  0x8b53b65cU, 0xdf906d20U, 0x23cd24e5U, 0x780adba9U,
  0xcc47926cU, 0x10844930U, 0x64c100f5U, 0xb8feb7b9U,
  0x0d3b6e7cU, 0x51782540U, 0xa5b5dc05U, 0xf9f293c9U,
  0x4e2f4a8cU, 0x926c0150U, 0xe6a9b815U, 0x3ae66fd9U,
};

int prollyRollingHashInit(ProllyRollingHash *rh, int windowSize){
  memset(rh, 0, sizeof(*rh));
  rh->windowSize = windowSize;
  rh->window = (u8 *)sqlite3_malloc(windowSize);
  if( rh->window == 0 ) return SQLITE_NOMEM;
  memset(rh->window, 0, windowSize);
  return SQLITE_OK;
}

u32 prollyRollingHashUpdate(ProllyRollingHash *rh, u8 byte){
  u8 outgoing;
  u32 h;

  outgoing = rh->window[rh->pos];
  rh->window[rh->pos] = byte;
  rh->pos = (rh->pos + 1) % rh->windowSize;
  if( rh->filled < rh->windowSize ){
    rh->filled++;
  }

  /* Rotate left by 1 */
  h = rh->hash;
  h = (h << 1) | (h >> 31);

  /* XOR out the contribution of the outgoing byte (rotated by windowSize) */
  {
    u32 outHash = buzHashTable[outgoing];
    int rot = rh->windowSize % 32;
    u32 rotatedOut = (outHash << rot) | (outHash >> (32 - rot));
    h ^= rotatedOut;
  }

  /* XOR in the incoming byte */
  h ^= buzHashTable[byte];

  rh->hash = h;
  return h;
}

int prollyRollingHashAtBoundary(ProllyRollingHash *rh, u32 pattern){
  return (rh->hash & pattern) == pattern;
}

void prollyRollingHashReset(ProllyRollingHash *rh){
  rh->hash = 0;
  rh->pos = 0;
  rh->filled = 0;
  if( rh->window ){
    memset(rh->window, 0, rh->windowSize);
  }
}

void prollyRollingHashFree(ProllyRollingHash *rh){
  if( rh->window ){
    sqlite3_free(rh->window);
    rh->window = 0;
  }
}

#endif /* DOLTLITE_PROLLY */
