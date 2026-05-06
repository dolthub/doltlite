#ifdef DOLTLITE_PROLLY

#include "prolly_hash.h"
#include <string.h>

static u64 sha512_rotr(u64 x, int n){ return (x >> n) | (x << (64 - n)); }
static u64 sha512_Ch(u64 x, u64 y, u64 z){ return (x & y) ^ (~x & z); }
static u64 sha512_Maj(u64 x, u64 y, u64 z){ return (x & y) ^ (x & z) ^ (y & z); }
static u64 sha512_Sigma0(u64 x){ return sha512_rotr(x,28) ^ sha512_rotr(x,34) ^ sha512_rotr(x,39); }
static u64 sha512_Sigma1(u64 x){ return sha512_rotr(x,14) ^ sha512_rotr(x,18) ^ sha512_rotr(x,41); }
static u64 sha512_sigma0(u64 x){ return sha512_rotr(x,1) ^ sha512_rotr(x,8) ^ (x >> 7); }
static u64 sha512_sigma1(u64 x){ return sha512_rotr(x,19) ^ sha512_rotr(x,61) ^ (x >> 6); }

static const u64 K512[80] = {
  0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL,
  0x3956c25bf348b538ULL, 0x59f111f1b605d019ULL, 0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL,
  0xd807aa98a3030242ULL, 0x12835b0145706fbeULL, 0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL,
  0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL, 0xc19bf174cf692694ULL,
  0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL, 0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
  0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL,
  0x983e5152ee66dfabULL, 0xa831c66d2db43210ULL, 0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL,
  0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL, 0x06ca6351e003826fULL, 0x142929670a0e6e70ULL,
  0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL, 0x53380d139d95b3dfULL,
  0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
  0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL, 0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL,
  0xd192e819d6ef5218ULL, 0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL,
  0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL, 0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL,
  0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL, 0x5b9cca4f7763e373ULL, 0x682e6ff3d6b2b8a3ULL,
  0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
  0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL,
  0xca273eceea26619cULL, 0xd186b8c721c0c207ULL, 0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL,
  0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL, 0x113f9804bef90daeULL, 0x1b710b35131c471bULL,
  0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL, 0x431d67c49c100d4cULL,
  0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL, 0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL,
};

static u64 sha512_load64be(const u8 *p){
  return ((u64)p[0]<<56) | ((u64)p[1]<<48) | ((u64)p[2]<<40) | ((u64)p[3]<<32)
       | ((u64)p[4]<<24) | ((u64)p[5]<<16) | ((u64)p[6]<<8)  | (u64)p[7];
}

static void sha512_store64be(u8 *p, u64 v){
  p[0]=(u8)(v>>56); p[1]=(u8)(v>>48); p[2]=(u8)(v>>40); p[3]=(u8)(v>>32);
  p[4]=(u8)(v>>24); p[5]=(u8)(v>>16); p[6]=(u8)(v>>8);  p[7]=(u8)v;
}

static void sha512_transform(u64 state[8], const u8 block[128]){
  u64 W[80], a, b, c, d, e, f, g, h, T1, T2;
  int t;
  for(t=0; t<16; t++) W[t] = sha512_load64be(block + t*8);
  for(t=16; t<80; t++) W[t] = sha512_sigma1(W[t-2]) + W[t-7] + sha512_sigma0(W[t-15]) + W[t-16];
  a=state[0]; b=state[1]; c=state[2]; d=state[3];
  e=state[4]; f=state[5]; g=state[6]; h=state[7];
  for(t=0; t<80; t++){
    T1 = h + sha512_Sigma1(e) + sha512_Ch(e,f,g) + K512[t] + W[t];
    T2 = sha512_Sigma0(a) + sha512_Maj(a,b,c);
    h=g; g=f; f=e; e=d+T1; d=c; c=b; b=a; a=T1+T2;
  }
  state[0]+=a; state[1]+=b; state[2]+=c; state[3]+=d;
  state[4]+=e; state[5]+=f; state[6]+=g; state[7]+=h;
}

static void sha512_hash(const void *data, int len, u8 digest[64]){
  u64 state[8] = {
    0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL,
    0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
    0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
    0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL,
  };
  const u8 *p = (const u8*)data;
  int remaining = len;
  u8 block[128];
  int i;

  while( remaining >= 128 ){
    sha512_transform(state, p);
    p += 128;
    remaining -= 128;
  }


  memset(block, 0, 128);
  memcpy(block, p, remaining);
  block[remaining] = 0x80;

  if( remaining >= 112 ){
    sha512_transform(state, block);
    memset(block, 0, 128);
  }


  sha512_store64be(block + 120, (u64)len * 8);

  sha512_transform(state, block);

  for(i=0; i<8; i++) sha512_store64be(digest + i*8, state[i]);
}

void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut){
  u8 digest[64];
  sha512_hash(pData, nData, digest);
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


  h = rh->hash;
  h = (h << 1) | (h >> 31);


  {
    /* `outHash >> (32 - rot)` is UB when rot == 0 (shift exponent
    ** equals the full type width, C11 6.5.7/3). Masking with 31
    ** makes rot=0 a no-op rotation without losing the standard
    ** rol32 idiom the compiler recognizes for rot in [1, 31]. */
    u32 outHash = buzHashTable[outgoing];
    int rot = rh->windowSize % 32;
    u32 rotatedOut = (outHash << rot) | (outHash >> ((32 - rot) & 31));
    h ^= rotatedOut;
  }


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

#endif
