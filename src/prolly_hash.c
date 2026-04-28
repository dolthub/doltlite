#ifdef DOLTLITE_PROLLY

#include "prolly_hash.h"
#include "blake3.h"
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
