#include "doltlite_creds.h"
#include "doltlite_parse.h"

#include "ed25519.h"
#include "sha512.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#include <bcrypt.h>
#include <direct.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h> /* amalgamator: keep */
#include <unistd.h>
#endif

void *sqlite3_malloc(int);
void *sqlite3_realloc(void*, int);
void sqlite3_free(void*);

static char *dlStrDup(const char *s){
  size_t n;
  char *z;
  if( !s ) return 0;
  n = strlen(s) + 1;
  if( n > (size_t)0x7fffffff ) return 0;
  z = (char*)sqlite3_malloc((int)n);
  if( z ) memcpy(z, s, n);
  return z;
}

struct DoltliteCreds {
  unsigned char seed[DOLTLITE_SEED_LEN];
  unsigned char pub[DOLTLITE_PUBKEY_LEN];
  unsigned char expanded[64];
};

static const uint64_t SHA512_224_IV[8] = {
    UINT64_C(0x8C3D37C819544DA2), UINT64_C(0x73E1996689DCD4D6),
    UINT64_C(0x1DFAB7AE32FF9C82), UINT64_C(0x679DD514582F9FCF),
    UINT64_C(0x0F6D2B697BD44DA8), UINT64_C(0x77E36F7304C48942),
    UINT64_C(0x3F9D85A86A1D36C8), UINT64_C(0x1112E6AD91D692A1),
};

void doltliteSha512_224(const unsigned char *in, size_t inlen,
                        unsigned char out[DOLTLITE_KID_RAW_LEN]) {
  sha512_context ctx;
  unsigned char full[64];
  int i;

  sha512_init(&ctx);
  for (i = 0; i < 8; i++) ctx.state[i] = SHA512_224_IV[i];

  sha512_update(&ctx, in, inlen);
  sha512_final(&ctx, full);
  memcpy(out, full, DOLTLITE_KID_RAW_LEN);
}

static const char B32_ALPHABET[] = "0123456789abcdefghijklmnopqrstuv";

static char *doltliteBase32Encode(const unsigned char *in, size_t inlen) {
  size_t outlen = (inlen * 8 + 4) / 5;
  char *out = (char *)sqlite3_malloc(outlen + 1);
  size_t oi = 0;
  uint32_t buf = 0;
  int bits = 0;
  size_t i;

  if (!out) return NULL;
  for (i = 0; i < inlen; i++) {
    buf = (buf << 8) | in[i];
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out[oi++] = B32_ALPHABET[(buf >> bits) & 0x1f];
    }
  }
  if (bits > 0) {
    out[oi++] = B32_ALPHABET[(buf << (5 - bits)) & 0x1f];
  }
  out[oi] = '\0';
  return out;
}

static const char B64URL_ALPHABET[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

char *doltliteBase64UrlEncode(const unsigned char *in, size_t inlen) {
  size_t outlen = ((inlen + 2) / 3) * 4;
  char *out = (char *)sqlite3_malloc(outlen + 1);
  size_t i = 0, o = 0;

  if (!out) return NULL;
  while (i + 3 <= inlen) {
    uint32_t n = (in[i] << 16) | (in[i + 1] << 8) | in[i + 2];
    out[o++] = B64URL_ALPHABET[(n >> 18) & 63];
    out[o++] = B64URL_ALPHABET[(n >> 12) & 63];
    out[o++] = B64URL_ALPHABET[(n >> 6) & 63];
    out[o++] = B64URL_ALPHABET[n & 63];
    i += 3;
  }
  if (inlen - i == 1) {
    uint32_t n = in[i] << 16;
    out[o++] = B64URL_ALPHABET[(n >> 18) & 63];
    out[o++] = B64URL_ALPHABET[(n >> 12) & 63];
    out[o++] = '=';
    out[o++] = '=';
  } else if (inlen - i == 2) {
    uint32_t n = (in[i] << 16) | (in[i + 1] << 8);
    out[o++] = B64URL_ALPHABET[(n >> 18) & 63];
    out[o++] = B64URL_ALPHABET[(n >> 12) & 63];
    out[o++] = B64URL_ALPHABET[(n >> 6) & 63];
    out[o++] = '=';
  }
  out[o] = '\0';
  return out;
}

static int b64urlVal(char c) {
  if (c >= 'A' && c <= 'Z') return c - 'A';
  if (c >= 'a' && c <= 'z') return c - 'a' + 26;
  if (c >= '0' && c <= '9') return c - '0' + 52;
  if (c == '-') return 62;
  if (c == '_') return 63;
  return -1;
}

int doltliteBase64UrlDecode(const char *in, unsigned char **out, size_t *outlen) {
  size_t inlen = strlen(in);
  size_t cap, o = 0, i = 0;
  unsigned char *buf;
  uint32_t acc = 0;
  int nbits = 0;

  if (inlen % 4 == 1) return 1;
  cap = inlen / 4 * 3 + 3;
  buf = (unsigned char *)sqlite3_malloc(cap ? cap : 1);
  if (!buf) return 1;

  for (i = 0; i < inlen; i++) {
    char c = in[i];
    int v;
    if (c == '=') break;
    v = b64urlVal(c);
    if (v < 0) {
      sqlite3_free(buf);
      return 1;
    }
    acc = (acc << 6) | (uint32_t)v;
    nbits += 6;
    if (nbits >= 8) {
      nbits -= 8;
      buf[o++] = (unsigned char)((acc >> nbits) & 0xff);
    }
  }
  *out = buf;
  *outlen = o;
  return 0;
}

static int randomBytes(unsigned char *p, size_t n) {
#ifdef _WIN32
  return BCryptGenRandom(NULL, p, (ULONG)n,
                         BCRYPT_USE_SYSTEM_PREFERRED_RNG) == 0
             ? 0
             : 1;
#else
  int fd = open("/dev/urandom", O_RDONLY);
  size_t got = 0;
  if (fd < 0) return 1;
  while (got < n) {
    ssize_t r = read(fd, p + got, n - got);
    if (r <= 0) {
      if (r < 0 && errno == EINTR) continue;
      close(fd);
      return 1;
    }
    got += (size_t)r;
  }
  close(fd);
  return 0;
#endif
}

int doltliteCredsFromSeed(const unsigned char seed[DOLTLITE_SEED_LEN],
                          DoltliteCreds **out) {
  DoltliteCreds *c = (DoltliteCreds *)sqlite3_malloc((int)sizeof(*c));
  if (!c) return 1;
  memset(c, 0, sizeof(*c));
  memcpy(c->seed, seed, DOLTLITE_SEED_LEN);
  ed25519_create_keypair(c->pub, c->expanded, c->seed);
  *out = c;
  return 0;
}

int doltliteCredsGenerate(DoltliteCreds **out) {
  unsigned char seed[DOLTLITE_SEED_LEN];
  int rc;
  if (randomBytes(seed, sizeof(seed))) return 1;
  rc = doltliteCredsFromSeed(seed, out);
  memset(seed, 0, sizeof(seed));
  return rc;
}

void doltliteCredsFree(DoltliteCreds *c) {
  if (!c) return;
  memset(c, 0, sizeof(*c));
  sqlite3_free(c);
}

char *doltliteCredsKid(const DoltliteCreds *c) {
  unsigned char raw[DOLTLITE_KID_RAW_LEN];
  doltliteSha512_224(c->pub, DOLTLITE_PUBKEY_LEN, raw);
  return doltliteBase32Encode(raw, sizeof(raw));
}

char *doltliteCredsPubKeyB32(const DoltliteCreds *c) {
  return doltliteBase32Encode(c->pub, DOLTLITE_PUBKEY_LEN);
}

void doltliteCredsSign(const DoltliteCreds *c, const unsigned char *msg,
                       size_t msglen, unsigned char sig[DOLTLITE_SIG_LEN]) {
  ed25519_sign(sig, msg, msglen, c->pub, c->expanded);
}

#define JWT_ISSUER        "dolt-client.dolthub.com"
#define JWT_SUBJECT_PREFIX "doltClientCredentials/"
#define JWT_TOKEN_VERSION "2023.01"
#define JWT_TTL_SECONDS   30
#define JWT_CLOCK_SKEW_SECONDS 60

static char *b64urlRaw(const unsigned char *in, size_t len) {
  char *s = doltliteBase64UrlEncode(in, len);
  size_t n;
  if (!s) return NULL;
  n = strlen(s);
  while (n > 0 && s[n - 1] == '=') s[--n] = '\0';
  return s;
}

int doltliteCredsBearerTokenAt(const DoltliteCreds *c, const char *audience,
                               long iat, char **jwtOut) {
  char *kid = NULL, *header = NULL, *claims = NULL;
  char *h64 = NULL, *c64 = NULL, *sig64 = NULL, *input = NULL, *token = NULL;
  unsigned char sig[DOLTLITE_SIG_LEN];
  int rc = 1;
  size_t hn, cn, need;

  if( !c || !audience || !audience[0] || !jwtOut ) return 1;
  *jwtOut = 0;

  kid = doltliteCredsKid(c);
  if (!kid) goto done;

  hn = strlen(kid) + 96;
  header = (char *)sqlite3_malloc(hn);
  if (!header) goto done;
  snprintf(header, hn,
           "{\"alg\":\"EdDSA\",\"kid\":\"%s\",\"dolt_token_version\":\"%s\"}",
           kid, JWT_TOKEN_VERSION);

  cn = strlen(kid) + strlen(audience) + 164;
  claims = (char *)sqlite3_malloc(cn);
  if (!claims) goto done;
  snprintf(claims, cn,
           "{\"iss\":\"%s\",\"sub\":\"%s%s\",\"aud\":[\"%s\"],\"iat\":%ld,\"exp\":%ld}",
           JWT_ISSUER, JWT_SUBJECT_PREFIX, kid, audience, iat,
           iat + JWT_TTL_SECONDS);

  h64 = b64urlRaw((const unsigned char *)header, strlen(header));
  c64 = b64urlRaw((const unsigned char *)claims, strlen(claims));
  if (!h64 || !c64) goto done;

  need = strlen(h64) + 1 + strlen(c64) + 1;
  input = (char *)sqlite3_malloc(need);
  if (!input) goto done;
  snprintf(input, need, "%s.%s", h64, c64);

  doltliteCredsSign(c, (const unsigned char *)input, strlen(input), sig);
  sig64 = b64urlRaw(sig, sizeof(sig));
  if (!sig64) goto done;

  need = strlen(input) + 1 + strlen(sig64) + 1;
  token = (char *)sqlite3_malloc(need);
  if (!token) goto done;
  snprintf(token, need, "%s.%s", input, sig64);

  *jwtOut = token;
  token = NULL;
  rc = 0;

done:
  sqlite3_free(kid);
  sqlite3_free(header);
  sqlite3_free(claims);
  sqlite3_free(h64);
  sqlite3_free(c64);
  sqlite3_free(sig64);
  sqlite3_free(input);
  sqlite3_free(token);
  return rc;
}

int doltliteCredsBearerToken(const DoltliteCreds *c, const char *audience,
                             char **jwtOut) {
  return doltliteCredsBearerTokenAt(c, audience, (long)time(NULL), jwtOut);
}

char *doltliteCredsToJwk(const DoltliteCreds *c) {
  char *x = doltliteBase64UrlEncode(c->pub, DOLTLITE_PUBKEY_LEN);
  char *d = doltliteBase64UrlEncode(c->seed, DOLTLITE_SEED_LEN);
  char *json = NULL;
  if (x && d) {
    size_t n = strlen(x) + strlen(d) + 64;
    json = (char *)sqlite3_malloc(n);
    if (json) {
      snprintf(json, n,
               "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"x\":\"%s\",\"d\":\"%s\"}",
               x, d);
    }
  }
  sqlite3_free(x);
  sqlite3_free(d);
  return json;
}

char *doltliteCredsToPublicJwk(const DoltliteCreds *c) {
  char *x = doltliteBase64UrlEncode(c->pub, DOLTLITE_PUBKEY_LEN);
  char *json = NULL;
  if (x) {
    size_t n = strlen(x) + 48;
    json = (char *)sqlite3_malloc(n);
    if (json) {
      snprintf(json, n,
               "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"x\":\"%s\"}", x);
    }
  }
  sqlite3_free(x);
  return json;
}

static const char *jsonSkipWs(const char *p){
  while( p && (*p==' ' || *p=='\t' || *p=='\r' || *p=='\n') ) p++;
  return p;
}

static const char *jsonSkipString(const char *p){
  if( !p || *p!='"' ) return 0;
  p++;
  while( *p && *p!='"' ){
    if( *p=='\\' ){
      p++;
      if( !*p ) return 0;
    }
    p++;
  }
  return *p=='"' ? p+1 : 0;
}

static const char *jsonSkipValue(const char *p){
  p = jsonSkipWs(p);
  if( !p || !*p ) return 0;
  if( *p=='"' ) return jsonSkipString(p);
  if( *p=='{' || *p=='[' ){
    char open = *p;
    char close = (open=='{') ? '}' : ']';
    int depth = 1;
    p++;
    while( *p && depth ){
      if( *p=='"' ){
        p = jsonSkipString(p);
        if( !p ) return 0;
        continue;
      }
      if( *p==open ) depth++;
      else if( *p==close ) depth--;
      p++;
    }
    return depth ? 0 : p;
  }
  while( *p && *p!=',' && *p!='}' && *p!=']' ) p++;
  return p;
}

static const char *credsJsonObjectValue(const char *json, const char *key){
  const char *p;
  size_t keylen;
  if( !json || !key ) return 0;
  keylen = strlen(key);
  p = jsonSkipWs(json);
  if( *p!='{' ) return 0;
  p++;
  for(;;){
    const char *kstart;
    const char *kend;
    p = jsonSkipWs(p);
    if( *p=='}' ) return 0;
    if( *p!='"' ) return 0;
    kstart = p+1;
    p = jsonSkipString(p);
    if( !p ) return 0;
    kend = p-1;
    p = jsonSkipWs(p);
    if( *p!=':' ) return 0;
    p = jsonSkipWs(p+1);
    if( (size_t)(kend-kstart)==keylen && memcmp(kstart, key, keylen)==0 ){
      return p;
    }
    p = jsonSkipValue(p);
    if( !p ) return 0;
    p = jsonSkipWs(p);
    if( *p==',' ){ p++; continue; }
    return 0;
  }
}

static char *jsonDupQuoted(const char *p){
  const char *end;
  const char *s;
  char *out;
  size_t cap;
  size_t o = 0;
  if( !p || *p!='"' ) return 0;
  end = jsonSkipString(p);
  if( !end ) return 0;
  cap = (size_t)(end - p);
  if( cap>(size_t)0x7fffffff ) return 0;
  out = (char*)sqlite3_malloc((int)cap);
  if( !out ) return 0;
  for(s=p+1; s<end-1; s++){
    if( *s=='\\' && s+1<end-1 ) s++;
    out[o++] = *s;
  }
  out[o] = 0;
  return out;
}

static char *jsonFindString(const char *json, const char *key) {
  return jsonDupQuoted(credsJsonObjectValue(json, key));
}

static int jsonParseLong(const char *p, long *out) {
  const char *end;
  uint64_t v;
  if( !p || !out ) return 0;
  end = p;
  while( *end>='0' && *end<='9' ) end++;
  if( doltliteParseDecimal(p, end, LONG_MAX, &v)!=DOLTLITE_DECIMAL_OK ){
    return 0;
  }
  end = jsonSkipWs(end);
  if( *end!=',' && *end!='}' ) return 0;
  *out = (long)v;
  return 1;
}

static int jsonFindLong(const char *json, const char *key, long *out) {
  return jsonParseLong(credsJsonObjectValue(json, key), out);
}

static int jsonFindOptionalLong(
  const char *json,
  const char *key,
  long *out,
  int *present
) {
  const char *p = credsJsonObjectValue(json, key);
  *present = p != 0;
  return !p || jsonParseLong(p, out);
}

static int jsonAudienceMatches(const char *json, const char *expected){
  const char *p = credsJsonObjectValue(json, "aud");
  if( !p || !expected || !expected[0] ) return 0;
  if( *p=='"' ){
    char *s = jsonDupQuoted(p);
    int ok = s && strcmp(s, expected)==0;
    sqlite3_free(s);
    return ok;
  }
  if( *p!='[' ) return 0;
  p++;
  for(;;){
    char *s;
    p = jsonSkipWs(p);
    if( *p==']' ) return 0;
    if( *p!='"' ) return 0;
    s = jsonDupQuoted(p);
    if( s && strcmp(s, expected)==0 ){
      sqlite3_free(s);
      return 1;
    }
    sqlite3_free(s);
    p = jsonSkipString(p);
    if( !p ) return 0;
    p = jsonSkipWs(p);
    if( *p==',' ){ p++; continue; }
    return 0;
  }
}

static char *decodeSegZ(const char *seg, size_t seglen) {
  char *z = (char *)sqlite3_malloc(seglen + 1);
  unsigned char *raw = NULL;
  size_t rawlen = 0;
  char *s = NULL;
  if (!z) return NULL;
  memcpy(z, seg, seglen);
  z[seglen] = '\0';
  if (doltliteBase64UrlDecode(z, &raw, &rawlen) == 0) {
    s = (char *)sqlite3_malloc(rawlen + 1);
    if (s) {
      memcpy(s, raw, rawlen);
      s[rawlen] = '\0';
    }
  }
  sqlite3_free(z);
  sqlite3_free(raw);
  return s;
}

int doltliteCredsFromJwk(const char *json, DoltliteCreds **out) {
  char *dstr = jsonFindString(json, "d");
  char *xstr = jsonFindString(json, "x");
  char *kty = jsonFindString(json, "kty");
  char *crv = jsonFindString(json, "crv");
  unsigned char *d = NULL, *x = NULL;
  size_t nd = 0, nx = 0;
  DoltliteCreds *c = NULL;
  int rc = 1;

  *out = NULL;
  if (!dstr || !xstr || !kty || !crv || strcmp(kty, "OKP") ||
      strcmp(crv, "Ed25519")) goto done;
  if (doltliteBase64UrlDecode(dstr, &d, &nd) ||
      doltliteBase64UrlDecode(xstr, &x, &nx)) goto done;
  if (nd != 32 || (nx != 32 && nx != 64)) goto done;
  if (doltliteCredsFromSeed(nx == 64 ? x : d, &c)) goto done;
  if (memcmp(c->pub, nx == 64 ? d : x, 32) ||
      (nx == 64 && memcmp(c->pub, x + 32, 32))) goto done;
  *out = c;
  c = NULL;
  rc = 0;

done:
  doltliteCredsFree(c);
  if (d) { memset(d, 0, nd); sqlite3_free(d); }
  if (x) { memset(x, 0, nx); sqlite3_free(x); }
  sqlite3_free(dstr);
  sqlite3_free(xstr);
  sqlite3_free(kty);
  sqlite3_free(crv);
  return rc;
}

char *doltliteCredsDir(void) {
  const char *override = getenv("DOLTLITE_CREDS_DIR");
  const char *home;
  char *dir;
  if (override && *override) {
    return dlStrDup(override);
  }
  home = getenv("HOME");
#ifdef _WIN32
  if (!home || !*home) home = getenv("USERPROFILE");
  if (!home || !*home) home = getenv("APPDATA");
#endif
  if (!home || !*home) return NULL;
  {
    size_t n = strlen(home) + strlen("/.doltlite/creds") + 1;
    dir = (char *)sqlite3_malloc(n);
    if (!dir) return NULL;
    snprintf(dir, n, "%s/.doltlite/creds", home);
  }
  return dir;
}

static int makeDir(const char *path) {
#ifdef _WIN32
  return _mkdir(path);
#else
  return mkdir(path, 0700);
#endif
}

static int mkdirp(const char *path) {
  char *tmp = dlStrDup(path);
  size_t len, i;
  if (!tmp) return 1;
  len = strlen(tmp);
  for (i = 1; i < len; i++) {
    if (tmp[i] == '/' || tmp[i] == '\\') {
      char sep = tmp[i];
      tmp[i] = '\0';
      /* Skip a Windows drive prefix ("C:"). */
      if (tmp[i - 1] != ':' && makeDir(tmp) != 0 && errno != EEXIST) {
        sqlite3_free(tmp);
        return 1;
      }
      tmp[i] = sep;
    }
  }
  if (makeDir(tmp) != 0 && errno != EEXIST) {
    sqlite3_free(tmp);
    return 1;
  }
  sqlite3_free(tmp);
  return 0;
}

static char *credsResolveKid(const char*);

static int credsKidValid(const char *kid) {
  const size_t nKid = (DOLTLITE_KID_RAW_LEN * 8 + 4) / 5;
  size_t i;
  if (!kid) return 0;
  for (i = 0; i < nKid; i++) {
    const char *p;
    if (!kid[i]) return 0;
    p = strchr(B32_ALPHABET, kid[i]);
    if (!p) return 0;
    if (i == nKid - 1 && ((p - B32_ALPHABET) & 1) != 0) return 0;
  }
  return kid[nKid] == '\0';
}

static char *credsFilePath(const char *dir, const char *kid) {
  size_t n;
  char *path;
  if (!credsKidValid(kid)) return NULL;
  n = strlen(dir) + 1 + strlen(kid) + strlen(".jwk") + 1;
  path = (char *)sqlite3_malloc(n);
  if (!path) return NULL;
  snprintf(path, n, "%s/%s.jwk", dir, kid);
  return path;
}

#ifndef _WIN32
static int credsFsyncDir(const char *dir) {
  int fd;
  int rc;
  fd = open(dir, O_RDONLY | O_DIRECTORY);
  if (fd < 0) return 1;
  do {
    rc = fsync(fd);
  } while (rc < 0 && errno == EINTR);
  close(fd);
  return rc < 0;
}
#endif

/* Write a temporary file in the same directory, fsync it, and rename it
   over path. A short write removes the temporary file and leaves path
   unchanged. exclusive leaves an existing path untouched. */
static int credsReplaceDurable(const char *dir, const char *path,
                               const char *data, size_t len, int exclusive) {
  char *tmp = NULL;
  int created = 0;
  int committed = 0;
  int rc = 1;
#ifdef _WIN32
  FILE *out = NULL;
  size_t ntmp;
  static volatile LONG seq = 0;
#else
  int fd = -1;
  int existing;
  size_t ntmp;
  size_t off = 0;
  ssize_t wrote;
  int synced;
#endif

  if (exclusive) {
#ifdef _WIN32
    if (GetFileAttributesA(path) != INVALID_FILE_ATTRIBUTES) return 1;
#else
    existing = open(path, O_RDONLY);
    if (existing >= 0) {
      close(existing);
      return 1;
    }
    if (errno != ENOENT) return 1;
#endif
  }

#ifdef _WIN32
  (void)dir;
  ntmp = strlen(path) + 48;
  if (ntmp > (size_t)0x7fffffff) return 1;
  tmp = (char *)sqlite3_malloc((int)ntmp);
  if (!tmp) return 1;
  snprintf(tmp, ntmp, "%s.tmp.%lu.%ld", path,
           (unsigned long)GetCurrentProcessId(),
           (long)InterlockedIncrement(&seq));
  out = fopen(tmp, "wb");
  if (!out) goto done;
  created = 1;
  if (fwrite(data, 1, len, out) != len) goto done;
  if (fflush(out)) goto done;
  if (fclose(out) != 0) {
    out = NULL;
    goto done;
  }
  out = NULL;
  if (exclusive) {
    if (!MoveFileExA(tmp, path, MOVEFILE_WRITE_THROUGH)) goto done;
  } else if (!MoveFileExA(tmp, path,
                          MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    goto done;
  }
  committed = 1;
#else
  ntmp = strlen(path) + 8;
  if (ntmp > (size_t)0x7fffffff) return 1;
  tmp = (char *)sqlite3_malloc((int)ntmp);
  if (!tmp) return 1;
  snprintf(tmp, ntmp, "%s.XXXXXX", path);
  fd = mkstemp(tmp);
  if (fd < 0) goto done;
  created = 1;
  (void)fchmod(fd, 0600);
  while (off < len) {
    wrote = write(fd, data + off, len - off);
    if (wrote < 0 && errno == EINTR) continue;
    if (wrote <= 0) goto done;
    off += (size_t)wrote;
  }
  do {
    synced = fsync(fd);
    if (synced == 0) break;
    if (errno != EINTR) goto done;
  } while (1);
  if (close(fd) != 0) {
    fd = -1;
    goto done;
  }
  fd = -1;
  if (exclusive) {
    if (link(tmp, path) != 0) goto done;
    if (unlink(tmp) != 0) goto done;
  } else if (rename(tmp, path) != 0) {
    goto done;
  }
  committed = 1;
  if (credsFsyncDir(dir)) goto done;
#endif
  rc = 0;

done:
#ifdef _WIN32
  if (out) fclose(out);
#else
  if (fd >= 0) close(fd);
#endif
  if (created && !committed) remove(tmp);
  sqlite3_free(tmp);
  return rc;
}

static int credsSaveJwk(const DoltliteCreds *c, const char *dir,
                        int publicOnly) {
  char *owned = NULL;
  char *kid = NULL;
  char *path = NULL;
  char *json = NULL;
  int rc = 1;

  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) goto done;
  if (mkdirp(dir)) goto done;

  kid = doltliteCredsKid(c);
  if (!kid) goto done;
  path = credsFilePath(dir, kid);
  if (!path) goto done;
  json = publicOnly ? doltliteCredsToPublicJwk(c) : doltliteCredsToJwk(c);
  if (!json) goto done;
  if (credsReplaceDurable(dir, path, json, strlen(json), publicOnly)) goto done;
  rc = 0;

done:
  sqlite3_free(json);
  sqlite3_free(path);
  sqlite3_free(kid);
  sqlite3_free(owned);
  return rc;
}

int doltliteCredsSave(const DoltliteCreds *c, const char *dir) {
  return credsSaveJwk(c, dir, 0);
}

int doltliteCredsSavePublic(const DoltliteCreds *c, const char *dir) {
  return credsSaveJwk(c, dir, 1);
}

int doltliteCredsLoad(const char *dir, const char *kid, DoltliteCreds **out) {
  char *owned = NULL;
  char *path = NULL;
  FILE *f = NULL;
  char *json = NULL;
  long sz;
  int rc = 1;

  *out = NULL;
  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) goto done;
  path = credsFilePath(dir, kid);
  if (!path) goto done;

  f = fopen(path, "rb");
  if (!f) goto done;
  if (fseek(f, 0, SEEK_END) != 0) goto done;
  sz = ftell(f);
  if (sz < 0 || sz > 1 << 20) goto done;
  if (fseek(f, 0, SEEK_SET) != 0) goto done;
  json = (char *)sqlite3_malloc((size_t)sz + 1);
  if (!json) goto done;
  if (fread(json, 1, (size_t)sz, f) != (size_t)sz) goto done;
  json[sz] = '\0';

  rc = doltliteCredsFromJwk(json, out);
  if (!rc) {
    char *actual = doltliteCredsKid(*out);
    if (!actual || strcmp(actual, kid)) {
      doltliteCredsFree(*out);
      *out = NULL;
      rc = 1;
    }
    sqlite3_free(actual);
  }

done:
  if (f) fclose(f);
  if (json) {
    memset(json, 0, json ? strlen(json) : 0);
    sqlite3_free(json);
  }
  sqlite3_free(path);
  sqlite3_free(owned);
  return rc;
}

typedef struct DirIter {
#ifdef _WIN32
  HANDLE h;
  WIN32_FIND_DATAA fd;
  int first;
#else
  DIR *d;
#endif
} DirIter;

static int dirOpen(DirIter *it, const char *dir) {
#ifdef _WIN32
  size_t n = strlen(dir) + 3;
  char *pat = (char *)sqlite3_malloc(n);
  if (!pat) return 1;
  snprintf(pat, n, "%s\\*", dir);
  it->h = FindFirstFileA(pat, &it->fd);
  sqlite3_free(pat);
  if (it->h == INVALID_HANDLE_VALUE) {
    DWORD err = GetLastError();
    errno = err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND
          ? ENOENT : EACCES;
    return 1;
  }
  it->first = 1;
  return 0;
#else
  it->d = opendir(dir);
  return it->d ? 0 : 1;
#endif
}

static const char *dirNext(DirIter *it) {
#ifdef _WIN32
  if (it->first) {
    it->first = 0;
    return it->fd.cFileName;
  }
  return FindNextFileA(it->h, &it->fd) ? it->fd.cFileName : NULL;
#else
  struct dirent *e = readdir(it->d);
  return e ? e->d_name : NULL;
#endif
}

static void dirClose(DirIter *it) {
#ifdef _WIN32
  FindClose(it->h);
#else
  closedir(it->d);
#endif
}

int doltliteCredsLoadDefault(const char *dir, DoltliteCreds **out) {
  char *error = NULL;
  int rc = doltliteCredsResolve(dir, NULL, NULL, out, NULL, &error);
  sqlite3_free(error);
  return rc == DOLTLITE_CREDS_SELECTED ? 0 : 1;
}

int doltliteCredsList(const char *dir, char ***out, int *n) {
  char *owned = NULL;
  DirIter it;
  const char *name;
  char **arr = NULL;
  int cap = 0, cnt = 0;

  *out = NULL;
  *n = 0;
  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) return 1;

  if (dirOpen(&it, dir)) {
    int rc = errno == ENOENT ? 0 : 1;
    sqlite3_free(owned);
    return rc;
  }
  while ((name = dirNext(&it)) != NULL) {
    size_t ln = strlen(name);
    char *kid;
    if (!(ln > 4 && strcmp(name + ln - 4, ".jwk") == 0)) continue;
    kid = (char *)sqlite3_malloc(ln - 4 + 1);
    if (!kid) goto oom;
    memcpy(kid, name, ln - 4);
    kid[ln - 4] = '\0';
    if (!credsKidValid(kid)) {
      sqlite3_free(kid);
      continue;
    }
    if (cnt == cap) {
      int nc = cap ? cap * 2 : 8;
      char **na = (char **)sqlite3_realloc(arr, (size_t)nc * sizeof(char *));
      if (!na) {
        sqlite3_free(kid);
        goto oom;
      }
      arr = na;
      cap = nc;
    }
    arr[cnt++] = kid;
  }
  dirClose(&it);
  sqlite3_free(owned);
  *out = arr;
  *n = cnt;
  return 0;

oom:
  dirClose(&it);
  sqlite3_free(owned);
  doltliteCredsFreeList(arr, cnt);
  return 1;
}

void doltliteCredsFreeList(char **list, int n) {
  int i;
  if (!list) return;
  for (i = 0; i < n; i++) sqlite3_free(list[i]);
  sqlite3_free(list);
}

static char *credsExistingPath(const char *dir, const char *kid, char **pOwned) {
  char *canonical;
  char *path;
  *pOwned = NULL;
  if (!kid) return NULL;
  if (!dir) {
    *pOwned = doltliteCredsDir();
    dir = *pOwned;
  }
  if (!dir) return NULL;
  canonical = credsResolveKid(kid);
  path = canonical ? credsFilePath(dir, canonical) : NULL;
  sqlite3_free(canonical);
  if (!path) {
    sqlite3_free(*pOwned);
    *pOwned = NULL;
  }
  return path;
}

int doltliteCredsExists(const char *dir, const char *kid) {
  char *owned = NULL;
  char *path = credsExistingPath(dir, kid, &owned);
  FILE *f;
  if (!path) return 1;
  f = fopen(path, "rb");
  sqlite3_free(path);
  sqlite3_free(owned);
  if (!f) return 1;
  fclose(f);
  return 0;
}

int doltliteCredsRemove(const char *dir, const char *kid) {
  char *owned = NULL;
  char *path = credsExistingPath(dir, kid, &owned);
  int rc;
  if (!path) return 1;
  rc = (remove(path) == 0) ? 0 : 1;
  sqlite3_free(path);
  sqlite3_free(owned);
  return rc;
}

int doltliteCredsLoadPubKey(const char *dir, const char *kid,
                            unsigned char pub[DOLTLITE_PUBKEY_LEN]) {
  char *owned = NULL;
  char *path = NULL;
  FILE *f = NULL;
  char *json = NULL;
  char *kty = NULL;
  char *crv = NULL;
  char *xstr = NULL;
  char *derivedKid = NULL;
  unsigned char *raw = NULL;
  unsigned char kidRaw[DOLTLITE_KID_RAW_LEN];
  size_t rawlen = 0;
  long sz;
  int rc = 1;

  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) return 1;
  path = credsFilePath(dir, kid);
  if (!path) goto done;

  f = fopen(path, "rb");
  if (!f) goto done;
  if (fseek(f, 0, SEEK_END) != 0) goto done;
  sz = ftell(f);
  if (sz < 0 || sz > 1 << 20) goto done;
  if (fseek(f, 0, SEEK_SET) != 0) goto done;
  json = (char *)sqlite3_malloc((size_t)sz + 1);
  if (!json) goto done;
  if (fread(json, 1, (size_t)sz, f) != (size_t)sz) goto done;
  json[sz] = '\0';

  if (credsJsonObjectValue(json, "d")) goto done;
  kty = jsonFindString(json, "kty");
  crv = jsonFindString(json, "crv");
  if (!kty || strcmp(kty, "OKP") != 0) goto done;
  if (!crv || strcmp(crv, "Ed25519") != 0) goto done;
  xstr = jsonFindString(json, "x");
  if (!xstr) goto done;
  if (doltliteBase64UrlDecode(xstr, &raw, &rawlen)) goto done;
  if (rawlen != DOLTLITE_PUBKEY_LEN) goto done;
  doltliteSha512_224(raw, rawlen, kidRaw);
  derivedKid = doltliteBase32Encode(kidRaw, sizeof(kidRaw));
  if (!derivedKid || strcmp(derivedKid, kid) != 0) goto done;
  memcpy(pub, raw, DOLTLITE_PUBKEY_LEN);
  rc = 0;

done:
  if (f) fclose(f);
  sqlite3_free(json);
  sqlite3_free(path);
  sqlite3_free(owned);
  sqlite3_free(kty);
  sqlite3_free(crv);
  sqlite3_free(xstr);
  sqlite3_free(derivedKid);
  sqlite3_free(raw);
  return rc;
}

static int credsFileContainsPrivateJwk(const char *dir, const char *name) {
  char *path = NULL;
  FILE *f = NULL;
  char *json = NULL;
  size_t pathLen;
  long sz;
  int found = 0;
#ifdef _WIN32
  DWORD attrs;
#else
  struct stat st;
#endif

  pathLen = strlen(dir) + strlen(name) + 2;
  path = (char *)sqlite3_malloc(pathLen);
  if (!path) return 0;
  snprintf(path, pathLen, "%s/%s", dir, name);
#ifdef _WIN32
  attrs = GetFileAttributesA(path);
  if (attrs == INVALID_FILE_ATTRIBUTES ||
      (attrs & FILE_ATTRIBUTE_DIRECTORY) != 0) goto done;
#else
  if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) goto done;
#endif
  f = fopen(path, "rb");
  if (!f) goto done;
  if (fseek(f, 0, SEEK_END) != 0) goto done;
  sz = ftell(f);
  if (sz < 0 || sz > 1 << 20) goto done;
  if (fseek(f, 0, SEEK_SET) != 0) goto done;
  json = (char *)sqlite3_malloc((size_t)sz + 1);
  if (!json) goto done;
  if (fread(json, 1, (size_t)sz, f) != (size_t)sz) goto done;
  json[sz] = '\0';
  found = credsJsonObjectValue(json, "d") != NULL;

done:
  if (f) fclose(f);
  sqlite3_free(json);
  sqlite3_free(path);
  return found;
}

int doltliteCredsValidateAuthDir(const char *dir) {
  DirIter it;
  const char *name;
  int rc = 0;

  if (!dir || dirOpen(&it, dir)) return 1;
  while ((name = dirNext(&it)) != NULL) {
    size_t n = strlen(name);
    char *kid;
    unsigned char pub[DOLTLITE_PUBKEY_LEN];
    if (!(n > 4 && strcmp(name + n - 4, ".jwk") == 0)) {
      if (credsFileContainsPrivateJwk(dir, name)) {
        rc = 1;
        break;
      }
      continue;
    }
    kid = (char *)sqlite3_malloc(n - 4 + 1);
    if (!kid) {
      rc = 1;
      break;
    }
    memcpy(kid, name, n - 4);
    kid[n - 4] = '\0';
    if (!credsKidValid(kid) || doltliteCredsLoadPubKey(dir, kid, pub) != 0) {
      rc = 1;
      sqlite3_free(kid);
      break;
    }
    sqlite3_free(kid);
  }
  dirClose(&it);
  return rc;
}

int doltliteCredsVerifyBearer(const char *authValue, const char *expectedAudience,
                              const char *authKeysDir, long now, char **kidOut) {
  const char *jwt;
  const char *dot1, *dot2;
  char *hdr = NULL, *claims = NULL;
  char *alg = NULL, *ver = NULL, *kid = NULL;
  char *iss = NULL, *sub = NULL, *expectSub = NULL;
  char *sigStr = NULL;
  unsigned char *sig = NULL;
  size_t siglen = 0;
  unsigned char pub[DOLTLITE_PUBKEY_LEN];
  long exp = 0, iat = 0, nbf = 0;
  int hasIat = 0, hasNbf = 0;
  int rc = 1;

  if (kidOut) *kidOut = NULL;
  if (!authValue) return 1;
  if (!expectedAudience || !expectedAudience[0]) return 1;
  if (now < 0) return 1;

  jwt = authValue;
  if (strncmp(jwt, "Bearer ", 7) == 0) jwt += 7;
  while (*jwt == ' ') jwt++;

  dot1 = strchr(jwt, '.');
  if (!dot1) return 1;
  dot2 = strchr(dot1 + 1, '.');
  if (!dot2) return 1;

  hdr = decodeSegZ(jwt, (size_t)(dot1 - jwt));
  claims = decodeSegZ(dot1 + 1, (size_t)(dot2 - (dot1 + 1)));
  if (!hdr || !claims) goto done;

  alg = jsonFindString(hdr, "alg");
  ver = jsonFindString(hdr, "dolt_token_version");
  kid = jsonFindString(hdr, "kid");
  if (!alg || strcmp(alg, "EdDSA") != 0) goto done;
  if (!ver || strcmp(ver, JWT_TOKEN_VERSION) != 0) goto done;
  if (!kid) goto done;

  if (doltliteCredsLoadPubKey(authKeysDir, kid, pub) != 0) goto done;

  sigStr = (char *)sqlite3_malloc(strlen(dot2 + 1) + 1);
  if (!sigStr) goto done;
  strcpy(sigStr, dot2 + 1);
  if (doltliteBase64UrlDecode(sigStr, &sig, &siglen)) goto done;
  if (siglen != DOLTLITE_SIG_LEN) goto done;
  if (ed25519_verify(sig, (const unsigned char *)jwt, (size_t)(dot2 - jwt), pub) != 1) {
    goto done;
  }

  iss = jsonFindString(claims, "iss");
  sub = jsonFindString(claims, "sub");
  if (!iss || strcmp(iss, JWT_ISSUER) != 0) goto done;

  expectSub = (char *)sqlite3_malloc(strlen(JWT_SUBJECT_PREFIX) + strlen(kid) + 1);
  if (!expectSub) goto done;
  strcpy(expectSub, JWT_SUBJECT_PREFIX);
  strcat(expectSub, kid);
  if (!sub || strcmp(sub, expectSub) != 0) goto done;

  if (!jsonAudienceMatches(claims, expectedAudience)) goto done;

  if (!jsonFindLong(claims, "exp", &exp)) goto done;
  if (!jsonFindOptionalLong(claims, "iat", &iat, &hasIat)) goto done;
  if (!jsonFindOptionalLong(claims, "nbf", &nbf, &hasNbf)) goto done;
  if (now > exp && now - exp > JWT_CLOCK_SKEW_SECONDS) goto done;
  if (exp > now && exp - now > JWT_TTL_SECONDS + JWT_CLOCK_SKEW_SECONDS) {
    goto done;
  }
  if (hasIat) {
    if (iat > now && iat - now > JWT_CLOCK_SKEW_SECONDS) goto done;
    if (exp < iat || exp - iat > JWT_TTL_SECONDS) goto done;
  }
  if (hasNbf) {
    if (nbf > now && nbf - now > JWT_CLOCK_SKEW_SECONDS) goto done;
    if (nbf > exp) goto done;
  }

  rc = 0;
  if (kidOut) {
    *kidOut = kid;
    kid = NULL;
  }

done:
  sqlite3_free(hdr);
  sqlite3_free(claims);
  sqlite3_free(alg);
  sqlite3_free(ver);
  sqlite3_free(kid);
  sqlite3_free(iss);
  sqlite3_free(sub);
  sqlite3_free(expectSub);
  sqlite3_free(sigStr);
  sqlite3_free(sig);
  return rc;
}

typedef struct CredsSetting CredsSetting;
struct CredsSetting {
  char *key;
  char *value;
  CredsSetting *next;
};

static void credsSettingsFree(CredsSetting *p) {
  while (p) {
    CredsSetting *next = p->next;
    sqlite3_free(p->key);
    sqlite3_free(p->value);
    sqlite3_free(p);
    p = next;
  }
}

/* Keys this build does not know are kept and written back, so an older
** build neither rejects nor drops a newer build's settings. With repair set,
** malformed lines are dropped instead of failing the read. */
static int credsSettingsRead(const char *path, CredsSetting **out, int repair) {
  FILE *f = fopen(path, "rb");
  char line[1024];
  CredsSetting **tail = out;
  *out = NULL;
  if (!f) return errno == ENOENT ? 0 : 1;
  while (fgets(line, sizeof(line), f)) {
    size_t n = strlen(line);
    char *eq = NULL;
    int bad = 0;
    CredsSetting *p, *prev;
    if (n && line[n-1] == '\n') {
      line[--n] = 0;
    } else if (!feof(f)) {
      int c;
      while ((c = fgetc(f)) != EOF && c != '\n') {}
      bad = 1;
    }
    if (n && line[n-1] == '\r') line[--n] = 0;
    if (!bad && !n) continue;
    if (!bad) eq = strchr(line, '=');
    if (!eq || eq == line) {
      bad = 1;
    } else {
      *eq++ = 0;
      if (!strcmp(line, "user.creds") || !strncmp(line, "remote.", 7)) {
        if (strcmp(line, "user.creds") && !credsKidValid(line + 7)) bad = 1;
        if (strcmp(eq, "anonymous") && !credsKidValid(eq)) bad = 1;
        for (prev = *out; prev && !bad; prev = prev->next) {
          if (!strcmp(prev->key, line)) bad = 1;
        }
      }
    }
    if (bad) {
      if (repair) continue;
      goto bad;
    }
    p = sqlite3_malloc(sizeof(*p));
    if (!p) goto bad;
    memset(p, 0, sizeof(*p));
    *tail = p;
    tail = &p->next;
    p->key = dlStrDup(line);
    p->value = dlStrDup(eq);
    if (!p->key || !p->value) goto bad;
  }
  if (ferror(f)) goto bad;
  fclose(f);
  return 0;
bad:
  fclose(f);
  credsSettingsFree(*out);
  *out = NULL;
  return 1;
}

static char *credsConfigPath(const char *dir, const char *suffix) {
  size_t n = strlen(dir) + strlen(suffix) + 9;
  char *path = sqlite3_malloc(n);
  if (path) snprintf(path, n, "%s/config%s", dir, suffix);
  return path;
}

int doltliteCredsConfigSet(
  const char *dir, const char *key, const char *value, int onlyIfMissing
) {
  char *owned = NULL, *path = NULL, *lockPath = NULL, *tmp = NULL;
  CredsSetting *settings = NULL, *p;
  FILE *f = NULL;
  int rc = 1;
#ifdef _WIN32
  HANDLE lock = INVALID_HANDLE_VALUE;
  OVERLAPPED ov = {0};
#else
  int lock = -1;
#endif
  if (strcmp(key, "user.creds") &&
      (strncmp(key, "remote.", 7) || !credsKidValid(key+7))) return 1;
  if (value && strcmp(value, "anonymous") && !credsKidValid(value)) return 1;
  if (!dir) dir = owned = doltliteCredsDir();
  if (!dir || mkdirp(dir)) goto done;
  path = credsConfigPath(dir, "");
  tmp = credsConfigPath(dir, ".tmp");
  lockPath = credsConfigPath(dir, ".lock");
  if (!path || !tmp || !lockPath) goto done;
#ifdef _WIN32
  lock = CreateFileA(lockPath, GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS,
                    FILE_ATTRIBUTE_NORMAL, NULL);
  if (lock == INVALID_HANDLE_VALUE ||
      !LockFileEx(lock, LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ov)) goto done;
#else
  lock = open(lockPath, O_RDWR | O_CREAT, 0600);
  if (lock < 0) goto done;
  while (flock(lock, LOCK_EX)) {
    if (errno != EINTR) goto done;
  }
#endif
  if (credsSettingsRead(path, &settings, 1)) goto done;
  for (p = settings; p; p = p->next) {
    if (!strcmp(p->key, key)) break;
  }
  if (p && onlyIfMissing) { rc = 0; goto done; }
  if (!p) {
    p = sqlite3_malloc(sizeof(*p));
    if (!p) goto done;
    memset(p, 0, sizeof(*p));
    p->next = settings;
    settings = p;
    p->key = dlStrDup(key);
    if (!p->key) goto done;
  }
  sqlite3_free(p->value);
  p->value = value ? dlStrDup(value) : NULL;
  if (value && !p->value) goto done;
#ifdef _WIN32
  f = fopen(tmp, "wb");
#else
  {
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) goto done;
    fchmod(fd, 0600);
    f = fdopen(fd, "wb");
    if (!f) close(fd);
  }
#endif
  if (!f) goto done;
  for (p = settings; p; p = p->next) {
    if (p->value && fprintf(f, "%s=%s\n", p->key, p->value) < 0) goto done;
  }
  if (fflush(f)) goto done;
#ifndef _WIN32
  if (fsync(fileno(f))) goto done;
#endif
  if (fclose(f)) { f = NULL; goto done; }
  f = NULL;
#ifdef _WIN32
  if (!MoveFileExA(tmp, path, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
    goto done;
#else
  if (rename(tmp, path)) goto done;
#endif
  rc = 0;
done:
  if (f) fclose(f);
#ifdef _WIN32
  if (lock != INVALID_HANDLE_VALUE) {
    UnlockFileEx(lock, 0, 1, 0, &ov);
    CloseHandle(lock);
  }
#else
  if (lock >= 0) close(lock);
#endif
  credsSettingsFree(settings);
  sqlite3_free(owned);
  sqlite3_free(path);
  sqlite3_free(tmp);
  sqlite3_free(lockPath);
  return rc;
}

char *doltliteCredsRemoteKey(const char *url) {
  char *canonical = dlStrDup(url);
  char *end, *p, *key, *hash;
  unsigned char digest[DOLTLITE_KID_RAW_LEN];
  if (!canonical) return NULL;
  end = strstr(canonical, "://");
  if (!end) { sqlite3_free(canonical); return NULL; }
  end = strchr(end + 3, '/');
  if (!end) end = canonical + strlen(canonical);
  for (p = canonical; p < end; p++) {
    if (*p >= 'A' && *p <= 'Z') *p += 'a' - 'A';
  }
  if (end-canonical >= 4 && !strncmp(canonical, "https://", 8) &&
      !memcmp(end-4, ":443", 4)) memmove(end-4, end, strlen(end)+1);
  p = canonical + strlen(canonical);
  while (p > canonical && p[-1] == '/') *--p = 0;
  doltliteSha512_224((const unsigned char*)canonical, strlen(canonical), digest);
  sqlite3_free(canonical);
  hash = doltliteBase32Encode(digest, sizeof(digest));
  if (!hash) return NULL;
  key = sqlite3_malloc(strlen(hash)+8);
  if (key) sprintf(key, "remote.%s", hash);
  sqlite3_free(hash);
  return key;
}

static char *credsResolveKid(const char *name) {
  unsigned char pub[32], digest[DOLTLITE_KID_RAW_LEN];
  uint32_t bits = 0;
  int nbit = 0, n = 0;
  size_t i;
  if (!name) return NULL;
  if (credsKidValid(name)) return dlStrDup(name);
  if (strlen(name) != 52) return NULL;
  for (i = 0; i < 52; i++) {
    const char *p = strchr(B32_ALPHABET, name[i]);
    if (!p) return NULL;
    bits = (bits << 5) | (uint32_t)(p - B32_ALPHABET);
    nbit += 5;
    if (nbit >= 8) {
      nbit -= 8;
      if (n >= 32) return NULL;
      pub[n++] = (unsigned char)(bits >> nbit);
    }
  }
  if (n != 32 || (bits & 15)) return NULL;
  doltliteSha512_224(pub, sizeof(pub), digest);
  return doltliteBase32Encode(digest, sizeof(digest));
}

int doltliteCredsLoadNamed(const char *dir, const char *name, DoltliteCreds **out) {
  char *kid = credsResolveKid(name);
  int rc;
  *out = NULL;
  rc = kid ? doltliteCredsLoad(dir, kid, out) : 1;
  sqlite3_free(kid);
  return rc;
}

int doltliteCredsImport(const char *path, DoltliteCreds **out) {
  char json[4097];
  FILE *f = fopen(path, "rb");
  size_t n;
  int rc;
  *out = NULL;
  if (!f) return 1;
  n = fread(json, 1, sizeof(json)-1, f);
  rc = ferror(f) || !feof(f) || memchr(json, 0, n) != NULL;
  fclose(f);
  if (rc) return 1;
  json[n] = 0;
  rc = doltliteCredsFromJwk(json, out);
  memset(json, 0, sizeof(json));
  return rc;
}

int doltliteCredsResolve(
  const char *dir, const char *override, const char *url,
  DoltliteCreds **out, char **source, char **error
) {
  char *selected = NULL, *key = NULL;
  CredsSetting *settings = NULL, *setting;
  char **kids = NULL;
  const char *why = NULL;
  int n = 0, rc = DOLTLITE_CREDS_ERROR;
  *out = NULL;
  *error = NULL;
  if (source) *source = NULL;
  if (override && *override) {
    selected = dlStrDup(override);
    why = "connection";
    if (!selected) goto oom;
  } else {
    const char *env = getenv("DOLTLITE_CREDS_KID");
    if (env && *env) {
      selected = dlStrDup(env);
      why = "DOLTLITE_CREDS_KID";
      if (!selected) goto oom;
    }
  }
  if (!selected) {
    char *ownedDir = dir ? NULL : doltliteCredsDir();
    char *path = dir || ownedDir ? credsConfigPath(dir ? dir : ownedDir, "") : NULL;
    if (!path) { sqlite3_free(ownedDir); goto oom; }
    if (credsSettingsRead(path, &settings, 0)) {
      char message[1200];
      snprintf(message, sizeof(message),
          "cannot read credential configuration %s; dolt_creds_use() rewrites it",
          path);
      *error = dlStrDup(message);
      sqlite3_free(path);
      sqlite3_free(ownedDir);
      if (!*error) goto oom;
      goto done;
    }
    sqlite3_free(path);
    sqlite3_free(ownedDir);
    if (url) {
      key = doltliteCredsRemoteKey(url);
      if (!key) goto oom;
      for (setting = settings; setting; setting = setting->next) {
        if (!strcmp(setting->key, key)) {
          selected = dlStrDup(setting->value);
          if (!selected) goto oom;
          why = "remote";
          break;
        }
      }
    }
    if (!selected) {
      for (setting = settings; setting; setting = setting->next) {
        if (!strcmp(setting->key, "user.creds")) {
          selected = dlStrDup(setting->value);
          if (!selected) goto oom;
          why = "default";
          break;
        }
      }
    }
  }
  if (!selected) {
    if (doltliteCredsList(dir, &kids, &n)) {
      *error = dlStrDup("cannot read credential directory");
      goto done;
    }
    if (n > 1) {
      *error = dlStrDup("multiple credentials found; select one with dolt_creds_use()");
      goto done;
    }
    if (!n) { rc = DOLTLITE_CREDS_NONE; goto done; }
    selected = dlStrDup(kids[0]);
    why = "single key";
    if (!selected) goto oom;
  }
  if (!strcmp(selected, "anonymous")) {
    rc = DOLTLITE_CREDS_ANONYMOUS;
  } else if (doltliteCredsLoadNamed(dir, selected, out)) {
    char message[256];
    snprintf(message, sizeof(message),
        "%s selected credential is missing, unreadable or invalid: %.52s; use dolt_creds_ls() and dolt_creds_use()",
        why, selected);
    *error = dlStrDup(message);
  } else {
    rc = DOLTLITE_CREDS_SELECTED;
  }
  if (source) {
    *source = dlStrDup(why);
    if (!*source) goto oom;
  }
  goto done;
oom:
  sqlite3_free(*error);
  *error = dlStrDup("out of memory resolving credentials");
  rc = DOLTLITE_CREDS_ERROR;
done:
  credsSettingsFree(settings);
  sqlite3_free(selected);
  sqlite3_free(key);
  doltliteCredsFreeList(kids, n);
  return rc;
}
