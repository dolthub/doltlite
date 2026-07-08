#include "doltlite_creds.h"

#include "ed25519.h"
#include "sha512.h"

#include <errno.h>
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
#include <unistd.h>
#endif

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

char *doltliteBase32Encode(const unsigned char *in, size_t inlen) {
  size_t outlen = (inlen * 8 + 4) / 5;
  char *out = (char *)malloc(outlen + 1);
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
  char *out = (char *)malloc(outlen + 1);
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
  buf = (unsigned char *)malloc(cap ? cap : 1);
  if (!buf) return 1;

  for (i = 0; i < inlen; i++) {
    char c = in[i];
    int v;
    if (c == '=') break;
    v = b64urlVal(c);
    if (v < 0) {
      free(buf);
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
  DoltliteCreds *c = (DoltliteCreds *)calloc(1, sizeof(*c));
  if (!c) return 1;
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
  free(c);
}

const unsigned char *doltliteCredsPubKey(const DoltliteCreds *c) { return c->pub; }
const unsigned char *doltliteCredsSeed(const DoltliteCreds *c) { return c->seed; }

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

  kid = doltliteCredsKid(c);
  if (!kid) goto done;

  hn = strlen(kid) + 96;
  header = (char *)malloc(hn);
  if (!header) goto done;
  snprintf(header, hn,
           "{\"alg\":\"EdDSA\",\"kid\":\"%s\",\"dolt_token_version\":\"%s\"}",
           kid, JWT_TOKEN_VERSION);

  cn = strlen(kid) + strlen(audience) + 160;
  claims = (char *)malloc(cn);
  if (!claims) goto done;
  snprintf(claims, cn,
           "{\"iss\":\"%s\",\"sub\":\"%s%s\",\"aud\":\"%s\",\"iat\":%ld,\"exp\":%ld}",
           JWT_ISSUER, JWT_SUBJECT_PREFIX, kid, audience, iat,
           iat + JWT_TTL_SECONDS);

  h64 = b64urlRaw((const unsigned char *)header, strlen(header));
  c64 = b64urlRaw((const unsigned char *)claims, strlen(claims));
  if (!h64 || !c64) goto done;

  need = strlen(h64) + 1 + strlen(c64) + 1;
  input = (char *)malloc(need);
  if (!input) goto done;
  snprintf(input, need, "%s.%s", h64, c64);

  doltliteCredsSign(c, (const unsigned char *)input, strlen(input), sig);
  sig64 = b64urlRaw(sig, sizeof(sig));
  if (!sig64) goto done;

  need = strlen(input) + 1 + strlen(sig64) + 1;
  token = (char *)malloc(need);
  if (!token) goto done;
  snprintf(token, need, "%s.%s", input, sig64);

  *jwtOut = token;
  token = NULL;
  rc = 0;

done:
  free(kid);
  free(header);
  free(claims);
  free(h64);
  free(c64);
  free(sig64);
  free(input);
  free(token);
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
    json = (char *)malloc(n);
    if (json) {
      snprintf(json, n,
               "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"x\":\"%s\",\"d\":\"%s\"}",
               x, d);
    }
  }
  free(x);
  free(d);
  return json;
}

static char *jsonFindString(const char *json, const char *key) {
  size_t keylen = strlen(key);
  const char *p = json;
  while ((p = strchr(p, '"')) != NULL) {
    const char *kstart = p + 1;
    if (strncmp(kstart, key, keylen) == 0 && kstart[keylen] == '"') {
      const char *q = kstart + keylen + 1;
      const char *vend;
      while (*q == ' ' || *q == ':' || *q == '\t') q++;
      if (*q != '"') return NULL;
      q++;
      vend = strchr(q, '"');
      if (!vend) return NULL;
      {
        size_t vlen = (size_t)(vend - q);
        char *v = (char *)malloc(vlen + 1);
        if (!v) return NULL;
        memcpy(v, q, vlen);
        v[vlen] = '\0';
        return v;
      }
    }
    p = kstart;
  }
  return NULL;
}

static int jsonFindLong(const char *json, const char *key, long *out) {
  size_t keylen = strlen(key);
  const char *p = json;
  while ((p = strchr(p, '"')) != NULL) {
    const char *kstart = p + 1;
    if (strncmp(kstart, key, keylen) == 0 && kstart[keylen] == '"') {
      const char *q = kstart + keylen + 1;
      char *end;
      long v;
      while (*q == ' ' || *q == ':' || *q == '\t') q++;
      v = strtol(q, &end, 10);
      if (end == q) return 0;
      *out = v;
      return 1;
    }
    p = kstart;
  }
  return 0;
}

static char *decodeSegZ(const char *seg, size_t seglen) {
  char *z = (char *)malloc(seglen + 1);
  unsigned char *raw = NULL;
  size_t rawlen = 0;
  char *s = NULL;
  if (!z) return NULL;
  memcpy(z, seg, seglen);
  z[seglen] = '\0';
  if (doltliteBase64UrlDecode(z, &raw, &rawlen) == 0) {
    s = (char *)malloc(rawlen + 1);
    if (s) {
      memcpy(s, raw, rawlen);
      s[rawlen] = '\0';
    }
  }
  free(z);
  free(raw);
  return s;
}

int doltliteCredsFromJwk(const char *json, DoltliteCreds **out) {
  char *dstr = jsonFindString(json, "d");
  unsigned char *seed = NULL;
  size_t seedlen = 0;
  int rc = 1;

  if (!dstr) goto done;
  if (doltliteBase64UrlDecode(dstr, &seed, &seedlen)) goto done;
  if (seedlen != DOLTLITE_SEED_LEN) goto done;
  rc = doltliteCredsFromSeed(seed, out);

done:
  if (seed) {
    memset(seed, 0, seedlen);
    free(seed);
  }
  free(dstr);
  return rc;
}

char *doltliteCredsDir(void) {
  const char *override = getenv("DOLTLITE_CREDS_DIR");
  const char *home;
  char *dir;
  if (override && *override) {
    return strdup(override);
  }
  home = getenv("HOME");
#ifdef _WIN32
  if (!home || !*home) home = getenv("USERPROFILE");
  if (!home || !*home) home = getenv("APPDATA");
#endif
  if (!home || !*home) return NULL;
  {
    size_t n = strlen(home) + strlen("/.doltlite/creds") + 1;
    dir = (char *)malloc(n);
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
  char *tmp = strdup(path);
  size_t len, i;
  if (!tmp) return 1;
  len = strlen(tmp);
  for (i = 1; i < len; i++) {
    if (tmp[i] == '/' || tmp[i] == '\\') {
      char sep = tmp[i];
      tmp[i] = '\0';
      /* Skip a Windows drive prefix like "C:" which cannot be created. */
      if (tmp[i - 1] != ':' && makeDir(tmp) != 0 && errno != EEXIST) {
        free(tmp);
        return 1;
      }
      tmp[i] = sep;
    }
  }
  if (makeDir(tmp) != 0 && errno != EEXIST) {
    free(tmp);
    return 1;
  }
  free(tmp);
  return 0;
}

static char *credsFilePath(const char *dir, const char *kid) {
  size_t n = strlen(dir) + 1 + strlen(kid) + strlen(".jwk") + 1;
  char *path = (char *)malloc(n);
  if (!path) return NULL;
  snprintf(path, n, "%s/%s.jwk", dir, kid);
  return path;
}

int doltliteCredsSave(const DoltliteCreds *c, const char *dir) {
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
  json = doltliteCredsToJwk(c);
  if (!json) goto done;

  {
    size_t len = strlen(json);
#ifdef _WIN32
    FILE *f = fopen(path, "wb");
    if (!f) goto done;
    if (fwrite(json, 1, len, f) != len) {
      fclose(f);
      goto done;
    }
    if (fclose(f) != 0) goto done;
#else
    size_t off = 0;
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) goto done;
    (void)fchmod(fd, 0600);
    while (off < len) {
      ssize_t w = write(fd, json + off, len - off);
      if (w <= 0) {
        if (w < 0 && errno == EINTR) continue;
        close(fd);
        goto done;
      }
      off += (size_t)w;
    }
    close(fd);
#endif
  }
  rc = 0;

done:
  free(json);
  free(path);
  free(kid);
  free(owned);
  return rc;
}

int doltliteCredsLoad(const char *dir, const char *kid, DoltliteCreds **out) {
  char *owned = NULL;
  char *path = NULL;
  FILE *f = NULL;
  char *json = NULL;
  long sz;
  int rc = 1;

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
  json = (char *)malloc((size_t)sz + 1);
  if (!json) goto done;
  if (fread(json, 1, (size_t)sz, f) != (size_t)sz) goto done;
  json[sz] = '\0';

  rc = doltliteCredsFromJwk(json, out);

done:
  if (f) fclose(f);
  if (json) {
    memset(json, 0, json ? strlen(json) : 0);
    free(json);
  }
  free(path);
  free(owned);
  return rc;
}

/* Minimal cross-platform directory iteration over the creds dir. */
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
  char *pat = (char *)malloc(n);
  if (!pat) return 1;
  snprintf(pat, n, "%s\\*", dir);
  it->h = FindFirstFileA(pat, &it->fd);
  free(pat);
  if (it->h == INVALID_HANDLE_VALUE) return 1;
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
  char *owned = NULL;
  DirIter it;
  const char *name;
  char *kid = NULL;
  int count = 0, rc = 1;

  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) return 1;
  if (dirOpen(&it, dir)) {
    free(owned);
    return 1;
  }
  while ((name = dirNext(&it)) != NULL) {
    size_t n = strlen(name);
    if (n > 4 && strcmp(name + n - 4, ".jwk") == 0) {
      count++;
      if (count == 1) {
        kid = (char *)malloc(n - 4 + 1);
        if (kid) {
          memcpy(kid, name, n - 4);
          kid[n - 4] = '\0';
        }
      }
    }
  }
  dirClose(&it);

  if (count == 1 && kid) {
    rc = doltliteCredsLoad(dir, kid, out);
  }
  free(kid);
  free(owned);
  return rc;
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
    free(owned);
    return 0;
  }
  while ((name = dirNext(&it)) != NULL) {
    size_t ln = strlen(name);
    char *kid;
    if (!(ln > 4 && strcmp(name + ln - 4, ".jwk") == 0)) continue;
    if (cnt == cap) {
      int nc = cap ? cap * 2 : 8;
      char **na = (char **)realloc(arr, (size_t)nc * sizeof(char *));
      if (!na) goto oom;
      arr = na;
      cap = nc;
    }
    kid = (char *)malloc(ln - 4 + 1);
    if (!kid) goto oom;
    memcpy(kid, name, ln - 4);
    kid[ln - 4] = '\0';
    arr[cnt++] = kid;
  }
  dirClose(&it);
  free(owned);
  *out = arr;
  *n = cnt;
  return 0;

oom:
  dirClose(&it);
  free(owned);
  doltliteCredsFreeList(arr, cnt);
  return 1;
}

void doltliteCredsFreeList(char **list, int n) {
  int i;
  if (!list) return;
  for (i = 0; i < n; i++) free(list[i]);
  free(list);
}

int doltliteCredsRemove(const char *dir, const char *kid) {
  char *owned = NULL;
  char *path;
  int rc;
  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) return 1;
  path = credsFilePath(dir, kid);
  if (!path) {
    free(owned);
    return 1;
  }
  rc = (remove(path) == 0) ? 0 : 1;
  free(path);
  free(owned);
  return rc;
}

int doltliteCredsLoadPubKey(const char *dir, const char *kid,
                            unsigned char pub[DOLTLITE_PUBKEY_LEN]) {
  char *owned = NULL;
  char *path = NULL;
  FILE *f = NULL;
  char *json = NULL;
  char *xstr = NULL;
  unsigned char *raw = NULL;
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
  json = (char *)malloc((size_t)sz + 1);
  if (!json) goto done;
  if (fread(json, 1, (size_t)sz, f) != (size_t)sz) goto done;
  json[sz] = '\0';

  xstr = jsonFindString(json, "x");
  if (!xstr) goto done;
  if (doltliteBase64UrlDecode(xstr, &raw, &rawlen)) goto done;
  if (rawlen != DOLTLITE_PUBKEY_LEN) goto done;
  memcpy(pub, raw, DOLTLITE_PUBKEY_LEN);
  rc = 0;

done:
  if (f) fclose(f);
  free(json);
  free(path);
  free(owned);
  free(xstr);
  free(raw);
  return rc;
}

#define JWT_ISSUER_EXPECTED   "dolt-client.dolthub.com"
#define JWT_SUBJECT_PREFIX_V  "doltClientCredentials/"
#define JWT_TOKEN_VERSION_V   "2023.01"

int doltliteCredsVerifyBearer(const char *authValue, const char *expectedAudience,
                              const char *authKeysDir, long now, char **kidOut) {
  const char *jwt;
  const char *dot1, *dot2;
  char *hdr = NULL, *claims = NULL;
  char *alg = NULL, *ver = NULL, *kid = NULL;
  char *iss = NULL, *sub = NULL, *aud = NULL, *expectSub = NULL;
  char *sigStr = NULL;
  unsigned char *sig = NULL;
  size_t siglen = 0;
  unsigned char pub[DOLTLITE_PUBKEY_LEN];
  long exp = 0;
  int rc = 1;

  if (kidOut) *kidOut = NULL;
  if (!authValue) return 1;

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
  if (!ver || strcmp(ver, JWT_TOKEN_VERSION_V) != 0) goto done;
  if (!kid) goto done;

  if (doltliteCredsLoadPubKey(authKeysDir, kid, pub) != 0) goto done;

  sigStr = (char *)malloc(strlen(dot2 + 1) + 1);
  if (!sigStr) goto done;
  strcpy(sigStr, dot2 + 1);
  if (doltliteBase64UrlDecode(sigStr, &sig, &siglen)) goto done;
  if (siglen != DOLTLITE_SIG_LEN) goto done;
  if (ed25519_verify(sig, (const unsigned char *)jwt, (size_t)(dot2 - jwt), pub) != 1) {
    goto done;
  }

  iss = jsonFindString(claims, "iss");
  sub = jsonFindString(claims, "sub");
  aud = jsonFindString(claims, "aud");
  if (!iss || strcmp(iss, JWT_ISSUER_EXPECTED) != 0) goto done;

  expectSub = (char *)malloc(strlen(JWT_SUBJECT_PREFIX_V) + strlen(kid) + 1);
  if (!expectSub) goto done;
  strcpy(expectSub, JWT_SUBJECT_PREFIX_V);
  strcat(expectSub, kid);
  if (!sub || strcmp(sub, expectSub) != 0) goto done;

  if (expectedAudience && *expectedAudience) {
    if (!aud || strcmp(aud, expectedAudience) != 0) goto done;
  }

  if (!jsonFindLong(claims, "exp", &exp)) goto done;
  if (now >= exp) goto done;

  rc = 0;
  if (kidOut) {
    *kidOut = kid;
    kid = NULL;
  }

done:
  free(hdr);
  free(claims);
  free(alg);
  free(ver);
  free(kid);
  free(iss);
  free(sub);
  free(aud);
  free(expectSub);
  free(sigStr);
  free(sig);
  return rc;
}
