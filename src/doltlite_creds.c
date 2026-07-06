/*
** doltlite remote credentials — see doltlite_creds.h.
**
** Depends on the vendored ed25519 (ext/ed25519): ed25519_create_keypair /
** ed25519_sign, and the sha512 streaming context (reused, with an overridden
** IV, to compute SHA-512/224 for the credential id).
*/

#include "doltlite_creds.h"

#include "ed25519.h"
#include "sha512.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct DoltliteCreds {
  unsigned char seed[DOLTLITE_SEED_LEN];
  unsigned char pub[DOLTLITE_PUBKEY_LEN];
  unsigned char expanded[64]; /* orlp ed25519 "private_key" (sha512(seed), clamped) */
};

/* ------------------------------------------------------------------ */
/* SHA-512/224                                                         */
/* ------------------------------------------------------------------ */

/* FIPS 180-4 SHA-512/224 initial hash values. */
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

  /* SHA-512 and SHA-512/224 share padding and the compression function; only
  ** the IV and the truncation differ. Init sets the SHA-512 IV and zeroes the
  ** counters; we then swap in the /224 IV before absorbing any data. */
  sha512_init(&ctx);
  for (i = 0; i < 8; i++) ctx.state[i] = SHA512_224_IV[i];

  sha512_update(&ctx, in, inlen);
  sha512_final(&ctx, full);
  memcpy(out, full, DOLTLITE_KID_RAW_LEN);
}

/* ------------------------------------------------------------------ */
/* base32 (dolt alphabet, no padding)                                  */
/* ------------------------------------------------------------------ */

static const char B32_ALPHABET[] = "0123456789abcdefghijklmnopqrstuv";

char *doltliteBase32Encode(const unsigned char *in, size_t inlen) {
  size_t outlen = (inlen * 8 + 4) / 5; /* ceil(inlen*8/5) */
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

/* ------------------------------------------------------------------ */
/* base64url (matches Go base64.URLEncoding: '-'/'_' alphabet, '=' pad) */
/* ------------------------------------------------------------------ */

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

  /* Accept both padded (JWK) and unpadded (JWS) base64url. A remainder of one
  ** base64 char is never valid. */
  if (inlen % 4 == 1) return 1;
  cap = inlen / 4 * 3 + 3;
  buf = (unsigned char *)malloc(cap ? cap : 1);
  if (!buf) return 1;

  for (i = 0; i < inlen; i++) {
    char c = in[i];
    int v;
    if (c == '=') break; /* padding: rest must be padding */
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

/* ------------------------------------------------------------------ */
/* randomness                                                          */
/* ------------------------------------------------------------------ */

static int randomBytes(unsigned char *p, size_t n) {
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
}

/* ------------------------------------------------------------------ */
/* credential lifecycle                                                */
/* ------------------------------------------------------------------ */

int doltliteCredsFromSeed(const unsigned char seed[DOLTLITE_SEED_LEN],
                          DoltliteCreds **out) {
  DoltliteCreds *c = (DoltliteCreds *)calloc(1, sizeof(*c));
  if (!c) return 1;
  memcpy(c->seed, seed, DOLTLITE_SEED_LEN);
  /* orlp: private_key = clamped sha512(seed); public_key derived from it. */
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

/* ------------------------------------------------------------------ */
/* bearer JWT                                                          */
/* ------------------------------------------------------------------ */

#define JWT_ISSUER        "dolt-client.dolthub.com"
#define JWT_SUBJECT_PREFIX "doltClientCredentials/"
#define JWT_TOKEN_VERSION "2023.01"
#define JWT_TTL_SECONDS   30

/* base64url without padding, as required by JWS (RFC 7515). */
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

  /* Header and claims. kid is base32 and audience is a hostname, so neither
  ** needs JSON string escaping. */
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

  /* Signing input is "<b64url header>.<b64url claims>". */
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

/* ------------------------------------------------------------------ */
/* JWK                                                                 */
/* ------------------------------------------------------------------ */

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

/* Extract the string value of a top-level "key":"value" pair. Values produced
** by doltliteCredsToJwk are base64url and contain no JSON escapes, so a plain
** scan between the quotes is sufficient. Returns malloc'd value or NULL. */
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
    p = kstart; /* advance past this quote and keep scanning */
  }
  return NULL;
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

/* ------------------------------------------------------------------ */
/* file store                                                          */
/* ------------------------------------------------------------------ */

char *doltliteCredsDir(void) {
  const char *override = getenv("DOLTLITE_CREDS_DIR");
  const char *home;
  char *dir;
  if (override && *override) {
    return strdup(override);
  }
  home = getenv("HOME");
  if (!home || !*home) return NULL;
  {
    size_t n = strlen(home) + strlen("/.doltlite/creds") + 1;
    dir = (char *)malloc(n);
    if (!dir) return NULL;
    snprintf(dir, n, "%s/.doltlite/creds", home);
  }
  return dir;
}

/* mkdir -p for a single path (each intermediate component). */
static int mkdirp(const char *path) {
  char *tmp = strdup(path);
  size_t len, i;
  if (!tmp) return 1;
  len = strlen(tmp);
  for (i = 1; i < len; i++) {
    if (tmp[i] == '/') {
      tmp[i] = '\0';
      if (mkdir(tmp, 0700) != 0 && errno != EEXIST) {
        free(tmp);
        return 1;
      }
      tmp[i] = '/';
    }
  }
  if (mkdir(tmp, 0700) != 0 && errno != EEXIST) {
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
  int fd = -1, rc = 1;

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

  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) goto done;
  /* Enforce 0600 even if the file already existed with looser perms. */
  (void)fchmod(fd, 0600);
  {
    size_t len = strlen(json), off = 0;
    while (off < len) {
      ssize_t w = write(fd, json + off, len - off);
      if (w <= 0) {
        if (w < 0 && errno == EINTR) continue;
        goto done;
      }
      off += (size_t)w;
    }
  }
  rc = 0;

done:
  if (fd >= 0) close(fd);
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

int doltliteCredsLoadDefault(const char *dir, DoltliteCreds **out) {
  char *owned = NULL;
  DIR *d;
  struct dirent *e;
  char *kid = NULL;
  int count = 0, rc = 1;

  if (!dir) {
    owned = doltliteCredsDir();
    dir = owned;
  }
  if (!dir) return 1;
  d = opendir(dir);
  if (!d) {
    free(owned);
    return 1;
  }
  while ((e = readdir(d)) != NULL) {
    size_t n = strlen(e->d_name);
    if (n > 4 && strcmp(e->d_name + n - 4, ".jwk") == 0) {
      count++;
      if (count == 1) {
        kid = (char *)malloc(n - 4 + 1);
        if (kid) {
          memcpy(kid, e->d_name, n - 4);
          kid[n - 4] = '\0';
        }
      }
    }
  }
  closedir(d);

  /* Only auto-select when there is exactly one key; otherwise the caller must
  ** name a kid (e.g. via $DOLTLITE_CREDS_KID). */
  if (count == 1 && kid) {
    rc = doltliteCredsLoad(dir, kid, out);
  }
  free(kid);
  free(owned);
  return rc;
}
