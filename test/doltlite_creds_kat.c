
#include "doltlite_creds.h"
#include "ed25519.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void sqlite3_free(void*);
extern int sqlite3_alloc_stub_fail_after;
extern int sqlite3_alloc_stub_live;
extern void (*sqlite3_alloc_stub_on_alloc)(void *p, int n);
extern void (*sqlite3_alloc_stub_on_free)(const void *p);

static int watch_target_len = -1;
static const void *watch_ptr = NULL;
static int watch_saw_alloc = 0;
static int watch_saw_free = 0;
static int watch_all_zero = 1;

static void watchOnAlloc(void *p, int n) {
  if (n == watch_target_len && watch_ptr == NULL) {
    memset(p, 0xAA, (size_t)n);
    watch_ptr = p;
    watch_saw_alloc = 1;
  }
}

static void watchOnFree(const void *p) {
  if (p && p == watch_ptr) {
    const unsigned char *b = (const unsigned char *)p;
    int i;
    watch_saw_free = 1;
    for (i = 0; i < watch_target_len; i++) {
      if (b[i] != 0) {
        watch_all_zero = 0;
        break;
      }
    }
    watch_ptr = NULL;
  }
}

static void watchArm(int targetLen) {
  watch_target_len = targetLen;
  watch_ptr = NULL;
  watch_saw_alloc = 0;
  watch_saw_free = 0;
  watch_all_zero = 1;
  sqlite3_alloc_stub_on_alloc = watchOnAlloc;
  sqlite3_alloc_stub_on_free = watchOnFree;
}

static void watchDisarm(void) {
  sqlite3_alloc_stub_on_alloc = NULL;
  sqlite3_alloc_stub_on_free = NULL;
  watch_target_len = -1;
  watch_ptr = NULL;
}

static int writeBytes(const char *path, const void *data, size_t len) {
  FILE *f = fopen(path, "wb");
  int ok;
  if (!f) return 0;
  ok = (len == 0 || fwrite(data, 1, len, f) == len);
  if (fclose(f) != 0) ok = 0;
  return ok;
}

static char *dupEnv(const char *name) {
  const char *v = getenv(name);
  char *copy;
  if (!v) return NULL;
  copy = (char *)malloc(strlen(v) + 1);
  if (copy) strcpy(copy, v);
  return copy;
}

static void setEnvVar(const char *name, const char *val) {
#ifdef _WIN32
  _putenv_s(name, val ? val : "");
#else
  if (val) setenv(name, val, 1);
  else unsetenv(name);
#endif
}

static int failures = 0;

static void hexenc(const unsigned char *p, size_t n, char *out) {
  static const char h[] = "0123456789abcdef";
  size_t i;
  for (i = 0; i < n; i++) {
    out[i * 2] = h[p[i] >> 4];
    out[i * 2 + 1] = h[p[i] & 0xf];
  }
  out[n * 2] = '\0';
}

static void check_str(const char *name, const char *got, const char *want) {
  if (got && strcmp(got, want) == 0) {
    printf("  PASS  %s\n", name);
  } else {
    failures++;
    printf("  FAIL  %s\n        want: %s\n        got:  %s\n", name, want,
           got ? got : "(null)");
  }
}

static const char *PUB_HEX =
    "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
static const char *MSG = "doltlite auth";
static const char *SIG_HEX =
    "c653b359661fd6bb0bae9bb99cfabd0f3ac38324b2c7035e084bf31efd986a07"
    "39f35929ab4b66b624f3bcdb292ccf7fdb5bd01d68c741dcc718aefc33238900";
static const char *KID = "7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbars";
static const char *PUBB32 =
    "0eggffvjpo8bs7bgrkceeiu0j5ju9lhgjeigqnotri3684il66s0";
static const char *JWK_X = "A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=";
static const char *JWK_D = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

static void seedBytes(unsigned char out[32]) {
  int i;
  for (i = 0; i < 32; i++) out[i] = (unsigned char)i;
}

static void check_bool(const char *name, int cond) {
  if (cond) {
    printf("  PASS  %s\n", name);
  } else {
    failures++;
    printf("  FAIL  %s\n", name);
  }
}

static int fileExists(const char *path) {
  FILE *f = fopen(path, "rb");
  if (!f) return 0;
  fclose(f);
  return 1;
}

static char *decodeSegZ(const char *seg, size_t seglen, size_t *outlen) {
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
      if (outlen) *outlen = rawlen;
    }
  }
  free(z);
  sqlite3_free(raw);
  return s;
}

#define AUD "doltremoteapi.dolthub.com"
static const char *GOLDEN_JWT =
    "eyJhbGciOiJFZERTQSIsImtpZCI6IjdrdTFjZ2Q3dWprY3JpNXU0c21tcnNycGNwM2Vqc21n"
    "Yzl0N28zZGtkYmFycyIsImRvbHRfdG9rZW5fdmVyc2lvbiI6IjIwMjMuMDEifQ.eyJpc3Mi"
    "OiJkb2x0LWNsaWVudC5kb2x0aHViLmNvbSIsInN1YiI6ImRvbHRDbGllbnRDcmVkZW50aWFs"
    "cy83a3UxY2dkN3Vqa2NyaTV1NHNtbXJzcnBjcDNlanNtZ2M5dDdvM2RrZGJhcnMiLCJhdWQi"
    "OlsiZG9sdHJlbW90ZWFwaS5kb2x0aHViLmNvbSJdLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6"
    "MTcwMDAwMDAzMH0.BEmqWAF_yBLZu2GnOmri7caHyu2smBeyGrCH40GO8otnbV1-xokfy1l2"
    "jF5x2nVGino6X2_EhYBFtERt5CjGDQ";
static const char *EXP_HEADER =
    "{\"alg\":\"EdDSA\",\"kid\":\"7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbars\","
    "\"dolt_token_version\":\"2023.01\"}";
static const char *EXP_CLAIMS =
    "{\"iss\":\"dolt-client.dolthub.com\","
    "\"sub\":\"doltClientCredentials/7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbars\","
    "\"aud\":[\"doltremoteapi.dolthub.com\"],\"iat\":1700000000,\"exp\":1700000030}";

/* The seed and public key as the shipped JWK export publishes them. */
static int jwkBytes(const DoltliteCreds *c, const char *zField,
                    unsigned char out[32]) {
  char *json = doltliteCredsToJwk(c);
  char key[8];
  const char *p, *e;
  unsigned char *dec = NULL;
  size_t n = 0;
  int ok = 0;
  snprintf(key, sizeof(key), "\"%s\":\"", zField);
  if (json && (p = strstr(json, key)) != NULL) {
    char *val;
    p += strlen(key);
    e = strchr(p, '"');
    if (e && (val = (char *)malloc((size_t)(e - p) + 1)) != NULL) {
      memcpy(val, p, (size_t)(e - p));
      val[e - p] = 0;
      if (doltliteBase64UrlDecode(val, &dec, &n) == 0 && n == 32) {
        memcpy(out, dec, 32);
        ok = 1;
      }
      free(val);
    }
  }
  sqlite3_free(dec);
  sqlite3_free(json);
  return ok;
}

int main(int argc, char **argv) {
  char hexbuf[256];
  unsigned char seed[32];
  unsigned char pub[32], jwkSeed[32];
  DoltliteCreds *c = NULL, *c2 = NULL;

  {
    unsigned char d[DOLTLITE_KID_RAW_LEN];
    doltliteSha512_224((const unsigned char *)"", 0, d);
    hexenc(d, sizeof(d), hexbuf);
    check_str("sha512_224(\"\")", hexbuf,
              "6ed0dd02806fa89e25de060c19d3ac86cabb87d6a0ddd05c333b84f4");
    doltliteSha512_224((const unsigned char *)"abc", 3, d);
    hexenc(d, sizeof(d), hexbuf);
    check_str("sha512_224(\"abc\")", hexbuf,
              "4634270f707b6a54daae7530460842e20e37ed265ceee9a43e8924aa");
  }

  seedBytes(seed);
  if (doltliteCredsFromSeed(seed, &c) != 0) {
    printf("  FAIL  doltliteCredsFromSeed\n");
    return 1;
  }
  check_bool("JWK exports the public key", jwkBytes(c, "x", pub));
  check_bool("JWK exports the seed it was built from",
             jwkBytes(c, "d", jwkSeed) && memcmp(jwkSeed, seed, 32) == 0);
  hexenc(pub, 32, hexbuf);
  check_str("pubkey from seed", hexbuf, PUB_HEX);

  {
    char *kid = doltliteCredsKid(c);
    char *pubb32 = doltliteCredsPubKeyB32(c);
    check_str("kid = base32(sha512_224(pub))", kid, KID);
    check_str("pubkey base32 (approval URL fragment)", pubb32, PUBB32);
    sqlite3_free(kid);
    sqlite3_free(pubb32);
  }

  {
    unsigned char sig[DOLTLITE_SIG_LEN];
    doltliteCredsSign(c, (const unsigned char *)MSG, strlen(MSG), sig);
    hexenc(sig, sizeof(sig), hexbuf);
    check_str("ed25519 sign(\"doltlite auth\")", hexbuf, SIG_HEX);
    if (ed25519_verify(sig, (const unsigned char *)MSG, strlen(MSG),
                       pub) == 1) {
      printf("  PASS  ed25519 verify\n");
    } else {
      failures++;
      printf("  FAIL  ed25519 verify\n");
    }
  }

  {
    char *x = doltliteBase64UrlEncode(pub, 32);
    char *d = doltliteBase64UrlEncode(jwkSeed, 32);
    check_str("base64url(pub) == JWK x", x, JWK_X);
    check_str("base64url(seed) == JWK d", d, JWK_D);
    {
      unsigned char *dec = NULL;
      size_t dlen = 0;
      if (doltliteBase64UrlDecode(x, &dec, &dlen) == 0 && dlen == 32 &&
          memcmp(dec, pub, 32) == 0) {
        printf("  PASS  base64url decode round-trip\n");
      } else {
        failures++;
        printf("  FAIL  base64url decode round-trip\n");
      }
      sqlite3_free(dec);
    }
    sqlite3_free(x);
    sqlite3_free(d);
  }

  {
    static const struct {
      const char *in;
      const unsigned char want[3];
      size_t wantLen;
    } validCases[] = {
      { "",     { 0, 0, 0 }, 0 },
      { "AQ",   { 1, 0, 0 }, 1 },
      { "AQ==", { 1, 0, 0 }, 1 },
      { "AQI",  { 1, 2, 0 }, 2 },
      { "AQI=", { 1, 2, 0 }, 2 },
      { "AQID", { 1, 2, 3 }, 3 }
    };
    static const char *invalidCases[] = {
      "AAAA=invalid!!!",
      "AAAA=invalid!!!!",
      "AQ=invalid",
      "AQ=x",
      "AQ==extra",
      "AQ==extr",
      "AQ=",
      "AQI==",
      "=AAA",
      "A===",
      "====",
      "AAAA=",
      "AAAA====",
      "=",
      "==",
      "==="
    };
    size_t k;
    for (k = 0; k < sizeof(validCases) / sizeof(validCases[0]); k++) {
      unsigned char *dec = NULL;
      size_t dlen = 99;
      int rc = doltliteBase64UrlDecode(validCases[k].in, &dec, &dlen);
      char label[64];
      snprintf(label, sizeof(label), "base64url decode valid \"%s\"",
               validCases[k].in);
      check_bool(label,
                 rc == 0 && dlen == validCases[k].wantLen &&
                 (dlen == 0 || memcmp(dec, validCases[k].want, dlen) == 0));
      sqlite3_free(dec);
    }
    for (k = 0; k < sizeof(invalidCases) / sizeof(invalidCases[0]); k++) {
      unsigned char *dec = NULL;
      size_t dlen = 0;
      int rc = doltliteBase64UrlDecode(invalidCases[k], &dec, &dlen);
      char label[64];
      snprintf(label, sizeof(label), "base64url decode rejects \"%s\"",
               invalidCases[k]);
      check_bool(label, rc != 0 && dec == NULL);
      sqlite3_free(dec);
    }
  }

  {
    char *json = doltliteCredsToJwk(c);
    if (!json || doltliteCredsFromJwk(json, &c2) != 0) {
      failures++;
      printf("  FAIL  JWK round-trip (parse)\n");
    } else {
      char *k1 = doltliteCredsKid(c);
      char *k2 = doltliteCredsKid(c2);
      unsigned char seed2[32];
      if (k1 && k2 && strcmp(k1, k2) == 0 &&
          jwkBytes(c2, "d", seed2) && memcmp(jwkSeed, seed2, 32) == 0) {
        printf("  PASS  JWK round-trip\n");
      } else {
        failures++;
        printf("  FAIL  JWK round-trip (mismatch)\n");
      }
      sqlite3_free(k1);
      sqlite3_free(k2);
    }
    sqlite3_free(json);
  }

  {
    unsigned char privateKey[64];
    char json[512];
    char *x, *d, *kid;
    DoltliteCreds *imported = NULL;
    memcpy(privateKey, jwkSeed, 32);
    memcpy(privateKey+32, pub, 32);
    x = doltliteBase64UrlEncode(privateKey, 64);
    d = doltliteBase64UrlEncode(pub, 32);
    check_bool("allocate Dolt JWK", x && d);
    if (x && d) {
      snprintf(json, sizeof(json),
          "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"d\":\"%s\",\"x\":\"%s\"}", d, x);
      check_bool("Dolt private JWK imports", doltliteCredsFromJwk(json, &imported)==0);
      kid = imported ? doltliteCredsKid(imported) : NULL;
      check_str("Dolt import preserves credential identity", kid, KID);
      sqlite3_free(kid);
      doltliteCredsFree(imported);
      imported = NULL;
    }
    sqlite3_free(x);
    privateKey[63] ^= 1;
    x = doltliteBase64UrlEncode(privateKey, 64);
    if (x && d) {
      snprintf(json, sizeof(json),
          "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"d\":\"%s\",\"x\":\"%s\"}", d, x);
      check_bool("Dolt mismatched private key rejected",
                 doltliteCredsFromJwk(json, &imported)!=0);
      doltliteCredsFree(imported);
      imported = NULL;
    }
    snprintf(json, sizeof(json),
        "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"d\":\"%s\",\"x\":\"%s\"}", JWK_D, JWK_D);
    check_bool("mismatched standard JWK rejected", doltliteCredsFromJwk(json, &imported)!=0);
    doltliteCredsFree(imported);
    sqlite3_free(x);
    sqlite3_free(d);
  }

  {
    char *json = doltliteCredsToPublicJwk(c);
    check_str("public JWK omits private seed", json,
              "{\"kty\":\"OKP\",\"crv\":\"Ed25519\","
              "\"x\":\"A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=\"}");
    sqlite3_free(json);
  }

  {
    char *jwt = NULL;
    check_bool("null audience rejected at mint",
               doltliteCredsBearerTokenAt(c, NULL, 1700000000L, &jwt) != 0);
    check_bool("empty audience rejected at mint",
               doltliteCredsBearerTokenAt(c, "", 1700000000L, &jwt) != 0);
    if (doltliteCredsBearerTokenAt(c, AUD, 1700000000L, &jwt) != 0 || !jwt) {
      failures++;
      printf("  FAIL  bearer token build\n");
    } else {
      const char *d1 = strchr(jwt, '.');
      const char *d2 = d1 ? strchr(d1 + 1, '.') : NULL;
      check_str("bearer JWT (golden)", jwt, GOLDEN_JWT);
      if (!d1 || !d2) {
        failures++;
        printf("  FAIL  JWT has three segments\n");
      } else {
        char *hdr = decodeSegZ(jwt, (size_t)(d1 - jwt), NULL);
        char *cls = decodeSegZ(d1 + 1, (size_t)(d2 - (d1 + 1)), NULL);
        size_t siglen = 0;
        char *sig = decodeSegZ(d2 + 1, strlen(d2 + 1), &siglen);
        check_str("JWT header JSON", hdr, EXP_HEADER);
        check_str("JWT claims JSON", cls, EXP_CLAIMS);
        check_bool("JWT signature length 64", sig != NULL && siglen == 64);
        if (sig && siglen == 64) {

          check_bool("JWT signature verifies over signing input",
                     ed25519_verify((const unsigned char *)sig,
                                    (const unsigned char *)jwt,
                                    (size_t)(d2 - jwt),
                                    pub) == 1);
        }
        free(hdr);
        free(cls);
        free(sig);
      }
      sqlite3_free(jwt);
    }
  }

  {
    const char *dir = (argc > 1) ? argv[1] : NULL;
    DoltliteCreds *loaded = NULL;
    unsigned char loadedPub[32];
    char *kid = doltliteCredsKid(c);
    if (doltliteCredsSave(c, dir) != 0) {
      failures++;
      printf("  FAIL  creds save\n");
    } else if (doltliteCredsLoad(dir, kid, &loaded) != 0) {
      failures++;
      printf("  FAIL  creds load\n");
    } else if (!jwkBytes(loaded, "x", loadedPub)
               || memcmp(pub, loadedPub, 32) != 0) {
      failures++;
      printf("  FAIL  creds load (pubkey mismatch)\n");
    } else {
      printf("  PASS  creds save/load by kid\n");
    }
    doltliteCredsFree(loaded);
    sqlite3_free(kid);
  }

  {
    const char *dir = (argc > 1) ? argv[1] : NULL;
    char *kid = doltliteCredsKid(c);
    char *jwk = doltliteCredsToJwk(c);
    char *jwkPath = (char *)malloc(strlen(dir) + 1 + strlen(kid) + 5);
    size_t jwkPathLen = strlen(dir) + 1 + strlen(kid) + 5;
    DoltliteCreds *loaded = NULL;
    unsigned char loadedPub[32], loadedSeed[32];
    unsigned char buf[512];
    size_t jwkLen = jwk ? strlen(jwk) : 0;

    snprintf(jwkPath, jwkPathLen, "%s/%s.jwk", dir, kid);

    memset(buf, 'X', sizeof(buf));
    buf[0] = '\0';
    if (jwk && jwkLen + 1 < sizeof(buf)) {
      memcpy(buf + 1, jwk, jwkLen);
    }
    check_bool("write leading-NUL creds file",
               writeBytes(jwkPath, buf, sizeof(buf)));
    watchArm((int)sizeof(buf) + 1);
    check_bool("creds load rejects leading-NUL file",
               doltliteCredsLoad(dir, kid, &loaded) != 0 && loaded == NULL);
    watchDisarm();
    check_bool("creds load wipes full buffer on leading-NUL file",
               watch_saw_alloc && watch_saw_free && watch_all_zero);

    memset(buf, 'S', sizeof(buf));
    if (jwk && jwkLen + 1 < sizeof(buf)) {
      memcpy(buf, jwk, jwkLen);
      buf[jwkLen] = '\0';
    }
    check_bool("write trailing-NUL creds file",
               writeBytes(jwkPath, buf, sizeof(buf)));
    watchArm((int)sizeof(buf) + 1);
    check_bool("creds load rejects embedded-NUL file",
               doltliteCredsLoad(dir, kid, &loaded) != 0 && loaded == NULL);
    watchDisarm();
    check_bool("creds load wipes bytes after embedded NUL",
               watch_saw_alloc && watch_saw_free && watch_all_zero);
    check_bool("creds import rejects embedded-NUL file",
               doltliteCredsImport(jwkPath, &loaded) != 0 && loaded == NULL);

    check_bool("write empty creds file", writeBytes(jwkPath, "", 0));
    watchArm(1);
    check_bool("creds load rejects empty file",
               doltliteCredsLoad(dir, kid, &loaded) != 0 && loaded == NULL);
    watchDisarm();
    check_bool("creds load wipes buffer on empty file",
               watch_saw_alloc && watch_saw_free && watch_all_zero);

    {
      const char *trunc = "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"d\":\"AA";
      size_t tlen = strlen(trunc);
      check_bool("write truncated creds file",
                 writeBytes(jwkPath, trunc, tlen));
      watchArm((int)tlen + 1);
      check_bool("creds load rejects truncated file",
                 doltliteCredsLoad(dir, kid, &loaded) != 0 && loaded == NULL);
      watchDisarm();
      check_bool("creds load wipes full buffer on truncated file",
                 watch_saw_alloc && watch_saw_free && watch_all_zero);
    }

    remove(jwkPath);
    check_bool("creds load rejects missing file",
               doltliteCredsLoad(dir, kid, &loaded) != 0 && loaded == NULL);

    memset(buf, ' ', sizeof(buf));
    if (jwk && jwkLen <= sizeof(buf)) {
      memcpy(buf, jwk, jwkLen);
    }
    check_bool("write valid padded creds file",
               writeBytes(jwkPath, buf, sizeof(buf)));
    watchArm((int)sizeof(buf) + 1);
    check_bool("creds load succeeds on valid file",
               doltliteCredsLoad(dir, kid, &loaded) == 0 && loaded != NULL);
    watchDisarm();
    check_bool("creds load wipes full buffer on valid load",
               watch_saw_alloc && watch_saw_free && watch_all_zero);
    check_bool("creds load round-trip matches pubkey and seed",
               loaded != NULL &&
               jwkBytes(loaded, "x", loadedPub) &&
               memcmp(pub, loadedPub, 32) == 0 &&
               jwkBytes(loaded, "d", loadedSeed) &&
               memcmp(jwkSeed, loadedSeed, 32) == 0);
    doltliteCredsFree(loaded);

    check_bool("restore saved creds file", doltliteCredsSave(c, dir) == 0);
    free(jwkPath);
    sqlite3_free(jwk);
    sqlite3_free(kid);
  }

  {
    const char *dir = (argc > 1) ? argv[1] : NULL;
    const struct {
      const char *name;
      const char *kid;
    } invalid[] = {
        {"parent traversal KID rejected", "../victim"},
        {"Windows traversal KID rejected", "..\\victim"},
        {"dot KID rejected", "."},
        {"empty KID rejected", ""},
        {"drive-relative KID rejected", "C:credential"},
        {"uppercase KID rejected",
         "7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbarS"},
        {"non-canonical final base32 bits rejected",
         "7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbart"}};
    unsigned char pub[DOLTLITE_PUBKEY_LEN];
    DoltliteCreds *loaded = NULL;
    char *json = doltliteCredsToJwk(c);
    char *outside;
    FILE *f;
    size_t i;
    size_t n = strlen(dir) + strlen("/../victim.jwk") + 1;

    outside = (char *)malloc(n);
    snprintf(outside, n, "%s/../victim.jwk", dir);
    f = fopen(outside, "wb");
    if (f && json) fwrite(json, 1, strlen(json), f);
    if (f) fclose(f);

    check_bool("creds load rejects parent traversal",
               doltliteCredsLoad(dir, "../victim", &loaded) != 0);
    check_bool("public-key load rejects parent traversal",
               doltliteCredsLoadPubKey(dir, "../victim", pub) != 0);
    check_bool("creds remove rejects parent traversal",
               doltliteCredsRemove(dir, "../victim") != 0);
    check_bool("traversal rejection preserves outside file", fileExists(outside));
    for (i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
      check_bool(invalid[i].name,
                 doltliteCredsRemove(dir, invalid[i].kid) != 0);
    }

    doltliteCredsFree(loaded);
    sqlite3_free(json);
    free(outside);
  }

  {
    const char *dir = (argc > 1) ? argv[1] : NULL;
    char *kid = doltliteCredsKid(c);
    char **kids = NULL;
    int n = 0, i, found = 0;
    char *invalidPath;
    FILE *invalidFile;

    invalidPath = (char *)malloc(strlen(dir) + strlen("/not-a-kid.jwk") + 1);
    sprintf(invalidPath, "%s/not-a-kid.jwk", dir);
    invalidFile = fopen(invalidPath, "wb");
    if (invalidFile) {
      fputs("{}", invalidFile);
      fclose(invalidFile);
    }

    if (doltliteCredsList(dir, &kids, &n) != 0) {
      failures++;
      printf("  FAIL  creds list\n");
    } else {
      for (i = 0; i < n; i++) {
        if (kid && strcmp(kids[i], kid) == 0) found = 1;
      }
      check_bool("creds list includes the saved kid", found);
      check_bool("creds list excludes non-canonical filenames", n == 1);
      doltliteCredsFreeList(kids, n);
    }

    {
      DoltliteCreds *loaded = NULL;
      check_bool("default load ignores non-canonical filenames",
                 doltliteCredsLoadDefault(dir, &loaded) == 0);
      doltliteCredsFree(loaded);
    }

    {
      DoltliteCreds *resolved = NULL;
      char *source = NULL;
      char *error = NULL;
      int rc = doltliteCredsResolve(dir, NULL, "/path/to/local/repo",
                                    &resolved, &source, &error);
      check_bool("resolve with non-URL path succeeds via single key",
                 rc == DOLTLITE_CREDS_SELECTED && resolved != NULL &&
                 error == NULL && source != NULL &&
                 strcmp(source, "single key") == 0);
      doltliteCredsFree(resolved);
      sqlite3_free(source);
      sqlite3_free(error);
    }

    {
      int step, succeeded = 0, sawOom = 0, leaked = 0, nonNullOut = 0;
      for (step = 0; step < 64; step++) {
        DoltliteCreds *resolved = NULL;
        char *source = NULL;
        char *error = NULL;
        int liveBefore = sqlite3_alloc_stub_live;
        int rc;
        sqlite3_alloc_stub_fail_after = step;
        rc = doltliteCredsResolve(dir, kid, NULL, &resolved, &source, &error);
        sqlite3_alloc_stub_fail_after = -1;
        if (rc == DOLTLITE_CREDS_SELECTED) {
          succeeded = 1;
          doltliteCredsFree(resolved);
          sqlite3_free(source);
          sqlite3_free(error);
          if (sqlite3_alloc_stub_live != liveBefore) leaked = 1;
          break;
        }
        if (resolved != NULL) nonNullOut = 1;
        if (error && strstr(error, "out of memory")) sawOom = 1;
        doltliteCredsFree(resolved);
        sqlite3_free(source);
        sqlite3_free(error);
        if (sqlite3_alloc_stub_live != liveBefore) leaked = 1;
      }
      check_bool("resolve OOM sweep reaches success", succeeded);
      check_bool("resolve OOM sweep hits OOM error path", sawOom);
      check_bool("resolve OOM leaves *out NULL", !nonNullOut);
      check_bool("resolve OOM has zero leaks", !leaked);
    }

    if (doltliteCredsRemove(dir, kid) != 0) {
      failures++;
      printf("  FAIL  creds remove\n");
    } else {
      kids = NULL;
      n = 0;
      found = 0;
      doltliteCredsList(dir, &kids, &n);
      for (i = 0; i < n; i++) {
        if (kid && strcmp(kids[i], kid) == 0) found = 1;
      }
      check_bool("creds remove deletes the kid", !found);
      check_bool("creds list remains empty with invalid filenames", n == 0);
      doltliteCredsFreeList(kids, n);
    }
    remove(invalidPath);
    free(invalidPath);
    sqlite3_free(kid);
  }

  {
    char *savedHome = dupEnv("HOME");
    char *savedCredsDir = dupEnv("DOLTLITE_CREDS_DIR");
    char *savedCredsKid = dupEnv("DOLTLITE_CREDS_KID");
#ifdef _WIN32
    char *savedUserProfile = dupEnv("USERPROFILE");
    char *savedAppData = dupEnv("APPDATA");
#endif
    DoltliteCreds *resolved = NULL;
    char *source = NULL;
    char *error = NULL;
    int rc;

    setEnvVar("HOME", NULL);
    setEnvVar("DOLTLITE_CREDS_DIR", NULL);
    setEnvVar("DOLTLITE_CREDS_KID", NULL);
#ifdef _WIN32
    setEnvVar("USERPROFILE", NULL);
    setEnvVar("APPDATA", NULL);
#endif

    rc = doltliteCredsResolve(NULL, NULL, "https://doltliteremoteapi.dolthub.com/org/repo",
                              &resolved, &source, &error);
    check_bool("resolve with unset HOME and DOLTLITE_CREDS_DIR returns NONE",
               rc == DOLTLITE_CREDS_NONE && resolved == NULL &&
               source == NULL && error == NULL);
    doltliteCredsFree(resolved);
    sqlite3_free(source);
    sqlite3_free(error);
    resolved = NULL;
    source = NULL;
    error = NULL;

    setEnvVar("DOLTLITE_CREDS_KID", "0123456789abcdefghijklmnopqrstuv0123456789abc");
    rc = doltliteCredsResolve(NULL, NULL, "https://doltliteremoteapi.dolthub.com/org/repo",
                              &resolved, &source, &error);
    check_bool("resolve with unset dirs and DOLTLITE_CREDS_KID returns non-OOM error",
               rc == DOLTLITE_CREDS_ERROR && resolved == NULL &&
               error != NULL && strstr(error, "out of memory") == NULL &&
               strstr(error, "credentials directory") != NULL &&
               source != NULL && strcmp(source, "DOLTLITE_CREDS_KID") == 0);
    doltliteCredsFree(resolved);
    sqlite3_free(source);
    sqlite3_free(error);
    resolved = NULL;
    source = NULL;
    error = NULL;

    setEnvVar("DOLTLITE_CREDS_KID", NULL);
    rc = doltliteCredsResolve(NULL, "0123456789abcdefghijklmnopqrstuv0123456789abc",
                              "https://doltliteremoteapi.dolthub.com/org/repo",
                              &resolved, &source, &error);
    check_bool("resolve with unset dirs and explicit override returns non-OOM error",
               rc == DOLTLITE_CREDS_ERROR && resolved == NULL &&
               error != NULL && strstr(error, "out of memory") == NULL &&
               strstr(error, "credentials directory") != NULL &&
               source != NULL && strcmp(source, "connection") == 0);
    doltliteCredsFree(resolved);
    sqlite3_free(source);
    sqlite3_free(error);
    resolved = NULL;
    source = NULL;
    error = NULL;

    rc = doltliteCredsResolve(NULL, "anonymous",
                              "https://doltliteremoteapi.dolthub.com/org/repo",
                              &resolved, &source, &error);
    check_bool("resolve with unset dirs and explicit anonymous succeeds",
               rc == DOLTLITE_CREDS_ANONYMOUS && resolved == NULL &&
               error == NULL && source != NULL &&
               strcmp(source, "connection") == 0);
    doltliteCredsFree(resolved);
    sqlite3_free(source);
    sqlite3_free(error);

    setEnvVar("HOME", savedHome);
    setEnvVar("DOLTLITE_CREDS_DIR", savedCredsDir);
    setEnvVar("DOLTLITE_CREDS_KID", savedCredsKid);
    free(savedHome);
    free(savedCredsDir);
    free(savedCredsKid);
#ifdef _WIN32
    setEnvVar("USERPROFILE", savedUserProfile);
    setEnvVar("APPDATA", savedAppData);
    free(savedUserProfile);
    free(savedAppData);
#endif
  }

  doltliteCredsFree(c);
  doltliteCredsFree(c2);

  printf("\n%s: %d failure(s)\n", failures ? "FAILED" : "OK", failures);
  return failures ? 1 : 0;
}
