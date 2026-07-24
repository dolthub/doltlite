
#include "doltlite_creds.h"
#include "ed25519.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
  free(raw);
  return s;
}

#define AUD "doltremoteapi.dolthub.com"
static const char *GOLDEN_JWT =
    "eyJhbGciOiJFZERTQSIsImtpZCI6IjdrdTFjZ2Q3dWprY3JpNXU0c21tcnNycGNwM2Vqc21n"
    "Yzl0N28zZGtkYmFycyIsImRvbHRfdG9rZW5fdmVyc2lvbiI6IjIwMjMuMDEifQ.eyJpc3Mi"
    "OiJkb2x0LWNsaWVudC5kb2x0aHViLmNvbSIsInN1YiI6ImRvbHRDbGllbnRDcmVkZW50aWFs"
    "cy83a3UxY2dkN3Vqa2NyaTV1NHNtbXJzcnBjcDNlanNtZ2M5dDdvM2RrZGJhcnMiLCJhdWQi"
    "OiJkb2x0cmVtb3RlYXBpLmRvbHRodWIuY29tIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE3"
    "MDAwMDAwMzB9.zYNXsaON_rblUat4NuSeDYNcCkkn5V5M28BVXgTybOs4lldxD_GUxRroTTeK"
    "xlLebVONlBZxI6ukuhbW_1hvCg";
static const char *EXP_HEADER =
    "{\"alg\":\"EdDSA\",\"kid\":\"7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbars\","
    "\"dolt_token_version\":\"2023.01\"}";
static const char *EXP_CLAIMS =
    "{\"iss\":\"dolt-client.dolthub.com\","
    "\"sub\":\"doltClientCredentials/7ku1cgd7ujkcri5u4smmrsrpcp3ejsmgc9t7o3dkdbars\","
    "\"aud\":\"doltremoteapi.dolthub.com\",\"iat\":1700000000,\"exp\":1700000030}";

int main(int argc, char **argv) {
  char hexbuf[256];
  unsigned char seed[32];
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
  hexenc(doltliteCredsPubKey(c), 32, hexbuf);
  check_str("pubkey from seed", hexbuf, PUB_HEX);

  {
    char *kid = doltliteCredsKid(c);
    char *pubb32 = doltliteCredsPubKeyB32(c);
    check_str("kid = base32(sha512_224(pub))", kid, KID);
    check_str("pubkey base32 (approval URL fragment)", pubb32, PUBB32);
    free(kid);
    free(pubb32);
  }

  {
    unsigned char sig[DOLTLITE_SIG_LEN];
    doltliteCredsSign(c, (const unsigned char *)MSG, strlen(MSG), sig);
    hexenc(sig, sizeof(sig), hexbuf);
    check_str("ed25519 sign(\"doltlite auth\")", hexbuf, SIG_HEX);
    if (ed25519_verify(sig, (const unsigned char *)MSG, strlen(MSG),
                       doltliteCredsPubKey(c)) == 1) {
      printf("  PASS  ed25519 verify\n");
    } else {
      failures++;
      printf("  FAIL  ed25519 verify\n");
    }
  }

  {
    char *x = doltliteBase64UrlEncode(doltliteCredsPubKey(c), 32);
    char *d = doltliteBase64UrlEncode(doltliteCredsSeed(c), 32);
    check_str("base64url(pub) == JWK x", x, JWK_X);
    check_str("base64url(seed) == JWK d", d, JWK_D);
    {
      unsigned char *dec = NULL;
      size_t dlen = 0;
      if (doltliteBase64UrlDecode(x, &dec, &dlen) == 0 && dlen == 32 &&
          memcmp(dec, doltliteCredsPubKey(c), 32) == 0) {
        printf("  PASS  base64url decode round-trip\n");
      } else {
        failures++;
        printf("  FAIL  base64url decode round-trip\n");
      }
      free(dec);
    }
    free(x);
    free(d);
  }

  {
    char *json = doltliteCredsToJwk(c);
    if (!json || doltliteCredsFromJwk(json, &c2) != 0) {
      failures++;
      printf("  FAIL  JWK round-trip (parse)\n");
    } else {
      char *k1 = doltliteCredsKid(c);
      char *k2 = doltliteCredsKid(c2);
      if (k1 && k2 && strcmp(k1, k2) == 0 &&
          memcmp(doltliteCredsSeed(c), doltliteCredsSeed(c2), 32) == 0) {
        printf("  PASS  JWK round-trip\n");
      } else {
        failures++;
        printf("  FAIL  JWK round-trip (mismatch)\n");
      }
      free(k1);
      free(k2);
    }
    free(json);
  }

  {
    char *jwt = NULL;
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
                                    doltliteCredsPubKey(c)) == 1);
        }
        free(hdr);
        free(cls);
        free(sig);
      }
      free(jwt);
    }
  }

  {
    const char *dir = (argc > 1) ? argv[1] : NULL;
    DoltliteCreds *loaded = NULL;
    char *kid = doltliteCredsKid(c);
    if (doltliteCredsSave(c, dir) != 0) {
      failures++;
      printf("  FAIL  creds save\n");
    } else if (doltliteCredsLoad(dir, kid, &loaded) != 0) {
      failures++;
      printf("  FAIL  creds load\n");
    } else if (memcmp(doltliteCredsPubKey(c), doltliteCredsPubKey(loaded), 32) != 0) {
      failures++;
      printf("  FAIL  creds load (pubkey mismatch)\n");
    } else {
      printf("  PASS  creds save/load by kid\n");
    }
    doltliteCredsFree(loaded);
    free(kid);
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
    free(json);
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
    free(kid);
  }

  doltliteCredsFree(c);
  doltliteCredsFree(c2);

  printf("\n%s: %d failure(s)\n", failures ? "FAILED" : "OK", failures);
  return failures ? 1 : 0;
}
