/*
** Known-answer tests for doltlite_creds (see src/doltlite_creds.c).
**
** Golden values were produced with Go's crypto/ed25519 and crypto/sha512
** (i.e. the exact algorithms DoltHub/dolt use), so passing here proves the
** vendored orlp ed25519 + our SHA-512/224 and encodings are wire-compatible.
**
** Build/run via test/creds_kat_test.sh.
*/

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

/* Fixed ed25519 seed 00,01,...,1f and its Go-derived artifacts. */
static const char *SEED_HEX =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
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

int main(int argc, char **argv) {
  char hexbuf[256];
  unsigned char seed[32];
  DoltliteCreds *c = NULL, *c2 = NULL;

  /* --- SHA-512/224 --- */
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

  /* --- keypair from seed (orlp == Go == dolt) --- */
  seedBytes(seed);
  if (doltliteCredsFromSeed(seed, &c) != 0) {
    printf("  FAIL  doltliteCredsFromSeed\n");
    return 1;
  }
  hexenc(doltliteCredsPubKey(c), 32, hexbuf);
  check_str("pubkey from seed", hexbuf, PUB_HEX);

  /* --- kid and pubkey base32 --- */
  {
    char *kid = doltliteCredsKid(c);
    char *pubb32 = doltliteCredsPubKeyB32(c);
    check_str("kid = base32(sha512_224(pub))", kid, KID);
    check_str("pubkey base32 (approval URL fragment)", pubb32, PUBB32);
    free(kid);
    free(pubb32);
  }

  /* --- detached signature (RFC 8032) --- */
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

  /* --- base64url --- */
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

  /* --- JWK round-trip --- */
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

  /* --- file store save/load (uses DOLTLITE_CREDS_DIR from the harness) --- */
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

  doltliteCredsFree(c);
  doltliteCredsFree(c2);

  printf("\n%s: %d failure(s)\n", failures ? "FAILED" : "OK", failures);
  return failures ? 1 : 0;
}
