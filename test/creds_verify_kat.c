
#include "doltlite_creds.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void sqlite3_free(void*);

static int failures = 0;

static void check(const char *name, int ok) {
  if (ok) {
    printf("  PASS  %s\n", name);
  } else {
    failures++;
    printf("  FAIL  %s\n", name);
  }
}

#define AUD "doltremoteapi.dolthub.com"
#define IAT 1700000000L
#define MID 1700000015L
#define LATE 1700001000L

static char *encodeRaw(const unsigned char *p, size_t n) {
  char *z = doltliteBase64UrlEncode(p, n);
  size_t len;
  if (!z) return NULL;
  len = strlen(z);
  while (len > 0 && z[len - 1] == '=') z[--len] = '\0';
  return z;
}

static char *tokenWithClaims(
  const DoltliteCreds *c,
  const char *kid,
  const char *audJson,
  const char *iat,
  const char *nbf,
  const char *exp
) {
  char *header = NULL, *claims = NULL, *h64 = NULL, *c64 = NULL;
  char *input = NULL, *s64 = NULL, *token = NULL;
  unsigned char sig[DOLTLITE_SIG_LEN];
  size_t n;

  n = strlen(kid) + 96;
  header = (char *)malloc(n);
  if (!header) goto done;
  snprintf(header, n,
           "{\"alg\":\"EdDSA\",\"kid\":\"%s\",\"dolt_token_version\":\"2023.01\"}",
           kid);
  n = strlen(kid) * 2 + strlen(audJson) +
      (iat ? strlen(iat) : 0) + (nbf ? strlen(nbf) : 0) +
      (exp ? strlen(exp) : 0) + 192;
  claims = (char *)malloc(n);
  if (!claims) goto done;
  snprintf(claims, n,
           "{\"iss\":\"dolt-client.dolthub.com\","
           "\"sub\":\"doltClientCredentials/%s\",\"aud\":%s%s%s%s%s%s%s}",
           kid, audJson,
           iat ? ",\"iat\":" : "", iat ? iat : "",
           nbf ? ",\"nbf\":" : "", nbf ? nbf : "",
           exp ? ",\"exp\":" : "", exp ? exp : "");
  h64 = encodeRaw((const unsigned char *)header, strlen(header));
  c64 = encodeRaw((const unsigned char *)claims, strlen(claims));
  if (!h64 || !c64) goto done;
  n = strlen(h64) + strlen(c64) + 2;
  input = (char *)malloc(n);
  if (!input) goto done;
  snprintf(input, n, "%s.%s", h64, c64);
  doltliteCredsSign(c, (const unsigned char *)input, strlen(input), sig);
  s64 = encodeRaw(sig, sizeof(sig));
  if (!s64) goto done;
  n = strlen(input) + strlen(s64) + 2;
  token = (char *)malloc(n);
  if (token) snprintf(token, n, "%s.%s", input, s64);

done:
  free(header);
  free(claims);
  sqlite3_free(h64);
  sqlite3_free(c64);
  free(input);
  sqlite3_free(s64);
  return token;
}

static char *tokenWithAudJson(
  const DoltliteCreds *c,
  const char *kid,
  const char *audJson,
  const char *exp
) {
  return tokenWithClaims(c, kid, audJson, "1700000000", NULL, exp);
}

static char *tokenForKid(
  const DoltliteCreds *c,
  const char *kid,
  const char *exp
) {
  return tokenWithAudJson(c, kid, "\"" AUD "\"", exp);
}

int main(int argc, char **argv) {
  const char *authDir = argc > 1 ? argv[1] : ".";
  const char *emptyDir = argc > 2 ? argv[2] : ".";
  const char *outsideDir = argc > 3 ? argv[3] : ".";
  const char *mismatchDir = argc > 4 ? argv[4] : ".";
  const char *disguisedDir = argc > 5 ? argv[5] : ".";
  unsigned char seed[32];
  DoltliteCreds *c = NULL;
  char *jwt = NULL, *kid = NULL, *kidOut = NULL;
  char *bearer = NULL, *tampered = NULL;
  char *invalidExp = NULL, *overflowExp = NULL;
  unsigned char pub[DOLTLITE_PUBKEY_LEN];
  int i;

  for (i = 0; i < 32; i++) seed[i] = (unsigned char)i;
  if (doltliteCredsFromSeed(seed, &c) != 0) {
    printf("  FAIL  creds from seed\n");
    return 1;
  }
  kid = doltliteCredsKid(c);

  if (doltliteCredsSavePublic(c, authDir) != 0) {
    printf("  FAIL  save authorized key\n");
    return 1;
  }
  check("public authorization directory accepted",
        doltliteCredsValidateAuthDir(authDir) == 0);
  {
    char *notePath = (char *)malloc(strlen(authDir) + 10);
    FILE *note;
    sprintf(notePath, "%s/note.txt", authDir);
    note = fopen(notePath, "wb");
    if (note) {
      fputs("operator note\n", note);
      fclose(note);
    }
    check("unrelated authorization-directory file accepted",
          doltliteCredsValidateAuthDir(authDir) == 0);
    free(notePath);
  }

  if (doltliteCredsBearerTokenAt(c, AUD, IAT, &jwt) != 0 || !jwt) {
    printf("  FAIL  build bearer token\n");
    return 1;
  }

  kidOut = NULL;
  check("valid token accepted",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, MID, &kidOut) == 0);
  check("kidOut matches", kidOut && kid && strcmp(kidOut, kid) == 0);
  sqlite3_free(kidOut);

  bearer = (char *)malloc(strlen(jwt) + 8);
  sprintf(bearer, "Bearer %s", jwt);
  check("\"Bearer \" prefix accepted",
        doltliteCredsVerifyBearer(bearer, AUD, authDir, MID, NULL) == 0);

  check("null expected-audience rejected",
        doltliteCredsVerifyBearer(jwt, NULL, authDir, MID, NULL) != 0);
  check("empty expected-audience rejected",
        doltliteCredsVerifyBearer(jwt, "", authDir, MID, NULL) != 0);

  {
    char *arrayJwt = tokenWithAudJson(c, kid, "[\"" AUD "\"]", "1700000030");
    char *multiJwt = tokenWithAudJson(c, kid, "[\"other\",\"" AUD "\"]",
                                      "1700000030");
    char *wrongArr = tokenWithAudJson(c, kid, "[\"other\"]", "1700000030");
    char *emptyArr = tokenWithAudJson(c, kid, "[]", "1700000030");
    check("Dolt array aud accepted",
          arrayJwt &&
              doltliteCredsVerifyBearer(arrayJwt, AUD, authDir, MID, NULL) == 0);
    check("aud array containing expected accepted",
          multiJwt &&
              doltliteCredsVerifyBearer(multiJwt, AUD, authDir, MID, NULL) == 0);
    check("aud array without expected rejected",
          wrongArr &&
              doltliteCredsVerifyBearer(wrongArr, AUD, authDir, MID, NULL) != 0);
    check("empty aud array rejected",
          emptyArr &&
              doltliteCredsVerifyBearer(emptyArr, AUD, authDir, MID, NULL) != 0);
    free(arrayJwt);
    free(multiJwt);
    free(wrongArr);
    free(emptyArr);
  }

  check("expired token rejected",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, LATE, NULL) != 0);

  check("iat at positive clock-skew boundary accepted",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, IAT - 60, NULL) == 0);
  check("iat beyond positive clock-skew boundary rejected",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, IAT - 61, NULL) != 0);
  check("token accepted at exp",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, IAT + 30, NULL) == 0);
  check("token accepted at negative clock-skew boundary",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, IAT + 90, NULL) == 0);
  check("token rejected beyond negative clock-skew boundary",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, IAT + 91, NULL) != 0);

  {
    char *missingIat = tokenWithClaims(c, kid, "\"" AUD "\"", NULL, NULL,
                                       "1700000030");
    char *futureNbf = tokenWithClaims(c, kid, "\"" AUD "\"", NULL,
                                      "1700000060", "1700000090");
    char *tooFutureNbf = tokenWithClaims(c, kid, "\"" AUD "\"", NULL,
                                         "1700000061", "1700000090");
    char *invalidIat = tokenWithClaims(c, kid, "\"" AUD "\"",
                                       "1700000000x", NULL, "1700000030");
    char *invalidNbf = tokenWithClaims(c, kid, "\"" AUD "\"", NULL,
                                       "1700000000x", "1700000030");
    char *longTtl = tokenWithClaims(c, kid, "\"" AUD "\"", "1700000000",
                                    NULL, "1700000031");
    char *reversed = tokenWithClaims(c, kid, "\"" AUD "\"", "1700000000",
                                     NULL, "1699999999");
    char *longRemaining = tokenWithClaims(c, kid, "\"" AUD "\"", NULL,
                                          NULL, "1700000091");
    char *nbfAfterExp = tokenWithClaims(c, kid, "\"" AUD "\"", NULL,
                                        "1700000031", "1700000030");
    char *missingExp = tokenWithClaims(c, kid, "\"" AUD "\"",
                                       "1700000000", NULL, NULL);
    check("missing iat accepted for Dolt compatibility",
          missingIat &&
              doltliteCredsVerifyBearer(missingIat, AUD, authDir, MID, NULL) == 0);
    check("nbf at positive clock-skew boundary accepted",
          futureNbf &&
              doltliteCredsVerifyBearer(futureNbf, AUD, authDir, IAT, NULL) == 0);
    check("nbf beyond positive clock-skew boundary rejected",
          tooFutureNbf &&
              doltliteCredsVerifyBearer(tooFutureNbf, AUD, authDir, IAT, NULL) != 0);
    check("malformed iat rejected",
          invalidIat &&
              doltliteCredsVerifyBearer(invalidIat, AUD, authDir, MID, NULL) != 0);
    check("malformed nbf rejected",
          invalidNbf &&
              doltliteCredsVerifyBearer(invalidNbf, AUD, authDir, MID, NULL) != 0);
    check("token lifetime above 30 seconds rejected",
          longTtl &&
              doltliteCredsVerifyBearer(longTtl, AUD, authDir, MID, NULL) != 0);
    check("exp before iat rejected",
          reversed &&
              doltliteCredsVerifyBearer(reversed, AUD, authDir, MID, NULL) != 0);
    check("missing-iat token above maximum remaining lifetime rejected",
          longRemaining &&
              doltliteCredsVerifyBearer(longRemaining, AUD, authDir, IAT, NULL) != 0);
    check("nbf after exp rejected",
          nbfAfterExp &&
              doltliteCredsVerifyBearer(nbfAfterExp, AUD, authDir, IAT, NULL) != 0);
    check("missing exp rejected",
          missingExp &&
              doltliteCredsVerifyBearer(missingExp, AUD, authDir, MID, NULL) != 0);
    free(missingIat);
    free(futureNbf);
    free(tooFutureNbf);
    free(invalidIat);
    free(invalidNbf);
    free(longTtl);
    free(reversed);
    free(longRemaining);
    free(nbfAfterExp);
    free(missingExp);
  }

  check("wrong audience rejected",
        doltliteCredsVerifyBearer(jwt, "evil.example.com", authDir, MID, NULL) != 0);
  check("unknown key (empty authorized dir) rejected",
        doltliteCredsVerifyBearer(jwt, AUD, emptyDir, MID, NULL) != 0);
  check("missing token rejected", doltliteCredsVerifyBearer(NULL, AUD, authDir, MID, NULL) != 0);
  check("malformed token rejected",
        doltliteCredsVerifyBearer("not-a-jwt", AUD, authDir, MID, NULL) != 0);

  invalidExp = tokenForKid(c, kid, "1700000030x");
  check("signed token with trailing exp text rejected",
        invalidExp &&
        doltliteCredsVerifyBearer(invalidExp, AUD, authDir, MID, NULL) != 0);
  overflowExp = tokenForKid(c, kid, "999999999999999999999999");
  check("signed token with overflowing exp rejected",
        overflowExp &&
        doltliteCredsVerifyBearer(overflowExp, AUD, authDir, MID, NULL) != 0);

  tampered = (char *)malloc(strlen(jwt) + 1);
  strcpy(tampered, jwt);
  {
    size_t n = strlen(tampered);
    tampered[n - 1] = (tampered[n - 1] == 'A') ? 'B' : 'A';
  }
  check("tampered signature rejected",
        doltliteCredsVerifyBearer(tampered, AUD, authDir, MID, NULL) != 0);

  {
    char *otherKid = (char *)malloc(strlen(kid) + 1);
    char *from;
    char *to;
    size_t pathLen = strlen(mismatchDir) + strlen(kid) + 7;
    strcpy(otherKid, kid);
    otherKid[0] = otherKid[0] == '0' ? '1' : '0';
    from = (char *)malloc(pathLen);
    to = (char *)malloc(pathLen);
    snprintf(from, pathLen, "%s/%s.jwk", mismatchDir, kid);
    snprintf(to, pathLen, "%s/%s.jwk", mismatchDir, otherKid);
    check("save public key for filename mismatch test",
          doltliteCredsSavePublic(c, mismatchDir) == 0);
    check("rename public key to a different canonical id",
          rename(from, to) == 0);
    check("public key filename must match its derived id",
          doltliteCredsLoadPubKey(mismatchDir, otherKid, pub) != 0);
    check("mismatched key id invalidates authorization directory",
          doltliteCredsValidateAuthDir(mismatchDir) != 0);
    free(otherKid);
    free(from);
    free(to);
  }

  {
    char *traversalKid;
    char *traversalJwt;
    size_t n = strlen(kid) + strlen("../outside/") + 1;
    check("save key outside authorized directory",
          doltliteCredsSave(c, outsideDir) == 0);
    check("private credential rejected as an authorization key",
          doltliteCredsLoadPubKey(outsideDir, kid, pub) != 0);
    check("private credential invalidates authorization directory",
          doltliteCredsValidateAuthDir(outsideDir) != 0);
    traversalKid = (char *)malloc(n);
    snprintf(traversalKid, n, "../outside/%s", kid);
    traversalJwt = tokenForKid(c, traversalKid, "1700000030");
    check("signed token cannot traverse auth-key directory",
          traversalJwt &&
              doltliteCredsVerifyBearer(traversalJwt, AUD, authDir, MID, NULL) != 0);
    free(traversalKid);
    free(traversalJwt);
  }

  {
    char *from;
    char *to;
    size_t pathLen = strlen(disguisedDir) + strlen(kid) + 7;
    from = (char *)malloc(pathLen);
    to = (char *)malloc(strlen(disguisedDir) + 14);
    snprintf(from, pathLen, "%s/%s.jwk", disguisedDir, kid);
    sprintf(to, "%s/private.json", disguisedDir);
    check("save private credential for disguised-file test",
          doltliteCredsSave(c, disguisedDir) == 0);
    check("rename private credential outside JWK convention",
          rename(from, to) == 0);
    check("disguised private credential invalidates authorization directory",
          doltliteCredsValidateAuthDir(disguisedDir) != 0);
    free(from);
    free(to);
  }

  sqlite3_free(jwt);
  sqlite3_free(kid);
  free(bearer);
  free(tampered);
  free(invalidExp);
  free(overflowExp);
  doltliteCredsFree(c);

  printf("\n%s: %d failure(s)\n", failures ? "FAILED" : "OK", failures);
  return failures ? 1 : 0;
}
