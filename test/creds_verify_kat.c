
#include "doltlite_creds.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

int main(int argc, char **argv) {
  const char *authDir = argc > 1 ? argv[1] : ".";
  const char *emptyDir = argc > 2 ? argv[2] : ".";
  unsigned char seed[32];
  DoltliteCreds *c = NULL;
  char *jwt = NULL, *kid = NULL, *kidOut = NULL;
  char *bearer = NULL, *tampered = NULL;
  int i;

  for (i = 0; i < 32; i++) seed[i] = (unsigned char)i;
  if (doltliteCredsFromSeed(seed, &c) != 0) {
    printf("  FAIL  creds from seed\n");
    return 1;
  }
  kid = doltliteCredsKid(c);

  if (doltliteCredsSave(c, authDir) != 0) {
    printf("  FAIL  save authorized key\n");
    return 1;
  }

  if (doltliteCredsBearerTokenAt(c, AUD, IAT, &jwt) != 0 || !jwt) {
    printf("  FAIL  build bearer token\n");
    return 1;
  }

  kidOut = NULL;
  check("valid token accepted",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, MID, &kidOut) == 0);
  check("kidOut matches", kidOut && kid && strcmp(kidOut, kid) == 0);
  free(kidOut);

  bearer = (char *)malloc(strlen(jwt) + 8);
  sprintf(bearer, "Bearer %s", jwt);
  check("\"Bearer \" prefix accepted",
        doltliteCredsVerifyBearer(bearer, AUD, authDir, MID, NULL) == 0);

  check("null expected-audience accepted",
        doltliteCredsVerifyBearer(jwt, NULL, authDir, MID, NULL) == 0);

  check("expired token rejected",
        doltliteCredsVerifyBearer(jwt, AUD, authDir, LATE, NULL) != 0);
  check("wrong audience rejected",
        doltliteCredsVerifyBearer(jwt, "evil.example.com", authDir, MID, NULL) != 0);
  check("unknown key (empty authorized dir) rejected",
        doltliteCredsVerifyBearer(jwt, AUD, emptyDir, MID, NULL) != 0);
  check("missing token rejected", doltliteCredsVerifyBearer(NULL, AUD, authDir, MID, NULL) != 0);
  check("malformed token rejected",
        doltliteCredsVerifyBearer("not-a-jwt", AUD, authDir, MID, NULL) != 0);

  tampered = (char *)malloc(strlen(jwt) + 1);
  strcpy(tampered, jwt);
  {
    size_t n = strlen(tampered);
    tampered[n - 1] = (tampered[n - 1] == 'A') ? 'B' : 'A';
  }
  check("tampered signature rejected",
        doltliteCredsVerifyBearer(tampered, AUD, authDir, MID, NULL) != 0);

  free(jwt);
  free(kid);
  free(bearer);
  free(tampered);
  doltliteCredsFree(c);

  printf("\n%s: %d failure(s)\n", failures ? "FAILED" : "OK", failures);
  return failures ? 1 : 0;
}
