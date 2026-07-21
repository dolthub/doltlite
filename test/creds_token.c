#include "doltlite_creds.h"

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
  DoltliteCreds *cred = NULL;
  char *jwt = NULL;

  if (argc != 3) {
    fprintf(stderr, "usage: %s CREDS_DIR AUDIENCE\n", argv[0]);
    return 2;
  }
  if (doltliteCredsLoadDefault(argv[1], &cred) != 0 || !cred) {
    fprintf(stderr, "failed to load credential\n");
    return 1;
  }
  if (doltliteCredsBearerToken(cred, argv[2], &jwt) != 0 || !jwt) {
    fprintf(stderr, "failed to create bearer token\n");
    doltliteCredsFree(cred);
    return 1;
  }

  puts(jwt);
  free(jwt);
  doltliteCredsFree(cred);
  return 0;
}
