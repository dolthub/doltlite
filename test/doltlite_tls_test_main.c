
#include "doltlite_tls.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *testHost(void) {
  const char *h = getenv("DOLTLITE_TLS_TEST_HOST");
  return (h && *h) ? h : "dolthub.com";
}

int main(int argc, char **argv) {
  const char *host = testHost();
  const int port = 443;
  int fails = 0;
  DoltliteConn *probe, *c;

  unsetenv("DOLTLITE_CA_FILE");

  probe = doltliteConnOpen(host, port, 0);
  if (!probe) {
    printf("SKIP  cannot reach %s:%d (offline?)\n", host, port);
    return 0;
  }
  doltliteConnClose(probe);

  c = doltliteConnOpen(host, port, 1);
  if (!c) {
    printf("FAIL  TLS connect+verify to %s failed\n", host);
    return 1;
  } else {
    char req[256];
    int n = snprintf(req, sizeof(req),
                     "GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
                     host);
    if (doltliteConnWriteAll(c, req, n) != 0) {
      printf("FAIL  TLS write\n");
      fails++;
    } else {
      char buf[512];
      int r = doltliteConnRead(c, buf, (int)sizeof(buf) - 1);
      if (r <= 0) {
        printf("FAIL  TLS read returned %d\n", r);
        fails++;
      } else {
        buf[r] = '\0';
        if (strncmp(buf, "HTTP/", 5) == 0) {
          char *eol = strpbrk(buf, "\r\n");
          if (eol) *eol = '\0';
          printf("  PASS  HTTPS GET %s -> \"%s\"\n", host, buf);
        } else {
          printf("FAIL  unexpected response start: %.16s\n", buf);
          fails++;
        }
      }
    }
    doltliteConnClose(c);
  }

  if (argc > 1 && argv[1][0] != '\0') {
    setenv("DOLTLITE_CA_FILE", argv[1], 1);
    DoltliteConn *bad = doltliteConnOpen(host, port, 1);
    if (bad) {
      printf("FAIL  handshake succeeded under wrong CA (verification not enforced)\n");
      doltliteConnClose(bad);
      fails++;
    } else {
      printf("  PASS  handshake rejected under wrong CA\n");
    }
    unsetenv("DOLTLITE_CA_FILE");
  } else {
    printf("  (skipped wrong-CA negative test: no openssl to make a fake CA)\n");
  }

  printf("\n%s\n", fails ? "FAILED" : "OK");
  return fails ? 1 : 0;
}
