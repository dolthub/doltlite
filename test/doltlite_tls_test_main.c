
#include "doltlite_tls.h"
#include "doltlite_net.h"

#include "mbedtls/ctr_drbg.h"
#include "mbedtls/entropy.h"
#include "mbedtls/ssl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
static int setenv(const char *n, const char *v, int overwrite) {
  (void)overwrite;
  return _putenv_s(n, v);
}
static int unsetenv(const char *n) { return _putenv_s(n, ""); }
#endif

typedef struct TestCliBio {
  int fd;
  int holdRecv;
} TestCliBio;

static int testCliSend(void *ctx, const unsigned char *buf, size_t len) {
  TestCliBio *b = (TestCliBio *)ctx;
  int flags = 0;
  int n;
#ifdef MSG_NOSIGNAL
  flags = MSG_NOSIGNAL;
#endif
#ifdef _WIN32
  n = send(b->fd, (const char *)buf, (int)len, flags);
  if (n < 0) {
    int err = WSAGetLastError();
    if (err == WSAEWOULDBLOCK || err == WSAEINTR) return MBEDTLS_ERR_SSL_WANT_WRITE;
    return -1;
  }
#else
  n = (int)send(b->fd, buf, len, flags);
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      return MBEDTLS_ERR_SSL_WANT_WRITE;
    }
    return -1;
  }
#endif
  return n;
}

static int testCliRecv(void *ctx, unsigned char *buf, size_t len) {
  TestCliBio *b = (TestCliBio *)ctx;
  int n;
  if (b->holdRecv) return MBEDTLS_ERR_SSL_WANT_READ;
  n = doltliteSocketRecv(b->fd, buf, (int)len);
  if (n == -2) return MBEDTLS_ERR_SSL_WANT_READ;
  return n;
}

static int makeLoopbackPair(int *pSrvFd, int *pCliFd) {
  struct sockaddr_in addr;
  int listenFd = -1, cliFd = -1, srvFd = -1;
  int one = 1;
  int bufSz = 4096;
#ifdef _WIN32
  int addrLen = (int)sizeof(addr);
#else
  socklen_t addrLen = (socklen_t)sizeof(addr);
#endif

  if (doltliteNetInit() != 0) return -1;
  listenFd = (int)socket(AF_INET, SOCK_STREAM, 0);
  if (listenFd < 0) goto fail;
  setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, (const char *)&one, sizeof(one));
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (bind(listenFd, (struct sockaddr *)&addr, sizeof(addr)) != 0) goto fail;
  if (listen(listenFd, 1) != 0) goto fail;
  if (getsockname(listenFd, (struct sockaddr *)&addr, &addrLen) != 0) goto fail;

  cliFd = (int)socket(AF_INET, SOCK_STREAM, 0);
  if (cliFd < 0) goto fail;
  setsockopt(cliFd, SOL_SOCKET, SO_RCVBUF, (const char *)&bufSz, sizeof(bufSz));
  if (connect(cliFd, (struct sockaddr *)&addr, sizeof(addr)) != 0) goto fail;

  srvFd = (int)accept(listenFd, NULL, NULL);
  if (srvFd < 0) goto fail;
  setsockopt(srvFd, SOL_SOCKET, SO_SNDBUF, (const char *)&bufSz, sizeof(bufSz));
  doltliteCloseSocket(listenFd);

  if (doltliteSocketSetBlocking(srvFd, 0) != 0 ||
      doltliteSocketSetBlocking(cliFd, 0) != 0) {
    doltliteCloseSocket(srvFd);
    doltliteCloseSocket(cliFd);
    return -1;
  }
  *pSrvFd = srvFd;
  *pCliFd = cliFd;
  return 0;

fail:
  if (listenFd >= 0) doltliteCloseSocket(listenFd);
  if (cliFd >= 0) doltliteCloseSocket(cliFd);
  if (srvFd >= 0) doltliteCloseSocket(srvFd);
  return -1;
}

static int testNonblockHandshakeWantWrite(const char *certFile, const char *keyFile) {
  int srvFd = -1, rawSrvFd = -1, cliFd = -1;
  DoltliteTlsServer *tlsSrv = NULL;
  DoltliteConn *srvConn = NULL;
  mbedtls_ssl_context cliSsl;
  mbedtls_ssl_config cliConf;
  mbedtls_ctr_drbg_context drbg;
  mbedtls_entropy_context entropy;
  TestCliBio bio;
  unsigned char fill[4096];
  size_t totalFilled = 0;
  size_t drained = 0;
  int sawWantWrite = 0;
  int srvDone = 0, cliDone = 0;
  int i, rc = 1;

  mbedtls_ssl_init(&cliSsl);
  mbedtls_ssl_config_init(&cliConf);
  mbedtls_ctr_drbg_init(&drbg);
  mbedtls_entropy_init(&entropy);

  if (makeLoopbackPair(&srvFd, &cliFd) != 0) {
    printf("FAIL  makeLoopbackPair failed\n");
    goto cleanup;
  }

  tlsSrv = doltliteTlsServerNew(certFile, keyFile);
  if (!tlsSrv) {
    printf("FAIL  doltliteTlsServerNew failed\n");
    goto cleanup;
  }
  rawSrvFd = srvFd;
  srvConn = doltliteConnServerAttach(tlsSrv, srvFd);
  srvFd = -1;
  if (!srvConn) {
    printf("FAIL  doltliteConnServerAttach failed\n");
    goto cleanup;
  }

  if (mbedtls_ctr_drbg_seed(&drbg, mbedtls_entropy_func, &entropy,
                            (const unsigned char *)"tls-nb-test", 11) != 0 ||
      mbedtls_ssl_config_defaults(&cliConf, MBEDTLS_SSL_IS_CLIENT,
                                  MBEDTLS_SSL_TRANSPORT_STREAM,
                                  MBEDTLS_SSL_PRESET_DEFAULT) != 0) {
    printf("FAIL  client TLS config failed\n");
    goto cleanup;
  }
  mbedtls_ssl_conf_min_tls_version(&cliConf, MBEDTLS_SSL_VERSION_TLS1_3);
  mbedtls_ssl_conf_authmode(&cliConf, MBEDTLS_SSL_VERIFY_NONE);
  mbedtls_ssl_conf_rng(&cliConf, mbedtls_ctr_drbg_random, &drbg);
  if (mbedtls_ssl_setup(&cliSsl, &cliConf) != 0) {
    printf("FAIL  mbedtls_ssl_setup failed\n");
    goto cleanup;
  }
  bio.fd = cliFd;
  bio.holdRecv = 1;
  mbedtls_ssl_set_bio(&cliSsl, &bio, testCliSend, testCliRecv, NULL);

  for (i = 0; i < 20; i++) {
    int cret = mbedtls_ssl_handshake(&cliSsl);
    if (cret == MBEDTLS_ERR_SSL_WANT_READ) break;
    if (cret != MBEDTLS_ERR_SSL_WANT_WRITE) {
      printf("FAIL  client ClientHello step returned %d\n", cret);
      goto cleanup;
    }
  }

  {
    doltlite_pollfd pfd;
    pfd.fd = rawSrvFd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    if (doltlitePoll(&pfd, 1, 2000) <= 0) {
      printf("FAIL  ClientHello did not arrive on server socket\n");
      goto cleanup;
    }
  }

  memset(fill, 0x5a, sizeof(fill));
  {
    size_t chunk = 1024;
    for (;;) {
      int flags = 0;
      int n;
#ifdef MSG_NOSIGNAL
      flags = MSG_NOSIGNAL;
#endif
#ifdef _WIN32
      n = send(rawSrvFd, (const char *)fill, (int)chunk, flags);
      if (n < 0) {
        if (WSAGetLastError() == WSAEWOULDBLOCK) {
          if (chunk > 1) { chunk = 1; continue; }
          {
            doltlite_pollfd pfd;
            pfd.fd = rawSrvFd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            if (doltlitePoll(&pfd, 1, 50) <= 0) break;
            continue;
          }
        }
        printf("FAIL  unexpected send error while filling buffer\n");
        goto cleanup;
      }
#else
      n = (int)send(rawSrvFd, fill, chunk, flags);
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          if (chunk > 1) { chunk = 1; continue; }
          {
            doltlite_pollfd pfd;
            pfd.fd = rawSrvFd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            if (doltlitePoll(&pfd, 1, 50) <= 0) break;
            continue;
          }
        }
        printf("FAIL  unexpected send error while filling buffer\n");
        goto cleanup;
      }
#endif
      if (n == 0) break;
      totalFilled += (size_t)n;
      if (totalFilled > 32 * 1024 * 1024) break;
    }
  }
  if (totalFilled == 0) {
    printf("FAIL  could not fill send buffer\n");
    goto cleanup;
  }

  {
    int lastStep = 0;
    for (i = 0; i < 50; i++) {
      int step = doltliteConnHandshakeStep(srvConn);
      lastStep = step;
      if (step == 2) {
        sawWantWrite = 1;
        break;
      }
      if (step < 0) {
        printf("FAIL  server handshake aborted on full send buffer (expected WANT_WRITE=2, got %d)\n", step);
        goto cleanup;
      }
      if (step == 0) {
        printf("FAIL  server handshake unexpectedly finished while send buffer was full\n");
        goto cleanup;
      }
      {
        doltlite_pollfd pfd;
        pfd.fd = cliFd;
        pfd.events = 0;
        pfd.revents = 0;
        doltlitePoll(&pfd, 1, 10);
      }
    }
    if (!sawWantWrite) {
      printf("FAIL  server handshake never returned WANT_WRITE on full send buffer (filled=%zu lastStep=%d)\n", totalFilled, lastStep);
      goto cleanup;
    }
  }

  while (drained < totalFilled) {
    size_t want = totalFilled - drained;
    int n;
    if (want > sizeof(fill)) want = sizeof(fill);
    n = doltliteSocketRecv(cliFd, fill, (int)want);
    if (n > 0) {
      drained += (size_t)n;
    } else if (n == -2) {
      doltlite_pollfd pfd;
      pfd.fd = cliFd;
      pfd.events = POLLIN;
      pfd.revents = 0;
      if (doltlitePoll(&pfd, 1, 1000) <= 0) {
        printf("FAIL  timed out draining filler bytes (%zu/%zu)\n", drained, totalFilled);
        goto cleanup;
      }
    } else {
      printf("FAIL  error draining filler bytes (%d)\n", n);
      goto cleanup;
    }
  }

  bio.holdRecv = 0;
  for (i = 0; i < 200 && (!srvDone || !cliDone); i++) {
    if (!srvDone) {
      int step = doltliteConnHandshakeStep(srvConn);
      if (step == 0) srvDone = 1;
      else if (step < 0) {
        printf("FAIL  server handshake failed after drain (%d)\n", step);
        goto cleanup;
      }
    }
    if (!cliDone) {
      int cret = mbedtls_ssl_handshake(&cliSsl);
      if (cret == 0) cliDone = 1;
      else if (cret != MBEDTLS_ERR_SSL_WANT_READ && cret != MBEDTLS_ERR_SSL_WANT_WRITE) {
        printf("FAIL  client handshake failed after drain (%d)\n", cret);
        goto cleanup;
      }
    }
    if (!srvDone || !cliDone) {
      doltlite_pollfd pfd;
      pfd.fd = cliFd;
      pfd.events = POLLIN;
      pfd.revents = 0;
      doltlitePoll(&pfd, 1, 10);
    }
  }
  if (!srvDone || !cliDone) {
    printf("FAIL  handshake did not complete after drain (srv=%d cli=%d)\n", srvDone, cliDone);
    goto cleanup;
  }

  printf("  PASS  non-blocking server handshake returns WANT_WRITE on EAGAIN and completes after drain\n");
  rc = 0;

cleanup:
  mbedtls_ssl_free(&cliSsl);
  mbedtls_ssl_config_free(&cliConf);
  mbedtls_ctr_drbg_free(&drbg);
  mbedtls_entropy_free(&entropy);
  if (srvConn) doltliteConnClose(srvConn);
  if (srvFd >= 0) doltliteCloseSocket(srvFd);
  if (cliFd >= 0) doltliteCloseSocket(cliFd);
  if (tlsSrv) doltliteTlsServerFree(tlsSrv);
  return rc;
}

static int testNonblockReadWantReadAndWantWrite(const char *certFile, const char *keyFile) {
  int srvFd = -1, rawSrvFd = -1, cliFd = -1;
  DoltliteTlsServer *tlsSrv = NULL;
  DoltliteConn *srvConn = NULL;
  mbedtls_ssl_context cliSsl;
  mbedtls_ssl_config cliConf;
  mbedtls_ctr_drbg_context drbg;
  mbedtls_entropy_context entropy;
  TestCliBio bio;
  unsigned char fill[4096];
  char readBuf[64];
  size_t totalFilled = 0;
  size_t drained = 0;
  int sawWantWrite = 0;
  int srvDone = 0, cliDone = 0;
  int i, n, rc = 1;

  mbedtls_ssl_init(&cliSsl);
  mbedtls_ssl_config_init(&cliConf);
  mbedtls_ctr_drbg_init(&drbg);
  mbedtls_entropy_init(&entropy);

  if (makeLoopbackPair(&srvFd, &cliFd) != 0) {
    printf("FAIL  makeLoopbackPair failed\n");
    goto cleanup;
  }

  tlsSrv = doltliteTlsServerNew(certFile, keyFile);
  if (!tlsSrv) {
    printf("FAIL  doltliteTlsServerNew failed\n");
    goto cleanup;
  }
  rawSrvFd = srvFd;
  srvConn = doltliteConnServerAttach(tlsSrv, srvFd);
  srvFd = -1;
  if (!srvConn) {
    printf("FAIL  doltliteConnServerAttach failed\n");
    goto cleanup;
  }

  n = doltliteConnReadNonblock(srvConn, readBuf, (int)sizeof(readBuf));
  if (n != -2) {
    printf("FAIL  nonblock read before ClientHello expected -2 (WANT_READ), got %d\n", n);
    goto cleanup;
  }

  if (mbedtls_ctr_drbg_seed(&drbg, mbedtls_entropy_func, &entropy,
                            (const unsigned char *)"tls-nb-rd", 9) != 0 ||
      mbedtls_ssl_config_defaults(&cliConf, MBEDTLS_SSL_IS_CLIENT,
                                  MBEDTLS_SSL_TRANSPORT_STREAM,
                                  MBEDTLS_SSL_PRESET_DEFAULT) != 0) {
    printf("FAIL  client TLS config failed\n");
    goto cleanup;
  }
  mbedtls_ssl_conf_min_tls_version(&cliConf, MBEDTLS_SSL_VERSION_TLS1_3);
  mbedtls_ssl_conf_authmode(&cliConf, MBEDTLS_SSL_VERIFY_NONE);
  mbedtls_ssl_conf_rng(&cliConf, mbedtls_ctr_drbg_random, &drbg);
  if (mbedtls_ssl_setup(&cliSsl, &cliConf) != 0) {
    printf("FAIL  mbedtls_ssl_setup failed\n");
    goto cleanup;
  }
  bio.fd = cliFd;
  bio.holdRecv = 1;
  mbedtls_ssl_set_bio(&cliSsl, &bio, testCliSend, testCliRecv, NULL);

  for (i = 0; i < 20; i++) {
    int cret = mbedtls_ssl_handshake(&cliSsl);
    if (cret == MBEDTLS_ERR_SSL_WANT_READ) break;
    if (cret != MBEDTLS_ERR_SSL_WANT_WRITE) {
      printf("FAIL  client ClientHello step returned %d\n", cret);
      goto cleanup;
    }
  }

  {
    doltlite_pollfd pfd;
    pfd.fd = rawSrvFd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    if (doltlitePoll(&pfd, 1, 2000) <= 0) {
      printf("FAIL  ClientHello did not arrive on server socket\n");
      goto cleanup;
    }
  }

  memset(fill, 0x5a, sizeof(fill));
  {
    size_t chunk = 1024;
    for (;;) {
      int flags = 0;
#ifdef MSG_NOSIGNAL
      flags = MSG_NOSIGNAL;
#endif
#ifdef _WIN32
      n = send(rawSrvFd, (const char *)fill, (int)chunk, flags);
      if (n < 0) {
        if (WSAGetLastError() == WSAEWOULDBLOCK) {
          if (chunk > 1) { chunk = 1; continue; }
          {
            doltlite_pollfd pfd;
            pfd.fd = rawSrvFd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            if (doltlitePoll(&pfd, 1, 50) <= 0) break;
            continue;
          }
        }
        printf("FAIL  unexpected send error while filling buffer\n");
        goto cleanup;
      }
#else
      n = (int)send(rawSrvFd, fill, chunk, flags);
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          if (chunk > 1) { chunk = 1; continue; }
          {
            doltlite_pollfd pfd;
            pfd.fd = rawSrvFd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            if (doltlitePoll(&pfd, 1, 50) <= 0) break;
            continue;
          }
        }
        printf("FAIL  unexpected send error while filling buffer\n");
        goto cleanup;
      }
#endif
      if (n == 0) break;
      totalFilled += (size_t)n;
      if (totalFilled > 32 * 1024 * 1024) break;
    }
  }
  if (totalFilled == 0) {
    printf("FAIL  could not fill send buffer\n");
    goto cleanup;
  }

  {
    int lastRet = 0;
    for (i = 0; i < 50; i++) {
      int r = doltliteConnReadNonblock(srvConn, readBuf, (int)sizeof(readBuf));
      lastRet = r;
      if (r == -3) {
        sawWantWrite = 1;
        break;
      }
      if (r == -1) {
        printf("FAIL  doltliteConnReadNonblock aborted on full send buffer (expected -3, got %d)\n", r);
        goto cleanup;
      }
      if (r >= 0) {
        printf("FAIL  doltliteConnReadNonblock unexpectedly returned %d while send buffer was full\n", r);
        goto cleanup;
      }
      {
        doltlite_pollfd pfd;
        pfd.fd = cliFd;
        pfd.events = 0;
        pfd.revents = 0;
        doltlitePoll(&pfd, 1, 10);
      }
    }
    if (!sawWantWrite) {
      printf("FAIL  doltliteConnReadNonblock never returned -3 (WANT_WRITE) on full send buffer (filled=%zu lastRet=%d)\n",
             totalFilled, lastRet);
      goto cleanup;
    }
  }

  while (drained < totalFilled) {
    size_t want = totalFilled - drained;
    if (want > sizeof(fill)) want = sizeof(fill);
    n = doltliteSocketRecv(cliFd, fill, (int)want);
    if (n > 0) {
      drained += (size_t)n;
    } else if (n == -2) {
      doltlite_pollfd pfd;
      pfd.fd = cliFd;
      pfd.events = POLLIN;
      pfd.revents = 0;
      if (doltlitePoll(&pfd, 1, 1000) <= 0) {
        printf("FAIL  timed out draining filler bytes (%zu/%zu)\n", drained, totalFilled);
        goto cleanup;
      }
    } else {
      printf("FAIL  error draining filler bytes (%d)\n", n);
      goto cleanup;
    }
  }

  bio.holdRecv = 0;
  for (i = 0; i < 200 && (!srvDone || !cliDone); i++) {
    if (!srvDone) {
      int step = doltliteConnHandshakeStep(srvConn);
      if (step == 0) srvDone = 1;
      else if (step < 0) {
        printf("FAIL  server handshake failed after drain (%d)\n", step);
        goto cleanup;
      }
    }
    if (!cliDone) {
      int cret = mbedtls_ssl_handshake(&cliSsl);
      if (cret == 0) cliDone = 1;
      else if (cret != MBEDTLS_ERR_SSL_WANT_READ && cret != MBEDTLS_ERR_SSL_WANT_WRITE) {
        printf("FAIL  client handshake failed after drain (%d)\n", cret);
        goto cleanup;
      }
    }
    if (!srvDone || !cliDone) {
      doltlite_pollfd pfd;
      pfd.fd = cliFd;
      pfd.events = POLLIN;
      pfd.revents = 0;
      doltlitePoll(&pfd, 1, 10);
    }
  }
  if (!srvDone || !cliDone) {
    printf("FAIL  handshake did not complete after drain (srv=%d cli=%d)\n", srvDone, cliDone);
    goto cleanup;
  }

  n = doltliteConnReadNonblock(srvConn, readBuf, (int)sizeof(readBuf));
  if (n != -2) {
    printf("FAIL  nonblock read on idle established TLS conn expected -2 (WANT_READ), got %d\n", n);
    goto cleanup;
  }

  for (i = 0; i < 50; i++) {
    int wret = mbedtls_ssl_write(&cliSsl, (const unsigned char *)"hello", 5);
    if (wret == 5) break;
    if (wret != MBEDTLS_ERR_SSL_WANT_READ && wret != MBEDTLS_ERR_SSL_WANT_WRITE) {
      printf("FAIL  client ssl_write returned %d\n", wret);
      goto cleanup;
    }
  }

  n = -2;
  for (i = 0; i < 100; i++) {
    n = doltliteConnReadNonblock(srvConn, readBuf, (int)sizeof(readBuf));
    if (n != -2 && n != -3) break;
    {
      doltlite_pollfd pfd;
      pfd.fd = rawSrvFd;
      pfd.events = (short)(n == -3 ? (POLLIN | POLLOUT) : POLLIN);
      pfd.revents = 0;
      doltlitePoll(&pfd, 1, 20);
    }
  }
  if (n != 5 || memcmp(readBuf, "hello", 5) != 0) {
    printf("FAIL  expected 5 bytes \"hello\" from nonblock read, got %d\n", n);
    goto cleanup;
  }

  printf("  PASS  doltliteConnReadNonblock distinguishes WANT_READ (-2) and WANT_WRITE (-3)\n");
  rc = 0;

cleanup:
  mbedtls_ssl_free(&cliSsl);
  mbedtls_ssl_config_free(&cliConf);
  mbedtls_ctr_drbg_free(&drbg);
  mbedtls_entropy_free(&entropy);
  if (srvConn) doltliteConnClose(srvConn);
  if (srvFd >= 0) doltliteCloseSocket(srvFd);
  if (cliFd >= 0) doltliteCloseSocket(cliFd);
  if (tlsSrv) doltliteTlsServerFree(tlsSrv);
  return rc;
}

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

  if (argc > 2 && argv[1][0] != '\0' && argv[2][0] != '\0') {
    if (testNonblockHandshakeWantWrite(argv[1], argv[2]) != 0) {
      fails++;
    }
    if (testNonblockReadWantReadAndWantWrite(argv[1], argv[2]) != 0) {
      fails++;
    }
  }

  probe = doltliteConnOpen(host, port, 0);
  if (!probe) {
    printf("SKIP  cannot reach %s:%d (offline?)\n", host, port);
    return fails ? 1 : 0;
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
