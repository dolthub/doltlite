
#include "doltlite_tls.h"

#include "mbedtls/ctr_drbg.h"
#include "mbedtls/entropy.h"
#include "mbedtls/net_sockets.h"
#include "mbedtls/pk.h"
#include "mbedtls/ssl.h"
#include "mbedtls/x509_crt.h"

#include "doltlite_net.h"
#ifdef _WIN32
#include <wincrypt.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *sqlite3_malloc(int);
void sqlite3_free(void*);

struct DoltliteConn {
  int useTls;
  int ownsConf;
  long long deadlineMs;
  int idleTimeoutMs;
  int timedOut;
  mbedtls_net_context net;

  mbedtls_ssl_context ssl;
  mbedtls_ssl_config conf;
  mbedtls_x509_crt cacert;
  mbedtls_ctr_drbg_context drbg;
  mbedtls_entropy_context entropy;
};

#ifndef DOLTLITE_AUTH_CLIENT_ONLY
struct DoltliteTlsServer {
  mbedtls_x509_crt cert;
  mbedtls_pk_context key;
  mbedtls_ssl_config conf;
  mbedtls_ctr_drbg_context drbg;
  mbedtls_entropy_context entropy;
};
#endif

static void configureSocket(int fd) {
#if !defined(_WIN32) && defined(SO_NOSIGPIPE)
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#else
  (void)fd;
#endif
}

static int connApplyDeadline(DoltliteConn *c) {
  long long remaining;
  if (c->deadlineMs == 0) return 0;
  remaining = c->deadlineMs - doltliteMonotonicMs();
  if (remaining <= 0) { c->timedOut = 1; return 1; }
  if (remaining > 0x7fffffff) remaining = 0x7fffffff;
  return doltliteSocketSetTimeout(c->net.fd, (int)remaining);
}

static int connIoResult(DoltliteConn *c, int n) {
  if (n > 0 && c->idleTimeoutMs > 0) {
    c->deadlineMs = doltliteMonotonicMs() + c->idleTimeoutMs;
  } else if (n < 0 && doltliteSocketTimedOut()) {
    c->timedOut = 1;
  }
  return n;
}

static int connSend(void *pCtx, const unsigned char *pBuf, size_t nBuf) {
  DoltliteConn *c = (DoltliteConn*)pCtx;
  int flags = 0, n;
  if (c->idleTimeoutMs && connApplyDeadline(c)) return MBEDTLS_ERR_SSL_TIMEOUT;
#ifdef MSG_NOSIGNAL
  flags = MSG_NOSIGNAL;
#endif
#ifdef _WIN32
  n = send(c->net.fd, (const char *)pBuf, (int)nBuf, flags);
#else
  n = (int)send(c->net.fd, pBuf, nBuf, flags);
#endif
  connIoResult(c, n);
  return n < 0 ? MBEDTLS_ERR_NET_SEND_FAILED : n;
}

static int connRecv(void *pCtx, unsigned char *pBuf, size_t nBuf) {
  DoltliteConn *c = (DoltliteConn*)pCtx;
  int n;
  if (c->idleTimeoutMs && connApplyDeadline(c)) return MBEDTLS_ERR_SSL_TIMEOUT;
  n = mbedtls_net_recv(&c->net, pBuf, nBuf);
  if (n > 0) connIoResult(c, n);
  else if (n < 0 && n != MBEDTLS_ERR_SSL_WANT_READ) connIoResult(c, n);
  return n;
}

#ifdef _WIN32
static int loadSystemRoots(mbedtls_x509_crt *ca) {
  HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
  PCCERT_CONTEXT ctx = NULL;
  int loaded = 0;
  if (!store) return 1;
  while ((ctx = CertEnumCertificatesInStore(store, ctx)) != NULL) {
    if (mbedtls_x509_crt_parse_der(ca, ctx->pbCertEncoded, ctx->cbCertEncoded) == 0) {
      loaded++;
    }
  }
  CertCloseStore(store, 0);
  return loaded > 0 ? 0 : 1;
}
#else
static const char *const CA_PATHS[] = {
    "/etc/ssl/cert.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    NULL,
};
#endif

static int loadTrustRoots(mbedtls_x509_crt *ca) {
  const char *env = getenv("DOLTLITE_CA_FILE");
  if (env && *env) {
    return (mbedtls_x509_crt_parse_file(ca, env) >= 0 && ca->version != 0) ? 0 : 1;
  }
#ifdef _WIN32
  return loadSystemRoots(ca);
#else
  {
    int i;
    for (i = 0; CA_PATHS[i] != NULL; i++) {
      if (mbedtls_x509_crt_parse_file(ca, CA_PATHS[i]) >= 0 && ca->version != 0) {
        return 0;
      }
    }
    return 1;
  }
#endif
}

DoltliteConn *doltliteConnOpenTimeout(
  const char *host,
  int port,
  int useTls,
  int timeoutMs
) {
  DoltliteConn *c = (DoltliteConn *)sqlite3_malloc((int)sizeof(*c));
  char portstr[16];
  int ret;

  if (!c) return NULL;
  memset(c, 0, sizeof(*c));
  c->useTls = useTls;
  c->ownsConf = 1;
  if (timeoutMs > 0) {
    c->deadlineMs = doltliteMonotonicMs() + timeoutMs;
  }
  mbedtls_net_init(&c->net);

  snprintf(portstr, sizeof(portstr), "%d", port);
  c->net.fd = doltliteTcpConnect(host, portstr, timeoutMs);
  if (c->net.fd < 0) goto fail_net;
  configureSocket(c->net.fd);
  if (connApplyDeadline(c) != 0) goto fail_net;
  if (!useTls) {
    return c;
  }

  mbedtls_ssl_init(&c->ssl);
  mbedtls_ssl_config_init(&c->conf);
  mbedtls_x509_crt_init(&c->cacert);
  mbedtls_ctr_drbg_init(&c->drbg);
  mbedtls_entropy_init(&c->entropy);

  if (mbedtls_ctr_drbg_seed(&c->drbg, mbedtls_entropy_func, &c->entropy,
                            (const unsigned char *)"doltlite-tls", 12) != 0) {
    goto fail_tls;
  }
  if (loadTrustRoots(&c->cacert) != 0) {
    goto fail_tls;
  }
  if (mbedtls_ssl_config_defaults(&c->conf, MBEDTLS_SSL_IS_CLIENT,
                                  MBEDTLS_SSL_TRANSPORT_STREAM,
                                  MBEDTLS_SSL_PRESET_DEFAULT) != 0) {
    goto fail_tls;
  }
  mbedtls_ssl_conf_min_tls_version(&c->conf, MBEDTLS_SSL_VERSION_TLS1_3);
  mbedtls_ssl_conf_authmode(&c->conf, MBEDTLS_SSL_VERIFY_REQUIRED);
  mbedtls_ssl_conf_ca_chain(&c->conf, &c->cacert, NULL);
  mbedtls_ssl_conf_rng(&c->conf, mbedtls_ctr_drbg_random, &c->drbg);

  if (mbedtls_ssl_setup(&c->ssl, &c->conf) != 0) goto fail_tls;

  if (mbedtls_ssl_set_hostname(&c->ssl, host) != 0) goto fail_tls;
  mbedtls_ssl_set_bio(&c->ssl, c, connSend, connRecv, NULL);

  do {
    if (connApplyDeadline(c) != 0) goto fail_tls;
    ret = mbedtls_ssl_handshake(&c->ssl);
  } while (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE);
  if (ret != 0) goto fail_tls;
  if (mbedtls_ssl_get_verify_result(&c->ssl) != 0) goto fail_tls;

  return c;

fail_tls:
  mbedtls_ssl_free(&c->ssl);
  mbedtls_ssl_config_free(&c->conf);
  mbedtls_x509_crt_free(&c->cacert);
  mbedtls_ctr_drbg_free(&c->drbg);
  mbedtls_entropy_free(&c->entropy);
fail_net:
  mbedtls_net_free(&c->net);
  sqlite3_free(c);
  return NULL;
}

int doltliteConnWriteAll(DoltliteConn *c, const void *buf, int nbuf) {
  const unsigned char *p = (const unsigned char *)buf;
  int off = 0;
  while (off < nbuf) {
    int n;
    if (connApplyDeadline(c) != 0) return 1;
    if (c->useTls) {
      n = mbedtls_ssl_write(&c->ssl, p + off, (size_t)(nbuf - off));
    } else {
      n = connSend(c, p + off, (size_t)(nbuf - off));
    }
    if (n == MBEDTLS_ERR_SSL_WANT_READ || n == MBEDTLS_ERR_SSL_WANT_WRITE) continue;
    if (n <= 0) return 1;
    off += n;
  }
  return 0;
}

static int connReadOnce(DoltliteConn *c, void *buf, int nbuf) {
  int n;
  if (c->useTls) {
    n = mbedtls_ssl_read(&c->ssl, (unsigned char *)buf, (size_t)nbuf);
  } else {
    n = connRecv(c, (unsigned char *)buf, (size_t)nbuf);
  }
  if (n == MBEDTLS_ERR_SSL_WANT_READ || n == MBEDTLS_ERR_SSL_WANT_WRITE) return -2;
  if (c->useTls && n == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY) return 0;
  if (n < 0) return -1;
  return n;
}

int doltliteConnRead(DoltliteConn *c, void *buf, int nbuf) {
  int n;
  for (;;) {
    if (connApplyDeadline(c) != 0) return -1;
    n = connReadOnce(c, buf, nbuf);
    if (n == -2) continue;
    return n;
  }
}

int doltliteConnReadNonblock(DoltliteConn *c, void *buf, int nbuf) {
  if (!c || nbuf <= 0) return -1;
  if (!c->useTls) return doltliteSocketRecv(c->net.fd, buf, nbuf);
  return connReadOnce(c, buf, nbuf);
}

int doltliteConnSetTimeout(DoltliteConn *c, int timeoutMs) {
  c->idleTimeoutMs = 0;
  c->timedOut = 0;
  c->deadlineMs = timeoutMs > 0 ? doltliteMonotonicMs() + timeoutMs : 0;
  if (timeoutMs <= 0) return doltliteSocketSetTimeout(c->net.fd, 0);
  return connApplyDeadline(c);
}

int doltliteConnSetIdleTimeout(DoltliteConn *c, int idleMs, int initialMs) {
  int rc = doltliteConnSetTimeout(c, initialMs);
  if (!rc) c->idleTimeoutMs = idleMs > 0 ? idleMs : 0;
  return rc;
}

int doltliteConnTimeoutRemaining(DoltliteConn *c) {
  long long remaining = c->deadlineMs - doltliteMonotonicMs();
  if (c->timedOut || remaining <= 0) return 0;
  return remaining > 0x7fffffff ? 0x7fffffff : (int)remaining;
}

void doltliteConnClose(DoltliteConn *c) {
  if (!c) return;
  if (c->useTls) {
    mbedtls_ssl_close_notify(&c->ssl);
    mbedtls_ssl_free(&c->ssl);

    if (c->ownsConf) {
      mbedtls_ssl_config_free(&c->conf);
      mbedtls_x509_crt_free(&c->cacert);
      mbedtls_ctr_drbg_free(&c->drbg);
      mbedtls_entropy_free(&c->entropy);
    }
  }
  mbedtls_net_free(&c->net);
  sqlite3_free(c);
}

#ifndef DOLTLITE_AUTH_CLIENT_ONLY
DoltliteTlsServer *doltliteTlsServerNew(const char *certFile, const char *keyFile) {
  DoltliteTlsServer *s = (DoltliteTlsServer *)sqlite3_malloc((int)sizeof(*s));
  if (!s) return NULL;
  memset(s, 0, sizeof(*s));

  mbedtls_x509_crt_init(&s->cert);
  mbedtls_pk_init(&s->key);
  mbedtls_ssl_config_init(&s->conf);
  mbedtls_ctr_drbg_init(&s->drbg);
  mbedtls_entropy_init(&s->entropy);

  if (mbedtls_ctr_drbg_seed(&s->drbg, mbedtls_entropy_func, &s->entropy,
                            (const unsigned char *)"doltlite-tls-srv", 16) != 0) {
    goto fail;
  }
  if (mbedtls_x509_crt_parse_file(&s->cert, certFile) != 0) goto fail;
  if (mbedtls_pk_parse_keyfile(&s->key, keyFile, NULL,
                               mbedtls_ctr_drbg_random, &s->drbg) != 0) {
    goto fail;
  }
  if (mbedtls_ssl_config_defaults(&s->conf, MBEDTLS_SSL_IS_SERVER,
                                  MBEDTLS_SSL_TRANSPORT_STREAM,
                                  MBEDTLS_SSL_PRESET_DEFAULT) != 0) {
    goto fail;
  }
  mbedtls_ssl_conf_min_tls_version(&s->conf, MBEDTLS_SSL_VERSION_TLS1_3);
  mbedtls_ssl_conf_rng(&s->conf, mbedtls_ctr_drbg_random, &s->drbg);

  mbedtls_ssl_conf_authmode(&s->conf, MBEDTLS_SSL_VERIFY_NONE);
  if (mbedtls_ssl_conf_own_cert(&s->conf, &s->cert, &s->key) != 0) goto fail;

  return s;

fail:
  doltliteTlsServerFree(s);
  return NULL;
}

void doltliteTlsServerFree(DoltliteTlsServer *s) {
  if (!s) return;
  mbedtls_ssl_config_free(&s->conf);
  mbedtls_x509_crt_free(&s->cert);
  mbedtls_pk_free(&s->key);
  mbedtls_ctr_drbg_free(&s->drbg);
  mbedtls_entropy_free(&s->entropy);
  sqlite3_free(s);
}

DoltliteConn *doltliteConnServerAttach(DoltliteTlsServer *s, int clientFd) {
  DoltliteConn *c = (DoltliteConn *)sqlite3_malloc((int)sizeof(*c));
  if (!c) {
    doltliteCloseSocket(clientFd);
    return NULL;
  }
  memset(c, 0, sizeof(*c));
  c->useTls = 1;
  c->ownsConf = 0;
  mbedtls_net_init(&c->net);
  c->net.fd = clientFd;
  configureSocket(clientFd);
  mbedtls_ssl_init(&c->ssl);

  if (mbedtls_ssl_setup(&c->ssl, &s->conf) != 0) goto fail;
  mbedtls_ssl_set_bio(&c->ssl, c, connSend, connRecv, NULL);
  return c;

fail:
  mbedtls_ssl_free(&c->ssl);
  mbedtls_net_free(&c->net);
  sqlite3_free(c);
  return NULL;
}

int doltliteConnHandshakeStep(DoltliteConn *c) {
  int ret;
  if (!c || !c->useTls) return -1;
  ret = mbedtls_ssl_handshake(&c->ssl);
  if (ret == 0) return 0;
  if (ret == MBEDTLS_ERR_SSL_WANT_READ) return 1;
  if (ret == MBEDTLS_ERR_SSL_WANT_WRITE) return 2;
  return -1;
}

DoltliteConn *doltliteConnFromFd(int clientFd) {
  DoltliteConn *c = (DoltliteConn *)sqlite3_malloc((int)sizeof(*c));
  if (!c) {
    doltliteCloseSocket(clientFd);
    return NULL;
  }
  memset(c, 0, sizeof(*c));
  c->useTls = 0;
  c->ownsConf = 0;
  mbedtls_net_init(&c->net);
  c->net.fd = clientFd;
  configureSocket(clientFd);
  return c;
}
#endif
