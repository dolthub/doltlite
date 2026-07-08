
#include "doltlite_tls.h"

#include "mbedtls/ctr_drbg.h"
#include "mbedtls/entropy.h"
#include "mbedtls/net_sockets.h"
#include "mbedtls/pk.h"
#include "mbedtls/ssl.h"
#include "mbedtls/x509_crt.h"

#include "doltlite_net.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct DoltliteConn {
  int useTls;
  int ownsConf;
  mbedtls_net_context net;

  mbedtls_ssl_context ssl;
  mbedtls_ssl_config conf;
  mbedtls_x509_crt cacert;
  mbedtls_ctr_drbg_context drbg;
  mbedtls_entropy_context entropy;
};

struct DoltliteTlsServer {
  mbedtls_x509_crt cert;
  mbedtls_pk_context key;
  mbedtls_ssl_config conf;
  mbedtls_ctr_drbg_context drbg;
  mbedtls_entropy_context entropy;
};

static const char *const CA_PATHS[] = {
    "/etc/ssl/cert.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    NULL,
};

static int loadTrustRoots(mbedtls_x509_crt *ca) {
  const char *env = getenv("DOLTLITE_CA_FILE");
  int i;
  if (env && *env) {

    return (mbedtls_x509_crt_parse_file(ca, env) >= 0 && ca->version != 0) ? 0 : 1;
  }
  for (i = 0; CA_PATHS[i] != NULL; i++) {
    if (mbedtls_x509_crt_parse_file(ca, CA_PATHS[i]) >= 0 && ca->version != 0) {
      return 0;
    }
  }
  return 1;
}

DoltliteConn *doltliteConnOpen(const char *host, int port, int useTls) {
  DoltliteConn *c = (DoltliteConn *)calloc(1, sizeof(*c));
  char portstr[16];
  int ret;

  if (!c) return NULL;
  c->useTls = useTls;
  c->ownsConf = 1;
  mbedtls_net_init(&c->net);

  snprintf(portstr, sizeof(portstr), "%d", port);
  if (mbedtls_net_connect(&c->net, host, portstr, MBEDTLS_NET_PROTO_TCP) != 0) {
    goto fail_net;
  }
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
  mbedtls_ssl_conf_authmode(&c->conf, MBEDTLS_SSL_VERIFY_REQUIRED);
  mbedtls_ssl_conf_ca_chain(&c->conf, &c->cacert, NULL);
  mbedtls_ssl_conf_rng(&c->conf, mbedtls_ctr_drbg_random, &c->drbg);

  if (mbedtls_ssl_setup(&c->ssl, &c->conf) != 0) goto fail_tls;

  if (mbedtls_ssl_set_hostname(&c->ssl, host) != 0) goto fail_tls;
  mbedtls_ssl_set_bio(&c->ssl, &c->net, mbedtls_net_send, mbedtls_net_recv, NULL);

  do {
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
  free(c);
  return NULL;
}

int doltliteConnWriteAll(DoltliteConn *c, const void *buf, int nbuf) {
  const unsigned char *p = (const unsigned char *)buf;
  int off = 0;
  while (off < nbuf) {
    int n;
    if (c->useTls) {
      n = mbedtls_ssl_write(&c->ssl, p + off, (size_t)(nbuf - off));
    } else {
      n = (int)mbedtls_net_send(&c->net, p + off, (size_t)(nbuf - off));
    }
    if (n == MBEDTLS_ERR_SSL_WANT_READ || n == MBEDTLS_ERR_SSL_WANT_WRITE) continue;
    if (n <= 0) return 1;
    off += n;
  }
  return 0;
}

int doltliteConnRead(DoltliteConn *c, void *buf, int nbuf) {
  int n;
  for (;;) {
    if (c->useTls) {
      n = mbedtls_ssl_read(&c->ssl, (unsigned char *)buf, (size_t)nbuf);
    } else {
      n = (int)mbedtls_net_recv(&c->net, (unsigned char *)buf, (size_t)nbuf);
    }
    if (n == MBEDTLS_ERR_SSL_WANT_READ || n == MBEDTLS_ERR_SSL_WANT_WRITE) continue;

    if (c->useTls && n == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY) return 0;
    if (n < 0) return -1;
    return n;
  }
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
  free(c);
}

DoltliteTlsServer *doltliteTlsServerNew(const char *certFile, const char *keyFile) {
  DoltliteTlsServer *s = (DoltliteTlsServer *)calloc(1, sizeof(*s));
  if (!s) return NULL;

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
  free(s);
}

DoltliteConn *doltliteConnServerAccept(DoltliteTlsServer *s, int clientFd) {
  DoltliteConn *c = (DoltliteConn *)calloc(1, sizeof(*c));
  int ret;
  if (!c) {
    doltliteCloseSocket(clientFd);
    return NULL;
  }
  c->useTls = 1;
  c->ownsConf = 0;
  mbedtls_net_init(&c->net);
  c->net.fd = clientFd;
  mbedtls_ssl_init(&c->ssl);

  if (mbedtls_ssl_setup(&c->ssl, &s->conf) != 0) goto fail;
  mbedtls_ssl_set_bio(&c->ssl, &c->net, mbedtls_net_send, mbedtls_net_recv, NULL);
  do {
    ret = mbedtls_ssl_handshake(&c->ssl);
  } while (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE);
  if (ret != 0) goto fail;
  return c;

fail:
  mbedtls_ssl_free(&c->ssl);
  mbedtls_net_free(&c->net);
  free(c);
  return NULL;
}

DoltliteConn *doltliteConnFromFd(int clientFd) {
  DoltliteConn *c = (DoltliteConn *)calloc(1, sizeof(*c));
  if (!c) {
    doltliteCloseSocket(clientFd);
    return NULL;
  }
  c->useTls = 0;
  c->ownsConf = 0;
  mbedtls_net_init(&c->net);
  c->net.fd = clientFd;
  return c;
}
