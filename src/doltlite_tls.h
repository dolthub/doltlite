#ifndef DOLTLITE_TLS_H
#define DOLTLITE_TLS_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoltliteConn DoltliteConn;

DoltliteConn *doltliteConnOpen(const char *host, int port, int useTls);
DoltliteConn *doltliteConnOpenTimeout(
  const char *host,
  int port,
  int useTls,
  int timeoutMs
);

#ifndef DOLTLITE_AUTH_CLIENT_ONLY
typedef struct DoltliteTlsServer DoltliteTlsServer;

DoltliteTlsServer *doltliteTlsServerNew(const char *certFile, const char *keyFile);
void doltliteTlsServerFree(DoltliteTlsServer *s);

DoltliteConn *doltliteConnServerAccept(DoltliteTlsServer *s, int clientFd);
DoltliteConn *doltliteConnFromFd(int clientFd);
#endif

int doltliteConnWriteAll(DoltliteConn *conn, const void *buf, int nbuf);

int doltliteConnRead(DoltliteConn *conn, void *buf, int nbuf);

void doltliteConnClose(DoltliteConn *conn);

#ifdef __cplusplus
}
#endif

#endif
