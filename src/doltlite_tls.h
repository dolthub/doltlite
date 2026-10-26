#ifndef DOLTLITE_TLS_H
#define DOLTLITE_TLS_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoltliteConn DoltliteConn;

DoltliteConn *doltliteConnOpenTimeout(
  const char *host,
  int port,
  int useTls,
  int timeoutMs
);
static inline DoltliteConn *doltliteConnOpen(const char *host, int port, int useTls){
  return doltliteConnOpenTimeout(host, port, useTls, 0);
}

#ifndef DOLTLITE_AUTH_CLIENT_ONLY
typedef struct DoltliteTlsServer DoltliteTlsServer;

DoltliteTlsServer *doltliteTlsServerNew(const char *certFile, const char *keyFile);
void doltliteTlsServerFree(DoltliteTlsServer *s);

/* Own clientFd. Handshake is doltliteConnHandshakeStep; the socket must be
** nonblocking or that step waits inside recv. */
DoltliteConn *doltliteConnServerAttach(DoltliteTlsServer *s, int clientFd);
/* 0 done, 1 want read, 2 want write, -1 failed. */
int doltliteConnHandshakeStep(DoltliteConn *conn);
DoltliteConn *doltliteConnFromFd(int clientFd);
#endif

/* >0 bytes, 0 on EOF, -1 on error, -2 if a nonblocking read would wait. */
int doltliteConnReadNonblock(DoltliteConn *conn, void *buf, int nbuf);

int doltliteConnWriteAll(DoltliteConn *conn, const void *buf, int nbuf);

int doltliteConnRead(DoltliteConn *conn, void *buf, int nbuf);

int doltliteConnSetTimeout(DoltliteConn *conn, int timeoutMs);
int doltliteConnSetIdleTimeout(DoltliteConn*, int idleMs, int initialMs);
int doltliteConnTimeoutRemaining(DoltliteConn*);

void doltliteConnClose(DoltliteConn *conn);

#ifdef __cplusplus
}
#endif

#endif
