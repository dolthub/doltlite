#ifndef DOLTLITE_REMOTESRV_H
#define DOLTLITE_REMOTESRV_H

typedef struct DoltliteServer DoltliteServer;

/* Listens on 127.0.0.1 by default. Use doltliteServeBind for other addresses. */
int doltliteServe(const char *zDir, int port);

/* Listens on the given dotted-quad IPv4 address (e.g. "0.0.0.0",
** "127.0.0.1", "192.168.1.5"). If zBindAddr is NULL, defaults to
** "127.0.0.1". The remote protocol has no auth or TLS — see issue #228.
*/
int doltliteServeBind(const char *zDir, int port, const char *zBindAddr);

DoltliteServer *doltliteServeAsync(const char *zDir, int port);
DoltliteServer *doltliteServeAsyncBind(const char *zDir, int port,
                                       const char *zBindAddr);
void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
