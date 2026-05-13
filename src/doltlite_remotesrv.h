#ifndef DOLTLITE_REMOTESRV_H
#define DOLTLITE_REMOTESRV_H

typedef struct DoltliteServer DoltliteServer;

/* Listens on the given dotted-quad IPv4 address (e.g. "0.0.0.0",
** "127.0.0.1", "192.168.1.5"). If zBindAddr is NULL or empty,
** defaults to "127.0.0.1". The remote protocol has no auth or TLS —
** see issue #228.
*/
int doltliteServe(const char *zDir, int port, const char *zBindAddr);

DoltliteServer *doltliteServeAsync(const char *zDir, int port,
                                   const char *zBindAddr);
void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
