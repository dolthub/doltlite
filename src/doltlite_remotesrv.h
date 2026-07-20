#ifndef DOLTLITE_REMOTESRV_H
#define DOLTLITE_REMOTESRV_H

typedef struct DoltliteServer DoltliteServer;

typedef struct DoltliteServeOpts {
  const char *zDir;
  int port;
  const char *zBindAddr;
  const char *certFile;
  const char *keyFile;
  const char *authKeysDir;
  const char *audience;
  int timeoutMs;
} DoltliteServeOpts;

int doltliteServe(const char *zDir, int port, const char *zBindAddr);
DoltliteServer *doltliteServeAsync(const char *zDir, int port,
                                   const char *zBindAddr);

int doltliteServeOpts(const DoltliteServeOpts *opts);
DoltliteServer *doltliteServeAsyncOpts(const DoltliteServeOpts *opts);

void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
