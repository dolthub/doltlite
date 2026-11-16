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

/* zPreReceive names a program run before a push moves a ref. It is told
** the ref, its old and new commit and who pushed through DOLTLITE_*
** environment variables; any exit status but 0 refuses the push. */
typedef struct DoltliteServeHooks {
  const char *zPreReceive;
  int preReceiveTimeoutMs;
} DoltliteServeHooks;

DoltliteServer *doltliteServeAsyncHooks(const DoltliteServeOpts *opts,
                                        const DoltliteServeHooks *hooks);

void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
