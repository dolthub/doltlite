#ifndef DOLTLITE_REMOTESRV_H
#define DOLTLITE_REMOTESRV_H

typedef struct DoltliteServer DoltliteServer;

int doltliteServe(const char *zDir, int port);

DoltliteServer *doltliteServeAsync(const char *zDir, int port);
void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
