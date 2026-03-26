#ifndef DOLTLITE_REMOTESRV_H
#define DOLTLITE_REMOTESRV_H

typedef struct DoltliteServer DoltliteServer;

/*
** Start serving doltlite databases in a directory over HTTP.
** Each .doltlite file in zDir is accessible at /{dbname}/...
** Blocks until stopped. Returns SQLITE_OK on clean shutdown.
*/
int doltliteServe(const char *zDir, int port);

/*
** Non-blocking: start server in background thread.
** Use doltliteServerStop() to shut down.
*/
DoltliteServer *doltliteServeAsync(const char *zDir, int port);
void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
