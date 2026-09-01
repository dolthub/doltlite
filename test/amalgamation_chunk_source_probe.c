#include "sqlite3.h"
#include <stdio.h>
#include <string.h>

#ifndef DOLTLITE_ENABLE_CHUNK_SOURCE
# define DOLTLITE_ENABLE_CHUNK_SOURCE 1
#endif

extern int doltliteInstallAutoExt(void);

static int sourceGet(
  void *pCtx,
  const unsigned char aHash[20],
  unsigned char **ppBytes,
  int *pnBytes
){
  (void)pCtx;
  (void)aHash;
  *ppBytes = 0;
  *pnBytes = 0;
  return DOLTLITE_SOURCE_NOTFOUND;
}

static int sourceGetMany(
  void *pCtx,
  int nHash,
  const unsigned char *aHash,
  unsigned char **apBytes,
  int *anBytes
){
  int i;
  (void)pCtx;
  (void)aHash;
  for(i=0; i<nHash; i++){
    apBytes[i] = 0;
    anBytes[i] = 0;
  }
  return DOLTLITE_SOURCE_OK;
}

int main(void){
  doltlite_chunk_source source;
  sqlite3 *db = 0;
  int rc;

  memset(&source, 0, sizeof(source));
  source.iVersion = 1;
  source.xGet = sourceGet;
  source.xGetMany = sourceGetMany;
  if( doltliteInstallAutoExt()!=SQLITE_OK ) return 1;
  if( sqlite3_open(":memory:", &db)!=SQLITE_OK ) return 1;
  rc = doltlite_set_chunk_source(db, "main", &source);
#if DOLTLITE_ENABLE_CHUNK_SOURCE
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "enabled registration failed: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }
  if( doltlite_init_lazy(db, 0, 0)!=SQLITE_MISUSE ){
    fprintf(stderr, "enabled bootstrap did not validate refs\n");
    sqlite3_close(db);
    return 1;
  }
#else
  if( rc!=SQLITE_NOTFOUND
   || !strstr(sqlite3_errmsg(db), "chunk source support is disabled") ){
    fprintf(stderr, "unexpected disabled registration: %s\n",
            sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }
  if( doltlite_init_lazy(db, 0, 0)!=SQLITE_NOTFOUND ){
    fprintf(stderr, "disabled bootstrap unexpectedly succeeded\n");
    sqlite3_close(db);
    return 1;
  }
#endif
  if( doltlite_set_chunk_source(db, "main", 0)!=SQLITE_OK ){
    sqlite3_close(db);
    return 1;
  }
  sqlite3_close(db);
  return 0;
}
