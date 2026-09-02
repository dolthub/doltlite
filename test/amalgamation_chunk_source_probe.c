#include "sqlite3.h"
#include <ctype.h>
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

#if !DOLTLITE_ENABLE_CHUNK_SOURCE
static int containsHash(const char *z){
  int nHex = 0;
  while( z && *z ){
    if( isxdigit((unsigned char)*z) ){
      nHex++;
      if( nHex>=40 ) return 1;
    }else{
      nHex = 0;
    }
    z++;
  }
  return 0;
}

static int checkDisabledLazy(const char *zPath){
  sqlite3 *db = 0;
  sqlite3_stmt *pStmt = 0;
  char *zUri;
  const char *zErr;
  int extendedRc;
  int rc;

  zUri = sqlite3_mprintf("file:%s?lazy_origin=1", zPath);
  if( !zUri ) return 1;
  rc = sqlite3_open_v2(zUri, &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, 0);
  sqlite3_free(zUri);
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(
        db, "SELECT count(*) FROM users", -1, &pStmt, 0);
  }
  if( rc==SQLITE_OK ) rc = sqlite3_step(pStmt);
  extendedRc = db ? sqlite3_extended_errcode(db) : rc;
  zErr = db ? sqlite3_errmsg(db) : "";
  if( extendedRc!=SQLITE_IOERR_CHUNK_SOURCE
   || !strstr(zErr, "chunk source support is disabled")
   || !containsHash(zErr) ){
    fprintf(stderr, "unexpected disabled lazy-store error rc=%d: %s\n",
            extendedRc, zErr);
    sqlite3_finalize(pStmt);
    sqlite3_close(db);
    return 1;
  }
  sqlite3_finalize(pStmt);
  sqlite3_close(db);
  return 0;
}
#endif

int main(int argc, char **argv){
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
  if( argc!=2 ){
    fprintf(stderr, "usage: %s LAZY_DB\n", argv[0]);
    sqlite3_close(db);
    return 1;
  }
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
#if !DOLTLITE_ENABLE_CHUNK_SOURCE
  if( checkDisabledLazy(argv[1]) ) return 1;
#else
  (void)argc;
  (void)argv;
#endif
  return 0;
}
