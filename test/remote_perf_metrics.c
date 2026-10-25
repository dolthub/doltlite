#include "../ext/misc/vfsstat.c"
#include <sys/resource.h>

static void remotePerfReset(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc;
  (void)argv;
  memset(aVfsCnt, 0, sizeof(aVfsCnt));
  sqlite3_result_text(ctx, "METRICS RESET", -1, SQLITE_STATIC);
}

static void remotePerfMetrics(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3_uint64 nRead = 0, nWrite = 0, nReadCall = 0, nWriteCall = 0;
  sqlite3_int64 nRss;
  struct rusage usage;
  char *z;
  int i;
  (void)argc;
  (void)argv;
  for(i=0; i<VFSSTAT_ANY; i++){
    nRead += STATCNT(i, VFSSTAT_BYTESIN);
    nWrite += STATCNT(i, VFSSTAT_BYTESOUT);
    nReadCall += STATCNT(i, VFSSTAT_READ);
    nWriteCall += STATCNT(i, VFSSTAT_WRITE);
  }
  if( getrusage(RUSAGE_SELF, &usage)!=0 ){
    sqlite3_result_error(ctx, "getrusage failed", -1);
    return;
  }
  nRss = usage.ru_maxrss;
#ifndef __APPLE__
  nRss *= 1024;
#endif
  z = sqlite3_mprintf("%llu|%llu|%llu|%llu|%lld", nRead, nWrite,
                      nReadCall, nWriteCall, nRss);
  sqlite3_result_text(ctx, z, -1, sqlite3_free);
}

static int remotePerfRegister(sqlite3 *db, char **pzErr,
                              const sqlite3_api_routines *pApi){
  int rc;
  (void)pzErr;
  (void)pApi;
  rc = sqlite3_create_function(db, "remote_perf_reset", 0, SQLITE_UTF8,
                               0, remotePerfReset, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "remote_perf_metrics", 0, SQLITE_UTF8,
                                 0, remotePerfMetrics, 0, 0);
  }
  return rc;
}

int sqlite3_remoteperfmetrics_init(sqlite3 *db, char **pzErr,
                                  const sqlite3_api_routines *pApi){
  int rc = sqlite3_vfsstat_init(db, pzErr, pApi);
  if( rc!=SQLITE_OK_LOAD_PERMANENTLY ) return rc;
  rc = remotePerfRegister(db, pzErr, pApi);
  if( rc==SQLITE_OK ){
    rc = sqlite3_auto_extension((void(*)(void))remotePerfRegister);
  }
  return rc==SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
}
