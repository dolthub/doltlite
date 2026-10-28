#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "sqlite3.h"
#ifdef _WIN32
#include <windows.h>
#endif

static double nowMs(void){
#ifdef _WIN32
  LARGE_INTEGER count, freq;
  QueryPerformanceCounter(&count);
  QueryPerformanceFrequency(&freq);
  return (double)count.QuadPart * 1000.0 / (double)freq.QuadPart;
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec*1000.0 + ts.tv_nsec/1e6;
#endif
}

static void require(sqlite3 *db, int ok, const char *zWhat){
  if( !ok ){
    fprintf(stderr, "%s: %s\n", zWhat, db ? sqlite3_errmsg(db) : "failed");
    exit(1);
  }
}

static void readBlob(sqlite3 *db, sqlite3_blob *pBlob, int expected){
  char a[9] = {0};
  require(db, sqlite3_blob_bytes(pBlob)==100, "blob size");
  require(db, sqlite3_blob_read(pBlob, a, 8, 0)==SQLITE_OK, "blob read");
  require(db, atoi(a)==expected, "blob contents");
}

int main(int argc, char **argv){
  sqlite3 *db = 0;
  int nRow, nProbe, table;
  char *zSql;
  if( argc!=4 && argc!=5 ){
    fprintf(stderr, "usage: %s NEW_DATABASE ROWS PROBES [MAX_COLD_BATCH_MS]\n", argv[0]);
    return 2;
  }
  nRow = atoi(argv[2]);
  nProbe = atoi(argv[3]);
  if( nRow<1 || nRow>5000000 || nProbe<1 ) return 2;
  require(db, sqlite3_open(argv[1], &db)==SQLITE_OK, "open");
  zSql = sqlite3_mprintf(
      "CREATE TABLE ck(k TEXT PRIMARY KEY,b BLOB);"
      "CREATE TABLE r(id INTEGER PRIMARY KEY,b BLOB);"
      "BEGIN; WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL "
      "SELECT i+1 FROM n WHERE i<%d) INSERT INTO ck "
      "SELECT printf('%%08d',i),CAST(printf('%%08d%%092d',i,0) AS BLOB) FROM n;"
      "INSERT INTO r SELECT rowid,b FROM ck; COMMIT;", nRow);
  require(db, sqlite3_exec(db, zSql, 0, 0, 0)==SQLITE_OK, "setup");
  sqlite3_free(zSql);
  for(table=0; table<2; table++){
    const char *zTable = table ? "r" : "ck";
    sqlite3_int64 *aRowid = malloc((size_t)nRow*sizeof(*aRowid));
    int *aExpected = malloc((size_t)nRow*sizeof(*aExpected));
    sqlite3_stmt *pStmt = 0;
    sqlite3_blob *pBlob = 0;
    double start, coldFirst, coldBatch, coldLast, reopens, opens;
    int i = 0, rc;
    require(db, aRowid!=0 && aExpected!=0, "allocation");
    zSql = sqlite3_mprintf("SELECT rowid,CAST(substr(b,1,8) AS INTEGER) "
                          "FROM %s ORDER BY b", zTable);
    require(db, sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0)==SQLITE_OK, "rowids");
    sqlite3_free(zSql);
    while( (rc=sqlite3_step(pStmt))==SQLITE_ROW ){
      require(db, i<nRow, "row count");
      aRowid[i] = sqlite3_column_int64(pStmt, 0);
      aExpected[i++] = sqlite3_column_int(pStmt, 1);
    }
    require(db, rc==SQLITE_DONE && i==nRow, "row count");
    require(db, sqlite3_finalize(pStmt)==SQLITE_OK, "finalize");
    require(db, sqlite3_close(db)==SQLITE_OK, "close");
    db = 0;
    require(db, sqlite3_open(argv[1], &db)==SQLITE_OK, "cold open");
    start = nowMs();
    require(db, sqlite3_blob_open(db,"main",zTable,"b",aRowid[0],0,&pBlob)==SQLITE_OK, "first open");
    readBlob(db, pBlob, aExpected[0]);
    coldFirst = nowMs()-start;
    for(i=0; i<nProbe; i++){
      int j = (int)(((sqlite3_uint64)i*2654435761u) % (unsigned)nRow);
      require(db, sqlite3_blob_reopen(pBlob,aRowid[j])==SQLITE_OK, "cold reopen");
      readBlob(db, pBlob, aExpected[j]);
    }
    coldBatch = nowMs()-start;
    require(db, sqlite3_blob_close(pBlob)==SQLITE_OK, "blob close");
    require(db, sqlite3_close(db)==SQLITE_OK, "close");
    db = 0;
    require(db, sqlite3_open(argv[1], &db)==SQLITE_OK, "cold open");
    start = nowMs();
    require(db, sqlite3_blob_open(db,"main",zTable,"b",aRowid[nRow-1],0,&pBlob)==SQLITE_OK, "last open");
    readBlob(db, pBlob, aExpected[nRow-1]);
    coldLast = nowMs()-start;
    start = nowMs();
    for(i=0; i<nProbe; i++){
      int j = (int)(((sqlite3_uint64)i*2654435761u) % (unsigned)nRow);
      require(db, sqlite3_blob_reopen(pBlob,aRowid[j])==SQLITE_OK, "reopen");
      readBlob(db, pBlob, aExpected[j]);
    }
    reopens = nowMs()-start;
    require(db, sqlite3_blob_close(pBlob)==SQLITE_OK, "blob close");
    start = nowMs();
    for(i=0; i<nProbe; i++){
      int j = (int)(((sqlite3_uint64)i*2654435761u) % (unsigned)nRow);
      require(db, sqlite3_blob_open(db,"main",zTable,"b",aRowid[j],0,&pBlob)==SQLITE_OK, "warm open");
      readBlob(db, pBlob, aExpected[j]);
      require(db, sqlite3_blob_close(pBlob)==SQLITE_OK, "blob close");
    }
    opens = nowMs()-start;
    printf("{\"table\":\"%s\",\"rows\":%d,\"probes\":%d,"
           "\"cold_first_ms\":%.6f,\"cold_last_ms\":%.6f,"
           "\"cold_reopen_ms\":%.6f,"
           "\"reopen_ms\":%.6f,\"open_ms\":%.6f}\n",
           zTable,nRow,nProbe,coldFirst,coldLast,coldBatch,reopens,opens);
    if( argc==5 && coldBatch>atof(argv[4]) ){
      fprintf(stderr,"%s cold batch %.3f ms exceeds %.3f ms\n",zTable,coldBatch,atof(argv[4]));
      return 1;
    }
    free(aRowid);
    free(aExpected);
  }
  require(db, sqlite3_close(db)==SQLITE_OK, "close");
  return 0;
}
