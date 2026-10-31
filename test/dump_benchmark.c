#include "doltlite.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <sys/resource.h>
#endif

static int run(sqlite3 *db, const char *sql){
  char *error=0;
  int rc=sqlite3_exec(db,sql,0,0,&error);
  if( rc ) fprintf(stderr,"%s\n",error ? error : sqlite3_errmsg(db));
  sqlite3_free(error);
  return rc;
}
static int verify(const char *source, const char *format, const char *path){
  sqlite3 *db=0,*out=0;
  sqlite3_stmt *a=0,*b=0;
  int rc=sqlite3_open_v2(source,&db,SQLITE_OPEN_READONLY,0);
  if( rc==SQLITE_OK ) rc=sqlite3_open_v2(!strcmp(format,"sql") ? "" : path,&out,
      !strcmp(format,"sql") ? SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE : SQLITE_OPEN_READONLY,0);
  if( rc==SQLITE_OK && !strcmp(format,"sql") ){
    FILE *input=fopen(path,"rb");
    char buffer[8192];
    sqlite3_str *line=sqlite3_str_new(out);
    if( !input ) return 1;
    while( rc==SQLITE_OK && fgets(buffer,sizeof(buffer),input) ){
      sqlite3_str_appendall(line,buffer);
      if( buffer[strlen(buffer)-1]=='\n' ){
        char *text=sqlite3_str_finish(line);
        rc=text ? run(out,text) : SQLITE_NOMEM;
        sqlite3_free(text);line=sqlite3_str_new(out);
      }
    }
    if( ferror(input) || sqlite3_str_length(line) ) rc=SQLITE_IOERR;
    sqlite3_free(sqlite3_str_finish(line));fclose(input);
  }
  if( rc==SQLITE_OK ) rc=sqlite3_prepare_v2(db,"SELECT id,payload FROM main.records ORDER BY id",-1,&a,0);
  if( rc==SQLITE_OK ) rc=sqlite3_prepare_v2(out,"SELECT id,payload FROM main.records ORDER BY id",-1,&b,0);
  while( rc==SQLITE_OK ){
    int left=sqlite3_step(a),right=sqlite3_step(b),n;
    if( left==SQLITE_DONE && right==SQLITE_DONE ) break;
    if( left!=SQLITE_ROW || right!=SQLITE_ROW ) {rc=SQLITE_ERROR;break;}
    n=sqlite3_column_bytes(a,1);
    if( sqlite3_column_int64(a,0)!=sqlite3_column_int64(b,0)
     || n!=sqlite3_column_bytes(b,1)
     || memcmp(sqlite3_column_blob(a,1),sqlite3_column_blob(b,1),n) ) rc=SQLITE_ERROR;
  }
  sqlite3_finalize(a);sqlite3_finalize(b);
  if( rc==SQLITE_OK ){
    rc=sqlite3_prepare_v2(out,"PRAGMA integrity_check",-1,&a,0);
    if( rc==SQLITE_OK ){
      if( sqlite3_step(a)!=SQLITE_ROW || strcmp((const char*)sqlite3_column_text(a,0),"ok") ) rc=SQLITE_CORRUPT;
    }
    sqlite3_finalize(a);
  }
  sqlite3_close(out);sqlite3_close(db);
  if( rc ) fprintf(stderr,"%s export verification failed: %s\n",format,sqlite3_errstr(rc));
  return rc ? 1 : 0;
}

static double seconds(void){
  struct timespec now;
  timespec_get(&now,TIME_UTC);
  return now.tv_sec+now.tv_nsec/1e9;
}
int main(int argc, char **argv){
  sqlite3 *db=0;
  sqlite3_stmt *q=0;
  sqlite3_int64 current=0, peak=0;
  double start;
  struct stat sourceStat, outputStat;
  int rc;
  if( argc!=4 && argc!=5 ) return 2;
  if( argc==5 && !strcmp(argv[1],"verify") ) return verify(argv[2],argv[3],argv[4]);
  if( !strcmp(argv[1],"seed") ){
    int mib=atoi(argv[3]);
    char *insert;
    if( argc!=4 || mib<=0 || mib>1024 ) return 2;
    rc=sqlite3_open(argv[2],&db);
    if( rc==SQLITE_OK ) rc=run(db,"CREATE TABLE records(id INTEGER PRIMARY KEY,payload BLOB); BEGIN");
    insert=sqlite3_mprintf("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<%d) "
                          "INSERT INTO records SELECT i,randomblob(65536) FROM n",mib*16);
    if( rc==SQLITE_OK ) rc=insert ? run(db,insert) : SQLITE_NOMEM;
    sqlite3_free(insert);
    if( rc==SQLITE_OK ) rc=run(db,"COMMIT; SELECT dolt_commit('-Am','export benchmark')");
    sqlite3_close(db);
    return rc ? 1 : 0;
  }
  if( strcmp(argv[1],"export") || argc!=5 ) return 2;
  start=seconds();
  rc=sqlite3_open_v2(argv[2],&db,SQLITE_OPEN_READONLY,0);
  if( rc==SQLITE_OK ) rc=sqlite3_prepare_v2(db,
      "SELECT dolt_dump('--file-name',?1,'-r',?2)",-1,&q,0);
  if( rc==SQLITE_OK ) rc=sqlite3_bind_text(q,1,argv[4],-1,SQLITE_STATIC);
  if( rc==SQLITE_OK ) rc=sqlite3_bind_text(q,2,argv[3],-1,SQLITE_STATIC);
  if( rc==SQLITE_OK ) rc=sqlite3_step(q);
  if( rc!=SQLITE_ROW ){
    fprintf(stderr,"export: %s\n",db ? sqlite3_errmsg(db) : sqlite3_errstr(rc));
    sqlite3_finalize(q);sqlite3_close(db);return 1;
  }
  sqlite3_finalize(q);
  sqlite3_status64(SQLITE_STATUS_MEMORY_USED,&current,&peak,0);
  if( stat(argv[2],&sourceStat) || stat(argv[4],&outputStat) ) return 1;
  printf("{\"format\":\"%s\",\"seconds\":%.6f,\"source_file_bytes\":%lld,"
         "\"output_file_bytes\":%lld,\"sqlite_peak_bytes\":%lld,\"peak_rss_bytes\":",
         argv[3],seconds()-start,(long long)sourceStat.st_size,
         (long long)outputStat.st_size,(long long)peak);
#ifndef _WIN32
  {
    struct rusage usage;
    if( getrusage(RUSAGE_SELF,&usage) ) return 1;
#ifdef __APPLE__
    printf("%lld",(long long)usage.ru_maxrss);
#else
    printf("%lld",(long long)usage.ru_maxrss*1024);
#endif
  }
#else
  printf("null");
#endif
  puts("}");
  sqlite3_close(db);
  return 0;
}
