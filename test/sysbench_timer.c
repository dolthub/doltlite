#include "sqlite3.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct Str {
  char *z;
  size_t n;
  size_t cap;
} Str;

static long long now_us(void){
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

static int append(Str *s, const char *z){
  size_t n = strlen(z);
  if( s->n + n + 1 > s->cap ){
    size_t cap = s->cap ? s->cap * 2 : 4096;
    while( s->n + n + 1 > cap ) cap *= 2;
    char *p = (char*)realloc(s->z, cap);
    if( p==0 ) return 1;
    s->z = p;
    s->cap = cap;
  }
  memcpy(s->z + s->n, z, n);
  s->n += n;
  s->z[s->n] = 0;
  return 0;
}

static int is_marker(const char *z, const char *marker){
  size_t n = strlen(marker);
  return strncmp(z, marker, n)==0 && (z[n]=='\n' || z[n]=='\r' || z[n]==0);
}

static int read_sections(const char *path, Str *setup, Str *workload, Str *teardown){
  FILE *f = fopen(path, "r");
  char line[65536];
  int phase = 0;
  int saw_start = 0;
  int saw_end = 0;
  if( f==0 ){
    fprintf(stderr, "open %s: %s\n", path, strerror(errno));
    return 1;
  }
  while( fgets(line, sizeof(line), f) ){
    if( is_marker(line, ".print BENCH_START") ){
      phase = 1;
      saw_start = 1;
      continue;
    }
    if( is_marker(line, ".print BENCH_END") ){
      phase = 2;
      saw_end = 1;
      continue;
    }
    if( phase==0 ){
      if( append(setup, line) ) goto oom;
    }else if( phase==1 ){
      if( append(workload, line) ) goto oom;
    }else{
      if( append(teardown, line) ) goto oom;
    }
  }
  if( ferror(f) ){
    fprintf(stderr, "read %s: %s\n", path, strerror(errno));
    fclose(f);
    return 1;
  }
  fclose(f);
  if( !saw_start || !saw_end ){
    fprintf(stderr, "%s: missing BENCH_START/BENCH_END markers\n", path);
    return 1;
  }
  return 0;

oom:
  fprintf(stderr, "out of memory\n");
  fclose(f);
  return 1;
}

static int exec_sql(sqlite3 *db, const char *sql, const char *label){
  char *err = 0;
  int rc;
  if( sql==0 || sql[0]==0 ) return SQLITE_OK;
  rc = sqlite3_exec(db, sql, 0, 0, &err);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "%s: %s\n", label, err ? err : sqlite3_errmsg(db));
    sqlite3_free(err);
  }
  return rc;
}

int main(int argc, char **argv){
  sqlite3 *db = 0;
  Str setup = {0,0,0};
  Str workload = {0,0,0};
  Str teardown = {0,0,0};
  long long t0, t1;
  int rc;

  if( argc!=3 ){
    fprintf(stderr, "usage: %s DB SQL_FILE\n", argv[0]);
    printf("-1\n");
    return 0;
  }
  if( read_sections(argv[2], &setup, &workload, &teardown) ){
    printf("-1\n");
    goto done;
  }
  rc = sqlite3_open(argv[1], &db);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "open %s: %s\n", argv[1], db ? sqlite3_errmsg(db) : "out of memory");
    printf("-1\n");
    goto done;
  }
  if( exec_sql(db, setup.z, "setup")!=SQLITE_OK ){
    printf("-1\n");
    goto done;
  }
  t0 = now_us();
  if( exec_sql(db, workload.z, "workload")!=SQLITE_OK ){
    printf("-1\n");
    goto done;
  }
  t1 = now_us();
  if( exec_sql(db, teardown.z, "teardown")!=SQLITE_OK ){
    printf("-1\n");
    goto done;
  }
  printf("%lld\n", t1 - t0);

done:
  if( db ) sqlite3_close(db);
  free(setup.z);
  free(workload.z);
  free(teardown.z);
  return 0;
}
