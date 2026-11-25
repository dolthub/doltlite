/*
** Minimal sqlite3 allocator for standalone auth/TLS unit tests that compile
** doltlite_creds.c / doltlite_tls.c without linking libdoltlite.
** Production builds resolve these symbols from SQLite's memsys.
*/
#include <stdlib.h>

int sqlite3_alloc_stub_fail_after = -1;
int sqlite3_alloc_stub_live = 0;

void *sqlite3_malloc(int n){
  void *p;
  if( n<=0 ) return 0;
  if( sqlite3_alloc_stub_fail_after>=0 ){
    if( sqlite3_alloc_stub_fail_after==0 ){
      sqlite3_alloc_stub_fail_after = -1;
      return 0;
    }
    sqlite3_alloc_stub_fail_after--;
  }
  p = malloc((size_t)n);
  if( p ) sqlite3_alloc_stub_live++;
  return p;
}

void *sqlite3_realloc(void *p, int n){
  void *q;
  if( n<=0 ){
    if( p ){
      free(p);
      sqlite3_alloc_stub_live--;
    }
    return 0;
  }
  if( sqlite3_alloc_stub_fail_after>=0 ){
    if( sqlite3_alloc_stub_fail_after==0 ){
      sqlite3_alloc_stub_fail_after = -1;
      return 0;
    }
    sqlite3_alloc_stub_fail_after--;
  }
  q = realloc(p, (size_t)n);
  if( q && !p ) sqlite3_alloc_stub_live++;
  return q;
}

void sqlite3_free(void *p){
  if( p ){
    free(p);
    sqlite3_alloc_stub_live--;
  }
}
