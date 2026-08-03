/*
** Minimal sqlite3 allocator for standalone auth/TLS unit tests that compile
** doltlite_creds.c / doltlite_tls.c without linking libdoltlite.
** Production builds resolve these symbols from SQLite's memsys.
*/
#include <stdlib.h>

void *sqlite3_malloc(int n){
  if( n<=0 ) return 0;
  return malloc((size_t)n);
}

void *sqlite3_realloc(void *p, int n){
  if( n<=0 ){
    free(p);
    return 0;
  }
  return realloc(p, (size_t)n);
}

void sqlite3_free(void *p){
  free(p);
}
