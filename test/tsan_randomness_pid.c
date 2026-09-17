/*
** One thread forces PRNG reseeds, which write os_unix.c's randomnessPid;
** the other opens databases, which read it. Built and run under
** ThreadSanitizer only.
*/
#include "sqlite3.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>

static atomic_int go = 1;

static void *reseed(void *arg){
  unsigned char buf[8];
  int i;
  for(i=0; i<20000 && go; i++){
    sqlite3_randomness(0, 0);
    sqlite3_randomness(sizeof buf, buf);
  }
  go = 0;
  return arg;
}

static void *opener(void *arg){
  while( go ){
    sqlite3 *db;
    if( sqlite3_open((const char*)arg, &db)==SQLITE_OK ){
      sqlite3_exec(db, "SELECT 1", 0, 0, 0);
      sqlite3_close(db);
    }
  }
  return 0;
}

int main(int argc, char **argv){
  pthread_t a, b;
  const char *zDb = argc>1 ? argv[1] : "tsan_randomness_pid.db";
  sqlite3_initialize();
  unlink(zDb);
  pthread_create(&b, 0, opener, (void*)zDb);
  pthread_create(&a, 0, reseed, 0);
  pthread_join(a, 0);
  pthread_join(b, 0);
  unlink(zDb);
  printf("tsan_randomness_pid: ok\n");
  return 0;
}
