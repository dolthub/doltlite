#ifdef DOLTLITE_PROLLY

#include "prolly_arena.h"
#include <string.h>

void prollyArenaInit(ProllyArena *a, int defaultBlockSize){
  memset(a, 0, sizeof(*a));
  a->defaultBlockSize = defaultBlockSize;
}

void *prollyArenaAlloc(ProllyArena *a, int n){
  ProllyArenaBlock *b;
  void *p;

  b = a->pCurrent;

  if( b ){
    int aligned_used = (b->used + 7) & ~7;
    if( (b->sz - aligned_used) >= n ){
      p = (u8*)(b + 1) + aligned_used;
      b->used = aligned_used + n;
      return p;
    }
  }


  {
    int blockSz = a->defaultBlockSize;
    int totalSz;
    if( n > blockSz ) blockSz = n;

    totalSz = (int)sizeof(ProllyArenaBlock) + blockSz;
    if( totalSz < blockSz ){
      return 0;
    }
    b = (ProllyArenaBlock*)sqlite3_malloc(totalSz);
    if( b==0 ) return 0;

    b->sz = blockSz;
    b->used = n;
    b->pNext = 0;

    if( a->pCurrent ){
      a->pCurrent->pNext = b;
    }else{
      a->pFirst = b;
    }
    a->pCurrent = b;

    return (u8*)(b + 1);
  }
}

void *prollyArenaAllocZero(ProllyArena *a, int n){
  void *p = prollyArenaAlloc(a, n);
  if( p ) memset(p, 0, n);
  return p;
}

void prollyArenaReset(ProllyArena *a){
  ProllyArenaBlock *b;

  b = a->pFirst;
  if( b==0 ) return;


  {
    ProllyArenaBlock *pNext;
    ProllyArenaBlock *p = b->pNext;
    while( p ){
      pNext = p->pNext;
      sqlite3_free(p);
      p = pNext;
    }
    b->pNext = 0;
  }

  b->used = 0;
  a->pCurrent = b;
}

void prollyArenaFree(ProllyArena *a){
  ProllyArenaBlock *b = a->pFirst;
  while( b ){
    ProllyArenaBlock *pNext = b->pNext;
    sqlite3_free(b);
    b = pNext;
  }
  memset(a, 0, sizeof(*a));
}

#endif
