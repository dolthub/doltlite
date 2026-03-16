#ifdef DOLTLITE_PROLLY

#include "prolly_arena.h"
#include <string.h>

/*
** Initialize an arena with the given default block size.
** No memory is allocated until the first call to prollyArenaAlloc.
*/
void prollyArenaInit(ProllyArena *a, int defaultBlockSize){
  memset(a, 0, sizeof(*a));
  a->defaultBlockSize = defaultBlockSize;
}

/*
** Allocate n bytes from the arena. Returns NULL on OOM.
** If the current block does not have enough room, a new block is allocated
** whose usable size is the larger of defaultBlockSize and n.
*/
void *prollyArenaAlloc(ProllyArena *a, int n){
  ProllyArenaBlock *b;
  void *p;

  b = a->pCurrent;
  if( b && (b->sz - b->used) >= n ){
    p = (u8*)(b + 1) + b->used;
    b->used += n;
    return p;
  }

  /* Need a new block. Size is at least defaultBlockSize or n. */
  {
    int blockSz = a->defaultBlockSize;
    if( n > blockSz ) blockSz = n;

    b = (ProllyArenaBlock*)sqlite3_malloc(sizeof(ProllyArenaBlock) + blockSz);
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

/*
** Allocate n bytes from the arena and zero-initialize them.
*/
void *prollyArenaAllocZero(ProllyArena *a, int n){
  void *p = prollyArenaAlloc(a, n);
  if( p ) memset(p, 0, n);
  return p;
}

/*
** Reset the arena: free all blocks except the first, reset the first
** block's usage counter to zero.
*/
void prollyArenaReset(ProllyArena *a){
  ProllyArenaBlock *b;

  b = a->pFirst;
  if( b==0 ) return;

  /* Free all blocks after the first */
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

/*
** Destroy the arena: free all blocks and zero out the struct.
*/
void prollyArenaFree(ProllyArena *a){
  ProllyArenaBlock *b = a->pFirst;
  while( b ){
    ProllyArenaBlock *pNext = b->pNext;
    sqlite3_free(b);
    b = pNext;
  }
  memset(a, 0, sizeof(*a));
}

#endif /* DOLTLITE_PROLLY */
