
#ifndef SQLITE_PROLLY_ARENA_H
#define SQLITE_PROLLY_ARENA_H

#include "sqliteInt.h"

typedef struct ProllyArena ProllyArena;
typedef struct ProllyArenaBlock ProllyArenaBlock;

struct ProllyArenaBlock {
  ProllyArenaBlock *pNext;
  int sz;
  int used;

};

struct ProllyArena {
  ProllyArenaBlock *pFirst;
  ProllyArenaBlock *pCurrent;
  int defaultBlockSize;
};

void prollyArenaInit(ProllyArena *a, int defaultBlockSize);

void *prollyArenaAlloc(ProllyArena *a, int n);

void *prollyArenaAllocZero(ProllyArena *a, int n);

void prollyArenaReset(ProllyArena *a);

void prollyArenaFree(ProllyArena *a);

#endif
