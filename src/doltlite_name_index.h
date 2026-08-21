#ifndef DOLTLITE_NAME_INDEX_H
#define DOLTLITE_NAME_INDEX_H

#include "sqlite3.h"
#include <string.h>
#include <stddef.h>

/* Open-addressing name to row index. Names are read from the caller's array
** at lookup, so in-place rewrites stay valid (dolt_commit -A). NULL names
** skipped; first duplicate wins. Lookup returns 0-based index or -1. */
typedef struct DoltliteNameIndex DoltliteNameIndex;
struct DoltliteNameIndex {
  int *aSlot;
  int nSlot;
  const char *aBase;
  int stride;
  int nameOff;
};

static inline unsigned doltliteNameIndexHash(const char *z){
  unsigned h = 2166136261u;
  while( z && *z ){ h ^= (unsigned char)*z++; h *= 16777619u; }
  return h;
}

static inline const char *doltliteNameIndexRowName(
  const DoltliteNameIndex *p, int row
){
  return *(const char *const *)(p->aBase + (size_t)row*p->stride + p->nameOff);
}

static inline int doltliteNameIndexInit(
  DoltliteNameIndex *p, const void *aBase, int nElem, int stride, int nameOff
){
  int nSlot = 16, i;
  memset(p, 0, sizeof(*p));
  p->aBase = (const char*)aBase;
  p->stride = stride;
  p->nameOff = nameOff;
  if( nElem<=0 ) return SQLITE_OK;
  while( nSlot < nElem*2 ) nSlot *= 2;
  p->aSlot = (int*)sqlite3_malloc(nSlot * (int)sizeof(int));
  if( !p->aSlot ) return SQLITE_NOMEM;
  memset(p->aSlot, 0, nSlot * (int)sizeof(int));
  p->nSlot = nSlot;
  for(i=0; i<nElem; i++){
    const char *zName = doltliteNameIndexRowName(p, i);
    unsigned slot;
    if( !zName ) continue;
    slot = doltliteNameIndexHash(zName) & (unsigned)(nSlot - 1);
    while( p->aSlot[slot] ){
      if( strcmp(doltliteNameIndexRowName(p, p->aSlot[slot]-1), zName)==0 ) break;
      slot = (slot + 1) & (unsigned)(nSlot - 1);
    }
    if( !p->aSlot[slot] ) p->aSlot[slot] = i + 1;
  }
  return SQLITE_OK;
}

static inline int doltliteNameIndexFind(
  const DoltliteNameIndex *p, const char *zName
){
  unsigned slot;
  int i;
  if( !zName || p->nSlot==0 ) return -1;
  slot = doltliteNameIndexHash(zName) & (unsigned)(p->nSlot - 1);
  for(i=0; i<p->nSlot; i++){
    int row = p->aSlot[slot];
    if( !row ) return -1;
    if( strcmp(doltliteNameIndexRowName(p, row-1), zName)==0 ) return row-1;
    slot = (slot + 1) & (unsigned)(p->nSlot - 1);
  }
  return -1;
}

static inline void doltliteNameIndexFree(DoltliteNameIndex *p){
  sqlite3_free(p->aSlot);
  memset(p, 0, sizeof(*p));
}

#endif
