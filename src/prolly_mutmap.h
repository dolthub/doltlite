/* Sorted map of pending INSERT/DELETE edits, consumed by prolly_mutate. */
#ifndef SQLITE_PROLLY_MUTMAP_H
#define SQLITE_PROLLY_MUTMAP_H

#include "sqliteInt.h"

#define PROLLY_EDIT_INSERT 1
#define PROLLY_EDIT_DELETE 2

typedef struct ProllyMutMap ProllyMutMap;
typedef struct ProllyMutMapEntry ProllyMutMapEntry;
typedef struct ProllyMutMapIter ProllyMutMapIter;

struct ProllyMutMapEntry {
  u8 op;                 
  u8 isIntKey;           
  i64 intKey;            
  u8 *pKey;              
  int nKey;              
  u8 *pVal;              
  int nVal;              
};

struct ProllyMutMap {
  u8 isIntKey;            
  int nEntries;           
  int nAlloc;             
  ProllyMutMapEntry *aEntries;  
};

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey);

int prollyMutMapInsert(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal);

int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey);

int prollyMutMapCount(ProllyMutMap *mm);

int prollyMutMapIsEmpty(ProllyMutMap *mm);

struct ProllyMutMapIter {
  ProllyMutMap *pMap;
  int idx;               
};

void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm);

void prollyMutMapIterNext(ProllyMutMapIter *it);

int prollyMutMapIterValid(ProllyMutMapIter *it);

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it);

void prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                          const u8 *pKey, int nKey, i64 intKey);

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm);

int prollyMutMapMerge(ProllyMutMap *pDst, ProllyMutMap *pSrc);

void prollyMutMapClear(ProllyMutMap *mm);

void prollyMutMapFree(ProllyMutMap *mm);

#endif 
