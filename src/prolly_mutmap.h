/*
** Mutable map: skip list for buffering pending edits before tree flush.
** Supports insert, delete, and ordered iteration.
*/
#ifndef SQLITE_PROLLY_MUTMAP_H
#define SQLITE_PROLLY_MUTMAP_H

#include "sqliteInt.h"

/* Max skip list levels */
#define PROLLY_SKIPLIST_MAXLEVEL 16

/* Edit operation types */
#define PROLLY_EDIT_INSERT 1
#define PROLLY_EDIT_DELETE 2

typedef struct ProllyMutMap ProllyMutMap;
typedef struct ProllyMutMapEntry ProllyMutMapEntry;
typedef struct ProllyMutMapIter ProllyMutMapIter;

struct ProllyMutMapEntry {
  u8 op;                 /* PROLLY_EDIT_INSERT or PROLLY_EDIT_DELETE */
  u8 isIntKey;           /* True if integer key */
  i64 intKey;            /* Integer key (for INTKEY tables) */
  u8 *pKey;              /* Blob key data (for INDEX tables) */
  int nKey;              /* Blob key size */
  u8 *pVal;              /* Value data (NULL for delete) */
  int nVal;              /* Value size */
  int nLevel;            /* Skip list level for this entry */
  ProllyMutMapEntry *aForward[];  /* Forward pointers [nLevel] */
};

struct ProllyMutMap {
  u8 isIntKey;            /* Table type */
  int nEntries;           /* Number of entries */
  int maxLevel;           /* Current max level in use */
  ProllyMutMapEntry *pHeader;  /* Skip list header (sentinel) */
  sqlite3_int64 prng;    /* PRNG state for level generation */
};

/* Initialize a mutable map */
int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey);

/* Insert or update a key-value pair. Copies key and value data. */
int prollyMutMapInsert(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal);

/* Mark a key for deletion */
int prollyMutMapDelete(ProllyMutMap *mm,
                       const u8 *pKey, int nKey, i64 intKey);

/* Look up a key. Returns the entry if found, NULL if not.
** For INTKEY: pass intKey. For BLOBKEY: pass pKey/nKey. */
ProllyMutMapEntry *prollyMutMapFind(ProllyMutMap *mm,
                                     const u8 *pKey, int nKey, i64 intKey);

/* Get number of pending edits */
int prollyMutMapCount(ProllyMutMap *mm);

/* Check if map is empty */
int prollyMutMapIsEmpty(ProllyMutMap *mm);

/* Iterator for ordered traversal */
struct ProllyMutMapIter {
  ProllyMutMap *pMap;
  ProllyMutMapEntry *pCurrent;
};

/* Initialize iterator at first entry */
void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm);

/* Advance iterator */
void prollyMutMapIterNext(ProllyMutMapIter *it);

/* Check if iterator is valid */
int prollyMutMapIterValid(ProllyMutMapIter *it);

/* Get current entry from iterator */
ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it);

/* Merge all entries from pSrc into pDst. pSrc is emptied. */
int prollyMutMapMerge(ProllyMutMap *pDst, ProllyMutMap *pSrc);

/* Deep-clone a mutable map */
ProllyMutMap *prollyMutMapClone(ProllyMutMap *mm);

/* Clear all entries */
void prollyMutMapClear(ProllyMutMap *mm);

/* Free all resources */
void prollyMutMapFree(ProllyMutMap *mm);

#endif /* SQLITE_PROLLY_MUTMAP_H */
