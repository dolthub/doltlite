/*
** Pager shim: minimal Pager facade for code that calls sqlite3BtreePager().
** Provides filename, file handle, and dummy journal mode responses
** without the real pager implementation.
*/
#ifndef SQLITE_PAGER_SHIM_H
#define SQLITE_PAGER_SHIM_H

#include "sqliteInt.h"

/*
** Shim Pager structure — enough fields to satisfy the 18 files
** that call sqlite3BtreePager() and then use the result.
*/
typedef struct PagerShim PagerShim;
typedef struct PagerOps PagerOps;
#define PAGER_SHIM_MAGIC 0x50534D31  /* "PSM1" */
struct PagerShim {
  u32 magic;                   /* PAGER_SHIM_MAGIC — identifies this as PagerShim */
  const PagerOps *pOps;        /* Vtable for shim or original pager dispatch */
  sqlite3_file *pFd;        /* File descriptor (may be NULL for :memory:) */
  char *zFilename;           /* Database filename */
  char *zJournal;            /* Journal filename (always empty string) */
  u8 eState;                 /* Pager state for compatibility */
  u8 eLock;                  /* Current lock level */
  u8 journalMode;            /* Journal mode (WAL for doltlite databases) */
  u8 noSync;                 /* Disable sync */
  u32 iDataVersion;          /* Data version counter */
  int nRef;                  /* Outstanding page references (always 0) */
  i64 nMmapSize;             /* Memory-mapped size (always 0) */
  sqlite3_vfs *pVfs;         /* VFS reference */
};

/* Create a pager shim for a given filename */
PagerShim *pagerShimCreate(sqlite3_vfs *pVfs, const char *zFilename,
                           sqlite3_file *pFd);

/* Destroy a pager shim */
void pagerShimDestroy(PagerShim *pShim);

/*
** The following functions match the subset of Pager API that external
** code actually uses after calling sqlite3BtreePager().
** They are implemented so that a PagerShim* can be cast to Pager*
** and these calls work.
*/

/* Get the file descriptor */
sqlite3_file *sqlite3PagerFile(Pager*);

/* Get filename */
const char *sqlite3PagerFilename(const Pager*, int);

/* Get journal name */
const char *sqlite3PagerJournalname(Pager*);

/* Get/set journal mode */
int sqlite3PagerGetJournalMode(Pager*);
int sqlite3PagerOkToChangeJournalMode(Pager*);
int sqlite3PagerSetJournalMode(Pager*, int);

/* Lock queries */
int sqlite3PagerExclusiveLock(Pager*);
u8 sqlite3PagerIsreadonly(Pager*);

/* Ref count */
int sqlite3PagerRefcount(Pager*);

/* Data version */
u32 sqlite3PagerDataVersion(Pager*);

/* No-op stubs */
void sqlite3PagerShrink(Pager*);
int sqlite3PagerFlush(Pager*);
void sqlite3PagerCacheStat(Pager*, int, int, u64*);
int sqlite3PagerIsMemdb(Pager*);
int sqlite3PagerLockingMode(Pager*, int);

#endif /* SQLITE_PAGER_SHIM_H */
