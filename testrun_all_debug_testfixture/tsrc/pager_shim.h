
#ifndef SQLITE_PAGER_SHIM_H
#define SQLITE_PAGER_SHIM_H

#include "sqliteInt.h"

typedef struct PagerShim PagerShim;
typedef struct PagerOps PagerOps;
/* PagerShim is cast to Pager* at the callsite, so the magic MUST be
** at offset 0 — getPagerOps sniffs it blindly on every incoming
** Pager* to decide shim vs. orig dispatch. */
#define PAGER_SHIM_MAGIC 0x50534D31
struct PagerShim {
  u32 magic;
  const PagerOps *pOps;
  sqlite3_file *pFd;
  char *zFilename;
  char *zJournal;
  u8 eLock;
  u8 journalMode;
  u32 iDataVersion;
  sqlite3_vfs *pVfs;
};

PagerShim *pagerShimCreate(sqlite3_vfs *pVfs, const char *zFilename,
                           sqlite3_file *pFd);

void pagerShimDestroy(PagerShim *pShim);

sqlite3_file *sqlite3PagerFile(Pager*);

const char *sqlite3PagerFilename(const Pager*, int);

const char *sqlite3PagerJournalname(Pager*);

int sqlite3PagerGetJournalMode(Pager*);
int sqlite3PagerOkToChangeJournalMode(Pager*);
int sqlite3PagerSetJournalMode(Pager*, int);

int sqlite3PagerExclusiveLock(Pager*);
u8 sqlite3PagerIsreadonly(Pager*);

int sqlite3PagerRefcount(Pager*);

u32 sqlite3PagerDataVersion(Pager*);

void sqlite3PagerShrink(Pager*);
int sqlite3PagerFlush(Pager*);
void sqlite3PagerCacheStat(Pager*, int, int, u64*);
int sqlite3PagerIsMemdb(Pager*);
int sqlite3PagerLockingMode(Pager*, int);

#endif
