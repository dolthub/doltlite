/*
** Pager shim implementation.
**
** When DOLTLITE_PROLLY is defined, the real pager.c is not linked.
** Instead, sqlite3BtreePager() returns a PagerShim* cast to Pager*.
** This file implements the subset of sqlite3Pager* functions that are
** actually called by the 18 files which use sqlite3BtreePager().
**
** The PagerShim struct is defined in pager_shim.h. External code
** receives a Pager* which is really a PagerShim*, so every function
** here casts its Pager* argument to PagerShim* before accessing fields.
*/
#ifdef DOLTLITE_PROLLY

#include "pager_shim.h"
#include "sqliteInt.h"

/* Globals expected by test code */
int sqlite3_pager_writej_count = 0;
int sqlite3_pager_readdb_count = 0;
int sqlite3_pager_writedb_count = 0;
int sqlite3_opentemp_count = 0;

/* Shared cache list (empty — no shared cache in prolly) */
BtShared *SQLITE_WSD sqlite3SharedCacheList = 0;

#include <string.h>

#define SHIM(p) ((PagerShim*)(p))
#define IS_SHIM(p) ((p) && ((PagerShim*)(p))->magic == PAGER_SHIM_MAGIC)

/* Forward declaration for original pager dispatch */
extern u32 orig_sqlite3PagerDataVersion(Pager *pPager);
extern sqlite3_file *orig_sqlite3PagerFile(Pager *pPager);
extern const char *orig_sqlite3PagerFilename(const Pager *pPager, int);
extern const char *orig_sqlite3PagerJournalname(Pager *pPager);
extern int orig_sqlite3PagerJournalMode(Pager *pPager, int);
extern int orig_sqlite3PagerGetJournalMode(Pager *pPager);
extern int orig_sqlite3PagerNosync(Pager *pPager);
extern int orig_sqlite3PagerGet(Pager*, Pgno, DbPage**, int);
extern void *orig_sqlite3PagerGetData(DbPage*);
extern void *orig_sqlite3PagerGetExtra(DbPage*);
extern void orig_sqlite3PagerUnref(DbPage*);
extern void orig_sqlite3PagerUnrefNotNull(DbPage*);
extern void orig_sqlite3PagerUnrefPageOne(DbPage*);
extern int orig_sqlite3PagerWrite(DbPage*);
extern Pgno orig_sqlite3PagerPagenumber(DbPage*);
extern int orig_sqlite3PagerPageRefcount(DbPage*);
extern int orig_sqlite3PagerSharedLock(Pager*);
extern int orig_sqlite3PagerOpenWal(Pager*, int*);
extern int orig_sqlite3PagerWalFramesize(Pager*);
extern int orig_sqlite3PagerIsMemdb(Pager*);
extern void orig_sqlite3PagerSetBusyHandler(Pager*, int(*)(void*), void*);
extern void orig_sqlite3PagerShrink(Pager*);
extern void orig_sqlite3PagerSetFlags(Pager*, unsigned);
extern int orig_sqlite3PagerLockingMode(Pager*, int);
extern int orig_sqlite3PagerSetPagesize(Pager*, u32*, int);
extern int orig_sqlite3PagerOkToChangeJournalMode(Pager*);
extern i64 orig_sqlite3PagerJournalSizeLimit(Pager*, i64);
extern sqlite3_backup **orig_sqlite3PagerBackupPtr(Pager*);
extern int orig_sqlite3PagerBegin(Pager*, int, int);
extern int orig_sqlite3PagerRollback(Pager*);
extern int orig_sqlite3PagerSync(Pager*, const char*);
extern int orig_sqlite3PagerFlush(Pager*);
extern void orig_sqlite3PagerClearCache(Pager*);
extern int orig_sqlite3PagerNosync(Pager*);
extern sqlite3_vfs *orig_sqlite3PagerVfs(Pager*);
extern int orig_sqlite3PagerJournalMode(Pager*, int);
extern Pgno orig_sqlite3PagerMaxPageCount(Pager*, Pgno);
extern int orig_sqlite3PagerSetJournalMode(Pager*, int);
extern int orig_sqlite3PagerExclusiveLock(Pager*);
extern u8 orig_sqlite3PagerIsreadonly(Pager*);
extern int orig_sqlite3PagerRefcount(Pager*);
extern void orig_sqlite3PagerCacheStat(Pager*, int, int, u64*);
extern int orig_sqlite3PagerMemUsed(Pager*);
extern sqlite3_file *orig_sqlite3PagerJrnlFile(Pager*);
extern int orig_sqlite3PagerOpenSavepoint(Pager*, int);
extern int orig_sqlite3PagerSavepoint(Pager*, int, int);
extern int orig_sqlite3PagerClose(Pager*, sqlite3*);
extern int orig_sqlite3PagerReadFileheader(Pager*, int, unsigned char*);
extern int orig_sqlite3PagerMovepage(Pager*, DbPage*, Pgno, int);
extern void *orig_sqlite3PagerTempSpace(Pager*);
extern DbPage *orig_sqlite3PagerLookup(Pager*, Pgno);
extern int orig_sqlite3PagerCheckpoint(Pager*, sqlite3*, int, int*, int*);
extern int orig_sqlite3PagerWalSupported(Pager*);
extern int orig_sqlite3PagerWalCallback(Pager*);
extern int orig_sqlite3PagerCloseWal(Pager*, sqlite3*);
extern int *orig_sqlite3PagerStats(Pager*);
extern void orig_sqlite3PagerSetCachesize(Pager*, int);
extern int orig_sqlite3PagerSetSpillsize(Pager*, int);
extern void orig_sqlite3PagerSetMmapLimit(Pager*, sqlite3_int64);
extern void orig_sqlite3PagerTruncateImage(Pager*, Pgno);
extern void orig_sqlite3PagerClearCache(Pager*);

/* -----------------------------------------------------------------------
** Dummy file object for :memory: databases
** ----------------------------------------------------------------------- */
static int dummyClose(sqlite3_file *p){ (void)p; return SQLITE_OK; }
static int dummyRead(sqlite3_file *p, void *b, int n, sqlite3_int64 o){
  (void)p; (void)b; (void)n; (void)o; return SQLITE_IOERR_SHORT_READ;
}
static int dummyWrite(sqlite3_file *p, const void *b, int n, sqlite3_int64 o){
  (void)p; (void)b; (void)n; (void)o; return SQLITE_OK;
}
static int dummyTruncate(sqlite3_file *p, sqlite3_int64 s){
  (void)p; (void)s; return SQLITE_OK;
}
static int dummySync(sqlite3_file *p, int f){ (void)p; (void)f; return SQLITE_OK; }
static int dummyFileSize(sqlite3_file *p, sqlite3_int64 *s){
  (void)p; *s = 0; return SQLITE_OK;
}
static int dummyLock(sqlite3_file *p, int l){ (void)p; (void)l; return SQLITE_OK; }
static int dummyUnlock(sqlite3_file *p, int l){ (void)p; (void)l; return SQLITE_OK; }
static int dummyCheckLock(sqlite3_file *p, int *r){ (void)p; *r = 0; return SQLITE_OK; }
static int dummyFileControl(sqlite3_file *p, int op, void *a){
  (void)p; (void)op; (void)a; return SQLITE_NOTFOUND;
}
static int dummySectorSize(sqlite3_file *p){ (void)p; return 4096; }
static int dummyDevChar(sqlite3_file *p){ (void)p; return 0; }
static const sqlite3_io_methods dummyIoMethods = {
  1,                  /* iVersion */
  dummyClose,         /* xClose */
  dummyRead,          /* xRead */
  dummyWrite,         /* xWrite */
  dummyTruncate,      /* xTruncate */
  dummySync,          /* xSync */
  dummyFileSize,      /* xFileSize */
  dummyLock,          /* xLock */
  dummyUnlock,        /* xUnlock */
  dummyCheckLock,     /* xCheckReservedLock */
  dummyFileControl,   /* xFileControl */
  dummySectorSize,    /* xSectorSize */
  dummyDevChar        /* xDeviceCharacteristics */
};
static sqlite3_file dummyFileObj;
static sqlite3_file *pagerShimDummyFile(void){
  dummyFileObj.pMethods = &dummyIoMethods;
  return &dummyFileObj;
}

/* -----------------------------------------------------------------------
** Lifecycle: create and destroy
** ----------------------------------------------------------------------- */

/*
** Allocate a new PagerShim.
**
**   pVfs       — VFS to remember (may be NULL)
**   zFilename  — database file path; NULL and "" are both valid
**   pFd        — open file descriptor (may be NULL for :memory:)
**
** Returns NULL on OOM.
*/
PagerShim *pagerShimCreate(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  sqlite3_file *pFd
){
  PagerShim *pShim;

  pShim = (PagerShim*)sqlite3_malloc(sizeof(PagerShim));
  if( pShim==0 ) return 0;
  memset(pShim, 0, sizeof(PagerShim));
  pShim->magic = PAGER_SHIM_MAGIC;

  /* Copy the filename.  Treat NULL as empty string. */
  if( zFilename && zFilename[0] ){
    int n = (int)strlen(zFilename);
    pShim->zFilename = (char*)sqlite3_malloc(n + 1);
    if( pShim->zFilename==0 ){
      sqlite3_free(pShim);
      return 0;
    }
    memcpy(pShim->zFilename, zFilename, n + 1);
  }else{
    pShim->zFilename = (char*)sqlite3_malloc(1);
    if( pShim->zFilename==0 ){
      sqlite3_free(pShim);
      return 0;
    }
    pShim->zFilename[0] = '\0';
  }

  /* Journal name is always an empty string for the shim. */
  pShim->zJournal = (char*)sqlite3_malloc(1);
  if( pShim->zJournal==0 ){
    sqlite3_free(pShim->zFilename);
    sqlite3_free(pShim);
    return 0;
  }
  pShim->zJournal[0] = '\0';

  /* Provide a dummy file object when pFd is NULL (e.g., :memory: dbs). */
  if( pFd==0 ){
    pFd = pagerShimDummyFile();
  }
  pShim->pFd          = pFd;
  pShim->pVfs         = pVfs;
  pShim->journalMode  = PAGER_JOURNALMODE_DELETE;
  pShim->eLock        = 0;
  pShim->eState       = 0;
  pShim->noSync       = 0;
  pShim->iDataVersion = 1;   /* Start at 1 so the first check sees data */
  pShim->nRef         = 0;
  pShim->nMmapSize    = 0;

  return pShim;
}

/*
** Free all memory associated with a PagerShim.
*/
void pagerShimDestroy(PagerShim *pShim){
  if( pShim==0 ) return;
  sqlite3_free(pShim->zFilename);
  sqlite3_free(pShim->zJournal);
  sqlite3_free(pShim);
}

/* -----------------------------------------------------------------------
** File descriptor
** ----------------------------------------------------------------------- */

/*
** Return the file descriptor associated with the pager.
*/
sqlite3_file *sqlite3PagerFile(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerFile(pPager);
  return SHIM(pPager)->pFd;
}

/* -----------------------------------------------------------------------
** Filename queries
** ----------------------------------------------------------------------- */

/*
** Return the full pathname of the database file.
**
** The second parameter (outputFormat) controls the format of the
** returned string in the real pager.  For the shim we always return
** the stored filename regardless.
*/
const char *sqlite3PagerFilename(const Pager *pPager, int outputFormat){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerFilename(pPager, outputFormat);
  (void)outputFormat;
  return SHIM(pPager)->zFilename;
}

/*
** Return the full pathname of the journal file.
** In the shim this is always an empty string — there is no journal.
*/
const char *sqlite3PagerJournalname(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerJournalname(pPager);
  return SHIM(pPager)->zJournal;
}

/* -----------------------------------------------------------------------
** Journal mode
** ----------------------------------------------------------------------- */

/*
** Return the current journal mode.
*/
extern int orig_sqlite3PagerGetJournalMode(Pager*);
int sqlite3PagerGetJournalMode(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerGetJournalMode(pPager);
  return (int)SHIM(pPager)->journalMode;
}

/*
** Return 1 — the shim always allows the journal mode to be changed.
*/
int sqlite3PagerOkToChangeJournalMode(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerOkToChangeJournalMode(pPager);
  (void)pPager;
  return 1;
}

/*
** Set the journal mode.  Store the value and return it.
**
** PAGER_JOURNALMODE_QUERY (-1) means "just return the current mode
** without changing it."
*/
int sqlite3PagerSetJournalMode(Pager *pPager, int eMode){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerSetJournalMode(pPager, eMode);
  PagerShim *pShim = SHIM(pPager);
  if( eMode!=PAGER_JOURNALMODE_QUERY ){
    pShim->journalMode = (u8)eMode;
  }
  return (int)pShim->journalMode;
}

/* -----------------------------------------------------------------------
** Lock helpers
** ----------------------------------------------------------------------- */

/*
** Attempt to acquire an exclusive lock.  The shim always succeeds.
*/
int sqlite3PagerExclusiveLock(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerExclusiveLock(pPager);
  (void)pPager;
  return SQLITE_OK;
}

/*
** Return non-zero if the pager is read-only.  The shim is always
** read-write, so return 0.
*/
u8 sqlite3PagerIsreadonly(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerIsreadonly(pPager);
  (void)pPager;
  return 0;
}

/* -----------------------------------------------------------------------
** Reference counting
** ----------------------------------------------------------------------- */

/*
** Return the number of outstanding page references.  The shim never
** hands out pages, so this is always 0.
*/
int sqlite3PagerRefcount(Pager *pPager){
  
  (void)pPager;
  return 0;
}

/* -----------------------------------------------------------------------
** Data version
** ----------------------------------------------------------------------- */

/*
** Return the data-version counter.  External code uses this to detect
** when the database content has been modified by another connection.
*/
u32 sqlite3PagerDataVersion(Pager *pPager){
  if( !IS_SHIM(pPager) ) return orig_sqlite3PagerDataVersion(pPager);
  return SHIM(pPager)->iDataVersion;
}

/* -----------------------------------------------------------------------
** No-op / stub functions
** ----------------------------------------------------------------------- */

/*
** Shrink the page cache.  Nothing to do in the shim.
*/
void sqlite3PagerShrink(Pager *pPager){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerShrink(pPager); return; }
  (void)pPager;
}

/*
** Flush all dirty pages.  The shim has no dirty pages, so just
** return SQLITE_OK.
*/
int sqlite3PagerFlush(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerFlush(pPager);
  (void)pPager;
  return SQLITE_OK;
}

/*
** Return pager cache statistics.  The shim has no page cache, so
** every stat value is 0.
**
** Parameters:
**   eStat  — which statistic to return
**   reset  — if true, reset the counter after reading
**   pStat  — output pointer
*/
void sqlite3PagerCacheStat(Pager *pPager, int eStat, int reset, u64 *pStat){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerCacheStat(pPager, eStat, reset, pStat); return; }
  (void)pPager;
  (void)eStat;
  (void)reset;
  if( pStat ) *pStat = 0;
}

/* -----------------------------------------------------------------------
** Memory-database detection
** ----------------------------------------------------------------------- */

/*
** Return non-zero if this pager represents an in-memory database.
**
** A database is considered in-memory if its filename is either the
** empty string or the special name ":memory:".
*/
int sqlite3PagerIsMemdb(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerIsMemdb(pPager);
  const char *z = SHIM(pPager)->zFilename;
  if( z==0 ) return 1;
  if( z[0]=='\0' ) return 1;
  if( strcmp(z, ":memory:")==0 ) return 1;
  return 0;
}

/* -----------------------------------------------------------------------
** Locking mode
** ----------------------------------------------------------------------- */

/*
** Get or set the locking mode.
**
** If eMode is PAGER_LOCKINGMODE_QUERY (-1), return the current mode
** without changing it.  Otherwise store eMode and return it.
**
** Valid non-query values:
**   PAGER_LOCKINGMODE_NORMAL      0
**   PAGER_LOCKINGMODE_EXCLUSIVE   1
*/
int sqlite3PagerLockingMode(Pager *pPager, int eMode){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerLockingMode(pPager, eMode);
  PagerShim *pShim = SHIM(pPager);
  if( eMode!=PAGER_LOCKINGMODE_QUERY ){
    pShim->eLock = (u8)eMode;
  }
  return (int)pShim->eLock;
}

/* -----------------------------------------------------------------------
** VFS accessor (bonus — cheap to provide, sometimes needed)
** ----------------------------------------------------------------------- */

/*
** Return the VFS associated with this pager.
*/
sqlite3_vfs *sqlite3PagerVfs(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerVfs(pPager);
  return SHIM(pPager)->pVfs;
}

/* -----------------------------------------------------------------------
** Additional pager stubs needed by external modules (vacuum, backup,
** dbpage, dbstat, pragma, main, etc.)
** ----------------------------------------------------------------------- */

i64 sqlite3PagerJournalSizeLimit(Pager *pPager, i64 iLimit){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerJournalSizeLimit(pPager, iLimit);
  (void)pPager; (void)iLimit;
  return -1;
}

sqlite3_backup **sqlite3PagerBackupPtr(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerBackupPtr(pPager);
  static sqlite3_backup *pDummy = 0;
  (void)pPager;
  return &pDummy;
}

int sqlite3PagerGet(Pager *pPager, Pgno pgno, DbPage **ppPage, int clrFlag){
  if( !IS_SHIM(pPager) ) return orig_sqlite3PagerGet(pPager, pgno, ppPage, clrFlag);
  (void)pPager; (void)pgno; (void)clrFlag;
  *ppPage = 0;
  return SQLITE_OK;
}

void *sqlite3PagerGetData(DbPage *pPg){
  /* DbPage from the original pager — delegate.
  ** We can't easily tell which pager a DbPage belongs to, but for
  ** the prolly shim, PagerGet always returns NULL pages. So if pPg
  ** is non-NULL, it came from the original pager. */
  if( pPg ) return orig_sqlite3PagerGetData(pPg);
  return 0;
}

void *sqlite3PagerGetExtra(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerGetExtra(pPg);
  return 0;
}

void sqlite3PagerUnref(DbPage *pPg){ if(pPg) orig_sqlite3PagerUnref(pPg); }
void sqlite3PagerUnrefNotNull(DbPage *pPg){ if(pPg) orig_sqlite3PagerUnrefNotNull(pPg); }
void sqlite3PagerUnrefPageOne(DbPage *pPg){ if(pPg) orig_sqlite3PagerUnrefPageOne(pPg); }
void sqlite3PagerRef(DbPage *pPg){ (void)pPg; }

int sqlite3PagerWrite(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerWrite(pPg);
  return SQLITE_OK;
}

void sqlite3PagerDontWrite(DbPage *pPg){ (void)pPg; }

int sqlite3PagerPageRefcount(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerPageRefcount(pPg);
  return 0;
}

extern void orig_sqlite3PagerPagecount(Pager*, int*);
void sqlite3PagerPagecount(Pager *pPager, int *pnPage){
  if( !IS_SHIM(pPager) ){ orig_sqlite3PagerPagecount(pPager, pnPage); return; }
  (void)pPager;
  if( pnPage ) *pnPage = 0;
}

int sqlite3PagerSync(Pager *pPager, const char *zSuper){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerSync(pPager, zSuper);
  (void)pPager; (void)zSuper;
  return SQLITE_OK;
}

void sqlite3PagerTruncateImage(Pager *pPager, Pgno nPage){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerTruncateImage(pPager, nPage); return; }
  (void)pPager; (void)nPage;
}

int sqlite3PagerMemUsed(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerMemUsed(pPager);
  (void)pPager;
  return 0;
}

sqlite3_file *sqlite3PagerJrnlFile(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerJrnlFile(pPager);
  (void)pPager;
  return 0;
}

void sqlite3PagerClearCache(Pager *pPager){ (void)pPager; }

extern int orig_sqlite3PagerCommitPhaseOne(Pager*, const char*, int);
extern int orig_sqlite3PagerCommitPhaseTwo(Pager*);
int sqlite3PagerCommitPhaseOne(Pager *pPager, const char *zSuper, int noSync){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerCommitPhaseOne(pPager, zSuper, noSync);
  (void)pPager; (void)zSuper; (void)noSync;
  return SQLITE_OK;
}

int sqlite3PagerCommitPhaseTwo(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerCommitPhaseTwo(pPager);
  SHIM(pPager)->iDataVersion++;
  return SQLITE_OK;
}

void sqlite3PagerSetBusyHandler(Pager *pPager, int(*xBusy)(void*), void *pCtx){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerSetBusyHandler(pPager, xBusy, pCtx); return; }
  (void)pPager; (void)xBusy; (void)pCtx;
}

int sqlite3PagerSetPagesize(Pager *pPager, u32 *pPageSize, int nReserve){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerSetPagesize(pPager, pPageSize, nReserve);
  (void)pPager; (void)pPageSize; (void)nReserve;
  return SQLITE_OK;
}

Pgno sqlite3PagerMaxPageCount(Pager *pPager, Pgno mxPage){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerMaxPageCount(pPager, mxPage);
  (void)pPager; (void)mxPage;
  return 0xFFFFFFFF;
}

void sqlite3PagerSetCachesize(Pager *pPager, int mxPage){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerSetCachesize(pPager, mxPage); return; }
  (void)pPager; (void)mxPage;
}

int sqlite3PagerSetSpillsize(Pager *pPager, int mxPage){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerSetSpillsize(pPager, mxPage);
  (void)pPager; (void)mxPage;
  return 0;
}

void sqlite3PagerSetMmapLimit(Pager *pPager, sqlite3_int64 szMmap){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerSetMmapLimit(pPager, szMmap); return; }
  (void)pPager; (void)szMmap;
}

void sqlite3PagerSetFlags(Pager *pPager, unsigned flags){
  if(!IS_SHIM(pPager)){ orig_sqlite3PagerSetFlags(pPager, flags); return; }
  (void)pPager; (void)flags;
}

int sqlite3PagerBegin(Pager *pPager, int exFlag, int subjInMemory){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerBegin(pPager, exFlag, subjInMemory);
  (void)pPager; (void)exFlag; (void)subjInMemory;
  return SQLITE_OK;
}

int sqlite3PagerRollback(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerRollback(pPager);
  (void)pPager;
  return SQLITE_OK;
}

int sqlite3PagerOpenSavepoint(Pager *pPager, int n){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerOpenSavepoint(pPager, n);
  (void)pPager; (void)n;
  return SQLITE_OK;
}

int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerSavepoint(pPager, op, iSavepoint);
  (void)pPager; (void)op; (void)iSavepoint;
  return SQLITE_OK;
}

int sqlite3PagerSharedLock(Pager *pPager){
  if( !IS_SHIM(pPager) ) return orig_sqlite3PagerSharedLock(pPager);
  return SQLITE_OK;
}

int sqlite3PagerOpen(
  sqlite3_vfs *pVfs,
  Pager **ppPager,
  const char *zFilename,
  int nExtra,
  int flags,
  int vfsFlags,
  void(*xReinit)(DbPage*)
){
  (void)pVfs; (void)zFilename; (void)nExtra;
  (void)flags; (void)vfsFlags; (void)xReinit;
  *ppPager = 0;
  return SQLITE_OK;
}

int sqlite3PagerClose(Pager *pPager, sqlite3 *db){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerClose(pPager, db);
  (void)pPager; (void)db;
  return SQLITE_OK;
}

int sqlite3PagerReadFileheader(Pager *pPager, int n, unsigned char *pDest){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerReadFileheader(pPager, n, pDest);
  (void)pPager;
  memset(pDest, 0, n);
  return SQLITE_OK;
}

int sqlite3PagerMovepage(Pager *pPager, DbPage *pPg, Pgno pgno, int isCommit){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerMovepage(pPager, pPg, pgno, isCommit);
  (void)pPager; (void)pPg; (void)pgno; (void)isCommit;
  return SQLITE_OK;
}

void *sqlite3PagerTempSpace(Pager *pPager){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerTempSpace(pPager);
  static u8 aTmpSpace[65536];
  (void)pPager;
  return aTmpSpace;
}

void sqlite3PagerRekey(DbPage *pPg, Pgno pgno, u16 flags){
  (void)pPg; (void)pgno; (void)flags;
}

DbPage *sqlite3PagerLookup(Pager *pPager, Pgno pgno){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerLookup(pPager, pgno);
  (void)pPager; (void)pgno;
  return 0;
}

int sqlite3SectorSize(sqlite3_file *pFile){
  (void)pFile;
  return 4096;
}

#ifndef SQLITE_OMIT_WAL
int sqlite3PagerCheckpoint(Pager *pPager, sqlite3 *db, int eMode, int *pnLog, int *pnCkpt){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerCheckpoint(pPager, db, eMode, pnLog, pnCkpt);
  (void)pPager; (void)db; (void)eMode;
  if( pnLog ) *pnLog = 0;
  if( pnCkpt ) *pnCkpt = 0;
  return SQLITE_OK;
}
int sqlite3PagerWalSupported(Pager *pPager){ (void)pPager; return 0; }
int sqlite3PagerWalCallback(Pager *pPager){ (void)pPager; return SQLITE_OK; }
int sqlite3PagerOpenWal(Pager *pPager, int *pisOpen){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerOpenWal(pPager, pisOpen);
  (void)pPager;
  if( pisOpen ) *pisOpen = 0;
  return SQLITE_OK;
}
int sqlite3PagerCloseWal(Pager *pPager, sqlite3 *db){
  if(!IS_SHIM(pPager)) return orig_sqlite3PagerCloseWal(pPager, db);
  (void)pPager; (void)db;
  return SQLITE_OK;
}
#endif

/* shared cache stub */
int sqlite3_enable_shared_cache(int enable){
  (void)enable;
  return SQLITE_OK;
}

/* database_file_object stub */
sqlite3_file *sqlite3_database_file_object(const char *zName){
  (void)zName;
  return 0;
}

/* Always provide these — test code links against libsqlite3.a which may
** not have been compiled with SQLITE_TEST, but testfixture needs them. */
Pgno sqlite3PagerPagenumber(DbPage *pPg){ (void)pPg; return 0; }
int sqlite3PagerIswriteable(DbPage *pPg){ (void)pPg; return 1; }
int *sqlite3PagerStats(Pager *pPager){
  
  static int aStats[11];
  (void)pPager;
  memset(aStats, 0, sizeof(aStats));
  return aStats;
}
void sqlite3PagerRefdump(Pager *pPager){ (void)pPager; }

/* Undef macros from pager.h so we can define actual functions */
#undef disable_simulated_io_errors
#undef enable_simulated_io_errors
void disable_simulated_io_errors(void){}
void enable_simulated_io_errors(void){}

/* -----------------------------------------------------------------------
** Backup API stubs (backup.c excluded from prolly build)
** ----------------------------------------------------------------------- */
sqlite3_backup *sqlite3_backup_init(sqlite3 *pDest, const char *zDestDb,
                                     sqlite3 *pSrc, const char *zSrcDb){
  (void)pDest; (void)zDestDb; (void)pSrc; (void)zSrcDb;
  return 0;
}
int sqlite3_backup_step(sqlite3_backup *p, int nPage){
  (void)p; (void)nPage;
  return SQLITE_DONE;
}
int sqlite3_backup_finish(sqlite3_backup *p){
  (void)p;
  return SQLITE_OK;
}
int sqlite3_backup_remaining(sqlite3_backup *p){
  (void)p;
  return 0;
}
int sqlite3_backup_pagecount(sqlite3_backup *p){
  (void)p;
  return 0;
}

#endif /* DOLTLITE_PROLLY */
