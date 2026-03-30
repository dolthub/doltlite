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
**
** Dispatch uses a PagerOps vtable: shim pagers use shimPagerOps,
** original pagers use origPagerOps. The getPagerOps() helper
** selects the correct table based on PagerShim magic.
*/
#ifdef DOLTLITE_PROLLY

#include "pager_shim.h"
#include "sqliteInt.h"
#include "chunk_store.h"

/* Globals expected by test code */
int sqlite3_pager_writej_count = 0;
int sqlite3_pager_readdb_count = 0;
int sqlite3_pager_writedb_count = 0;
int sqlite3_opentemp_count = 0;

/* Shared cache list (empty — no shared cache in prolly) */
BtShared *SQLITE_WSD sqlite3SharedCacheList = 0;

#include <string.h>

#define SHIM(p) ((PagerShim*)(p))

/* Forward declarations for original pager dispatch */
extern u32 orig_sqlite3PagerDataVersion(Pager *pPager);
extern sqlite3_file *orig_sqlite3PagerFile(Pager *pPager);
extern const char *orig_sqlite3PagerFilename(const Pager *pPager, int);
extern const char *orig_sqlite3PagerJournalname(Pager *pPager);
extern int orig_sqlite3PagerGetJournalMode(Pager *pPager);
extern int orig_sqlite3PagerGet(Pager*, Pgno, DbPage**, int);
extern void *orig_sqlite3PagerGetData(DbPage*);
extern void *orig_sqlite3PagerGetExtra(DbPage*);
extern void orig_sqlite3PagerUnref(DbPage*);
extern void orig_sqlite3PagerUnrefNotNull(DbPage*);
extern void orig_sqlite3PagerUnrefPageOne(DbPage*);
extern int orig_sqlite3PagerWrite(DbPage*);
extern int orig_sqlite3PagerPageRefcount(DbPage*);
extern int orig_sqlite3PagerSharedLock(Pager*);
extern int orig_sqlite3PagerOpenWal(Pager*, int*);
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
extern sqlite3_vfs *orig_sqlite3PagerVfs(Pager*);
extern Pgno orig_sqlite3PagerMaxPageCount(Pager*, Pgno);
extern int orig_sqlite3PagerSetJournalMode(Pager*, int);
extern int orig_sqlite3PagerExclusiveLock(Pager*);
extern u8 orig_sqlite3PagerIsreadonly(Pager*);
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
extern int orig_sqlite3PagerCloseWal(Pager*, sqlite3*);
extern void orig_sqlite3PagerSetCachesize(Pager*, int);
extern int orig_sqlite3PagerSetSpillsize(Pager*, int);
extern void orig_sqlite3PagerSetMmapLimit(Pager*, sqlite3_int64);
extern void orig_sqlite3PagerTruncateImage(Pager*, Pgno);
extern sqlite3_backup *orig_sqlite3_backup_init(sqlite3*,const char*,
                                                 sqlite3*,const char*);
extern void orig_sqlite3PagerPagecount(Pager*, int*);
extern int orig_sqlite3PagerCommitPhaseOne(Pager*, const char*, int);
extern int orig_sqlite3PagerCommitPhaseTwo(Pager*);

/* -----------------------------------------------------------------------
** PagerOps vtable — function pointers for all dispatched pager operations
** ----------------------------------------------------------------------- */
struct PagerOps {
  sqlite3_file *(*xFile)(Pager*);
  const char *(*xFilename)(const Pager*, int);
  const char *(*xJournalname)(Pager*);
  int (*xGetJournalMode)(Pager*);
  int (*xOkToChangeJournalMode)(Pager*);
  int (*xSetJournalMode)(Pager*, int);
  int (*xExclusiveLock)(Pager*);
  u8 (*xIsreadonly)(Pager*);
  u32 (*xDataVersion)(Pager*);
  void (*xShrink)(Pager*);
  int (*xFlush)(Pager*);
  void (*xCacheStat)(Pager*, int, int, u64*);
  int (*xIsMemdb)(Pager*);
  int (*xLockingMode)(Pager*, int);
  sqlite3_vfs *(*xVfs)(Pager*);
  i64 (*xJournalSizeLimit)(Pager*, i64);
  sqlite3_backup **(*xBackupPtr)(Pager*);
  int (*xGet)(Pager*, Pgno, DbPage**, int);
  void (*xPagecount)(Pager*, int*);
  int (*xSync)(Pager*, const char*);
  void (*xTruncateImage)(Pager*, Pgno);
  int (*xMemUsed)(Pager*);
  sqlite3_file *(*xJrnlFile)(Pager*);
  int (*xCommitPhaseOne)(Pager*, const char*, int);
  int (*xCommitPhaseTwo)(Pager*);
  void (*xSetBusyHandler)(Pager*, int(*)(void*), void*);
  int (*xSetPagesize)(Pager*, u32*, int);
  Pgno (*xMaxPageCount)(Pager*, Pgno);
  void (*xSetCachesize)(Pager*, int);
  int (*xSetSpillsize)(Pager*, int);
  void (*xSetMmapLimit)(Pager*, sqlite3_int64);
  void (*xSetFlags)(Pager*, unsigned);
  int (*xBegin)(Pager*, int, int);
  int (*xRollback)(Pager*);
  int (*xOpenSavepoint)(Pager*, int);
  int (*xSavepoint)(Pager*, int, int);
  int (*xSharedLock)(Pager*);
  int (*xClose)(Pager*, sqlite3*);
  int (*xReadFileheader)(Pager*, int, unsigned char*);
  int (*xMovepage)(Pager*, DbPage*, Pgno, int);
  void *(*xTempSpace)(Pager*);
  DbPage *(*xLookup)(Pager*, Pgno);
#ifndef SQLITE_OMIT_WAL
  int (*xCheckpoint)(Pager*, sqlite3*, int, int*, int*);
  int (*xWalSupported)(Pager*);
  int (*xOpenWal)(Pager*, int*);
  int (*xCloseWal)(Pager*, sqlite3*);
#endif
};

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
** Shim vtable implementations — extracted from inline dispatch logic
** ----------------------------------------------------------------------- */

static sqlite3_file *shimPagerFile(Pager *p){
  return SHIM(p)->pFd;
}
static const char *shimPagerFilename(const Pager *p, int fmt){
  (void)fmt;
  return ((const PagerShim*)p)->zFilename;
}
static const char *shimPagerJournalname(Pager *p){
  return SHIM(p)->zJournal;
}
static int shimPagerGetJournalMode(Pager *p){
  return (int)SHIM(p)->journalMode;
}
static int shimPagerOkToChangeJournalMode(Pager *p){
  (void)p; return 1;
}
static int shimPagerSetJournalMode(Pager *p, int eMode){
  PagerShim *s = SHIM(p);
  if( eMode!=PAGER_JOURNALMODE_QUERY ) s->journalMode = (u8)eMode;
  return (int)s->journalMode;
}
static int shimPagerExclusiveLock(Pager *p){
  (void)p; return SQLITE_OK;
}
static u8 shimPagerIsreadonly(Pager *p){
  (void)p; return 0;
}
static u32 shimPagerDataVersion(Pager *p){
  return SHIM(p)->iDataVersion;
}
static void shimPagerShrink(Pager *p){
  (void)p;
}
static int shimPagerFlush(Pager *p){
  (void)p; return SQLITE_OK;
}
static void shimPagerCacheStat(Pager *p, int eStat, int reset, u64 *pStat){
  (void)p; (void)eStat; (void)reset;
  if( pStat ) *pStat = 0;
}
static int shimPagerIsMemdb(Pager *p){
  const char *z = SHIM(p)->zFilename;
  if( z==0 ) return 1;
  if( z[0]=='\0' ) return 1;
  if( strcmp(z, ":memory:")==0 ) return 1;
  return 0;
}
static int shimPagerLockingMode(Pager *p, int eMode){
  PagerShim *s = SHIM(p);
  if( eMode!=PAGER_LOCKINGMODE_QUERY ) s->eLock = (u8)eMode;
  return (int)s->eLock;
}
static sqlite3_vfs *shimPagerVfs(Pager *p){
  return SHIM(p)->pVfs;
}
static i64 shimPagerJournalSizeLimit(Pager *p, i64 iLimit){
  (void)p; (void)iLimit; return -1;
}
static sqlite3_backup **shimPagerBackupPtr(Pager *p){
  static sqlite3_backup *pDummy = 0;
  (void)p; return &pDummy;
}
static int shimPagerGet(Pager *p, Pgno pgno, DbPage **ppPage, int clrFlag){
  (void)p; (void)pgno; (void)clrFlag;
  *ppPage = 0;
  return SQLITE_OK;
}
static void shimPagerPagecount(Pager *p, int *pnPage){
  (void)p;
  if( pnPage ) *pnPage = 0;
}
static int shimPagerSync(Pager *p, const char *zSuper){
  (void)p; (void)zSuper; return SQLITE_OK;
}
static void shimPagerTruncateImage(Pager *p, Pgno nPage){
  (void)p; (void)nPage;
}
static int shimPagerMemUsed(Pager *p){
  (void)p; return 0;
}
static sqlite3_file *shimPagerJrnlFile(Pager *p){
  (void)p; return 0;
}
static int shimPagerCommitPhaseOne(Pager *p, const char *zSuper, int noSync){
  (void)p; (void)zSuper; (void)noSync; return SQLITE_OK;
}
static int shimPagerCommitPhaseTwo(Pager *p){
  SHIM(p)->iDataVersion++;
  return SQLITE_OK;
}
static void shimPagerSetBusyHandler(Pager *p, int(*xBusy)(void*), void *pCtx){
  (void)p; (void)xBusy; (void)pCtx;
}
static int shimPagerSetPagesize(Pager *p, u32 *pPageSize, int nReserve){
  (void)p; (void)pPageSize; (void)nReserve; return SQLITE_OK;
}
static Pgno shimPagerMaxPageCount(Pager *p, Pgno mxPage){
  (void)p; (void)mxPage; return 0xFFFFFFFF;
}
static void shimPagerSetCachesize(Pager *p, int mxPage){
  (void)p; (void)mxPage;
}
static int shimPagerSetSpillsize(Pager *p, int mxPage){
  (void)p; (void)mxPage; return 0;
}
static void shimPagerSetMmapLimit(Pager *p, sqlite3_int64 szMmap){
  (void)p; (void)szMmap;
}
static void shimPagerSetFlags(Pager *p, unsigned flags){
  (void)p; (void)flags;
}
static int shimPagerBegin(Pager *p, int exFlag, int subjInMemory){
  (void)p; (void)exFlag; (void)subjInMemory; return SQLITE_OK;
}
static int shimPagerRollback(Pager *p){
  (void)p; return SQLITE_OK;
}
static int shimPagerOpenSavepoint(Pager *p, int n){
  (void)p; (void)n; return SQLITE_OK;
}
static int shimPagerSavepoint(Pager *p, int op, int iSavepoint){
  (void)p; (void)op; (void)iSavepoint; return SQLITE_OK;
}
static int shimPagerSharedLock(Pager *p){
  (void)p; return SQLITE_OK;
}
static int shimPagerClose(Pager *p, sqlite3 *db){
  (void)p; (void)db; return SQLITE_OK;
}
static int shimPagerReadFileheader(Pager *p, int n, unsigned char *pDest){
  (void)p; memset(pDest, 0, n); return SQLITE_OK;
}
static int shimPagerMovepage(Pager *p, DbPage *pPg, Pgno pgno, int isCommit){
  (void)p; (void)pPg; (void)pgno; (void)isCommit; return SQLITE_OK;
}
static void *shimPagerTempSpace(Pager *p){
  static u8 aTmpSpace[65536];
  (void)p; return aTmpSpace;
}
static DbPage *shimPagerLookup(Pager *p, Pgno pgno){
  (void)p; (void)pgno; return 0;
}

#ifndef SQLITE_OMIT_WAL
extern int doltliteGcCompact(sqlite3 *db);

static int shimPagerCheckpoint(Pager *p, sqlite3 *db, int eMode,
                               int *pnLog, int *pnCkpt){
  (void)p; (void)eMode;
  if( pnLog ) *pnLog = 0;
  if( pnCkpt ) *pnCkpt = 0;
  if( db ){
    int rc = doltliteGcCompact(db);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}
static int shimPagerWalSupported(Pager *p){
  (void)p; return 1;
}
static int shimPagerOpenWal(Pager *p, int *pisOpen){
  SHIM(p)->journalMode = PAGER_JOURNALMODE_WAL;
  if( pisOpen ) *pisOpen = 1;
  return SQLITE_OK;
}
static int shimPagerCloseWal(Pager *p, sqlite3 *db){
  (void)p; (void)db; return SQLITE_OK;
}
#endif /* SQLITE_OMIT_WAL */

/* -----------------------------------------------------------------------
** Vtable instances
** ----------------------------------------------------------------------- */

static const PagerOps shimPagerOps = {
  shimPagerFile,
  shimPagerFilename,
  shimPagerJournalname,
  shimPagerGetJournalMode,
  shimPagerOkToChangeJournalMode,
  shimPagerSetJournalMode,
  shimPagerExclusiveLock,
  shimPagerIsreadonly,
  shimPagerDataVersion,
  shimPagerShrink,
  shimPagerFlush,
  shimPagerCacheStat,
  shimPagerIsMemdb,
  shimPagerLockingMode,
  shimPagerVfs,
  shimPagerJournalSizeLimit,
  shimPagerBackupPtr,
  shimPagerGet,
  shimPagerPagecount,
  shimPagerSync,
  shimPagerTruncateImage,
  shimPagerMemUsed,
  shimPagerJrnlFile,
  shimPagerCommitPhaseOne,
  shimPagerCommitPhaseTwo,
  shimPagerSetBusyHandler,
  shimPagerSetPagesize,
  shimPagerMaxPageCount,
  shimPagerSetCachesize,
  shimPagerSetSpillsize,
  shimPagerSetMmapLimit,
  shimPagerSetFlags,
  shimPagerBegin,
  shimPagerRollback,
  shimPagerOpenSavepoint,
  shimPagerSavepoint,
  shimPagerSharedLock,
  shimPagerClose,
  shimPagerReadFileheader,
  shimPagerMovepage,
  shimPagerTempSpace,
  shimPagerLookup,
#ifndef SQLITE_OMIT_WAL
  shimPagerCheckpoint,
  shimPagerWalSupported,
  shimPagerOpenWal,
  shimPagerCloseWal,
#endif
};

static const PagerOps origPagerOps = {
  orig_sqlite3PagerFile,
  orig_sqlite3PagerFilename,
  orig_sqlite3PagerJournalname,
  orig_sqlite3PagerGetJournalMode,
  orig_sqlite3PagerOkToChangeJournalMode,
  orig_sqlite3PagerSetJournalMode,
  orig_sqlite3PagerExclusiveLock,
  orig_sqlite3PagerIsreadonly,
  orig_sqlite3PagerDataVersion,
  orig_sqlite3PagerShrink,
  orig_sqlite3PagerFlush,
  orig_sqlite3PagerCacheStat,
  orig_sqlite3PagerIsMemdb,
  orig_sqlite3PagerLockingMode,
  orig_sqlite3PagerVfs,
  orig_sqlite3PagerJournalSizeLimit,
  orig_sqlite3PagerBackupPtr,
  orig_sqlite3PagerGet,
  orig_sqlite3PagerPagecount,
  orig_sqlite3PagerSync,
  orig_sqlite3PagerTruncateImage,
  orig_sqlite3PagerMemUsed,
  orig_sqlite3PagerJrnlFile,
  orig_sqlite3PagerCommitPhaseOne,
  orig_sqlite3PagerCommitPhaseTwo,
  orig_sqlite3PagerSetBusyHandler,
  orig_sqlite3PagerSetPagesize,
  orig_sqlite3PagerMaxPageCount,
  orig_sqlite3PagerSetCachesize,
  orig_sqlite3PagerSetSpillsize,
  orig_sqlite3PagerSetMmapLimit,
  orig_sqlite3PagerSetFlags,
  orig_sqlite3PagerBegin,
  orig_sqlite3PagerRollback,
  orig_sqlite3PagerOpenSavepoint,
  orig_sqlite3PagerSavepoint,
  orig_sqlite3PagerSharedLock,
  orig_sqlite3PagerClose,
  orig_sqlite3PagerReadFileheader,
  orig_sqlite3PagerMovepage,
  orig_sqlite3PagerTempSpace,
  orig_sqlite3PagerLookup,
#ifndef SQLITE_OMIT_WAL
  orig_sqlite3PagerCheckpoint,
  orig_sqlite3PagerWalSupported,
  orig_sqlite3PagerOpenWal,
  orig_sqlite3PagerCloseWal,
#endif
};

/*
** Return the PagerOps vtable for a given Pager pointer.
** Shim pagers use shimPagerOps; original pagers use origPagerOps.
*/
static inline const PagerOps *getPagerOps(const Pager *p){
  if( p && ((const PagerShim*)p)->magic == PAGER_SHIM_MAGIC ){
    return ((const PagerShim*)p)->pOps;
  }
  return &origPagerOps;
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
  pShim->pOps = &shimPagerOps;

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
  pShim->journalMode  = PAGER_JOURNALMODE_WAL;
  pShim->eLock        = 0;
  pShim->iDataVersion = 1;   /* Start at 1 so the first check sees data */

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
** Dispatch functions — thin wrappers that delegate through PagerOps
** ----------------------------------------------------------------------- */

sqlite3_file *sqlite3PagerFile(Pager *pPager){
  return getPagerOps(pPager)->xFile(pPager);
}

const char *sqlite3PagerFilename(const Pager *pPager, int outputFormat){
  return getPagerOps(pPager)->xFilename(pPager, outputFormat);
}

const char *sqlite3PagerJournalname(Pager *pPager){
  return getPagerOps(pPager)->xJournalname(pPager);
}

int sqlite3PagerGetJournalMode(Pager *pPager){
  return getPagerOps(pPager)->xGetJournalMode(pPager);
}

int sqlite3PagerOkToChangeJournalMode(Pager *pPager){
  return getPagerOps(pPager)->xOkToChangeJournalMode(pPager);
}

int sqlite3PagerSetJournalMode(Pager *pPager, int eMode){
  return getPagerOps(pPager)->xSetJournalMode(pPager, eMode);
}

int sqlite3PagerExclusiveLock(Pager *pPager){
  return getPagerOps(pPager)->xExclusiveLock(pPager);
}

u8 sqlite3PagerIsreadonly(Pager *pPager){
  return getPagerOps(pPager)->xIsreadonly(pPager);
}

int sqlite3PagerRefcount(Pager *pPager){
  (void)pPager;
  return 0;
}

u32 sqlite3PagerDataVersion(Pager *pPager){
  return getPagerOps(pPager)->xDataVersion(pPager);
}

void sqlite3PagerShrink(Pager *pPager){
  getPagerOps(pPager)->xShrink(pPager);
}

int sqlite3PagerFlush(Pager *pPager){
  return getPagerOps(pPager)->xFlush(pPager);
}

void sqlite3PagerCacheStat(Pager *pPager, int eStat, int reset, u64 *pStat){
  getPagerOps(pPager)->xCacheStat(pPager, eStat, reset, pStat);
}

int sqlite3PagerIsMemdb(Pager *pPager){
  return getPagerOps(pPager)->xIsMemdb(pPager);
}

int sqlite3PagerLockingMode(Pager *pPager, int eMode){
  return getPagerOps(pPager)->xLockingMode(pPager, eMode);
}

sqlite3_vfs *sqlite3PagerVfs(Pager *pPager){
  return getPagerOps(pPager)->xVfs(pPager);
}

i64 sqlite3PagerJournalSizeLimit(Pager *pPager, i64 iLimit){
  return getPagerOps(pPager)->xJournalSizeLimit(pPager, iLimit);
}

sqlite3_backup **sqlite3PagerBackupPtr(Pager *pPager){
  return getPagerOps(pPager)->xBackupPtr(pPager);
}

int sqlite3PagerGet(Pager *pPager, Pgno pgno, DbPage **ppPage, int clrFlag){
  return getPagerOps(pPager)->xGet(pPager, pgno, ppPage, clrFlag);
}

/* -----------------------------------------------------------------------
** DbPage-based functions — dispatch on page pointer, not on Pager vtable
** ----------------------------------------------------------------------- */

void *sqlite3PagerGetData(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerGetData(pPg);
  return 0;
}

void *sqlite3PagerGetExtra(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerGetExtra(pPg);
  return 0;
}

void sqlite3PagerUnref(DbPage *pPg){
  if( pPg ) orig_sqlite3PagerUnref(pPg);
}
void sqlite3PagerUnrefNotNull(DbPage *pPg){
  if( pPg ) orig_sqlite3PagerUnrefNotNull(pPg);
}
void sqlite3PagerUnrefPageOne(DbPage *pPg){
  if( pPg ) orig_sqlite3PagerUnrefPageOne(pPg);
}
void sqlite3PagerRef(DbPage *pPg){
  (void)pPg;
}

int sqlite3PagerWrite(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerWrite(pPg);
  return SQLITE_OK;
}

void sqlite3PagerDontWrite(DbPage *pPg){
  (void)pPg;
}

int sqlite3PagerPageRefcount(DbPage *pPg){
  if( pPg ) return orig_sqlite3PagerPageRefcount(pPg);
  return 0;
}

/* -----------------------------------------------------------------------
** More Pager dispatch functions
** ----------------------------------------------------------------------- */

void sqlite3PagerPagecount(Pager *pPager, int *pnPage){
  getPagerOps(pPager)->xPagecount(pPager, pnPage);
}

int sqlite3PagerSync(Pager *pPager, const char *zSuper){
  return getPagerOps(pPager)->xSync(pPager, zSuper);
}

void sqlite3PagerTruncateImage(Pager *pPager, Pgno nPage){
  getPagerOps(pPager)->xTruncateImage(pPager, nPage);
}

int sqlite3PagerMemUsed(Pager *pPager){
  return getPagerOps(pPager)->xMemUsed(pPager);
}

sqlite3_file *sqlite3PagerJrnlFile(Pager *pPager){
  return getPagerOps(pPager)->xJrnlFile(pPager);
}

void sqlite3PagerClearCache(Pager *pPager){
  (void)pPager;
}

int sqlite3PagerCommitPhaseOne(Pager *pPager, const char *zSuper, int noSync){
  return getPagerOps(pPager)->xCommitPhaseOne(pPager, zSuper, noSync);
}

int sqlite3PagerCommitPhaseTwo(Pager *pPager){
  return getPagerOps(pPager)->xCommitPhaseTwo(pPager);
}

void sqlite3PagerSetBusyHandler(Pager *pPager, int(*xBusy)(void*), void *pCtx){
  getPagerOps(pPager)->xSetBusyHandler(pPager, xBusy, pCtx);
}

int sqlite3PagerSetPagesize(Pager *pPager, u32 *pPageSize, int nReserve){
  return getPagerOps(pPager)->xSetPagesize(pPager, pPageSize, nReserve);
}

Pgno sqlite3PagerMaxPageCount(Pager *pPager, Pgno mxPage){
  return getPagerOps(pPager)->xMaxPageCount(pPager, mxPage);
}

void sqlite3PagerSetCachesize(Pager *pPager, int mxPage){
  getPagerOps(pPager)->xSetCachesize(pPager, mxPage);
}

int sqlite3PagerSetSpillsize(Pager *pPager, int mxPage){
  return getPagerOps(pPager)->xSetSpillsize(pPager, mxPage);
}

void sqlite3PagerSetMmapLimit(Pager *pPager, sqlite3_int64 szMmap){
  getPagerOps(pPager)->xSetMmapLimit(pPager, szMmap);
}

void sqlite3PagerSetFlags(Pager *pPager, unsigned flags){
  getPagerOps(pPager)->xSetFlags(pPager, flags);
}

int sqlite3PagerBegin(Pager *pPager, int exFlag, int subjInMemory){
  return getPagerOps(pPager)->xBegin(pPager, exFlag, subjInMemory);
}

int sqlite3PagerRollback(Pager *pPager){
  return getPagerOps(pPager)->xRollback(pPager);
}

int sqlite3PagerOpenSavepoint(Pager *pPager, int n){
  return getPagerOps(pPager)->xOpenSavepoint(pPager, n);
}

int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint){
  return getPagerOps(pPager)->xSavepoint(pPager, op, iSavepoint);
}

int sqlite3PagerSharedLock(Pager *pPager){
  return getPagerOps(pPager)->xSharedLock(pPager);
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
  return getPagerOps(pPager)->xClose(pPager, db);
}

int sqlite3PagerReadFileheader(Pager *pPager, int n, unsigned char *pDest){
  return getPagerOps(pPager)->xReadFileheader(pPager, n, pDest);
}

int sqlite3PagerMovepage(Pager *pPager, DbPage *pPg, Pgno pgno, int isCommit){
  return getPagerOps(pPager)->xMovepage(pPager, pPg, pgno, isCommit);
}

void *sqlite3PagerTempSpace(Pager *pPager){
  return getPagerOps(pPager)->xTempSpace(pPager);
}

void sqlite3PagerRekey(DbPage *pPg, Pgno pgno, u16 flags){
  (void)pPg;
  (void)pgno;
  (void)flags;
}

DbPage *sqlite3PagerLookup(Pager *pPager, Pgno pgno){
  return getPagerOps(pPager)->xLookup(pPager, pgno);
}

int sqlite3SectorSize(sqlite3_file *pFile){
  (void)pFile;
  return 4096;
}

#ifndef SQLITE_OMIT_WAL
int sqlite3PagerCheckpoint(Pager *pPager, sqlite3 *db, int eMode,
                           int *pnLog, int *pnCkpt){
  return getPagerOps(pPager)->xCheckpoint(pPager, db, eMode, pnLog, pnCkpt);
}
int sqlite3PagerWalSupported(Pager *pPager){
  return getPagerOps(pPager)->xWalSupported(pPager);
}
int sqlite3PagerWalCallback(Pager *pPager){
  (void)pPager;
  return SQLITE_OK;
}
int sqlite3PagerOpenWal(Pager *pPager, int *pisOpen){
  return getPagerOps(pPager)->xOpenWal(pPager, pisOpen);
}
int sqlite3PagerCloseWal(Pager *pPager, sqlite3 *db){
  return getPagerOps(pPager)->xCloseWal(pPager, db);
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
Pgno sqlite3PagerPagenumber(DbPage *pPg){
  (void)pPg;
  return 0;
}
int sqlite3PagerIswriteable(DbPage *pPg){
  (void)pPg;
  return 1;
}
int *sqlite3PagerStats(Pager *pPager){
  static int aStats[11];
  (void)pPager;
  memset(aStats, 0, sizeof(aStats));
  return aStats;
}
void sqlite3PagerRefdump(Pager *pPager){
  (void)pPager;
}

/* Undef macros from pager.h so we can define actual functions */
#undef disable_simulated_io_errors
#undef enable_simulated_io_errors
void disable_simulated_io_errors(void){
}
void enable_simulated_io_errors(void){
}

/* -----------------------------------------------------------------------
** Backup API — copies the chunk store file from source to destination.
** The chunk store is a single self-contained file, so a complete backup
** is just a file copy.  For ATTACH'd standard SQLite databases, delegates
** to the original backup implementation.
** ----------------------------------------------------------------------- */

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);

/* Backup state for prolly-tree chunk store file copy. */
typedef struct DoltliteBackup DoltliteBackup;
struct DoltliteBackup {
  sqlite3 *pSrcDb;
  sqlite3 *pDestDb;
  char *zSrcFile;       /* Source chunk store file path (owned copy) */
  char *zDestFile;      /* Destination chunk store file path (owned copy) */
  sqlite3_vfs *pVfs;
  int done;             /* 1 after step completes the copy */
};

sqlite3_backup *sqlite3_backup_init(sqlite3 *pDest, const char *zDestDb,
                                     sqlite3 *pSrc, const char *zSrcDb){
  DoltliteBackup *p;
  ChunkStore *srcCs;
  ChunkStore *destCs;
  int iSrc, iDest;

  if( !pDest || !pSrc || pDest==pSrc ) return 0;

  /* Find the named databases */
  iSrc = sqlite3FindDbName(pSrc, zSrcDb);
  iDest = sqlite3FindDbName(pDest, zDestDb);
  if( iSrc < 0 || iDest < 0 ) return 0;

  /* For non-main databases (ATTACH'd SQLite btrees), delegate to original */
  if( iSrc != 0 || iDest != 0 ){
    return orig_sqlite3_backup_init(pDest, zDestDb, pSrc, zSrcDb);
  }

  /* Get chunk stores */
  srcCs = doltliteGetChunkStore(pSrc);
  destCs = doltliteGetChunkStore(pDest);
  if( !srcCs || !srcCs->zFilename ) return 0;
  if( srcCs->isMemory ) return 0;  /* Can't backup in-memory databases */
  if( !destCs || !destCs->zFilename ) return 0;

  p = (DoltliteBackup*)sqlite3_malloc(sizeof(DoltliteBackup));
  if( !p ) return 0;
  memset(p, 0, sizeof(*p));

  p->pSrcDb = pSrc;
  p->pDestDb = pDest;
  p->pVfs = srcCs->pVfs;
  p->zSrcFile = sqlite3_mprintf("%s", srcCs->zFilename);
  p->zDestFile = sqlite3_mprintf("%s", destCs->zFilename);
  if( !p->zSrcFile || !p->zDestFile ){
    sqlite3_free(p->zSrcFile);
    sqlite3_free(p->zDestFile);
    sqlite3_free(p);
    return 0;
  }

  return (sqlite3_backup*)p;
}

int sqlite3_backup_step(sqlite3_backup *pBackup, int nPage){
  DoltliteBackup *p = (DoltliteBackup*)pBackup;
  sqlite3_file *pSrc = 0;
  sqlite3_file *pDest = 0;
  i64 fileSize = 0;
  int rc;
  int openFlags;
  (void)nPage;

  if( !p ) return SQLITE_DONE;
  if( p->done ) return SQLITE_DONE;

  /* Open source file for reading */
  openFlags = SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB;
  rc = sqlite3OsOpenMalloc(p->pVfs, p->zSrcFile, &pSrc, openFlags, 0);
  if( rc != SQLITE_OK ) return rc;

  rc = sqlite3OsFileSize(pSrc, &fileSize);
  if( rc != SQLITE_OK ){
    sqlite3OsCloseFree(pSrc);
    return rc;
  }
  /* Open/create destination file for writing */
  openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
  rc = sqlite3OsOpenMalloc(p->pVfs, p->zDestFile, &pDest, openFlags, 0);
  if( rc != SQLITE_OK ){
    sqlite3OsCloseFree(pSrc);
    return rc;
  }

  /* Copy in 64KB chunks (VFS write size limit) */
  {
    u8 *buf = (u8*)sqlite3_malloc(65536);
    i64 off = 0;
    if( !buf ){
      sqlite3OsCloseFree(pSrc);
      sqlite3OsCloseFree(pDest);
      return SQLITE_NOMEM;
    }
    while( off < fileSize ){
      int toRead = (fileSize - off) > 65536 ? 65536 : (int)(fileSize - off);
      rc = sqlite3OsRead(pSrc, buf, toRead, off);
      if( rc != SQLITE_OK ) break;
      rc = sqlite3OsWrite(pDest, buf, toRead, off);
      if( rc != SQLITE_OK ) break;
      off += toRead;
    }
    sqlite3_free(buf);
  }

  if( rc == SQLITE_OK ){
    rc = sqlite3OsSync(pDest, SQLITE_SYNC_NORMAL);
  }

  sqlite3OsCloseFree(pSrc);
  sqlite3OsCloseFree(pDest);

  if( rc == SQLITE_OK ){
    p->done = 1;
    return SQLITE_DONE;
  }
  return rc;
}

int sqlite3_backup_finish(sqlite3_backup *pBackup){
  DoltliteBackup *p = (DoltliteBackup*)pBackup;
  if( !p ) return SQLITE_OK;
  sqlite3_free(p->zSrcFile);
  sqlite3_free(p->zDestFile);
  sqlite3_free(p);
  return SQLITE_OK;
}

int sqlite3_backup_remaining(sqlite3_backup *pBackup){
  DoltliteBackup *p = (DoltliteBackup*)pBackup;
  if( !p ) return 0;
  return p->done ? 0 : 1;
}

int sqlite3_backup_pagecount(sqlite3_backup *pBackup){
  DoltliteBackup *p = (DoltliteBackup*)pBackup;
  if( !p ) return 0;
  return 1;  /* Single "page" = entire chunk store file */
}

#endif /* DOLTLITE_PROLLY */
