#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct FaultFile FaultFile;
struct FaultFile {
  sqlite3_file base;
  sqlite3_file *pReal;
  int isTarget;
  int isJournal;
};

static sqlite3_vfs *gBaseVfs;
static sqlite3_vfs gFaultVfs;
static char gTargetPath[512];
static int gArmOnJournalSync;
static int gArmedFailWrite;
static int gFaultTriggered;

static int endsWith(const char *zStr, const char *zSuffix){
  size_t nStr, nSuffix;
  if( !zStr || !zSuffix ) return 0;
  nStr = strlen(zStr);
  nSuffix = strlen(zSuffix);
  return nStr>=nSuffix && strcmp(zStr + nStr - nSuffix, zSuffix)==0;
}

static int faultClose(sqlite3_file *pFile){
  FaultFile *p = (FaultFile*)pFile;
  if( p->pReal && p->pReal->pMethods ){
    return p->pReal->pMethods->xClose(p->pReal);
  }
  return SQLITE_OK;
}

static int faultRead(
  sqlite3_file *pFile, void *pBuf, int n, sqlite3_int64 off
){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xRead(p->pReal, pBuf, n, off);
}

static int faultWrite(
  sqlite3_file *pFile, const void *pBuf, int n, sqlite3_int64 off
){
  FaultFile *p = (FaultFile*)pFile;
  if( p->isTarget && gArmedFailWrite ){
    unsigned char aGarbage[16];
    memset(aGarbage, 0xDE, sizeof(aGarbage));
    /* Corrupt the start of the destination file to prove recovery restores it. */
    (void)p->pReal->pMethods->xWrite(p->pReal, aGarbage, (int)sizeof(aGarbage), 0);
    gArmedFailWrite = 0;
    gFaultTriggered = 1;
    return SQLITE_IOERR_WRITE;
  }
  return p->pReal->pMethods->xWrite(p->pReal, pBuf, n, off);
}

static int faultTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xTruncate(p->pReal, size);
}

static int faultSync(sqlite3_file *pFile, int flags){
  FaultFile *p = (FaultFile*)pFile;
  int rc = p->pReal->pMethods->xSync(p->pReal, flags);
  if( rc==SQLITE_OK && p->isJournal && gArmOnJournalSync ){
    gArmOnJournalSync = 0;
    gArmedFailWrite = 1;
  }
  return rc;
}

static int faultFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

static int faultLock(sqlite3_file *pFile, int lock){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xLock(p->pReal, lock);
}

static int faultUnlock(sqlite3_file *pFile, int lock){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xUnlock(p->pReal, lock);
}

static int faultCheckReservedLock(sqlite3_file *pFile, int *pOut){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xCheckReservedLock(p->pReal, pOut);
}

static int faultFileControl(sqlite3_file *pFile, int op, void *pArg){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
}

static int faultSectorSize(sqlite3_file *pFile){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xSectorSize(p->pReal);
}

static int faultDeviceCharacteristics(sqlite3_file *pFile){
  FaultFile *p = (FaultFile*)pFile;
  return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}

static const sqlite3_io_methods gFaultMethods = {
  1,
  faultClose,
  faultRead,
  faultWrite,
  faultTruncate,
  faultSync,
  faultFileSize,
  faultLock,
  faultUnlock,
  faultCheckReservedLock,
  faultFileControl,
  faultSectorSize,
  faultDeviceCharacteristics
};

static int faultOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  FaultFile *p = (FaultFile*)pFile;
  int rc;
  (void)pVfs;
  memset(p, 0, sizeof(*p));
  p->pReal = (sqlite3_file*)(((unsigned char*)p) + ((sizeof(FaultFile) + 7) & ~7));
  rc = gBaseVfs->xOpen(gBaseVfs, zName, p->pReal, flags, pOutFlags);
  if( rc!=SQLITE_OK ) return rc;
  if( zName ){
    p->isTarget = (strcmp(zName, gTargetPath)==0);
    p->isJournal = endsWith(zName, "-gc-replace");
  }
  p->base.pMethods = &gFaultMethods;
  return SQLITE_OK;
}

static int faultDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir){
  (void)pVfs;
  return gBaseVfs->xDelete(gBaseVfs, zName, syncDir);
}

static int faultAccess(
  sqlite3_vfs *pVfs, const char *zName, int flags, int *pOut
){
  (void)pVfs;
  return gBaseVfs->xAccess(gBaseVfs, zName, flags, pOut);
}

static int faultFullPathname(
  sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut
){
  (void)pVfs;
  return gBaseVfs->xFullPathname(gBaseVfs, zName, nOut, zOut);
}

static int faultRandomness(sqlite3_vfs *pVfs, int nByte, char *zOut){
  (void)pVfs;
  return gBaseVfs->xRandomness(gBaseVfs, nByte, zOut);
}

static int faultSleep(sqlite3_vfs *pVfs, int microseconds){
  (void)pVfs;
  return gBaseVfs->xSleep(gBaseVfs, microseconds);
}

static int faultCurrentTime(sqlite3_vfs *pVfs, double *pOut){
  (void)pVfs;
  return gBaseVfs->xCurrentTime(gBaseVfs, pOut);
}

static int faultGetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
  (void)pVfs;
  return gBaseVfs->xGetLastError(gBaseVfs, nBuf, zBuf);
}

static int faultCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pOut){
  (void)pVfs;
  return gBaseVfs->xCurrentTimeInt64(gBaseVfs, pOut);
}

static int fail(const char *zMsg){
  fprintf(stderr, "FAIL: %s\n", zMsg);
  return 1;
}

static int probeExecSql(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error (%d) on [%s]: %s\n", rc, zSql, zErr ? zErr : "");
    sqlite3_free(zErr);
  }
  return rc;
}

static int querySingleText(sqlite3 *db, const char *zSql, char *zBuf, int nBuf){
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    sqlite3_finalize(pStmt);
    return rc==SQLITE_DONE ? SQLITE_NOTFOUND : rc;
  }
  snprintf(zBuf, (size_t)nBuf, "%s", (const char*)sqlite3_column_text(pStmt, 0));
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

static int fileExists(const char *zPath){
  return access(zPath, F_OK)==0;
}

static int runCrashedBackup(const char *zSrc, const char *zDest){
  sqlite3 *pSrc = 0;
  sqlite3 *pDest = 0;
  sqlite3_backup *pBak = 0;
  int rc;

  gBaseVfs->xFullPathname(gBaseVfs, zDest, (int)sizeof(gTargetPath), gTargetPath);
  gArmOnJournalSync = 0;
  gArmedFailWrite = 0;
  gFaultTriggered = 0;

  rc = sqlite3_open_v2(zSrc, &pSrc, SQLITE_OPEN_READWRITE, "fault-vfs");
  if( rc!=SQLITE_OK ) return fail("open src");
  rc = sqlite3_open_v2(zDest, &pDest, SQLITE_OPEN_READWRITE, "fault-vfs");
  if( rc!=SQLITE_OK ){ sqlite3_close(pSrc); return fail("open dest"); }

  pBak = sqlite3_backup_init(pDest, "main", pSrc, "main");
  if( !pBak ){
    sqlite3_close(pDest);
    sqlite3_close(pSrc);
    return fail("backup_init");
  }

  gArmOnJournalSync = 1;
  rc = sqlite3_backup_step(pBak, -1);
  (void)sqlite3_backup_finish(pBak);
  sqlite3_close(pDest);
  sqlite3_close(pSrc);

  if( rc==SQLITE_DONE || !gFaultTriggered ){
    return fail("expected backup write fault during csWasmCopyReplacement");
  }
  return 0;
}

int main(void){
  char zDir[] = "/tmp/doltlite-wasm-backup-XXXXXX";
  char zSrc[512], zDest[512], zReplace[512], zBackupTmp[512], zGcTmp[512];
  char zVal[128];
  sqlite3 *db = 0;
  FILE *fp;
  int rc;

  if( !mkdtemp(zDir) ) return fail("mkdtemp");
  snprintf(zSrc, sizeof(zSrc), "%s/src.db", zDir);
  snprintf(zDest, sizeof(zDest), "%s/dest.db", zDir);
  snprintf(zReplace, sizeof(zReplace), "%s/dest.db-gc-replace", zDir);
  snprintf(zBackupTmp, sizeof(zBackupTmp), "%s/dest.db-backup-tmp", zDir);
  snprintf(zGcTmp, sizeof(zGcTmp), "%s/dest.db-gc-tmp", zDir);

  if( sqlite3_initialize()!=SQLITE_OK ) return fail("sqlite3_initialize");
  gBaseVfs = sqlite3_vfs_find(0);
  if( !gBaseVfs ) return fail("sqlite3_vfs_find");

  memset(&gFaultVfs, 0, sizeof(gFaultVfs));
  gFaultVfs.iVersion = 2;
  gFaultVfs.szOsFile = (int)(((sizeof(FaultFile) + 7) & ~7) + gBaseVfs->szOsFile);
  gFaultVfs.mxPathname = gBaseVfs->mxPathname;
  gFaultVfs.zName = "fault-vfs";
  gFaultVfs.xOpen = faultOpen;
  gFaultVfs.xDelete = faultDelete;
  gFaultVfs.xAccess = faultAccess;
  gFaultVfs.xFullPathname = faultFullPathname;
  gFaultVfs.xRandomness = faultRandomness;
  gFaultVfs.xSleep = faultSleep;
  gFaultVfs.xCurrentTime = faultCurrentTime;
  gFaultVfs.xGetLastError = faultGetLastError;
  gFaultVfs.xCurrentTimeInt64 = faultCurrentTimeInt64;
  if( sqlite3_vfs_register(&gFaultVfs, 0)!=SQLITE_OK ){
    return fail("sqlite3_vfs_register");
  }

  /* Create source and destination databases. */
  if( sqlite3_open(zSrc, &db)!=SQLITE_OK ) return fail("create src");
  if( probeExecSql(db, "CREATE TABLE t(x TEXT); INSERT INTO t VALUES('from_backup');")!=SQLITE_OK
   || probeExecSql(db, "SELECT dolt_commit('-Am', 'src1');")!=SQLITE_OK ){
    sqlite3_close(db);
    return fail("populate src");
  }
  sqlite3_close(db);

  if( sqlite3_open(zDest, &db)!=SQLITE_OK ) return fail("create dest");
  if( probeExecSql(db, "CREATE TABLE t(x TEXT); INSERT INTO t VALUES('old_dest');")!=SQLITE_OK
   || probeExecSql(db, "SELECT dolt_commit('-Am', 'dest1');")!=SQLITE_OK ){
    sqlite3_close(db);
    return fail("populate dest");
  }
  sqlite3_close(db);

  /* Scenario 1: Crash mid-WASM backup copy (-gc-replace + -backup-tmp). */
  if( runCrashedBackup(zSrc, zDest)!=0 ) return 1;
  if( !fileExists(zReplace) || !fileExists(zBackupTmp) ){
    return fail("expected -gc-replace and -backup-tmp after backup crash");
  }
  rc = sqlite3_open(zDest, &db);
  if( rc!=SQLITE_OK ) return fail("reopen dest after backup crash");
  rc = querySingleText(db, "SELECT x FROM t;", zVal, sizeof(zVal));
  sqlite3_close(db);
  if( rc!=SQLITE_OK || strcmp(zVal, "from_backup")!=0 ){
    return fail("recovered dest did not contain backup data");
  }
  if( fileExists(zReplace) || fileExists(zBackupTmp) ){
    return fail("recovery did not clean up -gc-replace and -backup-tmp");
  }

  /* Scenario 2: Stale orphan -gc-tmp present alongside crashed backup. */
  if( sqlite3_open(zSrc, &db)!=SQLITE_OK ) return fail("reopen src for scenario 2");
  if( probeExecSql(db, "UPDATE t SET x='from_backup_2'; SELECT dolt_commit('-am', 'src2');")!=SQLITE_OK ){
    sqlite3_close(db);
    return fail("update src for scenario 2");
  }
  sqlite3_close(db);

  if( runCrashedBackup(zSrc, zDest)!=0 ) return 1;
  fp = fopen(zGcTmp, "wb");
  if( !fp ) return fail("create stale -gc-tmp");
  fwrite("stale-orphan-gc-tmp", 1, 19, fp);
  fclose(fp);

  rc = sqlite3_open(zDest, &db);
  if( rc!=SQLITE_OK ) return fail("reopen dest with stale -gc-tmp");
  rc = querySingleText(db, "SELECT x FROM t;", zVal, sizeof(zVal));
  sqlite3_close(db);
  if( rc!=SQLITE_OK || strcmp(zVal, "from_backup_2")!=0 ){
    return fail("recovered dest with stale -gc-tmp had wrong data");
  }
  if( fileExists(zReplace) || fileExists(zBackupTmp) ){
    return fail("scenario 2 did not clean up -gc-replace and -backup-tmp");
  }
  unlink(zGcTmp);

  /* Scenario 3: Crashed GC (-gc-replace + -gc-tmp) still recovers cleanly. */
  gBaseVfs->xFullPathname(gBaseVfs, zDest, (int)sizeof(gTargetPath), gTargetPath);
  gArmOnJournalSync = 0;
  gArmedFailWrite = 0;
  gFaultTriggered = 0;
  rc = sqlite3_open_v2(zDest, &db, SQLITE_OPEN_READWRITE, "fault-vfs");
  if( rc!=SQLITE_OK ) return fail("open dest for gc");
  if( probeExecSql(db, "UPDATE t SET x='after_gc'; SELECT dolt_commit('-am', 'gc1');")!=SQLITE_OK ){
    sqlite3_close(db);
    return fail("update dest before gc");
  }
  gArmOnJournalSync = 1;
  rc = sqlite3_exec(db, "SELECT dolt_gc();", 0, 0, 0);
  sqlite3_close(db);
  if( rc==SQLITE_OK || !gFaultTriggered ){
    return fail("expected gc write fault during csWasmCopyReplacement");
  }
  if( !fileExists(zReplace) || !fileExists(zGcTmp) ){
    return fail("expected -gc-replace and -gc-tmp after gc crash");
  }
  rc = sqlite3_open(zDest, &db);
  if( rc!=SQLITE_OK ) return fail("reopen dest after gc crash");
  rc = querySingleText(db, "SELECT x FROM t;", zVal, sizeof(zVal));
  sqlite3_close(db);
  if( rc!=SQLITE_OK || strcmp(zVal, "after_gc")!=0 ){
    return fail("recovered dest after gc crash had wrong data");
  }
  if( fileExists(zReplace) || fileExists(zGcTmp) ){
    return fail("gc recovery did not clean up -gc-replace and -gc-tmp");
  }

  /* Scenario 4: Missing both temp files with armed -gc-replace returns CORRUPT. */
  if( runCrashedBackup(zSrc, zDest)!=0 ) return 1;
  unlink(zBackupTmp);
  rc = sqlite3_open(zDest, &db);
  if( db ) sqlite3_close(db);
  if( rc!=SQLITE_CORRUPT ){
    return fail("expected SQLITE_CORRUPT when both temp files are missing");
  }

  unlink(zReplace);
  unlink(zDest);
  unlink(zSrc);
  rmdir(zDir);
  puts("amalgamation wasm backup crash recovery: PASS");
  sqlite3_shutdown();
  return 0;
}
