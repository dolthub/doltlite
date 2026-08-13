#include "sqlite3.h"
#include <stdio.h>
#include <string.h>

typedef struct MemData MemData;
typedef struct ReplaceTestFile ReplaceTestFile;

struct MemData {
  const char *zName;
  unsigned char aData[128];
  int nData;
  int exists;
};

struct ReplaceTestFile {
  sqlite3_file base;
  MemData *pData;
};

static MemData gTmp = { "tmp" };
static MemData gDest = { "dest" };
static int gFailOpenDest;
static int gFailWriteDest;
static sqlite3_vfs gVfs;

static int memClose(sqlite3_file *pFile){
  (void)pFile;
  return SQLITE_OK;
}

static int memRead(sqlite3_file *pFile, void *pBuf, int n, sqlite3_int64 off){
  MemData *p = ((ReplaceTestFile*)pFile)->pData;
  if( off<0 || off+n>p->nData ) return SQLITE_IOERR_SHORT_READ;
  memcpy(pBuf, p->aData + off, n);
  return SQLITE_OK;
}

static int memWrite(
  sqlite3_file *pFile,
  const void *pBuf,
  int n,
  sqlite3_int64 off
){
  MemData *p = ((ReplaceTestFile*)pFile)->pData;
  if( p==&gDest && gFailWriteDest ){
    gFailWriteDest = 0;
    return SQLITE_IOERR_WRITE;
  }
  if( off<0 || off+n>(int)sizeof(p->aData) ) return SQLITE_FULL;
  memcpy(p->aData + off, pBuf, n);
  if( off+n>p->nData ) p->nData = (int)(off+n);
  return SQLITE_OK;
}

static int memTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  MemData *p = ((ReplaceTestFile*)pFile)->pData;
  if( size<0 || size>(int)sizeof(p->aData) ) return SQLITE_FULL;
  p->nData = (int)size;
  return SQLITE_OK;
}

static int memSync(sqlite3_file *pFile, int flags){
  (void)pFile;
  (void)flags;
  return SQLITE_OK;
}

static int memFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  *pSize = ((ReplaceTestFile*)pFile)->pData->nData;
  return SQLITE_OK;
}

static int memLock(sqlite3_file *pFile, int lock){
  (void)pFile;
  (void)lock;
  return SQLITE_OK;
}

static int memUnlock(sqlite3_file *pFile, int lock){
  (void)pFile;
  (void)lock;
  return SQLITE_OK;
}

static int memCheckReservedLock(sqlite3_file *pFile, int *pOut){
  (void)pFile;
  *pOut = 0;
  return SQLITE_OK;
}

static int memFileControl(sqlite3_file *pFile, int op, void *pArg){
  (void)pFile;
  (void)op;
  (void)pArg;
  return SQLITE_NOTFOUND;
}

static int memSectorSize(sqlite3_file *pFile){
  (void)pFile;
  return 512;
}

static int memDeviceCharacteristics(sqlite3_file *pFile){
  (void)pFile;
  return 0;
}

static const sqlite3_io_methods gMethods = {
  1,
  memClose,
  memRead,
  memWrite,
  memTruncate,
  memSync,
  memFileSize,
  memLock,
  memUnlock,
  memCheckReservedLock,
  memFileControl,
  memSectorSize,
  memDeviceCharacteristics
};

static MemData *findData(const char *zName){
  if( strcmp(zName, gTmp.zName)==0 ) return &gTmp;
  if( strcmp(zName, gDest.zName)==0 ) return &gDest;
  return 0;
}

static int memOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  MemData *pData = findData(zName);
  ReplaceTestFile *p = (ReplaceTestFile*)pFile;
  (void)pVfs;
  memset(p, 0, sizeof(*p));
  if( pData==&gDest && gFailOpenDest ){
    gFailOpenDest = 0;
    return SQLITE_CANTOPEN;
  }
  if( pData==0 ) return SQLITE_CANTOPEN;
  if( !pData->exists ){
    if( (flags & SQLITE_OPEN_CREATE)==0 ) return SQLITE_CANTOPEN;
    pData->exists = 1;
    pData->nData = 0;
  }else if( flags & SQLITE_OPEN_EXCLUSIVE ){
    return SQLITE_CANTOPEN;
  }
  p->pData = pData;
  p->base.pMethods = &gMethods;
  if( pOutFlags ) *pOutFlags = flags;
  return SQLITE_OK;
}

static int memDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir){
  MemData *pData = findData(zName);
  (void)pVfs;
  (void)syncDir;
  if( pData==0 || !pData->exists ) return SQLITE_IOERR_DELETE_NOENT;
  pData->exists = 0;
  pData->nData = 0;
  return SQLITE_OK;
}

static int memAccess(
  sqlite3_vfs *pVfs,
  const char *zName,
  int flags,
  int *pOut
){
  MemData *pData = findData(zName);
  (void)pVfs;
  (void)flags;
  *pOut = pData!=0 && pData->exists;
  return SQLITE_OK;
}

static int memFullPathname(
  sqlite3_vfs *pVfs,
  const char *zName,
  int nOut,
  char *zOut
){
  (void)pVfs;
  sqlite3_snprintf(nOut, zOut, "%s", zName);
  return SQLITE_OK;
}

static void resetFiles(void){
  static const unsigned char aOld[] = "old database";
  static const unsigned char aNew[] = "new database contents";
  gTmp.exists = 1;
  gTmp.nData = (int)sizeof(aNew);
  memcpy(gTmp.aData, aNew, sizeof(aNew));
  gDest.exists = 1;
  gDest.nData = (int)sizeof(aOld);
  memcpy(gDest.aData, aOld, sizeof(aOld));
}

static int oldDestinationIsIntact(void){
  static const unsigned char aOld[] = "old database";
  return gDest.exists && gDest.nData==(int)sizeof(aOld)
      && memcmp(gDest.aData, aOld, sizeof(aOld))==0;
}

static int replacementIsIntact(void){
  static const unsigned char aNew[] = "new database contents";
  return gTmp.exists && gTmp.nData==(int)sizeof(aNew)
      && memcmp(gTmp.aData, aNew, sizeof(aNew))==0;
}

static int newDestinationIsIntact(void){
  static const unsigned char aNew[] = "new database contents";
  return gDest.exists && gDest.nData==(int)sizeof(aNew)
      && memcmp(gDest.aData, aNew, sizeof(aNew))==0;
}

int sqlite3_os_init(void){ return sqlite3_vfs_register(&gVfs, 1); }
int sqlite3_os_end(void){ return SQLITE_OK; }

static int fail(const char *zName){
  fprintf(stderr, "FAIL: %s\n", zName);
  return 1;
}

int main(void){
  int rc;
  int retainTmp;

  memset(&gVfs, 0, sizeof(gVfs));
  gVfs.iVersion = 1;
  gVfs.szOsFile = sizeof(ReplaceTestFile);
  gVfs.mxPathname = 32;
  gVfs.zName = "replace-test";
  gVfs.xOpen = memOpen;
  gVfs.xDelete = memDelete;
  gVfs.xAccess = memAccess;
  gVfs.xFullPathname = memFullPathname;
  if( sqlite3_initialize()!=SQLITE_OK ) return fail("sqlite3_initialize");

  resetFiles();
  gFailOpenDest = 1;
  retainTmp = 0;
  rc = sqlite3OsReplaceFile(&gVfs, "tmp", "dest", &retainTmp);
  if( rc==SQLITE_OK || !retainTmp || !oldDestinationIsIntact()
   || !replacementIsIntact() ) return fail("destination open failure");

  resetFiles();
  gFailWriteDest = 1;
  retainTmp = 0;
  rc = sqlite3OsReplaceFile(&gVfs, "tmp", "dest", &retainTmp);
  if( rc==SQLITE_OK || !retainTmp || !oldDestinationIsIntact()
   || !replacementIsIntact() ) return fail("destination write failure");

  resetFiles();
  retainTmp = 0;
  rc = sqlite3OsReplaceFile(&gVfs, "tmp", "dest", &retainTmp);
  if( rc!=SQLITE_OK || retainTmp || gTmp.exists
   || !newDestinationIsIntact() ) return fail("successful replacement");

  puts("amalgamation replacement failure recovery: PASS");
  sqlite3_shutdown();
  return 0;
}
