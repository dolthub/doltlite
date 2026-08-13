#undef rename
#undef fsync

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int rename(const char *, const char *);
int fsync(int);

static int gFailRename;
static int gFailFsync;

int replaceTestRename(const char *zFrom, const char *zTo){
  if( gFailRename ){
    gFailRename = 0;
    errno = EIO;
    return -1;
  }
  return rename(zFrom, zTo);
}

int replaceTestFsync(int fd){
  if( gFailFsync ){
    gFailFsync = 0;
    errno = EIO;
    return -1;
  }
  return fsync(fd);
}

static int writeFile(const char *zPath, const char *zText){
  FILE *f = fopen(zPath, "wb");
  size_t n = strlen(zText);
  if( f==0 ) return 1;
  if( fwrite(zText, 1, n, f)!=n ){
    fclose(f);
    return 1;
  }
  return fclose(f)!=0;
}

static int fileEquals(const char *zPath, const char *zText){
  char zBuf[64];
  FILE *f = fopen(zPath, "rb");
  size_t n;
  if( f==0 ) return 0;
  n = fread(zBuf, 1, sizeof(zBuf), f);
  if( fclose(f)!=0 ) return 0;
  return n==strlen(zText) && memcmp(zBuf, zText, n)==0;
}

static int fileExists(const char *zPath){
  return access(zPath, F_OK)==0;
}

static int resetFiles(const char *zTmp, const char *zDest){
  return writeFile(zTmp, "new database") || writeFile(zDest, "old database");
}

static int fail(const char *zName){
  fprintf(stderr, "FAIL: %s\n", zName);
  return 1;
}

int main(void){
  char zDir[128];
  char zTmp[160];
  char zDest[160];
  sqlite3_vfs *pVfs;
  int retainTmp;
  int rc;

  sqlite3_snprintf(sizeof(zDir), zDir, "doltlite-replace-%ld", (long)getpid());
  sqlite3_snprintf(sizeof(zTmp), zTmp, "%s/tmp", zDir);
  sqlite3_snprintf(sizeof(zDest), zDest, "%s/dest", zDir);
  if( mkdir(zDir, 0700)!=0 ) return fail("mkdir");
  if( sqlite3_initialize()!=SQLITE_OK ) return fail("sqlite3_initialize");
  pVfs = sqlite3_vfs_find(0);
  if( pVfs==0 ) return fail("sqlite3_vfs_find");

  if( resetFiles(zTmp, zDest) ) return fail("rename setup");
  gFailRename = 1;
  retainTmp = 0;
  rc = sqlite3OsReplaceFile(pVfs, zTmp, zDest, &retainTmp);
  if( rc==SQLITE_OK || !retainTmp || !fileEquals(zTmp, "new database")
   || !fileEquals(zDest, "old database") ) return fail("rename failure");

  if( resetFiles(zTmp, zDest) ) return fail("sync setup");
  gFailFsync = 1;
  retainTmp = 1;
  rc = sqlite3OsReplaceFile(pVfs, zTmp, zDest, &retainTmp);
  if( rc==SQLITE_OK || retainTmp || fileExists(zTmp)
   || !fileEquals(zDest, "new database") ) return fail("sync failure");

  if( resetFiles(zTmp, zDest) ) return fail("success setup");
  retainTmp = 1;
  rc = sqlite3OsReplaceFile(pVfs, zTmp, zDest, &retainTmp);
  if( rc!=SQLITE_OK || retainTmp || fileExists(zTmp)
   || !fileEquals(zDest, "new database") ) return fail("success");

  unlink(zDest);
  rmdir(zDir);
  sqlite3_shutdown();
  puts("native replacement failure recovery: PASS");
  return 0;
}
