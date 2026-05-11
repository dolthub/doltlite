#include <sqliteInt.h>
#if SQLITE_OS_KV || (SQLITE_OS_UNIX && defined(SQLITE_OS_KV_OPTIONAL))

#if 0
#define SQLITE_KV_TRACE(X)  printf X
#else
#define SQLITE_KV_TRACE(X)
#endif

#if 0
#define SQLITE_KV_LOG(X)  printf X
#else
#define SQLITE_KV_LOG(X)
#endif

typedef struct KVVfsFile KVVfsFile;

struct KVVfsFile {
  sqlite3_file base;
  const char *zClass;
  int isJournal;
  unsigned int nJrnl;
  char *aJrnl;
  int szPage;
  sqlite3_int64 szDb;
  char *aData;
};
#define SQLITE_KVOS_SZ 133073

static int kvvfsClose(sqlite3_file*);
static int kvvfsReadDb(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int kvvfsReadJrnl(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int kvvfsWriteDb(sqlite3_file*,const void*,int iAmt, sqlite3_int64);
static int kvvfsWriteJrnl(sqlite3_file*,const void*,int iAmt, sqlite3_int64);
static int kvvfsTruncateDb(sqlite3_file*, sqlite3_int64 size);
static int kvvfsTruncateJrnl(sqlite3_file*, sqlite3_int64 size);
static int kvvfsSyncDb(sqlite3_file*, int flags);
static int kvvfsSyncJrnl(sqlite3_file*, int flags);
static int kvvfsFileSizeDb(sqlite3_file*, sqlite3_int64 *pSize);
static int kvvfsFileSizeJrnl(sqlite3_file*, sqlite3_int64 *pSize);
static int kvvfsLock(sqlite3_file*, int);
static int kvvfsUnlock(sqlite3_file*, int);
static int kvvfsCheckReservedLock(sqlite3_file*, int *pResOut);
static int kvvfsFileControlDb(sqlite3_file*, int op, void *pArg);
static int kvvfsFileControlJrnl(sqlite3_file*, int op, void *pArg);
static int kvvfsSectorSize(sqlite3_file*);
static int kvvfsDeviceCharacteristics(sqlite3_file*);

static int kvvfsOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int kvvfsDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int kvvfsAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int kvvfsFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *kvvfsDlOpen(sqlite3_vfs*, const char *zFilename);
static int kvvfsRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int kvvfsSleep(sqlite3_vfs*, int microseconds);
static int kvvfsCurrentTime(sqlite3_vfs*, double*);
static int kvvfsCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

static sqlite3_vfs sqlite3OsKvvfsObject = {
  2,
  sizeof(KVVfsFile),
  1024,
  0,
  "kvvfs",
  0,
  kvvfsOpen,
  kvvfsDelete,
  kvvfsAccess,
  kvvfsFullPathname,
  kvvfsDlOpen,
  0,
  0,
  0,
  kvvfsRandomness,
  kvvfsSleep,
  kvvfsCurrentTime,
  0,
  kvvfsCurrentTimeInt64
};

static sqlite3_io_methods kvvfs_db_io_methods = {
  1,
  kvvfsClose,
  kvvfsReadDb,
  kvvfsWriteDb,
  kvvfsTruncateDb,
  kvvfsSyncDb,
  kvvfsFileSizeDb,
  kvvfsLock,
  kvvfsUnlock,
  kvvfsCheckReservedLock,
  kvvfsFileControlDb,
  kvvfsSectorSize,
  kvvfsDeviceCharacteristics,
  0,
  0,
  0,
  0,
  0,
  0
};

static sqlite3_io_methods kvvfs_jrnl_io_methods = {
  1,
  kvvfsClose,
  kvvfsReadJrnl,
  kvvfsWriteJrnl,
  kvvfsTruncateJrnl,
  kvvfsSyncJrnl,
  kvvfsFileSizeJrnl,
  kvvfsLock,
  kvvfsUnlock,
  kvvfsCheckReservedLock,
  kvvfsFileControlJrnl,
  kvvfsSectorSize,
  kvvfsDeviceCharacteristics,
  0,
  0,
  0,
  0,
  0,
  0
};

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef SQLITE_WASM
static int kvrecordWrite(const char*, const char *zKey, const char *zData);
static int kvrecordDelete(const char*, const char *zKey);
static int kvrecordRead(const char*, const char *zKey, char *zBuf, int nBuf);
#endif
#ifndef KVRECORD_KEY_SZ
#define KVRECORD_KEY_SZ 32
#endif

static void kvrecordMakeKey(
  const char *zClass,
  const char *zKeyIn,
  char *zKeyOut
){
  assert( zKeyIn );
  assert( zKeyOut );
  assert( zClass );
  sqlite3_snprintf(KVRECORD_KEY_SZ, zKeyOut, "kvvfs-%s-%s",
                   zClass, zKeyIn);
}

#ifndef SQLITE_WASM
static int kvrecordWrite(
  const char *zClass,
  const char *zKey,
  const char *zData
){
  FILE *fd;
  char zXKey[KVRECORD_KEY_SZ];
  kvrecordMakeKey(zClass, zKey, zXKey);
  fd = fopen(zXKey, "wb");
  if( fd ){
    SQLITE_KV_TRACE(("KVVFS-WRITE  %-15s (%d) %.50s%s\n", zXKey,
                 (int)strlen(zData), zData,
                 strlen(zData)>50 ? "..." : ""));
    fputs(zData, fd);
    fclose(fd);
    return 0;
  }else{
    return 1;
  }
}

static int kvrecordDelete(const char *zClass, const char *zKey){
  char zXKey[KVRECORD_KEY_SZ];
  kvrecordMakeKey(zClass, zKey, zXKey);
  unlink(zXKey);
  SQLITE_KV_TRACE(("KVVFS-DELETE %-15s\n", zXKey));
  return 0;
}

static int kvrecordRead(
  const char *zClass,
  const char *zKey,
  char *zBuf,
  int nBuf
){
  FILE *fd;
  struct stat buf;
  char zXKey[KVRECORD_KEY_SZ];
  kvrecordMakeKey(zClass, zKey, zXKey);
  if( access(zXKey, R_OK)!=0
   || stat(zXKey, &buf)!=0
   || !S_ISREG(buf.st_mode)
  ){
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (-1)\n", zXKey));
    return -1;
  }
  if( nBuf<=0 ){
    return (int)buf.st_size;
  }else if( nBuf==1 ){
    zBuf[0] = 0;
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (%d)\n", zXKey,
                 (int)buf.st_size));
    return (int)buf.st_size;
  }
  if( nBuf > buf.st_size + 1 ){
    nBuf = buf.st_size + 1;
  }
  fd = fopen(zXKey, "rb");
  if( fd==0 ){
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (-1)\n", zXKey));
    return -1;
  }else{
    sqlite3_int64 n = fread(zBuf, 1, nBuf-1, fd);
    fclose(fd);
    zBuf[n] = 0;
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (%lld) %.50s%s\n", zXKey,
                 n, zBuf, n>50 ? "..." : ""));
    return (int)n;
  }
}
#endif

typedef struct sqlite3_kvvfs_methods sqlite3_kvvfs_methods;
struct sqlite3_kvvfs_methods {
  int (*xRcrdRead)(const char*, const char *zKey, char *zBuf, int nBuf);
  int (*xRcrdWrite)(const char*, const char *zKey, const char *zData);
  int (*xRcrdDelete)(const char*, const char *zKey);
  const int nKeySize;
  const int nBufferSize;
#ifndef SQLITE_WASM
#  define MAYBE_CONST const
#else
#  define MAYBE_CONST
#endif
  MAYBE_CONST sqlite3_vfs * pVfs;
  MAYBE_CONST sqlite3_io_methods *pIoDb;
  MAYBE_CONST sqlite3_io_methods *pIoJrnl;
#undef MAYBE_CONST
};

#ifndef SQLITE_WASM
const
#endif
sqlite3_kvvfs_methods sqlite3KvvfsMethods = {
#ifndef SQLITE_WASM
  .xRcrdRead       = kvrecordRead,
  .xRcrdWrite      = kvrecordWrite,
  .xRcrdDelete     = kvrecordDelete,
#else
  .xRcrdRead       = 0,
  .xRcrdWrite      = 0,
  .xRcrdDelete     = 0,
#endif
  .nKeySize        = KVRECORD_KEY_SZ,
  .nBufferSize     = SQLITE_KVOS_SZ,
  .pVfs            = &sqlite3OsKvvfsObject,
  .pIoDb           = &kvvfs_db_io_methods,
  .pIoJrnl         = &kvvfs_jrnl_io_methods
};

#ifndef SQLITE_WASM
static
#endif
int kvvfsEncode(const char *aData, int nData, char *aOut){
  int i, j;
  const unsigned char *a = (const unsigned char*)aData;
  for(i=j=0; i<nData; i++){
    unsigned char c = a[i];
    if( c!=0 ){
      aOut[j++] = "0123456789ABCDEF"[c>>4];
      aOut[j++] = "0123456789ABCDEF"[c&0xf];
    }else{
      int k;
      for(k=1; i+k<nData && a[i+k]==0; k++){}
      i += k-1;
      while( k>0 ){
        aOut[j++] = 'a'+(k%26);
        k /= 26;
      }
    }
  }
  aOut[j] = 0;
  return j;
}

static const signed char kvvfsHexValue[256] = {
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
   0,  1,  2,  3,  4,  5,  6,  7,    8,  9, -1, -1, -1, -1, -1, -1,
  -1, 10, 11, 12, 13, 14, 15, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,

  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1
};

#ifndef SQLITE_WASM
static
#endif
int kvvfsDecode(const char *a, char *aOut, int nOut){
  int i, j;
  int c;
  const unsigned char *aIn = (const unsigned char*)a;
  i = 0;
  j = 0;
  while( 1 ){
    c = kvvfsHexValue[aIn[i]];
    if( c<0 ){
      int n = 0;
      int mult = 1;
      c = aIn[i];
      if( c==0 ) break;
      while( c>='a' && c<='z' ){
        n += (c - 'a')*mult;
        mult *= 26;
        c = aIn[++i];
      }
      if( j+n>nOut ) return -1;
      memset(&aOut[j], 0, n);
      j += n;
      if( c==0 || mult==1 ) break;
    }else{
      aOut[j] = c<<4;
      c = kvvfsHexValue[aIn[++i]];
      if( c<0 ) return -1 ;
      aOut[j++] += c;
      i++;
    }
  }
  return j;
}

static void kvvfsDecodeJournal(
  KVVfsFile *pFile,
  const char *zTxt,
  int nTxt
){
  unsigned int n = 0;
  int c, i, mult;
  i = 0;
  mult = 1;
  while( (c = zTxt[i++])>='a' && c<='z' ){
    n += (zTxt[i] - 'a')*mult;
    mult *= 26;
  }
  sqlite3_free(pFile->aJrnl);
  pFile->aJrnl = sqlite3_malloc64( n );
  if( pFile->aJrnl==0 ){
    pFile->nJrnl = 0;
    return;
  }
  pFile->nJrnl = n;
  n = kvvfsDecode(zTxt+i, pFile->aJrnl, pFile->nJrnl);
  if( n<pFile->nJrnl ){
    sqlite3_free(pFile->aJrnl);
    pFile->aJrnl = 0;
    pFile->nJrnl = 0;
  }
}

static sqlite3_int64 kvvfsReadFileSize(KVVfsFile *pFile){
  char zData[50];
  zData[0] = 0;
  sqlite3KvvfsMethods.xRcrdRead(pFile->zClass, "sz", zData,
                                sizeof(zData)-1);
  return strtoll(zData, 0, 0);
}
static int kvvfsWriteFileSize(KVVfsFile *pFile, sqlite3_int64 sz){
  char zData[50];
  sqlite3_snprintf(sizeof(zData), zData, "%lld", sz);
  return sqlite3KvvfsMethods.xRcrdWrite(pFile->zClass, "sz", zData);
}

static int kvvfsClose(sqlite3_file *pProtoFile){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;

  SQLITE_KV_LOG(("xClose %s %s\n", pFile->zClass,
             pFile->isJournal ? "journal" : "db"));
  sqlite3_free(pFile->aJrnl);
  sqlite3_free(pFile->aData);
#ifdef SQLITE_WASM
  memset(pFile, 0, sizeof(*pFile));
#endif
  return SQLITE_OK;
}

static int kvvfsReadJrnl(
  sqlite3_file *pProtoFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  assert( pFile->isJournal );
  SQLITE_KV_LOG(("xRead('%s-journal',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  if( pFile->aJrnl==0 ){
    int rc;
    int szTxt = sqlite3KvvfsMethods.xRcrdRead(pFile->zClass, "jrnl",
                                              0, 0);
    char *aTxt;
    if( szTxt<=4 ){
      return SQLITE_IOERR;
    }
    aTxt = sqlite3_malloc64( szTxt+1 );
    if( aTxt==0 ) return SQLITE_NOMEM;
    rc = sqlite3KvvfsMethods.xRcrdRead(pFile->zClass, "jrnl",
                                       aTxt, szTxt+1);
    if( rc>=0 ){
      kvvfsDecodeJournal(pFile, aTxt, szTxt);
    }
    sqlite3_free(aTxt);
    if( rc ) return rc;
    if( pFile->aJrnl==0 ) return SQLITE_IOERR;
  }
  if( iOfst+iAmt>pFile->nJrnl ){
    return SQLITE_IOERR_SHORT_READ;
  }
  memcpy(zBuf, pFile->aJrnl+iOfst, iAmt);
  return SQLITE_OK;
}

static int kvvfsReadDb(
  sqlite3_file *pProtoFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  unsigned int pgno;
  int got, n;
  char zKey[30];
  char *aData = pFile->aData;
  assert( iOfst>=0 );
  assert( iAmt>=0 );
  SQLITE_KV_LOG(("xRead('%s-db',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  if( iOfst+iAmt>=512 ){
    if( (iOfst % iAmt)!=0 ){
      return SQLITE_IOERR_READ;
    }
    if( (iAmt & (iAmt-1))!=0 || iAmt<512 || iAmt>65536 ){
      return SQLITE_IOERR_READ;
    }
    pFile->szPage = iAmt;
    pgno = 1 + iOfst/iAmt;
  }else{
    pgno = 1;
  }
  sqlite3_snprintf(sizeof(zKey), zKey, "%u", pgno);
  got = sqlite3KvvfsMethods.xRcrdRead(pFile->zClass, zKey,
                                      aData, SQLITE_KVOS_SZ-1);
  if( got<0 ){
    n = 0;
  }else{
    aData[got] = 0;
    if( iOfst+iAmt<512 ){
      int k = iOfst+iAmt;
      aData[k*2] = 0;
      n = kvvfsDecode(aData, &aData[2000], SQLITE_KVOS_SZ-2000);
      if( n>=iOfst+iAmt ){
        memcpy(zBuf, &aData[2000+iOfst], iAmt);
        n = iAmt;
      }else{
        n = 0;
      }
    }else{
      n = kvvfsDecode(aData, zBuf, iAmt);
    }
  }
  if( n<iAmt ){
    memset(zBuf+n, 0, iAmt-n);
    return SQLITE_IOERR_SHORT_READ;
  }
  return SQLITE_OK;
}

static int kvvfsWriteJrnl(
  sqlite3_file *pProtoFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  sqlite3_int64 iEnd = iOfst+iAmt;
  SQLITE_KV_LOG(("xWrite('%s-journal',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  if( iEnd>=0x10000000 ) return SQLITE_FULL;
  if( pFile->aJrnl==0 || pFile->nJrnl<iEnd ){
    char *aNew = sqlite3_realloc(pFile->aJrnl, iEnd);
    if( aNew==0 ){
      return SQLITE_IOERR_NOMEM;
    }
    pFile->aJrnl = aNew;
    if( pFile->nJrnl<iOfst ){
      memset(pFile->aJrnl+pFile->nJrnl, 0, iOfst-pFile->nJrnl);
    }
    pFile->nJrnl = iEnd;
  }
  memcpy(pFile->aJrnl+iOfst, zBuf, iAmt);
  return SQLITE_OK;
}

static int kvvfsWriteDb(
  sqlite3_file *pProtoFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  unsigned int pgno;
  char zKey[30];
  char *aData = pFile->aData;
  int rc;
  SQLITE_KV_LOG(("xWrite('%s-db',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  assert( iAmt>=512 && iAmt<=65536 );
  assert( (iAmt & (iAmt-1))==0 );
  assert( pFile->szPage<0 || pFile->szPage==iAmt );
  pFile->szPage = iAmt;
  pgno = 1 + iOfst/iAmt;
  sqlite3_snprintf(sizeof(zKey), zKey, "%u", pgno);
  kvvfsEncode(zBuf, iAmt, aData);
  rc = sqlite3KvvfsMethods.xRcrdWrite(pFile->zClass, zKey, aData);
  if( 0==rc ){
    if( iOfst+iAmt > pFile->szDb ){
      pFile->szDb = iOfst + iAmt;
    }
  }
  return rc;
}

static int kvvfsTruncateJrnl(sqlite3_file *pProtoFile, sqlite_int64 size){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  SQLITE_KV_LOG(("xTruncate('%s-journal',%lld)\n", pFile->zClass, size));
  assert( size==0 );
  sqlite3KvvfsMethods.xRcrdDelete(pFile->zClass, "jrnl");
  sqlite3_free(pFile->aJrnl);
  pFile->aJrnl = 0;
  pFile->nJrnl = 0;
  return SQLITE_OK;
}
static int kvvfsTruncateDb(sqlite3_file *pProtoFile, sqlite_int64 size){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  if( pFile->szDb>size
   && pFile->szPage>0
   && (size % pFile->szPage)==0
  ){
    char zKey[50];
    unsigned int pgno, pgnoMax;
    SQLITE_KV_LOG(("xTruncate('%s-db',%lld)\n", pFile->zClass, size));
    pgno = 1 + size/pFile->szPage;
    pgnoMax = 2 + pFile->szDb/pFile->szPage;
    while( pgno<=pgnoMax ){
      sqlite3_snprintf(sizeof(zKey), zKey, "%u", pgno);
      sqlite3KvvfsMethods.xRcrdDelete(pFile->zClass, zKey);
      pgno++;
    }
    pFile->szDb = size;
    return kvvfsWriteFileSize(pFile, size) ? SQLITE_IOERR : SQLITE_OK;
  }
  return SQLITE_IOERR;
}

static int kvvfsSyncJrnl(sqlite3_file *pProtoFile, int flags){
  int i, n;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  char *zOut;
  SQLITE_KV_LOG(("xSync('%s-journal')\n", pFile->zClass));
  if( pFile->nJrnl<=0 ){
    return kvvfsTruncateJrnl(pProtoFile, 0);
  }
  zOut = sqlite3_malloc64( pFile->nJrnl*2 + 50 );
  if( zOut==0 ){
    return SQLITE_IOERR_NOMEM;
  }
  n = pFile->nJrnl;
  i = 0;
  do{
    zOut[i++] = 'a' + (n%26);
    n /= 26;
  }while( n>0 );
  zOut[i++] = ' ';
  kvvfsEncode(pFile->aJrnl, pFile->nJrnl, &zOut[i]);
  i = sqlite3KvvfsMethods.xRcrdWrite(pFile->zClass, "jrnl", zOut);
  sqlite3_free(zOut);
  return i ? SQLITE_IOERR : SQLITE_OK;
}
static int kvvfsSyncDb(sqlite3_file *pProtoFile, int flags){
  return SQLITE_OK;
}

static int kvvfsFileSizeJrnl(sqlite3_file *pProtoFile, sqlite_int64 *pSize){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  SQLITE_KV_LOG(("xFileSize('%s-journal')\n", pFile->zClass));
  *pSize = pFile->nJrnl;
  return SQLITE_OK;
}
static int kvvfsFileSizeDb(sqlite3_file *pProtoFile, sqlite_int64 *pSize){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  SQLITE_KV_LOG(("xFileSize('%s-db')\n", pFile->zClass));
  if( pFile->szDb>=0 ){
    *pSize = pFile->szDb;
  }else{
    *pSize = kvvfsReadFileSize(pFile);
  }
  return SQLITE_OK;
}

static int kvvfsLock(sqlite3_file *pProtoFile, int eLock){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  assert( !pFile->isJournal );
  SQLITE_KV_LOG(("xLock(%s,%d)\n", pFile->zClass, eLock));

  if( eLock!=SQLITE_LOCK_NONE ){
    pFile->szDb = kvvfsReadFileSize(pFile);
  }
  return SQLITE_OK;
}

static int kvvfsUnlock(sqlite3_file *pProtoFile, int eLock){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  assert( !pFile->isJournal );
  SQLITE_KV_LOG(("xUnlock(%s,%d)\n", pFile->zClass, eLock));
  if( eLock==SQLITE_LOCK_NONE ){
    pFile->szDb = -1;
  }
  return SQLITE_OK;
}

static int kvvfsCheckReservedLock(sqlite3_file *pProtoFile, int *pResOut){
  SQLITE_KV_LOG(("xCheckReservedLock\n"));
  *pResOut = 0;
  return SQLITE_OK;
}

static int kvvfsFileControlJrnl(sqlite3_file *pProtoFile, int op, void *pArg){
  SQLITE_KV_LOG(("xFileControl(%d) on journal\n", op));
  return SQLITE_NOTFOUND;
}
static int kvvfsFileControlDb(sqlite3_file *pProtoFile, int op, void *pArg){
  SQLITE_KV_LOG(("xFileControl(%d) on database\n", op));
  if( op==SQLITE_FCNTL_SYNC ){
    KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
    int rc = SQLITE_OK;
    SQLITE_KV_LOG(("xSync('%s-db')\n", pFile->zClass));
    if( pFile->szDb>0 && 0!=kvvfsWriteFileSize(pFile, pFile->szDb) ){
      rc = SQLITE_IOERR;
    }
    return rc;
  }
  return SQLITE_NOTFOUND;
}

static int kvvfsSectorSize(sqlite3_file *pFile){
  return 512;
}

static int kvvfsDeviceCharacteristics(sqlite3_file *pProtoFile){
  return 0;
}

static int kvvfsOpen(
  sqlite3_vfs *pProtoVfs,
  const char *zName,
  sqlite3_file *pProtoFile,
  int flags,
  int *pOutFlags
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  if( zName==0 ) zName = "";
  SQLITE_KV_LOG(("xOpen(\"%s\")\n", zName));
  assert(!pFile->zClass);
  assert(!pFile->aData);
  assert(!pFile->aJrnl);
  assert(!pFile->nJrnl);
  assert(!pFile->base.pMethods);
  pFile->szPage = -1;
  pFile->szDb = -1;
  if( 0==sqlite3_strglob("*-journal", zName) ){
    pFile->isJournal = 1;
    pFile->base.pMethods = &kvvfs_jrnl_io_methods;
    if( 0==strcmp("session-journal",zName) ){
      pFile->zClass = "session";
    }else if( 0==strcmp("local-journal",zName) ){
      pFile->zClass = "local";
    }
  }else{
    pFile->isJournal = 0;
    pFile->base.pMethods = &kvvfs_db_io_methods;
  }
  if( !pFile->zClass ){
    pFile->zClass = zName;
  }
  pFile->aData = sqlite3_malloc64(SQLITE_KVOS_SZ);
  if( pFile->aData==0 ){
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int kvvfsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  int rc ;
  if( strcmp(zPath, "local-journal")==0 ){
    rc = sqlite3KvvfsMethods.xRcrdDelete("local", "jrnl");
  }else
  if( strcmp(zPath, "session-journal")==0 ){
    rc = sqlite3KvvfsMethods.xRcrdDelete("session", "jrnl");
  }
  else{
    rc = 0;
  }
  return rc;
}

static int kvvfsAccess(
  sqlite3_vfs *pProtoVfs,
  const char *zPath,
  int flags,
  int *pResOut
){
  SQLITE_KV_LOG(("xAccess(\"%s\")\n", zPath));
#if 0 && defined(SQLITE_WASM)
  const char *zKey = (0==sqlite3_strglob("*-journal", zPath))
    ? "jrnl" : "sz";
  *pResOut =
    sqlite3KvvfsMethods.xRcrdRead(zPath, zKey, 0, 0)>0;
#else
  if( strcmp(zPath, "local-journal")==0 ){
    *pResOut =
      sqlite3KvvfsMethods.xRcrdRead("local", "jrnl", 0, 0)>0;
  }else
  if( strcmp(zPath, "session-journal")==0 ){
    *pResOut =
      sqlite3KvvfsMethods.xRcrdRead("session", "jrnl", 0, 0)>0;
  }else
  if( strcmp(zPath, "local")==0 ){
    *pResOut =
      sqlite3KvvfsMethods.xRcrdRead("local", "sz", 0, 0)>0;
  }else
  if( strcmp(zPath, "session")==0 ){
    *pResOut =
      sqlite3KvvfsMethods.xRcrdRead("session", "sz", 0, 0)>0;
  }else
  {
    *pResOut = 0;
  }
#endif
  SQLITE_KV_LOG(("xAccess returns %d\n",*pResOut));
  return SQLITE_OK;
}

static int kvvfsFullPathname(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int nOut,
  char *zOut
){
  size_t nPath;
#ifdef SQLITE_OS_KV_ALWAYS_LOCAL
  zPath = "local";
#endif
  nPath = strlen(zPath);
  SQLITE_KV_LOG(("xFullPathname(\"%s\")\n", zPath));
  if( nOut<nPath+1 ) nPath = nOut - 1;
  memcpy(zOut, zPath, nPath);
  zOut[nPath] = 0;
  return SQLITE_OK;
}

static void *kvvfsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}

static int kvvfsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  memset(zBufOut, 0, nByte);
  return nByte;
}

static int kvvfsSleep(sqlite3_vfs *pVfs, int nMicro){
  return SQLITE_OK;
}

static int kvvfsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  sqlite3_int64 i = 0;
  int rc;
  rc = kvvfsCurrentTimeInt64(0, &i);
  *pTimeOut = i/86400000.0;
  return rc;
}
#include <sys/time.h>
static int kvvfsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut){
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
  struct timeval sNow;
  (void)gettimeofday(&sNow, 0);
  *pTimeOut = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
  return SQLITE_OK;
}
#endif

#if SQLITE_OS_KV
int sqlite3_os_init(void){
  return sqlite3_vfs_register(&sqlite3OsKvvfsObject, 1);
}
int sqlite3_os_end(void){
  return SQLITE_OK;
}
#endif

#if SQLITE_OS_UNIX && defined(SQLITE_OS_KV_OPTIONAL)
int sqlite3KvvfsInit(void){
  return sqlite3_vfs_register(&sqlite3OsKvvfsObject, 0);
}
#endif
