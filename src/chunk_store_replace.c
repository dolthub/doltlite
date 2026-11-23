#ifdef DOLTLITE_PROLLY

#include "chunk_store_int.h"

#if defined(SQLITE_WASM) || defined(__EMSCRIPTEN__)
#include "../ext/blake3/blake3.h"

#define CS_REPLACE_MAGIC 0x31524344
#define CS_REPLACE_SIZE_OFF 4
#define CS_REPLACE_PATH_OFF 12
#define CS_REPLACE_IMAGE_OFF (CS_REPLACE_PATH_OFF+PROLLY_HASH_SIZE)
#define CS_REPLACE_SEAL_OFF (CS_REPLACE_IMAGE_OFF+PROLLY_HASH_SIZE)
#define CS_REPLACE_RECORD_SIZE (CS_REPLACE_SEAL_OFF+PROLLY_HASH_SIZE)

static int csWasmOpenSidecar(
  ChunkStore *cs, const char *zSuffix, int flags,
  sqlite3_file **ppFile, char **pzName
){
  char *zRaw = sqlite3_mprintf("%s%s", cs->file.zFilename, zSuffix);
  int rc;
  int outFlags = 0;
  *ppFile = 0;
  *pzName = 0;
  if( !zRaw ) return SQLITE_NOMEM;
  rc = chunkStoreDupFilenameDoubleNul(zRaw, pzName);
  sqlite3_free(zRaw);
  if( rc==SQLITE_OK && (flags & SQLITE_OPEN_CREATE)==0 ){
    int exists = 0;
    rc = sqlite3OsAccess(cs->file.pVfs, *pzName, SQLITE_ACCESS_EXISTS, &exists);
    if( rc!=SQLITE_OK || !exists ) return rc;
  }
  if( rc==SQLITE_OK ){
    rc = csOpenFile(cs->file.pVfs, *pzName, ppFile,
                     flags | SQLITE_OPEN_MAIN_DB, &outFlags);
    if( rc==SQLITE_OK && (flags & SQLITE_OPEN_READWRITE)!=0
     && (outFlags & SQLITE_OPEN_READONLY)!=0 ){
      rc = SQLITE_READONLY;
    }
  }
  return rc;
}

static int csWasmRecordRead(
  ChunkStore *cs, sqlite3_file *pJournal, u8 *aRecord, i64 *pSize
){
  ProllyHash hash;
  i64 size;
  int rc;
  *pSize = 0;
  if( !pJournal ) return SQLITE_OK;
  rc = sqlite3OsFileSize(pJournal, &size);
  if( rc!=SQLITE_OK || size<CS_REPLACE_RECORD_SIZE ) return rc;
  if( size!=CS_REPLACE_RECORD_SIZE ) return SQLITE_CORRUPT;
  rc = sqlite3OsRead(pJournal, aRecord, CS_REPLACE_RECORD_SIZE, 0);
  if( rc!=SQLITE_OK ) return rc;
  prollyHashCompute(aRecord, CS_REPLACE_SEAL_OFF, &hash);
  /* A torn first write has no seal, so it never authorized a copy. */
  if( memcmp(hash.data, aRecord+CS_REPLACE_SEAL_OFF, PROLLY_HASH_SIZE)!=0 ){
    return SQLITE_OK;
  }
  prollyHashCompute((const u8*)cs->file.zFilename,
                    sqlite3Strlen30(cs->file.zFilename), &hash);
  size = CS_READ_I64(aRecord+CS_REPLACE_SIZE_OFF);
  if( CS_READ_U32(aRecord)!=CS_REPLACE_MAGIC || size<CHUNK_MANIFEST_SIZE
   || memcmp(hash.data, aRecord+CS_REPLACE_PATH_OFF, PROLLY_HASH_SIZE)!=0 ){
    return SQLITE_CORRUPT;
  }
  *pSize = size;
  return SQLITE_OK;
}

int csWasmReplacementPending(ChunkStore *cs, int *pPending){
  sqlite3_file *pJournal = 0;
  char *zJournal = 0;
  u8 aRecord[CS_REPLACE_RECORD_SIZE];
  i64 size = 0;
  int rc = SQLITE_OK;
  *pPending = 0;
  if( cs->file.pFile && !cs->isBuffer && !cs->isSharedMemory ){
    rc = csWasmOpenSidecar(cs, "-gc-replace", SQLITE_OPEN_READONLY,
                           &pJournal, &zJournal);
    if( rc==SQLITE_OK ) rc = csWasmRecordRead(cs, pJournal, aRecord, &size);
  }
  if( pJournal ) sqlite3OsCloseFree(pJournal);
  sqlite3_free(zJournal);
  *pPending = size>0;
  return rc;
}

static int csWasmImageHash(
  sqlite3_file *pIn, i64 size, u8 *aBuf, ProllyHash *pHash
){
  blake3_hasher hasher;
  i64 offset;
  blake3_hasher_init(&hasher);
  for(offset=0; offset<size; ){
    int n = (int)MIN(size-offset, (i64)CS_SCAN_WINDOW);
    int rc = sqlite3OsRead(pIn, aBuf, n, offset);
    if( rc!=SQLITE_OK ) return rc;
    blake3_hasher_update(&hasher, aBuf, n);
    offset += n;
  }
  blake3_hasher_finalize(&hasher, pHash->data, PROLLY_HASH_SIZE);
  return SQLITE_OK;
}

static int csWasmCopyReplacement(
  ChunkStore *cs, sqlite3_file *pIn, sqlite3_file *pJournal,
  i64 size, u8 *aBuf
){
  i64 offset = 0;
  int rc = SQLITE_OK;
  while( rc==SQLITE_OK && offset<size ){
    int n = (int)MIN(size-offset, (i64)CS_SCAN_WINDOW);
    rc = sqlite3OsRead(pIn, aBuf, n, offset);
    if( rc==SQLITE_OK ) rc = sqlite3OsWrite(cs->file.pFile, aBuf, n, offset);
    offset += n;
  }
  if( rc==SQLITE_OK ) rc = sqlite3OsTruncate(cs->file.pFile, size);
  if( rc==SQLITE_OK ) rc = sqlite3OsSync(cs->file.pFile, SQLITE_SYNC_FULL);
  /* Disarm durably before deleting the only recovery image. */
  if( rc==SQLITE_OK ) rc = sqlite3OsTruncate(pJournal, 0);
  if( rc==SQLITE_OK ) rc = sqlite3OsSync(pJournal, SQLITE_SYNC_FULL);
  if( rc!=SQLITE_OK ) cs->corruptMidStream = 1;
  return rc;
}

static int csWasmReplacementLock(ChunkStore *cs){
  int rc = sqlite3OsLock(cs->file.pFile, SQLITE_LOCK_SHARED);
  if( rc==SQLITE_OK ) rc = sqlite3OsLock(cs->file.pFile, SQLITE_LOCK_RESERVED);
  if( rc==SQLITE_OK ) rc = sqlite3OsLock(cs->file.pFile, SQLITE_LOCK_EXCLUSIVE);
  if( rc!=SQLITE_OK ) sqlite3OsUnlock(cs->file.pFile, SQLITE_LOCK_NONE);
  return rc;
}

int csWasmReplaceLockedFile(
  ChunkStore *cs, const char *zTmp, int *pRetainTmp
){
  sqlite3_file *pIn = 0;
  sqlite3_file *pJournal = 0;
  char *zJournal = 0;
  u8 record[CS_REPLACE_RECORD_SIZE];
  u8 *aBuf = 0;
  ProllyHash hash;
  i64 size = 0;
  int rc;

  PROLLY_ASSERT_STORE_GRAPH_LOCKED(cs);
  *pRetainTmp = 0;
  rc = csWasmReplacementLock(cs);
  if( rc!=SQLITE_OK ) return rc;
  rc = csOpenFile(cs->file.pVfs, zTmp, &pIn,
                   SQLITE_OPEN_READONLY | SQLITE_OPEN_MAIN_DB, 0);
  if( rc==SQLITE_OK ) rc = sqlite3OsFileSize(pIn, &size);
  if( rc==SQLITE_OK && size<CHUNK_MANIFEST_SIZE ) rc = SQLITE_CORRUPT;
  if( rc==SQLITE_OK ){
    aBuf = sqlite3_malloc(CS_SCAN_WINDOW);
    if( !aBuf ) rc = SQLITE_NOMEM;
  }
  if( rc==SQLITE_OK ) rc = csWasmImageHash(pIn, size, aBuf, &hash);
  if( rc==SQLITE_OK ){
    CS_WRITE_U32(record, CS_REPLACE_MAGIC);
    CS_WRITE_I64(record+CS_REPLACE_SIZE_OFF, size);
    memcpy(record+CS_REPLACE_IMAGE_OFF, hash.data, PROLLY_HASH_SIZE);
    prollyHashCompute((const u8*)cs->file.zFilename,
                      sqlite3Strlen30(cs->file.zFilename), &hash);
    memcpy(record+CS_REPLACE_PATH_OFF, hash.data, PROLLY_HASH_SIZE);
    prollyHashCompute(record, CS_REPLACE_SEAL_OFF, &hash);
    memcpy(record+CS_REPLACE_SEAL_OFF, hash.data, PROLLY_HASH_SIZE);
    rc = csWasmOpenSidecar(cs, "-gc-replace",
                          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                          &pJournal, &zJournal);
  }
  if( rc==SQLITE_OK ) rc = sqlite3OsTruncate(pJournal, 0);
  if( rc==SQLITE_OK ){
    *pRetainTmp = 1;
    rc = sqlite3OsWrite(pJournal, record, sizeof(record), 0);
  }
  if( rc==SQLITE_OK ) rc = sqlite3OsSync(pJournal, SQLITE_SYNC_FULL);
  if( rc==SQLITE_OK ) rc = csWasmCopyReplacement(cs, pIn, pJournal, size, aBuf);
  if( pIn ) sqlite3OsCloseFree(pIn);
  if( pJournal ) sqlite3OsCloseFree(pJournal);
  if( rc==SQLITE_OK ){
    *pRetainTmp = 0;
    (void)sqlite3OsDelete(cs->file.pVfs, zTmp, 0);
    (void)sqlite3OsDelete(cs->file.pVfs, zJournal, 0);
  }
  sqlite3_free(zJournal);
  sqlite3_free(aBuf);
  sqlite3OsUnlock(cs->file.pFile, SQLITE_LOCK_NONE);
  return rc;
}

int csWasmRecoverReplacement(ChunkStore *cs){
  CsFileLock lock = CS_FILE_LOCK_INIT;
  sqlite3_file *pIn = 0;
  sqlite3_file *pJournal = 0;
  char *zLock = 0;
  char *zJournal = 0;
  char *zTmp = 0;
  u8 record[CS_REPLACE_RECORD_SIZE];
  u8 *aBuf = 0;
  ProllyHash hash;
  i64 size = 0, actual = 0;
  int rc, locked = 0;

  rc = csFileLock(cs->file.pVfs, cs->file.zFilename, SQLITE_OPEN_READONLY,
                  &lock, &zLock);
  if( rc!=SQLITE_OK ) return rc;
  rc = csWasmReplacementLock(cs);
  locked = rc==SQLITE_OK;
  if( rc==SQLITE_OK ){
    rc = csWasmOpenSidecar(cs, "-gc-replace", SQLITE_OPEN_READWRITE,
                           &pJournal, &zJournal);
  }
  if( rc==SQLITE_OK ) rc = csWasmRecordRead(cs, pJournal, record, &size);
  if( rc==SQLITE_OK && size>0 ){
    rc = csWasmOpenSidecar(cs, "-gc-tmp", SQLITE_OPEN_READONLY, &pIn, &zTmp);
    if( rc==SQLITE_OK && !pIn ) rc = SQLITE_CORRUPT;
    if( rc==SQLITE_OK ) rc = sqlite3OsFileSize(pIn, &actual);
    if( rc==SQLITE_OK && actual!=size ) rc = SQLITE_CORRUPT;
    if( rc==SQLITE_OK ){
      aBuf = sqlite3_malloc(CS_SCAN_WINDOW);
      if( !aBuf ) rc = SQLITE_NOMEM;
    }
    if( rc==SQLITE_OK ) rc = csWasmImageHash(pIn, size, aBuf, &hash);
    if( rc==SQLITE_OK
     && memcmp(hash.data, record+CS_REPLACE_IMAGE_OFF, PROLLY_HASH_SIZE)!=0 ){
      rc = SQLITE_CORRUPT;
    }
    /* The first publisher may have written the seal but failed to sync it. */
    if( rc==SQLITE_OK ) rc = sqlite3OsSync(pJournal, SQLITE_SYNC_FULL);
    if( rc==SQLITE_OK ) rc = csWasmCopyReplacement(cs, pIn, pJournal, size, aBuf);
  }
  if( pIn ) sqlite3OsCloseFree(pIn);
  if( pJournal ) sqlite3OsCloseFree(pJournal);
  if( rc==SQLITE_OK && size>0 ){
    (void)sqlite3OsDelete(cs->file.pVfs, zTmp, 0);
    (void)sqlite3OsDelete(cs->file.pVfs, zJournal, 0);
  }
  if( locked ) sqlite3OsUnlock(cs->file.pFile, SQLITE_LOCK_NONE);
  csFileUnlock(lock, &zLock);
  sqlite3_free(zJournal);
  sqlite3_free(zTmp);
  sqlite3_free(aBuf);
  return rc;
}
#endif

#endif
