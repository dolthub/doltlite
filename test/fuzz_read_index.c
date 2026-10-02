#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "sqliteInt.h"
#include "chunk_store.h"

static int g_initialized = 0;

/* A store image carries its index location in the manifest. A bare blob is
** the index itself, and nChunks has to cover every entry or the reader
** rejects the table before decoding it. */
static void indexFieldsFromInput(const uint8_t *data, size_t size,
                                 ChunkStore *cs) {
  cs->index.iIndexOffset = 0;
  cs->index.nIndexSize = (i64)size;
  cs->index.nChunks = 1;
  if (size >= CHUNK_MANIFEST_SIZE
   && CS_READ_U32(data + CS_MANIFEST_MAGIC_OFF) == CHUNK_STORE_MAGIC
   && CS_READ_U32(data + CS_MANIFEST_VERSION_OFF) == CHUNK_STORE_VERSION) {
    u32 nChunks = CS_READ_U32(data + CS_MANIFEST_CHUNK_COUNT_OFF);
    u32 nIndex = CS_READ_U32(data + CS_MANIFEST_INDEX_SIZE_OFF);
    i64 iIndex = CS_READ_I64(data + CS_MANIFEST_INDEX_OFFSET_OFF);
    if (nChunks <= (u32)0x7fffffff) cs->index.nChunks = (int)nChunks;
    cs->index.nIndexSize = (i64)nIndex;
    cs->index.iIndexOffset = iIndex;
    return;
  }
  if (size >= CHUNK_INDEX_ENTRY_SIZE) {
    size_t nEnt = size / CHUNK_INDEX_ENTRY_SIZE;
    if (nEnt > 1 && nEnt <= (size_t)0x7fffffff) cs->index.nChunks = (int)nEnt;
  }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ChunkStore cs;
  sqlite3_vfs *pVfs;
  sqlite3_file *pFile = 0;
  int outFlags = 0;
  int rc;
  char path[] = "/tmp/dl-fuzz-idx-XXXXXX";
  int fd;

  if (size > 1024 * 1024) return 0;
  if (size > (size_t)0x7fffffff) return 0;

  if (!g_initialized) {
    sqlite3_initialize();
    g_initialized = 1;
  }

  fd = mkstemp(path);
  if (fd < 0) return 0;
  if (size > 0 && write(fd, data, size) != (ssize_t)size) {
    close(fd);
    unlink(path);
    return 0;
  }
  close(fd);

  pVfs = sqlite3_vfs_find(0);
  rc = sqlite3OsOpenMalloc(pVfs, path, &pFile,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB, &outFlags);

  if (rc == SQLITE_OK && pFile) {
    memset(&cs, 0, sizeof(cs));
    cs.file.pFile = pFile;
    cs.file.zFilename = sqlite3_mprintf("%s", path);
    cs.file.pVfs = pVfs;
    cs.pGraphLockFile = 0;
    indexFieldsFromInput(data, size, &cs);

    (void)csReadIndex(&cs);

    chunkStoreClose(&cs);
  } else if (pFile) {
    sqlite3OsCloseFree(pFile);
  }

  unlink(path);
  return 0;
}
