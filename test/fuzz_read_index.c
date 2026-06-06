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
    cs.index.iIndexOffset = 0;
    cs.index.nIndexSize = (i64)size;
    cs.index.nChunks = 1;

    (void)csReadIndex(&cs);

    chunkStoreClose(&cs);
  } else if (pFile) {
    sqlite3OsCloseFree(pFile);
  }

  unlink(path);
  return 0;
}
