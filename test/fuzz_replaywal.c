#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "sqliteInt.h"
#include "chunk_store.h"

static int g_initialized = 0;

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size > 1024 * 1024) return 0;

  if (!g_initialized) {
    sqlite3_initialize();
    g_initialized = 1;
  }

  char path[] = "/tmp/dl-fuzz-XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0) return 0;

  unsigned char header = 0;
  if (write(fd, &header, 1) != 1) {
    close(fd);
    unlink(path);
    return 0;
  }
  if (size > 0) {
    ssize_t w = write(fd, data, size);
    if (w != (ssize_t)size) {
      close(fd);
      unlink(path);
      return 0;
    }
  }
  close(fd);

  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  sqlite3_file *pFile = 0;
  int outFlags = 0;
  int rc = sqlite3OsOpenMalloc(pVfs, path, &pFile,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB, &outFlags);

  if (rc == SQLITE_OK && pFile) {
    ChunkStore cs;
    memset(&cs, 0, sizeof(cs));
    cs.file.pFile = pFile;
    cs.file.zFilename = sqlite3_mprintf("%s", path);
    cs.file.pVfs = pVfs;
    cs.pGraphLockFile = 0;
    cs.wal.iWalOffset = 1;

    (void)csReplayWal(&cs);

    chunkStoreClose(&cs);
  } else if (pFile) {
    sqlite3OsCloseFree(pFile);
  }

  unlink(path);
  return 0;
}
