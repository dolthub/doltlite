#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "sqliteInt.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "lib/test_tmpdir.h"

static int g_initialized = 0;

size_t LLVMFuzzerMutate(uint8_t *Data, size_t Size, size_t MaxSize);

/* Replay rejects a chunk whose stored hash is not the blake3 of its body.
** Repair those hashes after mutation so the body is actually applied. */
static void rehashWalChunks(uint8_t *data, size_t size) {
  size_t pos = 0;
  while (pos + CS_WAL_CHUNK_HDR_SIZE <= size) {
    uint32_t len;
    ProllyHash hash;
    if (data[pos] == CS_WAL_TAG_ROOT) {
      if (pos + 1 + CHUNK_MANIFEST_SIZE > size) break;
      pos += 1 + CHUNK_MANIFEST_SIZE;
      continue;
    }
    if (data[pos] != CS_WAL_TAG_CHUNK) break;
    len = CS_READ_U32(data + pos + CS_WAL_CHUNK_LEN_OFF);
    if ((size_t)len > size - pos - CS_WAL_CHUNK_HDR_SIZE) break;
    if (len > 0x7fffffffu) break;
    prollyHashCompute(data + pos + CS_WAL_CHUNK_HDR_SIZE, (int)len, &hash);
    memcpy(data + pos + CS_WAL_CHUNK_HASH_OFF, hash.data, PROLLY_HASH_SIZE);
    pos += CS_WAL_CHUNK_HDR_SIZE + (size_t)len;
  }
}

size_t LLVMFuzzerCustomMutator(uint8_t *Data, size_t Size, size_t MaxSize,
                               unsigned int Seed) {
  (void)Seed;
  Size = LLVMFuzzerMutate(Data, Size, MaxSize);
  rehashWalChunks(Data, Size);
  return Size;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char path[] = DOLTLITE_TEST_TMPDIR "/dl-fuzz-XXXXXX";
  unsigned char header = 0;
  sqlite3_vfs *pVfs;
  sqlite3_file *pFile = 0;
  int outFlags = 0;
  int fd;
  int rc;
  ssize_t w;

  if (size > 1024 * 1024) return 0;

  if (!g_initialized) {
    sqlite3_initialize();
    g_initialized = 1;
  }

  fd = mkstemp(path);
  if (fd < 0) return 0;

  if (write(fd, &header, 1) != 1) {
    close(fd);
    unlink(path);
    return 0;
  }
  if (size > 0) {
    w = write(fd, data, size);
    if (w != (ssize_t)size) {
      close(fd);
      unlink(path);
      return 0;
    }
  }
  close(fd);

  pVfs = sqlite3_vfs_find(0);
  rc = sqlite3OsOpenMalloc(pVfs, path, &pFile,
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
