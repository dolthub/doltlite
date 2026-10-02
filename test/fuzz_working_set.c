#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "chunk_store.h"

static int g_initialized = 0;

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ProllyHash cat, commit, staged, merge, conflicts, pre, onto, cv;
  char *zOrig = 0;
  char *zRet = 0;
  u8 merging = 0;
  u8 rebasing = 0;
  int rc;

  if (size > 1024 * 1024 || size > (size_t)0x7fffffff) return 0;
  if (!g_initialized) {
    sqlite3_initialize();
    g_initialized = 1;
  }

  rc = btreeParseWorkingSetBlob(
      data, (int)size, &cat, &commit, &staged, &merging, &merge, &conflicts,
      &rebasing, &pre, &onto, &zOrig, &zRet, &cv);
  if (rc == SQLITE_OK) {
    if (zOrig) (void)strlen(zOrig);
    if (zRet) (void)strlen(zRet);
    (void)merging;
    (void)rebasing;
  }
  sqlite3_free(zOrig);
  sqlite3_free(zRet);
  return 0;
}
