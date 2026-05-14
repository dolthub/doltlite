#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include "sqlite3.h"

extern int doltliteDeserializeCatalogForTest(sqlite3 *db, const unsigned char *data, int nData);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  sqlite3 *db = 0;
  int rc;

  if (size > 1024 * 1024) return 0;
  if (size > (size_t)0x7fffffff) return 0;

  rc = sqlite3_open(":memory:", &db);
  if (rc != SQLITE_OK || !db) {
    if (db) sqlite3_close(db);
    return 0;
  }

  (void)doltliteDeserializeCatalogForTest(db, data, (int)size);

  sqlite3_close(db);
  return 0;
}
