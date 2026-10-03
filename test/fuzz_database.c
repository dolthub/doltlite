#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "sqlite3.h"
#include "lib/test_tmpdir.h"

static int g_initialized = 0;

static const char *const kProbeSql[] = {
  "PRAGMA integrity_check;",
  "SELECT count(*) FROM sqlite_master;",
  "SELECT count(*) FROM dolt_branches;",
  "SELECT count(*) FROM dolt_log;",
  "SELECT count(*) FROM dolt_status;",
};

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  char path[] = DOLTLITE_TEST_TMPDIR "/dl-fuzz-db-XXXXXX";
  sqlite3 *db = 0;
  int fd;
  int i;
  int rc;

  if (size > 4 * 1024 * 1024) return 0;
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

  rc = sqlite3_open(path, &db);
  if (rc == SQLITE_OK && db) {
    for (i = 0; i < (int)(sizeof(kProbeSql) / sizeof(kProbeSql[0])); i++) {
      (void)sqlite3_exec(db, kProbeSql[i], 0, 0, 0);
    }
  }
  if (db) sqlite3_close(db);
  unlink(path);
  return 0;
}
