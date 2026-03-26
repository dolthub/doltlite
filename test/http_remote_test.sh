#!/bin/bash
#
# HTTP remote integration tests for doltlite.
# Starts a remotesrv server, runs push/fetch/pull/clone against it.
#

DOLTLITE_ARG="${1:-$(dirname "$0")/../build/doltlite}"
DOLTLITE="$(cd "$(dirname "$DOLTLITE_ARG")" && pwd)/$(basename "$DOLTLITE_ARG")"
TMPDIR=$(mktemp -d)
trap 'kill $SERVER_PID 2>/dev/null; rm -rf $TMPDIR' EXIT

pass=0
fail=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$(echo "$expected" | head -5)|"
    echo "    actual:   |$(echo "$actual" | head -5)|"
    fail=$((fail+1))
  fi
}

DB="$DOLTLITE"

# ============================================================
# Basic URL scheme recognition tests
# ============================================================
echo "=== HTTP remote URL recognition tests ==="

result=$("$DB" "$TMPDIR/test.db" "SELECT dolt_remote('add','origin','http://localhost:19999/mydb');" 2>/dev/null)
check "http remote add succeeds" "0" "$result"

result=$("$DB" "$TMPDIR/test.db" "SELECT * FROM dolt_remotes;")
check "http remote in list" "origin|http://localhost:19999/mydb" "$result"

result=$("$DB" "$TMPDIR/test.db" "SELECT dolt_push('origin','main');" 2>&1)
check "push to dead server errors" "1" "$(echo "$result" | grep -c 'failed\|error\|connection')"

# ============================================================
echo ""
echo "=== Building embedded HTTP test binary ==="
# ============================================================

# Write a C test that starts server async, runs operations, verifies
cat > "$TMPDIR/http_test.c" << 'CEOF'
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "sqlite3.h"
#include "doltlite_remotesrv.h"

static int run_sql(sqlite3 *db, const char *sql) {
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if (rc != SQLITE_OK && err) {
    fprintf(stderr, "SQL error: %s\n  SQL: %s\n", err, sql);
    sqlite3_free(err);
  }
  return rc;
}

static char *query_str(sqlite3 *db, const char *sql) {
  sqlite3_stmt *stmt;
  static char buf[4096];
  buf[0] = 0;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      const char *t = (const char*)sqlite3_column_text(stmt, 0);
      if (t) strncpy(buf, t, sizeof(buf)-1);
    }
    sqlite3_finalize(stmt);
  }
  return buf;
}

static int query_int(sqlite3 *db, const char *sql) {
  sqlite3_stmt *stmt;
  int val = -1;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) val = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
  }
  return val;
}

#define CHECK(desc, expected, actual) do { \
  if ((expected) == (actual)) { printf("  PASS: %s\n", desc); pass++; } \
  else { printf("  FAIL: %s (expected %d, got %d)\n", desc, expected, actual); fail++; } \
} while(0)

#define CHECK_STR(desc, expected, actual) do { \
  if (strcmp(expected, actual)==0) { printf("  PASS: %s\n", desc); pass++; } \
  else { printf("  FAIL: %s\n    expected: |%s|\n    actual:   |%s|\n", desc, expected, actual); fail++; } \
} while(0)

int main(int argc, char **argv) {
  int pass = 0, fail = 0;
  char tmpdir[256], srvdir[256], sql[1024];
  const char *base = argc > 1 ? argv[1] : "/tmp/http_test";

  snprintf(tmpdir, sizeof(tmpdir), "%s", base);
  snprintf(srvdir, sizeof(srvdir), "%s/served", base);

  /* Create the server directory */
  mkdir(tmpdir, 0755);
  mkdir(srvdir, 0755);

  /* 1. Create source repo inside the served directory */
  printf("=== 1. Setup source repo ===\n");
  sqlite3 *srcDb;
  snprintf(sql, sizeof(sql), "%s/src.db", srvdir);
  sqlite3_open(sql, &srcDb);
  run_sql(srcDb, "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
  run_sql(srcDb, "INSERT INTO users VALUES(1,'alice',30),(2,'bob',25),(3,'charlie',35)");
  run_sql(srcDb, "SELECT dolt_add('-A')");
  run_sql(srcDb, "SELECT dolt_commit('-m','initial')");
  CHECK(
    "source has 3 users",
    3, query_int(srcDb, "SELECT count(*) FROM users")
  );
  sqlite3_close(srcDb);

  /* 2. Start HTTP server on the served directory */
  printf("=== 2. Start HTTP server ===\n");
  DoltliteServer *srv = doltliteServeAsync(srvdir, 0);
  if (!srv) {
    printf("  FAIL: could not start server\n");
    return 1;
  }
  int port = doltliteServerPort(srv);
  printf("  Server started on port %d\n", port);
  CHECK("server started", 1, port > 0);

  /* Small delay for server to be ready */
  usleep(100000);

  /* 3. Clone via HTTP -- URL is http://host:port/dbname */
  printf("=== 3. Clone via HTTP ===\n");
  sqlite3 *cloneDb;
  snprintf(sql, sizeof(sql), "%s/clone.db", tmpdir);
  sqlite3_open(sql, &cloneDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(cloneDb, sql);
  CHECK(
    "clone has 3 users",
    3, query_int(cloneDb, "SELECT count(*) FROM users")
  );
  CHECK_STR(
    "clone branch is main",
    "main", query_str(cloneDb, "SELECT active_branch()")
  );
  CHECK_STR(
    "clone has origin",
    "origin", query_str(cloneDb, "SELECT name FROM dolt_remotes")
  );

  /* 4. Push from clone back to server */
  printf("=== 4. Push from clone ===\n");
  run_sql(cloneDb, "INSERT INTO users VALUES(4,'diana',28)");
  run_sql(cloneDb, "SELECT dolt_add('-A')");
  run_sql(cloneDb, "SELECT dolt_commit('-m','add diana')");
  snprintf(sql, sizeof(sql), "SELECT dolt_push('origin','main')");
  run_sql(cloneDb, sql);
  sqlite3_close(cloneDb);

  /* Verify server has the new data */
  sqlite3 *verifyDb;
  snprintf(sql, sizeof(sql), "%s/src.db", srvdir);
  sqlite3_open(sql, &verifyDb);
  CHECK(
    "server has 4 users after push",
    4, query_int(verifyDb, "SELECT count(*) FROM users")
  );
  sqlite3_close(verifyDb);

  /* 5. Fetch from another client */
  printf("=== 5. Fetch via HTTP ===\n");
  sqlite3 *fetchDb;
  snprintf(sql, sizeof(sql), "%s/fetcher.db", tmpdir);
  sqlite3_open(sql, &fetchDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(fetchDb, sql);
  CHECK(
    "fetcher has 4 users",
    4, query_int(fetchDb, "SELECT count(*) FROM users")
  );
  sqlite3_close(fetchDb);

  /* 6. Stop server */
  printf("=== 6. Shutdown ===\n");
  doltliteServerStop(srv);
  CHECK("server stopped", 1, 1);

  printf("\n=======================================\n");
  printf("Results: %d passed, %d failed\n", pass, fail);
  printf("=======================================\n");
  return fail > 0 ? 1 : 0;
}
CEOF

# Build the test
cd "$TMPDIR"
BUILD_DIR="$(dirname "$DOLTLITE")"
cc -g -O0 -I"$BUILD_DIR" -I"$BUILD_DIR/../src" \
  -DDOLTLITE_PROLLY=1 -D_HAVE_SQLITE_CONFIG_H -DBUILD_sqlite \
  -o http_test http_test.c \
  "$BUILD_DIR/libdoltlite.a" -lz -lpthread -lm 2>&1
if [ $? -ne 0 ]; then
  echo "  FAIL: could not build http_test binary"
  fail=$((fail+1))
else
  echo "  Built http_test binary"
  mkdir -p "$TMPDIR/run"
  http_output=$(./http_test "$TMPDIR/run" 2>&1)
  test_exit=$?
  echo "$http_output"
  if [ $test_exit -eq 0 ]; then
    # Count passes/fails from the "  PASS:" and "  FAIL:" lines only
    embedded_pass=$(echo "$http_output" | grep -c "^  PASS:")
    embedded_fail=$(echo "$http_output" | grep -c "^  FAIL:")
    pass=$((pass + embedded_pass))
    fail=$((fail + embedded_fail))
  else
    echo "  FAIL: http_test exited with $test_exit"
    fail=$((fail+1))
  fi
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
