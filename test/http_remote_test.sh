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

static int run_sql_quiet(sqlite3 *db, const char *sql) {
  char *err = 0;
  int rc = sqlite3_exec(db, sql, 0, 0, &err);
  if (err) sqlite3_free(err);
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
  int _e = (expected), _a = (actual); \
  if (_e == _a) { printf("  PASS: %s\n", desc); pass++; } \
  else { printf("  FAIL: %s (expected %d, got %d)\n", desc, _e, _a); fail++; } \
} while(0)

#define CHECK_STR(desc, expected, actual) do { \
  const char *_e = (expected), *_a = (actual); \
  if (strcmp(_e, _a)==0) { printf("  PASS: %s\n", desc); pass++; } \
  else { printf("  FAIL: %s\n    expected: |%s|\n    actual:   |%s|\n", desc, _e, _a); fail++; } \
} while(0)

int main(int argc, char **argv) {
  int pass = 0, fail = 0;
  char tmpdir[512], srvdir[512], sql[2048], path[512];
  const char *base = argc > 1 ? argv[1] : "/tmp/http_test";

  snprintf(tmpdir, sizeof(tmpdir), "%s", base);
  snprintf(srvdir, sizeof(srvdir), "%s/served", base);

  mkdir(tmpdir, 0755);
  mkdir(srvdir, 0755);

  /* ============================================================
   * 1. Setup source repo with multi-table data
   * ============================================================ */
  printf("=== 1. Setup source repo ===\n");
  sqlite3 *srcDb;
  snprintf(path, sizeof(path), "%s/src.db", srvdir);
  sqlite3_open(path, &srcDb);
  run_sql(srcDb, "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
  run_sql(srcDb, "INSERT INTO users VALUES(1,'alice',30),(2,'bob',25),(3,'charlie',35)");
  run_sql(srcDb, "CREATE TABLE scores(uid INTEGER, score REAL, FOREIGN KEY(uid) REFERENCES users(id))");
  run_sql(srcDb, "INSERT INTO scores VALUES(1,95.5),(2,87.3),(3,91.0)");
  run_sql(srcDb, "SELECT dolt_add('-A')");
  run_sql(srcDb, "SELECT dolt_commit('-m','initial: two tables with data')");
  CHECK("source has 3 users", 3, query_int(srcDb, "SELECT count(*) FROM users"));
  CHECK("source has 3 scores", 3, query_int(srcDb, "SELECT count(*) FROM scores"));
  sqlite3_close(srcDb);

  /* ============================================================
   * 2. Start HTTP server
   * ============================================================ */
  printf("=== 2. Start HTTP server ===\n");
  DoltliteServer *srv = doltliteServeAsync(srvdir, 0);
  if (!srv) {
    printf("  FAIL: could not start server\n");
    return 1;
  }
  int port = doltliteServerPort(srv);
  printf("  Server started on port %d\n", port);
  CHECK("server started", 1, port > 0);
  usleep(100000);

  /* ============================================================
   * 3. Clone via HTTP - verify all data, branch, remote, history
   * ============================================================ */
  printf("=== 3. Clone via HTTP ===\n");
  sqlite3 *cloneDb;
  snprintf(path, sizeof(path), "%s/clone.db", tmpdir);
  sqlite3_open(path, &cloneDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(cloneDb, sql);
  CHECK("clone has 3 users", 3, query_int(cloneDb, "SELECT count(*) FROM users"));
  CHECK("clone has 3 scores", 3, query_int(cloneDb, "SELECT count(*) FROM scores"));
  CHECK_STR("clone branch is main", "main", query_str(cloneDb, "SELECT active_branch()"));
  CHECK_STR("clone has origin remote", "origin", query_str(cloneDb, "SELECT name FROM dolt_remotes"));
  CHECK_STR("clone has commit history", "initial: two tables with data",
    query_str(cloneDb, "SELECT message FROM dolt_log LIMIT 1"));
  CHECK_STR("clone data correct", "bob",
    query_str(cloneDb, "SELECT name FROM users WHERE id=2"));

  /* ============================================================
   * 4. Push from clone - verify server updated
   * ============================================================ */
  printf("=== 4. Push from clone ===\n");
  run_sql(cloneDb, "INSERT INTO users VALUES(4,'diana',28)");
  run_sql(cloneDb, "INSERT INTO scores VALUES(4,99.1)");
  run_sql(cloneDb, "SELECT dolt_add('-A')");
  run_sql(cloneDb, "SELECT dolt_commit('-m','add diana')");
  run_sql(cloneDb, "SELECT dolt_push('origin','main')");

  /* Verify server side */
  sqlite3 *verifyDb;
  snprintf(path, sizeof(path), "%s/src.db", srvdir);
  sqlite3_open(path, &verifyDb);
  CHECK("server has 4 users after push", 4, query_int(verifyDb, "SELECT count(*) FROM users"));
  CHECK("server has 4 scores after push", 4, query_int(verifyDb, "SELECT count(*) FROM scores"));
  CHECK_STR("server has diana", "diana", query_str(verifyDb, "SELECT name FROM users WHERE id=4"));
  sqlite3_close(verifyDb);

  /* ============================================================
   * 5. Fetch in original - tracking branch updated, local unchanged
   * ============================================================ */
  printf("=== 5. Fetch in original ===\n");
  sqlite3 *origDb;
  snprintf(path, sizeof(path), "%s/orig.db", tmpdir);
  sqlite3_open(path, &origDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(origDb, sql);
  /* origDb now has 4 users (cloned after push). Add data on clone side. */
  CHECK("orig has 4 users", 4, query_int(origDb, "SELECT count(*) FROM users"));

  /* Push more from cloneDb so origDb can fetch */
  run_sql(cloneDb, "INSERT INTO users VALUES(5,'eve',22)");
  run_sql(cloneDb, "SELECT dolt_add('-A')");
  run_sql(cloneDb, "SELECT dolt_commit('-m','add eve')");
  run_sql(cloneDb, "SELECT dolt_push('origin','main')");

  /* Fetch */
  run_sql(origDb, "SELECT dolt_fetch('origin','main')");
  CHECK("orig data unchanged after fetch", 4, query_int(origDb, "SELECT count(*) FROM users"));

  /* ============================================================
   * 6. Pull (fast-forward) - verify data updated
   * ============================================================ */
  printf("=== 6. Pull (fast-forward) ===\n");
  run_sql(origDb, "SELECT dolt_pull('origin','main')");
  CHECK("orig has 5 users after pull", 5, query_int(origDb, "SELECT count(*) FROM users"));
  CHECK_STR("orig has eve after pull", "eve",
    query_str(origDb, "SELECT name FROM users WHERE id=5"));
  sqlite3_close(origDb);

  /* ============================================================
   * 7. Already up-to-date push/pull
   * ============================================================ */
  printf("=== 7. Already up-to-date ===\n");
  CHECK("push when up-to-date succeeds", SQLITE_OK,
    run_sql(cloneDb, "SELECT dolt_push('origin','main')"));
  CHECK("pull when up-to-date succeeds", SQLITE_OK,
    run_sql(cloneDb, "SELECT dolt_pull('origin','main')"));

  /* ============================================================
   * 8. Create and push new branch to HTTP remote
   * ============================================================ */
  printf("=== 8. Push new branch ===\n");
  run_sql(cloneDb, "SELECT dolt_branch('feature')");
  run_sql(cloneDb, "SELECT dolt_checkout('feature')");
  run_sql(cloneDb, "INSERT INTO users VALUES(6,'frank',40)");
  run_sql(cloneDb, "SELECT dolt_add('-A')");
  run_sql(cloneDb, "SELECT dolt_commit('-m','add frank on feature')");
  run_sql(cloneDb, "SELECT dolt_push('origin','feature')");

  /* Verify on server */
  sqlite3_open(path, &verifyDb); /* path still srvdir/src.db */
  snprintf(path, sizeof(path), "%s/src.db", srvdir);
  sqlite3_open(path, &verifyDb);
  run_sql(verifyDb, "SELECT dolt_checkout('feature')");
  CHECK("server feature has 6 users", 6, query_int(verifyDb, "SELECT count(*) FROM users"));
  sqlite3_close(verifyDb);
  run_sql(cloneDb, "SELECT dolt_checkout('main')");

  /* ============================================================
   * 9. Clone gets multiple branches
   * ============================================================ */
  printf("=== 9. Clone gets multiple branches ===\n");
  sqlite3 *multiDb;
  snprintf(path, sizeof(path), "%s/multi_clone.db", tmpdir);
  sqlite3_open(path, &multiDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(multiDb, sql);
  CHECK("multi-clone has 2 branches", 2,
    query_int(multiDb, "SELECT count(*) FROM dolt_branches"));
  /* Clone may land on main or feature depending on server HEAD; just verify both exist */
  run_sql(multiDb, "SELECT dolt_checkout('main')");
  CHECK("multi-clone main has 5 users", 5, query_int(multiDb, "SELECT count(*) FROM users"));
  run_sql(multiDb, "SELECT dolt_checkout('feature')");
  CHECK("multi-clone feature has 6 users", 6, query_int(multiDb, "SELECT count(*) FROM users"));
  sqlite3_close(multiDb);

  /* ============================================================
   * 10. Fetch specific branch
   * ============================================================ */
  printf("=== 10. Fetch specific branch ===\n");
  sqlite3 *fetchDb;
  snprintf(path, sizeof(path), "%s/fetch_branch.db", tmpdir);
  sqlite3_open(path, &fetchDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(fetchDb, sql);
  /* Push more on feature from cloneDb */
  run_sql(cloneDb, "SELECT dolt_checkout('feature')");
  run_sql(cloneDb, "INSERT INTO users VALUES(7,'grace',33)");
  run_sql(cloneDb, "SELECT dolt_add('-A')");
  run_sql(cloneDb, "SELECT dolt_commit('-m','add grace on feature')");
  run_sql(cloneDb, "SELECT dolt_push('origin','feature')");
  run_sql(cloneDb, "SELECT dolt_checkout('main')");

  run_sql(fetchDb, "SELECT dolt_fetch('origin','feature')");
  run_sql(fetchDb, "SELECT dolt_checkout('feature')");
  run_sql(fetchDb, "SELECT dolt_pull('origin','feature')");
  CHECK("fetched feature has 7 users", 7, query_int(fetchDb, "SELECT count(*) FROM users"));
  sqlite3_close(fetchDb);

  /* ============================================================
   * 11. Deep history: 15 commits on a branch, push, verify
   * ============================================================ */
  printf("=== 11. Deep history ===\n");
  sqlite3 *deepDb;
  snprintf(path, sizeof(path), "%s/deep.db", srvdir);
  sqlite3_open(path, &deepDb);
  run_sql(deepDb, "CREATE TABLE log(id INTEGER PRIMARY KEY, step INTEGER, msg TEXT)");
  run_sql(deepDb, "SELECT dolt_add('-A')");
  run_sql(deepDb, "SELECT dolt_commit('-m','create log table')");
  for (int i = 1; i <= 15; i++) {
    snprintf(sql, sizeof(sql),
      "INSERT INTO log VALUES(%d,%d,'step %d')", i, i, i);
    run_sql(deepDb, sql);
    run_sql(deepDb, "SELECT dolt_add('-A')");
    snprintf(sql, sizeof(sql), "SELECT dolt_commit('-m','step %d')", i);
    run_sql(deepDb, sql);
  }
  CHECK("deep source has 15 rows", 15, query_int(deepDb, "SELECT count(*) FROM log"));
  /* 16 commits = 1 create + 15 inserts */
  CHECK("deep source has 16 commits", 16, query_int(deepDb, "SELECT count(*) FROM dolt_log"));
  sqlite3_close(deepDb);

  /* ============================================================
   * 12. Clone deep history - verify commit count
   * ============================================================ */
  printf("=== 12. Clone deep history ===\n");
  sqlite3 *deepClone;
  snprintf(path, sizeof(path), "%s/deep_clone.db", tmpdir);
  sqlite3_open(path, &deepClone);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/deep.db')", port);
  run_sql(deepClone, sql);
  CHECK("deep clone has 15 rows", 15, query_int(deepClone, "SELECT count(*) FROM log"));
  CHECK("deep clone has 16 commits", 16, query_int(deepClone, "SELECT count(*) FROM dolt_log"));
  CHECK_STR("deep clone latest msg", "step 15",
    query_str(deepClone, "SELECT message FROM dolt_log LIMIT 1"));
  sqlite3_close(deepClone);

  /* ============================================================
   * 13. Multiple commits, push once - verify all transferred
   * ============================================================ */
  printf("=== 13. Multiple commits, push once ===\n");
  sqlite3 *batchDb;
  snprintf(path, sizeof(path), "%s/batch.db", srvdir);
  sqlite3_open(path, &batchDb);
  run_sql(batchDb, "CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT)");
  run_sql(batchDb, "INSERT INTO items VALUES(1,'first')");
  run_sql(batchDb, "SELECT dolt_add('-A')");
  run_sql(batchDb, "SELECT dolt_commit('-m','commit 1')");
  sqlite3_close(batchDb);

  sqlite3 *batchClone;
  snprintf(path, sizeof(path), "%s/batch_clone.db", tmpdir);
  sqlite3_open(path, &batchClone);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/batch.db')", port);
  run_sql(batchClone, sql);
  /* Make 3 commits locally then push once */
  run_sql(batchClone, "INSERT INTO items VALUES(2,'second')");
  run_sql(batchClone, "SELECT dolt_add('-A')");
  run_sql(batchClone, "SELECT dolt_commit('-m','commit 2')");
  run_sql(batchClone, "INSERT INTO items VALUES(3,'third')");
  run_sql(batchClone, "SELECT dolt_add('-A')");
  run_sql(batchClone, "SELECT dolt_commit('-m','commit 3')");
  run_sql(batchClone, "INSERT INTO items VALUES(4,'fourth')");
  run_sql(batchClone, "SELECT dolt_add('-A')");
  run_sql(batchClone, "SELECT dolt_commit('-m','commit 4')");
  run_sql(batchClone, "SELECT dolt_push('origin','main')");

  /* Verify server got everything */
  snprintf(path, sizeof(path), "%s/batch.db", srvdir);
  sqlite3_open(path, &verifyDb);
  CHECK("batch server has 4 items", 4, query_int(verifyDb, "SELECT count(*) FROM items"));
  CHECK_STR("batch server last item", "fourth",
    query_str(verifyDb, "SELECT val FROM items WHERE id=4"));
  sqlite3_close(verifyDb);
  sqlite3_close(batchClone);

  /* ============================================================
   * 14. Schema changes: ALTER TABLE ADD COLUMN, push
   * ============================================================ */
  printf("=== 14. Schema changes ===\n");
  run_sql(cloneDb, "ALTER TABLE users ADD COLUMN email TEXT");
  run_sql(cloneDb, "UPDATE users SET email='alice@test.com' WHERE id=1");
  run_sql(cloneDb, "SELECT dolt_add('-A')");
  run_sql(cloneDb, "SELECT dolt_commit('-m','add email column')");
  run_sql(cloneDb, "SELECT dolt_push('origin','main')");

  /* Verify on server -- must checkout main since server DB was on feature */
  snprintf(path, sizeof(path), "%s/src.db", srvdir);
  sqlite3_open(path, &verifyDb);
  run_sql(verifyDb, "SELECT dolt_checkout('main')");
  CHECK_STR("server has email column", "alice@test.com",
    query_str(verifyDb, "SELECT email FROM users WHERE id=1"));
  sqlite3_close(verifyDb);

  /* ============================================================
   * 15. Pull schema change into another client
   * ============================================================ */
  printf("=== 15. Pull schema change ===\n");
  sqlite3 *schemaDb;
  snprintf(path, sizeof(path), "%s/schema_pull.db", tmpdir);
  sqlite3_open(path, &schemaDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/src.db')", port);
  run_sql(schemaDb, sql);
  CHECK_STR("schema pull has email", "alice@test.com",
    query_str(schemaDb, "SELECT email FROM users WHERE id=1"));
  /* Verify column exists (null for non-alice rows) */
  CHECK("schema pull email col exists for bob", 1,
    query_int(schemaDb, "SELECT email IS NULL FROM users WHERE id=2"));
  sqlite3_close(schemaDb);

  /* ============================================================
   * 16. Large data: insert 200 rows, push
   * ============================================================ */
  printf("=== 16. Large data push ===\n");
  sqlite3 *largeDb;
  snprintf(path, sizeof(path), "%s/large.db", srvdir);
  sqlite3_open(path, &largeDb);
  run_sql(largeDb, "CREATE TABLE big(id INTEGER PRIMARY KEY, data TEXT)");
  run_sql(largeDb,
    "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<200) "
    "INSERT INTO big SELECT x, printf('row_%d_padding_%s', x, hex(randomblob(50))) FROM cnt");
  run_sql(largeDb, "SELECT dolt_add('-A')");
  run_sql(largeDb, "SELECT dolt_commit('-m','200 rows')");
  CHECK("large source has 200 rows", 200, query_int(largeDb, "SELECT count(*) FROM big"));
  sqlite3_close(largeDb);

  /* ============================================================
   * 17. Clone 200 rows - verify count
   * ============================================================ */
  printf("=== 17. Clone large data ===\n");
  sqlite3 *largeClone;
  snprintf(path, sizeof(path), "%s/large_clone.db", tmpdir);
  sqlite3_open(path, &largeClone);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/large.db')", port);
  run_sql(largeClone, sql);
  CHECK("large clone has 200 rows", 200, query_int(largeClone, "SELECT count(*) FROM big"));
  CHECK("large clone first row exists", 1,
    query_int(largeClone, "SELECT count(*) FROM big WHERE id=1"));
  CHECK("large clone last row exists", 1,
    query_int(largeClone, "SELECT count(*) FROM big WHERE id=200"));
  sqlite3_close(largeClone);

  /* ============================================================
   * 18-22. Round-trip: A→server→B→server→A (3 hops)
   * ============================================================ */
  printf("=== 18. Round-trip: A creates and pushes ===\n");
  sqlite3 *hopA;
  snprintf(path, sizeof(path), "%s/hop_src.db", srvdir);
  sqlite3_open(path, &hopA);
  run_sql(hopA, "CREATE TABLE chain(id INTEGER PRIMARY KEY, who TEXT)");
  run_sql(hopA, "INSERT INTO chain VALUES(1,'hop_a')");
  run_sql(hopA, "SELECT dolt_add('-A')");
  run_sql(hopA, "SELECT dolt_commit('-m','from A')");
  sqlite3_close(hopA);

  /* Re-open A as a client (not server-side) with remote */
  snprintf(path, sizeof(path), "%s/hop_a.db", tmpdir);
  sqlite3_open(path, &hopA);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/hop_src.db')", port);
  run_sql(hopA, sql);
  CHECK("hop A has 1 row", 1, query_int(hopA, "SELECT count(*) FROM chain"));

  printf("=== 19. Round-trip: B clones, adds data, pushes ===\n");
  sqlite3 *hopB;
  snprintf(path, sizeof(path), "%s/hop_b.db", tmpdir);
  sqlite3_open(path, &hopB);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/hop_src.db')", port);
  run_sql(hopB, sql);
  run_sql(hopB, "INSERT INTO chain VALUES(2,'hop_b')");
  run_sql(hopB, "SELECT dolt_add('-A')");
  run_sql(hopB, "SELECT dolt_commit('-m','from B')");
  run_sql(hopB, "SELECT dolt_push('origin','main')");
  CHECK("hop B has 2 rows", 2, query_int(hopB, "SELECT count(*) FROM chain"));

  printf("=== 20. Round-trip: A pulls - gets B's data ===\n");
  run_sql(hopA, "SELECT dolt_pull('origin','main')");
  CHECK("hop A has 2 rows after pull", 2, query_int(hopA, "SELECT count(*) FROM chain"));
  CHECK_STR("hop A sees B's data", "hop_b",
    query_str(hopA, "SELECT who FROM chain WHERE id=2"));

  printf("=== 21. Round-trip: A adds more, pushes ===\n");
  run_sql(hopA, "INSERT INTO chain VALUES(3,'hop_a_again')");
  run_sql(hopA, "SELECT dolt_add('-A')");
  run_sql(hopA, "SELECT dolt_commit('-m','from A again')");
  run_sql(hopA, "SELECT dolt_push('origin','main')");

  printf("=== 22. Round-trip: B pulls - gets A's latest ===\n");
  run_sql(hopB, "SELECT dolt_pull('origin','main')");
  CHECK("hop B has 3 rows", 3, query_int(hopB, "SELECT count(*) FROM chain"));
  CHECK_STR("hop B has full chain", "hop_a",
    query_str(hopB, "SELECT who FROM chain WHERE id=1"));
  CHECK_STR("hop B has A's latest", "hop_a_again",
    query_str(hopB, "SELECT who FROM chain WHERE id=3"));
  sqlite3_close(hopA);
  sqlite3_close(hopB);

  /* ============================================================
   * 23. Multi-database server: create second DB
   * ============================================================ */
  printf("=== 23. Multi-database server ===\n");
  sqlite3 *db2;
  snprintf(path, sizeof(path), "%s/second.db", srvdir);
  sqlite3_open(path, &db2);
  run_sql(db2, "CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price REAL)");
  run_sql(db2, "INSERT INTO products VALUES(1,'widget',9.99),(2,'gadget',19.99)");
  run_sql(db2, "SELECT dolt_add('-A')");
  run_sql(db2, "SELECT dolt_commit('-m','products initial')");
  sqlite3_close(db2);

  /* ============================================================
   * 24. Clone the second database - verify independent data
   * ============================================================ */
  printf("=== 24. Clone second database ===\n");
  sqlite3 *db2Clone;
  snprintf(path, sizeof(path), "%s/second_clone.db", tmpdir);
  sqlite3_open(path, &db2Clone);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/second.db')", port);
  run_sql(db2Clone, sql);
  CHECK("second db clone has 2 products", 2,
    query_int(db2Clone, "SELECT count(*) FROM products"));
  CHECK_STR("second db has widget", "widget",
    query_str(db2Clone, "SELECT name FROM products WHERE id=1"));
  /* Verify it does NOT have users table from src.db */
  CHECK("second db has no users table", -1,
    query_int(db2Clone, "SELECT count(*) FROM users"));
  sqlite3_close(db2Clone);

  /* ============================================================
   * 25. Error: clone nonexistent database name
   * ============================================================ */
  printf("=== 25. Error: clone nonexistent DB ===\n");
  sqlite3 *errDb;
  snprintf(path, sizeof(path), "%s/err.db", tmpdir);
  sqlite3_open(path, &errDb);
  snprintf(sql, sizeof(sql), "SELECT dolt_clone('http://127.0.0.1:%d/nonexistent.db')", port);
  int errRc = run_sql_quiet(errDb, sql);
  CHECK("clone nonexistent DB errors", 1, errRc != SQLITE_OK);
  sqlite3_close(errDb);

  /* ============================================================
   * Cleanup
   * ============================================================ */
  sqlite3_close(cloneDb);
  printf("=== Shutdown ===\n");
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
