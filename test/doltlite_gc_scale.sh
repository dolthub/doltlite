#!/bin/bash
DLTEST_TIMEOUT=60
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== GC Tests at Scale ==="
echo ""

DB1=/tmp/test_gc1_$$.db; rm -f "$DB1"
dltest_require_slow "gc_10k_setup" "$DB1" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-Am','v1');
UPDATE t SET v = hex(randomblob(50)) WHERE id <= 1000;
SELECT dolt_commit('-am','v2');
SELECT dolt_gc();
SQL
)"

run_test "gc_10k_log" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB1"

run_test "gc_10k_status" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB1"

run_test "gc_10k_count" \
  "SELECT count(*) FROM t;" \
  "10000" "$DB1"

run_test "gc_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB1"

run_test "gc_10k_data_intact" \
  "SELECT count(*) FROM t WHERE length(v) > 0;" \
  "10000" "$DB1"

DB2=/tmp/test_gc2_$$.db; rm -f "$DB2"
dltest_require_slow "gc_branch_setup" "$DB2" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-Am','v1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(10001) UNION ALL SELECT x+1 FROM c WHERE x<15000) SELECT x FROM c);
SELECT dolt_commit('-am','feature work');
SQL
)"

run_test "gc_branch_feature_log" \
  "SELECT dolt_checkout('feature'); SELECT count(*) FROM dolt_log;" \
  "0
3" "$DB2"

dltest_require_slow "gc_branch_delete" "$DB2" "$(cat <<'SQL'
SELECT dolt_branch('-D','feature');
SELECT dolt_gc();
SQL
)"

run_test "gc_branch_log" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB2"

run_test "gc_branch_status" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB2"

run_test "gc_branch_count" \
  "SELECT count(*) FROM t;" \
  "10000" "$DB2"

run_test "gc_branch_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB2"

DB3=/tmp/test_gc3_$$.db; rm -f "$DB3"
dltest_require_slow "gc_index_setup" "$DB3" "$(cat <<'SQL'
CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tid TEXT NOT NULL, seq INTEGER NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(tid, seq)
);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO events(tid, seq, payload)
  SELECT 'thread-' || (x/200), x%200, hex(randomblob(100)) FROM c;
SELECT dolt_commit('-Am','v1');
UPDATE events SET payload = hex(randomblob(100)) WHERE id <= 2000;
SELECT dolt_commit('-am','v2');
SELECT dolt_gc();
SQL
)"

run_test "gc_index_log" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB3"

run_test "gc_index_status" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB3"

run_test "gc_index_count" \
  "SELECT count(*) FROM events;" \
  "10000" "$DB3"

run_test "gc_index_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB3"

run_test "gc_index_seek" \
  "SELECT count(*) FROM events WHERE tid='thread-25';" \
  "200" "$DB3"

DB4=/tmp/test_gc4_$$.db; rm -f "$DB4"
dltest_require_slow "gc_multi_setup" "$DB4" "$(cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-Am','v1');
UPDATE t SET v = hex(randomblob(50)) WHERE id <= 500;
SELECT dolt_commit('-am','v2');
SELECT dolt_gc();
UPDATE t SET v = hex(randomblob(50)) WHERE id BETWEEN 501 AND 1000;
SELECT dolt_commit('-am','v3');
SELECT dolt_gc();
SQL
)"

run_test "gc_multi_log" \
  "SELECT count(*) FROM dolt_log;" \
  "4" "$DB4"

run_test "gc_multi_status" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB4"

run_test "gc_multi_count" \
  "SELECT count(*) FROM t;" \
  "10000" "$DB4"

run_test "gc_multi_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB4"

SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
if [ -x "$SQLITE3" ]; then
  # shellcheck source=lib/require_stock_sqlite3.sh
  source "$(dirname "$0")/lib/require_stock_sqlite3.sh"
  if ! require_stock_sqlite3 "$SQLITE3"; then
    exit 1
  fi
  DB5=/tmp/test_gc5_$$.db; rm -f "$DB5"
  SQLDB=/tmp/test_gc5_att_$$.db; rm -f "$SQLDB"
  $SQLITE3 "$SQLDB" "CREATE TABLE ext(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO ext VALUES(1,'hello');"

  dltest_require_slow "gc_attach_setup" "$DB5" "$(cat <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-Am','v1');
UPDATE t SET v = hex(randomblob(50)) WHERE id <= 1000;
SELECT dolt_commit('-am','v2');
ATTACH DATABASE '$SQLDB' AS ext;
SELECT dolt_gc();
SQL
)"

  run_test "gc_attach_log" \
    "SELECT count(*) FROM dolt_log;" \
    "3" "$DB5"

  run_test "gc_attach_status" \
    "SELECT count(*) FROM dolt_status;" \
    "0" "$DB5"

  run_test "gc_attach_count" \
    "SELECT count(*) FROM t;" \
    "10000" "$DB5"

  run_test "gc_attach_integrity" \
    "PRAGMA integrity_check;" \
    "ok" "$DB5"

  rm -f "$DB5" "$SQLDB"
fi

rm -f "$DB1" "$DB2" "$DB3" "$DB4"

dltest_finish
