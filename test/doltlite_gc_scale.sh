#!/bin/bash
#
# GC correctness tests at scale (10K+ rows).
# Verifies dolt_gc() works on databases with realistic payload sizes
# that produce multi-level prolly trees.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(60);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi
}

echo "=== GC Tests at Scale ==="
echo ""

# ============================================================
# Test 1: GC after updates — 10K rows with randomblob payloads
# ============================================================

DB1=/tmp/test_gc1_$$.db; rm -f "$DB1"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-am','v1');
UPDATE t SET v = hex(randomblob(50)) WHERE id <= 1000;
SELECT dolt_commit('-am','v2');
SELECT dolt_gc();" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "gc_10k_count" \
  "SELECT count(*) FROM t;" \
  "10000" "$DB1"

run_test "gc_10k_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB1"

run_test "gc_10k_data_intact" \
  "SELECT count(*) FROM t WHERE length(v) > 0;" \
  "10000" "$DB1"

# ============================================================
# Test 2: GC after branch delete — 10K rows
# ============================================================

DB2=/tmp/test_gc2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-am','v1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(10001) UNION ALL SELECT x+1 FROM c WHERE x<15000) SELECT x FROM c);
SELECT dolt_commit('-am','feature work');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d','feature');
SELECT dolt_gc();" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "gc_branch_count" \
  "SELECT count(*) FROM t;" \
  "10000" "$DB2"

run_test "gc_branch_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB2"

# ============================================================
# Test 3: GC with composite index — 10K rows
# ============================================================

DB3=/tmp/test_gc3_$$.db; rm -f "$DB3"
echo "CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tid TEXT NOT NULL, seq INTEGER NOT NULL,
  payload TEXT NOT NULL,
  UNIQUE(tid, seq)
);
WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<9999)
INSERT INTO events(tid, seq, payload)
  SELECT 'thread-' || (x/200), x%200, hex(randomblob(100)) FROM c;
SELECT dolt_commit('-am','v1');
UPDATE events SET payload = hex(randomblob(100)) WHERE id <= 2000;
SELECT dolt_commit('-am','v2');
SELECT dolt_gc();" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "gc_index_count" \
  "SELECT count(*) FROM events;" \
  "10000" "$DB3"

run_test "gc_index_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB3"

run_test "gc_index_seek" \
  "SELECT count(*) FROM events WHERE tid='thread-25';" \
  "200" "$DB3"

# ============================================================
# Test 4: Multiple GC cycles
# ============================================================

DB4=/tmp/test_gc4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-am','v1');
UPDATE t SET v = hex(randomblob(50)) WHERE id <= 500;
SELECT dolt_commit('-am','v2');
SELECT dolt_gc();
UPDATE t SET v = hex(randomblob(50)) WHERE id BETWEEN 501 AND 1000;
SELECT dolt_commit('-am','v3');
SELECT dolt_gc();" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "gc_multi_count" \
  "SELECT count(*) FROM t;" \
  "10000" "$DB4"

run_test "gc_multi_integrity" \
  "PRAGMA integrity_check;" \
  "ok" "$DB4"

# ============================================================
# Test 5: GC with attached SQLite db
# ============================================================

SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
if [ -x "$SQLITE3" ]; then
  DB5=/tmp/test_gc5_$$.db; rm -f "$DB5"
  SQLDB=/tmp/test_gc5_att_$$.db; rm -f "$SQLDB"
  $SQLITE3 "$SQLDB" "CREATE TABLE ext(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO ext VALUES(1,'hello');"

  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, hex(randomblob(50))
  FROM (WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000) SELECT x FROM c);
SELECT dolt_commit('-am','v1');
UPDATE t SET v = hex(randomblob(50)) WHERE id <= 1000;
SELECT dolt_commit('-am','v2');
ATTACH DATABASE '$SQLDB' AS ext;
SELECT dolt_gc();" | $DOLTLITE "$DB5" > /dev/null 2>&1

  run_test "gc_attach_count" \
    "SELECT count(*) FROM t;" \
    "10000" "$DB5"

  run_test "gc_attach_integrity" \
    "PRAGMA integrity_check;" \
    "ok" "$DB5"

  rm -f "$DB5" "$SQLDB"
fi

# Cleanup
rm -f "$DB1" "$DB2" "$DB3" "$DB4"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
echo "All tests passed!"
