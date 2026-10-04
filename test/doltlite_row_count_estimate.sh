#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"
dltest_init_queries
DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

if [ ! -x "$DOLTLITE" ]; then
  echo "ERROR: $DOLTLITE not found or not executable"
  exit 1
fi

echo "=== DoltLite Row Count Estimate Tests ==="
echo ""


echo "--- Test 1: PRAGMA optimize skips small unchanged table ---"
DB=/tmp/rce_small_$$.db; rm -f "$DB"
ANALYZED=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<100)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x50';
INSERT INTO t VALUES(101,'newrow');
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "small_table_no_reanalysis" "$ANALYZED" "100 1"
rm -f "$DB"

echo "--- Test 2: PRAGMA optimize triggers when table grows 10x+ ---"
DB=/tmp/rce_grow_$$.db; rm -f "$DB"
GREW=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<100)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x50';
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<3000)
  INSERT INTO t SELECT n+100, 'x'||(n+100) FROM c;
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "large_growth_triggers_reanalysis" "$GREW" "3100 1"
rm -f "$DB"

echo "--- Test 3: Empty table reports zero rows ---"
DB=/tmp/rce_empty_$$.db; rm -f "$DB"
EMPTY=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
ANALYZE;
SELECT count(*) FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "empty_table" "$EMPTY" "0"
rm -f "$DB"

echo "--- Test 4: Single-row table estimate close to 1 ---"
DB=/tmp/rce_single_$$.db; rm -f "$DB"
SINGLE=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
INSERT INTO t VALUES(1,'one');
ANALYZE;
SELECT count(*) FROM t WHERE x='one';
INSERT INTO t VALUES(2,'two');
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "single_to_two_no_reanalysis" "$SINGLE" "1 1"
rm -f "$DB"

echo "--- Test 5: Large multi-level table tracks growth correctly ---"
DB=/tmp/rce_large_$$.db; rm -f "$DB"
LARGE=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<20000)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x50';
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<5000)
  INSERT INTO t SELECT n+20000, 'x'||(n+20000) FROM c;
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "large_table_growth_below_10x_no_reanalysis" "$LARGE" "20000 1"
rm -f "$DB"

echo "--- Test 6: After 10x growth from large baseline, reanalysis triggered ---"
DB=/tmp/rce_10x_$$.db; rm -f "$DB"
TENX=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<1000)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x50';
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<15000)
  INSERT INTO t SELECT n+1000, 'x'||(n+1000) FROM c;
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "10x_growth_triggers_reanalysis" "$TENX" "16000 1"
rm -f "$DB"

echo "--- Test 7: Join uses small table as outer loop after ANALYZE ---"
DB=/tmp/rce_join_$$.db; rm -f "$DB"
EQP=$(echo "CREATE TABLE small(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE big(id INTEGER PRIMARY KEY, y TEXT);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<10)
  INSERT INTO small SELECT n, 'x'||n FROM c;
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<5000)
  INSERT INTO big SELECT n, 'y'||n FROM c;
ANALYZE;
EXPLAIN QUERY PLAN SELECT * FROM big JOIN small ON small.id=big.id;" | dltest_query "$DB" 2>&1 | grep -E '^\|--SCAN|^\`--SCAN' | head -1)
dltest_assert_equal "join_picks_small_as_outer" "$EQP" "|--SCAN small"
rm -f "$DB"

echo "--- Test 8: Same-size tables don't flap reanalysis ---"
DB=/tmp/rce_flap_$$.db; rm -f "$DB"
FLAP=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<500)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x100';
DELETE FROM t WHERE id<50;
INSERT INTO t SELECT id+10000, 'r'||id FROM (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3);
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "small_perturbation_no_reanalysis" "$FLAP" "500 1"
rm -f "$DB"

echo "--- Test 9: Pending writes contribute to estimate ---"
DB=/tmp/rce_pending_$$.db; rm -f "$DB"
PENDING=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<100)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x50';
BEGIN;
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<5000)
  INSERT INTO t SELECT n+100, 'x'||(n+100) FROM c;
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';
COMMIT;" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "pending_writes_trigger_reanalysis" "$PENDING" "5100 1"
rm -f "$DB"

echo "--- Test 10: Multi-level tree estimate roughly tracks row count ---"
DB=/tmp/rce_multi_$$.db; rm -f "$DB"
MULTI=$(echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<50000)
  INSERT INTO t SELECT n, 'x'||n FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE x='x50';
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<10000)
  INSERT INTO t SELECT n+50000, 'x'||(n+50000) FROM c;
PRAGMA optimize(0x10002);
SELECT stat FROM sqlite_stat1 WHERE tbl='t';" | dltest_query "$DB" 2>&1 | tail -1)
dltest_assert_equal "multi_level_growth_below_10x_no_reanalysis" "$MULTI" "50000 1"
rm -f "$DB"

dltest_finish
