#!/bin/bash
DOLTLITE=${DOLTLITE:-./doltlite}
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Per-Branch WorkingSet Tests ==="
echo ""

DB=/tmp/test_ws_staged_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "UPDATE t SET val='A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main edit');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "main_committed" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

echo "SELECT dolt_checkout('feature');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature add');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1

run_test "feature_committed" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

echo "INSERT INTO t VALUES(4,'d');
SELECT dolt_add('t');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1

run_test "feature_has_staged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB/feature"

echo "SELECT dolt_commit('-m','feature staged');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "main_clean_after_switch" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "feature_clean_after_switch" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB/feature"

run_test "feature_data_count" \
  "SELECT count(*) FROM t;" "4" "$DB/feature"

rm -f "$DB"

DB=/tmp/test_ws_merge_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
SELECT dolt_branch('feature');
UPDATE t SET val='main_change' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main edit');
SELECT dolt_checkout('feature');
UPDATE t SET val='feature_change' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature edit');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "main_has_conflicts" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'CF|' || count(*) FROM dolt_conflicts; ROLLBACK;" "CF\\|1" "$DB"

run_test_match "main_clean_after_abort" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_merge('--abort'); SELECT 'ST|' || count(*) FROM dolt_status; ROLLBACK;" "ST\\|0" "$DB"

run_test_match "main_val_after_abort" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_merge('--abort'); SELECT 'VAL|' || val FROM t WHERE id=1; ROLLBACK;" "VAL\\|main_change" "$DB"

rm -f "$DB"

DB=/tmp/test_ws_persist_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
UPDATE t SET val='staged_val' WHERE id=1;
SELECT dolt_add('t');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ws_persist_staged_count" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

run_test "ws_persist_staged_status" \
  "SELECT status FROM dolt_status WHERE staged=1;" "modified" "$DB"

rm -f "$DB"

DB=/tmp/test_ws_gc_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
UPDATE t SET val='staged' WHERE id=1;
SELECT dolt_add('t');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ws_gc_staged_survives" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

run_test "ws_gc_data_ok" \
  "SELECT val FROM t WHERE id=1;" "staged" "$DB"

rm -f "$DB"

DB=/tmp/test_ws_reset_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
UPDATE t SET val='changed' WHERE id=1;
SELECT dolt_add('t');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "pre_reset_staged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "post_reset_clean" \
  "SELECT count(*) FROM dolt_status;" "0" "$DB"

run_test "post_reset_val" \
  "SELECT val FROM t WHERE id=1;" "a" "$DB"

rm -f "$DB"

DB=/tmp/test_ws_multi_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('b1');
UPDATE t SET val='b1_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1 edit');" | $DOLTLITE "$DB/b1" > /dev/null 2>&1

echo "SELECT dolt_checkout('b2');
INSERT INTO t VALUES(2,'b2_new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2 add');" | $DOLTLITE "$DB/b2" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_main_count" \
  "SELECT count(*) FROM t;" "1" "$DB"

echo "SELECT dolt_checkout('b1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_b1_val" \
  "SELECT val FROM t WHERE id=1;" "b1_val" "$DB/b1"

run_test "multi_b1_count" \
  "SELECT count(*) FROM t;" "1" "$DB/b1"

echo "SELECT dolt_checkout('b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_b2_count" \
  "SELECT count(*) FROM t;" "2" "$DB/b2"

run_test "multi_b2_new_row" \
  "SELECT val FROM t WHERE id=2;" "b2_new" "$DB/b2"

rm -f "$DB"

dltest_finish
