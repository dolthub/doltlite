#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite UPDATE REPLACE trigger tests ==="
echo ""

DB=/tmp/test_doltlite_update_replace_trigger_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

setup_sql="
CREATE TABLE t1(a UNIQUE ON CONFLICT REPLACE, b);
INSERT INTO t1(a,b) VALUES(4,12),(9,13);
CREATE INDEX i0 ON t1(b);
CREATE TRIGGER tr0 DELETE ON t1 BEGIN
  UPDATE t1 SET b = a;
END;
"

dltest_run_sql "$setup_sql" "$DB" >/dev/null

run_test_lastline "update_replace_trigger_initial_integrity" "
PRAGMA integrity_check;
" "ok" "$DB"

expect_constraint_failed() {
  local name="$1" sql="$2" db="$3" out rc
  out=$(dltest_run_sql "$sql" "$db" 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ] && echo "$out" | grep -q "constraint failed"; then
    dltest_pass
  else
    dltest_fail "$name" \
      "  expected constraint failed\n  rc=$rc\n  output:\n$out"
  fi
}

expect_constraint_failed "update_replace_trigger_constraint" "
PRAGMA recursive_triggers = true;
UPDATE t1 SET a=0;
" "$DB"

run_test_lastline "update_replace_trigger_final_integrity" "
PRAGMA integrity_check;
" "ok" "$DB"

run_test "update_replace_trigger_rows_unchanged" "
SELECT rowid,a,b FROM t1 ORDER BY rowid;
" "1|4|12
2|9|13" "$DB"

DB2=/tmp/test_doltlite_update_replace_trigger_ipk_$$.db
DB3=/tmp/test_doltlite_update_replace_trigger_wr_$$.db
DB4=/tmp/test_doltlite_update_replace_trigger_ignore_$$.db
rm -f "$DB2" "$DB3" "$DB4"
trap 'rm -f "$DB" "$DB2" "$DB3" "$DB4"' EXIT

dltest_run_sql "
CREATE TABLE t2(a INTEGER PRIMARY KEY, c UNIQUE, d);
CREATE INDEX t2d ON t2(d);
CREATE TRIGGER tr3 AFTER DELETE ON t2 BEGIN UPDATE t2 SET d=d+100; END;
INSERT INTO t2 VALUES(1,1,1),(2,2,2);
" "$DB2" >/dev/null

expect_constraint_failed "single_row_replace_trigger_update_constraint" "
PRAGMA recursive_triggers = true;
UPDATE OR REPLACE t2 SET c=1 WHERE a=2;
" "$DB2"

run_test_lastline "single_row_replace_trigger_update_integrity" "
PRAGMA integrity_check;
" "ok" "$DB2"

run_test "single_row_replace_trigger_update_index_lookup" "
SELECT a FROM t2 INDEXED BY t2d WHERE d=2;
SELECT a,c,d FROM t2 ORDER BY a;
" "2
1|1|1
2|2|2" "$DB2"

dltest_run_sql "
CREATE TABLE t2(a PRIMARY KEY, c UNIQUE) WITHOUT ROWID;
CREATE TRIGGER tr3 AFTER DELETE ON t2 BEGIN DELETE FROM t2; END;
INSERT INTO t2 VALUES(1,1),(2,2),(3,3);
" "$DB3" >/dev/null

expect_constraint_failed "without_rowid_replace_trigger_delete_constraint" "
PRAGMA recursive_triggers = true;
UPDATE OR REPLACE t2 SET c=1 WHERE a=2;
" "$DB3"

run_test "without_rowid_replace_trigger_delete_rows_unchanged" "
SELECT * FROM t2 ORDER BY a;
" "1|1
2|2
3|3" "$DB3"

dltest_run_sql "
CREATE TABLE t0(id INT, k TEXT, v0 INT, PRIMARY KEY(id,k)) WITHOUT ROWID;
CREATE TRIGGER tr0 AFTER DELETE ON t0 BEGIN INSERT OR IGNORE INTO t0 VALUES(1,'a',1); END;
INSERT INTO t0 VALUES(3,'e',6),(5,'e',6),(6,'e',4);
" "$DB4" >/dev/null

expect_constraint_failed "replace_trigger_insert_constraint" "
PRAGMA recursive_triggers = true;
UPDATE OR REPLACE t0 SET id=id+2;
" "$DB4"

run_test "replace_trigger_insert_rows_unchanged" "
SELECT * FROM t0 ORDER BY id;
PRAGMA integrity_check;
" "3|e|6
5|e|6
6|e|4
ok" "$DB4"

dltest_finish
