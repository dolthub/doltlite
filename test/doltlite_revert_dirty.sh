#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== dolt_revert dirty-set behavior ==="
echo ""


DB=/tmp/test_rv_dirty_unrelated_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');
INSERT INTO meta VALUES(1,'side');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_dirty_unrelated_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_dirty_unrelated_t_reverted" "SELECT count(*) FROM t;" "0" "$DB"
run_test "rv_dirty_unrelated_meta_kept" "SELECT note FROM meta WHERE id=1;" "side" "$DB"
run_test "rv_dirty_unrelated_status_clean" \
  "SELECT count(*) FROM dolt_status WHERE table_name='meta';" \
  "0" "$DB"
run_test "rv_dirty_unrelated_log_has_revert" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "1" "$DB"

db_rm "$DB"


DB=/tmp/test_rv_dirty_same_table_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');
INSERT INTO t VALUES(99,'side');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_dirty_same_refuses" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "Your local changes would be overwritten by revert" "$DB"
run_test "rv_dirty_same_no_new_commit" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "0" "$DB"
run_test "rv_dirty_same_row_kept" \
  "SELECT x FROM t WHERE id=99;" "side" "$DB"

db_rm "$DB"


DB=/tmp/test_rv_staged_unrelated_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');
INSERT INTO meta VALUES(1,'side');
SELECT dolt_add('meta');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_staged_unrelated_refuses" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "Your local changes would be overwritten by revert" "$DB"
run_test "rv_staged_unrelated_no_new_commit" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "0" "$DB"
run_test "rv_staged_unrelated_t_unchanged" \
  "SELECT x FROM t WHERE id=1;" "a" "$DB"
run_test "rv_staged_unrelated_meta_staged" \
  "SELECT count(*) FROM dolt_status WHERE table_name='meta';" \
  "1" "$DB"

db_rm "$DB"


DB=/tmp/test_rv_clean_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-Am','schema');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','add row');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_clean_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_clean_t_reverted" "SELECT count(*) FROM t;" "0" "$DB"

db_rm "$DB"

dltest_finish
