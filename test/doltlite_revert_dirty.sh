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

# Unstaged change to an unrelated table: revert succeeds, commits only the
# revert, and the change stays in the working set (Dolt 2.2.2: dolt_status
# keeps the table modified and no commit contains it).
run_test_match "rv_dirty_unrelated_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_dirty_unrelated_t_reverted" "SELECT count(*) FROM t;" "0" "$DB"
run_test "rv_dirty_unrelated_meta_kept" "SELECT note FROM meta WHERE id=1;" "side" "$DB"
run_test "rv_dirty_unrelated_meta_still_dirty" \
  "SELECT count(*) FROM dolt_status WHERE table_name='meta' AND staged=0;" \
  "1" "$DB"
run_test "rv_dirty_unrelated_commit_excludes_meta" \
  "SELECT count(*) FROM dolt_at_meta WHERE commit_ref='HEAD';" \
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


# Index-only revert of table t plus a dirty row on t: Dolt refuses. The
# table-entry overlap check misses this because DROP INDEX is a separate
# catalog object, so the split revert used to write the restored index
# into the working set, fail the commit-side merge, restore only in
# memory, and reopen with the index back.
DB=/tmp/test_rv_dirty_same_index_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'one');
CREATE INDEX t_v ON t(v);
SELECT dolt_commit('-A','-m','base with index');
DROP INDEX t_v;
SELECT dolt_commit('-A','-m','drop index');
INSERT INTO t VALUES(99,'unrelated dirty');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_dirty_same_index_refuses" \
  "SELECT dolt_revert('HEAD');" \
  "Your local changes would be overwritten by revert" "$DB"
run_test "rv_dirty_same_index_no_new_commit" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "0" "$DB"
run_test "rv_dirty_same_index_row_kept" \
  "SELECT v FROM t WHERE id=99;" "unrelated dirty" "$DB"
run_test "rv_dirty_same_index_still_absent" \
  "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v';" \
  "0" "$DB"
run_test "rv_dirty_same_index_reopen_absent" \
  "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v';" \
  "0" "$DB"
run_test "rv_dirty_same_index_reopen_row" \
  "SELECT v FROM t WHERE id=99;" "unrelated dirty" "$DB"
run_test "rv_dirty_same_index_reopen_no_revert" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'Revert%';" \
  "0" "$DB"

db_rm "$DB"


DB=/tmp/test_rv_dirty_unrelated_index_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'one');
CREATE INDEX t_v ON t(v);
SELECT dolt_commit('-A','-m','base with index');
DROP INDEX t_v;
SELECT dolt_commit('-A','-m','drop index');
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
INSERT INTO meta VALUES(1,'side');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_dirty_unrelated_index_hash" \
  "SELECT dolt_revert('HEAD');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_dirty_unrelated_index_restored" \
  "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v';" \
  "1" "$DB"
run_test "rv_dirty_unrelated_index_meta_kept" \
  "SELECT note FROM meta WHERE id=1;" "side" "$DB"
run_test "rv_dirty_unrelated_index_reopen_index" \
  "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v';" \
  "1" "$DB"
run_test "rv_dirty_unrelated_index_reopen_meta" \
  "SELECT note FROM meta WHERE id=1;" "side" "$DB"

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
