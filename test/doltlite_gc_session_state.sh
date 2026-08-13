#!/bin/bash

DLTEST_TIMEOUT=15
. "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Doltlite GC over session state ==="
echo ""

DB=/tmp/test_gc_session_staged_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_gc();
SELECT dolt_commit('-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "staged_commit_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "staged_commit_log"   "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test "staged_commit_value" "SELECT v FROM t WHERE id=3;" "c" "$DB"

run_test "staged_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_session_merge_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_branch' WHERE id=1;
SELECT dolt_commit('-A','-m','main change');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_branch' WHERE id=1;
SELECT dolt_commit('-A','-m','feat change');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_merge('feat');
SELECT dolt_gc();
SELECT dolt_merge('--abort');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_abort_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "merge_abort_value" "SELECT v FROM t WHERE id=1;" "main_branch" "$DB"
run_test "merge_abort_status_empty" "SELECT count(*) FROM dolt_status;" "0" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_session_checkpoint_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'b'),(3,'c');
SELECT dolt_add('-A');
PRAGMA wal_checkpoint;
SELECT dolt_commit('-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ckpt_commit_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "ckpt_commit_log"   "SELECT count(*) FROM dolt_log;" "3" "$DB"

run_test "ckpt_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_session_cv_$$.db; db_rm "$DB"
echo "CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1);
INSERT INTO c VALUES(10, 1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
DELETE FROM p WHERE id=1;
SELECT dolt_commit('-A','-m','main del');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES(20, 1);
SELECT dolt_commit('-A','-m','feat insert');
SELECT dolt_checkout('main');
PRAGMA foreign_keys=1;
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cv_table_p" "SELECT count(*) FROM p;" "[0-9]+" "$DB"
run_test_match "cv_table_c" "SELECT count(*) FROM c;" "[0-9]+" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_session_detached_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','feat row');" | $DOLTLITE "$DB" > /dev/null 2>&1

FEAT_TIP=$(dltest_run_sql "SELECT hash FROM dolt_branches WHERE name='feat';" "$DB")

echo "SELECT dolt_checkout('main');
SELECT dolt_branch('-D','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "detached_head_before_gc" "SELECT count(*) FROM t;" "2" "$DB/$FEAT_TIP"

echo "SELECT dolt_gc();" | $DOLTLITE "$DB/$FEAT_TIP" > /dev/null 2>&1

run_test "detached_head_survives_own_gc" "SELECT count(*) FROM t;" "2" "$DB/$FEAT_TIP"
run_test "detached_head_log_survives" "SELECT count(*) FROM dolt_log;" "3" "$DB/$FEAT_TIP"
run_test "detached_head_branch_intact" "SELECT count(*) FROM t;" "1" "$DB"

db_rm "$DB"

dltest_finish
