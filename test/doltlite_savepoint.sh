#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local name="$1" sql="$2" expected="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  local exit_code=$?
  if [ $exit_code -eq 137 ] || [ $exit_code -eq 139 ]; then
    result="CRASH (exit $exit_code)"
  fi
  if [ "$result" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"
  fi
}

run_test_match() {
  local name="$1" sql="$2" pattern="$3" db="$4"
  local result=$(echo "$sql" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$db" 2>&1)
  local exit_code=$?
  if [ $exit_code -eq 137 ] || [ $exit_code -eq 139 ]; then
    result="CRASH (exit $exit_code)"
  fi
  if echo "$result" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"
  fi
}

echo "=== Doltlite Savepoint & Transaction Interaction Tests ==="
echo ""

DB1=/tmp/test_savepoint1_$$.db; rm -f "$DB1"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB1" > /dev/null 2>&1

echo "BEGIN; INSERT INTO t VALUES(2,'txn'); SELECT dolt_commit('-A','-m','in-txn commit'); ROLLBACK;" | $DOLTLITE "$DB1" > /dev/null 2>&1

run_test "txn_rollback_data_count" \
  "SELECT count(*) FROM t;" \
  "2" "$DB1"

run_test "txn_rollback_dolt_commit_survives" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB1"

DB2=/tmp/test_savepoint2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1

echo "BEGIN; INSERT INTO t VALUES(2,'before-sp'); SAVEPOINT x; INSERT INTO t VALUES(3,'after-sp'); SELECT dolt_commit('-A','-m','mid-savepoint'); ROLLBACK TO x; COMMIT;" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "savepoint_rollback_keeps_pre_sp" \
  "SELECT count(*) FROM t;" \
  "3" "$DB2"

run_test "savepoint_rollback_row2_exists" \
  "SELECT v FROM t WHERE id=2;" \
  "before-sp" "$DB2"

run_test "savepoint_rollback_row3_gone" \
  "SELECT count(*) FROM t WHERE id=3;" \
  "1" "$DB2"

run_test "savepoint_dolt_commit_in_log" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB2"

DB3=/tmp/test_savepoint3_$$.db; rm -f "$DB3"

run_test_match "basic_no_txn_commit" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'hello'); SELECT dolt_commit('-A','-m','basic');" \
  "^[0-9a-f]{40}$" "$DB3"

run_test "basic_no_txn_data" \
  "SELECT v FROM t;" \
  "hello" "$DB3"

run_test "basic_no_txn_log" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "basic" "$DB3"

DB4=/tmp/test_savepoint4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1

echo "BEGIN;
  INSERT INTO t VALUES(2,'level0');
  SAVEPOINT sp1;
    INSERT INTO t VALUES(3,'level1');
    SAVEPOINT sp2;
      INSERT INTO t VALUES(4,'level2');
      RELEASE sp2;
    RELEASE sp1;
COMMIT;" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "nested_sp_all_released_count" \
  "SELECT count(*) FROM t;" \
  "4" "$DB4"

DB4b=/tmp/test_savepoint4b_$$.db; rm -f "$DB4b"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4b" > /dev/null 2>&1

echo "BEGIN;
  INSERT INTO t VALUES(2,'keep');
  SAVEPOINT sp1;
    INSERT INTO t VALUES(3,'discard');
    SAVEPOINT sp2;
      INSERT INTO t VALUES(4,'also-discard');
    ROLLBACK TO sp2;
  ROLLBACK TO sp1;
COMMIT;" | $DOLTLITE "$DB4b" > /dev/null 2>&1

run_test "nested_sp_rollback_count" \
  "SELECT count(*) FROM t;" \
  "2" "$DB4b"

run_test "nested_sp_rollback_kept_row" \
  "SELECT v FROM t WHERE id=2;" \
  "keep" "$DB4b"

DB5=/tmp/test_savepoint5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'staged');" | $DOLTLITE "$DB5" > /dev/null 2>&1

echo "BEGIN; SELECT dolt_reset('--hard'); COMMIT;" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "hard_reset_in_txn_count" \
  "SELECT count(*) FROM t;" \
  "1" "$DB5"

run_test "hard_reset_in_txn_status_clean" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB5"

DB5b=/tmp/test_savepoint5b_$$.db; rm -f "$DB5b"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5b" > /dev/null 2>&1
run_test_match "hard_reset_savepoint_invalidated" \
  "SAVEPOINT sp1; SELECT dolt_reset('--hard','HEAD'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB5b"

DB5c=/tmp/test_savepoint5c_$$.db; rm -f "$DB5c"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init'); UPDATE t SET v='dirty';" | $DOLTLITE "$DB5c" > /dev/null 2>&1
run_test_match "bad_reset_savepoint_invalidated" \
  "SAVEPOINT sp1; SELECT dolt_reset('--hard','bogus'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB5c"
run_test "bad_reset_savepoint_row_persists" \
  "SELECT v FROM t WHERE id=1;" \
  "dirty" "$DB5c"

DB5d=/tmp/test_savepoint5d_$$.db; rm -f "$DB5d"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5d" > /dev/null 2>&1
run_test_match "bad_reset_nested_savepoint_allows_rollback" \
  "BEGIN; SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_reset('--hard','bogus'); ROLLBACK TO sp1; COMMIT; SELECT count(*) FROM t;" \
  "^1$" "$DB5d"

DB5e=/tmp/test_savepoint5e_$$.db; rm -f "$DB5e"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');" | $DOLTLITE "$DB5e" > /dev/null 2>&1
run_test "conflicts_resolve_ours_named_savepoint_rollback" \
  "BEGIN; SELECT dolt_merge('feature'); SAVEPOINT sp1; SELECT dolt_conflicts_resolve('--ours','t'); ROLLBACK TO sp1; SELECT 'C|'||count(*) FROM dolt_conflicts; SELECT 'V|'||v FROM t WHERE id=1;" \
  "Error near line 1: Merge has 1 conflict(s). Resolve and then commit with dolt_commit.
0
C|1
V|main" "$DB5e"
run_test "conflicts_resolve_theirs_named_savepoint_rollback" \
  "BEGIN; SELECT dolt_merge('feature'); SAVEPOINT sp1; SELECT dolt_conflicts_resolve('--theirs','t'); ROLLBACK TO sp1; SELECT 'C|'||count(*) FROM dolt_conflicts; SELECT 'V|'||v FROM t WHERE id=1;" \
  "Error near line 1: Merge has 1 conflict(s). Resolve and then commit with dolt_commit.
0
C|1
V|main" "$DB5e"
run_test "conflicts_delete_named_savepoint_rollback" \
  "BEGIN; SELECT dolt_merge('feature'); SAVEPOINT sp1; DELETE FROM dolt_conflicts_t WHERE our_id=1; ROLLBACK TO sp1; SELECT 'C|'||count(*) FROM dolt_conflicts; SELECT 'V|'||v FROM t WHERE id=1;" \
  "Error near line 1: Merge has 1 conflict(s). Resolve and then commit with dolt_commit.
C|1
V|main" "$DB5e"

DB6=/tmp/test_savepoint6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('other');" | $DOLTLITE "$DB6" > /dev/null 2>&1

echo "BEGIN; SELECT dolt_checkout('other'); COMMIT;" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test_match "checkout_in_txn_branch" \
  "SELECT active_branch();" \
  "^(main|other)$" "$DB6"

DB6b=/tmp/test_savepoint6b_$$.db; rm -f "$DB6b"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6b" > /dev/null 2>&1
echo "SELECT dolt_branch('other');" | $DOLTLITE "$DB6b" > /dev/null 2>&1

run_test "checkout_dirty_in_txn" \
  "BEGIN; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_checkout('other'); COMMIT;" \
  "0" "$DB6b"

DB6c=/tmp/test_savepoint6c_$$.db; rm -f "$DB6c"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6c" > /dev/null 2>&1
echo "SELECT dolt_branch('other');" | $DOLTLITE "$DB6c" > /dev/null 2>&1
echo "SELECT dolt_checkout('other'); UPDATE t SET v='other'; SELECT dolt_commit('-A','-m','other'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB6c" > /dev/null 2>&1

run_test_match "checkout_dirty_rollback_branch" \
  "BEGIN; UPDATE t SET v='dirty'; SELECT dolt_checkout('other'); ROLLBACK; SELECT active_branch();" \
  "^other$" "$DB6c"

run_test_match "checkout_dirty_rollback_data" \
  "BEGIN; UPDATE t SET v='dirty'; SELECT dolt_checkout('other'); ROLLBACK; SELECT v FROM t WHERE id=1;" \
  "^other$" "$DB6c"

DB6d=/tmp/test_savepoint6d_$$.db; rm -f "$DB6d"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6d" > /dev/null 2>&1
run_test_match "branch_dirty_rollback_branch" \
  "BEGIN; UPDATE t SET v='dirty'; SELECT dolt_branch('txb'); ROLLBACK; SELECT group_concat(name, ',') FROM dolt_branches;" \
  "^main,txb$|^txb,main$" "$DB6d"

DB6e=/tmp/test_savepoint6e_$$.db; rm -f "$DB6e"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6e" > /dev/null 2>&1
run_test_match "branch_dirty_rollback_data" \
  "BEGIN; UPDATE t SET v='dirty'; SELECT dolt_branch('txb'); ROLLBACK; SELECT v FROM t WHERE id=1;" \
  "^dirty$" "$DB6e"

DB6f=/tmp/test_savepoint6f_$$.db; rm -f "$DB6f"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6f" > /dev/null 2>&1
run_test_match "tag_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; UPDATE t SET v='dirty'; SELECT dolt_tag('v1'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6f"
run_test_match "tag_savepoint_row_persists" \
  "SELECT v FROM t WHERE id=1;" \
  "^dirty$" "$DB6f"
run_test_match "tag_savepoint_tag_persists" \
  "SELECT group_concat(tag_name, ',') FROM dolt_tags;" \
  "^v1$" "$DB6f"

DB6g=/tmp/test_savepoint6g_$$.db; rm -f "$DB6g"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g" > /dev/null 2>&1
run_test_match "remote_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; UPDATE t SET v='dirty'; SELECT dolt_remote('add','origin','file:///tmp/savepoint-remote'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g"
run_test_match "remote_savepoint_row_persists" \
  "SELECT v FROM t WHERE id=1;" \
  "^dirty$" "$DB6g"
run_test_match "remote_savepoint_remote_persists" \
  "SELECT group_concat(name, ',') FROM dolt_remotes;" \
  "^origin$" "$DB6g"

DB6g1=/tmp/test_savepoint6g1_$$.db; rm -f "$DB6g1"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1" > /dev/null 2>&1
run_test_match "add_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_add('.'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1"
run_test_match "add_savepoint_row_persists" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1"

DB6g1b=/tmp/test_savepoint6g1b_$$.db; rm -f "$DB6g1b"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1b" > /dev/null 2>&1
run_test_match "add_savepoint_bad_option_rollback_to_errors" \
  "SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_add('--bogus'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1b"
run_test_match "add_savepoint_bad_option_row_persists" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1b"

DB6g1c=/tmp/test_savepoint6g1c_$$.db; rm -f "$DB6g1c"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1c" > /dev/null 2>&1
run_test_match "add_savepoint_missing_table_rollback_to_errors" \
  "SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_add('nope'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1c"
run_test_match "add_savepoint_missing_table_row_persists" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1c"

DB6g1d=/tmp/test_savepoint6g1d_$$.db; rm -f "$DB6g1d"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1d" > /dev/null 2>&1
run_test_match "commit_nested_savepoint_bad_option_rollback_to_succeeds" \
  "BEGIN; SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_commit('--bogus'); ROLLBACK TO sp1; SELECT count(*) FROM t; ROLLBACK;" \
  "^1$" "$DB6g1d"

DB6g1e=/tmp/test_savepoint6g1e_$$.db; rm -f "$DB6g1e"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1e" > /dev/null 2>&1
run_test_match "commit_begin_bad_option_reopen_row_rolled_back" \
  "BEGIN; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_commit('--bogus');" \
  "unknown option \`--bogus\`" "$DB6g1e"
run_test_match "commit_begin_bad_option_count_after_reopen" \
  "SELECT count(*) FROM t;" \
  "^1$" "$DB6g1e"

DB6g1r=/tmp/test_savepoint6g1r_$$.db; rm -f "$DB6g1r"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1r" > /dev/null 2>&1
run_test_match "rebase_missing_upstream_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_rebase('nope'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1r"
run_test_match "rebase_missing_upstream_savepoint_row_persists" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1r"

DB6g1s=/tmp/test_savepoint6g1s_$$.db; rm -f "$DB6g1s"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g1s" > /dev/null 2>&1
run_test_match "rebase_bad_option_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; INSERT INTO t VALUES(2,'dirty'); SELECT dolt_rebase('--bogus'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1s"
run_test_match "rebase_bad_option_savepoint_row_persists" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1s"

DB6g1t=/tmp/test_savepoint6g1t_$$.db; rm -f "$DB6g1t"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_checkout('-b','feature'); UPDATE t SET v='feat' WHERE id=1; SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main'); UPDATE t SET v='main' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB6g1t" > /dev/null 2>&1
echo "BEGIN; SELECT dolt_merge('feature');" | $DOLTLITE "$DB6g1t" > /dev/null 2>&1
run_test "merge_conflict_reopen_restores_main" \
  "SELECT v FROM t WHERE id=1;" \
  "main" "$DB6g1t"
run_test "merge_conflict_reopen_no_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" \
  "0" "$DB6g1t"

DB6g1u=/tmp/test_savepoint6g1u_$$.db; rm -f "$DB6g1u"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,'feat'); SELECT dolt_commit('-A','-m','f1'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,'main'); SELECT dolt_commit('-A','-m','m1'); SELECT dolt_checkout('feat'); SELECT dolt_rebase('-i','main'); BEGIN; SAVEPOINT sp1; SELECT dolt_rebase('--continue'); COMMIT;" | $DOLTLITE "$DB6g1u" > /dev/null 2>&1
run_test_match "rebase_continue_nested_savepoint_reopens_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g1u"
run_test_match "rebase_continue_nested_savepoint_no_temp_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "^0$" "$DB6g1u"
run_test_match "rebase_continue_nested_savepoint_rows_persist" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1u"
run_test_match "rebase_continue_nested_savepoint_log_persists" \
  "SELECT count(*)-1 FROM dolt_log;" \
  "^2$" "$DB6g1u"

DB6g1v=/tmp/test_savepoint6g1v_$$.db; rm -f "$DB6g1v"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB6g1v" > /dev/null 2>&1
run_test_match "rebase_start_preexisting_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_rebase('-i','main'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1v/feat"
run_test_match "rebase_start_preexisting_savepoint_temp_branch_survives" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "^1$" "$DB6g1v"

DB6g1w=/tmp/test_savepoint6g1w_$$.db; rm -f "$DB6g1w"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB6g1w" > /dev/null 2>&1
run_test_match "rebase_continue_preexisting_savepoint_rollback_to_errors" \
  "BEGIN; SAVEPOINT sp1; SELECT dolt_rebase('-i','main'); SELECT dolt_rebase('--continue'); ROLLBACK TO sp1; COMMIT;" \
  "no such savepoint: sp1" "$DB6g1w/feat"
run_test_match "rebase_continue_preexisting_savepoint_no_temp_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "^0$" "$DB6g1w"
run_test_match "rebase_continue_preexisting_savepoint_rows_persist" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g1w"
run_test_match "rebase_continue_preexisting_savepoint_log_persists" \
  "SELECT count(*)-1 FROM dolt_log;" \
  "^2$" "$DB6g1w"

DB6g1x=/tmp/test_savepoint6g1x_$$.db; rm -f "$DB6g1x"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); UPDATE t SET v=2 WHERE id=1; SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); SELECT dolt_checkout('main'); UPDATE t SET v=3 WHERE id=1; SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB6g1x" > /dev/null 2>&1
run_test_match "rebase_resolve_theirs_top_savepoint_rollback_to_errors" \
  "SELECT dolt_rebase('-i','main'); SAVEPOINT sp1; SELECT dolt_conflicts_resolve('--theirs','t'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g1x/feat"
run_test_match "rebase_resolve_theirs_top_savepoint_reopen_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g1x"
run_test_match "rebase_resolve_theirs_top_savepoint_temp_branch_survives" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "^1$" "$DB6g1x"
run_test_match "rebase_resolve_theirs_top_savepoint_reopen_no_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" \
  "^0$" "$DB6g1x"

DB6g2=/tmp/test_savepoint6g2_$$.db; rm -f "$DB6g2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g2" > /dev/null 2>&1
run_test_match "branch_delete_current_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_branch('-d','main'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g2"
run_test_match "branch_delete_current_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g2"

DB6g3=/tmp/test_savepoint6g3_$$.db; rm -f "$DB6g3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g3" > /dev/null 2>&1
run_test_match "branch_delete_missing_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_branch('-d','nope'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g3"
run_test_match "branch_delete_missing_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g3"

DB6g4=/tmp/test_savepoint6g4_$$.db; rm -f "$DB6g4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g4" > /dev/null 2>&1
run_test_match "tag_delete_missing_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_tag('-d','missing'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g4"
run_test_match "tag_delete_missing_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g4"

DB6g5=/tmp/test_savepoint6g5_$$.db; rm -f "$DB6g5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g5" > /dev/null 2>&1
run_test_match "remote_delete_missing_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_remote('remove','missing'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g5"
run_test_match "remote_delete_missing_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g5"

DB6g6=/tmp/test_savepoint6g6_$$.db; rm -f "$DB6g6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g6" > /dev/null 2>&1
run_test_match "checkout_missing_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_checkout('missing'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g6"
run_test_match "checkout_missing_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g6"

DB6g7=/tmp/test_savepoint6g7_$$.db; rm -f "$DB6g7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g7" > /dev/null 2>&1
run_test_match "push_missing_remote_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_push('missing','main'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g7"
run_test_match "push_missing_remote_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g7"

DB6g8=/tmp/test_savepoint6g8_$$.db; rm -f "$DB6g8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g8" > /dev/null 2>&1
run_test_match "fetch_missing_remote_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_fetch('missing'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g8"
run_test_match "fetch_missing_remote_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g8"

DB6g9=/tmp/test_savepoint6g9_$$.db; rm -f "$DB6g9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6g9" > /dev/null 2>&1
run_test_match "pull_missing_remote_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_pull('missing','main'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g9"
run_test_match "pull_missing_remote_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g9"
DB6g10=/tmp/test_savepoint6g10_$$.db; rm -f "$DB6g10"
run_test_match "clone_bad_url_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_clone('bogus://remote'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g10"
run_test_match "clone_bad_url_savepoint_branch_stays_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6g10"

DB6g10s_DIR=/tmp/test_savepoint6g10s_$$; rm -rf "$DB6g10s_DIR"; mkdir -p "$DB6g10s_DIR"
DB6g10s_SEED="$DB6g10s_DIR/seed.db"
DB6g10s_REMOTE="file://$DB6g10s_DIR/remote.db"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_remote('add','origin','$DB6g10s_REMOTE'); SELECT dolt_push('origin','main');" | $DOLTLITE "$DB6g10s_SEED" > /dev/null 2>&1

DB6g10s_TOP="$DB6g10s_DIR/top.db"; rm -f "$DB6g10s_TOP"
run_test "clone_savepoint_rollback_to_succeeds" \
  "SAVEPOINT sp1; SELECT dolt_clone('$DB6g10s_REMOTE'); ROLLBACK TO sp1; SELECT active_branch(); SELECT count(*) FROM t; SELECT count(*) FROM dolt_remotes;" \
  "0
main
1
1" "$DB6g10s_TOP"

DB6g10s_BEGIN="$DB6g10s_DIR/begin.db"; rm -f "$DB6g10s_BEGIN"
run_test "clone_begin_rollback_keeps_clone" \
  "BEGIN; SELECT dolt_clone('$DB6g10s_REMOTE'); ROLLBACK; SELECT active_branch(); SELECT count(*) FROM t; SELECT count(*) FROM dolt_remotes;" \
  "0
main
1
1" "$DB6g10s_BEGIN"

DB6g10s_NESTED="$DB6g10s_DIR/nested.db"; rm -f "$DB6g10s_NESTED"
run_test "clone_nested_savepoint_rollback_to_succeeds" \
  "BEGIN; SAVEPOINT sp1; SELECT dolt_clone('$DB6g10s_REMOTE'); ROLLBACK TO sp1; COMMIT; SELECT active_branch(); SELECT count(*) FROM t; SELECT count(*) FROM dolt_remotes;" \
  "0
main
1
1" "$DB6g10s_NESTED"

DB6g11_DIR=/tmp/test_savepoint6g11_$$; rm -rf "$DB6g11_DIR"; mkdir -p "$DB6g11_DIR"
DB6g11="$DB6g11_DIR/src.db"
DB6g11_OTHER="$DB6g11_DIR/other.db"
DB6g11_REMOTE="file://$DB6g11_DIR/remote.db"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_remote('add','origin','$DB6g11_REMOTE'); SELECT dolt_push('origin','main');" | $DOLTLITE "$DB6g11" > /dev/null 2>&1
echo "SELECT dolt_clone('$DB6g11_REMOTE'); INSERT INTO t VALUES(2,'other'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','other'); SELECT dolt_push('origin','main');" | $DOLTLITE "$DB6g11_OTHER" > /dev/null 2>&1
run_test_match "pull_nested_savepoint_rollback_to_errors" \
  "BEGIN; SAVEPOINT sp1; SELECT dolt_pull('origin','main'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6g11"
run_test_match "pull_nested_savepoint_rows_persist" \
  "SELECT count(*) FROM t;" \
  "^2$" "$DB6g11"
run_test_match "pull_nested_savepoint_log_persists" \
  "SELECT count(*)-1 FROM dolt_log;" \
  "^2$" "$DB6g11"

DB6h=/tmp/test_savepoint6h_$$.db; rm -f "$DB6h"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET v='other'; SELECT dolt_commit('-A','-m','other'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB6h" > /dev/null 2>&1
run_test_match "checkout_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; UPDATE t SET v='dirty'; SELECT dolt_checkout('other'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6h"
run_test_match "checkout_savepoint_branch_reopens_on_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6h"
run_test_match "checkout_savepoint_row_reopens_dirty" \
  "SELECT v FROM t WHERE id=1;" \
  "^dirty$" "$DB6h"

DB6i=/tmp/test_savepoint6i_$$.db; rm -f "$DB6i"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6i" > /dev/null 2>&1
run_test_match "checkout_b_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; UPDATE t SET v='dirty'; SELECT dolt_checkout('-b','side'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6i"
run_test_match "checkout_b_savepoint_branch_reopens_on_main" \
  "SELECT active_branch();" \
  "^main$" "$DB6i"
run_test_match "checkout_b_savepoint_row_reopens_dirty" \
  "SELECT v FROM t WHERE id=1;" \
  "^dirty$" "$DB6i"
run_test_match "checkout_b_savepoint_branch_created" \
  "SELECT group_concat(name, ',') FROM dolt_branches;" \
  "^main,side$|^side,main$" "$DB6i"

DB6j=/tmp/test_savepoint6j_$$.db; rm -f "$DB6j"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'main'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB6j" > /dev/null 2>&1
run_test_match "cherry_pick_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_cherry_pick('feat'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6j"
run_test_match "cherry_pick_savepoint_rows_persist" \
  "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered;" \
  "^1:main,2:feat$" "$DB6j"
run_test_match "cherry_pick_savepoint_log_persists" \
  "SELECT count(*) FROM dolt_log;" \
  "^3$" "$DB6j"

DB6k=/tmp/test_savepoint6k_$$.db; rm -f "$DB6k"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); UPDATE t SET v='c2' WHERE id=1; SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB6k" > /dev/null 2>&1
run_test_match "revert_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_revert('HEAD'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB6k"
run_test_match "revert_savepoint_rows_persist" \
  "SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered;" \
  "^1:base$" "$DB6k"
run_test_match "revert_savepoint_log_persists" \
  "SELECT count(*) FROM dolt_log;" \
  "^4$" "$DB6k"

DB7=/tmp/test_savepoint7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'feature-row'); SELECT dolt_commit('-A','-m','feature work');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB7" > /dev/null 2>&1

echo "BEGIN; SELECT dolt_merge('feature'); COMMIT;" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test "merge_in_txn_data" \
  "SELECT count(*) FROM t;" \
  "2" "$DB7"

run_test "merge_in_txn_feature_row" \
  "SELECT v FROM t WHERE id=2;" \
  "feature-row" "$DB7"

run_test "merge_in_txn_log" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "feature work" "$DB7"

DB7b=/tmp/test_savepoint7b_$$.db; rm -f "$DB7b"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB7b" > /dev/null 2>&1
run_test_match "merge_savepoint_success_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_merge('feat'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB7b"
run_test "merge_savepoint_success_rows_persist" \
  "SELECT count(*) FROM t;" \
  "2" "$DB7b"
run_test "merge_savepoint_success_log_persists" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB7b"

DB7c=/tmp/test_savepoint7c_$$.db; rm -f "$DB7c"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET v='feat' WHERE id=1; SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main'); UPDATE t SET v='main' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7c" > /dev/null 2>&1
run_test_match "merge_abort_savepoint_rollback_to_errors" \
  "BEGIN; SELECT dolt_merge('feat'); SAVEPOINT sp1; SELECT dolt_merge('--abort'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB7c"
run_test "merge_abort_savepoint_clears_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" \
  "0" "$DB7c"
run_test "merge_abort_savepoint_restores_rows" \
  "SELECT v FROM t WHERE id=1;" \
  "main" "$DB7c"

DB7d=/tmp/test_savepoint7d_$$.db; rm -f "$DB7d"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET v='feat' WHERE id=1; SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main'); UPDATE t SET v='main' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7d" > /dev/null 2>&1
run_test_match "merge_conflict_nested_savepoint_allows_rollback" \
  "BEGIN; SAVEPOINT sp1; SELECT dolt_merge('feat'); ROLLBACK TO sp1; SELECT count(*) FROM dolt_conflicts; SELECT v FROM t WHERE id=1;" \
  "0
main$" "$DB7d"

DB7e=/tmp/test_savepoint7e_$$.db; rm -f "$DB7e"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); INSERT INTO t VALUES(3,3); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f2'); INSERT INTO t VALUES(4,4); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f3'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','m'); SELECT dolt_checkout('feat');" | $DOLTLITE "$DB7e" > /dev/null 2>&1
run_test_match "rebase_abort_savepoint_rollback_to_errors" \
  "SAVEPOINT sp1; SELECT dolt_rebase('-i','main'); SELECT dolt_rebase('--abort'); ROLLBACK TO sp1;" \
  "no such savepoint: sp1" "$DB7e"
run_test "rebase_abort_savepoint_reopen_main" \
  "SELECT active_branch();" \
  "main" "$DB7e"
run_test "rebase_abort_savepoint_reopen_no_rebase_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" "$DB7e"
run_test "rebase_abort_savepoint_reopen_main_rows" \
  "SELECT count(*) FROM t;" \
  "2" "$DB7e"
run_test "rebase_abort_savepoint_reopen_no_plan_table" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dolt_rebase';" \
  "0" "$DB7e"

DB7e1=/tmp/test_savepoint7e1_$$.db; rm -f "$DB7e1"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1'); SELECT dolt_checkout('feat'); SELECT dolt_rebase('-i','main'); BEGIN; SELECT dolt_rebase('--abort'); COMMIT;" | $DOLTLITE "$DB7e1" > /dev/null 2>&1
run_test "rebase_abort_explicit_txn_reopen_main" \
  "SELECT active_branch();" \
  "main" "$DB7e1"
run_test "rebase_abort_explicit_txn_reopen_no_rebase_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" "$DB7e1"
run_test "rebase_abort_explicit_txn_reopen_main_rows" \
  "SELECT count(*) FROM t;" \
  "2" "$DB7e1"

DB7e2=/tmp/test_savepoint7e2_$$.db; rm -f "$DB7e2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); UPDATE t SET v=2 WHERE id=1; SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); SELECT dolt_checkout('main'); UPDATE t SET v=3 WHERE id=1; SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1'); SELECT dolt_checkout('feat'); SELECT dolt_rebase('-i','main'); SELECT dolt_conflicts_resolve('--theirs','t'); BEGIN; SELECT dolt_rebase('--abort'); COMMIT;" | $DOLTLITE "$DB7e2" > /dev/null 2>&1
run_test "rebase_abort_after_resolve_explicit_txn_reopen_main" \
  "SELECT active_branch();" \
  "main" "$DB7e2"
run_test "rebase_abort_after_resolve_explicit_txn_reopen_no_rebase_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" "$DB7e2"
run_test "rebase_abort_after_resolve_explicit_txn_reopen_no_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" \
  "0" "$DB7e2"
run_test "rebase_abort_after_resolve_explicit_txn_reopen_main_value" \
  "SELECT v FROM t WHERE id=1;" \
  "3" "$DB7e2"

DB7f=/tmp/test_savepoint7f_$$.db; rm -f "$DB7f"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init'); SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','f1'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','m1'); SELECT dolt_checkout('feat'); SAVEPOINT sp1; SELECT dolt_rebase('-i','main'); SELECT dolt_rebase('--continue'); ROLLBACK TO sp1;" | $DOLTLITE "$DB7f" > /dev/null 2>&1
run_test "rebase_continue_top_savepoint_reopen_main" \
  "SELECT active_branch();" \
  "main" "$DB7f"
run_test "rebase_continue_top_savepoint_reopen_no_temp_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" \
  "0" "$DB7f"
run_test "rebase_continue_top_savepoint_rows_rolled_back" \
  "SELECT count(*) FROM t;" \
  "2" "$DB7f"
run_test "rebase_continue_top_savepoint_log_rolled_back" \
  "SELECT count(*)-1 FROM dolt_log;" \
  "2" "$DB7f"

DB8=/tmp/test_savepoint8_$$.db; rm -f "$DB8"
run_test "bulk_threshold_savepoint_rollback" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   BEGIN;
   SAVEPOINT sp1;
   WITH RECURSIVE gen(x) AS (
     VALUES(1)
     UNION ALL
     SELECT x+1 FROM gen WHERE x<66000
   )
   INSERT INTO t
   SELECT x, printf('row-%d', x) FROM gen;
   ROLLBACK TO sp1;
   COMMIT;
   SELECT count(*) FROM t;" \
  "0" "$DB8"

DB9=/tmp/test_savepoint9_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB9" > /dev/null 2>&1

echo "BEGIN; SELECT dolt_branch('ephemeral'); ROLLBACK;" | $DOLTLITE "$DB9" > /dev/null 2>&1

run_test_match "branch_survives_rollback" \
  "SELECT count(*) FROM dolt_branches;" \
  "^[12]$" "$DB9"

run_test_match "branch_name_after_rollback" \
  "SELECT name FROM dolt_branches ORDER BY name;" \
  "main" "$DB9"

# A branch switch seals the transaction it runs in. Releasing the savepoints
# is not enough on its own: an enclosing BEGIN left open means a later
# ROLLBACK reverts the working set to the branch we left while the ref already
# names the one we switched to, and that mismatch is what gets persisted. The
# plain-BEGIN and bare-SAVEPOINT shapes above never had the open BEGIN, so the
# combination is the case that needs pinning.
DBSP1=/tmp/test_sp_begin_checkout_$$.db; rm -f "$DBSP1"
echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('side');
INSERT INTO t VALUES(4,40);
SELECT dolt_commit('-A','-m','main row 4');
SELECT dolt_checkout('side');
INSERT INTO t VALUES(9,90);
SELECT dolt_commit('-A','-m','side row 9');
SELECT dolt_checkout('main');" | $DOLTLITE "$DBSP1" > /dev/null 2>&1
echo "BEGIN;
SAVEPOINT s;
INSERT INTO t VALUES(7,70);
SELECT dolt_checkout('side');
ROLLBACK;" | $DOLTLITE "$DBSP1" > /dev/null 2>&1

run_test "begin_savepoint_checkout_keeps_target_rows" \
  "SELECT group_concat(a) FROM t;" "1,2,9" "$DBSP1/side"
run_test "begin_savepoint_checkout_target_clean" \
  "SELECT count(*) FROM dolt_status;" "0" "$DBSP1/side"
run_test "begin_savepoint_checkout_source_intact" \
  "SELECT group_concat(a) FROM t WHERE a IN (1,2,4);" "1,2,4" "$DBSP1"

# The same seal carries the interactive rebase claim and plan, which are
# written before the switch and are lost with the transaction if it stays open.
DBSP2=/tmp/test_sp_begin_rebase_$$.db; rm -f "$DBSP2"
echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
INSERT INTO t VALUES(9,90);
SELECT dolt_commit('-A','-m','side');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,40);
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DBSP2" > /dev/null 2>&1
echo "BEGIN;
SAVEPOINT s;
SELECT dolt_rebase('-i','main');
ROLLBACK;" | $DOLTLITE "$DBSP2/side" > /dev/null 2>&1

run_test_match "begin_savepoint_rebase_plan_survives" \
  "SELECT count(*) FROM dolt_rebase;" "^[1-9][0-9]*$" "$DBSP2/dolt_rebase_side"
run_test_match "begin_savepoint_rebase_continues" \
  "SELECT dolt_rebase('--continue');" "Successfully rebased" "$DBSP2/side"

rm -f "$DBSP1" "$DBSP2"

rm -f "$DB1" "$DB2" "$DB3" "$DB4" "$DB4b" "$DB5" "$DB6" "$DB6b" "$DB7" "$DB8" "$DB9" \
  "$DB6g1" "$DB6g2" "$DB6g3" "$DB6g4" "$DB6g5" "$DB6g6" "$DB6g7" "$DB6g8" "$DB6g9" "$DB6g10" \
  "$DB6g1u" "$DB6g1v" "$DB6g1w" "$DB7d" "$DB7e" "$DB7f"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
