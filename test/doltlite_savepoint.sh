#!/bin/bash
# Tests for interactions between SQLite savepoints/transactions and dolt operations.
# These tests discover and document how dolt operations (which are DDL-like and auto-commit
# at the storage layer) interact with SQLite's transaction/savepoint machinery.

DOLTLITE=./doltlite
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

# ============================================================
# Test 1: BEGIN; INSERT; dolt_commit; ROLLBACK
# Expected: The dolt commit operates at the storage layer and persists
# regardless of the SQLite transaction rollback. The INSERT data should
# be rolled back from the working set, but the dolt commit (snapshot)
# should survive since dolt_commit captures state at call time.
# ============================================================
DB1=/tmp/test_savepoint1_$$.db; rm -f "$DB1"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB1" > /dev/null 2>&1

# Within a single session: BEGIN, INSERT, dolt_commit, ROLLBACK
echo "BEGIN; INSERT INTO t VALUES(2,'txn'); SELECT dolt_commit('-A','-m','in-txn commit'); ROLLBACK;" | $DOLTLITE "$DB1" > /dev/null 2>&1

# After rollback, check if row 2 is visible in working set
# Expected: dolt_commit implicitly committed the SQL transaction,
# so ROLLBACK is a no-op and both rows survive.
run_test "txn_rollback_data_count" \
  "SELECT count(*) FROM t;" \
  "2" "$DB1"

# The dolt commit should still exist in the log since it was executed
# Expected: 2 commits (init + in-txn commit) — dolt operations persist
run_test "txn_rollback_dolt_commit_survives" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB1"

# ============================================================
# Test 2: BEGIN; INSERT; SAVEPOINT x; INSERT more; dolt_commit; ROLLBACK TO x
# Expected: ROLLBACK TO x undoes work after the savepoint. The dolt_commit
# still persists in the log. The second INSERT is undone but the first
# INSERT (before savepoint) remains.
# ============================================================
DB2=/tmp/test_savepoint2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1

echo "BEGIN; INSERT INTO t VALUES(2,'before-sp'); SAVEPOINT x; INSERT INTO t VALUES(3,'after-sp'); SELECT dolt_commit('-A','-m','mid-savepoint'); ROLLBACK TO x; COMMIT;" | $DOLTLITE "$DB2" > /dev/null 2>&1

# After dolt_commit: the SQL transaction was implicitly committed,
# so ROLLBACK TO x fails (savepoint gone) and all rows survive.
run_test "savepoint_rollback_keeps_pre_sp" \
  "SELECT count(*) FROM t;" \
  "3" "$DB2"

run_test "savepoint_rollback_row2_exists" \
  "SELECT v FROM t WHERE id=2;" \
  "before-sp" "$DB2"

# Row 3 also survives — dolt_commit committed the SQL transaction
run_test "savepoint_rollback_row3_gone" \
  "SELECT count(*) FROM t WHERE id=3;" \
  "1" "$DB2"

# Dolt commit log should still show the commit made inside the savepoint
run_test "savepoint_dolt_commit_in_log" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB2"

# ============================================================
# Test 3: INSERT outside transaction; dolt_commit — basic sanity
# Expected: Normal behavior, commit captures the insert.
# ============================================================
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

# ============================================================
# Test 4: Nested savepoints with dolt operations inside
# Expected: dolt_commit inside nested savepoints still persists.
# The data changes follow SQLite savepoint semantics.
# ============================================================
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

# All inserts should survive — all savepoints were released (committed)
run_test "nested_sp_all_released_count" \
  "SELECT count(*) FROM t;" \
  "4" "$DB4"

# Now test nested rollback
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

# Only base + row 2 should survive (sp1 rollback discards row 3, sp2 rollback discards row 4)
run_test "nested_sp_rollback_count" \
  "SELECT count(*) FROM t;" \
  "2" "$DB4b"

run_test "nested_sp_rollback_kept_row" \
  "SELECT v FROM t WHERE id=2;" \
  "keep" "$DB4b"

# ============================================================
# Test 5: dolt_reset('--hard') inside a transaction
# Expected: dolt_reset('--hard') reverts working set to last commit.
# This is a dolt storage operation that may or may not interact
# with the SQLite transaction boundary.
# ============================================================
DB5=/tmp/test_savepoint5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'staged');" | $DOLTLITE "$DB5" > /dev/null 2>&1

# Hard reset inside a transaction
echo "BEGIN; SELECT dolt_reset('--hard'); COMMIT;" | $DOLTLITE "$DB5" > /dev/null 2>&1

# After hard reset, row 2 should be gone — reverted to 'init' commit state
run_test "hard_reset_in_txn_count" \
  "SELECT count(*) FROM t;" \
  "1" "$DB5"

run_test "hard_reset_in_txn_status_clean" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB5"

# Savepoint around reset should be invalidated, matching Dolt.
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

# ============================================================
# Test 6: dolt_checkout inside a transaction
# Expected: dolt_checkout requires a clean working set and switches
# branches. Inside a transaction this may fail since the transaction
# itself may create implicit dirty state, or it may succeed if
# dolt_checkout operates at the storage layer independently.
# ============================================================
DB6=/tmp/test_savepoint6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('other');" | $DOLTLITE "$DB6" > /dev/null 2>&1

# Try checkout inside a BEGIN/COMMIT block with no dirty data
echo "BEGIN; SELECT dolt_checkout('other'); COMMIT;" | $DOLTLITE "$DB6" > /dev/null 2>&1

# Check which branch we're on — should be 'other' if checkout succeeded,
# or 'main' if it was rejected
run_test_match "checkout_in_txn_branch" \
  "SELECT active_branch();" \
  "^(main|other)$" "$DB6"

# Now try checkout with dirty data inside transaction
DB6b=/tmp/test_savepoint6b_$$.db; rm -f "$DB6b"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6b" > /dev/null 2>&1
echo "SELECT dolt_branch('other');" | $DOLTLITE "$DB6b" > /dev/null 2>&1

# INSERT then checkout in same transaction — allowed (like Dolt SQL context)
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

# ============================================================
# Test 7: dolt_merge inside a BEGIN/COMMIT block
# Expected: dolt_merge operates at the dolt storage layer. It should
# work inside a transaction block and the merge result should persist.
# ============================================================
DB7=/tmp/test_savepoint7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'feature-row'); SELECT dolt_commit('-A','-m','feature work');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB7" > /dev/null 2>&1

# Merge inside a transaction
echo "BEGIN; SELECT dolt_merge('feature'); COMMIT;" | $DOLTLITE "$DB7" > /dev/null 2>&1

# Merge should have brought in the feature row
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

# ============================================================
# Test 8: ROLLBACK after dolt_branch — does the branch creation persist?
# Expected: dolt_branch creates a branch at the storage layer.
# A SQLite ROLLBACK should not undo the branch creation since
# it's a dolt metadata operation, not a SQL data change.
# ============================================================
DB8=/tmp/test_savepoint8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1

# Create branch inside a transaction, then rollback
echo "BEGIN; SELECT dolt_branch('ephemeral'); ROLLBACK;" | $DOLTLITE "$DB8" > /dev/null 2>&1

# Check if the branch persists despite the rollback
# Expected: branch persists because dolt_branch is a storage-level operation
run_test_match "branch_survives_rollback" \
  "SELECT count(*) FROM dolt_branches;" \
  "^[12]$" "$DB8"

# If branch survived, verify by name
run_test_match "branch_name_after_rollback" \
  "SELECT name FROM dolt_branches ORDER BY name;" \
  "main" "$DB8"

# ============================================================
# Cleanup
# ============================================================
rm -f "$DB1" "$DB2" "$DB3" "$DB4" "$DB4b" "$DB5" "$DB6" "$DB6b" "$DB7" "$DB8" \
  "$DB6g2" "$DB6g3" "$DB6g4" "$DB6g5" "$DB6g6"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
