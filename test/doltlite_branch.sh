#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Branch Tests (Per-Session) ==="
echo ""
DB=/tmp/test_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "default_branch" "SELECT active_branch();" "main" "$DB"
run_test "create_branch" "SELECT dolt_branch('feature');" "0" "$DB"
run_test "list_branches" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test "main_current" "SELECT is_current FROM dolt_branches WHERE name='main';" "1" "$DB"

run_test "checkout_feature" "SELECT dolt_checkout('feature');" "0" "$DB"
run_test "active_feature" "SELECT active_branch();" "feature" "$DB"

echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','on feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "data_on_feature" "SELECT count(*) FROM t;" "2" "$DB"

run_test "checkout_main" "SELECT dolt_checkout('main');" "0" "$DB"
run_test "main_one_row" "SELECT count(*) FROM t;" "1" "$DB"

echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "feature_two_rows" "SELECT count(*) FROM t;" "2" "$DB"

run_test_match "dup_branch" "SELECT dolt_branch('feature');" "already exists" "$DB"
run_test_match "del_current" "SELECT dolt_branch('-d','feature');" "cannot delete" "$DB"
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "delete_branch" "SELECT dolt_branch('-d','feature');" "0" "$DB"
run_test "one_branch" "SELECT count(*) FROM dolt_branches;" "1" "$DB"
run_test_match "checkout_gone" "SELECT dolt_checkout('feature');" "not found" "$DB"

DB2=/tmp/test_branch2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(x);" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test_match "branch_no_commit" "SELECT dolt_branch('foo');" "no commits" "$DB2"

DB3=/tmp/test_branch3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','i');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('b2');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test_match "dirty_checkout" "SELECT dolt_checkout('b2');" "uncommitted" "$DB3"

# --- Checkout after hard reset (issue #107) ---
DB4=/tmp/test_branch4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1

# Make changes, stage, hard reset, then checkout should work
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_add('-A'); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test "checkout_after_hard_reset" "SELECT dolt_checkout('feat');" "0" "$DB4"
run_test "active_after_hard_reset" "SELECT active_branch();" "feat" "$DB4"

# Checkout back should also work (clean state)
run_test "checkout_back_after_hard_reset" "SELECT dolt_checkout('main');" "0" "$DB4"

# Hard reset with no prior staging
DB5=/tmp/test_branch5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_branch('b2');" | $DOLTLITE "$DB5" > /dev/null 2>&1
run_test "checkout_after_hard_reset_no_stage" "SELECT dolt_checkout('b2');" "0" "$DB5"

# Multiple hard resets then checkout
DB6=/tmp/test_branch6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('b3');" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test "checkout_after_multi_hard_reset" "SELECT dolt_checkout('b3');" "0" "$DB6"

# Verify dirty check still works after hard reset + new changes
DB7=/tmp/test_branch7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(99);" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('b4');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test_match "dirty_after_hard_reset_new_changes" "SELECT dolt_checkout('b4');" "uncommitted" "$DB7"

# Schema change (CREATE TABLE) then hard reset then checkout
DB8=/tmp/test_branch8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "CREATE TABLE extra(y); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('b5');" | $DOLTLITE "$DB8" > /dev/null 2>&1
run_test "checkout_after_schema_change_hard_reset" "SELECT dolt_checkout('b5');" "0" "$DB8"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
