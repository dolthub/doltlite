#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Merge Tests ==="
echo ""

# Test 1: Three-way merge, different tables modified
DB=/tmp/test_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT); INSERT INTO users VALUES(1,'Alice'); INSERT INTO orders VALUES(1,'hat'); SELECT dolt_commit('-A','-m','initial');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE users SET name='ALICE' WHERE id=1; SELECT dolt_commit('-A','-m','main updates');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO orders VALUES(2,'coat'); SELECT dolt_commit('-A','-m','feature adds');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "merge_hash" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_users" "SELECT name FROM users;" "ALICE" "$DB"
run_test "merge_orders" "SELECT count(*) FROM orders;" "2" "$DB"
run_test "merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge branch 'feature'" "$DB"
run_test "merge_log_count" "SELECT count(*) FROM dolt_log;" "3" "$DB"

# Test 2: Fast-forward merge
DB2=/tmp/test_merge2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "ff_before" "SELECT count(*) FROM t;" "1" "$DB2"
run_test_match "ff_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB2"
run_test "ff_after" "SELECT count(*) FROM t;" "2" "$DB2"

# Test 3: Already up to date
run_test "up_to_date" "SELECT dolt_merge('feature');" "Already up to date" "$DB2"

# Test 4: Conflict (both modify same table)
DB3=/tmp/test_merge3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='main'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='feat'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test_match "conflict" "SELECT dolt_merge('feature');" "merge conflict" "$DB3"
run_test "conflict_unchanged" "SELECT v FROM t;" "main" "$DB3"

# Test 5: Nonexistent branch
run_test_match "no_branch" "SELECT dolt_merge('nope');" "not found" "$DB3"

# Test 6: Feature adds new table (only feature creates)
DB4=/tmp/test_merge4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "CREATE TABLE new_t(y); INSERT INTO new_t VALUES(42); SELECT dolt_commit('-A','-m','add table');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test_match "new_table_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB4"
run_test "new_table_visible" "SELECT y FROM new_t;" "42" "$DB4"
run_test "original_intact" "SELECT x FROM t;" "1" "$DB4"

# Test 7: Feature deletes row
DB5=/tmp/test_merge5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "DELETE FROM t WHERE id=2; SELECT dolt_commit('-A','-m','del');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB5" > /dev/null 2>&1
run_test "pre_merge_rows" "SELECT count(*) FROM t;" "2" "$DB5"
run_test_match "merge_del" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB5"
run_test "post_merge_rows" "SELECT count(*) FROM t;" "1" "$DB5"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
