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
run_test "ff_merge_msg" "SELECT message FROM dolt_log LIMIT 1;" "Merge branch 'feature'" "$DB2"
run_test "ff_log_count" "SELECT count(*) FROM dolt_log;" "3" "$DB2"

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
run_test_match "conflict" "SELECT dolt_merge('feature');" "conflict" "$DB3"
run_test "conflict_ours_preserved" "SELECT v FROM t;" "main" "$DB3"

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

# Test 8: dolt_diff after three-way merge (DB from Test 1)
# The merge commit is HEAD (offset 0), ancestor is 2 commits back (offset 2 = "initial")
# Between ancestor and merged result, users table should show 'Alice'->'ALICE', orders should show added row
run_test_match "diff_3way_users" \
  "SELECT diff_type, from_value, to_value FROM dolt_diff('users', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0));" \
  "modified" "$DB"
run_test_match "diff_3way_orders" \
  "SELECT diff_type FROM dolt_diff('orders', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0));" \
  "added" "$DB"

# Test 9: dolt_diff after fast-forward merge (DB2 from Test 2)
# Log now has 3 entries: merge commit (0), feature (1), init (2)
# Compare init (offset 2) to merge commit (offset 0) to see the added row
run_test_match "diff_ff_added" \
  "SELECT diff_type, to_value FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0));" \
  "added" "$DB2"

# Test 10: After merge with conflicts, dolt_diff between init and HEAD shows the main change
# Conflicted merges don't auto-commit, so HEAD is the 'main' commit (offset 0), init is offset 1
run_test_match "diff_conflict_shows_change" \
  "SELECT diff_type FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0));" \
  "modified" "$DB3"

# Test 11: Row-level auto-merge (both modify same table, different rows)
DB6=/tmp/test_merge6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "UPDATE t SET v='MAIN' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "UPDATE t SET v='FEAT' WHERE id=3; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test_match "row_merge_succeeds" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB6"
run_test "row_merge_row1" "SELECT v FROM t WHERE id=1;" "MAIN" "$DB6"
run_test "row_merge_row2" "SELECT v FROM t WHERE id=2;" "b" "$DB6"
run_test "row_merge_row3" "SELECT v FROM t WHERE id=3;" "FEAT" "$DB6"

# Test 12: Row-level conflict (same row modified on both branches)
DB7=/tmp/test_merge7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'orig'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "UPDATE t SET v='main-val' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "UPDATE t SET v='feat-val' WHERE id=1; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test_match "row_conflict_detected" "SELECT dolt_merge('feature');" "conflict" "$DB7"
run_test "row_conflict_ours_kept" "SELECT v FROM t WHERE id=1;" "main-val" "$DB7"

# Test 13: Mixed — some rows conflict, others auto-merge
DB8=/tmp/test_merge8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "UPDATE t SET v='main1' WHERE id=1; INSERT INTO t VALUES(3,'main3'); SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "UPDATE t SET v='feat1' WHERE id=1; INSERT INTO t VALUES(4,'feat4'); SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB8" > /dev/null 2>&1
run_test_match "mixed_merge" "SELECT dolt_merge('feature');" "conflict" "$DB8"
run_test "mixed_row2_unchanged" "SELECT v FROM t WHERE id=2;" "b" "$DB8"
run_test "mixed_row3_from_main" "SELECT v FROM t WHERE id=3;" "main3" "$DB8"
run_test "mixed_row4_from_feat" "SELECT v FROM t WHERE id=4;" "feat4" "$DB8"

# --- dolt_merge('--abort') ---
DB9=/tmp/test_merge9_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB9" > /dev/null 2>&1
echo "SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET v='OTHER'; SELECT dolt_commit('-A','-m','other');" | $DOLTLITE "$DB9" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET v='MAIN'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB9" > /dev/null 2>&1

# Conflicted merge — no auto-commit
run_test_match "abort_merge_has_conflict" "SELECT dolt_merge('other');" "conflict" "$DB9"
run_test "abort_conflicts_exist" "SELECT count(*) FROM dolt_conflicts;" "1" "$DB9"

# Abort restores to pre-merge state
run_test "abort_succeeds" "SELECT dolt_merge('--abort');" "0" "$DB9"
run_test "abort_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB9"
run_test "abort_data_restored" "SELECT v FROM t WHERE id=1;" "MAIN" "$DB9"

# Abort without merge in progress
run_test_match "abort_no_merge" "SELECT dolt_merge('--abort');" "no merge in progress" "$DB9"

# Clean merge still auto-commits
DB10=/tmp/test_merge10_$$.db; rm -f "$DB10"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB10" > /dev/null 2>&1

run_test_match "clean_merge_commits" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB10"
run_test_match "clean_merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB10"
run_test "clean_merge_data" "SELECT count(*) FROM t;" "2" "$DB10"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
