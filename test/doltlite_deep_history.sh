#!/bin/bash
#
# Stress test for deep commit history (500 commits).
# Tests dolt_log, dolt_diff, dolt_history_<table>, dolt_merge_base,
# dolt_at, dolt_diff_<table>, and performance.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(30); exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(30); exec @ARGV' $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi
}

echo "=== Doltlite Deep Commit History Stress Tests ==="
echo ""

DB=/tmp/test_deep_history_$$.db
rm -f "$DB"

# ============================================================
# Build 500 commits: INSERT a row per commit, UPDATE a tracker row
# ============================================================

echo "Building 500 commits..."

# Create table and initial commit with a tracker row (id=0) updated every commit
SQL="CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);"
SQL="${SQL} INSERT INTO t VALUES(0,'v0');"
SQL="${SQL} SELECT dolt_commit('-A','-m','commit_0');"

for i in $(seq 1 499); do
  SQL="${SQL} INSERT INTO t VALUES($i,'row$i');"
  SQL="${SQL} UPDATE t SET val='v$i' WHERE id=0;"
  SQL="${SQL} SELECT dolt_commit('-A','-m','commit_$i');"
done

echo "$SQL" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "Done building commits."

# ============================================================
# Test 1: dolt_log returns all 500 commits
# ============================================================

echo "Test 1: dolt_log count..."
run_test "log_count_500" \
  "SELECT count(*) FROM dolt_log;" \
  "500" "$DB"

# ============================================================
# Test 2: dolt_diff between first and last commit works
# ============================================================

echo "Test 2: dolt_diff first-to-last..."
# The first commit (oldest) is at the largest offset; last commit is offset 0.
# Between commit_0 and commit_499, 499 rows were added (ids 1..499) and row 0 was modified.
run_test "diff_first_last_count" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 499), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0));" \
  "500" "$DB"

run_test_match "diff_first_last_has_added" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 499), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0)) WHERE diff_type='added';" \
  "^499$" "$DB"

run_test_match "diff_first_last_has_modified" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 499), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0)) WHERE diff_type='modified';" \
  "^1$" "$DB"

# ============================================================
# Test 3: dolt_history_t returns all historical versions of tracker row (id=0)
# Row 0 was present in all 500 commits (inserted at commit_0, updated in commits 1-499)
# ============================================================

echo "Test 3: dolt_history for tracker row..."
run_test "history_tracker_row_count" \
  "SELECT count(*) FROM dolt_history_t WHERE id=0;" \
  "500" "$DB"

# Verify distinct commit hashes in history match 500
run_test "history_distinct_commits" \
  "SELECT count(DISTINCT commit_hash) FROM dolt_history_t WHERE id=0;" \
  "500" "$DB"

# ============================================================
# Test 4: Branch at commit 250, make changes on both, merge, verify merge_base
# ============================================================

echo "Test 4: branch, diverge, merge, merge_base..."

# Get commit hash at position 250 (offset 249 from HEAD since log is newest-first)
COMMIT_250=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 249;" | $DOLTLITE "$DB" 2>/dev/null)

# Create branch at commit 250 by branching, then we make independent changes on each
echo "SELECT dolt_branch('deep_branch');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Make a change on main
echo "INSERT INTO t VALUES(1000,'main_extra'); SELECT dolt_commit('-A','-m','main_after_branch');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Switch to branch, make a change there
echo "SELECT dolt_checkout('deep_branch');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2000,'branch_extra'); SELECT dolt_commit('-A','-m','branch_change');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Get branch heads
MAIN_HEAD=$(echo "SELECT dolt_hashof('main');" | $DOLTLITE "$DB" 2>/dev/null)
BRANCH_HEAD=$(echo "SELECT dolt_hashof('deep_branch');" | $DOLTLITE "$DB" 2>/dev/null)

# Verify merge_base finds the right ancestor (commit_499 on main before the extra commit)
# The branch was created from the tip of main (commit_499), so merge base = commit_499's hash
run_test_match "merge_base_valid_hash" \
  "SELECT dolt_merge_base('$MAIN_HEAD','$BRANCH_HEAD');" \
  "^[0-9a-f]{40}$" "$DB"

# The merge base should equal the commit that was HEAD when we branched (commit_499)
EXPECTED_BASE=$(echo "SELECT commit_hash FROM dolt_log WHERE message='commit_499' LIMIT 1;" | $DOLTLITE "$DB" 2>/dev/null)
run_test "merge_base_is_branch_point" \
  "SELECT dolt_merge_base('$MAIN_HEAD','$BRANCH_HEAD');" \
  "$EXPECTED_BASE" "$DB"

# Now merge
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "merge_deep_branch" \
  "SELECT dolt_merge('deep_branch');" \
  "^[0-9a-f]{40}$" "$DB"

# Both rows should be present after merge
run_test "merge_has_main_row" \
  "SELECT val FROM t WHERE id=1000;" \
  "main_extra" "$DB"

run_test "merge_has_branch_row" \
  "SELECT val FROM t WHERE id=2000;" \
  "branch_extra" "$DB"

# ============================================================
# Test 5: dolt_at for an early commit (commit #10, offset 492 from current HEAD)
# At commit_10 the table should have rows 0..10 = 11 rows
# ============================================================

echo "Test 5: dolt_at for early commit..."

# After the merge we have 502 log entries (500 original + main_after_branch + merge)
# commit_10 is at offset 491 from HEAD (502 - 11 = 491)
run_test "at_commit10_count" \
  "SELECT count(*) FROM dolt_at_t((SELECT commit_hash FROM dolt_log WHERE message='commit_10' LIMIT 1));" \
  "11" "$DB"

run_test "at_commit10_tracker_val" \
  "SELECT val FROM dolt_at_t((SELECT commit_hash FROM dolt_log WHERE message='commit_10' LIMIT 1)) WHERE id=0;" \
  "v10" "$DB"

# ============================================================
# Test 6: dolt_diff_t audit log returns correct entries for full history
# Each of the 500 commits produced changes:
#   commit_0: 2 inserts (id=0 tracker + but actually just 1 insert of id=0 with val v0... let's count)
#   Actually: commit_0 inserts row 0. commits 1-499 each insert a row + update row 0 = 2 changes each.
#   Total = 1 (commit_0: insert id=0) + 499*2 (insert + update) = 999
#   Plus main_after_branch: 1 insert = 1000
#   Plus merge commit brings in branch_extra: 1 insert = 1001
# ============================================================

echo "Test 6: dolt_diff_t audit log..."
run_test "diff_table_total_entries" \
  "SELECT count(*) FROM dolt_diff_t;" \
  "1001" "$DB"

# Verify we have both added and modified types
run_test_match "diff_table_has_added" \
  "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" \
  "^502$" "$DB"

run_test_match "diff_table_has_modified" \
  "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" \
  "^499$" "$DB"

# ============================================================
# Test 7: Performance — dolt_log query for 500 commits under 5 seconds
# ============================================================

echo "Test 7: Performance sanity check..."
START=$(perl -e 'use Time::HiRes qw(time); print time')
LOG_COUNT=$(echo "SELECT count(*) FROM dolt_log;" | perl -e 'alarm(5); exec @ARGV' $DOLTLITE "$DB" 2>&1)
END=$(perl -e 'use Time::HiRes qw(time); print time')
ELAPSED=$(perl -e "printf '%.2f', $END - $START")

if [ "$LOG_COUNT" -gt 0 ] 2>/dev/null && [ "$(echo "$ELAPSED < 5" | bc)" = "1" ]; then
  PASS=$((PASS+1))
  echo "  log query completed in ${ELAPSED}s"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: perf_log_under_5s\n  expected: count>0 in <5s\n  got:      count=$LOG_COUNT in ${ELAPSED}s"
fi

# ============================================================
# Cleanup
# ============================================================

rm -f "$DB"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
