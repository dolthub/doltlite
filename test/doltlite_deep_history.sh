#!/bin/bash
DLTEST_TIMEOUT=30
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Deep Commit History Stress Tests ==="
echo ""

DB=/tmp/test_deep_history_$$.db
rm -f "$DB"

echo "Building 500 commits..."

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

echo "Test 1: dolt_log count..."
run_test "log_count_500" \
  "SELECT count(*) FROM dolt_log;" \
  "501" "$DB"

echo "Test 2: dolt_diff first-to-last..."
run_test "diff_first_last_count" \
  "SELECT rows_added + rows_modified + rows_deleted FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 499), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 't');" \
  "500" "$DB"

run_test_match "diff_first_last_has_added" \
  "SELECT rows_added FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 499), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 't');" \
  "^499$" "$DB"

run_test_match "diff_first_last_has_modified" \
  "SELECT rows_modified FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 499), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 't');" \
  "^1$" "$DB"

echo "Test 3: dolt_history for tracker row..."
run_test "history_tracker_row_count" \
  "SELECT count(*) FROM dolt_history_t WHERE id=0;" \
  "500" "$DB"

run_test "history_distinct_commits" \
  "SELECT count(DISTINCT commit_hash) FROM dolt_history_t WHERE id=0;" \
  "500" "$DB"

echo "Test 4: branch, diverge, merge, merge_base..."

COMMIT_250=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 249;" | $DOLTLITE "$DB" 2>/dev/null)

echo "SELECT dolt_branch('deep_branch');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(1000,'main_extra'); SELECT dolt_commit('-A','-m','main_after_branch');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('deep_branch');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2000,'branch_extra'); SELECT dolt_commit('-A','-m','branch_change');" | $DOLTLITE "$DB/deep_branch" > /dev/null 2>&1

MAIN_HEAD=$(echo "SELECT hash FROM dolt_branches WHERE name='main';" | $DOLTLITE "$DB" 2>/dev/null)
BRANCH_HEAD=$(echo "SELECT hash FROM dolt_branches WHERE name='deep_branch';" | $DOLTLITE "$DB" 2>/dev/null)

run_test_match "merge_base_valid_hash" \
  "SELECT dolt_merge_base('$MAIN_HEAD','$BRANCH_HEAD');" \
  "^[0-9a-f]{40}$" "$DB"

EXPECTED_BASE=$(echo "SELECT commit_hash FROM dolt_log WHERE message='commit_499' LIMIT 1;" | $DOLTLITE "$DB" 2>/dev/null)
run_test "merge_base_is_branch_point" \
  "SELECT dolt_merge_base('$MAIN_HEAD','$BRANCH_HEAD');" \
  "$EXPECTED_BASE" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "merge_deep_branch" \
  "SELECT dolt_merge('deep_branch');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "merge_has_main_row" \
  "SELECT val FROM t WHERE id=1000;" \
  "main_extra" "$DB"

run_test "merge_has_branch_row" \
  "SELECT val FROM t WHERE id=2000;" \
  "branch_extra" "$DB"

echo "Test 5: dolt_at for early commit..."

run_test "at_commit10_count" \
  "SELECT count(*) FROM dolt_at_t((SELECT commit_hash FROM dolt_log WHERE message='commit_10' LIMIT 1));" \
  "11" "$DB"

run_test "at_commit10_tracker_val" \
  "SELECT val FROM dolt_at_t((SELECT commit_hash FROM dolt_log WHERE message='commit_10' LIMIT 1)) WHERE id=0;" \
  "v10" "$DB"

echo "Test 6: dolt_diff_t audit log..."
run_test "diff_table_total_entries" \
  "SELECT count(*) FROM dolt_diff_t;" \
  "1002" "$DB"

run_test_match "diff_table_has_added" \
  "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" \
  "^503$" "$DB"

run_test_match "diff_table_has_modified" \
  "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" \
  "^499$" "$DB"

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

rm -f "$DB"

dltest_finish
