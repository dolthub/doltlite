#!/bin/bash
# dsCountRows sums INTERNAL subtree counts; tables >800 rows so the tree is multi-level.

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r
  r=$(printf '%s\n' "$s" | "$DOLTLITE" "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

DB=/tmp/test_diff_stat_scale_$$.db
rm -f "$DB" "${DB}-wal"

printf '%s\n' "
CREATE TABLE other(x INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c0');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(id, v)
  WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<2000)
  SELECT i, 'v'||i FROM c;
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t(id, v)
  WITH RECURSIVE c(i) AS (SELECT 2001 UNION ALL SELECT i+1 FROM c WHERE i<2500)
  SELECT i, 'v'||i FROM c;
SELECT dolt_commit('-A','-m','c2');
" | "$DOLTLITE" "$DB" > /dev/null 2>&1

run_test "scale_row_count_live" \
  "SELECT count(*) FROM t;" "2500" "$DB"

run_test "scale_whole_table_add_rows_added" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD~2','HEAD^','t');" "2000" "$DB"
run_test "scale_whole_table_add_new_row_count" \
  "SELECT new_row_count FROM dolt_diff_stat('HEAD~2','HEAD^','t');" "2000" "$DB"
run_test "scale_whole_table_add_old_row_count" \
  "SELECT old_row_count FROM dolt_diff_stat('HEAD~2','HEAD^','t');" "0" "$DB"

# Both-sided: old/new_row_count and rows_unmodified from dsCountRows; rows_added from the diff iterator.
run_test "scale_incremental_old_row_count" \
  "SELECT old_row_count FROM dolt_diff_stat('HEAD^','HEAD','t');" "2000" "$DB"
run_test "scale_incremental_new_row_count" \
  "SELECT new_row_count FROM dolt_diff_stat('HEAD^','HEAD','t');" "2500" "$DB"
run_test "scale_incremental_rows_added" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD^','HEAD','t');" "500" "$DB"
run_test "scale_incremental_rows_unmodified" \
  "SELECT rows_unmodified FROM dolt_diff_stat('HEAD^','HEAD','t');" "2000" "$DB"

rm -f "$DB" "${DB}-wal"

echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ "$FAIL" -ne 0 ]; then
  printf '%b\n' "$ERRORS"
  exit 1
fi
