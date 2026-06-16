#!/bin/bash
# dolt_diff_stat row counts on a multi-level prolly tree.
#
# dsCountRows feeds old_row_count / new_row_count / rows_unmodified and,
# on a whole-table add or drop, rows_added / rows_deleted. It counts a
# tree by summing per-node subtree counts, so the count is only correct
# if the summation across INTERNAL nodes is right. Small tables are a
# single leaf and never exercise that path; these tables are >800 rows,
# which the chunker splits into a multi-level tree.

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

# c0: t does not exist yet. c1: t with 2000 rows. c2: +500 rows (2500).
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

# Sanity: the tree really holds the rows we think it does.
run_test "scale_row_count_live" \
  "SELECT count(*) FROM t;" "2500" "$DB"

# Whole-table add (c0 -> c1): rows_added and new_row_count come from
# dsCountRows counting the whole new tree; old side is absent.
run_test "scale_whole_table_add_rows_added" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD~2','HEAD^','t');" "2000" "$DB"
run_test "scale_whole_table_add_new_row_count" \
  "SELECT new_row_count FROM dolt_diff_stat('HEAD~2','HEAD^','t');" "2000" "$DB"
run_test "scale_whole_table_add_old_row_count" \
  "SELECT old_row_count FROM dolt_diff_stat('HEAD~2','HEAD^','t');" "0" "$DB"

# Both-sided diff (c1 -> c2): old/new_row_count and rows_unmodified are
# dsCountRows-derived; rows_added is from the diff iterator.
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
