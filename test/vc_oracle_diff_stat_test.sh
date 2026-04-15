#!/bin/bash
#
# Version-control oracle test: dolt_diff_stat and dolt_diff_summary
#
# Compares doltlite's two new TVFs against Dolt 1.86.0+ across a
# range of scenarios:
#
#   dolt_diff_stat(from, to [, table])
#     table_name, rows_unmodified, rows_added, rows_deleted,
#     rows_modified, cells_added, cells_deleted, cells_modified,
#     old_row_count, new_row_count, old_cell_count, new_cell_count
#
#   dolt_diff_summary(from, to [, table])
#     from_table_name, to_table_name, diff_type, data_change,
#     schema_change
#
# Both TVFs take (from_ref, to_ref) with an optional third argument
# to filter to a single table. Refs resolve via the same rules as
# dolt_log (hash, branch name, HEAD~N, etc.).
#
# Normalization: Dolt emits data_change/schema_change as 'true'/'false';
# doltlite emits 1/0. Both are mapped to 0/1 for comparison. Rows are
# sorted for order-independence.
#
# Usage: bash vc_oracle_diff_stat_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize_stat() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 12 && $1 == "S" { print }' \
    | sort
}

normalize_summary() {
  tr -d '\r' \
    | sed -e 's/	true$/	1/' -e 's/	true	/	1	/g' \
          -e 's/	false$/	0/' -e 's/	false	/	0	/g' \
    | awk -F'\t' 'NF >= 5 && $1 == "M" { print }' \
    | sort
}

# Oracle for dolt_diff_stat over a commit range. The setup creates
# whatever history is needed; the query is run in a single engine
# invocation with the setup so refs like HEAD~N resolve correctly.
oracle_stat() {
  local name="$1" setup="$2" from="$3" to="$4" tbl="${5:-}"
  local dir="$TMPROOT/${name}_stat"
  mkdir -p "$dir/dl" "$dir/dt"

  local args="'$from','$to'"
  if [ -n "$tbl" ]; then args="'$from','$to','$tbl'"; fi

  local q="SELECT 'S' || char(9) || table_name || char(9) || rows_unmodified || char(9) || rows_added || char(9) || rows_deleted || char(9) || rows_modified || char(9) || cells_added || char(9) || cells_deleted || char(9) || cells_modified || char(9) || old_row_count || char(9) || new_row_count || char(9) || old_cell_count || char(9) || new_cell_count FROM dolt_diff_stat($args) ORDER BY table_name"
  local q_dolt="SELECT concat('S', char(9), table_name, char(9), rows_unmodified, char(9), rows_added, char(9), rows_deleted, char(9), rows_modified, char(9), cells_added, char(9), cells_deleted, char(9), cells_modified, char(9), old_row_count, char(9), new_row_count, char(9), old_cell_count, char(9), new_cell_count) FROM dolt_diff_stat($args) ORDER BY table_name"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize_stat)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_stat
  )

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES ${name}_stat"
    echo "  FAIL: ${name}_stat"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

# Oracle for dolt_diff_summary.
oracle_summary() {
  local name="$1" setup="$2" from="$3" to="$4" tbl="${5:-}"
  local dir="$TMPROOT/${name}_summary"
  mkdir -p "$dir/dl" "$dir/dt"

  local args="'$from','$to'"
  if [ -n "$tbl" ]; then args="'$from','$to','$tbl'"; fi

  local q="SELECT 'M' || char(9) || from_table_name || char(9) || to_table_name || char(9) || diff_type || char(9) || data_change || char(9) || schema_change FROM dolt_diff_summary($args) ORDER BY from_table_name, to_table_name"
  local q_dolt="SELECT concat('M', char(9), from_table_name, char(9), to_table_name, char(9), diff_type, char(9), data_change, char(9), schema_change) FROM dolt_diff_summary($args) ORDER BY from_table_name, to_table_name"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize_summary)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_summary
  )

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES ${name}_summary"
    echo "  FAIL: ${name}_summary"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

# Convenience: run BOTH stat and summary oracles against the same
# setup and commit range.
oracle_both() {
  oracle_stat    "$@"
  oracle_summary "$@"
}

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT, name VARCHAR(32));
INSERT INTO t VALUES(1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'carol');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
"

echo "=== Version Control Oracle Tests: dolt_diff_stat / dolt_diff_summary ==="
echo ""

echo "--- no changes ---"

oracle_both "no_changes" "$SEED" "HEAD" "HEAD"

echo "--- single row modify ---"

oracle_both "modify_one_cell" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "modify_two_cells_same_row" "
$SEED
UPDATE t SET v = 99, name = 'ALICE' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- insert / delete ---"

oracle_both "insert_one_row" "
$SEED
INSERT INTO t VALUES(4, 40, 'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "delete_one_row" "
$SEED
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "insert_delete_modify_mixed" "
$SEED
INSERT INTO t VALUES(4, 40, 'dave');
DELETE FROM t WHERE id = 3;
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- table creation / drop ---"

oracle_both "create_table_empty" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
" "HEAD~1" "HEAD"

oracle_both "create_table_with_rows" "
$SEED
" "HEAD~1" "HEAD"

oracle_both "drop_table_empty" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "drop_table_with_rows" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- schema change: ADD COLUMN ---"

oracle_both "add_column_no_data_change" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "add_column_plus_update" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
UPDATE t SET extra = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- multi-table ---"

oracle_both "two_tables_one_modified" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, x VARCHAR(32));
INSERT INTO t VALUES(1, 10);
INSERT INTO u VALUES(1, 'alice');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "two_tables_both_modified" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, x VARCHAR(32));
INSERT INTO t VALUES(1, 10);
INSERT INTO u VALUES(1, 'alice');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 99 WHERE id = 1;
INSERT INTO u VALUES(2, 'bob');
DELETE FROM u WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- table filter ---"

oracle_both "filter_to_one_table" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, x VARCHAR(32));
INSERT INTO t VALUES(1, 10);
INSERT INTO u VALUES(1, 'alice');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 99 WHERE id = 1;
UPDATE u SET x = 'ALICE' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD" "t"

echo "--- ranges spanning multiple commits ---"

oracle_both "range_three_commits" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES(4, 40, 'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
" "HEAD~3" "HEAD"

echo "--- branch refs ---"

oracle_both "diff_main_to_feature_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(4, 40, 'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
" "main" "feature"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
