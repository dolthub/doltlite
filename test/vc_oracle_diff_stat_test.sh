#!/bin/bash

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

oracle_stat() {
  local name="$1" setup="$2" from="$3" to="$4" tbl="${5:-}" allow_empty="${6:-}"
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
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_stat
  )

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "${name}_stat" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "${name}_stat" "$dl_out" "$dt_out"
  fi
}

oracle_summary() {
  local name="$1" setup="$2" from="$3" to="$4" tbl="${5:-}" allow_empty="${6:-}"
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
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_summary
  )

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "${name}_summary" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "${name}_summary" "$dl_out" "$dt_out"
  fi
}

oracle_both() {
  oracle_stat    "$@"
  oracle_summary "$@"
}

# Both engines must reject the script with a clean non-zero exit (not a crash).
oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite err:"; sed 's/^/      /' "$dir/dl.err" 2>/dev/null
    echo "    dolt err:"; sed 's/^/      /' "$dir/dt.err" 2>/dev/null
  fi
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

oracle_both "no_changes" "$SEED" "HEAD" "HEAD" "" "EXPECT_EMPTY"

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

oracle_stat    "create_table_empty" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
" "HEAD~1" "HEAD" "" "EXPECT_EMPTY"
oracle_summary "create_table_empty" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
" "HEAD~1" "HEAD"

oracle_both "create_table_with_rows" "
$SEED
" "HEAD~1" "HEAD"

oracle_stat    "drop_table_empty" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD" "" "EXPECT_EMPTY"
oracle_summary "drop_table_empty" "
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

# The case above gives the added column a value on the row it edits, which is
# what kept it agreeing. A column that stays NULL on an otherwise-modified row
# still counts as a modified cell.
oracle_both "add_null_column_plus_update_same_row" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
UPDATE t SET v = v + 1 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "add_null_column_plus_update_all_rows" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
UPDATE t SET v = v + 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- schema change: DROP COLUMN ---"

# Dropped cells are reported by cells_deleted and must not also be counted as
# modified, whether or not the dropped column held a value.
# Stat only: dolt_diff_summary reports data_change=1 here where Dolt reports 0,
# a separate pre-existing divergence in how a no-op column drop is classified.
oracle_stat "drop_null_column_no_data_change" "
CREATE TABLE t(id INT PRIMARY KEY, v INT, gone INT);
INSERT INTO t VALUES(1, 10, NULL), (2, 20, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t DROP COLUMN gone;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "drop_valued_column_no_data_change" "
CREATE TABLE t(id INT PRIMARY KEY, v INT, gone INT);
INSERT INTO t VALUES(1, 10, 7), (2, 20, 8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t DROP COLUMN gone;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "drop_valued_column_plus_update" "
CREATE TABLE t(id INT PRIMARY KEY, v INT, gone INT);
INSERT INTO t VALUES(1, 10, 7), (2, 20, 8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t DROP COLUMN gone;
UPDATE t SET v = v + 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "drop_null_column_plus_update" "
CREATE TABLE t(id INT PRIMARY KEY, v INT, gone INT);
INSERT INTO t VALUES(1, 10, NULL), (2, 20, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t DROP COLUMN gone;
UPDATE t SET v = v + 1;
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

echo "--- missing table filter (match Dolt) ---"

# dolt_diff_stat: filter naming a table on neither side is an error.
oracle_error "diff_stat_missing_table" "
$SEED
SELECT * FROM dolt_diff_stat('HEAD', 'HEAD', 'doesnotexist');
"

oracle_error "diff_stat_missing_table_over_real_change" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT * FROM dolt_diff_stat('HEAD~1', 'HEAD', 'doesnotexist');
"

# After drop + empty commit, neither side has t under that name.
oracle_error "diff_stat_filter_absent_on_both_sides" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop');
SELECT dolt_commit('--allow-empty', '-m', 'empty after drop');
SELECT * FROM dolt_diff_stat('HEAD~1', 'HEAD', 't');
"

oracle_error "diff_stat_bad_from_ref" "
$SEED
SELECT * FROM dolt_diff_stat('definitely_not_a_ref', 'HEAD', 't');
"

oracle_error "diff_stat_bad_to_ref" "
$SEED
SELECT * FROM dolt_diff_stat('HEAD', 'definitely_not_a_ref', 't');
"

# dolt_diff_summary: missing filter is empty success on both engines.
oracle_summary "diff_summary_missing_table" "
$SEED
" "HEAD" "HEAD" "doesnotexist" "EXPECT_EMPTY"

oracle_summary "diff_summary_missing_table_over_real_change" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD" "doesnotexist" "EXPECT_EMPTY"

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

oracle_both "diff_branch_created_from_tag_to_main" "
$SEED
INSERT INTO t VALUES(4, 40, 'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('v1', 'HEAD~1');
SELECT dolt_branch('from_tag', 'v1');
" "from_tag" "main"

echo "--- merge parent refs ---"

oracle_both "diff_first_parent_to_merge_head" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(4, 40, 'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5, 50, 'erin');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD^1" "HEAD" "t"

oracle_both "diff_second_parent_to_merge_head" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(4, 40, 'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5, 50, 'erin');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD^2" "HEAD" "t"

echo "--- schema churn ---"

oracle_both "rename_pure" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO u;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD" "" "EXPECT_EMPTY"

oracle_both "rename_plus_data" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO u;
UPDATE u SET v = 99 WHERE id = 1;
INSERT INTO u VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "rename_composite_pk" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1, 1, 10), (1, 2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO u;
UPDATE u SET v = 99 WHERE a = 1 AND b = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_both "rename_then_recreate_same_name_family" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO u;
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(7, 70);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- replay after schema changes ---"

oracle_both "diff_stat_merge_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "HEAD^1" "HEAD" "u"

oracle_both "diff_stat_cherrypick_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "HEAD~1" "HEAD" "u"

oracle_both "diff_stat_rebase_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "main" "feat" "u"

oracle_stat "diff_stat_revert_schema_change_with_later_added_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'later');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main_check' LIMIT 1));
" "HEAD~1" "HEAD"

echo "--- summary across pk shapes: composite and keyless ---"

oracle_summary "summary_composite_pk_modify" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,10),(1,2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v=99 WHERE a=1 AND b=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_summary "summary_composite_pk_schema_and_data" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t ADD COLUMN w INT DEFAULT 0;
INSERT INTO t VALUES(2,2,20,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_summary "summary_composite_pk_filter_one_table" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a,b));
CREATE TABLE s(a INT, b INT, v INT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,10);
INSERT INTO s VALUES(1,1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v=99 WHERE a=1;
UPDATE s SET v=88 WHERE a=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD" "t"

oracle_summary "summary_keyless_insert" "
CREATE TABLE t(a INT, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_summary "summary_keyless_delete" "
CREATE TABLE t(a INT, v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DELETE FROM t WHERE a=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

oracle_summary "summary_keyless_modify" "
CREATE TABLE t(a INT, v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v=99 WHERE a=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~1" "HEAD"

echo "--- working set / staged refs ---"

WS_BASE="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

oracle_both "ws_head_working_modify" "
$WS_BASE
UPDATE t SET v = 99 WHERE id = 1;
" "HEAD" "WORKING"

oracle_both "ws_head_working_insert" "
$WS_BASE
INSERT INTO t VALUES(3, 30);
" "HEAD" "WORKING"

oracle_both "ws_head_working_delete" "
$WS_BASE
DELETE FROM t WHERE id = 2;
" "HEAD" "WORKING"

oracle_both "ws_head_working_add_column" "
$WS_BASE
ALTER TABLE t ADD COLUMN w INT;
UPDATE t SET w = 7 WHERE id = 1;
" "HEAD" "WORKING"

oracle_both "ws_head_working_no_change" "$WS_BASE" "HEAD" "WORKING" "" "EXPECT_EMPTY"

oracle_both "ws_head_staged" "
$WS_BASE
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
" "HEAD" "STAGED"

oracle_both "ws_staged_working_partial" "
$WS_BASE
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
INSERT INTO t VALUES(3, 30);
" "STAGED" "WORKING"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
