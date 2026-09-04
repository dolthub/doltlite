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

normalize_diff_table() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 7 && $1 == "T" { print }' \
    | awk -F'\t' '
        {
          tbl     = $2
          to_id   = $3
          to_msg  = $4
          diff    = $5
          from_id = $6
          from_msg = $7
          if (to_msg == "" || to_msg ~ /^0+$/) to_msg = "EMPTY"
          if (from_msg == "" || from_msg ~ /^0+$/) from_msg = "EMPTY"
          print "T\t" tbl "\t" diff "\t" to_id "\t" to_msg "\t" from_id "\t" from_msg
        }
      ' \
    | sort
}

oracle() {
  local name="$1" setup="$2" tables="${3:-t}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_table_q=""
  IFS=',' read -ra tarr <<< "$tables"
  for tn in "${tarr[@]}"; do
    local part="SELECT 'T' || char(9) || '${tn}' || char(9) || coalesce(dt.to_id,'') || char(9) || coalesce(log_to.message, dt.to_commit) || char(9) || dt.diff_type || char(9) || coalesce(dt.from_id,'') || char(9) || coalesce(log_from.message, dt.from_commit) FROM dolt_diff_${tn} dt LEFT JOIN dolt_log log_to ON log_to.commit_hash = dt.to_commit LEFT JOIN dolt_log log_from ON log_from.commit_hash = dt.from_commit"
    if [ -z "$dl_table_q" ]; then
      dl_table_q="$part"
    else
      dl_table_q="$dl_table_q UNION ALL $part"
    fi
  done

  local dl_table
  dl_table=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_table_q" \
             | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
             | grep -v '^[0-9]*$' \
             | grep -v '^[0-9a-f]\{40\}$' \
             | normalize_diff_table)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_table_q=""
  for tn in "${tarr[@]}"; do
    local part="SELECT concat('T', char(9), '${tn}', char(9), coalesce(dt.to_id,''), char(9), coalesce(log_to.message, dt.to_commit), char(9), dt.diff_type, char(9), coalesce(dt.from_id,''), char(9), coalesce(log_from.message, dt.from_commit)) FROM dolt_diff_${tn} dt LEFT JOIN dolt_log log_to ON log_to.commit_hash = dt.to_commit LEFT JOIN dolt_log log_from ON log_from.commit_hash = dt.from_commit"
    if [ -z "$dt_table_q" ]; then
      dt_table_q="$part"
    else
      dt_table_q="$dt_table_q UNION ALL $part"
    fi
  done

  local dt_table
  dt_table=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_table_q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_diff_table
  )

  vc_oracle_assert_match "$name" "$dl_table" "$dt_table"
}

normalize_summary() {
  tr -d '\r' \
    | sed -e 's/	true$/	1/' -e 's/	true	/	1	/g' \
          -e 's/	false$/	0/' -e 's/	false	/	0	/g' \
    | awk -F'\t' 'NF >= 5 && $1 == "S" {
        if ($3 ~ /^Revert "/) {
          sub(/^Revert "/, "Revert ", $3)
          sub(/"$/, "", $3)
        }
        print $1 "\t" $2 "\t" $3 "\t" $4 "\t" $5
      }' \
    | sort
}

oracle_summary() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_summary"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('S', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize_summary)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_summary
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

# Separate setups for DROP INDEX / trigger syntax.
oracle_summary_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3"
  local dir="$TMPROOT/${name}_summary"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('S', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$dl_setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize_summary)

  local dolt_setup
  dolt_setup=$(echo "$dt_setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_summary
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_summary_filter_name() {
  local name="$1" setup="$2" target="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/${name}_filter"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash WHERE dd.table_name = '$target'"
  local q_dolt="SELECT concat('S', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash WHERE dd.table_name = '$target'"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize_summary)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

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
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

echo "=== Version Control Oracle Tests: dolt_diff / dolt_diff_<table> ==="
echo ""

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- multi-table ---"

oracle "two_tables_independent" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO u VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init_both');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_t_only');
INSERT INTO u VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_u_only');
" "t,u"

echo "--- per-table: row-level diff ---"

oracle "table_diff_modify_row" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_update');
"

oracle "table_diff_delete_row" "
$SEED
INSERT INTO t VALUES (2, 20);
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_two');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_delete');
"

oracle "table_diff_add_then_modify_then_delete_same_row" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add');
UPDATE t SET v = 22 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_modify');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4_delete');
"

echo "--- staged state interactions ---"

oracle "table_diff_after_stage_only" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
"

oracle "table_diff_stage_then_more_working" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 99 WHERE id = 1;
"

oracle "table_diff_stage_insert" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
"

oracle "table_diff_stage_delete" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
"

oracle "table_diff_mixed_staged_and_unstaged" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
INSERT INTO t VALUES (3, 30);
"

echo "--- working set diff ---"

oracle "table_diff_working_modify" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
"

oracle "table_diff_working_insert" "
$SEED
INSERT INTO t VALUES (2, 20);
"

oracle "table_diff_working_delete" "
$SEED
DELETE FROM t WHERE id = 1;
"

oracle "table_diff_working_then_committed" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

oracle "table_diff_history_plus_working" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
UPDATE t SET v = 99 WHERE id = 1;
"

echo "--- branching ---"

oracle "diff_on_feature_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
"

echo "--- merges ---"

oracle "diff_after_simple_merge" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
"

oracle "diff_after_merge_with_intermediate_commits" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main3');
SELECT dolt_merge('feature');
"

oracle "diff_after_merge_no_main_changes_after_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_update');
SELECT dolt_merge('feature');
"

echo "--- DDL across commits (ALTER TABLE in history) ---"

oracle "diff_alter_add_col_no_data_change" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col_only');
"

oracle "diff_alter_add_col_then_update_old_col" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'update_v');
"

oracle "diff_alter_add_col_then_update_new_col" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
UPDATE t SET extra = 42 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'populate_extra');
"

oracle "diff_alter_add_col_plus_insert_same_commit" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
INSERT INTO t VALUES (2, 20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col_and_insert');
"

oracle "diff_alter_add_col_then_insert_next_commit" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
INSERT INTO t VALUES (2, 20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'insert_after_alter');
"

oracle "diff_alter_add_col_then_delete_next_commit" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_row');
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_row');
"

oracle "diff_alter_drop_col_no_data_change" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_extra');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_extra');
"

oracle "diff_alter_drop_col_with_data_also_shared_match" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
UPDATE t SET extra = 100 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_and_populate');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_extra');
"

oracle "diff_alter_rename_col_no_data_change" "
$SEED
ALTER TABLE t RENAME COLUMN v TO vv;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_v');
"

oracle "diff_multiple_alters_single_commit_no_data" "
$SEED
ALTER TABLE t ADD COLUMN a TEXT;
ALTER TABLE t ADD COLUMN b INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi_alter');
"

oracle "diff_alter_then_merge" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_row');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_col');
SELECT dolt_merge('feature');
"

oracle "diff_schema_replay_after_merge_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_table');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feature');
" "u"

oracle "diff_schema_replay_after_cherrypick_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_table');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feature');
" "u"

oracle "diff_schema_replay_after_rebase_disjoint_indexes" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_idx');
SELECT dolt_checkout('feature');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_idx');
SELECT dolt_rebase('main');
" "b"

oracle "diff_schema_replay_after_merge_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, '100');
INSERT INTO c VALUES (1, '100');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feature');
" "c"

oracle "diff_schema_replay_after_cherrypick_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, '100');
INSERT INTO c VALUES (1, '100');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feature');
" "c"

oracle "diff_schema_replay_after_rebase_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, '100');
INSERT INTO c VALUES (1, '100');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feature');
SELECT dolt_rebase('main');
" "c"

oracle "diff_alter_add_col_working_set_only" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
"

oracle "diff_alter_add_col_and_update_working_set" "
$SEED
ALTER TABLE t ADD COLUMN extra INT;
UPDATE t SET extra = 42 WHERE id = 1;
"

oracle "diff_pk_covering_add_col_and_update_working_set" "
CREATE TABLE pk_all(id INT, k VARCHAR(10), PRIMARY KEY(id, k));
INSERT INTO pk_all VALUES (1, 'x'), (2, 'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE pk_all ADD COLUMN c VARCHAR(10);
UPDATE pk_all SET c = 'z' WHERE id = 1;
" "pk_all"

oracle "diff_pk_covering_add_col_across_commits" "
CREATE TABLE pk_all(id INT, k VARCHAR(10), PRIMARY KEY(id, k));
INSERT INTO pk_all VALUES (1, 'x'), (2, 'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE pk_all ADD COLUMN c VARCHAR(10);
UPDATE pk_all SET c = 'z' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "pk_all"

echo ""
echo "--- summary form: dolt_diff (no args) ---"

oracle_summary "summary_two_commits_one_table" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

oracle_summary "summary_linear_three_commits" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_insert');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_update');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4_delete');
"

oracle_summary "summary_two_tables_independent" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO u VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init_both');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_t_only');
INSERT INTO u VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_u_only');
"

oracle_summary "summary_schema_change_add_column" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
"

oracle_summary "summary_data_and_schema_in_one_commit" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
INSERT INTO t VALUES (2, 20, 'hi');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'col_and_row');
"

oracle_summary "summary_table_added_later" "
$SEED
CREATE TABLE u(id INT PRIMARY KEY, x TEXT);
INSERT INTO u VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
"

oracle_summary "summary_working_set_excluded" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
"

oracle_summary "summary_staged_change" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
"

oracle_summary "summary_staged_then_working" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 99 WHERE id = 1;
"

oracle_summary "summary_staged_and_working_split_tables" "
$SEED
CREATE TABLE u(id INT PRIMARY KEY, x INT);
INSERT INTO u VALUES (1, 1);
SELECT dolt_add('-A');
UPDATE t SET v = 99 WHERE id = 1;
"

oracle_summary "summary_staged_new_table" "
$SEED
CREATE TABLE w(id INT PRIMARY KEY, x INT);
INSERT INTO w VALUES (1, 1);
SELECT dolt_add('-A');
"

oracle_summary "summary_merge_replay_add_table_plus_check" "
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
"

oracle_summary "summary_cherrypick_replay_add_table_plus_check" "
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
"

oracle_summary "summary_rebase_replay_add_table_plus_check" "
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
"

oracle_summary "summary_merge_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
"

oracle_summary "summary_cherrypick_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
"

oracle_summary "summary_rebase_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
"

oracle_summary "summary_revert_schema_change_with_later_added_table" "
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
"

echo "--- summary form: WHERE table_name=... filter ---"

oracle_summary_filter_name "filter_dropped_table" "
CREATE TABLE dropped(id INT PRIMARY KEY, v INT);
INSERT INTO dropped VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1_create_dropped');
INSERT INTO dropped VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_modify_dropped');
DROP TABLE dropped;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_drop_dropped');
" "dropped"

oracle_summary_filter_name "filter_nonexistent_name" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "never_existed" "EXPECT_EMPTY"

oracle_summary_filter_name "filter_mixed_history" "
CREATE TABLE x(id INT PRIMARY KEY, v INT);
INSERT INTO x VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'create_x');
CREATE TABLE y(id INT PRIMARY KEY);
INSERT INTO y VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'create_y');
INSERT INTO x VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'update_x');
INSERT INTO y VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'update_y');
DROP TABLE x;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_x');
" "x"


# Index changes have no summary row; attribute to the parent table (schema_change=1).

oracle_summary "summary_index_only_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_index');
"

oracle_summary_dual "summary_drop_index_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP INDEX iv;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_index');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP INDEX iv ON t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_index');
"

oracle_summary "summary_index_plus_data_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (2, 20);
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'both');
"

oracle_summary_dual "summary_index_change_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP INDEX iv;
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP INDEX iv ON t;
"

oracle_summary "summary_index_untouched_sibling" "
CREATE TABLE x(a INTEGER PRIMARY KEY);
CREATE TABLE y(b INTEGER PRIMARY KEY);
INSERT INTO x VALUES (1);
INSERT INTO y VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX ix ON x(a);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'ix_only');
"

oracle_summary "summary_empty_table_create_drop" "
CREATE TABLE baseline(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'baseline');
CREATE TABLE empty_t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'create_empty');
DROP TABLE empty_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_empty');
"

oracle_summary_filter_name "summary_filter_index_only_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_index');
" "t"

oracle_summary "summary_view_only_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE VIEW vv AS SELECT id FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view');
"

oracle_summary_dual "summary_trigger_only_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW SET @x = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
"

oracle "index_only_commit_no_row_diffs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_index');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data');
" "t"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
