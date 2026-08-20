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

normalize() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 { print }' \
    | sort -t$'\t' -k2 \
    | awk -F'\t' '
        {
          h = $1
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print seen[h] "\t" $2
        }
      '
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT commit_hash || char(9) || message FROM dolt_log'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"' | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_no_merge_commit() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_nm"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_count
  printf '%s\n' "$setup" | "$DOLTLITE" "$dir/dl/db" >/dev/null 2>"$dir/dl.err" || true
  dl_count=$(printf ".headers off\n.mode list\nSELECT count(*) FROM dolt_log;\n" \
             | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
             | grep -E '^[0-9]+$' | tail -1)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_count
  dt_count=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err" || true
    "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_log;" 2>>"$dir/dt.err" \
      | tail -n +2
  )

  if [ -z "$dl_count" ] || [ -z "$dt_count" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (count query failed)"
    return
  fi

  if [ "$dl_count" = "$dt_count" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite log count: $dl_count"
    echo "    dolt log count:     $dt_count"
  fi
}

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
  fi
}

oracle_error_poststate() {
  local name="$1" setup="$2" query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_post"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup" || true
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s;\n" "$query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_same_session() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_ss"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    {
      printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s\n" "$setup" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
      | tr -d '\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf "%s\n" "$dolt_setup"
      printf "%s\n" "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_reopen_state() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_reopen"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup" || true
  local dl_out
  dl_out=$(
    {
      printf ".headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_merge ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
"

echo "--- fast forward ---"

oracle "fast_forward_clean" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "fast_forward_after_end_options" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('--', 'feature');
"

echo "--- three-way no conflict ---"

oracle "three_way_independent_inserts" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_disjoint_columns_modified" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES (1, 0, 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET a = 10 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_a');
SELECT dolt_checkout('feature');
UPDATE t SET b = 20 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_b');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- non-INTEGER primary key shapes ---"

oracle "three_way_int_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (100, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (200, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_text_pk" "
CREATE TABLE t(id VARCHAR(64) PRIMARY KEY, v INT);
INSERT INTO t VALUES ('alpha', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES ('beta', 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES ('gamma', 3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_composite_pk" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (1, 2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 1, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_text_pk_modify_non_pk_col" "
CREATE TABLE t(id VARCHAR(64) PRIMARY KEY, v INT);
INSERT INTO t VALUES ('alpha', 1);
INSERT INTO t VALUES ('beta', 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 10 WHERE id = 'alpha';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 20 WHERE id = 'beta';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- --no-ff ---"

oracle "no_ff_creates_merge_commit" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '--no-ff');
"

echo "--- custom message ---"

oracle "merge_with_custom_message" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '-m', 'release sync');
"

oracle "merge_with_message_long_flag" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '--message', 'release sync');
"

echo "--- conflict (no merge commit) ---"

oracle_no_merge_commit "modify_modify_conflict_blocks_merge" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_error_poststate "modify_modify_conflict_rolls_back" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_reopen_state "modify_modify_conflict_explicit_txn_reconnect_rolls_back" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
BEGIN;
SELECT dolt_merge('feature');
" "SELECT 'Q' || char(9) || v FROM t WHERE id = 1;
SELECT 'Q' || char(9) || count(*) FROM dolt_conflicts;" \
"SELECT concat('Q', char(9), v) FROM t WHERE id = 1;
SELECT concat('Q', char(9), count(*)) FROM dolt_conflicts;"

echo "--- savepoint parity ---"

oracle_same_session "merge_savepoint_success_invalidated" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SAVEPOINT sp1;
SELECT dolt_merge('feature');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || count(*) FROM t;
SELECT 'Q' || char(9) || count(*) FROM dolt_log;" \
"SELECT concat('Q', char(9), count(*)) FROM t;
SELECT concat('Q', char(9), count(*)) FROM dolt_log;"

oracle_same_session "merge_abort_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feat' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
BEGIN;
SELECT dolt_merge('feature');
SAVEPOINT sp1;
SELECT dolt_merge('--abort');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts);
SELECT 'Q' || char(9) || (SELECT v FROM t WHERE id = 1);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts));
SELECT concat('Q', char(9), (SELECT v FROM t WHERE id = 1));"

oracle_same_session "merge_conflict_nested_savepoint_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feat' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
BEGIN;
SAVEPOINT sp1;
SELECT dolt_merge('feature');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts);
SELECT 'Q' || char(9) || (SELECT v FROM t WHERE id = 1);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts));
SELECT concat('Q', char(9), (SELECT v FROM t WHERE id = 1));"

oracle_same_session "merge_conflict_top_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feat' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
SAVEPOINT sp1;
SELECT dolt_merge('feature');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts);
SELECT 'Q' || char(9) || (SELECT v FROM t WHERE id = 1);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts));
SELECT concat('Q', char(9), (SELECT v FROM t WHERE id = 1));"

echo "--- already up to date ---"

oracle "merge_same_branch_no_op" "
$SEED
SELECT dolt_merge('main');
"

echo "--- other operation pairs ---"

oracle "add_modify_disjoint_rows_clean" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "add_add_same_key_same_value" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_no_merge_commit "add_add_same_key_different_value_conflict" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "delete_delete_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_no_merge_commit "delete_modify_conflict" "
$SEED
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- multi-commit branches ---"

oracle "multi_commit_feature_branch" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat3');
INSERT INTO t VALUES (5, 50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat4');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- multi-table merges ---"

oracle "multi_table_independent_inserts" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO a VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "multi_table_modify_one_insert_other" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE a SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_tables_disjoint_changes" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
INSERT INTO c VALUES (1, 1000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO a VALUES (2, 20);
INSERT INTO c VALUES (2, 2000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO b VALUES (2, 200);
UPDATE c SET v = 1500 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- schema deltas ---"

oracle "feature_creates_new_table" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_new_table');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "feature_adds_column_main_inserts_old_shape" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN tag TEXT;
UPDATE t SET tag = 'a' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_col');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "both_branches_add_disjoint_columns" "
$SEED
ALTER TABLE t ADD COLUMN main_col INT;
UPDATE t SET main_col = 1 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_col');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN feat_col INT;
UPDATE t SET feat_col = 2 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_col');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_same_session "merge_disjoint_fk_tables_plus_check_same_session" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
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
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_reopen_state "merge_disjoint_fk_tables_plus_check_reopen" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
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
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_same_session "merge_recreate_fk_family_plus_check_same_session" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE c;
DROP TABLE p;
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE, label TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (2, 200, 'x');
INSERT INTO c VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_recreate_fk_family');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_same_session "merge_self_ref_fk_cascade_same_session" "
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_descendant');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM t WHERE id = 1;
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM t) || '|' ||
          (SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', IFNULL(parent_id, -1)) ORDER BY id SEPARATOR ',') FROM t))"

oracle_reopen_state "merge_self_ref_fk_cascade_reopen" "
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_descendant');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM t WHERE id = 1;
" "PRAGMA foreign_keys = ON;
SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM t) || '|' ||
          (SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id))" \
  "SET @@foreign_key_checks = 1;
SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', IFNULL(parent_id, -1)) ORDER BY id SEPARATOR ',') FROM t))"

oracle_same_session "merge_fk_chain_cascade_same_session" "
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_checkout('main');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM gp WHERE id = 1;
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM gp) || '|' ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c)" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM gp), '|', (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c))"

oracle_reopen_state "merge_fk_chain_cascade_reopen" "
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_checkout('main');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM gp WHERE id = 1;
" "PRAGMA foreign_keys = ON;
SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM gp) || '|' ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c)" \
  "SET @@foreign_key_checks = 1;
SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM gp), '|', (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c))"

echo "--- non-branch merge sources ---"

oracle "merge_from_tag" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_tag('release-1');
SELECT dolt_checkout('main');
SELECT dolt_merge('release-1');
"

oracle "merge_from_commit_hash" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge((SELECT commit_hash FROM dolt_log WHERE message = 'feat1'));
"

echo "--- merge sequences ---"

oracle "merge_then_reverse_fast_forward" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_checkout('feature');
SELECT dolt_merge('main');
"

oracle "merge_twice_with_intermediate_commits" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (30, 300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- error paths ---"

oracle_error "merge_nonexistent_branch" "
$SEED
SELECT dolt_merge('nope');
"

oracle_error "merge_unknown_flag" "
$SEED
SELECT dolt_merge('feature', '--bogus');
"

oracle_error "merge_no_args" "
SELECT dolt_merge();
"

oracle_error "merge_abort_with_extra_args" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_commit('-A', '-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_commit('-A', '-m', 'feat1');
SELECT dolt_checkout('main');
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_commit('-A', '-m', 'main2');
SELECT dolt_merge('feature');
SELECT dolt_merge('--abort', 'extra');
"

oracle_error_poststate "merge_constraint_violation_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1, 1, 'base1'), (2, 2, 'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET u = 9, v = 'feat2' WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u = 9, v = 'main1' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_unique');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id, u, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "merge_conflict_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feature' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feature edit');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main edit');
BEGIN;
SELECT dolt_merge('feature');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT v FROM t WHERE id = 1)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT v FROM t WHERE id = 1))"

oracle_error_poststate "merge_schema_conflict_autocommit_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN c TEXT;
SELECT dolt_commit('-Am', 'feature schema');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN c INTEGER;
SELECT dolt_commit('-Am', 'main schema');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' ||
          (SELECT count(*) FROM dolt_conflicts) || '|' ||
          (SELECT count(*) FROM dolt_status WHERE status = 'schema conflict')" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_schema_conflicts), '|', (SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT COUNT(*) FROM dolt_status WHERE status = 'schema conflict'))"

oracle_same_session "merge_schema_conflict_in_session" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN c TEXT;
SELECT dolt_commit('-Am', 'feature schema');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN c INTEGER;
SELECT dolt_commit('-Am', 'main schema');
BEGIN;
SELECT dolt_merge('feature');
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM dolt_schema_conflicts) || char(9) ||
          (SELECT count(*) FROM dolt_conflicts) || char(9) ||
          (SELECT coalesce(sum(num_conflicts), -1) FROM dolt_conflicts) || char(9) ||
          (SELECT count(*) FROM dolt_status WHERE status = 'schema conflict');" \
"SELECT CONCAT('Q', char(9),
               (SELECT COUNT(*) FROM dolt_schema_conflicts), char(9),
               (SELECT COUNT(*) FROM dolt_conflicts), char(9),
               (SELECT COALESCE(SUM(num_conflicts), -1) FROM dolt_conflicts), char(9),
               (SELECT COUNT(*) FROM dolt_status WHERE status = 'schema conflict'));"

oracle_same_session "merge_schema_conflict_ours_deleted" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN c INTEGER;
SELECT dolt_commit('-Am', 'feature schema');
SELECT dolt_checkout('main');
DROP TABLE t;
SELECT dolt_commit('-Am', 'main drop');
BEGIN;
SELECT dolt_merge('feature');
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM dolt_schema_conflicts) || char(9) ||
          (SELECT count(*) FROM dolt_conflicts) || char(9) ||
          (SELECT count(*) FROM dolt_status WHERE status = 'schema conflict') || char(9) ||
          (SELECT our_schema FROM dolt_schema_conflicts);" \
"SELECT CONCAT('Q', char(9),
               (SELECT COUNT(*) FROM dolt_schema_conflicts), char(9),
               (SELECT COUNT(*) FROM dolt_conflicts), char(9),
               (SELECT COUNT(*) FROM dolt_status WHERE status = 'schema conflict'), char(9),
               (SELECT our_schema FROM dolt_schema_conflicts));"

oracle_error_poststate "merge_constraint_violation_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1, 1, 'base1'), (2, 2, 'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET u = 9, v = 'feat2' WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u = 9, v = 'main1' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_unique');
BEGIN;
SELECT dolt_merge('feature');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id, u, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

echo ""
echo "--- Schema objects (views, triggers, indexes) ---"

# Trigger bodies and DROP INDEX cannot share one script across dialects
# (SQLite BEGIN...END vs MySQL FOR EACH ROW; DROP INDEX x vs x ON t), so
# these run separate per-system setups against the same post-state query.
oracle_dual_poststate() {
  local name="$1" dl_setup="$2" dt_setup="$3" dl_query="$4" dt_query="$5"
  local dir="$TMPROOT/${name}_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_setup" || true
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s;\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_dual_error() {
  local name="$1" dl_setup="$2" dt_setup="$3"
  local dir="$TMPROOT/${name}_dualerr"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
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
  fi
}

VIEW_BASE="
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 20);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"

oracle_error_poststate "merge_view_add_theirs" "
$VIEW_BASE
CREATE VIEW v AS SELECT a, b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM v) || '|' || (SELECT group_concat(a || ':' || b, ',') FROM (SELECT a, b FROM v ORDER BY a))" \
"SELECT CONCAT((SELECT COUNT(*) FROM v), '|', (SELECT GROUP_CONCAT(CONCAT(a, ':', b) ORDER BY a SEPARATOR ',') FROM v))"

oracle_error_poststate "merge_view_add_ours_data_theirs" "
$VIEW_BASE
INSERT INTO t VALUES (3, 30);
SELECT dolt_commit('-am', 'feat_data');
SELECT dolt_checkout('main');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'main_view');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM v) || '|' || (SELECT group_concat(a, ',') FROM (SELECT a FROM v ORDER BY a))" \
"SELECT CONCAT((SELECT COUNT(*) FROM v), '|', (SELECT GROUP_CONCAT(a ORDER BY a SEPARATOR ',') FROM v))"

oracle_error_poststate "merge_view_both_add_identical" "
$VIEW_BASE
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'main_view');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM v) || '|' || (SELECT group_concat(a, ',') FROM (SELECT a FROM v ORDER BY a))" \
"SELECT CONCAT((SELECT COUNT(*) FROM v), '|', (SELECT GROUP_CONCAT(a ORDER BY a SEPARATOR ',') FROM v))"

oracle_error "merge_view_both_add_different" "
$VIEW_BASE
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'main_view');
SELECT dolt_merge('feature');
"

VIEW_MOD_BASE="
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 20);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"

oracle_error "merge_view_both_modify_different" "
$VIEW_MOD_BASE
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
DROP VIEW v;
CREATE VIEW v AS SELECT a, b FROM t;
SELECT dolt_commit('-Am', 'main_view');
SELECT dolt_merge('feature');
"

oracle_error "merge_view_modify_vs_drop" "
$VIEW_MOD_BASE
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
DROP VIEW v;
SELECT dolt_commit('-Am', 'main_drop');
SELECT dolt_merge('feature');
"

oracle_error_poststate "merge_view_both_drop" "
$VIEW_MOD_BASE
DROP VIEW v;
SELECT dolt_commit('-Am', 'feat_drop');
SELECT dolt_checkout('main');
DROP VIEW v;
SELECT dolt_commit('-Am', 'main_drop');
SELECT dolt_merge('feature');
" "SELECT count(*) FROM sqlite_master WHERE type = 'view'" \
"SELECT COUNT(*) FROM dolt_schemas WHERE type = 'view'"

oracle_error_poststate "merge_view_modify_theirs" "
$VIEW_MOD_BASE
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
" "SELECT group_concat(b, ',') FROM (SELECT b FROM v ORDER BY b)" \
"SELECT GROUP_CONCAT(b ORDER BY b SEPARATOR ',') FROM v"

oracle_same_session "merge_view_conflict_in_session" "
$VIEW_MOD_BASE
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
DROP VIEW v;
CREATE VIEW v AS SELECT a, b FROM t;
SELECT dolt_commit('-Am', 'main_view');
BEGIN;
SELECT dolt_merge('feature');
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts WHERE \"table\"='(sqlite_master)') || '|' || (SELECT CASE WHEN (SELECT count(*) FROM dolt_conflicts)+(SELECT count(*) FROM dolt_schema_conflicts)>0 THEN 1 ELSE 0 END);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts WHERE \`table\`='(sqlite_master)'), '|', CASE WHEN (SELECT count(*) FROM dolt_conflicts)+(SELECT count(*) FROM dolt_schema_conflicts)>0 THEN 1 ELSE 0 END);"

oracle_same_session "merge_view_both_add_different_not_master" "
$VIEW_BASE
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'main_view');
BEGIN;
SELECT dolt_merge('feature');
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts WHERE \"table\"='(sqlite_master)') || '|' || (SELECT CASE WHEN (SELECT count(*) FROM dolt_conflicts)+(SELECT count(*) FROM dolt_schema_conflicts)>0 THEN 1 ELSE 0 END);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts WHERE \`table\`='(sqlite_master)'), '|', CASE WHEN (SELECT count(*) FROM dolt_conflicts)+(SELECT count(*) FROM dolt_schema_conflicts)>0 THEN 1 ELSE 0 END);"

TRG_DL_BASE="
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE TABLE audit(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"
TRG_DT_BASE="
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE TABLE audit(a INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"
TRG_Q_DL="SELECT (SELECT count(*) FROM audit) || '|' || (SELECT coalesce(group_concat(a, ','), '~') FROM (SELECT a FROM audit ORDER BY a)) || '|' || (SELECT count(*) FROM t)"
TRG_Q_DT="SELECT CONCAT((SELECT COUNT(*) FROM audit), '|', (SELECT COALESCE(GROUP_CONCAT(a ORDER BY a SEPARATOR ','), '~') FROM audit), '|', (SELECT COUNT(*) FROM t))"

oracle_dual_poststate "merge_trigger_add_theirs_fires" "
$TRG_DL_BASE
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (5, 50);
" "$TRG_DT_BASE
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (5, 50);
" "$TRG_Q_DL" "$TRG_Q_DT"

oracle_dual_poststate "merge_trigger_both_add_identical" "
$TRG_DL_BASE
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'main_trigger');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (5, 50);
" "$TRG_DT_BASE
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'main_trigger');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (5, 50);
" "$TRG_Q_DL" "$TRG_Q_DT"

oracle_dual_error "merge_trigger_both_add_different" "
$TRG_DL_BASE
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a + 100); END;
SELECT dolt_commit('-Am', 'main_trigger');
SELECT dolt_merge('feature');
" "$TRG_DT_BASE
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a + 100);
SELECT dolt_commit('-Am', 'main_trigger');
SELECT dolt_merge('feature');
"

TRG_MOD_DL_BASE="
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE TABLE audit(a INTEGER PRIMARY KEY);
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"
TRG_MOD_DT_BASE="
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE TABLE audit(a INT PRIMARY KEY);
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"

oracle_dual_error "merge_trigger_modify_vs_drop" "
$TRG_MOD_DL_BASE
DROP TRIGGER trg;
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a + 100); END;
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'main_drop');
SELECT dolt_merge('feature');
" "$TRG_MOD_DT_BASE
DROP TRIGGER trg;
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a + 100);
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'main_drop');
SELECT dolt_merge('feature');
"

oracle_dual_poststate "merge_trigger_both_drop" "
$TRG_MOD_DL_BASE
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'feat_drop');
SELECT dolt_checkout('main');
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'main_drop');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (5, 50);
" "$TRG_MOD_DT_BASE
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'feat_drop');
SELECT dolt_checkout('main');
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'main_drop');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (5, 50);
" "$TRG_Q_DL" "$TRG_Q_DT"

IDX_BASE="
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT, c INT);
INSERT INTO t VALUES (1, 10, 100), (2, 20, 200);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"

oracle_error_poststate "merge_index_add_theirs" "
$IDX_BASE
CREATE INDEX idx_b ON t(b);
SELECT dolt_commit('-Am', 'feat_index');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 20, 300);
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
" "SELECT (SELECT group_concat(a, ',') FROM (SELECT a FROM t WHERE b = 20 ORDER BY a)) || '|' || (SELECT count(*) FROM t)" \
"SELECT CONCAT((SELECT GROUP_CONCAT(a ORDER BY a SEPARATOR ',') FROM t WHERE b = 20), '|', (SELECT COUNT(*) FROM t))"

oracle_error_poststate "merge_index_both_add_different_names" "
$IDX_BASE
CREATE INDEX idx_b ON t(b);
SELECT dolt_commit('-Am', 'feat_index');
SELECT dolt_checkout('main');
CREATE INDEX idx_c ON t(c);
SELECT dolt_commit('-Am', 'main_index');
SELECT dolt_merge('feature');
" "SELECT (SELECT group_concat(a, ',') FROM (SELECT a FROM t WHERE b = 20 ORDER BY a)) || '|' || (SELECT group_concat(a, ',') FROM (SELECT a FROM t WHERE c = 100 ORDER BY a))" \
"SELECT CONCAT((SELECT GROUP_CONCAT(a ORDER BY a SEPARATOR ',') FROM t WHERE b = 20), '|', (SELECT GROUP_CONCAT(a ORDER BY a SEPARATOR ',') FROM t WHERE c = 100))"

oracle_error_poststate "merge_index_both_add_identical" "
$IDX_BASE
CREATE INDEX idx_b ON t(b);
SELECT dolt_commit('-Am', 'feat_index');
SELECT dolt_checkout('main');
CREATE INDEX idx_b ON t(b);
SELECT dolt_commit('-Am', 'main_index');
SELECT dolt_merge('feature');
" "SELECT group_concat(a, ',') FROM (SELECT a FROM t WHERE b = 20 ORDER BY a)" \
"SELECT GROUP_CONCAT(a ORDER BY a SEPARATOR ',') FROM t WHERE b = 20"

oracle_error "merge_index_same_name_different" "
$IDX_BASE
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am', 'feat_index');
SELECT dolt_checkout('main');
CREATE INDEX idx ON t(c);
SELECT dolt_commit('-Am', 'main_index');
SELECT dolt_merge('feature');
"

oracle_dual_poststate "merge_index_drop_vs_data_change" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 20);
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
DROP INDEX idx;
SELECT dolt_commit('-Am', 'feat_drop');
SELECT dolt_checkout('main');
UPDATE t SET b = 99 WHERE a = 1;
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 20);
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
DROP INDEX idx ON t;
SELECT dolt_commit('-Am', 'feat_drop');
SELECT dolt_checkout('main');
UPDATE t SET b = 99 WHERE a = 1;
SELECT dolt_commit('-am', 'main_data');
SELECT dolt_merge('feature');
" "SELECT group_concat(a || ':' || b, ',') FROM (SELECT a, b FROM t ORDER BY a)" \
"SELECT GROUP_CONCAT(CONCAT(a, ':', b) ORDER BY a SEPARATOR ',') FROM t"

echo ""
echo "--- Dual ADD COLUMN with concurrent row changes ---"

# Both branches add a disjoint column; the row-level changes on each side must
# still merge (edits to shared columns, deletes, and each side's new-column
# values), and a true same-cell edit must still conflict. A naive "take ours +
# backfill added columns" would silently drop theirs' shared-column edits and
# deletes.
CADD_BASE="
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES (1, 'one'), (5, 'five'), (9, 'nine');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
"

oracle_error_poststate "dual_addcol_theirs_edit_and_delete" "
$CADD_BASE
UPDATE t SET a = 'five-theirs' WHERE id = 5;
DELETE FROM t WHERE id = 9;
ALTER TABLE t ADD COLUMN c TEXT;
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN b TEXT;
SELECT dolt_commit('-Am', 'main_addcol');
SELECT dolt_merge('feature');
" "SELECT group_concat(id || ':' || coalesce(a,'~'), ',') FROM (SELECT id, a FROM t ORDER BY id)" \
"SELECT GROUP_CONCAT(CONCAT(id, ':', coalesce(a,'~')) ORDER BY id SEPARATOR ',') FROM t"

oracle_error_poststate "dual_addcol_each_sets_own_new_col_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
ALTER TABLE t ADD COLUMN feat_col INT;
UPDATE t SET feat_col = 2 WHERE id = 1;
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN main_col INT;
UPDATE t SET main_col = 1 WHERE id = 1;
SELECT dolt_commit('-Am', 'main_change');
SELECT dolt_merge('feature');
" "SELECT id || '|' || v || '|' || coalesce(main_col,'~') || '|' || coalesce(feat_col,'~') FROM t" \
"SELECT CONCAT(id, '|', v, '|', coalesce(main_col,'~'), '|', coalesce(feat_col,'~')) FROM t"

oracle_error_poststate "dual_addcol_theirs_adds_row_with_new_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES (1, 'one');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
ALTER TABLE t ADD COLUMN c TEXT;
INSERT INTO t VALUES (2, 'two', 'c2');
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN b TEXT;
SELECT dolt_commit('-Am', 'main_addcol');
SELECT dolt_merge('feature');
" "SELECT group_concat(id || ':' || coalesce(a,'~') || ':' || coalesce(c,'~'), ',') FROM (SELECT id, a, c FROM t ORDER BY id)" \
"SELECT GROUP_CONCAT(CONCAT(id, ':', coalesce(a,'~'), ':', coalesce(c,'~')) ORDER BY id SEPARATOR ',') FROM t"

oracle_error "dual_addcol_same_cell_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
INSERT INTO t VALUES (1, 'one');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
ALTER TABLE t ADD COLUMN c TEXT;
UPDATE t SET a = 'theirs-a' WHERE id = 1;
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN b TEXT;
UPDATE t SET a = 'ours-a' WHERE id = 1;
SELECT dolt_commit('-Am', 'main_change');
SELECT dolt_merge('feature');
"

# Mixed-case shared columns in the ancestor CREATE TABLE: dual ADD COLUMN must
# still three-way merge row edits/deletes (column-name remap is case-insensitive).
oracle_error_poststate "dual_addcol_mixed_case_shared_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, Name TEXT, Score INT);
INSERT INTO t VALUES (1, 'alice', 10), (2, 'bob', 20);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
ALTER TABLE t ADD COLUMN FeatNote TEXT;
UPDATE t SET Name = 'ALICE', Score = 11, FeatNote = 'f1' WHERE id = 1;
DELETE FROM t WHERE id = 2;
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN MainNote TEXT;
UPDATE t SET MainNote = 'm1' WHERE id = 1;
SELECT dolt_commit('-Am', 'main_change');
SELECT dolt_merge('feature');
" "SELECT id || '|' || Name || '|' || Score || '|' || coalesce(MainNote,'~') || '|' || coalesce(FeatNote,'~') FROM t" \
"SELECT CONCAT(id, '|', Name, '|', Score, '|', coalesce(MainNote,'~'), '|', coalesce(FeatNote,'~')) FROM t"

# Concluding a conflicted merge with dolt_commit still has to record the merged
# branch as a second parent. Without it the merged commits leave the log, and
# every later merge base is computed against a history that never recorded the
# merge -- so dolt_merge('feature') would replay work already merged.
oracle "conflict_resolved_commit_keeps_merged_branch_in_log" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'feature edit');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'main edit');
BEGIN;
SELECT dolt_merge('feature');
SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-Am', 'resolved');
COMMIT;
"

# Same shape through cherry-pick, which must stay a single-parent commit: the
# picked branch is a source to resolve against, not a parent to record.
oracle "conflict_resolved_cherry_pick_stays_single_parent" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'feature edit');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'main edit');
BEGIN;
SELECT dolt_cherry_pick('feature');
SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-Am', 'picked');
COMMIT;
"

# The merged branch really is an ancestor afterwards, so re-merging it is a
# no-op rather than a replay of work already merged.
oracle_reopen_state "conflict_resolved_commit_makes_merged_branch_an_ancestor" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'feature edit');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'main edit');
BEGIN;
SELECT dolt_merge('feature');
SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-Am', 'resolved');
COMMIT;
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM dolt_log WHERE message = 'feature edit') || char(9) ||
          (SELECT count(*) FROM dolt_commit_ancestors
             WHERE commit_hash = dolt_hashof('HEAD'));" \
"SELECT CONCAT('Q', char(9),
               (SELECT COUNT(*) FROM dolt_log WHERE message = 'feature edit'), char(9),
               (SELECT COUNT(*) FROM dolt_commit_ancestors
                  WHERE commit_hash = HASHOF('HEAD')));"

echo ""
echo "--- dual rename keeps both tables ---"

oracle_error_poststate "dual_rename_keeps_both_tables" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME TO theirs_t;
SELECT dolt_commit('-Am', 'theirs rename');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME TO ours_t;
SELECT dolt_commit('-Am', 'ours rename');
SELECT dolt_merge('feat');
" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(name, ',') FROM (SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ours_t','theirs_t','t') ORDER BY name)) || '|' || (SELECT count(*) FROM ours_t) || '|' || (SELECT count(*) FROM theirs_t)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_schema_conflicts), '|', (SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME SEPARATOR ',') FROM information_schema.tables WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('ours_t','theirs_t','t')), '|', (SELECT COUNT(*) FROM ours_t), '|', (SELECT COUNT(*) FROM theirs_t))"

oracle_error_poststate "dual_rename_with_row_edits_keeps_both" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME TO theirs_t;
INSERT INTO theirs_t VALUES (2, 'feat');
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME TO ours_t;
INSERT INTO ours_t VALUES (3, 'main');
SELECT dolt_commit('-Am', 'ours');
SELECT dolt_merge('feat');
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM ours_t ORDER BY id)) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM theirs_t ORDER BY id))" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM ours_t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM theirs_t))"

echo ""
echo "--- add/add matching schema ---"

oracle_error_poststate "dual_add_table_same_schema_union" "
SELECT dolt_commit('-Am', 'init empty');
SELECT dolt_branch('feat');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main'), (3, 'm3');
SELECT dolt_commit('-Am', 'main adds t');
SELECT dolt_checkout('feat');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (2, 'feat'), (4, 'f4');
SELECT dolt_commit('-Am', 'feat adds t');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id))" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_schema_conflicts), '|', (SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_same_session "dual_add_table_same_pk_is_row_conflict" "
SELECT dolt_commit('-Am', 'init empty');
SELECT dolt_branch('feat');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_commit('-Am', 'main adds t');
SELECT dolt_checkout('feat');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'feat');
SELECT dolt_commit('-Am', 'feat adds t');
SELECT dolt_checkout('main');
BEGIN;
SELECT dolt_merge('feat');
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_schema_conflicts) || char(9) || (SELECT count(*) FROM dolt_conflicts) || char(9) || (SELECT coalesce(sum(num_conflicts), 0) FROM dolt_conflicts) || char(9) || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id));" \
"SELECT CONCAT('Q', char(9), (SELECT COUNT(*) FROM dolt_schema_conflicts), char(9), (SELECT COUNT(*) FROM dolt_conflicts), char(9), (SELECT COALESCE(SUM(num_conflicts), 0) FROM dolt_conflicts), char(9), (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t));"

oracle_error "dual_add_table_different_schema_refused" "
SELECT dolt_commit('-Am', 'init empty');
SELECT dolt_branch('feat');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_commit('-Am', 'main adds t');
SELECT dolt_checkout('feat');
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (2, 7);
SELECT dolt_commit('-Am', 'feat adds t');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"

oracle_error_poststate "dual_add_index_same_sql_covers_merged_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT);
INSERT INTO t VALUES (1, 10, 'x');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES (2, 20, 'main');
CREATE INDEX tk ON t(k);
SELECT dolt_commit('-Am', 'main idx');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 30, 'feat');
CREATE INDEX tk ON t(k);
SELECT dolt_commit('-Am', 'feat idx');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT group_concat(id || ':' || k, ',') FROM (SELECT id, k FROM t ORDER BY id)) || '|' || (SELECT group_concat(id || ':' || k, ',') FROM (SELECT id, k FROM t INDEXED BY tk WHERE k >= 10 ORDER BY k))" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_schema_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', k) ORDER BY id SEPARATOR ',') FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', k) ORDER BY k SEPARATOR ',') FROM t WHERE k >= 10))"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
