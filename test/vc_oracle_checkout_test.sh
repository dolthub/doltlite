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

normalize_branch() {
  tr -d '\r' | awk -F'\t' 'NF >= 2 && $1 == "B" { print }'
}

normalize_rows() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "R" { print }' \
    | sort
}

normalize_status() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 4 && $1 == "S" { print }' \
    | sort -t$'\t' -k2,2 -k3,3 -k4,4
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_branch dl_rows dl_status
  dl_branch=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'B' || char(9) || active_branch();\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_branch)
  dl_rows=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'R' || char(9) || id || char(9) || v FROM t ORDER BY id;\n" "$setup" \
            | "$DOLTLITE" "$dir/dl/db.r" 2>>"$dir/dl.err" \
            | grep -v '^[0-9]*$' \
            | grep -v '^[0-9a-f]\{40\}$' \
            | normalize_rows)
  dl_status=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S' || char(9) || table_name || char(9) || staged || char(9) || status FROM dolt_status;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_status)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_branch dt_rows dt_status
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "SELECT concat('B', char(9), active_branch());"
      echo "SELECT concat('R', char(9), id, char(9), v) FROM t ORDER BY id;"
      echo "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_branch=$(tr -d '"' < "$dir/dt.raw" | normalize_branch)
  dt_rows=$(tr -d '"' < "$dir/dt.raw" | normalize_rows)
  dt_status=$(tr -d '"' < "$dir/dt.raw" | normalize_status)

  local dl_combined dt_combined
  dl_combined="$dl_branch"$'\n'"$dl_rows"$'\n'"$dl_status"
  dt_combined="$dt_branch"$'\n'"$dt_rows"$'\n'"$dt_status"

  vc_oracle_assert_match "$name" "$dl_combined" "$dt_combined"
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
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_errpost"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_post dt_post

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_post=$(printf ".headers off\n.mode list\n.separator '\t'\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_post=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"' | tr -d '\r')

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_post" = "$dt_post" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/post:"; { echo "$dl_rc"; echo "$dl_post"; } | sed 's/^/      /'
    echo "    dolt rc/post:"; { echo "$dt_rc"; echo "$dt_post"; } | sed 's/^/      /'
  fi
}

oracle_savepoint_poststate() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_post dt_post

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_post=$(printf ".headers off\n.mode list\n.separator '\t'\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_post=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"' | tr -d '\r')

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_post" = "$dt_post" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/post:"; { echo "$dl_rc"; echo "$dl_post"; } | sed 's/^/      /'
    echo "    dolt rc/post:"; { echo "$dt_rc"; echo "$dt_post"; } | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_checkout ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- branch switch ---"

oracle "switch_to_existing" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
"

oracle "switch_to_main_noop" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('main');
"

oracle "switch_then_back" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
"

oracle "switch_sees_branch_data" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 'feature_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
"

oracle "main_unchanged_after_branch_commit" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 'feature_only');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
"

oracle "txn_checkout_rollback_keeps_checked_out_branch_state" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v='feature_v' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
BEGIN;
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_checkout('feature');
ROLLBACK;
"

echo "--- create-and-switch (-b) ---"

oracle "dash_b_creates_and_switches" "
$SEED
SELECT dolt_checkout('-b', 'newfeat');
"

oracle "dash_b_then_commit" "
$SEED
SELECT dolt_checkout('-b', 'newfeat');
INSERT INTO t VALUES (2, 'b_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

oracle "dash_b_then_switch_back_to_main" "
$SEED
SELECT dolt_checkout('-b', 'newfeat');
INSERT INTO t VALUES (2, 'b_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('main');
"

oracle "dash_b_from_start_point" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 'feature_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b', 'newfeat', 'feature');
"

echo "--- per-table checkout ---"

oracle "revert_single_table_working" "
$SEED
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_checkout('t');
"

oracle "revert_multiple_tables_working" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='dirty_a' WHERE id=1;
UPDATE b SET v='dirty_b' WHERE id=1;
SELECT dolt_checkout('a', 'b');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t
SELECT id, v FROM a
UNION ALL
SELECT id + 10, v FROM b;
"

oracle "revert_table_with_uncommitted_insert" "
$SEED
INSERT INTO t VALUES (2, 'uncommitted');
SELECT dolt_checkout('t');
"

oracle "revert_table_with_delete" "
$SEED
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
DELETE FROM t WHERE id=2;
SELECT dolt_checkout('t');
"

oracle "checkout_table_from_branch_ref" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 'feature_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature', 't');
"

oracle "checkout_table_from_commit_ish_restores_dropped_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t');
SELECT dolt_checkout('HEAD~1', 't');
"

oracle "checkout_multiple_tables_from_branch_ref" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO a VALUES (2, 'feature_a');
INSERT INTO b VALUES (2, 'feature_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature', 'a', 'b');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t
SELECT id, v FROM a
UNION ALL
SELECT id + 10, v FROM b;
"

oracle "checkout_multitable_from_tag_ref" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('v1', 'a', 'b');
SELECT (SELECT v FROM a WHERE id=1) AS id, (SELECT v FROM b WHERE id=1) AS v;
"

oracle "checkout_multitable_from_commit_ish" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('HEAD~1', 'a', 'b');
SELECT (SELECT v FROM a WHERE id=1) AS id, (SELECT v FROM b WHERE id=1) AS v;
"

oracle "checkout_multitable_from_first_parent_shorthand" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('HEAD^', 'a', 'b');
SELECT (SELECT v FROM a WHERE id=1) AS id, (SELECT v FROM b WHERE id=1) AS v;
"

oracle "checkout_multitable_from_first_parent_explicit" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('HEAD^1', 'a', 'b');
SELECT (SELECT v FROM a WHERE id=1) AS id, (SELECT v FROM b WHERE id=1) AS v;
"

oracle "checkout_multitable_from_raw_commit_hash" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout(dolt_hashof('HEAD~1'), 'a', 'b');
SELECT (SELECT v FROM a WHERE id=1) AS id, (SELECT v FROM b WHERE id=1) AS v;
"

oracle "checkout_multitable_from_raw_first_parent_hash" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout(dolt_hashof('HEAD^1'), 'a', 'b');
SELECT (SELECT v FROM a WHERE id=1) AS id, (SELECT v FROM b WHERE id=1) AS v;
"

oracle "checkout_multitable_from_second_parent_ref" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO a VALUES (2, 'feat_a');
INSERT INTO b VALUES (2, 'feat_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2f');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main_a');
INSERT INTO b VALUES (3, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2m');
SELECT dolt_merge('feature');
SELECT dolt_checkout('HEAD^2', 'a', 'b');
SELECT (SELECT group_concat(v, ',') FROM (SELECT v FROM a ORDER BY id)) AS id,
       (SELECT group_concat(v, ',') FROM (SELECT v FROM b ORDER BY id)) AS v;
"

oracle "checkout_multitable_from_raw_second_parent_hash" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO a VALUES (2, 'feat_a');
INSERT INTO b VALUES (2, 'feat_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2f');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main_a');
INSERT INTO b VALUES (3, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2m');
SELECT dolt_merge('feature');
SELECT dolt_checkout(dolt_hashof('HEAD^2'), 'a', 'b');
SELECT (SELECT group_concat(v, ',') FROM (SELECT v FROM a ORDER BY id)) AS id,
       (SELECT group_concat(v, ',') FROM (SELECT v FROM b ORDER BY id)) AS v;
"

echo "--- error paths ---"

oracle_error "checkout_nonexistent" "
$SEED
SELECT dolt_checkout('nope');
"

oracle_error "dash_b_existing_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('-b', 'feature');
"

oracle_error "no_args" "
$SEED
SELECT dolt_checkout();
"

oracle_error "dash_b_no_name" "
$SEED
SELECT dolt_checkout('-b');
"

oracle_error "dash_b_empty_name" "
$SEED
SELECT dolt_checkout('-b', '');
"

oracle_error "dash_b_bad_start_point" "
$SEED
SELECT dolt_checkout('-b', 'newfeat', 'does-not-exist');
"

oracle_error "checkout_table_from_missing_ref" "
$SEED
SELECT dolt_checkout('does-not-exist', 't');
"

oracle_error "checkout_branch_with_missing_table" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature', 'nope');
"

oracle_error_poststate "checkout_explicit_source_missing_table_no_partial_mutation" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO a VALUES (2, 'main_a');
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO b VALUES (1, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('HEAD~1', 'a', 'b');
" "SELECT concat((SELECT count(*) FROM a), char(9), (SELECT count(*) FROM b));"

oracle_error_poststate "checkout_tag_source_missing_table_no_partial_mutation" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout('v1', 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1));"

oracle_error_poststate "checkout_tag_source_missing_table_preserves_staged_and_working_state" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
UPDATE a SET v='stage_a' WHERE id=1;
UPDATE b SET v='dirty_b' WHERE id=1;
SELECT dolt_add('a');
SELECT dolt_checkout('v1', 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='a' AND staged=1 AND status='modified'), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='b' AND staged=0 AND status='modified'));"

oracle_error_poststate "checkout_branch_source_missing_table_no_partial_mutation" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE a SET v='feature_a' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
UPDATE a SET v='dirty_a' WHERE id=1;
UPDATE b SET v='dirty_b' WHERE id=1;
SELECT dolt_checkout('feature', 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1));"

oracle_error_poststate "checkout_branch_source_missing_table_preserves_staged_and_working_state" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE a SET v='feature_a' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
UPDATE a SET v='stage_a' WHERE id=1;
UPDATE b SET v='dirty_b' WHERE id=1;
SELECT dolt_add('a');
SELECT dolt_checkout('feature', 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='a' AND staged=1 AND status='modified'), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='b' AND staged=0 AND status='modified'));"

oracle_error_poststate "checkout_raw_hash_source_missing_table_no_partial_mutation" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_checkout(dolt_hashof('HEAD~1'), 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1));"

oracle_error_poststate "checkout_raw_hash_source_missing_table_preserves_staged_and_working_state" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='stage_a' WHERE id=1;
UPDATE b SET v='dirty_b' WHERE id=1;
SELECT dolt_add('a');
SELECT dolt_checkout(dolt_hashof('HEAD'), 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='a' AND staged=1 AND status='modified'), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='b' AND staged=0 AND status='modified'));"

oracle_error_poststate "checkout_second_parent_source_missing_table_no_partial_mutation" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO a VALUES (2, 'feat_a');
INSERT INTO b VALUES (2, 'feat_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2f');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main_a');
INSERT INTO b VALUES (3, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2m');
SELECT dolt_merge('feature');
SELECT dolt_checkout('HEAD^2', 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM a WHERE id=2), char(9), (SELECT v FROM a WHERE id=3), char(9), (SELECT v FROM b WHERE id=1), char(9), (SELECT v FROM b WHERE id=2), char(9), (SELECT v FROM b WHERE id=3));"

oracle_error_poststate "checkout_raw_second_parent_hash_missing_table_no_partial_mutation" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO a VALUES (2, 'feat_a');
INSERT INTO b VALUES (2, 'feat_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2f');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main_a');
INSERT INTO b VALUES (3, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2m');
SELECT dolt_merge('feature');
SELECT dolt_checkout(dolt_hashof('HEAD^2'), 'a', 'missing');
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM a WHERE id=2), char(9), (SELECT v FROM a WHERE id=3), char(9), (SELECT v FROM b WHERE id=1), char(9), (SELECT v FROM b WHERE id=2), char(9), (SELECT v FROM b WHERE id=3));"

echo "--- savepoint parity ---"

oracle_savepoint_poststate "savepoint_checkout_existing_branch_reopens_on_original_branch" "
$SEED
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
UPDATE t SET v='other' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'other');
SELECT dolt_checkout('main');
SAVEPOINT sp1;
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_checkout('other');
ROLLBACK TO sp1;
" "SELECT concat(active_branch(), char(9), (SELECT v FROM t WHERE id=1));"

oracle_savepoint_poststate "savepoint_checkout_dash_b_keeps_new_branch_but_reopens_original_branch" "
$SEED
SAVEPOINT sp1;
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_checkout('-b', 'side');
ROLLBACK TO sp1;
" "SELECT concat(active_branch(), char(9), (SELECT v FROM t WHERE id=1), char(9), (SELECT group_concat(name, ',') FROM dolt_branches));"

oracle_savepoint_poststate "savepoint_checkout_missing_invalidates" "
$SEED
SAVEPOINT sp1;
SELECT dolt_checkout('does-not-exist');
ROLLBACK TO sp1;
" "SELECT active_branch();"

oracle_savepoint_poststate "savepoint_checkout_tag_source_invalidates" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SAVEPOINT sp1;
SELECT dolt_checkout('v1', 'a', 'b');
ROLLBACK TO sp1;
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1));"

oracle_savepoint_poststate "savepoint_checkout_branch_source_invalidates" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE a SET v='feature_a' WHERE id=1;
UPDATE b SET v='feature_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_feat');
SELECT dolt_checkout('main');
UPDATE a SET v='dirty_a' WHERE id=1;
UPDATE b SET v='dirty_b' WHERE id=1;
SAVEPOINT sp1;
SELECT dolt_checkout('feature', 'a', 'b');
ROLLBACK TO sp1;
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1));"

oracle_savepoint_poststate "savepoint_checkout_raw_hash_source_invalidates" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v='main_a' WHERE id=1;
UPDATE b SET v='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SAVEPOINT sp1;
SELECT dolt_checkout(dolt_hashof('HEAD~1'), 'a', 'b');
ROLLBACK TO sp1;
" "SELECT concat((SELECT v FROM a WHERE id=1), char(9), (SELECT v FROM b WHERE id=1));"

oracle_savepoint_poststate "savepoint_checkout_second_parent_source_invalidates" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO a VALUES (2, 'feat_a');
INSERT INTO b VALUES (2, 'feat_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2f');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main_a');
INSERT INTO b VALUES (3, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2m');
SELECT dolt_merge('feature');
SAVEPOINT sp1;
SELECT dolt_checkout('HEAD^2', 'a', 'b');
ROLLBACK TO sp1;
" "SELECT concat(active_branch(), char(9), IFNULL((SELECT v FROM a WHERE id=1), ''), char(9), IFNULL((SELECT v FROM a WHERE id=2), ''), char(9), IFNULL((SELECT v FROM a WHERE id=3), ''), char(9), IFNULL((SELECT v FROM b WHERE id=1), ''), char(9), IFNULL((SELECT v FROM b WHERE id=2), ''), char(9), IFNULL((SELECT v FROM b WHERE id=3), ''), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='a' AND staged=1 AND status='modified'), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='b' AND staged=1 AND status='modified'));"

oracle_savepoint_poststate "savepoint_checkout_raw_second_parent_hash_invalidates" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO a VALUES (2, 'feat_a');
INSERT INTO b VALUES (2, 'feat_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2f');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main_a');
INSERT INTO b VALUES (3, 'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2m');
SELECT dolt_merge('feature');
SAVEPOINT sp1;
SELECT dolt_checkout(dolt_hashof('HEAD^2'), 'a', 'b');
ROLLBACK TO sp1;
" "SELECT concat(active_branch(), char(9), IFNULL((SELECT v FROM a WHERE id=1), ''), char(9), IFNULL((SELECT v FROM a WHERE id=2), ''), char(9), IFNULL((SELECT v FROM a WHERE id=3), ''), char(9), IFNULL((SELECT v FROM b WHERE id=1), ''), char(9), IFNULL((SELECT v FROM b WHERE id=2), ''), char(9), IFNULL((SELECT v FROM b WHERE id=3), ''), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='a' AND staged=1 AND status='modified'), char(9), (SELECT count(*) FROM dolt_status WHERE table_name='b' AND staged=1 AND status='modified'));"

echo ""
echo "--- Schema objects (views, triggers, indexes) ---"

# Schema-object observability diverges (sqlite_master vs dolt_schemas /
# information_schema) and trigger bodies and DROP INDEX cannot share one
# script across dialects, so these run per-system setups and queries and
# compare the projected results.
oracle_dual_poststate() {
  local name="$1" dl_setup="$2" dt_setup="$3" dl_query="$4" dt_query="$5"
  local dir="$TMPROOT/${name}_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  # The observation runs in the SAME session as the setup: dolt_checkout is
  # session-scoped, so a fresh session would observe the default branch.
  # Queries tag their rows (Q|, V|, I|) and everything else is filtered out.
  local dl_post
  dl_post=$(printf "%s\n.headers off\n.mode list\n%s\n" "$dl_setup" "$dl_query" \
            | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
            | tr -d '\r' \
            | grep -aE '^[QVI]\|')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
  local dt_post
  dt_post=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s\n' "$dt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" \
      | tr -d '"\r' \
      | grep -aE '^[QVI]\|'
  )

  vc_oracle_assert_match "$name" "$dl_post" "$dt_post"
}

DL_VIEW_COUNT="SELECT 'V|' || count(*) FROM sqlite_master WHERE type = 'view';"
DT_VIEW_COUNT="SELECT concat('V|', count(*)) FROM dolt_schemas WHERE type = 'view';"
DL_IDX_COUNT="SELECT 'I|' || count(*) FROM sqlite_master WHERE name = 'idx';"
DT_IDX_COUNT="SELECT concat('I|', count(*)) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx';"

oracle_dual_poststate "checkout_unstaged_view_stays_behind" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_checkout('feature');
" "
CREATE TABLE t(a INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_checkout('feature');
" "$DL_VIEW_COUNT" "$DT_VIEW_COUNT"

oracle_dual_poststate "checkout_unstaged_view_survives_roundtrip" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
" "
CREATE TABLE t(a INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
" "$DL_VIEW_COUNT" "$DT_VIEW_COUNT"

oracle_dual_poststate "checkout_branch_local_view" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1), (2);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
" "
CREATE TABLE t(a INT PRIMARY KEY);
INSERT INTO t VALUES (1), (2);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
" "SELECT 'Q|' || group_concat(a, ',') FROM (SELECT a FROM v ORDER BY a);" \
"SELECT concat('Q|', group_concat(a ORDER BY a SEPARATOR ',')) FROM v;"

oracle_dual_poststate "checkout_view_absent_on_main" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
" "
CREATE TABLE t(a INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
" "$DL_VIEW_COUNT" "$DT_VIEW_COUNT"

oracle_dual_poststate "checkout_dirty_view_modify_switches" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
DROP VIEW v;
CREATE VIEW v AS SELECT a, b FROM t;
SELECT dolt_checkout('feature');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am', 'feat_view');
SELECT dolt_checkout('main');
DROP VIEW v;
CREATE VIEW v AS SELECT a, b FROM t;
SELECT dolt_checkout('feature');
" "SELECT 'Q|' || group_concat(b, ',') FROM (SELECT b FROM v ORDER BY b);" \
"SELECT concat('Q|', group_concat(b ORDER BY b SEPARATOR ',')) FROM v;"

oracle_error "checkout_view_by_name_errors" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am', 'base');
DROP VIEW v;
SELECT dolt_checkout('v');
"

oracle_dual_poststate "checkout_branch_local_trigger_fires" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
CREATE TABLE audit(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (5);
" "
CREATE TABLE t(a INT PRIMARY KEY);
CREATE TABLE audit(a INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (5);
" "SELECT 'Q|' || count(*) || '|' || coalesce((SELECT a FROM audit), '~') FROM audit;" \
"SELECT concat('Q|', count(*), '|', coalesce((SELECT a FROM audit), '~')) FROM audit;"

oracle_dual_poststate "checkout_trigger_inert_on_main" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
CREATE TABLE audit(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.a); END;
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (5);
" "
CREATE TABLE t(a INT PRIMARY KEY);
CREATE TABLE audit(a INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.a);
SELECT dolt_commit('-Am', 'feat_trigger');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (5);
" "SELECT 'Q|' || count(*) FROM audit;" \
"SELECT concat('Q|', count(*)) FROM audit;"

oracle_dual_poststate "checkout_unstaged_index_stays_behind" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
CREATE INDEX idx ON t(b);
SELECT dolt_checkout('feature');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
CREATE INDEX idx ON t(b);
SELECT dolt_checkout('feature');
" "$DL_IDX_COUNT" "$DT_IDX_COUNT"

oracle_dual_poststate "checkout_branch_local_index" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am', 'feat_index');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10), (2, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am', 'feat_index');
SELECT dolt_checkout('main');
SELECT dolt_checkout('feature');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT group_concat(a, ',') FROM (SELECT a FROM t WHERE b = 10 ORDER BY a));" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT group_concat(a ORDER BY a SEPARATOR ',') FROM t WHERE b = 10));"

oracle_dual_poststate "checkout_table_restores_dropped_index" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE INDEX idx ON t(b);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx;
UPDATE t SET b = 99;
SELECT dolt_checkout('t');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE INDEX idx ON t(b);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t DROP INDEX idx;
UPDATE t SET b = 99;
SELECT dolt_checkout('t');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT b FROM t WHERE a = 1);" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT b FROM t WHERE a = 1));"

oracle_dual_poststate "checkout_table_removes_added_index" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx ON t(b);
UPDATE t SET b = 99;
SELECT dolt_checkout('t');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx ON t(b);
UPDATE t SET b = 99;
SELECT dolt_checkout('t');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT b FROM t WHERE a = 1);" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT b FROM t WHERE a = 1));"

oracle_dual_poststate "checkout_table_restores_index_definition" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT, c INT);
CREATE INDEX idx ON t(b);
INSERT INTO t VALUES (1, 10, 100);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx;
CREATE INDEX idx ON t(c);
SELECT dolt_checkout('t');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT);
CREATE INDEX idx ON t(b);
INSERT INTO t VALUES (1, 10, 100);
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t DROP INDEX idx;
CREATE INDEX idx ON t(c);
SELECT dolt_checkout('t');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx' AND sql LIKE '%(b)') || '|' || (SELECT b FROM t WHERE a = 1);" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx' AND column_name = 'b'), '|', (SELECT b FROM t WHERE a = 1));"

oracle_dual_poststate "checkout_cross_branch_table_with_index" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE INDEX idx ON t(b);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
DROP INDEX idx;
UPDATE t SET b = 77;
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main', 't');
" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE INDEX idx ON t(b);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
ALTER TABLE t DROP INDEX idx;
UPDATE t SET b = 77;
SELECT dolt_commit('-Am', 'feat_change');
SELECT dolt_checkout('main', 't');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT b FROM t WHERE a = 1);" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT b FROM t WHERE a = 1));"

# ROLLBACK does not revert dolt_checkout: the session stays on the branch it
# checked out, and later writes land there. Reported once as state corruption
# because dolt_default_branch() still says "main" -- that is the repo default,
# not the session branch. Dolt behaves the same way, so this pins the parity and
# keeps someone from "fixing" it into a divergence.
oracle "checkout_survives_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 'feature_wip');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 'main_wip');
BEGIN;
SELECT dolt_checkout('feature');
ROLLBACK;
"


echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
