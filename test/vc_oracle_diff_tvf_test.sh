#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_error() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_err"
  local dl_rc dt_rc dolt_setup dolt_query
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" \
    "$dir/dl.err" "$(printf '%s\n%s\n' "$setup" "$query")"
  dl_rc=$?

  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" \
    "$dir/dt.err" "$(printf '%s\n%s\n' "$dolt_setup" "$dolt_query")"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (doltlite rc=$dl_rc, dolt rc=$dt_rc)"
  fi
}

oracle_reopen() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  printf "%s\n" "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.setup" 2>"$dir/dl.setup.err"
  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n%s\n" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.setup.err"
    echo "$dolt_query" | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_diff TVF form ==="
echo ""

SETUP_LINEAR="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1), (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (3, 3);
UPDATE t SET v = 20 WHERE id = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c3');
"

echo "--- linear history slice ---"

oracle "slice_one_commit" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "slice_two_commits" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~2', 'HEAD');"

oracle "slice_full_range" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~3', 'HEAD');"

echo "--- ref types ---"

oracle "slice_branch_refs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_c1');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('main', 'feat');"

oracle_reopen "slice_table_only_on_sibling_branch" "
SELECT dolt_checkout('-b', 'feature');
CREATE TABLE feature_only(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO feature_only VALUES(1, 'feature');
SELECT dolt_commit('-A', '-m', 'feature-only table');
SELECT dolt_checkout('main');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', diff_type) FROM dolt_diff_feature_only('main', 'feature');"

oracle "slice_tag_refs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'v1');
SELECT dolt_tag('v1', (SELECT commit_hash FROM dolt_log WHERE message='v1'));
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'v2');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('v1', 'HEAD');"

echo "--- WORKING ref ---"

oracle "slice_to_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
INSERT INTO t VALUES (99, 99);
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD', 'WORKING');"

echo "--- STAGED ref ---"

oracle "slice_head_to_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD', 'STAGED');"

oracle "slice_staged_to_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
INSERT INTO t VALUES (2, 2);
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('STAGED', 'WORKING');"

echo "--- no diff (same ref both sides) ---"

oracle "slice_no_change" "$SETUP_LINEAR" \
  "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD', 'HEAD');" \
  "EXPECT_EMPTY"

echo "--- divergent history ---"

SETUP_DIVERGENT="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
INSERT INTO t VALUES(1, 'main only');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main change');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2, 'feature only');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feature change');
"

oracle "divergent_two_ref" "$SETUP_DIVERGENT" \
  "SELECT CONCAT('R|', IFNULL(from_id,to_id), '|', diff_type) FROM dolt_diff_t('main', 'feature');"

oracle "divergent_two_dot" "$SETUP_DIVERGENT" \
  "SELECT CONCAT('R|', IFNULL(from_id,to_id), '|', diff_type) FROM dolt_diff_t('main..feature');"

oracle "divergent_three_dot_feature" "$SETUP_DIVERGENT" \
  "SELECT CONCAT('R|', IFNULL(from_id,to_id), '|', diff_type) FROM dolt_diff_t('main...feature');"

oracle "divergent_three_dot_main" "$SETUP_DIVERGENT" \
  "SELECT CONCAT('R|', IFNULL(from_id,to_id), '|', diff_type) FROM dolt_diff_t('feature...main');"

oracle "log_two_dot" "$SETUP_DIVERGENT" \
  "SELECT CONCAT('R|', message) FROM dolt_log('main..feature');"

oracle "log_three_dot" "$SETUP_DIVERGENT" \
  "SELECT CONCAT('R|', message) FROM dolt_log('main...feature');"

oracle_error "diff_two_dot_missing_left" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('..feature');"

oracle_error "diff_two_dot_missing_right" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('feature..');"

oracle_error "diff_three_dot_missing_left" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('...feature');"

oracle_error "diff_three_dot_missing_right" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('feature...');"

oracle_error "diff_slice_unknown_from_ref" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('nosuchref', 'feature');"

oracle_error "diff_slice_unknown_to_ref" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('feature', 'nosuchref');"

oracle_error "diff_slice_unknown_hash_prefix" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('abc123', 'feature');"

oracle_error "diff_slice_invalid_ancestor" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('HEAD~99', 'feature');"

oracle_error "diff_slice_null_from_ref" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t(NULL, 'feature');"

oracle_error "diff_slice_null_to_ref" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('main', NULL);"

oracle_error "diff_range_unknown_ref" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_diff_t('nosuchref..feature');"

oracle_error "log_two_dot_missing_left" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_log('..feature');"

oracle_error "log_two_dot_missing_right" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_log('feature..');"

oracle_error "log_three_dot_missing_left" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_log('...feature');"

oracle_error "log_three_dot_missing_right" "$SETUP_DIVERGENT" \
  "SELECT count(*) FROM dolt_log('feature...');"

echo "--- multi-column table ---"

SETUP_MULTI="
CREATE TABLE m(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO m VALUES (1, 1, 'one'), (1, 2, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
UPDATE m SET v = 'TWO' WHERE a = 1 AND b = 2;
INSERT INTO m VALUES (2, 1, 'three');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "slice_multi_col" "$SETUP_MULTI" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_m('HEAD~1', 'HEAD');"

echo "--- historical sides render by column name ---"

SETUP_NAMEMAP="
CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(32), c VARCHAR(32));
INSERT INTO t VALUES(1,'BEE','CEE');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','drop_b');
"

oracle "namemap_from_side_after_col_drop" "$SETUP_NAMEMAP" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_c,''), '|', IFNULL(from_a,''), '|', IFNULL(from_c,''), '|', diff_type) FROM dolt_diff_t('HEAD~1','HEAD');"

oracle "namemap_to_side_at_old_commit" "$SETUP_NAMEMAP" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_c,''), '|', IFNULL(from_a,''), '|', IFNULL(from_c,''), '|', diff_type) FROM dolt_diff_t('HEAD~2','HEAD~1');"

SETUP_READD="
CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(32), c VARCHAR(32));
INSERT INTO t VALUES(1,'BEE','CEE');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
ALTER TABLE t DROP COLUMN b;
ALTER TABLE t ADD COLUMN b VARCHAR(32);
UPDATE t SET b='NEWBEE';
SELECT dolt_add('-A'); SELECT dolt_commit('-m','moved_b');
"

oracle "namemap_moved_column" "$SETUP_READD" \
  "SELECT CONCAT('R|', IFNULL(to_b,''), '|', IFNULL(to_c,''), '|', IFNULL(from_b,''), '|', IFNULL(from_c,''), '|', diff_type) FROM dolt_diff_t('HEAD~1','HEAD');"


echo "--- to_commit filter is a subset of head ancestry ---"

oracle "to_commit_foreign_branch_yields_nothing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_commit');
SELECT dolt_checkout('main');
" "SELECT CONCAT('R|', count(*)) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log('feat') LIMIT 1);"

oracle "to_commit_merge_emits_all_parents" "
CREATE TABLE t(k INTEGER PRIMARY KEY, v VARCHAR(32));
INSERT INTO t VALUES(100,'a'),(200,'b');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_branch('side');
UPDATE t SET v='main' WHERE k=100;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','mainc');
SELECT dolt_checkout('side');
UPDATE t SET v='side' WHERE k=200;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','sidec');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');
" "SELECT CONCAT('R|', IFNULL(to_k,''), '|', IFNULL(from_v,''), '|', IFNULL(to_v,''), '|', diff_type, '|', IFNULL(log_from.message, from_commit)) FROM dolt_diff_t dt LEFT JOIN dolt_log log_from ON log_from.commit_hash = dt.from_commit WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1);"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
