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

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
