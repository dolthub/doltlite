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

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT CONCAT('ROW|TOTAL|', count(*)) FROM dolt_commit_ancestors;
SELECT CONCAT('ROW|ROOTS|', count(*)) FROM dolt_commit_ancestors WHERE parent_hash IS NULL;
SELECT CONCAT('ROW|DISTINCT_COMMITS|', count(DISTINCT commit_hash)) FROM dolt_commit_ancestors;
SELECT CONCAT('ROW|IDX|', parent_index, '|', count(*)) FROM dolt_commit_ancestors GROUP BY parent_index ORDER BY parent_index;"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ROW|' \
           | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ROW|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_commit_ancestors ==="
echo ""

echo "--- linear chains ---"

oracle "init_only" ""

oracle "single_user_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

oracle "three_linear_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
"

echo "--- merge commits ---"

oracle "merge_commit_two_parents" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c');
SELECT dolt_merge('feat');
"

oracle "two_merges" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c0');
SELECT dolt_checkout('-b', 'a');
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main1');
SELECT dolt_merge('a');
SELECT dolt_checkout('-b', 'b');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (20, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('b');
"

oracle "fast_forward_no_merge_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'f1');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"

echo "--- detached subgraph (only-on-feat doesn't appear from main HEAD) ---"

oracle "branch_not_merged_invisible_from_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feat');
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_only');
"

oracle "ancestors_from_feat_head" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_only');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_only');
SELECT dolt_checkout('feat');
"

echo "--- consistency with dolt_log ---"

oracle "ancestors_count_matches_log" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c');
SELECT dolt_merge('feat');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
