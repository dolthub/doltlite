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
  local name="$1" setup="$2" ref1="$3" ref2="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT CONCAT('ANS|', coalesce((SELECT message FROM dolt_log WHERE commit_hash = dolt_merge_base($ref1, $ref2)), 'NULL'));"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ANS|' \
           | sed 's/^ANS|//')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ANS|' | sed 's/^ANS|//')

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_in_set() {
  local name="$1" setup="$2" ref1="$3" ref2="$4" expect_set="$5"
  local dir="$TMPROOT/${name}_set"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT CONCAT('ANS|', coalesce((SELECT message FROM dolt_log WHERE commit_hash = dolt_merge_base($ref1, $ref2)), 'NULL'));"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ANS|' \
           | sed 's/^ANS|//')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ANS|' | sed 's/^ANS|//')

  local dl_ok=0 dt_ok=0
  for cand in $expect_set; do
    [ "$dl_out" = "$cand" ] && dl_ok=1
    [ "$dt_out" = "$cand" ] && dt_ok=1
  done

  if [ "$dl_ok" = 1 ] && [ "$dt_ok" = 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both in set: $expect_set)"
    echo "    doltlite: $dl_out"
    echo "    dolt:     $dt_out"
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

echo "=== Version Control Oracle Tests: dolt_merge_base ==="
echo ""

LINEAR="
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

BRANCHED="
$LINEAR
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c1');
INSERT INTO t VALUES (11, 110);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c4');
"

echo "--- linear history ---"

oracle "linear_self" "$LINEAR" "'main'" "'main'"
oracle "linear_head_head" "$LINEAR" "'HEAD'" "'HEAD'"
oracle "linear_head_parent" "$LINEAR" "'HEAD'" "'HEAD~1'"
oracle "linear_parent_head" "$LINEAR" "'HEAD~1'" "'HEAD'"
oracle "linear_head_grandparent" "$LINEAR" "'HEAD'" "'HEAD~2'"

echo "--- two branches off a common commit ---"

oracle "branched_main_feat" "$BRANCHED" "'main'" "'feat'"
oracle "branched_feat_main" "$BRANCHED" "'feat'" "'main'"
oracle "branched_head_feat" "$BRANCHED" "'HEAD'" "'feat'"

echo "--- post-merge ---"

POST_MERGE="
$BRANCHED
SELECT dolt_merge('feat');
"

oracle "merged_main_feat" "$POST_MERGE" "'main'" "'feat'"
oracle "merged_feat_head" "$POST_MERGE" "'feat'" "'HEAD'"
oracle "merged_second_parent_self" "$POST_MERGE" "'HEAD^2'" "'HEAD^2'"
oracle "merged_second_parent_vs_first_parent" "$POST_MERGE" "'HEAD^2'" "'HEAD~1'"

echo "--- tag refs ---"

WITH_TAG="
$LINEAR
SELECT dolt_tag('v1', (SELECT commit_hash FROM dolt_log WHERE message='c1'));
SELECT dolt_branch('from_tag', 'v1');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
"

oracle "tag_vs_head" "$WITH_TAG" "'v1'" "'HEAD'"
oracle "tag_vs_tag" "$WITH_TAG" "'v1'" "'v1'"
oracle "branch_from_tag_vs_main" "$WITH_TAG" "'from_tag'" "'main'"

echo "--- bare commit hash ref ---"

oracle "hash_vs_branch" "$LINEAR" "(SELECT commit_hash FROM dolt_log WHERE message='c1')" "'main'"

echo "--- copied and renamed branches ---"

WITH_COPY="
$LINEAR
SELECT dolt_branch('src');
SELECT dolt_branch('-c', 'src', 'copy');
"

oracle "copied_branch_vs_source" "$WITH_COPY" "'src'" "'copy'"

WITH_RENAME="
$LINEAR
SELECT dolt_branch('old');
SELECT dolt_branch('-m', 'old', 'renamed');
"

oracle "renamed_branch_vs_main" "$WITH_RENAME" "'main'" "'renamed'"

echo "--- criss-cross ---"

CRISS_CROSS="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base', '--date', '2020-01-01T00:00:00');
SELECT dolt_branch('A');
SELECT dolt_branch('B');
SELECT dolt_checkout('A');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a1', '--date', '2030-01-01T00:00:00');
SELECT dolt_checkout('B');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b1', '--date', '2020-06-01T00:00:00');
SELECT dolt_branch('C', 'A');
SELECT dolt_branch('D', 'B');
SELECT dolt_checkout('C');
SELECT dolt_merge('B');
SELECT dolt_checkout('D');
SELECT dolt_merge('A');
SELECT dolt_checkout('C');
"

oracle_in_set "criss_cross_c_d" "$CRISS_CROSS" "'C'" "'D'" "a1 b1"
oracle_in_set "criss_cross_d_c" "$CRISS_CROSS" "'D'" "'C'" "a1 b1"
oracle "criss_cross_c_c" "$CRISS_CROSS" "'C'" "'C'"

CRISS_CROSS_REV="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base', '--date', '2020-01-01T00:00:00');
SELECT dolt_branch('A');
SELECT dolt_branch('B');
SELECT dolt_checkout('A');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a1', '--date', '2020-06-01T00:00:00');
SELECT dolt_checkout('B');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b1', '--date', '2030-01-01T00:00:00');
SELECT dolt_branch('C', 'A');
SELECT dolt_branch('D', 'B');
SELECT dolt_checkout('C');
SELECT dolt_merge('B');
SELECT dolt_checkout('D');
SELECT dolt_merge('A');
SELECT dolt_checkout('C');
"

oracle_in_set "criss_cross_rev_c_d" "$CRISS_CROSS_REV" "'C'" "'D'" "a1 b1"

DEEP_CRISS_CROSS="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base', '--date', '2020-01-01T00:00:00');
SELECT dolt_branch('A');
SELECT dolt_branch('B');
SELECT dolt_checkout('A');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a1', '--date', '2030-01-01T00:00:00');
SELECT dolt_checkout('B');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b1', '--date', '2020-06-01T00:00:00');
SELECT dolt_branch('C', 'A');
SELECT dolt_branch('D', 'B');
SELECT dolt_checkout('C');
SELECT dolt_merge('B');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2', '--date', '2030-02-01T00:00:00');
SELECT dolt_checkout('D');
SELECT dolt_merge('A');
INSERT INTO t VALUES (5, 50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'd2', '--date', '2030-03-01T00:00:00');
SELECT dolt_checkout('C');
"

oracle_in_set "deep_criss_cross_c_d" "$DEEP_CRISS_CROSS" "'C'" "'D'" "a1 b1"

echo "--- multi-merge fan-in ---"

FANIN="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'm1', '--date', '2020-01-01T00:00:00');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'f1', '--date', '2020-02-01T00:00:00');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'm2', '--date', '2020-03-01T00:00:00');
SELECT dolt_merge('feat');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'm3', '--date', '2020-04-01T00:00:00');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (5, 50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'f2', '--date', '2020-05-01T00:00:00');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"

oracle "fanin_main_feat" "$FANIN" "'main'" "'feat'"
oracle "fanin_feat_main" "$FANIN" "'feat'" "'main'"
oracle "fanin_head_feat" "$FANIN" "'HEAD'" "'feat'"
oracle "fanin_head_parent_feat" "$FANIN" "'HEAD~1'" "'feat'"

echo "--- error paths ---"

oracle_error "bad_ref1" "
$LINEAR
SELECT dolt_merge_base('nope', 'main');
"

oracle_error "bad_ref2" "
$LINEAR
SELECT dolt_merge_base('main', 'nope');
"

oracle_error "no_args" "
$LINEAR
SELECT dolt_merge_base();
"

oracle_error "one_arg" "
$LINEAR
SELECT dolt_merge_base('main');
"

oracle_error "three_args" "
$LINEAR
SELECT dolt_merge_base('main', 'main', 'main');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
