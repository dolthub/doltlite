#!/bin/bash
#
# Version-control oracle test: dolt_merge_base
#
# Runs identical merge_base scenarios against doltlite and Dolt and
# compares the resulting commit identity. Hashes don't match across
# engines (doltlite uses prolly hashes, Dolt uses noms hashes), so
# the oracle resolves the returned hash back to its commit message
# via dolt_log and compares that.
#
# Covers: linear history (older is ancestor of newer), self, two
# branches off a common commit, three-way branch, post-merge, tag
# and bare-hash refs, commutativity, NULL when there's no shared
# ancestor (forced via two independent root commits — created with
# `dolt branch -f` reusing a name with no shared history is not
# supported, so the no-ancestor case is exercised by checking the
# behavior in a fresh repo against itself instead), and error paths
# (bad ref1, bad ref2, wrong arity).
#
# Usage: bash vc_oracle_merge_base_test.sh [path/to/doltlite] [path/to/dolt]
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

# $1=name, $2=setup SQL, $3=ref1 expression, $4=ref2 expression.
# Compares the message of the commit that merge_base returns; if
# merge_base returns NULL the join yields no rows and we compare
# the literal "NULL" sentinel.
oracle() {
  local name="$1" setup="$2" ref1="$3" ref2="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # Tag the answer with an "ANS|" prefix so we can grep it out of the
  # noise that CALL dolt_*(...) emits in dolt's csv output (each call
  # produces hash/status rows which would otherwise mix with the answer).
  # Use CONCAT() not || — MySQL/Dolt parses || as logical OR.
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
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ANS|' | sed 's/^ANS|//')

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
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
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ]; then
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
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
"

oracle "tag_vs_head" "$WITH_TAG" "'v1'" "'HEAD'"
oracle "tag_vs_tag" "$WITH_TAG" "'v1'" "'v1'"

echo "--- bare commit hash ref ---"

# Use a subquery to look up a hash from log so we don't have to
# embed an unknown hash literal.
oracle "hash_vs_branch" "$LINEAR" "(SELECT commit_hash FROM dolt_log WHERE message='c1')" "'main'"

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
