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
  tr -d '\r' | awk -F'\t' '
    {
      h = $2
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $2 = seen[h]
      out = $1
      for (i = 2; i <= NF; i++) out = out "\t" $i
      print out
    }
  '
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT name || char(9) || hash || char(9) || latest_commit_message || char(9) || remote || char(9) || branch || char(9) || dirty FROM dolt_branches ORDER BY name'

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
    "$DOLT" sql -r csv -q "SELECT concat(name, char(9), hash, char(9), latest_commit_message, char(9), remote, char(9), branch, char(9), dirty) FROM dolt_branches ORDER BY name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(vc_oracle_tail_csv_body "$dir/dt.raw" \
           | tr -d '"' \
           | sed -E 's/\ttrue$/\t1/; s/\tfalse$/\t0/' \
           | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_branches ==="
echo ""

echo "--- baseline ---"

oracle "default_branch_only" "
SELECT 1;
"

echo "--- single commit ---"

oracle "main_after_one_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
"

echo "--- branch creation ---"

oracle "create_branch_from_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('feature');
"

oracle "create_two_branches_share_head" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('feature');
SELECT dolt_branch('experiment');
"

echo "--- divergence ---"

oracle "branches_at_different_heads" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feature_c2');
"

echo "--- deletion ---"

oracle "delete_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('temp');
SELECT dolt_branch('-d', 'temp');
"

echo "--- dirty bit ---"

oracle "dirty_after_uncommitted_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (2);
"

oracle "dirty_after_staged_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (2);
SELECT dolt_add('-A');
"

oracle "clean_after_commit_no_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
"

echo "--- branch at older commit ---"

oracle "branch_at_head_minus_one" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'second');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'third');
SELECT dolt_branch('back_one', 'HEAD~1');
SELECT dolt_branch('back_two', 'HEAD~2');
"

echo "--- branch copy ---"

oracle "branch_copy_main_to_clone" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('-c', 'main', 'clone');
"

echo "--- branch rename ---"

oracle "branch_rename_non_current" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('old_name');
SELECT dolt_branch('-m', 'old_name', 'new_name');
"

echo "--- multi-branch states ---"

oracle "three_branches_three_heads" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_branch('back', 'HEAD~1');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_only');
"

oracle "other_branch_dirty_bit_untracked" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SELECT dolt_branch('feature');
SELECT dolt_branch('experiment');
INSERT INTO t VALUES (2);
"

echo "--- branch after merge ---"

oracle "main_head_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
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

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
