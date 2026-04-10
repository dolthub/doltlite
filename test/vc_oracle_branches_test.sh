#!/bin/bash
#
# Version-control oracle test: dolt_branches
#
# Runs identical branch-management scenarios against doltlite and Dolt and
# compares the normalized dolt_branches output. Catches divergence in how
# each engine reports branch listings, the latest-commit metadata for each
# branch, upstream tracking, and the per-branch dirty bit.
#
# Columns compared: name, hash (normalized), latest_commit_message,
# remote, branch, dirty. The committer/email/date columns are excluded
# because their values come from process/config and legitimately differ
# across the two engines.
#
# Usage: bash vc_oracle_branches_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Replace each distinct hash with H1, H2, ... in first-appearance order;
# strip CRLF.
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
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(name, char(9), hash, char(9), latest_commit_message, char(9), remote, char(9), branch, char(9), dirty) FROM dolt_branches ORDER BY name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  # Dolt prints "true"/"false" for the dirty tinyint(1); doltlite prints
  # "0"/"1". Map both to 0/1 before comparison.
  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" \
           | tr -d '"' \
           | sed -E 's/\ttrue$/\t1/; s/\tfalse$/\t0/' \
           | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  fi
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

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
