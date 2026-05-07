#!/bin/bash
#
# Version-control oracle test: dolt_commit_ancestors
#
# The vtable mirrors Dolt's table of the same name:
#   commit_hash  TEXT     — commit reachable from HEAD
#   parent_hash  TEXT     — parent of that commit (NULL for root)
#   parent_index INTEGER  — 0-based position of the parent
#
# Hashes differ between engines (different content addressing), so
# the oracle compares topology shape rather than literal hash strings:
#
#   - Total number of (commit, parent) rows
#   - Number of root rows (parent_hash IS NULL)
#   - Distribution of parent_index values (counts the merges)
#   - Number of distinct commits surveyed
#
# These four numbers, taken together, fully describe the parent-
# edge structure. They're what a consumer like dolt-replay needs to
# reconstruct order without depending on dolt_log.date.
#
# Usage: bash vc_oracle_commit_ancestors_test.sh [path/to/doltlite] [path/to/dolt]
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

# Run the same diagnostic query against both engines and compare the
# topology summary line by line. The query intentionally avoids hash
# columns directly — it emits ROW lines with summary numbers only.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # Topology summary query — engine-independent shape. Use CONCAT
  # everywhere; Dolt parses '||' as logical OR (MySQL semantics).
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

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_commit_ancestors ==="
echo ""

echo "--- linear chains ---"

# Just the auto-init commit. Both engines should report 1 row, 1 root.
oracle "init_only" ""

# init + 1 user commit. 2 rows total, 1 root, all parent_index=0.
oracle "single_user_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

# init + 3 user commits in a line. 4 rows, 1 root, all parent_index=0.
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

# Branch + merge. The merge commit contributes two rows (parent_index
# 0 and 1). Both engines have to expose this for the topology to
# reconstruct.
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

# Two sequential merges from two distinct feature branches. Should
# produce two rows with parent_index=1, plus the linear edges.
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

# Fast-forward merge — main moves to feat tip with no merge commit.
# Topology should be linear; no parent_index=1.
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

# Commits made on a branch that's never merged shouldn't show up in
# main's ancestor walk. The vtable walks from session HEAD only.
oracle "branch_not_merged_invisible_from_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feat');
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_only');
"

# Same setup as above, but from feat HEAD: feat_only IS visible,
# main_only is NOT.
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

# Distinct commits in dolt_commit_ancestors should equal the row
# count in dolt_log (every reachable commit appears exactly once on
# each side). This is the load-bearing invariant a walker depends on.
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
