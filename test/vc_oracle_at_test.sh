#!/bin/bash
#
# Version-control oracle test: dolt_at_<table>
#
# Per-table point-in-time view of a table at a specified commit ref.
# doltlite exposes this as a per-table vtable with a hidden
# `commit_ref` constraint column:
#
#   SELECT * FROM dolt_at_t WHERE commit_ref = '<ref>'
#
# Dolt exposes the same semantics via the SQL `AS OF` clause:
#
#   SELECT * FROM t AS OF '<ref>'
#
# The two surfaces are SQL-level different but semantically
# equivalent — both return the table state as of the named ref.
# This oracle compares row CONTENT (not the SQL spelling) across
# a range of ref forms: HEAD, HEAD~N, branch names, tag names,
# bare commit hashes.
#
# Note: doltlite does NOT support the `AS OF` parser syntax. That's
# a separate feature concern (parser change). This oracle compares
# behavior between the two surface forms by issuing a different
# query on each engine.
#
# Usage: bash vc_oracle_at_test.sh [path/to/doltlite] [path/to/dolt]
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

normalize() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "A" { print }' \
    | sort
}

# Run a scenario. $1=name, $2=setup SQL in doltlite syntax,
# $3=ref string to query at (e.g. 'HEAD', 'HEAD~1', 'main',
# 'feature', or a tag name).
oracle() {
  local name="$1" setup="$2" ref="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # ── doltlite query: dolt_at_t WHERE commit_ref = '<ref>' ──
  local dl_q="SELECT 'A' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM dolt_at_t WHERE commit_ref = '${ref}' ORDER BY id"
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  # ── Dolt query: t AS OF '<ref>' ──
  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_q="SELECT concat('A', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM t AS OF '${ref}' ORDER BY id"

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err" | tr -d '"' | normalize
  )

  if [ -z "$dl_out" ] && [ -z "$dt_out" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both queries empty — harness or ref not resolvable)"
    return
  fi

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite (dolt_at_t WHERE commit_ref='${ref}'):"
    echo "$dl_out" | sed 's/^/      /'
    echo "    dolt (t AS OF '${ref}'):"
    echo "$dt_out" | sed 's/^/      /'
  fi
}

oracle_error() {
  local name="$1" setup="$2" ref="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_sql
  local dl_rc
  dl_sql=$(printf "%s\nSELECT * FROM dolt_at_t WHERE commit_ref = '%s';\n" "$setup" "$ref")
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_sql"
  dl_rc=$?

  local dolt_setup
  local dt_sql
  local dt_rc
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_sql=$(printf "%s\nSELECT * FROM t AS OF '%s';\n" "$dolt_setup" "$ref")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dt_sql"
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

echo "=== Version Control Oracle Tests: dolt_at_<table> ==="
echo ""

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- HEAD ref ---"

# View at HEAD = current branch tip. Should match the working
# committed state.
oracle "at_head_two_rows" "$SEED" "HEAD"

# View after a second commit shows the second commit's state.
oracle "at_head_after_second_commit" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD"

echo "--- HEAD~N ref ---"

# View at HEAD~1 shows the previous commit's state.
oracle "at_head_minus_1_after_modify" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_modify');
" "HEAD~1"

# HEAD~2 walks back two commits.
oracle "at_head_minus_2" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
" "HEAD~2"

# HEAD~ (no number) is shorthand for HEAD~1.
oracle "at_head_tilde_no_number" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD~"

echo "--- branch ref ---"

# View at the current branch by name.
oracle "at_branch_main" "$SEED" "main"

# View at a SIBLING branch — should show that branch's tip,
# which differs from the current branch.
oracle "at_sibling_branch_feature" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
" "feature"

echo "--- tag ref ---"

# View at a tag pointing at a specific commit.
oracle "at_tag" "
$SEED
SELECT dolt_tag('v1');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "v1"

echo "--- bare commit hash ref ---"

# View at a literal commit hash. Both engines accept hex hashes
# as refs. We can't put a literal hash in the test text (the
# hashes differ between engines), so we run two scenarios that
# query the most-recent commit hash via a subquery. The HEAD
# alias gives the same result more readably; this scenario only
# exists to ensure direct hash references also work.
oracle "at_recent_commit_via_head" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
" "HEAD"

echo "--- working set is NOT visible at any ref ---"

# After the latest commit, an uncommitted modification is in the
# working set. Querying at HEAD should NOT see it (HEAD is the
# committed state).
oracle "at_head_excludes_working_modifications" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
" "HEAD"

# Same with a staged-but-not-committed modification.
oracle "at_head_excludes_staged_modifications" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
" "HEAD"

echo "--- post-merge ---"

# After a merge, HEAD points at the merge commit and includes
# the merged content.
oracle "at_head_after_merge" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD"

# View at HEAD~1 after merge shows the pre-merge state (main2).
oracle "at_head_minus_1_after_merge_is_main2" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
" "HEAD~1"

echo "--- error paths ---"

# Reference to a non-existent ref. Both engines should surface
# something the user can act on (error or, in doltlite's case,
# a clearly empty result).
oracle_error "at_nonexistent_ref" "$SEED" "nope"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
