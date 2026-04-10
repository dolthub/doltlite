#!/bin/bash
#
# Version-control oracle test: dolt_reset
#
# Runs identical reset scenarios against doltlite and Dolt and compares
# the resulting (dolt_log, dolt_status) post-state. dolt_reset has three
# overlapping concerns the oracle has to verify together:
#
#   1. Where HEAD points after the reset (visible in dolt_log)
#   2. The staged-tables set (visible in dolt_status with staged=1)
#   3. The working-set tables (visible in dolt_status with staged=0)
#
# Comparing only one of those would miss class of bugs that change the
# wrong surface — e.g. a soft reset that incorrectly clobbers the
# working set, or a hard reset that fails to advance HEAD. So the
# oracle compares the log AND the status output, concatenated, for
# each scenario.
#
# Covers: --soft (default) with no ref (un-stage), --hard with no ref
# (un-stage + drop working changes), --soft and --hard with a target
# ref (branch / tag / commit hash), reset to current HEAD as a no-op,
# table-name positionals (Dolt's path-based unstage), and error paths.
#
# Usage: bash vc_oracle_reset_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Strip CRs and drop blank lines, sort the log section by message and
# the status section by table_name. The H1/H2/... renaming makes the
# commit hashes deterministic across the two engines (which disagree
# on hash content because doltlite uses prolly hashes and Dolt uses
# noms hashes — only the SHAPE of the chain has to match).
normalize_log() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 3 && $1 == "L" { print }' \
    | sort -t$'\t' -k3 \
    | awk -F'\t' '
        {
          h = $2
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print "L\t" seen[h] "\t" $3
        }
      '
}

normalize_status() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 4 && $1 == "S" { print }' \
    | sort -t$'\t' -k2,2 -k3,3 -k4,4
}

# Run a scenario. $1=name, $2=setup SQL in doltlite syntax. The harness
# rewrites SELECT dolt_*(...) -> CALL dolt_*(...) for Dolt.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_log dl_status
  dl_log=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'L' || char(9) || commit_hash || char(9) || message FROM dolt_log;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_log)
  dl_status=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S' || char(9) || table_name || char(9) || staged || char(9) || status FROM dolt_status;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_status)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_log dt_status
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('L', char(9), commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.log.raw"
  dt_log=$(tail -n +2 "$dir/dt.log.raw" | tr -d '"' | normalize_log)

  (
    cd "$dir/dt.s" 2>/dev/null || mkdir -p "$dir/dt.s" && cd "$dir/dt.s" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.s.err"
    "$DOLT" sql -r csv -q "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.status.raw"
  dt_status=$(tail -n +2 "$dir/dt.status.raw" | tr -d '"' | normalize_status)

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_status"
  dt_combined="$dt_log"$'\n'"$dt_status"

  if [ "$dl_combined" = "$dt_combined" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite log:";    echo "$dl_log"    | sed 's/^/      /'
    echo "    dolt log:";        echo "$dt_log"    | sed 's/^/      /'
    echo "    doltlite status:"; echo "$dl_status" | sed 's/^/      /'
    echo "    dolt status:";     echo "$dt_status" | sed 's/^/      /'
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_err=0
  echo "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then dl_err=1; fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_err=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
  )
  if grep -qiE 'error|fail' "$dir/dt.out" "$dir/dt.err" 2>/dev/null; then dt_err=1; fi

  if [ "$dl_err" = 1 ] && [ "$dt_err" = 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite errored: $([ $dl_err = 1 ] && echo yes || echo NO)"
    echo "    dolt errored:     $([ $dt_err = 1 ] && echo yes || echo NO)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_reset ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- reset with no ref (unstage) ---"

# Stage some changes, then dolt_reset() with no args. Both engines
# should leave HEAD where it is and move the staged changes back to
# unstaged.
oracle "reset_no_args_unstages_all" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_reset();
"

# Same as above with explicit --soft. Should be identical to no-args.
oracle "reset_soft_no_ref_unstages_all" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_reset('--soft');
"

# --hard with no ref both unstages AND drops working-set changes.
# Final state: clean working set, no staged changes, table contents
# match HEAD.
oracle "reset_hard_no_ref_clears_everything" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
INSERT INTO t VALUES (3, 30);
SELECT dolt_reset('--hard');
"

# Reset on a clean tree should be a no-op.
oracle "reset_no_changes_to_unstage" "
$SEED
SELECT dolt_reset();
"

# Reset --hard on a clean tree should also be a no-op.
oracle "reset_hard_no_changes" "
$SEED
SELECT dolt_reset('--hard');
"

echo "--- reset with ref (move HEAD) ---"

# --soft to the previous commit: HEAD moves back, working set is
# unchanged, the diff between c2 and c1 shows up as STAGED changes.
oracle "reset_soft_to_previous_commit" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--soft', 'HEAD~1');
"

# --hard to the previous commit: HEAD moves back, working set is
# rewound, no staged or unstaged changes.
oracle "reset_hard_to_previous_commit" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'HEAD~1');
"

# Reset to another branch's tip. Pulls main back to feature's tip
# (which is identical to main's c1 because feature was branched
# right after c1 with no further commits).
oracle "reset_hard_to_branch_name" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'feature');
"

# Reset to a tag. Tags were promoted to first-class objects in
# 0557f09b8 — verifies dolt_reset accepts a tag name as the target.
# (This is the same surface gap that bit dolt_merge in PR #364, so
# explicit oracle coverage matters.)
oracle "reset_hard_to_tag" "
$SEED
SELECT dolt_tag('release-1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'release-1');
"

# Reset to a bare commit hash. Captures c1's hash via subquery so
# the scenario is fully self-contained.
oracle "reset_hard_to_commit_hash" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', (SELECT commit_hash FROM dolt_log WHERE message = 'c1'));
"

# Reset to current HEAD: HEAD doesn't move, staged/working unchanged.
oracle "reset_hard_to_current_head_noop" "
$SEED
SELECT dolt_reset('--hard', 'HEAD');
"

# Working-set-only changes (no add) plus --hard should drop them.
oracle "reset_hard_with_uncommitted_modifications" "
$SEED
INSERT INTO t VALUES (2, 20);
INSERT INTO t VALUES (3, 30);
SELECT dolt_reset('--hard');
"

echo "--- table-name positional unstage ---"

# Stage two new tables, then reset only one of them. The other
# should remain staged.
oracle "reset_specific_table_unstages_only_that" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO a VALUES (2, 20);
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_reset('a');
"

echo "--- error paths ---"

oracle_error "reset_to_nonexistent_ref" "
$SEED
SELECT dolt_reset('--hard', 'nope');
"

oracle_error "reset_unknown_flag" "
$SEED
SELECT dolt_reset('--bogus');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
