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
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

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
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_log dt_status
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('L', char(9), commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.log.raw"
  dt_log=$(tail -n +2 "$dir/dt.log.raw" | tr -d '"' | normalize_log)

  (
    cd "$dir/dt.s" 2>/dev/null || mkdir -p "$dir/dt.s" && cd "$dir/dt.s" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.s.err"
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

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
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

oracle_error_match() {
  local name="$1" setup="$2" pattern="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"

  if grep -qE "$pattern" "$dir/dl.err" "$dir/dl.out" \
    && grep -qE "$pattern" "$dir/dt.err" "$dir/dt.out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to match error pattern)"
    echo "    pattern: $pattern"
  fi
}

oracle_same_session() {
  local name="$1" dl_setup="$2" dl_query="$3" dolt_setup="${4:-$2}" dolt_query="${5:-$3}"
  local dir="$TMPROOT/${name}_ss"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    {
      printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_setup" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>&1 \
      | tr -d '\r' \
      | awk '/^Q\|/ {print; next} /[Nn]o such savepoint:|SAVEPOINT .*does not exist/ {print "E|savepoint"}'
  )

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$(vc_oracle_translate_for_dolt "$dolt_setup")"
      printf '%s\n' "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>&1 \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk '/^Q\|/ {print; next} /[Nn]o such savepoint:|SAVEPOINT .*does not exist/ {print "E|savepoint"}'
  )

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

oracle_error "reset_mixed_no_ref_unsupported" "
$SEED
SELECT dolt_reset('--mixed');
"

oracle_error "reset_mixed_with_ref_unsupported" "
$SEED
SELECT dolt_reset('--mixed', 'HEAD');
"

oracle_error "reset_soft_hard_mutually_exclusive" "
$SEED
SELECT dolt_reset('--soft', '--hard');
"

oracle_error "reset_soft_hard_mutually_exclusive_with_ref" "
$SEED
SELECT dolt_reset('--soft', '--hard', 'HEAD');
"

echo "--- merge conflict guards ---"

oracle_error_match "reset_no_args_during_merge_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_reset();
" "Merge conflict detected"

oracle_error_match "reset_soft_during_merge_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_reset('--soft');
" "Merge conflict detected"

echo "--- savepoint parity ---"

oracle_same_session "reset_hard_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-A', '-m', 'c1');
SAVEPOINT sp1;
SELECT dolt_reset('--hard', 'HEAD');
" "SELECT 'Q|' || v FROM t;
ROLLBACK TO sp1;" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_commit('-A', '-m', 'c1');
SAVEPOINT sp1;
CALL dolt_reset('--hard', 'HEAD');
" "SELECT concat('Q|', v) FROM t;
ROLLBACK TO sp1;"

oracle_same_session "reset_bad_ref_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-A', '-m', 'c1');
UPDATE t SET v='dirty';
SAVEPOINT sp1;
SELECT dolt_reset('--hard', 'bogus');
" "SELECT 'Q|' || v FROM t;
ROLLBACK TO sp1;" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_commit('-A', '-m', 'c1');
UPDATE t SET v='dirty';
SAVEPOINT sp1;
CALL dolt_reset('--hard', 'bogus');
" "SELECT concat('Q|', v) FROM t;
ROLLBACK TO sp1;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
