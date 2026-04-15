#!/bin/bash
#
# Version-control oracle test: dolt_revert and dolt_cherry_pick
#
# Runs identical revert / cherry-pick scenarios against doltlite and
# Dolt and compares the resulting (dolt_log, table contents) post-state.
# Both procedures take a commit-ish argument, walk the parent chain to
# build a three-way merge, and produce a new commit on the current
# branch — so the test surface is largely about
#
#   1. argument parsing (ref / branch / tag / hash / HEAD~N)
#   2. message formatting on the new commit
#   3. table contents after the operation completes
#   4. error paths (initial commit, nonexistent ref, no args)
#
# The harness compares dolt_log AND a SELECT * from the user table,
# concatenated. dolt_log alone wouldn't catch a divergence where the
# same commit message is generated but the resulting catalog is
# wrong; the table SELECT alone wouldn't catch divergences in the
# new commit's parent linkage.
#
# Commit messages from cherry-pick / revert are normalized to a
# canonical form before comparison because the two engines format
# them differently — that's a known divergence we test for separately
# in the message_format scenarios, but we don't want it to dominate
# every other test.
#
# Usage: bash vc_oracle_revert_cherrypick_test.sh [path/to/doltlite] [path/to/dolt]
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

# Strip CRs, drop blank lines, sort the log section by message and
# the table section by row contents. The H1/H2/... renaming makes
# the commit hashes deterministic across the two engines.
#
# Cherry-pick / revert messages are canonicalized: anything matching
# /^(cherry-pick|revert).*'?<orig>'?$/i becomes "OP:<orig>". This
# isolates the commit content from the message wording so we don't
# trip on every scenario just because Dolt writes "Revert \"x\"" and
# doltlite writes "Revert 'x'".
normalize_log() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 3 && $1 == "L" { print }' \
    | awk -F'\t' '
        {
          msg = $3
          # Cherry-pick: canonicalize as CP:<orig> if recognizable
          lower = tolower(msg)
          if (match(lower, /^cherry-pick[: ]/) > 0) {
            sub(/^[Cc]herry-?[Pp]ick[: ]+/, "", msg)
            gsub(/^"|"$|^'\''|'\''$/, "", msg)
            msg = "CP:" msg
          } else if (match(lower, /^revert/) > 0) {
            sub(/^[Rr]evert[ ]+/, "", msg)
            gsub(/^"|"$|^'\''|'\''$/, "", msg)
            msg = "RV:" msg
          }
          print "L\t" $2 "\t" msg
        }
      ' \
    | sort -t$'\t' -k3 \
    | awk -F'\t' '
        {
          h = $2
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print "L\t" seen[h] "\t" $3
        }
      '
}

normalize_table() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "T" { print }' \
    | sort
}

# Run a scenario. $1=name, $2=setup SQL in doltlite syntax. The
# harness rewrites SELECT dolt_*(...) -> CALL dolt_*(...) for Dolt.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_log dl_table
  dl_log=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'L' || char(9) || commit_hash || char(9) || message FROM dolt_log;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_log)
  dl_table=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'T' || char(9) || coalesce(id, '') || char(9) || coalesce(v, '') FROM t;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_table)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_log dt_table
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('L', char(9), commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.log.raw"
  dt_log=$(tail -n +2 "$dir/dt.log.raw" | tr -d '"' | normalize_log)

  (
    mkdir -p "$dir/dt.s" && cd "$dir/dt.s" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.s.err"
    "$DOLT" sql -r csv -q "SELECT concat('T', char(9), coalesce(id, ''), char(9), coalesce(v, '')) FROM t;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.table.raw"
  dt_table=$(tail -n +2 "$dir/dt.table.raw" | tr -d '"' | normalize_table)

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_table"
  dt_combined="$dt_log"$'\n'"$dt_table"

  if [ "$dl_combined" = "$dt_combined" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite log:";   echo "$dl_log"   | sed 's/^/      /'
    echo "    dolt log:";       echo "$dt_log"   | sed 's/^/      /'
    echo "    doltlite table:"; echo "$dl_table" | sed 's/^/      /'
    echo "    dolt table:";     echo "$dt_table" | sed 's/^/      /'
  fi
}

# Both engines should error in the same direction. Doesn't compare
# the error text — just that both refuse the operation.
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

echo "=== Version Control Oracle Tests: dolt_revert / dolt_cherry_pick ==="
echo ""

# Common seed: a single-row table on main, branched off to feature.
SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
"

echo "--- cherry-pick: basic ---"

# Cross-branch cherry-pick has a notable trap: dolt_log on main
# only sees main's history, so a subquery like
#   SELECT commit_hash FROM dolt_log WHERE message = 'feat_add_2'
# returns NOTHING when run from main and the cherry-pick silently
# fails on both engines, producing a "passing" but meaningless test
# (same false-positive class as merge_from_commit_hash in PR #364).
# Every cherry-pick scenario below uses TAGS or branch~N references
# instead, both of which resolve cross-branch in both engines.

# Pick the tip of feature by branch name. The picked commit adds a
# new row, no overlap with main.
oracle "cherry_pick_branch_tip" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature');
"

# Cherry-pick a commit that updates a non-PK column. Reference by
# tag (cross-branch visible in both engines).
oracle "cherry_pick_update_non_pk" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_tag('upd-source');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('upd-source');
"

# Cherry-pick by tag — same surface gap that bit dolt_merge in
# PR #364, so explicit oracle coverage matters.
oracle "cherry_pick_by_tag" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_tag('cherry-source');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('cherry-source');
"

# Cherry-pick by branch~N — picks the second-most-recent commit
# on feature. Verifies the HEAD~N resolver work from #365.
oracle "cherry_pick_by_branch_relative" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature~1');
"

echo "--- cherry-pick: chains ---"

# Pick two commits from feature in succession via branch~N. Both
# should land on main as separate commits.
oracle "cherry_pick_two_in_sequence" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature~1');
SELECT dolt_cherry_pick('feature');
"

echo "--- revert: basic ---"

# Revert the most recent commit. Should produce a new commit that
# undoes the change but leaves earlier history intact.
oracle "revert_undoes_last_insert" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c2_add_2'));
"

# Revert an update — should restore the original value.
oracle "revert_undoes_update" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_update');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c2_update'));
"

# Revert a delete — row should come back.
oracle "revert_undoes_delete" "
$SEED
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_delete');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c2_delete'));
"

# Revert by tag. Same resolver-coverage rationale as cherry-pick.
oracle "revert_by_tag" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
SELECT dolt_tag('to-revert');
SELECT dolt_revert('to-revert');
"

# Revert HEAD~0 (current HEAD) — same as reverting the last commit.
oracle "revert_head" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
SELECT dolt_revert('HEAD');
"

# Revert HEAD~1 — should undo the second-to-last commit, leaving
# the most recent change intact (only its base changes).
oracle "revert_head_relative" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_add_3');
SELECT dolt_revert('HEAD~1');
"

echo "--- revert: chains ---"

# Make three commits, revert two of them in reverse order. The
# table should reflect only the surviving change.
oracle "revert_two_in_reverse_order" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_add_3');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4_add_4');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c4_add_4'));
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c3_add_3'));
"

# Reverting a revert ("undo the undo") would be the natural test
# here, but Dolt and doltlite format the nested commit message
# differently in this specific case: Dolt strips inner quotes
# (`Revert "Revert c2_add_2"`) while doltlite preserves them
# (`Revert "Revert "c2_add_2""`). The functional outcome — table
# state restored to the original — matches between engines, but
# the message difference would dominate the comparison. The
# `revert_two_in_reverse_order` scenario above already exercises
# chained reverts on independent commits, so this case is left
# uncovered intentionally rather than being weakened to a state-only
# check that drifts from the rest of the harness.

echo "--- conflicts ---"

# A conflicted cherry-pick / revert should NOT advance the branch
# (no new merge commit) and SHOULD leave dolt_conflicts populated.
# Both engines use the same harness pattern as the merge oracle's
# `oracle_no_merge_commit`: count commits before and after, expect
# them to match.
oracle_no_merge_commit() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_nm"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_count
  printf '%s\n' "$setup" | "$DOLTLITE" "$dir/dl/db" >/dev/null 2>"$dir/dl.err" || true
  dl_count=$(printf ".headers off\n.mode list\nSELECT count(*) FROM dolt_log;\n" \
             | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
             | grep -E '^[0-9]+$' | tail -1)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_count
  dt_count=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "SET @@dolt_allow_commit_conflicts = 1;"
      printf '%s\n' "$dolt_setup"
    } | "$DOLT" sql >/dev/null 2>"$dir/dt.err" || true
    "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_log;" 2>>"$dir/dt.err" \
      | tail -n +2
  )

  if [ "$dl_count" = "$dt_count" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite log count: $dl_count"
    echo "    dolt log count:     $dt_count"
  fi
}

# Cherry-pick a commit whose change overlaps with main's most
# recent commit on the same row. Both sides modified id=1's v to
# different values — three-way merge produces a real conflict and
# neither engine should produce a clean cherry-pick commit.
oracle_no_merge_commit "cherry_pick_modify_modify_conflict" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_99');
SELECT dolt_tag('feat-conflict');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_11');
SELECT dolt_cherry_pick('feat-conflict');
"

# Revert a commit when a later commit on the same branch has
# already touched the same row. Reverting the older commit would
# require undoing a change that was subsequently overwritten —
# both engines should refuse to produce a clean revert commit.
oracle_no_merge_commit "revert_with_later_overlap_conflict" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_set_99');
SELECT dolt_revert('HEAD~1');
"

# After a conflicting cherry-pick, dolt_conflicts should show one
# row for the conflicting table, and a follow-up
# dolt_conflicts_resolve('--ours', 't') should clear it. The full
# count check would pass even if the conflict surface was empty
# (zero commits added either way), so the comparison query is the
# conflicts vtable instead.
oracle_conflicts_count() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_cc"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_count
  printf '%s\n' "$setup" | "$DOLTLITE" "$dir/dl/db" >/dev/null 2>"$dir/dl.err" || true
  dl_count=$(printf ".headers off\n.mode list\nSELECT count(*) FROM dolt_conflicts;\n" \
             | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
             | grep -E '^[0-9]+$' | tail -1)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_count
  dt_count=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "SET @@dolt_allow_commit_conflicts = 1;"
      printf '%s\n' "$dolt_setup"
    } | "$DOLT" sql >/dev/null 2>"$dir/dt.err" || true
    "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_conflicts;" 2>>"$dir/dt.err" \
      | tail -n +2
  )

  if [ "$dl_count" = "$dt_count" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite conflicts: $dl_count"
    echo "    dolt conflicts:     $dt_count"
  fi
}

# After a conflicted cherry-pick, both engines should report
# exactly one conflicting table in dolt_conflicts.
oracle_conflicts_count "cherry_pick_conflict_populates_dolt_conflicts" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_99');
SELECT dolt_tag('feat-conflict');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_11');
SELECT dolt_cherry_pick('feat-conflict');
"

# Resolving --ours should clear dolt_conflicts.
oracle_conflicts_count "cherry_pick_conflict_resolved_ours_clears" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_99');
SELECT dolt_tag('feat-conflict');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_11');
SELECT dolt_cherry_pick('feat-conflict');
SELECT dolt_conflicts_resolve('--ours', 't');
"

# Revert conflict surface is not stable across the two engines yet.
# Keep the cross-engine invariant above (`oracle_no_merge_commit`) and
# pin doltlite's stronger conflict-table behavior here as a local
# regression instead of an oracle comparison.
doltlite_conflicts_count() {
  local name="$1" setup="$2" expected="$3"
  local dir="$TMPROOT/${name}_dl"
  mkdir -p "$dir"
  printf '%s\n' "$setup" | "$DOLTLITE" "$dir/db" >/dev/null 2>"$dir.err" || true
  local count
  count=$(printf ".headers off\n.mode list\nSELECT count(*) FROM dolt_conflicts;\n" \
          | "$DOLTLITE" "$dir/db" 2>>"$dir.err" | grep -E '^[0-9]+$' | tail -1)
  if [ "$count" = "$expected" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite conflicts: $count"
    echo "    expected:           $expected"
  fi
}

doltlite_conflicts_count "revert_conflict_populates_dolt_conflicts" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_set_99');
SELECT dolt_revert('HEAD~1');
 " "1"

doltlite_conflicts_count "revert_conflict_resolved_theirs_clears" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_set_99');
SELECT dolt_revert('HEAD~1');
SELECT dolt_conflicts_resolve('--theirs', 't');
" "0"

echo "--- error paths ---"

oracle_error "cherry_pick_no_args" "
$SEED
SELECT dolt_cherry_pick();
"

oracle_error "cherry_pick_nonexistent_ref" "
$SEED
SELECT dolt_cherry_pick('does-not-exist');
"

# Dolt's dolt_revert() with no args is a silent no-op rather than
# an error (likely an undocumented quirk — there's no defined
# semantics for "revert nothing"). doltlite matches Dolt for
# consistency. So this is a positive oracle scenario, not an error.
oracle "revert_no_args_is_noop" "
$SEED
SELECT dolt_revert();
"

oracle_error "revert_nonexistent_ref" "
$SEED
SELECT dolt_revert('does-not-exist');
"

# Reverting the initial commit has no parent to diff against — should
# error in both engines.
oracle_error "cherry_pick_initial_commit" "
$SEED
SELECT dolt_cherry_pick((SELECT commit_hash FROM dolt_log WHERE message = 'Initialize data repository'));
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
