#!/bin/bash
#
# Version-control oracle test: HEAD / staged / working state transitions
#
# Doltlite has three stages for table state — HEAD (committed),
# staged (what dolt_commit will commit), and working (live database
# state) — and the forward path (modify → add → commit) is well
# covered by other oracles. This file targets the REVERSE and
# SIDE-STEP paths where things tend to subtly diverge: dolt_reset
# moving rows between stages, dolt_checkout swapping branches with
# uncommitted changes, table-level reset undoing only some changes,
# and the interactions between concurrent staged and working
# modifications to the same table.
#
# Each scenario builds up a specific (HEAD, staged, working) state
# and then runs a state-changing operation. The oracle compares
# the resulting state across THREE surfaces:
#
#   1. dolt_log    — what HEAD points to
#   2. dolt_status — what's staged vs unstaged
#   3. SELECT * FROM t — actual working-set table contents
#
# Comparing all three is essential here. The status alone could
# show "no changes" while the working table actually contains
# divergent data; the log alone could show the right HEAD while
# the working set is silently wrong; and the table contents alone
# could match while the staged catalog is in a wrong state that
# would corrupt the next commit.
#
# Usage: bash vc_oracle_state_transitions_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

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

normalize_table() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "T" { print }' \
    | sort
}

# Each scenario provides setup SQL and the name of the table whose
# contents should be compared (some scenarios use multiple tables;
# they pass a comma-separated list which becomes a UNION ALL).
oracle() {
  local name="$1" setup="$2" tables="${3:-t}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # Build the table-content query as a UNION ALL across the named
  # tables, prefixing each row with the table name so the harness
  # can sort across tables consistently. Schema is assumed
  # (id, v) — every scenario in this file uses that shape.
  local table_query=""
  IFS=',' read -ra tarr <<< "$tables"
  for tn in "${tarr[@]}"; do
    if [ -z "$table_query" ]; then
      table_query="SELECT 'T' || char(9) || '$tn' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM $tn"
    else
      table_query="$table_query UNION ALL SELECT 'T' || char(9) || '$tn' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM $tn"
    fi
  done

  local dl_log dl_status dl_table
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
  dl_table=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$table_query" \
             | "$DOLTLITE" "$dir/dl/db.t" 2>>"$dir/dl.err" \
             | grep -v '^[0-9]*$' \
             | grep -v '^[0-9a-f]\{40\}$' \
             | normalize_table)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_log dt_status dt_table
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
    "$DOLT" sql -r csv -q "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.status.raw"
  dt_status=$(tail -n +2 "$dir/dt.status.raw" | tr -d '"' | normalize_status)

  # Build a Dolt-syntax table query (concat instead of ||).
  local dolt_table_query=""
  for tn in "${tarr[@]}"; do
    local part="SELECT concat('T', char(9), '$tn', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM $tn"
    if [ -z "$dolt_table_query" ]; then
      dolt_table_query="$part"
    else
      dolt_table_query="$dolt_table_query UNION ALL $part"
    fi
  done
  (
    mkdir -p "$dir/dt.t" && cd "$dir/dt.t" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.t.err"
    "$DOLT" sql -r csv -q "$dolt_table_query;" 2>>"$dir/dt.t.err"
  ) > "$dir/dt.table.raw"
  dt_table=$(tail -n +2 "$dir/dt.table.raw" | tr -d '"' | normalize_table)

  # Empty-on-both-sides safeguard. Every scenario in this file
  # commits at least once and queries at least one table, so an
  # empty log AND empty table on both sides means the harness
  # query errored and the test is meaningless.
  if [ -z "$dl_log" ] && [ -z "$dt_log" ] && [ -z "$dl_table" ] && [ -z "$dt_table" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (everything empty on both sides — harness bug)"
    return
  fi

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_status"$'\n'"$dl_table"
  dt_combined="$dt_log"$'\n'"$dt_status"$'\n'"$dt_table"

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
    echo "    doltlite table:";  echo "$dl_table"  | sed 's/^/      /'
    echo "    dolt table:";      echo "$dt_table"  | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: HEAD / staged / working state transitions ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- reset moving things between stages ---"

# Stage one change, modify ANOTHER thing in working only, then
# reset (no args). The staged change should move back to working,
# joining the existing unstaged change. Both should appear as
# unstaged after the reset.
oracle "reset_unstages_while_working_has_separate_diff" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
INSERT INTO t VALUES (4, 40);
SELECT dolt_reset();
"

# Stage a row, then DELETE it from working. After --hard reset,
# both the staged add and the working delete should be gone, and
# the table should be back to HEAD's contents.
oracle "hard_reset_undoes_stage_then_working_delete" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
DELETE FROM t WHERE id = 3;
SELECT dolt_reset('--hard');
"

# Stage a modification, then make a SECOND modification to the
# same row in working. After plain reset (mixed), staged is
# cleared but working keeps both modifications visible (since
# the second one was never committed and -mixed leaves working
# alone). The visible row v should reflect the second change.
oracle "mixed_reset_preserves_subsequent_working_change" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 200 WHERE id = 1;
SELECT dolt_reset();
"

# Same setup as above but --hard. Now BOTH the staged
# modification AND the second working modification should be
# wiped, and id=1 should be back to HEAD's value of 10.
oracle "hard_reset_wipes_both_staged_and_working" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 200 WHERE id = 1;
SELECT dolt_reset('--hard');
"

# Stage a row addition, then reset that ONE table by name. The
# staged add for that specific table should move back to working;
# any other staged work should remain staged.
oracle "table_reset_unstages_only_named_table" "
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
" "a,b"

# Reset to a previous commit with --mixed. HEAD moves back, the
# diff between c2 and c1 becomes UNSTAGED working changes, and
# nothing is staged.
oracle "mixed_reset_to_prev_commit_moves_diff_to_working" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('HEAD~1');
"

# Reset to a previous commit with --hard. HEAD moves back, the
# diff is GONE entirely from working — table back to c1's content.
oracle "hard_reset_to_prev_commit_drops_diff_entirely" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'HEAD~1');
"

echo "--- checkout moving things between stages ---"

# Checkout to another branch with a CLEAN working set. Working
# should swap to that branch's HEAD; staged should also reflect
# that branch's HEAD.
oracle "checkout_branch_clean_swaps_working" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
"

# Working set is per-BRANCH in the Dolt server model (different
# from git's session-tied behavior). When the user makes a
# working-only change on main and checks out feature, the
# change STAYS on main and is not visible on feature. Round-tripping
# back to main restores the change. The harness uses two
# separate `dolt sql` invocations to defeat single-session
# carry-over so the comparison reflects the persistent
# per-branch model.
oracle "checkout_branch_per_branch_working_set" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
"

# checkout -b creates the new branch from current HEAD and
# switches to it. Under the per-branch model, the new branch
# starts with HEAD's working set — uncommitted main changes
# are NOT inherited by the new branch.
oracle "checkout_b_starts_from_head_not_working" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_checkout('feature');
SELECT dolt_checkout('-b', 'feature2');
SELECT dolt_checkout('main');
"

# Checkout a single TABLE from HEAD. Working changes to that
# table are reverted, working changes to other tables are
# preserved.
oracle "checkout_table_reverts_only_named_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v = 999 WHERE id = 1;
UPDATE b SET v = 999 WHERE id = 1;
SELECT dolt_checkout('a');
" "a,b"

# Checkout a table that was both modified in working AND staged.
# Both the staged modification and the working modification for
# that table should be reverted.
oracle "checkout_table_clears_both_staged_and_working" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 200 WHERE id = 1;
SELECT dolt_checkout('t');
"

echo "--- full cycle ---"

# Full forward-then-reverse cycle: commit something, reset --soft
# back, the diff is now staged, commit again. Should produce a
# new commit with the same content but a different message.
oracle "soft_reset_uncommit_then_recommit" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--soft', 'HEAD~1');
SELECT dolt_commit('-m', 'c2-recommitted');
"

# Commit, then mixed reset, then re-stage and re-commit with a
# new message. The diff was unstaged in the middle.
oracle "mixed_reset_uncommit_then_readd_recommit" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('HEAD~1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2-take-two');
"

# Cycle through branches: create feature, modify, switch back to
# main, modify differently, switch to feature, switch to main.
# Each switch should restore the right working set.
oracle "branch_round_trip_preserves_each_side" "
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
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
"

echo "--- edge cases ---"

# Stage a deletion of a tracked table (the whole table). After
# reset, the deletion is undone and the table is back.
oracle "reset_undoes_staged_table_deletion" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_reset('--hard');
"

# Working has a brand-new table; mixed reset shouldn't touch it
# (the new table is untracked, like an unstaged file in git).
oracle "mixed_reset_does_not_touch_untracked_new_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO u VALUES (1, 99);
SELECT dolt_reset();
" "t,u"

# Hard reset DOES wipe new untracked tables (matches git --hard
# semantics: anything in working goes away).
# UPDATE: actually git --hard does NOT remove untracked files;
# you need --hard plus -x or git clean. Let's see what Dolt does.
oracle "hard_reset_with_untracked_new_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO u VALUES (1, 99);
SELECT dolt_reset('--hard');
" "t,u"

# Add a table, commit, then reset --hard to BEFORE the add. The
# table should be gone from working entirely (it was tracked,
# now it's not even in HEAD).
oracle "hard_reset_drops_table_added_after_target" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO u VALUES (1, 99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
SELECT dolt_reset('--hard', 'HEAD~1');
" "t"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
