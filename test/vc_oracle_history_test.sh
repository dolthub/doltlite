#!/bin/bash
#
# Version-control oracle test: dolt_history_<table>
#
# Per-table commit history vtable. For each commit in the current
# branch's history (where the table existed), emits one row per
# table row containing the full row state plus the commit metadata.
#
# Schema (matches Dolt):
#   <user_col_1>, <user_col_2>, ..., <user_col_N>,
#   commit_hash TEXT, committer TEXT, commit_date TEXT
#
# The harness compares the rows after canonicalizing:
#   - commit_hash → commit message (LEFT JOIN with dolt_log)
#   - committer → DEFAULT for engine-specific defaults
#   - commit_date → dropped from comparison (millisecond vs second
#     precision differs and isn't a meaningful divergence)
#
# Setup and query run in a single engine invocation so the session
# state at the end of setup (current branch, working set) is
# preserved through to the queries — same lesson as the diff
# oracle in #372.
#
# Usage: bash vc_oracle_history_test.sh [path/to/doltlite] [path/to/dolt]
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

# Input row (built by the harness query):
#   $1=H, $2=table, $3=id, $4=v, $5=msg, $6=committer
# Output: H, table, id, v, msg, committer (canonicalized)
normalize_history() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 6 && $1 == "H" { print }' \
    | awk -F'\t' '
        {
          tbl = $2
          id  = $3
          v   = $4
          msg = $5
          who = $6
          if (who == "" \
           || who == "root" \
           || who == "oracle" \
           || who == "doltlite") {
            who = "DEFAULT"
          }
          print "H\t" tbl "\t" id "\t" v "\t" msg "\t" who
        }
      ' \
    | sort
}

oracle() {
  local name="$1" setup="$2" tables="${3:-t}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  IFS=',' read -ra tarr <<< "$tables"

  # ── doltlite query ──
  local dl_q=""
  for tn in "${tarr[@]}"; do
    local part="SELECT 'H' || char(9) || '${tn}' || char(9) || coalesce(h.id,'') || char(9) || coalesce(h.v,'') || char(9) || coalesce(log.message, h.commit_hash) || char(9) || coalesce(h.committer,'') FROM dolt_history_${tn} h LEFT JOIN dolt_log log ON log.commit_hash = h.commit_hash"
    if [ -z "$dl_q" ]; then
      dl_q="$part"
    else
      dl_q="$dl_q UNION ALL $part"
    fi
  done

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_history)

  # ── Dolt query ──
  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_q=""
  for tn in "${tarr[@]}"; do
    local part="SELECT concat('H', char(9), '${tn}', char(9), coalesce(h.id,''), char(9), coalesce(h.v,''), char(9), coalesce(log.message, h.commit_hash), char(9), coalesce(h.committer,'')) FROM dolt_history_${tn} h LEFT JOIN dolt_log log ON log.commit_hash = h.commit_hash"
    if [ -z "$dt_q" ]; then
      dt_q="$part"
    else
      dt_q="$dt_q UNION ALL $part"
    fi
  done

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_history
  )

  # Empty-on-both-sides safeguard.
  if [ -z "$dl_out" ] && [ -z "$dt_out" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both queries empty — harness bug)"
    return
  fi

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

echo "=== Version Control Oracle Tests: dolt_history_<table> ==="
echo ""

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- basic ---"

# Single commit. Each table row appears once with that commit.
oracle "single_commit_two_rows" "
$SEED
"

# Two commits, second updates a row. The row appears in BOTH
# commits with the respective values. Other rows appear in both
# commits unchanged.
oracle "modify_one_row_then_query" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_mod');
"

# Insert a new row in a later commit. The new row appears only
# in the later commit; existing rows appear in both.
oracle "insert_new_row" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add');
"

# Delete a row in a later commit. The deleted row still appears
# in the OLDER commits (history is immutable) but NOT in the new
# commit.
oracle "delete_row_in_later_commit" "
$SEED
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_del');
"

# Multiple modifications across multiple commits.
oracle "many_commits_many_changes" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
UPDATE t SET v = 200 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
DELETE FROM t WHERE id = 3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c5');
"

echo "--- working set is excluded ---"

# History only includes COMMITTED state. Uncommitted working
# changes do NOT appear in dolt_history_<table>.
oracle "working_set_changes_not_in_history" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
"

# Same with staged-but-not-committed changes.
oracle "staged_changes_not_in_history" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
"

echo "--- multi-table ---"

# Two tables in the same scenario; each table's history is
# independent of the other's.
oracle "two_tables_independent_history" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO u VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init_both');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_t');
INSERT INTO u VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "t,u"

echo "--- branching ---"

# History on a feature branch shows feature's commits AND the
# shared ancestor commits, but not main's later commits.
oracle "history_on_feature_branch" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
"

# After a merge, history on main includes commits from both sides
# (including the merge commit itself).
oracle "history_after_merge" "
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
"

echo "--- edge cases ---"

# Single-row table at single commit.
oracle "single_row_single_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_one');
"

# Same row id, value churned across many commits.
oracle "same_row_value_churned" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = 2 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
UPDATE t SET v = 3 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
UPDATE t SET v = 4 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4');
"

# Add a row, delete it, re-add with the SAME id but different
# value. The history should reflect each state.
oracle "add_delete_readd_same_id" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1_add');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_del');
INSERT INTO t VALUES (1, 999);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_readd');
"

# History across a checkout: switch to a sibling branch and verify
# only that branch's history is visible. Working changes that
# stay on main per the per-branch model (#370) should NOT leak
# into feature's history.
oracle "history_after_checkout_sibling_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_only');
SELECT dolt_checkout('main');
"

# After a hard reset to a previous commit, history should reflect
# the rewound state. The reverted commits are no longer reachable
# from HEAD.
oracle "history_after_hard_reset" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3');
SELECT dolt_reset('--hard', 'HEAD~1');
"

# After amending the most recent commit, history should reflect
# the amended commit, not the original.
oracle "history_after_amend" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_original');
SELECT dolt_commit('--amend', '-m', 'c2_amended');
"

echo "--- NULL values and other types ---"

# NULL values in non-PK columns should round-trip through history.
oracle "null_value_in_history" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = NULL WHERE id = 1;
UPDATE t SET v = 99 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_swap_nulls');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
