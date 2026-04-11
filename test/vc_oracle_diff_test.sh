#!/bin/bash
#
# Version-control oracle tests: dolt_diff_<table> AND dolt_diff
#
# Two surfaces are tested here:
#
# 1. dolt_diff_<table> (per-table row-level diff history)
#    Schema layout (matches Dolt):
#      to_<col1>, ..., to_<colN>, to_commit, to_commit_date,
#      from_<col1>, ..., from_<colN>, from_commit, from_commit_date,
#      diff_type
#    The harness LEFT JOINs dolt_log on to_commit and from_commit so
#    the comparison uses commit MESSAGES (stable across engines)
#    instead of engine-specific commit hashes.
#
# 2. dolt_diff no-arg vtable (commits-touching-tables summary)
#    One row per (commit, changed_table). Dolt-compatible columns:
#      commit_hash, table_name, committer, email, date, message,
#      data_change, schema_change
#    Compared on (table_name, message, data_change, schema_change),
#    sorted, with engine-specific hashes/timestamps stripped. Note
#    that doltlite's dolt_diff is polymorphic: with no constraint
#    on table_name it returns the summary form; with `dolt_diff('t')`
#    it falls through to the legacy per-row form used by the 90+
#    existing self-tests.
#
# Usage: bash vc_oracle_diff_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize_diff_table() {
  # Input row schema (built by the harness query):
  #   $1=T, $2=table_name, $3=to_id, $4=to_msg, $5=diff_type,
  #   $6=from_id, $7=from_msg
  # to_msg / from_msg are commit messages (via LEFT JOIN with
  # dolt_log) so the comparison is engine-independent. WORKING and
  # EMPTY tokens are preserved verbatim.
  tr -d '\r' \
    | awk -F'\t' 'NF >= 7 && $1 == "T" { print }' \
    | awk -F'\t' '
        {
          tbl     = $2
          to_id   = $3
          to_msg  = $4
          diff    = $5
          from_id = $6
          from_msg = $7
          if (to_msg == "" || to_msg ~ /^0+$/) to_msg = "EMPTY"
          if (from_msg == "" || from_msg ~ /^0+$/) from_msg = "EMPTY"
          print "T\t" tbl "\t" diff "\t" to_id "\t" to_msg "\t" from_id "\t" from_msg
        }
      ' \
    | sort
}

oracle() {
  local name="$1" setup="$2" tables="${3:-t}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # Build the per-table query as a UNION ALL across all named
  # tables. The whole thing — setup + summary + per-table — runs
  # in a SINGLE engine invocation so the session state at the
  # end of setup (current branch, working set) is preserved
  # through to the queries. Splitting into multiple invocations
  # caused two bugs in earlier iterations:
  #   1. Each Dolt re-open landed on the default branch, losing
  #      the setup's `dolt_checkout('feature')` and so missing
  #      feature-only commits in dolt_diff.
  #   2. Dolt's commit hashes have a non-deterministic component,
  #      so running the same setup twice (once for table t, once
  #      for table u) produced different hashes for the same
  #      logical commit, breaking the harness's hash-renaming.

  # ── doltlite query ──
  # Per-table query LEFT JOINs dolt_log on both to_commit and
  # from_commit so the comparison uses the COMMIT MESSAGE instead
  # of the engine-specific hash. WORKING and EMPTY tokens that
  # aren't real commits coalesce to themselves.
  local dl_table_q=""
  IFS=',' read -ra tarr <<< "$tables"
  for tn in "${tarr[@]}"; do
    local part="SELECT 'T' || char(9) || '${tn}' || char(9) || coalesce(dt.to_id,'') || char(9) || coalesce(log_to.message, dt.to_commit) || char(9) || dt.diff_type || char(9) || coalesce(dt.from_id,'') || char(9) || coalesce(log_from.message, dt.from_commit) FROM dolt_diff_${tn} dt LEFT JOIN dolt_log log_to ON log_to.commit_hash = dt.to_commit LEFT JOIN dolt_log log_from ON log_from.commit_hash = dt.from_commit"
    if [ -z "$dl_table_q" ]; then
      dl_table_q="$part"
    else
      dl_table_q="$dl_table_q UNION ALL $part"
    fi
  done

  local dl_table
  dl_table=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_table_q" \
             | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
             | grep -v '^[0-9]*$' \
             | grep -v '^[0-9a-f]\{40\}$' \
             | normalize_diff_table)

  # ── Dolt query ──
  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_table_q=""
  for tn in "${tarr[@]}"; do
    local part="SELECT concat('T', char(9), '${tn}', char(9), coalesce(dt.to_id,''), char(9), coalesce(log_to.message, dt.to_commit), char(9), dt.diff_type, char(9), coalesce(dt.from_id,''), char(9), coalesce(log_from.message, dt.from_commit)) FROM dolt_diff_${tn} dt LEFT JOIN dolt_log log_to ON log_to.commit_hash = dt.to_commit LEFT JOIN dolt_log log_from ON log_from.commit_hash = dt.from_commit"
    if [ -z "$dt_table_q" ]; then
      dt_table_q="$part"
    else
      dt_table_q="$dt_table_q UNION ALL $part"
    fi
  done

  local dt_table
  dt_table=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_table_q"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_diff_table
  )

  # Empty-on-both-sides safeguard.
  if [ -z "$dl_table" ] && [ -z "$dt_table" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both table queries empty — harness bug)"
    return
  fi

  if [ "$dl_table" = "$dt_table" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_table" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_table" | sed 's/^/      /'
  fi
}

# Normalize summary-form rows. Dolt emits data_change/schema_change
# as `true`/`false` in csv; doltlite emits 0/1. Coerce both to 0/1.
# Then strip CR, drop blank lines, sort.
normalize_summary() {
  tr -d '\r' \
    | sed -e 's/	true$/	1/' -e 's/	true	/	1	/g' \
          -e 's/	false$/	0/' -e 's/	false	/	0	/g' \
    | awk -F'\t' 'NF >= 5 && $1 == "S" { print }' \
    | sort
}

# Oracle for dolt_diff no-arg summary form. Compares the
# (table_name, message, data_change, schema_change) tuples after
# joining with dolt_log to use commit MESSAGES rather than the
# engine-specific commit hashes (which differ between engines and
# are non-deterministic in dolt).
oracle_summary() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_summary"
  mkdir -p "$dir/dl" "$dir/dt"

  # Both engines: SELECT joins dolt_diff against dolt_log on
  # commit_hash to translate the hash into the commit MESSAGE.
  # Some commits may not appear in dolt_log (e.g. unreachable),
  # in which case coalesce falls back to the hash.
  local q="SELECT 'S' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('S', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize_summary)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err" | tr -d '"' | normalize_summary
  )

  if [ -z "$dl_out" ] && [ -z "$dt_out" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both summary queries empty — harness bug)"
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

echo "=== Version Control Oracle Tests: dolt_diff / dolt_diff_<table> ==="
echo ""

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- multi-table ---"

oracle "two_tables_independent" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO u VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init_both');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_t_only');
INSERT INTO u VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_u_only');
" "t,u"

echo "--- per-table: row-level diff ---"

oracle "table_diff_modify_row" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_update');
"

oracle "table_diff_delete_row" "
$SEED
INSERT INTO t VALUES (2, 20);
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_two');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_delete');
"

oracle "table_diff_add_then_modify_then_delete_same_row" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add');
UPDATE t SET v = 22 WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_modify');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4_delete');
"

echo "--- staged state interactions ---"

# Stage a modification, verify dolt_diff_t shows the staged change
# as part of the WORKING row (since the vtable form rolls up
# WORKING vs HEAD; the staged diff is implicit). Note: Dolt also
# exposes a separate dolt_diff('STAGED','WORKING','t') table-
# valued function that surfaces STAGED specifically; that's a
# different surface and is not covered by this oracle.
oracle "table_diff_after_stage_only" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
"

# Stage some changes, then make MORE working changes on top.
# The WORKING row should reflect the cumulative state (final
# v=99), not the intermediate staged state (v=50).
oracle "table_diff_stage_then_more_working" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 99 WHERE id = 1;
"

# Stage an insert, verify it appears in WORKING row.
oracle "table_diff_stage_insert" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
"

# Stage a delete, verify it appears in WORKING row.
oracle "table_diff_stage_delete" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
"

# Mixed: some changes staged, some unstaged. Both engines should
# include both in the WORKING row.
oracle "table_diff_mixed_staged_and_unstaged" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
INSERT INTO t VALUES (3, 30);
"

echo "--- working set diff ---"

# Both engines should include the WORKING diff at the head of
# dolt_diff_<table> output when there are uncommitted changes.
oracle "table_diff_working_modify" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
"

oracle "table_diff_working_insert" "
$SEED
INSERT INTO t VALUES (2, 20);
"

oracle "table_diff_working_delete" "
$SEED
DELETE FROM t WHERE id = 1;
"

# Working changes followed by a commit — the WORKING row should
# disappear after the commit.
oracle "table_diff_working_then_committed" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

# Mixed: working diff on top of multiple committed changes.
oracle "table_diff_history_plus_working" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
UPDATE t SET v = 99 WHERE id = 1;
"

echo "--- branching ---"

# Diff on a branch picks up that branch's commits but not main's.
oracle "diff_on_feature_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
"

echo "--- merges ---"

# Simple merge: feature adds row 2, main adds row 3, then merge.
# Dolt's algorithm (now mirrored by doltlite) attributes row 3 to
# the merge-vs-feat1 edge — NOT to main2-vs-c1 — because the merge
# absorbs main2's first-parent diff. See doltlite_diff_table.c
# buildDiffPairs() and the dolt diff_table.go processCommit /
# CommitItrForRoots LIFO walk for the canonical algorithm.
oracle "diff_after_simple_merge" "
$SEED
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

# Merge with multiple commits between branch base and merge on the
# main side. This exercises the more interesting case where main3
# is the merge's first parent but main2 is reachable only via
# main3's first-parent edge — main3 vs main2 IS emitted, but
# main2 vs c1 is suppressed because the merge edge (merge,feat1)
# already covers row 3.
oracle "diff_after_merge_with_intermediate_commits" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main3');
SELECT dolt_merge('feature');
"

# Merge that introduces no new content on the main side (feature is
# a fast-forwardable change but we force a merge commit by having
# main also commit). Verifies the per-edge row attribution still
# matches when the merge's first-parent diff is empty.
oracle "diff_after_merge_no_main_changes_after_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_update');
SELECT dolt_merge('feature');
"

echo ""
echo "--- summary form: dolt_diff (no args) ---"

# Single-table linear history. Two commits should produce two
# summary rows, both with data_change=1; the first commit (table
# creation) also has schema_change=1.
oracle_summary "summary_two_commits_one_table" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

# Multi-commit linear history with INSERT, UPDATE, DELETE.
oracle_summary "summary_linear_three_commits" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_insert');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_update');
DELETE FROM t WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4_delete');
"

# Two independent tables changing in different commits. Each
# commit should produce a row only for the table it touched.
oracle_summary "summary_two_tables_independent" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO u VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init_both');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_t_only');
INSERT INTO u VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_u_only');
"

# Schema change via ALTER TABLE — schema_change should be 1.
oracle_summary "summary_schema_change_add_column" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
"

# Combined data and schema change in one commit.
oracle_summary "summary_data_and_schema_in_one_commit" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
INSERT INTO t VALUES (2, 20, 'hi');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'col_and_row');
"

# Newly created table in a later commit.
oracle_summary "summary_table_added_later" "
$SEED
CREATE TABLE u(id INT PRIMARY KEY, x TEXT);
INSERT INTO u VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
"

# Working-set changes are NOT reachable via dolt_diff (which is
# commits-only); both engines should ignore them. The only
# summary rows should be for the committed setup.
oracle_summary "summary_working_set_excluded" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
