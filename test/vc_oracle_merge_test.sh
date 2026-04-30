#!/bin/bash
#
# Version-control oracle test: dolt_merge
#
# Runs identical merge scenarios against doltlite and Dolt and compares
# the resulting dolt_log post-state. Covers fast-forward, three-way no
# conflict, --no-ff (force a merge commit even when fast-forward would
# work), -m / --message overrides, --abort, and a few error paths.
#
# Scenarios use a mix of primary key shapes to exercise the storage
# layer: INTEGER PRIMARY KEY (the sqlite rowid alias), INT PRIMARY KEY
# (auto-converted to WITHOUT ROWID by doltlite), TEXT PRIMARY KEY, and
# composite PRIMARY KEY. All shapes should merge correctly because
# doltlite encodes the storage key from the user PK columns rather
# than from sqlite's auto-allocated rowid.
#
# Conflict scenarios are checked in two ways:
# 1. oracle_no_merge_commit: neither engine should advance history.
# 2. oracle_error_poststate: under autocommit, both should roll back
#    and leave no persisted conflict state.
#
# Usage: bash vc_oracle_merge_test.sh [path/to/doltlite] [path/to/dolt]
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
  # 1. Strip CRs.
  # 2. Drop non-tab lines (function-result text like "Already up to date"
  #    leaks through stdout in list mode).
  # 3. Sort by message (column 2) so the H1/H2/... assignment in step 4 is
  #    deterministic across both engines regardless of native log walk
  #    order. After a merge, the two engines disagree on which parent
  #    appears first in dolt_log; sorting by message canonicalizes.
  # 4. Replace each distinct hash with H1, H2, ... in first-appearance
  #    order on the sorted output.
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 { print }' \
    | sort -t$'\t' -k2 \
    | awk -F'\t' '
        {
          h = $1
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print seen[h] "\t" $2
        }
      '
}

# Compare post-state via dolt_log: (commit_hash normalized, message),
# in order from newest to oldest. Same shape as the log oracle.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT commit_hash || char(9) || message FROM dolt_log'

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
    "$DOLT" sql -r csv -q "SELECT concat(commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"' | normalize)

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

# A merge that should not produce a new merge commit (because both
# engines diverge on how they surface conflict). Compares only the
# property that BOTH engines refuse to advance the branch tip to a
# clean merge commit — checked by counting commits on the current
# branch and asserting both report the same pre-merge count.
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
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err" || true
    "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_log;" 2>>"$dir/dt.err" \
      | tail -n +2
  )

  if [ -z "$dl_count" ] || [ -z "$dt_count" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (count query failed)"
    return
  fi

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

oracle_error_poststate() {
  local name="$1" setup="$2" query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_post"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup" || true
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s;\n" "$query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r'
  )

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite: |$dl_out|"
    echo "    dolt:     |$dt_out|"
  fi
}

oracle_same_session() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_ss"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    {
      printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s\n" "$setup" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
      | tr -d '\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf "%s\n" "$dolt_setup"
      printf "%s\n" "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite: |$dl_out|"
    echo "    dolt:     |$dt_out|"
  fi
}

oracle_reopen_state() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_reopen"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup" || true
  local dl_out
  dl_out=$(
    {
      printf ".headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk -F'\t' '$1=="Q"{print}'
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

echo "=== Version Control Oracle Tests: dolt_merge ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
"

echo "--- fast forward ---"

oracle "fast_forward_clean" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- three-way no conflict ---"

oracle "three_way_independent_inserts" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_disjoint_columns_modified" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES (1, 0, 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET a = 10 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_a');
SELECT dolt_checkout('feature');
UPDATE t SET b = 20 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_b');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- non-INTEGER primary key shapes ---"

# These three scenarios were the original motivation for this entire
# work. Before the user-PK row keying fix, doltlite stored rows in INT
# / TEXT / composite PK tables under sqlite's auto-allocated rowid,
# which collided across branches and produced phantom modify-modify
# conflicts on every cross-branch insert. With the storage layer now
# encoding the prolly tree key from the user PK columns, these merge
# cleanly the same way they do in Dolt.

oracle "three_way_int_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (100, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (200, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_text_pk" "
CREATE TABLE t(id VARCHAR(64) PRIMARY KEY, v INT);
INSERT INTO t VALUES ('alpha', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES ('beta', 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES ('gamma', 3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_composite_pk" "
CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (1, 2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 1, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_text_pk_modify_non_pk_col" "
CREATE TABLE t(id VARCHAR(64) PRIMARY KEY, v INT);
INSERT INTO t VALUES ('alpha', 1);
INSERT INTO t VALUES ('beta', 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 10 WHERE id = 'alpha';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 20 WHERE id = 'beta';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- --no-ff ---"

oracle "no_ff_creates_merge_commit" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '--no-ff');
"

echo "--- custom message ---"

oracle "merge_with_custom_message" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '-m', 'release sync');
"

oracle "merge_with_message_long_flag" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '--message', 'release sync');
"

echo "--- conflict (no merge commit) ---"

oracle_no_merge_commit "modify_modify_conflict_blocks_merge" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_error_poststate "modify_modify_conflict_rolls_back" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_reopen_state "modify_modify_conflict_explicit_txn_reconnect_rolls_back" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
BEGIN;
SELECT dolt_merge('feature');
" "SELECT 'Q' || char(9) || v FROM t WHERE id = 1;
SELECT 'Q' || char(9) || count(*) FROM dolt_conflicts;" \
"SELECT concat('Q', char(9), v) FROM t WHERE id = 1;
SELECT concat('Q', char(9), count(*)) FROM dolt_conflicts;"

echo "--- savepoint parity ---"

oracle_same_session "merge_savepoint_success_invalidated" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SAVEPOINT sp1;
SELECT dolt_merge('feature');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || count(*) FROM t;
SELECT 'Q' || char(9) || count(*) FROM dolt_log;" \
"SELECT concat('Q', char(9), count(*)) FROM t;
SELECT concat('Q', char(9), count(*)) FROM dolt_log;"

oracle_same_session "merge_abort_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feat' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
BEGIN;
SELECT dolt_merge('feature');
SAVEPOINT sp1;
SELECT dolt_merge('--abort');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts);
SELECT 'Q' || char(9) || (SELECT v FROM t WHERE id = 1);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts));
SELECT concat('Q', char(9), (SELECT v FROM t WHERE id = 1));"

oracle_same_session "merge_conflict_nested_savepoint_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feat' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
BEGIN;
SAVEPOINT sp1;
SELECT dolt_merge('feature');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts);
SELECT 'Q' || char(9) || (SELECT v FROM t WHERE id = 1);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts));
SELECT concat('Q', char(9), (SELECT v FROM t WHERE id = 1));"

oracle_same_session "merge_conflict_top_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feat' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
SAVEPOINT sp1;
SELECT dolt_merge('feature');
ROLLBACK TO sp1;
" "SELECT 'Q' || char(9) || (SELECT count(*) FROM dolt_conflicts);
SELECT 'Q' || char(9) || (SELECT v FROM t WHERE id = 1);" \
"SELECT concat('Q', char(9), (SELECT count(*) FROM dolt_conflicts));
SELECT concat('Q', char(9), (SELECT v FROM t WHERE id = 1));"

echo "--- already up to date ---"

oracle "merge_same_branch_no_op" "
$SEED
SELECT dolt_merge('main');
"

echo "--- other operation pairs ---"

# main adds row, feature modifies a different row. Different rows, so
# no conflict — three-way merge should land both changes cleanly.
oracle "add_modify_disjoint_rows_clean" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# Both branches insert the same key with the same value. This is
# convergent — both sides did the same thing, three-way merge should
# treat it as no conflict.
oracle "add_add_same_key_same_value" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# Both branches insert the same key with DIFFERENT values. The PK
# collides and the values disagree — should be a conflict, both
# engines refuse to produce a clean merge commit.
oracle_no_merge_commit "add_add_same_key_different_value_conflict" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# Both branches delete the same row. Convergent delete, clean merge.
oracle "delete_delete_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# One side deletes a row, the other modifies it. This is a real
# delete/modify conflict — both Dolt and doltlite should refuse to
# produce a clean merge commit.
oracle_no_merge_commit "delete_modify_conflict" "
$SEED
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- multi-commit branches ---"

# Feature branch with four commits before the merge. Exercises the
# iterative ancestor walk and confirms the resulting merge commit's
# parent linkage isn't off-by-one.
oracle "multi_commit_feature_branch" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat3');
INSERT INTO t VALUES (5, 50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat4');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- multi-table merges ---"

# Independent inserts spread across two tables. Tests that the
# per-table merge descent doesn't drop or duplicate either side.
oracle "multi_table_independent_inserts" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO a VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# One branch modifies table a, the other inserts into table b. Each
# table sees only one side change.
oracle "multi_table_modify_one_insert_other" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE a SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# Three tables, all touched on one side or the other. Stresses the
# per-table descent across more breadth than two tables.
oracle "three_tables_disjoint_changes" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
INSERT INTO c VALUES (1, 1000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
INSERT INTO a VALUES (2, 20);
INSERT INTO c VALUES (2, 2000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO b VALUES (2, 200);
UPDATE c SET v = 1500 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- schema deltas ---"

# Feature branch creates a brand new table. Main concurrently inserts
# into the pre-existing table. Merge should land the new table on
# main alongside main's insert.
oracle "feature_creates_new_table" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_new_table');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# Feature branch adds a new column to the existing table. Main
# concurrently inserts a row using only the original columns. Merge
# should produce a schema with the new column and have main's row
# using the column default.
oracle "feature_adds_column_main_inserts_old_shape" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN tag TEXT;
UPDATE t SET tag = 'a' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_col');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

# Both branches add a column. Different column names — concurrent
# add of disjoint columns should merge cleanly with both columns
# present in the result.
oracle "both_branches_add_disjoint_columns" "
$SEED
ALTER TABLE t ADD COLUMN main_col INT;
UPDATE t SET main_col = 1 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_col');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN feat_col INT;
UPDATE t SET feat_col = 2 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_col');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle_same_session "merge_disjoint_fk_tables_plus_check_same_session" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_reopen_state "merge_disjoint_fk_tables_plus_check_reopen" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_same_session "merge_self_ref_fk_cascade_same_session" "
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_descendant');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM t WHERE id = 1;
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM t) || '|' ||
          (SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id))" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', IFNULL(parent_id, -1)) ORDER BY id SEPARATOR ',') FROM t))"

oracle_reopen_state "merge_self_ref_fk_cascade_reopen" "
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_descendant');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM t WHERE id = 1;
" "PRAGMA foreign_keys = ON;
SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM t) || '|' ||
          (SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id))" \
  "SET @@foreign_key_checks = 1;
SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', IFNULL(parent_id, -1)) ORDER BY id SEPARATOR ',') FROM t))"

oracle_same_session "merge_fk_chain_cascade_same_session" "
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_checkout('main');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM gp WHERE id = 1;
" "SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM gp) || '|' ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c)" \
  "SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM gp), '|', (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c))"

oracle_reopen_state "merge_fk_chain_cascade_reopen" "
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_checkout('main');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_merge('feat');
DELETE FROM gp WHERE id = 1;
" "PRAGMA foreign_keys = ON;
SELECT 'Q' || char(9) ||
          (SELECT count(*) FROM gp) || '|' ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c)" \
  "SET @@foreign_key_checks = 1;
SELECT CONCAT('Q', CHAR(9), (SELECT COUNT(*) FROM gp), '|', (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c))"

echo "--- non-branch merge sources ---"

# Merge from a tag instead of a branch. Tags were promoted to first-
# class objects with metadata in 0557f09b8 — this verifies dolt_merge
# accepts a tag name as the source revision.
oracle "merge_from_tag" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_tag('release-1');
SELECT dolt_checkout('main');
SELECT dolt_merge('release-1');
"

# Merge from a commit hash. Both engines need to accept a hex hash
# as the merge source. Captured via subquery against dolt_log so the
# scenario is fully self-contained.
oracle "merge_from_commit_hash" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge((SELECT commit_hash FROM dolt_log WHERE message = 'feat1'));
"

echo "--- merge sequences ---"

# Merge feature into main (creates a merge commit), then merge main
# into feature. The second merge should fast-forward feature to
# wherever main is now, with no new commit.
oracle "merge_then_reverse_fast_forward" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_checkout('feature');
SELECT dolt_merge('main');
"

# Merge feature into main once, then add more commits to feature and
# merge again. The second merge should produce another clean merge
# commit (or fast-forward if main hasn't moved).
oracle "merge_twice_with_intermediate_commits" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (30, 300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- error paths ---"

oracle_error "merge_nonexistent_branch" "
$SEED
SELECT dolt_merge('nope');
"

oracle_error "merge_unknown_flag" "
$SEED
SELECT dolt_merge('feature', '--bogus');
"

oracle_error "merge_no_args" "
SELECT dolt_merge();
"

oracle_error "merge_abort_with_extra_args" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_commit('-A', '-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_commit('-A', '-m', 'feat1');
SELECT dolt_checkout('main');
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_commit('-A', '-m', 'main2');
SELECT dolt_merge('feature');
SELECT dolt_merge('--abort', 'extra');
"

oracle_error_poststate "merge_constraint_violation_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1, 1, 'base1'), (2, 2, 'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET u = 9, v = 'feat2' WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u = 9, v = 'main1' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_unique');
SELECT dolt_merge('feature');
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id, u, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "merge_conflict_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feature' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feature edit');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main edit');
BEGIN;
SELECT dolt_merge('feature');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT v FROM t WHERE id = 1)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT v FROM t WHERE id = 1))"

oracle_error_poststate "merge_constraint_violation_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1, 1, 'base1'), (2, 2, 'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET u = 9, v = 'feat2' WHERE id = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u = 9, v = 'main1' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_unique');
BEGIN;
SELECT dolt_merge('feature');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id, u, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
