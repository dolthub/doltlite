#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Every exec or query helper below starts a new CLI process.  These tests are
# intentionally about state left by one SQL connection and observed or acted
# upon by a later, independent connection.

setup_pair() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  local dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"

  if ! printf '%s\n' "$setup" \
      | "$DOLTLITE" "$dir/dl/db.sqlite" >"$dir/dl.setup.out" 2>"$dir/dl.setup.err"; then
    echo "doltlite setup failed for $name" >&2
    sed 's/^/  /' "$dir/dl.setup.err" >&2
    exit 1
  fi

  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  if ! (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c
  ) >"$dir/dt.setup.out" 2>"$dir/dt.setup.err"; then
    echo "dolt setup failed for $name" >&2
    sed 's/^/  /' "$dir/dt.setup.err" >&2
    exit 1
  fi
}

dl_exec() {
  local name="$1" sql="$2" dbspec="${3:-$TMPROOT/$1/dl/db.sqlite}"
  if ! printf '%s\n' "$sql" \
      | "$DOLTLITE" "$dbspec" >"$TMPROOT/$name/dl.exec.out" 2>"$TMPROOT/$name/dl.exec.err"; then
    echo "doltlite connection failed for $name" >&2
    sed 's/^/  /' "$TMPROOT/$name/dl.exec.err" >&2
    exit 1
  fi
}

dl_exec_conflicting_merge() {
  local name="$1" sql="$2" rc
  printf '%s\n' "$sql" \
    | "$DOLTLITE" "$TMPROOT/$name/dl/db.sqlite" \
        >"$TMPROOT/$name/dl.exec.out" 2>"$TMPROOT/$name/dl.exec.err"
  rc=$?
  # DoltLite reports the merge conflict as a handled SQL error, then the shell
  # continues to COMMIT the explicit transaction.  Either a clean error or a
  # future success return is acceptable here; the following connection proves
  # whether the conflicted state was actually persisted.
  if [ "$rc" -ge 128 ]; then
    echo "doltlite conflict connection crashed for $name (rc=$rc)" >&2
    sed 's/^/  /' "$TMPROOT/$name/dl.exec.err" >&2
    exit 1
  fi
}

dt_exec() {
  local name="$1" sql="$2"
  if ! (
    cd "$TMPROOT/$name/dt" || exit 1
    printf '%s\n' "$sql" | "$DOLT" sql -c
  ) >"$TMPROOT/$name/dt.exec.out" 2>"$TMPROOT/$name/dt.exec.err"; then
    echo "dolt connection failed for $name" >&2
    sed 's/^/  /' "$TMPROOT/$name/dt.exec.err" >&2
    exit 1
  fi
}

dl_exec_branch() {
  local name="$1" branch="$2" sql="$3"
  dl_exec "$name" "$sql" "$TMPROOT/$name/dl/db.sqlite@$branch"
}

dt_exec_branch() {
  local name="$1" branch="$2" sql="$3"
  local repo parent repo_name
  repo="$TMPROOT/$name/dt"
  parent=$(dirname "$repo")
  repo_name=$(basename "$repo")
  if ! (
    cd "$parent" || exit 1
    printf '%s\n' "$sql" | "$DOLT" --use-db "$repo_name/$branch" sql -c
  ) >"$TMPROOT/$name/dt.exec.out" 2>"$TMPROOT/$name/dt.exec.err"; then
    echo "dolt branch connection failed for $name@$branch" >&2
    sed 's/^/  /' "$TMPROOT/$name/dt.exec.err" >&2
    exit 1
  fi
}

paired_query() {
  local name="$1" assertion="$2" dl_sql="$3" dt_sql="$4"
  local dl_out dt_out

  dl_out=$(
    printf ".headers off\n.mode list\n%s\n" "$dl_sql" \
      | "$DOLTLITE" "$TMPROOT/$name/dl/db.sqlite" 2>"$TMPROOT/$name/dl.query.err" \
      | tr -d '\r' | grep '^Q|'
  )
  dt_out=$(
    cd "$TMPROOT/$name/dt" || exit 1
    printf '%s\n' "$dt_sql" | "$DOLT" sql -c -r csv 2>"$TMPROOT/$name/dt.query.err" \
      | tr -d '"\r' | grep '^Q|'
  )
  vc_oracle_assert_match "$assertion" "$dl_out" "$dt_out"
}

paired_branch_query() {
  local name="$1" assertion="$2" branch="$3" dl_sql="$4" dt_sql="$5"
  local dl_out dt_out repo parent repo_name
  repo="$TMPROOT/$name/dt"
  parent=$(dirname "$repo")
  repo_name=$(basename "$repo")

  dl_out=$(
    printf ".headers off\n.mode list\n%s\n" "$dl_sql" \
      | "$DOLTLITE" "$TMPROOT/$name/dl/db.sqlite@$branch" 2>"$TMPROOT/$name/dl.query.err" \
      | tr -d '\r' | grep '^Q|'
  )
  dt_out=$(
    cd "$parent" || exit 1
    printf '%s\n' "$dt_sql" | "$DOLT" --use-db "$repo_name/$branch" sql -c -r csv \
      2>"$TMPROOT/$name/dt.query.err" | tr -d '"\r' | grep '^Q|'
  )
  vc_oracle_assert_match "$assertion" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: state across connections ==="
echo ""
echo "--- working and staged state handoff ---"

setup_pair "working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'base');
SELECT dolt_commit('-A', '-m', 'base');
"

# Connection A leaves an unstaged edit.  Connection B must see both the row
# and its version-control status.
dl_exec "working" "UPDATE t SET v='pending' WHERE id=1;"
dt_exec "working" "UPDATE t SET v='pending' WHERE id=1;"
paired_query "working" "unstaged_change_visible_to_next_connection" \
  "SELECT 'Q|row|' || v FROM t WHERE id=1;
   SELECT 'Q|status|' || table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "SELECT CONCAT('Q|row|', v) FROM t WHERE id=1;
   SELECT CONCAT('Q|status|', table_name, '|', staged, '|', status) FROM dolt_status;"

# Connection B stages it.  Connection C must inherit the staged root.
dl_exec "working" "SELECT dolt_add('t');"
dt_exec "working" "CALL dolt_add('t');"
paired_query "working" "staged_change_visible_to_next_connection" \
  "SELECT 'Q|status|' || table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "SELECT CONCAT('Q|status|', table_name, '|', staged, '|', status) FROM dolt_status;"

# Connection C commits the already-staged edit.  Connection D sees a clean
# working set and the new value without sharing any connection state.
dl_exec "working" "SELECT dolt_commit('-m', 'pending');"
dt_exec "working" "CALL dolt_commit('-m', 'pending');"
paired_query "working" "commit_of_inherited_stage_visible_to_next_connection" \
  "SELECT 'Q|row|' || v FROM t WHERE id=1;
   SELECT 'Q|status-count|' || count(*) FROM dolt_status;
   SELECT 'Q|commit-count|' || count(*) FROM dolt_log;" \
  "SELECT CONCAT('Q|row|', v) FROM t WHERE id=1;
   SELECT CONCAT('Q|status-count|', count(*)) FROM dolt_status;
   SELECT CONCAT('Q|commit-count|', count(*)) FROM dolt_log;"

echo ""
echo "--- branch-local working sets ---"

setup_pair "branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'base');
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_branch('side');
"

# A connection pinned to side dirties only side's working set.
dl_exec_branch "branches" "side" "UPDATE t SET v='side-pending' WHERE id=1;"
dt_exec_branch "branches" "side" "UPDATE t SET v='side-pending' WHERE id=1;"
paired_query "branches" "main_connection_isolated_from_side_working_set" \
  "SELECT 'Q|branch|' || active_branch();
   SELECT 'Q|row|' || v FROM t WHERE id=1;
   SELECT 'Q|status-count|' || count(*) FROM dolt_status;" \
  "SELECT CONCAT('Q|branch|', active_branch());
   SELECT CONCAT('Q|row|', v) FROM t WHERE id=1;
   SELECT CONCAT('Q|status-count|', count(*)) FROM dolt_status;"
paired_branch_query "branches" "side_connection_recovers_its_working_set" "side" \
  "SELECT 'Q|branch|' || active_branch();
   SELECT 'Q|row|' || v FROM t WHERE id=1;
   SELECT 'Q|status|' || table_name || '|' || status FROM dolt_status;" \
  "SELECT CONCAT('Q|branch|', active_branch());
   SELECT CONCAT('Q|row|', v) FROM t WHERE id=1;
   SELECT CONCAT('Q|status|', table_name, '|', status) FROM dolt_status;"

echo ""
echo "--- conflict handoff and partial resolution ---"

setup_pair "resolve" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES(1, 10);
INSERT INTO b VALUES(1, 20);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE a SET v=100 WHERE id=1;
UPDATE b SET v=200 WHERE id=1;
SELECT dolt_commit('-A', '-m', 'feature');
SELECT dolt_checkout('main');
UPDATE a SET v=1000 WHERE id=1;
UPDATE b SET v=2000 WHERE id=1;
SELECT dolt_commit('-A', '-m', 'main');
"

# Connection A persists a conflicted merge transaction.
dl_exec_conflicting_merge "resolve" "
BEGIN;
SELECT dolt_merge('feature');
COMMIT;
"
dt_exec "resolve" "
SET @@autocommit=0;
SET @@dolt_allow_commit_conflicts=1;
CALL dolt_merge('feature');
COMMIT;
"

# Connection B recovers the conflict summary and row payload.
paired_query "resolve" "conflicts_visible_to_next_connection" \
  "SELECT 'Q|summary|' || \"table\" || '|' || num_conflicts FROM dolt_conflicts ORDER BY \"table\";
   SELECT 'Q|a|' || base_v || '|' || our_v || '|' || their_v FROM dolt_conflicts_a;
   SELECT 'Q|b|' || base_v || '|' || our_v || '|' || their_v FROM dolt_conflicts_b;" \
  "SELECT CONCAT('Q|summary|', \`table\`, '|', num_conflicts) FROM dolt_conflicts ORDER BY \`table\`;
   SELECT CONCAT('Q|a|', base_v, '|', our_v, '|', their_v) FROM dolt_conflicts_a;
   SELECT CONCAT('Q|b|', base_v, '|', our_v, '|', their_v) FROM dolt_conflicts_b;"

# Connection B resolves one table.  Connection C sees the partial resolution
# while the other table remains conflicted.
dl_exec "resolve" "SELECT dolt_conflicts_resolve('--ours', 'a');"
dt_exec "resolve" "CALL dolt_conflicts_resolve('--ours', 'a');"
paired_query "resolve" "partial_resolution_visible_to_next_connection" \
  "SELECT 'Q|summary|' || \"table\" || '|' || num_conflicts FROM dolt_conflicts ORDER BY \"table\";
   SELECT 'Q|a-value|' || v FROM a WHERE id=1;
   SELECT 'Q|b|' || base_v || '|' || our_v || '|' || their_v FROM dolt_conflicts_b;" \
  "SELECT CONCAT('Q|summary|', \`table\`, '|', num_conflicts) FROM dolt_conflicts ORDER BY \`table\`;
   SELECT CONCAT('Q|a-value|', v) FROM a WHERE id=1;
   SELECT CONCAT('Q|b|', base_v, '|', our_v, '|', their_v) FROM dolt_conflicts_b;"

# Connection C resolves the final table with the other side.  Connection D
# sees both choices and no remaining conflicts.
dl_exec "resolve" "SELECT dolt_conflicts_resolve('--theirs', 'b');"
dt_exec "resolve" "CALL dolt_conflicts_resolve('--theirs', 'b');"
paired_query "resolve" "completed_resolution_visible_to_next_connection" \
  "SELECT 'Q|a-value|' || v FROM a WHERE id=1;
   SELECT 'Q|b-value|' || v FROM b WHERE id=1;
   SELECT 'Q|conflict-count|' || count(*) FROM dolt_conflicts;" \
  "SELECT CONCAT('Q|a-value|', v) FROM a WHERE id=1;
   SELECT CONCAT('Q|b-value|', v) FROM b WHERE id=1;
   SELECT CONCAT('Q|conflict-count|', count(*)) FROM dolt_conflicts;"

echo ""
echo "--- abort handoff ---"

setup_pair "abort" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=100 WHERE id=1;
SELECT dolt_commit('-A', '-m', 'feature');
SELECT dolt_checkout('main');
UPDATE t SET v=1000 WHERE id=1;
SELECT dolt_commit('-A', '-m', 'main');
"
dl_exec_conflicting_merge "abort" "BEGIN; SELECT dolt_merge('feature'); COMMIT;"
dt_exec "abort" "
SET @@autocommit=0;
SET @@dolt_allow_commit_conflicts=1;
CALL dolt_merge('feature');
COMMIT;
"

# A later connection aborts the recovered merge, and another connection sees
# the exact pre-merge working state restored.
dl_exec "abort" "SELECT dolt_merge('--abort');"
dt_exec "abort" "CALL dolt_merge('--abort');"
paired_query "abort" "aborted_conflict_cleanup_visible_to_next_connection" \
  "SELECT 'Q|row|' || v FROM t WHERE id=1;
   SELECT 'Q|conflict-count|' || count(*) FROM dolt_conflicts;
   SELECT 'Q|status-count|' || count(*) FROM dolt_status;" \
  "SELECT CONCAT('Q|row|', v) FROM t WHERE id=1;
   SELECT CONCAT('Q|conflict-count|', count(*)) FROM dolt_conflicts;
   SELECT CONCAT('Q|status-count|', count(*)) FROM dolt_status;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
