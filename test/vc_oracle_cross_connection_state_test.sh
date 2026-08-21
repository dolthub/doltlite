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

# Each helper starts a new CLI process; later connections observe leftover state.

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
  # Conflict is a handled SQL error; the shell still COMMITs. Next connection proves persistence.
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

dl_exec "working" "UPDATE t SET v='pending' WHERE id=1;"
dt_exec "working" "UPDATE t SET v='pending' WHERE id=1;"
paired_query "working" "unstaged_change_visible_to_next_connection" \
  "SELECT 'Q|row|' || v FROM t WHERE id=1;
   SELECT 'Q|status|' || table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "SELECT CONCAT('Q|row|', v) FROM t WHERE id=1;
   SELECT CONCAT('Q|status|', table_name, '|', staged, '|', status) FROM dolt_status;"

dl_exec "working" "SELECT dolt_add('t');"
dt_exec "working" "CALL dolt_add('t');"
paired_query "working" "staged_change_visible_to_next_connection" \
  "SELECT 'Q|status|' || table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "SELECT CONCAT('Q|status|', table_name, '|', staged, '|', status) FROM dolt_status;"

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
# Conflict handoff is not compared: Dolt can persist conflicts; DoltLite cannot.

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
