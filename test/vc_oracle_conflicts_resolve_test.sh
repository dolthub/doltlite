#!/bin/bash
#
# Version-control oracle test: dolt_conflicts_resolve
#
# Runs identical conflict-resolution scenarios against doltlite and Dolt
# and compares the resulting table data and conflict state. Covers:
#   - --ours resolution (keep our value)
#   - --theirs resolution (accept their value)
#   - multi-row conflicts resolved in one call
#   - partial resolution (multiple tables, resolve one)
#   - resolution followed by commit
#
# Usage: bash vc_oracle_conflicts_resolve_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Build a standard merge-conflict setup and add a resolution + query.
# Compare the final table data (SELECT id, v FROM t ORDER BY id) and
# the remaining conflict count.
#
# $1=name, $2=setup (up to and including the merge), $3=resolve+query SQL
oracle() {
  local name="$1" setup="$2" resolve_and_query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # -- doltlite --
  local dl_out
  dl_out=$(printf "%s\n%s\n" "$setup" "$resolve_and_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^Merge has' \
           | grep '^R|' \
           | tr -d '\r' | sort)

  # -- Dolt --
  # Everything must run in ONE `dolt sql` invocation so
  # @@autocommit=0 and @@dolt_allow_commit_conflicts=1 persist
  # across the setup, merge, resolution, and query.
  local dolt_all
  dolt_all=$(printf '%s\n%s' "$setup" "$resolve_and_query" \
             | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_all"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | tr -d '\r' | sort)

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

# Standard conflict setup: both sides modify the same row differently.
CONFLICT_SETUP="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=200 WHERE id=1;
UPDATE t SET v=201 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_checkout('main');
UPDATE t SET v=300 WHERE id=1;
UPDATE t SET v=301 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_update');
SELECT dolt_merge('feature');
"

echo "=== Version Control Oracle Tests: dolt_conflicts_resolve ==="
echo ""

echo "--- --ours resolution ---"

oracle "resolve_ours_single_table" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- --theirs resolution ---"

oracle "resolve_theirs_single_table" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- multi-table: resolve one, check other still conflicted ---"

oracle "resolve_one_of_two_tables" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, x INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO u VALUES(1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=20 WHERE id=1;
UPDATE u SET x=200 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v=30 WHERE id=1;
UPDATE u SET x=300 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_upd');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|t|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|u_conflicts|', num_conflicts) FROM dolt_conflicts WHERE \`table\`='u';"

echo "--- resolve then commit ---"

oracle "resolve_and_commit" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-A', '-m', 'resolved');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
