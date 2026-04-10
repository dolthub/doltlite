#!/bin/bash
#
# Version-control oracle test: dolt_status
#
# Runs identical working-set scenarios against doltlite and Dolt, then
# compares the normalized `dolt_status` output. Catches divergence in how
# each engine classifies new/modified/deleted/renamed tables, staged vs
# unstaged, and mixed states.
#
# Usage: bash vc_oracle_status_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Strip CRLF only; status rows are already in sort order.
normalize() {
  tr -d '\r'
}

# Run a scenario. $1=name, $2=setup SQL in doltlite syntax.
# The harness rewrites `SELECT dolt_*(...)` -> `CALL dolt_*(...)` for Dolt.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # doltlite side: single tab-separated column, no csv quoting.
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT table_name || char(9) || staged || char(9) || status FROM dolt_status ORDER BY table_name, staged, status;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(table_name, char(9), staged, char(9), status) FROM dolt_status ORDER BY table_name, staged, status;" 2>>"$dir/dt.err"
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

echo "=== Version Control Oracle Tests: dolt_status ==="
echo ""

echo "--- empty / baseline ---"

oracle "empty_fresh_db" "
-- no DDL, no commits; both sides should report empty status
SELECT 1;
"

echo "--- new tables ---"

oracle "new_table_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
"

oracle "new_table_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
"

oracle "two_new_tables_one_staged" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a');
"

echo "--- modifications ---"

oracle "modified_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
"

oracle "modified_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
"

oracle "modified_mixed_staged_and_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
INSERT INTO t VALUES (3, 30);
"

echo "--- deletions ---"

oracle "deleted_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
"

oracle "deleted_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
SELECT dolt_add('-A');
"

echo "--- renames ---"

oracle "renamed_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t RENAME TO t2;
"

echo "--- multi-table ---"

oracle "multi_table_mixed_states" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
INSERT INTO a VALUES (2, 20);
DROP TABLE b;
CREATE TABLE c(id INTEGER PRIMARY KEY);
INSERT INTO c VALUES (1);
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
