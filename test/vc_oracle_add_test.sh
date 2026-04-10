#!/bin/bash
#
# Version-control oracle test: dolt_add
#
# Runs identical dolt_add scenarios against doltlite and Dolt and compares
# the resulting dolt_status (since dolt_add's whole purpose is to mutate
# the staged catalog, and dolt_status is what makes that mutation visible).
#
# Error scenarios are checked separately: both engines must fail, but the
# specific error text is allowed to differ.
#
# Usage: bash vc_oracle_add_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() { tr -d '\r'; }

# Compare post-state status. $1=name, $2=setup SQL using doltlite syntax.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

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

# Both engines must fail on this setup. Error text is allowed to differ.
oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc=0
  echo "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then
    dl_rc=0  # error reported
  else
    dl_rc=1  # no error
  fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_rc=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
  )
  if grep -qiE 'error|fail' "$dir/dt.out" "$dir/dt.err" 2>/dev/null; then
    dt_rc=0
  else
    dt_rc=1
  fi

  if [ "$dl_rc" = 0 ] && [ "$dt_rc" = 0 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite errored: $([ $dl_rc = 0 ] && echo yes || echo NO)"
    echo "    dolt errored:     $([ $dt_rc = 0 ] && echo yes || echo NO)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_add ==="
echo ""

echo "--- argument shapes ---"

oracle "single_table_by_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
"

oracle "two_tables_by_name" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a', 'b');
"

oracle "all_dash_capital_A" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('-A');
"

oracle "all_dash_lowercase_a" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('-a');
"

oracle "all_long_flag" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('--all');
"

oracle "dot_pathspec" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('.');
"

echo "--- idempotency and additivity ---"

oracle "idempotent_repeat" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_add('t');
"

oracle "additive_separate_calls" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a');
SELECT dolt_add('b');
"

oracle "stage_subset_leaves_others_unstaged" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a');
"

echo "--- working tree advances past staged ---"

oracle "stage_then_modify_more" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
INSERT INTO t VALUES (3, 30);
"

echo "--- staging deletions ---"

oracle "stage_dropped_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
SELECT dolt_add('t');
"

oracle "stage_dropped_table_via_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
SELECT dolt_add('-A');
"

echo "--- noop and clean states ---"

oracle "all_on_empty_repo" "
SELECT dolt_add('-A');
"

oracle "all_after_commit_no_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
SELECT dolt_add('-A');
"

echo "--- error paths ---"

oracle_error "no_args" "
SELECT dolt_add();
"

oracle_error "nonexistent_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('nonexistent');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
