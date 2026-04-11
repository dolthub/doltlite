#!/bin/bash
#
# Oracle test: shell `.import` dot-command
#
# Imports the same CSV via doltlite (`.import file table`) and Dolt
# (`dolt table import -c table file`) and compares the resulting row
# data. Per project convention, column names are NOT compared (doltlite
# uses ANY-typed columns and may name them differently than dolt's
# inferred schema). Only row content is compared, with rows sorted
# textually after stripping CSV quoting.
#
# Specifically targets the regression where doltlite's `.import` would
# inherit the column separator from the *display* mode (default `list`
# in batch mode → `|`), causing comma-CSV files to be parsed as a single
# column whose name was the entire header line (issue #383).
#
# Usage: bash oracle_import_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Strip CSV quoting and CR, sort the rows. The two engines may quote
# differently on output even when the underlying values agree.
normalize() {
  tr -d '"\r' | sort
}

# $1=name, $2=csv body, $3=optional explicit pk column for dolt
oracle_import() {
  local name="$1" csv="$2" pk="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"
  printf '%s' "$csv" > "$dir/data.csv"

  # doltlite: use .import, then dump rows as CSV without headers.
  local dl_out
  dl_out=$(printf '.import %s t\n.headers off\n.mode csv\nSELECT * FROM t;\n' "$dir/data.csv" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize)

  # dolt: use `dolt table import -c`, then dump rows.
  local pk_arg=""
  if [ -n "$pk" ]; then pk_arg="--pk $pk"; fi
  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    "$DOLT" table import -c $pk_arg t "$dir/data.csv" >"$dir/dt.imp" 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT * FROM t" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"
  # Strip dolt's header row (first line of csv output).
  dt_out=$(tail -n +2 "$dir/dt.raw" | normalize)

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

# Like oracle_import, but only checks doltlite — verifies that the
# created table has the expected number of columns. This is the most
# direct test for the #383 regression: a header line with N comma-
# separated names must produce a table with N columns, not 1.
assert_dl_columns() {
  local name="$1" csv="$2" expected_ncols="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl"
  printf '%s' "$csv" > "$dir/data.csv"

  local actual
  actual=$(printf '.import %s t\nSELECT count(*) FROM pragma_table_info(%s);\n' \
                  "$dir/data.csv" "'t'" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r')

  if [ "$actual" = "$expected_ncols" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected $expected_ncols columns, got: $actual"
  fi
}

echo "=== Oracle Tests: .import dot-command ==="
echo ""

echo "--- column count regression (#383) ---"

# The exact bug: a comma-CSV must produce N columns, not 1.
assert_dl_columns "header_three_cols" \
"name,age,city
Alice,30,NYC
Bob,25,LA
" "3"

assert_dl_columns "header_one_col" \
"only
a
b
" "1"

assert_dl_columns "header_six_cols" \
"a,b,c,d,e,f
1,2,3,4,5,6
" "6"

echo "--- basic CSV import (vs dolt) ---"

oracle_import "basic_three_cols" \
"id,name,city
1,Alice,NYC
2,Bob,LA
3,Carol,SF
"

oracle_import "two_cols" \
"k,v
alpha,1
beta,2
gamma,3
"

echo "--- empty fields ---"

oracle_import "empty_fields" \
"id,name,note
1,Alice,
2,,present
3,Bob,hi
"

echo "--- many rows ---"

# Build a 50-row CSV.
big_csv="id,n
"
for i in $(seq 1 50); do
  big_csv="${big_csv}${i},row${i}
"
done
oracle_import "fifty_rows" "$big_csv"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
