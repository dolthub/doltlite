#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  tr -d '"\r' | sort
}

oracle_import() {
  local name="$1" csv="$2" pk="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"
  printf '%s' "$csv" > "$dir/data.csv"

  local dl_out
  dl_out=$(printf '.import %s t\n.headers off\n.mode csv\nSELECT * FROM t;\n' "$dir/data.csv" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize)

  local pk_arg=""
  if [ -n "$pk" ]; then pk_arg="--pk $pk"; fi
  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    "$DOLT" table import -c $pk_arg t "$dir/data.csv" >"$dir/dt.imp" 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT * FROM t" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"
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
