#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/sql_oracle_common.sh"

stock_oracle_init() {
  nonempty=0
  sql_oracle_check_binaries "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
}

stock_oracle_assert() {
  local name="$1" dl_out="$2" sq_out="$3" dl_rc="$4" sq_rc="$5"
  local expected_error="${6-}" expect_empty="${7:-0}" expected_rc=0
  if [ -n "$expected_error" ]; then expected_rc=1; fi
  if [ "$dl_rc" -eq "$expected_rc" ] && [ "$sq_rc" -eq "$expected_rc" ] \
     && [ "$dl_out" = "$sq_out" ] \
     && { [ "$expect_empty" -eq 1 ] && [ -z "$sq_out" ] \
          || { [ "$expect_empty" -eq 0 ] && [ -n "$sq_out" ]; }; } \
     && { [ "$expected_rc" -eq 0 ] || [[ "$sq_out" == *"$expected_error"* ]]; }; then
    pass=$((pass+1))
    if [ -n "$sq_out" ]; then nonempty=$((nonempty+1)); fi
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected rc: $expected_rc${expected_error:+ ($expected_error)}; empty: $expect_empty"
    echo "    doltlite rc: $dl_rc"
    printf '%s\n' "$dl_out" | sed 's/^/      /'
    echo "    sqlite3 rc: $sq_rc"
    printf '%s\n' "$sq_out" | sed 's/^/      /'
  fi
}

oracle_error() {
  oracle "$1" "$3" "${2:?expected error text required}"
}

oracle_empty() {
  oracle "$1" "$2" "" 1
}

stock_oracle_finish() {
  if [ "$pass" -eq 0 ] || [ "$nonempty" -eq 0 ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES suite_floor"
    echo "  FAIL: suite needs passing comparisons and non-empty compared output"
  fi
  echo ""
  echo "=== Results: $pass passed, $fail failed ==="
  if [ "$fail" -gt 0 ]; then
    echo "Failed:$FAILED_NAMES"
    return 1
  fi
}
