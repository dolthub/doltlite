#!/usr/bin/env bash
# Stock sqlite3 must not satisfy DoltLite suites or sql-differential.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENG="${1:-${DOLTLITE:-$SCRIPT_DIR/../build/doltlite}}"
STOCK="${2:-${SQLITE3:-}}"
if [ -z "$STOCK" ] || [ ! -x "$STOCK" ]; then
  for c in "$SCRIPT_DIR/../build-stockref/sqlite3" ./sqlite3-stock; do
    if [ -x "$c" ]; then STOCK="$c"; break; fi
  done
fi

echo "=== engine floor ==="

if [ -x "$ENG" ]; then
  if ! bash "$SCRIPT_DIR/lib/assert_doltlite_engine.sh" "$ENG"; then
    echo "FAIL: assert_doltlite_engine.sh rejected $ENG"
    exit 1
  fi
  echo "PASS: $ENG accepted as engine"
  if ! DOLTLITE="$ENG" bash -c 'set -u
    . "'"$SCRIPT_DIR"'/lib/doltlite_test_common.sh"
    run_test_match "nounset_no_bail" "SELECT 1;" "." ":memory:"
    [ "$FAIL" -eq 0 ]
  '; then
    echo "FAIL: common.sh run_test_match broke under set -u"
    exit 1
  fi
  echo "PASS: common.sh run_test_match under set -u"

  fake="$(mktemp "${TMPDIR:-/tmp}/dltest-fake.XXXXXX")"
  printf '%s\n' '#!/bin/sh' 'echo 1' 'exit 7' >"$fake"
  chmod +x "$fake"
  if DLTEST_SKIP_ENGINE_FLOOR=1 DOLTLITE="$fake" bash -c '
    . "'"$SCRIPT_DIR"'/lib/doltlite_test_common.sh"
    run_test "nonzero_exit" "SELECT 1;" "1" ":memory:"
    [ "$FAIL" -gt 0 ]
  '; then
    echo "PASS: run_test rejects matching output with rc!=0"
  else
    echo "FAIL: run_test passed when the engine exited 7"
    rm -f "$fake"
    exit 1
  fi
  rm -f "$fake"

  crash="$(mktemp "${TMPDIR:-/tmp}/parity-crash.XXXXXX")"
  ok="$(mktemp "${TMPDIR:-/tmp}/parity-ok.XXXXXX")"
  printf '%s\n' '#!/bin/sh' 'echo 1' 'exit 134' >"$crash"
  printf '%s\n' '#!/bin/sh' 'echo 1' 'exit 0' >"$ok"
  chmod +x "$crash" "$ok"
  if ! (
    PASS=0 FAIL=0 ERRORS=""
    DOLTLITE="$crash" SQLITE3="$ok"
    . "$SCRIPT_DIR/lib/parity_run.sh"
    run_parity "rc_mismatch" "SELECT 1;"
    [ "$FAIL" -eq 1 ]
  ); then
    echo "FAIL: run_parity passed when stdout matched and rcs differed"
    rm -f "$crash" "$ok"
    exit 1
  fi
  echo "PASS: run_parity rejects matching stdout with mismatched rc"
  if ! (
    PASS=0 FAIL=0 ERRORS=""
    DOLTLITE="$crash" SQLITE3="$crash"
    . "$SCRIPT_DIR/lib/parity_run.sh"
    run_parity "matching_crashes" "SELECT 1;"
    [ "$FAIL" -eq 1 ]
  ); then
    echo "FAIL: run_parity passed when both sides crashed with matching stdout"
    rm -f "$crash" "$ok"
    exit 1
  fi
  echo "PASS: run_parity rejects matching crashes"
  if ! (
    PASS=0 FAIL=0 ERRORS=""
    DOLTLITE="$ok" SQLITE3="$ok"
    . "$SCRIPT_DIR/lib/parity_run.sh"
    run_parity "matching_success" "SELECT 1;"
    [ "$PASS" -eq 1 ] && [ "$FAIL" -eq 0 ]
  ); then
    echo "FAIL: run_parity rejected matching stdout with rc=0"
    rm -f "$crash" "$ok"
    exit 1
  fi
  echo "PASS: run_parity accepts matching stdout with rc=0"
  rm -f "$crash" "$ok"
else
  echo "SKIP: no DoltLite engine at $ENG"
fi

if [ ! -x "$STOCK" ]; then
  echo "SKIP: no stock sqlite3 for rejection tests"
  echo "engine floor: PASS"
  echo "__SUITE_COMPLETE__"
  exit 0
fi

if bash "$SCRIPT_DIR/lib/assert_doltlite_engine.sh" "$STOCK" >/tmp/floor-stock.out 2>&1; then
  echo "FAIL: assert_doltlite_engine.sh accepted stock sqlite3"
  cat /tmp/floor-stock.out
  exit 1
fi
echo "PASS: stock sqlite3 rejected as engine"

if DOLTLITE="$STOCK" bash -c '. "'"$SCRIPT_DIR"'/lib/doltlite_test_common.sh"' \
     >/tmp/floor-common.out 2>&1; then
  echo "FAIL: common.sh accepted stock sqlite3"
  cat /tmp/floor-common.out
  exit 1
fi
echo "PASS: common.sh rejects stock sqlite3"

if bash "$SCRIPT_DIR/sql_differential_test.sh" "$STOCK" "$STOCK" 1 1 \
     >/tmp/floor-diff.out 2>&1; then
  echo "FAIL: sql-differential stock vs stock passed"
  cat /tmp/floor-diff.out
  exit 1
fi
if ! grep -q "SQLite database header\|same file" /tmp/floor-diff.out; then
  echo "FAIL: sql-differential stock vs stock failed for the wrong reason"
  cat /tmp/floor-diff.out
  exit 1
fi
echo "PASS: sql-differential stock vs stock rejected"

echo "engine floor: PASS"
echo "__SUITE_COMPLETE__"
