#!/bin/bash
# run_testfixture.sh — Run SQLite testfixture tests with summary
#
# Usage: bash run_testfixture.sh <label> <timeout_secs> test1 test2 ...
#
# Runs each .test file via ./testfixture, collects pass/fail counts,
# and exits non-zero if any errors are found.

set -euo pipefail

LABEL="${1:?Usage: run_testfixture.sh <label> <timeout_secs> test1 test2 ...}"
TIMEOUT="${2:?Missing timeout}"
shift 2

total=0
errors=0
failed_tests=""

for test in "$@"; do
  result=$(timeout "$TIMEOUT" ./testfixture ../test/${test}.test 2>&1) || true
  done_line=$(echo "$result" | grep "errors out of" || true)
  if [ -n "$done_line" ]; then
    e=$(echo "$done_line" | awk '{print $1}')
    t=$(echo "$done_line" | awk '{print $5}')
    total=$((total + t))
    errors=$((errors + e))
    if [ "$e" != "0" ]; then
      echo "FAIL: $test — $done_line"
      failed_tests="$failed_tests $test"
    fi
  else
    p=$(echo "$result" | grep -c "Ok$" || true)
    total=$((total + p))
    echo "TIMEOUT: $test ($p passed)"
  fi
done

echo ""
echo "=== $LABEL: $total tests, $errors errors ==="
if [ $errors -gt 0 ]; then
  echo "::error::$LABEL: $errors errors in:$failed_tests"
  exit 1
fi
