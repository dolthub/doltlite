#!/bin/bash
#
# run_sqllogictest.sh — Run the full sqllogictest suite using the C runner binary.
#
# Usage: bash run_sqllogictest.sh <doltlite-runner> <stock-runner> <test-dir>
#
#   doltlite-runner: sqllogictest binary linked against doltlite's amalgamation
#   stock-runner:    sqllogictest binary linked against stock SQLite (reference)
#   test-dir:        root of sqllogictest test/ directory (from the fossil repo)
#
# Runs every .test file found recursively under test-dir through both runners
# in --verify mode, comparing pass/fail counts.
# Exit code 0 always (informational — does not fail CI).

set -euo pipefail

DOLTLITE_RUNNER="$1"
STOCK_RUNNER="$2"
TESTDIR="$3"

# Find ALL .test files recursively
mapfile -t TEST_FILES < <(find "$TESTDIR" -name '*.test' -type f | sort)

if [ ${#TEST_FILES[@]} -eq 0 ]; then
  echo "ERROR: No .test files found under $TESTDIR"
  exit 1
fi

echo "============================================"
echo "SQL Logic Test: doltlite vs stock SQLite"
echo "============================================"
echo "Test directory: $TESTDIR"
echo "Test files found: ${#TEST_FILES[@]}"
echo ""

# Per-file timeout (seconds). Most files finish in seconds; a few large ones
# (e.g. random/expr/*.test) can take minutes.
PER_FILE_TIMEOUT=300

dl_total_pass=0 dl_total_fail=0 dl_total_errors=0
sq_total_pass=0 sq_total_fail=0 sq_total_errors=0
n_files=0

# Parse "N errors out of M tests" from sqllogictest output.
# Sets: _errors _tests
parse_result() {
  local output="$1" rc="$2"
  _errors=0 _tests=0
  if [[ "$output" =~ ([0-9]+)\ errors\ out\ of\ ([0-9]+)\ tests ]]; then
    _errors="${BASH_REMATCH[1]}"
    _tests="${BASH_REMATCH[2]}"
  fi
  # rc=124 means timeout killed it
  if [ "$rc" -eq 124 ]; then
    _errors=-1 _tests=-1
  fi
}

for f in "${TEST_FILES[@]}"; do
  fname="${f#$TESTDIR/}"
  n_files=$((n_files + 1))

  # Run stock SQLite (reference)
  sq_out=$(timeout "$PER_FILE_TIMEOUT" "$STOCK_RUNNER" --verify "$f" 2>&1) && sq_rc=0 || sq_rc=$?
  parse_result "$sq_out" "$sq_rc"
  sq_e=$_errors sq_t=$_tests

  # Run doltlite
  dl_out=$(timeout "$PER_FILE_TIMEOUT" "$DOLTLITE_RUNNER" --verify "$f" 2>&1) && dl_rc=0 || dl_rc=$?
  parse_result "$dl_out" "$dl_rc"
  dl_e=$_errors dl_t=$_tests

  # Format output
  if [ "$sq_e" -eq -1 ] 2>/dev/null; then
    sq_display="TIMEOUT"
    sq_total_errors=$((sq_total_errors + 1))
  elif [ "$sq_t" -eq 0 ] && [ "$sq_rc" -ne 0 ]; then
    sq_display="ERROR"
    sq_total_errors=$((sq_total_errors + 1))
  else
    sq_p=$((sq_t - sq_e))
    sq_display="$sq_p/$sq_t"
    sq_total_pass=$((sq_total_pass + sq_p))
    sq_total_fail=$((sq_total_fail + sq_e))
  fi

  if [ "$dl_e" -eq -1 ] 2>/dev/null; then
    dl_display="TIMEOUT"
    dl_total_errors=$((dl_total_errors + 1))
  elif [ "$dl_t" -eq 0 ] && [ "$dl_rc" -ne 0 ]; then
    dl_display="ERROR"
    dl_total_errors=$((dl_total_errors + 1))
  else
    dl_p=$((dl_t - dl_e))
    dl_display="$dl_p/$dl_t"
    dl_total_pass=$((dl_total_pass + dl_p))
    dl_total_fail=$((dl_total_fail + dl_e))
  fi

  printf "[%d/%d] %-55s  stock: %10s  doltlite: %10s\n" \
    "$n_files" "${#TEST_FILES[@]}" "$fname" "$sq_display" "$dl_display"
done

echo ""
echo "============================================"
echo "Totals ($n_files files)"
echo "  stock SQLite: $sq_total_pass passed, $sq_total_fail failed, $sq_total_errors errors/timeouts"
echo "  doltlite:     $dl_total_pass passed, $dl_total_fail failed, $dl_total_errors errors/timeouts"
if [ "$sq_total_pass" -gt 0 ]; then
  pct=$((dl_total_pass * 100 / sq_total_pass))
  echo "  doltlite pass rate: ${pct}% of stock SQLite"
fi
echo "============================================"

# Informational — don't fail CI
exit 0
