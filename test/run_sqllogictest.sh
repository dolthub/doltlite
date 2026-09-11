#!/bin/bash

set -uo pipefail

DOLTLITE_RUNNER="${1:?Usage: run_sqllogictest.sh <doltlite-runner> <stock-runner> <test-dir> [divergence-file]}"
STOCK_RUNNER="${2:?Missing stock runner}"
TESTDIR="${3:?Missing test dir}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIVERGENCE_FILE="${4:-$SCRIPT_DIR/known_sqllogictest_divergences.txt}"

PER_FILE_TIMEOUT=300

mapfile -t TEST_FILES < <(find "$TESTDIR" -name '*.test' -type f | sort)
if [ ${#TEST_FILES[@]} -eq 0 ]; then
  echo "ERROR: No .test files found under $TESTDIR"
  exit 1
fi

echo "============================================"
echo "SQL Logic Test: per-assertion divergence gate"
echo "============================================"
echo "Test directory:  $TESTDIR"
echo "Test files:      ${#TEST_FILES[@]}"
echo "Divergence list: $DIVERGENCE_FILE ($(grep -cvE '^[[:space:]]*(#|$)' "$DIVERGENCE_FILE" 2>/dev/null || echo 0) entries)"
echo ""

exec python3 "$SCRIPT_DIR/sqllogictest_gate.py" \
  "$DOLTLITE_RUNNER" "$STOCK_RUNNER" "$TESTDIR" "$DIVERGENCE_FILE" \
  "$PER_FILE_TIMEOUT" "${TEST_FILES[@]}"
