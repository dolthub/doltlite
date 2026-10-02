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

# The gate is only as good as what the runners report. A statement that
# succeeds where both engines are expected to error is a doltlite-only
# divergence (stock has no dolt_version()); runners that cannot surface it
# would let every statement mismatch in the corpus pass.
CANARY=$(mktemp -d "${TMPDIR:-/tmp}/slt_canary.XXXXXX")
printf 'statement error\nSELECT dolt_version()\n' > "$CANARY/canary.test"
: > "$CANARY/none.txt"
if python3 "$SCRIPT_DIR/sqllogictest_gate.py" "$DOLTLITE_RUNNER" "$STOCK_RUNNER" \
     "$CANARY" "$CANARY/none.txt" "$PER_FILE_TIMEOUT" "$CANARY/canary.test" \
     >"$CANARY/out" 2>&1 \
   || ! grep -q 'canary.test 1' "$CANARY/out"; then
  echo "ERROR: the gate did not report a doltlite-only statement divergence:"
  cat "$CANARY/out"
  rm -rf "$CANARY"
  exit 1
fi
rm -rf "$CANARY"

exec python3 "$SCRIPT_DIR/sqllogictest_gate.py" \
  "$DOLTLITE_RUNNER" "$STOCK_RUNNER" "$TESTDIR" "$DIVERGENCE_FILE" \
  "$PER_FILE_TIMEOUT" "${TEST_FILES[@]}"
