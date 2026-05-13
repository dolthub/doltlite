#!/usr/bin/env bash
#
# Runner for the orphan C tests that are wired into CI.
#
# Usage: run_c_tests.sh BUILD_DIR
#   BUILD_DIR is where the test binaries live (typically the doltlite build
#   directory, e.g. "build" or "build-test"). All test binaries are expected
#   to already be built. Use `make c-tests` to build them.

set -u

BUILD_DIR="${1:-.}"
cd "$BUILD_DIR" || { echo "Build dir $BUILD_DIR not found"; exit 2; }

# Tests that pass cleanly on master and should break CI on regression.
GATING=(
  ancestor_test
  sql_transaction_test
  invariant_test
  three_way_diff_test
  multi_process_test
  oom_dolt_fault_test
  cross_branch_test
  corruption_test
)

run_one() {
  local name="$1"
  local kind="$2"
  local exe="./$name"

  if [ ! -x "$exe" ]; then
    echo "MISSING: $exe (not built)"
    return 1
  fi

  echo "============================================================"
  echo "[$kind] running $name"
  echo "============================================================"
  "$exe"
  local ec=$?
  echo "[$kind] $name exited with $ec"
  return $ec
}

gating_failures=0

for t in "${GATING[@]}"; do
  if ! run_one "$t" GATING; then
    gating_failures=$((gating_failures + 1))
  fi
done

echo
echo "============================================================"
echo "Summary"
echo "============================================================"
echo "GATING failures        : $gating_failures / ${#GATING[@]}"

if [ "$gating_failures" -ne 0 ]; then
  echo "FAIL: $gating_failures gating tests failed"
  exit 1
fi

echo "OK: all gating tests passed"
exit 0
