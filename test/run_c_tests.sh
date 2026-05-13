#!/usr/bin/env bash
#
# Runner for the 8 orphan C tests that were previously not wired into CI.
#
# Two categories:
#
#   GATING            - currently passes on master. A failure here breaks CI.
#   EXPECTED_FAIL     - currently has preexisting failures or crashes on
#                       master. Run anyway (because the whole point of wiring
#                       these in is to surface bugs), but a nonzero exit
#                       does NOT break CI. The exit status and pass/fail
#                       counts are still printed so PRs can see them.
#
# When you fix the bug surfaced by a test in EXPECTED_FAIL, move it to GATING
# in this file so future regressions break CI.
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
)

# Tests that have known preexisting failures or crashes on master. These are
# run but their nonzero exit does not break CI. Each one has a follow-up
# bug to track.
#
# Counts at time of wiring (master @ HEAD when this file was first added):
#   cross_branch_test        crashes after 4 reported fails (SIGSEGV)
#   corruption_test          crashes after 4 reported fails (SIGABRT/139)
#
# These represent real, latent bugs that the CI suite was previously
# hiding. They should be triaged and fixed one at a time, after which
# each test should be promoted into GATING.
EXPECTED_FAIL=(
  cross_branch_test
  corruption_test
)

run_one() {
  local name="$1"
  local kind="$2"   # "GATING" or "EXPECTED_FAIL"
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
expected_failures=0

for t in "${GATING[@]}"; do
  if ! run_one "$t" GATING; then
    gating_failures=$((gating_failures + 1))
  fi
done

for t in "${EXPECTED_FAIL[@]}"; do
  if ! run_one "$t" EXPECTED_FAIL; then
    expected_failures=$((expected_failures + 1))
  fi
done

echo
echo "============================================================"
echo "Summary"
echo "============================================================"
echo "GATING failures        : $gating_failures / ${#GATING[@]}"
echo "EXPECTED_FAIL failures : $expected_failures / ${#EXPECTED_FAIL[@]} (not gating)"

if [ "$gating_failures" -ne 0 ]; then
  echo "FAIL: $gating_failures gating tests failed"
  exit 1
fi

echo "OK: all gating tests passed"
exit 0
