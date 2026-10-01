#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$REPO_ROOT/build}"

source "$SCRIPT_DIR/lib/doltlite_suite_manifest.sh"

if [ ! -d "$BUILD_DIR" ]; then
  echo "ERROR: build directory not found: $BUILD_DIR"
  echo "Run configure/make first, or set DOLTLITE_BUILD_DIR."
  exit 1
fi

if [ -z "${DOLTLITE:-}" ] \
   && [ ! -x "$BUILD_DIR/doltlite" ] \
   && [ ! -x "$BUILD_DIR/doltlite.exe" ]; then
  echo "ERROR: $BUILD_DIR/doltlite not found or not executable"
  echo "Run make in the build directory first."
  exit 1
fi

BUILD_DIR="$(cd "$BUILD_DIR" && pwd)"
export DOLTLITE_BUILD_DIR="$BUILD_DIR"
if [ -z "${DOLTLITE:-}" ]; then
  DOLTLITE="$BUILD_DIR/doltlite"
  [ ! -x "$BUILD_DIR/doltlite.exe" ] || DOLTLITE="$BUILD_DIR/doltlite.exe"
fi

TESTS=()
case "${DOLTLITE_SUITE_SET:-all}" in
  all) suite_manifest=doltlite_all_suites ;;
  coverage) suite_manifest=doltlite_coverage_suites ;;
  timing) suite_manifest=doltlite_timing_suites ;;
  sanitizer) suite_manifest=doltlite_sanitizer_suites ;;
  windows) suite_manifest=doltlite_windows_suites ;;
  *)
    echo "ERROR: unknown DOLTLITE_SUITE_SET: $DOLTLITE_SUITE_SET"
    exit 1
    ;;
esac
while IFS= read -r line; do TESTS+=("$line"); done < <("$suite_manifest")

# DOLTLITE_SUITE_SHARD=k/n keeps every n-th suite starting at k (1-based), so
# a slow build can split one set across parallel jobs without a second list.
if [ -n "${DOLTLITE_SUITE_SHARD:-}" ]; then
  shard_k="${DOLTLITE_SUITE_SHARD%/*}"; shard_n="${DOLTLITE_SUITE_SHARD#*/}"
  if ! [ "$shard_k" -ge 1 ] 2>/dev/null || ! [ "$shard_k" -le "$shard_n" ] 2>/dev/null; then
    echo "ERROR: DOLTLITE_SUITE_SHARD must be k/n with 1 <= k <= n: $DOLTLITE_SUITE_SHARD"
    exit 1
  fi
  SHARDED=()
  for i in "${!TESTS[@]}"; do
    if [ $(( i % shard_n )) -eq $(( shard_k - 1 )) ]; then SHARDED+=("${TESTS[$i]}"); fi
  done
  TESTS=("${SHARDED[@]}")
  echo "Shard $DOLTLITE_SUITE_SHARD: ${#TESTS[@]} suites"
fi

total_pass=0
total_fail=0
total_skip=0
skipped=""
failed=""

cd "$BUILD_DIR"

for t in "${TESTS[@]}"; do
  echo ""
  echo "━━━ $t ━━━"
  guarded=(bash "$SCRIPT_DIR/run_guarded_suite.sh" "$SCRIPT_DIR/$t" "$DOLTLITE")
  if "${guarded[@]}"; then
    total_pass=$((total_pass + 1))
  else
    rc=$?
    if [ "$rc" -eq 77 ]; then
      total_skip=$((total_skip + 1))
      skipped="$skipped $t"
      continue
    fi
    total_fail=$((total_fail + 1))
    failed="$failed $t"
    echo "FAIL: $t"
  fi
done

echo ""
echo "════════════════════════════════════════"
echo "Doltlite tests: $total_pass passed, $total_fail failed, $total_skip skipped out of $((total_pass + total_fail + total_skip)) suites"
if [ "$total_skip" -gt 0 ]; then
  echo "Unexpected skips:$skipped"
fi
if [ "$total_fail" -gt 0 ] || [ "$total_skip" -gt 0 ]; then
  echo "Failures:$failed"
  echo "════════════════════════════════════════"
  exit 1
fi
echo "════════════════════════════════════════"
