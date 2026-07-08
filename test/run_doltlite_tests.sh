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

if [ ! -x "$BUILD_DIR/doltlite" ]; then
  echo "ERROR: $BUILD_DIR/doltlite not found or not executable"
  echo "Run make in the build directory first."
  exit 1
fi

TESTS=()
while IFS= read -r line; do TESTS+=("$line"); done < <(doltlite_all_suites)

total_pass=0
total_fail=0
failed=""

cd "$BUILD_DIR"

for t in "${TESTS[@]}"; do
  echo ""
  echo "━━━ $t ━━━"
  if bash "$SCRIPT_DIR/$t"; then
    total_pass=$((total_pass + 1))
  else
    total_fail=$((total_fail + 1))
    failed="$failed $t"
    echo "FAIL: $t"
  fi
done

echo ""
echo "════════════════════════════════════════"
echo "Doltlite tests: $total_pass passed, $total_fail failed out of $((total_pass + total_fail)) suites"
if [ $total_fail -gt 0 ]; then
  echo "Failures:$failed"
  echo "════════════════════════════════════════"
  exit 1
fi
echo "════════════════════════════════════════"
