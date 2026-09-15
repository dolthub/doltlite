#!/usr/bin/env bash
# The wasm default stack is 64KB. Frame-size and STACK_SIZE must stay
# pinned so a 16KB record struct cannot come back unnoticed.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
PASS=0
FAIL=0
ERRORS=""

check() {
  local n="$1" cond="$2"
  if eval "$cond"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n"
  fi
}

check "main.mk_has_frame_larger_than" \
  "grep -q -- 'frame-larger-than=' '$root/main.mk'"
check "wasm_makefile_has_stack_size" \
  "grep -q -- '-sSTACK_SIZE=' '$root/ext/wasm/GNUmakefile'"
check "wasm_compile_test_has_stack_size" \
  "grep -q -- 'STACK_SIZE=' '$root/test/amalgamation_wasm_compile_test.sh'"

echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  echo -e "$ERRORS"
  echo "__SUITE_COMPLETE__"
  exit 1
fi
echo "__SUITE_COMPLETE__"
