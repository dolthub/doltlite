#!/usr/bin/env bash
set -u

failed=0

require() {  # require <job> <wanted> <result>
  if [ "$2" = true ]; then
    [ "$3" = success ] && return
  else
    case "$3" in success|skipped) return ;; esac
  fi
  echo "::error::$1: $3 (wanted: $2)"
  failed=1
}

any_of() {
  local value
  for value in "$@"; do [ "$value" = true ] && { echo true; return; }; done
  echo false
}

[ "${CLASSIFY:-}" = success ] || { echo "::error::classify: ${CLASSIFY:-missing}"; exit 1; }
require doc_links false "$DOC_LINKS"
built=true
[ "$LIGHT" = true ] && built=false
require build_ubuntu "$built" "$BUILD_UBUNTU"
require test_ubuntu "$(any_of "$SUITES" "$PLATFORMS" "$BENCHMARKS")" "$TEST_UBUNTU"
require compile_checks "$COMPILE" "$COMPILE_CHECKS"
require wasm "$WASM_WANTED" "$WASM"
require build_macos "$PLATFORMS" "$BUILD_MACOS"
require test_macos "$PLATFORMS" "$TEST_MACOS"
require build_windows "$PLATFORMS" "$BUILD_WINDOWS"
require test_windows "$PLATFORMS" "$TEST_WINDOWS"
require windows_suites "$PLATFORMS" "$WINDOWS_SUITES"
exit "$failed"
