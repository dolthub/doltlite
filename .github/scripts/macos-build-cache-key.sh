#!/usr/bin/env bash
set -euo pipefail

case "$CACHE_BUILD_CONFIGURATION" in checked|asan) ;; *) exit 1 ;; esac
{
  sw_vers
  shasum -a 256 "$(xcrun --find clang)"
  xcrun --show-sdk-path
  xcrun --show-sdk-version
  cc --version
  make --version
  ccache --version
  printf '%s\n' "$CACHE_BUILD_CONFIGURATION" "$CACHE_BUILD_CFLAGS" \
    "$CACHE_BUILD_LDFLAGS" "${ImageOS:-}" "${ImageVersion:-}" \
    "${CC:-}" "${CXX:-}" "${CPPFLAGS:-}" "${CXXFLAGS:-}"
} | shasum -a 256 | cut -d ' ' -f 1
