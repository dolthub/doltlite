#!/usr/bin/env bash
set -euo pipefail

{
  sw_vers
  shasum -a 256 "$(xcrun --find clang)"
  xcrun --show-sdk-path
  xcrun --show-sdk-version
  cc --version
  go version
  go env CC CGO_CFLAGS CGO_CPPFLAGS CGO_CXXFLAGS CGO_LDFLAGS GOOS GOARCH
  rustc -vV
  cargo --version
  ccache --version
  printf '%s\n' "${ImageOS:-}" "${ImageVersion:-}" \
    "${CC:-}" "${CXX:-}" "${CFLAGS:-}" "${CPPFLAGS:-}" \
    "${CXXFLAGS:-}" "${LDFLAGS:-}" "${RUSTFLAGS:-}"
} | shasum -a 256 | cut -d ' ' -f 1
