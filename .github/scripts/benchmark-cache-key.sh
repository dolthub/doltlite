#!/usr/bin/env bash

set -euo pipefail

source_dir="${1:?Usage: benchmark-cache-key.sh <base-source-directory>}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
read -r -a compiler <<< "${CC:-cc}"
{
  git -C "$source_dir" rev-parse HEAD
  sha256sum "$script_dir/build-optimized-benchmark.sh" "$script_dir/benchmark-cache-key.sh"
  uname -m
  printf '%s\n' "${ImageOS:-}" "${ImageVersion:-}"
  cc --version
  cc -dumpmachine
  "${compiler[@]}" --version
  for tool in "${compiler[@]}"; do
    if tool_path=$(command -v "$tool"); then
      sha256sum "$tool_path"
    fi
  done
  sha256sum "$(command -v cc)" "$(command -v as)" "$(command -v ld)"
  make --version
  dpkg-query -W -f='${binary:Package}=${Version}\n' | LC_ALL=C sort
  env | LC_ALL=C sort | grep -E '^(CC|CXX|CPP|CFLAGS|CPPFLAGS|LDFLAGS|LIBS|AR|ARFLAGS|AS|LD|NM|RANLIB|MAKEFLAGS|CPATH|C_INCLUDE_PATH|LIBRARY_PATH|PKG_CONFIG_PATH|TCL_CONFIG)=' || [ "$?" -eq 1 ]
} | sha256sum | cut -d ' ' -f1
