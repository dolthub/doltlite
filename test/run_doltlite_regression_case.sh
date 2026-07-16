#!/bin/bash
set -euo pipefail

case_name="${1:-all}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
build_dir="${DOLTLITE_BUILD_DIR:-$repo_root/build}"

if [ ! -d "$build_dir" ]; then
  echo "ERROR: build directory not found: $build_dir"
  exit 1
fi

link_libs=(-lz -lpthread -lm)
case "$(uname -s 2>/dev/null || echo unknown)" in
  MINGW*|MSYS*|CYGWIN*) link_libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac

cc -g -I"$build_dir" -I"$repo_root/src" -o "$build_dir/doltlite_regression_test_c" \
  "$repo_root/test/doltlite_regression_test_c.c" "$build_dir/libdoltlite.a" \
  "${link_libs[@]}"
"$build_dir/doltlite_regression_test_c" "$case_name"
