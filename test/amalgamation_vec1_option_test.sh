#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cc_bin="${CC:-cc}"
repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-vec1-option.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

if [ ! -f "$build_dir/sqlite3.c" ] || [ ! -f "$build_dir/sqlite3.h" ]; then
  echo "FAIL: amalgamation not found in $build_dir" >&2
  exit 1
fi

libs=(-lz -lpthread -lm)
case "$(uname -s)" in
  Linux*) libs+=(-ldl) ;;
esac

"$cc_bin" -w -I"$build_dir" \
  "$repo_dir/test/amalgamation_vec1_probe.c" "$build_dir/sqlite3.c" \
  "${libs[@]}" -o "$tmp/default"
"$tmp/default" disabled

"$cc_bin" -w -DDOLTLITE_VEC1=1 -I"$build_dir" \
  "$repo_dir/test/amalgamation_vec1_probe.c" "$build_dir/sqlite3.c" \
  "${libs[@]}" -o "$tmp/enabled"
"$tmp/enabled" enabled

case "$(uname -s)" in
  Darwin)
    module="$tmp/vec1.dylib"
    "$cc_bin" -O2 -fPIC -dynamiclib -undefined dynamic_lookup \
      -I"$build_dir" "$repo_dir/ext/vec1/vec1.c" -o "$module"
    ;;
  *)
    module="$tmp/vec1.so"
    "$cc_bin" -O2 -fPIC -shared -I"$build_dir" \
      "$repo_dir/ext/vec1/vec1.c" -o "$module"
    ;;
esac
"$tmp/default" load "$module"

echo "amalgamation vec1 compile option: PASS"
