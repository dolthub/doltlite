#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cc_bin="${CC:-cc}"
repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-blake3.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

if [ ! -f "$build_dir/sqlite3.c" ]; then
  echo "FAIL: amalgamation not found in $build_dir" >&2
  exit 1
fi

libs=(-lz -lpthread -lm)
case "$(uname -s)" in
  Linux*) libs+=(-ldl) ;;
esac

"$cc_bin" -w -O1 -c "$build_dir/sqlite3.c" -o "$tmp/amalgamation.o"

case "$("$cc_bin" -dumpmachine)" in
  x86_64*|amd64*) kernel=blake3_hash_many_sse2 ;;
  aarch64*|arm64*) kernel=blake3_hash_many_neon ;;
  *) kernel= ;;
esac
if [ -n "$kernel" ]; then
  nm "$tmp/amalgamation.o" > "$tmp/symbols"
  if ! grep -q "T _\{0,1\}$kernel\$" "$tmp/symbols"; then
    echo "FAIL: amalgamation does not define $kernel" >&2
    exit 1
  fi
  echo "  PASS  amalgamation defines $kernel"
fi

"$cc_bin" -w -O1 -I"$repo_dir/ext/blake3" "$repo_dir/test/blake3_kat.c" \
  "$tmp/amalgamation.o" "${libs[@]}" -o "$tmp/blake3_kat"
DOLTLITE_BLAKE3_KAT_BIN="$tmp/blake3_kat" \
  bash "$repo_dir/test/blake3_kat_test.sh" "$tmp/blake3_kat"

echo "amalgamation blake3 kernels: PASS"
