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
  x86_64*|amd64*) kernels="sse2 sse41 avx2 avx512" ;;
  aarch64*|arm64*) kernels="neon" ;;
  *) kernels= ;;
esac
nm "$tmp/amalgamation.o" > "$tmp/symbols"
for kernel in $kernels; do
  if ! grep -q "T _\{0,1\}doltlite_blake3_hash_many_$kernel\$" "$tmp/symbols"; then
    echo "FAIL: amalgamation does not define doltlite_blake3_hash_many_$kernel" >&2
    exit 1
  fi
  echo "  PASS  amalgamation defines doltlite_blake3_hash_many_$kernel"
done
if grep " [A-Za-z] _\{0,1\}blake3_" "$tmp/symbols"; then
  echo "FAIL: amalgamation exports or imports an unprefixed blake3_ symbol" >&2
  exit 1
fi
echo "  PASS  no unprefixed blake3_ symbol in the amalgamation"

"$cc_bin" -w -O1 -I"$repo_dir/ext/blake3" \
  -Dblake3_hasher_init=doltlite_blake3_hasher_init \
  -Dblake3_hasher_update=doltlite_blake3_hasher_update \
  -Dblake3_hasher_finalize=doltlite_blake3_hasher_finalize \
  "$repo_dir/test/blake3_kat.c" \
  "$tmp/amalgamation.o" "${libs[@]}" -o "$tmp/blake3_kat"
DOLTLITE_BLAKE3_KAT_BIN="$tmp/blake3_kat" \
  bash "$repo_dir/test/blake3_kat_test.sh" "$tmp/blake3_kat"

echo "amalgamation blake3 kernels: PASS"
