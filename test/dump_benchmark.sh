#!/usr/bin/env bash
set -euo pipefail
build=$(cd "${1:-build}" && pwd)
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mib=${DOLTLITE_EXPORT_BENCH_MIB:-100}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
libs=(-lz -lpthread -lm)
case $(uname -s) in
  Linux*) libs+=(-ldl) ;;
  MINGW*|MSYS*|CYGWIN*) libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac
${CC:-cc} ${CFLAGS:--O2} -I"$build" "$root/test/dump_benchmark.c" \
  "$build/libdoltlite.a" "${libs[@]}" -o "$tmp/benchmark"
"$tmp/benchmark" seed "$tmp/source" "$mib"
for format in sql sqlite; do
  "$tmp/benchmark" export "$tmp/source" "$format" "$tmp/export.$format"
  "$tmp/benchmark" verify "$tmp/source" "$format" "$tmp/export.$format"
done
