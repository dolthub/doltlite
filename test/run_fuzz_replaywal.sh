#!/bin/bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
build_dir="${DOLTLITE_FUZZ_BUILD_DIR:-$repo_root/build-fuzz}"
corpus_dir="$repo_root/test/fuzz-corpus/replaywal"

# Resolve the compiler so the lib build and the harness link use the same
# clang. libFuzzer is a clang feature; refuse to run without it.
CC="${CC:-clang}"
if ! "$CC" --version 2>&1 | grep -qi clang; then
  echo "ERROR: fuzzer requires clang (got: $($CC --version 2>&1 | head -1))" >&2
  exit 1
fi

mkdir -p "$build_dir" "$corpus_dir"

if [ ! -f "$build_dir/libdoltlite.a" ] || [ -n "${DOLTLITE_FUZZ_REBUILD:-}" ]; then
  echo "=== Building libdoltlite.a with fuzzer + ASan in $build_dir ==="
  (
    cd "$build_dir"
    if [ ! -f Makefile ] || [ -n "${DOLTLITE_FUZZ_RECONFIGURE:-}" ]; then
      "$repo_root/configure"
    fi
    make \
      CC="$CC" \
      CFLAGS="-O1 -g -fsanitize=fuzzer-no-link,address -fno-omit-frame-pointer -fno-sanitize-recover=address" \
      LDFLAGS="-fsanitize=fuzzer-no-link,address" \
      libdoltlite.a
  )
fi

echo "=== Compiling fuzz harness ==="
"$CC" -O1 -g \
  -fsanitize=fuzzer,address -fno-omit-frame-pointer -fno-sanitize-recover=address \
  -DDOLTLITE_PROLLY=1 -D_HAVE_SQLITE_CONFIG_H \
  -I"$build_dir" -I"$repo_root/src" \
  -o "$build_dir/fuzz_replaywal" \
  "$repo_root/test/fuzz_replaywal.c" \
  "$build_dir/libdoltlite.a" \
  -lz -lpthread

echo "=== Running fuzzer ==="
exec "$build_dir/fuzz_replaywal" "$corpus_dir" "$@"
