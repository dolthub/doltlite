#!/bin/bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <target> [libFuzzer args...]" >&2
  echo "  target: replaywal | prolly_node | deserialize_refs | read_index | deserialize_catalog | wire_response" >&2
  exit 2
fi

target="$1"
shift

case "$target" in
  replaywal)
    src="fuzz_replaywal.c"
    corpus="replaywal"
    ;;
  prolly_node)
    src="fuzz_prolly_node.c"
    corpus="prolly_node"
    ;;
  deserialize_refs)
    src="fuzz_deserialize_refs.c"
    corpus="deserialize_refs"
    ;;
  read_index)
    src="fuzz_read_index.c"
    corpus="read_index"
    ;;
  deserialize_catalog)
    src="fuzz_deserialize_catalog.c"
    corpus="deserialize_catalog"
    ;;
  wire_response)
    src="fuzz_wire_response.c"
    corpus="wire_response"
    ;;
  *)
    echo "ERROR: unknown target '$target' (expected: replaywal | prolly_node | deserialize_refs | read_index | deserialize_catalog | wire_response)" >&2
    exit 2
    ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
build_dir="${DOLTLITE_FUZZ_BUILD_DIR:-$repo_root/build-fuzz}"
corpus_dir="$repo_root/test/fuzz-corpus/$corpus"

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

echo "=== Compiling fuzz_$target harness ==="
"$CC" -O1 -g \
  -fsanitize=fuzzer,address -fno-omit-frame-pointer -fno-sanitize-recover=address \
  -DDOLTLITE_PROLLY=1 -D_HAVE_SQLITE_CONFIG_H \
  -I"$build_dir" -I"$repo_root/src" \
  -o "$build_dir/fuzz_$target" \
  "$repo_root/test/$src" \
  "$build_dir/libdoltlite.a" \
  -lz -lpthread

echo "=== Running fuzz_$target ==="
exec "$build_dir/fuzz_$target" "$corpus_dir" "$@"
