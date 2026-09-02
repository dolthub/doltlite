#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_file="$repo_dir/src/prolly_xxhash.c"
header_file="$repo_dir/sqlite3.h"
object_file="$repo_dir/ext/wasm/bld/doltlite-build/prolly_xxhash.o"
library_file="$repo_dir/ext/wasm/bld/doltlite-build/libdoltlite.a"
wasm_file="$repo_dir/ext/wasm/jswasm/sqlite3.wasm"
copied_header="$repo_dir/ext/wasm/bld/doltlite-build/sqlite3.h"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-wasm-incremental.XXXXXX")"
cleanup() {
  if [ -f "$tmp/source.timestamp" ]; then
    touch -r "$tmp/source.timestamp" "$source_file"
  fi
  if [ -f "$tmp/header.timestamp" ]; then
    touch -r "$tmp/header.timestamp" "$header_file"
  fi
  if [ -f "$tmp/copied-header.timestamp" ]; then
    touch -r "$tmp/copied-header.timestamp" "$copied_header"
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

for artifact in "$object_file" "$library_file" "$wasm_file" "$copied_header"; do
  if [ ! -f "$artifact" ]; then
    echo "FAIL: missing initial WASM artifact: $artifact"
    exit 1
  fi
done

touch -r "$source_file" "$tmp/source.timestamp"
touch -r "$header_file" "$tmp/header.timestamp"
touch -r "$copied_header" "$tmp/copied-header.timestamp"
touch -r "$object_file" "$tmp/object.timestamp"
touch -r "$library_file" "$tmp/library.timestamp"
touch -r "$wasm_file" "$tmp/wasm.timestamp"

sleep 1
touch "$header_file"
make -C "$repo_dir/ext/wasm" doltlite.wasm.sync
if [ ! "$copied_header" -nt "$tmp/copied-header.timestamp" ]; then
  echo "FAIL: updated WASM build header was not copied"
  exit 1
fi
touch -r "$tmp/header.timestamp" "$header_file"
touch -r "$tmp/copied-header.timestamp" "$copied_header"

sleep 1
touch "$source_file"

make -C "$repo_dir/ext/wasm" emcc_opt=-Oz jswasm/sqlite3-node.mjs

if [ ! "$object_file" -nt "$tmp/object.timestamp" ]; then
  echo "FAIL: WASM object was not rebuilt after its source changed"
  exit 1
fi
if [ ! "$library_file" -nt "$tmp/library.timestamp" ]; then
  echo "FAIL: WASM libdoltlite.a was not rebuilt after its source changed"
  exit 1
fi
if [ ! "$wasm_file" -nt "$tmp/wasm.timestamp" ]; then
  echo "FAIL: Node entry point's WASM binary was not rebuilt after its source changed"
  exit 1
fi

echo "WASM incremental rebuild test passed"
