#!/usr/bin/env bash
set -euo pipefail

amalgamation="${1:-sqlite3.c}"
emcc_bin="${EMCC:-emcc}"

if ! command -v "$emcc_bin" >/dev/null 2>&1; then
  echo "SKIP: emcc not found"
  exit 0
fi
if [ ! -f "$amalgamation" ]; then
  echo "FAIL: amalgamation not found at $amalgamation"
  exit 1
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-wasm.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

"$emcc_bin" -Werror -Wno-comment -c "$amalgamation" \
  -DSQLITE_WASM \
  -DSQLITE_ENABLE_BYTECODE_VTAB \
  -DSQLITE_ENABLE_COLUMN_METADATA \
  -DSQLITE_ENABLE_DBPAGE_VTAB \
  -DSQLITE_ENABLE_DBSTAT_VTAB \
  -DSQLITE_ENABLE_FTS4 \
  -DSQLITE_ENABLE_FTS5 \
  -DSQLITE_ENABLE_PREUPDATE_HOOK \
  -DSQLITE_ENABLE_RTREE \
  -DSQLITE_ENABLE_SESSION \
  -DSQLITE_ENABLE_STMTVTAB \
  -DSQLITE_THREADSAFE=0 \
  -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
  -DSQLITE_TEMP_STORE=2 \
  -DSQLITE_ENABLE_MATH_FUNCTIONS \
  -DSQLITE_OS_OTHER=1 \
  -DVEC1_THREADS=0 \
  -DSQLITE_C=sqlite3.c \
  -DSQLITE_OMIT_DEPRECATED \
  -DSQLITE_OMIT_UTF16 \
  -DSQLITE_OMIT_LOAD_EXTENSION \
  -DSQLITE_OMIT_SHARED_CACHE \
  -DDOLTLITE_PROLLY=1 \
  -DDOLTLITE_VEC1=0 \
  -DDOLTLITE_VERSION=\"wasm-compile-test\" \
  -DSQLITE_WASM_SPLIT_BUILD \
  -D_HAVE_SQLITE_CONFIG_H \
  -DBUILD_sqlite \
  -o "$tmp/sqlite3.o"

"$emcc_bin" -Werror -Wno-comment -c "$amalgamation" \
  -Oz -flto \
  -DSQLITE_DQS=0 \
  -DSQLITE_THREADSAFE=0 \
  -DSQLITE_DEFAULT_MEMSTATUS=0 \
  -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
  -DSQLITE_OMIT_WAL \
  -DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
  -DSQLITE_OMIT_DECLTYPE \
  -DSQLITE_OMIT_DEPRECATED \
  -DSQLITE_OMIT_SHARED_CACHE \
  -DSQLITE_OMIT_AUTOINIT \
  -DSQLITE_OMIT_UTF16 \
  -DDOLTLITE_PROLLY=1 \
  -DSQLITE_WASM \
  -DSQLITE_USE_ALLOCA \
  -DVEC1_THREADS=0 \
  -DSQLITE_OS_OTHER=1 \
  -DSQLITE_ENABLE_BATCH_ATOMIC_WRITE \
  -o "$tmp/sqlite3-user-flags.o"

test -s "$tmp/sqlite3.o"
test -s "$tmp/sqlite3-user-flags.o"

"$emcc_bin" -Werror -Wno-comment "$amalgamation" \
  -s ALLOW_MEMORY_GROWTH=1 \
  -s WASM=1 \
  -s 'ENVIRONMENT=web,worker,node' \
  -s STACK_SIZE=512KB \
  --no-entry \
  --minify=0 \
  -DSQLITE_DQS=0 \
  -DSQLITE_THREADSAFE=0 \
  -DSQLITE_DEFAULT_MEMSTATUS=0 \
  -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
  -DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
  -DSQLITE_OMIT_DECLTYPE \
  -DSQLITE_OMIT_DEPRECATED \
  -DSQLITE_OMIT_SHARED_CACHE \
  -DSQLITE_OMIT_AUTOINIT \
  -DSQLITE_OMIT_UTF16 \
  -DDOLTLITE_PROLLY=1 \
  -DSQLITE_WASM \
  -DSQLITE_USE_ALLOCA \
  -DVEC1_THREADS=0 \
  -DSQLITE_ENABLE_BATCH_ATOMIC_WRITE \
  -Oz \
  -flto \
  -s MAIN_MODULE=2 \
  -s "EXPORTED_FUNCTIONS=['_sqlite3_initialize']" \
  -o "$tmp/sqlite.mjs"

test -s "$tmp/sqlite.mjs"
echo "amalgamation wa-sqlite compile: PASS"
