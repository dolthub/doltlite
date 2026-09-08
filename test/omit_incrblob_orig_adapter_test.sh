#!/usr/bin/env bash
# SQLITE_OMIT_INCRBLOB must not leave unresolved orig_sqlite3Btree* cursor
# symbols in btree_orig_api.o.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$ROOT/build}"
CC="${CC:-cc}"

if [ ! -f "$BUILD_DIR/sqlite_cfg.h" ]; then
  echo "SETUP FAILED: $BUILD_DIR/sqlite_cfg.h missing; configure+build first" >&2
  exit 2
fi

OBJ=$(mktemp)
ERR=$(mktemp)
trap 'rm -f "$OBJ" "$ERR"' EXIT

CFLAGS=(
  -DNDEBUG -O1 -g
  -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_THREADSAFE=1
  -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION='"dev"'
  -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE
  -D_HAVE_SQLITE_CONFIG_H -DBUILD_sqlite
  -DSQLITE_OMIT_INCRBLOB -DSQLITE_OMIT_WAL
  "-I$BUILD_DIR" "-I$ROOT/src" -Iext/rtree -Iext/icu -Iext/fts3 -Iext/session
  -Iext/misc -Iext/blake3 -Iext/ed25519 -Iext/mbedtls/include
)

if ! "$CC" "${CFLAGS[@]}" -c -o "$OBJ" src/btree_orig_api.c 2>"$ERR"; then
  echo "FAIL: btree_orig_api.c did not compile with SQLITE_OMIT_INCRBLOB" >&2
  sed 's/^/  /' "$ERR" >&2
  exit 1
fi

undef=$(nm -u "$OBJ" 2>/dev/null | grep -E 'orig_sqlite3Btree(EnterCursor|LeaveCursor|PayloadChecked)$' || true)
if [ -n "$undef" ]; then
  echo "FAIL: SQLITE_OMIT_INCRBLOB left unresolved orig btree symbols:" >&2
  echo "$undef" | sed 's/^/  /' >&2
  exit 1
fi

echo "omit_incrblob_orig_adapter: PASS"
