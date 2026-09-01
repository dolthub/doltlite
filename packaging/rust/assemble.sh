#!/bin/bash
#
# Stage the doltlite crate into an output directory ready for `cargo publish`.
# Copies this directory's manifest/src/README and vendors the amalgamation the
# build produced (build/sqlite3.c is genuinely doltlite -- mksqlite3c.tcl
# --doltlite weaves the prolly engine and version control into it), then stamps
# the version.
#
# Usage: assemble.sh <version> <out_dir> <build_dir>
#   version    release version without the leading "v" (e.g. 0.50.1)
#   out_dir    directory to create and populate
#   build_dir  build directory containing sqlite3.c and sqlite3.h

set -euo pipefail

VERSION="${1:?usage: assemble.sh <version> <out_dir> <build_dir>}"
OUT_DIR="${2:?usage: assemble.sh <version> <out_dir> <build_dir>}"
BUILD_DIR="${3:?usage: assemble.sh <version> <out_dir> <build_dir>}"

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"

for f in sqlite3.c sqlite3.h; do
  if [ ! -f "$BUILD_DIR/$f" ]; then
    echo "ERROR: missing $BUILD_DIR/$f (did 'make sqlite3.c sqlite3.h' run?)" >&2
    exit 1
  fi
done

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/vendor"

cp -R "$PKG_DIR/src" "$OUT_DIR/src"
cp "$PKG_DIR/build.rs" "$OUT_DIR/build.rs"
cp "$PKG_DIR/README.md" "$OUT_DIR/README.md"
cp "$ROOT/LICENSE.md" "$OUT_DIR/LICENSE.md"
cp "$BUILD_DIR/sqlite3.c" "$OUT_DIR/vendor/doltlite.c"
cp "$BUILD_DIR/sqlite3.h" "$OUT_DIR/vendor/doltlite.h"

sed "s/^version = \"0.0.0\"/version = \"$VERSION\"/" \
  "$PKG_DIR/Cargo.toml" > "$OUT_DIR/Cargo.toml"

echo "Staged doltlite $VERSION in $OUT_DIR:"
ls -la "$OUT_DIR" "$OUT_DIR/vendor"
