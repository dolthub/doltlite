#!/bin/bash
#
# Stage the doltlite-go module into an output directory ready to push to the
# distribution repo. Copies this directory's module source and vendors the
# amalgamation the build produced (build/sqlite3.c is genuinely doltlite --
# mksqlite3c.tcl --doltlite weaves the prolly engine and version control into
# it), which cgo compiles as part of the package.
#
# Usage: assemble.sh <version> <out_dir> <build_dir>
#   version    release version without the leading "v" (e.g. 0.50.2)
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
mkdir -p "$OUT_DIR"

cp "$PKG_DIR/doltlite.go" "$OUT_DIR/doltlite.go"
cp "$PKG_DIR/go.mod"      "$OUT_DIR/go.mod"
cp "$PKG_DIR/README.md"   "$OUT_DIR/README.md"
cp "$ROOT/LICENSE.md"     "$OUT_DIR/LICENSE.md"
cp "$BUILD_DIR/sqlite3.c" "$OUT_DIR/doltlite.c"
cp "$BUILD_DIR/sqlite3.h" "$OUT_DIR/doltlite.h"

# Go modules take their version from the repository tag, not from go.mod, so
# there is nothing to stamp -- record it for the pushing side instead.
echo "$VERSION" > "$OUT_DIR/.version"

echo "Staged doltlite-go $VERSION in $OUT_DIR:"
ls -la "$OUT_DIR"
