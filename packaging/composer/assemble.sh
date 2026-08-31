#!/bin/bash
#
# Stage the dolthub/doltlite-php Composer package into an output directory.
# Copies this directory's composer.json/src/README alongside the shared
# library `make doltlite-lib` produced (placed under lib/<os>-<arch>/ where
# src/Lib.php resolves it), stamps the version, and drops in the repo LICENSE.
# A release merges the lib/ trees of per-platform runs of this script.
#
# Usage: assemble.sh <version> <out_dir> <build_dir>
#   version    release version without the leading "v" (e.g. 0.11.15)
#   out_dir    directory to create and populate (e.g. dist/composer)
#   build_dir  build directory containing libdoltlite.{so,dylib,dll}

set -euo pipefail

VERSION="${1:?usage: assemble.sh <version> <out_dir> <build_dir>}"
OUT_DIR="${2:?usage: assemble.sh <version> <out_dir> <build_dir>}"
BUILD_DIR="${3:?usage: assemble.sh <version> <out_dir> <build_dir>}"

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"

case "$(uname -s)" in
  Darwin) OS=darwin; EXT=dylib ;;
  MINGW*|MSYS*|CYGWIN*) OS=windows; EXT=dll ;;
  *) OS=linux; EXT=so ;;
esac
case "$(uname -m)" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64) ARCH=x86_64 ;;
  *) ARCH="$(uname -m)" ;;
esac

LIB="$BUILD_DIR/libdoltlite.$EXT"
if [ ! -f "$LIB" ]; then
  echo "ERROR: missing shared library: $LIB (did 'make doltlite-lib' run?)" >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/lib/$OS-$ARCH"

cp -R "$PKG_DIR/src" "$OUT_DIR/src"
cp "$PKG_DIR/README.md" "$OUT_DIR/README.md"
cp "$ROOT/LICENSE.md" "$OUT_DIR/LICENSE.md"
cp "$LIB" "$OUT_DIR/lib/$OS-$ARCH/libdoltlite.$EXT"

# Stamp the version into the staged composer.json without depending on jq.
php -r '
  $src = $argv[1]; $dst = $argv[2]; $v = $argv[3];
  $j = json_decode(file_get_contents($src), true, 512, JSON_THROW_ON_ERROR);
  $j["version"] = $v;
  file_put_contents($dst, json_encode($j,
      JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n");
' "$PKG_DIR/composer.json" "$OUT_DIR/composer.json" "$VERSION"

echo "Staged dolthub/doltlite-php@$VERSION ($OS-$ARCH) in $OUT_DIR:"
ls -laR "$OUT_DIR" | head -30
