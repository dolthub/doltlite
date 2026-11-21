#!/bin/bash
#
# Stage the dolthub/doltlite-php Composer package into an output directory.
# Copies this directory's composer.json/src/README alongside the shared
# library `make doltlite-lib` produced (placed under lib/<os>-<arch>/ where
# src/Lib.php resolves it), stamps the version, and drops in the repo LICENSE.
#
# Usage: assemble.sh <version> <out_dir> <build_dir>
#        assemble.sh <version> <out_dir> --release-assets <artifact_dir>

set -euo pipefail

VERSION="${1:?package version required}"
OUT_DIR="${2:?output directory required}"
INPUT="${3:?build directory or --release-assets required}"

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"

copy_library() {
  local lib=$1 platform=$2
  if [ ! -f "$lib" ]; then
    echo "ERROR: missing shared library: $lib" >&2
    exit 1
  fi
  mkdir -p "$OUT_DIR/lib/$platform"
  cp "$lib" "$OUT_DIR/lib/$platform/"
}

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
cp -R "$PKG_DIR/src" "$OUT_DIR/src"
cp "$PKG_DIR/README.md" "$OUT_DIR/README.md"
cp "$ROOT/LICENSE.md" "$OUT_DIR/LICENSE.md"

if [ "$INPUT" = --release-assets ]; then
  assets="${4:?artifact directory required}"
  unpacked="$(mktemp -d)"
  trap 'rm -rf "$unpacked"' EXIT
  for spec in \
    "linux-x64 linux-x86_64 so" \
    "linux-arm64 linux-arm64 so" \
    "osx-arm64 darwin-arm64 dylib" \
    "win-x64 windows-x86_64 dll"; do
    read -r target platform extension <<<"$spec"
    asset="doltlite-lib-$target-$VERSION"
    unzip -q "$assets/$asset.zip" -d "$unpacked"
    lib="$unpacked/$asset/libdoltlite.$extension"
    if [ "$target" = win-x64 ] && [ ! -f "$lib" ]; then
      echo "warning: no libdoltlite.dll in $asset.zip"
      continue
    fi
    copy_library "$lib" "$platform"
  done
else
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
  copy_library "$INPUT/libdoltlite.$EXT" "$OS-$ARCH"
fi

# Stamp the version into the staged composer.json without depending on jq.
php -r '
  $src = $argv[1]; $dst = $argv[2]; $v = $argv[3];
  $j = json_decode(file_get_contents($src), true, 512, JSON_THROW_ON_ERROR);
  $j["version"] = $v;
  file_put_contents($dst, json_encode($j,
      JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n");
' "$PKG_DIR/composer.json" "$OUT_DIR/composer.json" "$VERSION"

echo "Staged dolthub/doltlite-php@$VERSION in $OUT_DIR:"
find "$OUT_DIR/lib" -type f -print
