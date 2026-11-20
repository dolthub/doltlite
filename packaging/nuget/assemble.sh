#!/bin/bash
#
# Stage and pack the DoltHub.Doltlite NuGet package. Copies this directory's
# csproj/src/README alongside the shared library `make doltlite-lib` produced
# (placed under runtimes/<rid>/native/ where NativeLibrary.Load resolves it)
# and runs `dotnet pack` with the version.
#
# Usage: assemble.sh <version> <out_dir> <build_dir>
#   version    release version without the leading "v" (e.g. 0.11.57)
#   out_dir    directory to create and populate; the .nupkg lands in it
#   build_dir  build directory containing libdoltlite.{so,dylib,dll}
# Use <asset_dir> --release-assets in place of <build_dir> to pack all runtimes
# from the four release library ZIPs.

set -euo pipefail

VERSION="${1:?usage: assemble.sh <version> <out_dir> <build_dir>}"
OUT_DIR="${2:?usage: assemble.sh <version> <out_dir> <build_dir>}"
BUILD_DIR="${3:?usage: assemble.sh <version> <out_dir> <build_dir>}"

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/pkg"
cp "$PKG_DIR/DoltHub.Doltlite.csproj" "$OUT_DIR/pkg/"
cp -R "$PKG_DIR/src" "$OUT_DIR/pkg/src"
cp "$PKG_DIR/README.md" "$OUT_DIR/pkg/README.md"

if [ "${4:-}" = "--release-assets" ]; then
  for spec in \
    "linux-x64 libdoltlite.so libdoltlite.so" \
    "linux-arm64 libdoltlite.so libdoltlite.so" \
    "osx-arm64 libdoltlite.dylib libdoltlite.dylib" \
    "win-x64 libdoltlite.dll doltlite.dll"; do
    read -r rid source destination <<< "$spec"
    bundle="doltlite-lib-$rid-$VERSION"
    native="$OUT_DIR/pkg/runtimes/$rid/native"
    mkdir -p "$native"
    unzip -p "$BUILD_DIR/$bundle.zip" "$bundle/$source" > "$native/$destination"
    test -s "$native/$destination"
  done
else
  case "$(uname -s)" in
    Darwin) os=osx; source=libdoltlite.dylib; destination=libdoltlite.dylib ;;
    MINGW*|MSYS*|CYGWIN*) os=win; source=libdoltlite.dll; destination=doltlite.dll ;;
    *) os=linux; source=libdoltlite.so; destination=libdoltlite.so ;;
  esac
  case "$(uname -m)" in
    arm64|aarch64) arch=arm64 ;;
    *) arch=x64 ;;
  esac
  library="$BUILD_DIR/$source"
  if [ ! -f "$library" ]; then
    echo "ERROR: missing shared library: $library (did 'make doltlite-lib' run?)" >&2
    exit 1
  fi
  native="$OUT_DIR/pkg/runtimes/$os-$arch/native"
  mkdir -p "$native"
  cp "$library" "$native/$destination"
fi

dotnet pack "$OUT_DIR/pkg/DoltHub.Doltlite.csproj" \
  -p:Version="$VERSION" -c Release -o "$OUT_DIR" --nologo -v quiet

echo "Packed DoltHub.Doltlite $VERSION into $OUT_DIR:"
ls -la "$OUT_DIR"/*.nupkg
