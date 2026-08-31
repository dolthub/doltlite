#!/bin/bash
#
# Stage and pack the DoltHub.Doltlite NuGet package. Copies this directory's
# csproj/src/README alongside the shared library `make doltlite-lib` produced
# (placed under runtimes/<rid>/native/ where NativeLibrary.Load resolves it)
# and runs `dotnet pack` with the version. A release merges the runtimes/
# trees of per-platform runs before the final pack.
#
# Usage: assemble.sh <version> <out_dir> <build_dir>
#   version    release version without the leading "v" (e.g. 0.11.57)
#   out_dir    directory to create and populate; the .nupkg lands in it
#   build_dir  build directory containing libdoltlite.{so,dylib,dll}

set -euo pipefail

VERSION="${1:?usage: assemble.sh <version> <out_dir> <build_dir>}"
OUT_DIR="${2:?usage: assemble.sh <version> <out_dir> <build_dir>}"
BUILD_DIR="${3:?usage: assemble.sh <version> <out_dir> <build_dir>}"

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$(uname -s)" in
  Darwin) OS=osx; SRC_LIB=libdoltlite.dylib; DST_LIB=libdoltlite.dylib ;;
  MINGW*|MSYS*|CYGWIN*) OS=win; SRC_LIB=libdoltlite.dll; DST_LIB=doltlite.dll ;;
  *) OS=linux; SRC_LIB=libdoltlite.so; DST_LIB=libdoltlite.so ;;
esac
case "$(uname -m)" in
  arm64|aarch64) ARCH=arm64 ;;
  *) ARCH=x64 ;;
esac
RID="$OS-$ARCH"

LIB="$BUILD_DIR/$SRC_LIB"
if [ ! -f "$LIB" ]; then
  echo "ERROR: missing shared library: $LIB (did 'make doltlite-lib' run?)" >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/pkg/runtimes/$RID/native"

cp "$PKG_DIR/DoltHub.Doltlite.csproj" "$OUT_DIR/pkg/"
cp -R "$PKG_DIR/src" "$OUT_DIR/pkg/src"
cp "$PKG_DIR/README.md" "$OUT_DIR/pkg/README.md"
cp "$LIB" "$OUT_DIR/pkg/runtimes/$RID/native/$DST_LIB"

dotnet pack "$OUT_DIR/pkg/DoltHub.Doltlite.csproj" \
  -p:Version="$VERSION" -c Release -o "$OUT_DIR" --nologo -v quiet

echo "Packed DoltHub.Doltlite $VERSION ($RID) into $OUT_DIR:"
ls -la "$OUT_DIR"/*.nupkg
