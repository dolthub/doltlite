#!/bin/bash
#
# Build doltlite.xcframework from the doltlite amalgamation for the Apple
# platforms a Swift Package needs: iOS device, iOS simulator, macOS, and Mac
# Catalyst. Each slice compiles sqlite3.c (the doltlite-woven amalgamation, so
# the dolt_* version-control functions are built in and auto-registered) as a
# single translation unit and archives it into a static library; the slices are
# combined with `xcodebuild -create-xcframework`. Emits a zipped xcframework and
# its SwiftPM checksum (the zip's SHA-256).
#
# Requires full Xcode (xcodebuild + the iOS SDKs) -- it does not run with the
# Command Line Tools alone. Run from the repo root after the amalgamation exists
# (`make sqlite3.c sqlite3.h`).
#
# Usage: build-xcframework.sh <version> [out_dir]
#   version  release version without the leading "v" (e.g. 0.11.17)
#   out_dir  directory for the .xcframework and zip (default: ./build-swift)

set -euo pipefail

VERSION="${1:?usage: build-xcframework.sh <version> [out_dir]}"
OUT_DIR="${2:-build-swift}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [ ! -f sqlite3.c ] || [ ! -f sqlite3.h ]; then
  echo "ERROR: sqlite3.c/sqlite3.h not found; run 'make sqlite3.c sqlite3.h' first" >&2
  exit 1
fi

WORK="$OUT_DIR/work"
rm -rf "$OUT_DIR"
mkdir -p "$WORK"

# Feature flags: keep in step with the release build's amalgamation. zlib and
# libm are part of the Apple platform SDKs, so SQLITE_HAVE_ZLIB stays on and the
# module map links z.
CFLAGS_COMMON=(
  -O2 -fembed-bitcode-marker
  -DNDEBUG
  -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_THREADSAFE=1
  -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION="\"v${VERSION}\""
  -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_DBSTAT_VTAB
  -Wno-comment
)

# compile_slice <label> <sdk> <min-flag> <arch...>
# Compiles one static lib (one object per arch, lipo'd) for a platform slice.
compile_slice() {
  local label="$1" sdk="$2" minflag="$3"; shift 3
  local sysroot; sysroot="$(xcrun --sdk "$sdk" --show-sdk-path)"
  local objs=()
  local arch
  for arch in "$@"; do
    local obj="$WORK/${label}-${arch}.o"
    # Catalyst uses an explicit -target triple rather than -arch + min flag.
    if [ "$label" = "maccatalyst" ]; then
      clang -target "${arch}-apple-ios14.0-macabi" -isysroot "$sysroot" \
        "${CFLAGS_COMMON[@]}" -c sqlite3.c -o "$obj"
    else
      clang -arch "$arch" -isysroot "$sysroot" "$minflag" \
        "${CFLAGS_COMMON[@]}" -c sqlite3.c -o "$obj"
    fi
    objs+=("$obj")
  done
  local lib="$WORK/libdoltlite-${label}.a"
  if [ "${#objs[@]}" -gt 1 ]; then
    local archlibs=()
    for obj in "${objs[@]}"; do
      local al="${obj%.o}.a"; ar rcs "$al" "$obj"; archlibs+=("$al")
    done
    lipo -create "${archlibs[@]}" -output "$lib"
  else
    ar rcs "$lib" "${objs[0]}"
  fi
  echo "$lib"
}

IOS_DEVICE_LIB="$(compile_slice ios        iphoneos        -mios-version-min=14.0           arm64)"
IOS_SIM_LIB="$(compile_slice    iossim     iphonesimulator -mios-simulator-version-min=14.0 arm64 x86_64)"
MACOS_LIB="$(compile_slice      macos      macosx          -mmacosx-version-min=11.0        arm64 x86_64)"
CATALYST_LIB="$(compile_slice   maccatalyst macosx         ''                               arm64 x86_64)"

# Shared headers + module map for every slice. The C module is CDoltlite; it
# exposes the sqlite3_* C API (the dolt_* functions are reached through SQL).
HDRS="$WORK/headers"
mkdir -p "$HDRS"
cp sqlite3.h "$HDRS/doltlite.h"
cat > "$HDRS/module.modulemap" <<'EOF'
module CDoltlite {
    header "doltlite.h"
    link "z"
    export *
}
EOF

XCF="$OUT_DIR/doltlite.xcframework"
xcodebuild -create-xcframework \
  -library "$IOS_DEVICE_LIB" -headers "$HDRS" \
  -library "$IOS_SIM_LIB"    -headers "$HDRS" \
  -library "$MACOS_LIB"      -headers "$HDRS" \
  -library "$CATALYST_LIB"   -headers "$HDRS" \
  -output "$XCF"

ZIP="$OUT_DIR/doltlite-${VERSION}.xcframework.zip"
# Zip the .xcframework directory itself (not its parent path) so the archive
# unpacks to doltlite.xcframework/ at its root, as SwiftPM expects.
( cd "$OUT_DIR" && zip -ryq "doltlite-${VERSION}.xcframework.zip" "doltlite.xcframework" )

# SwiftPM's binaryTarget checksum is the zip's SHA-256.
CHECKSUM="$(shasum -a 256 "$ZIP" | awk '{print $1}')"

echo "xcframework: $XCF"
echo "zip:         $ZIP"
echo "checksum:    $CHECKSUM"
# Machine-readable outputs for CI.
echo "$CHECKSUM" > "$OUT_DIR/checksum.txt"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "checksum=$CHECKSUM" >> "$GITHUB_OUTPUT"
  echo "zip=$ZIP" >> "$GITHUB_OUTPUT"
fi
