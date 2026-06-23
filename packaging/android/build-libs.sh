#!/bin/bash
#
# Cross-compile libdoltlite for the Android ABIs using the NDK and lay the
# results out as jniLibs/<abi>/libdoltlite.so for the Gradle library to bundle
# into its AAR. Each ABI compiles sqlite3.c (the doltlite-woven amalgamation, so
# the dolt_* functions are built in and auto-registered) as a single shared
# object.
#
# Requires the Android NDK (ANDROID_NDK_HOME or the first arg) and a built
# amalgamation (`make sqlite3.c sqlite3.h`). Run from the repo root.
#
# Usage: build-libs.sh <version> [ndk_dir] [min_api]
#   version  release version without the leading "v" (informational)
#   ndk_dir  NDK path (default: $ANDROID_NDK_HOME)
#   min_api  minimum Android API level (default: 21)

set -euo pipefail

VERSION="${1:?usage: build-libs.sh <version> [ndk_dir] [min_api]}"
NDK="${2:-${ANDROID_NDK_HOME:-}}"
MIN_API="${3:-21}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [ -z "$NDK" ] || [ ! -d "$NDK" ]; then
  echo "ERROR: Android NDK not found (set ANDROID_NDK_HOME or pass it as arg 2)" >&2
  exit 1
fi
if [ ! -f sqlite3.c ] || [ ! -f sqlite3.h ]; then
  echo "ERROR: sqlite3.c/sqlite3.h not found; run 'make sqlite3.c sqlite3.h' first" >&2
  exit 1
fi

# NDK host tag (CI is linux-x86_64; allow darwin for local runs).
case "$(uname -s)" in
  Darwin) HOST_TAG="darwin-x86_64" ;;
  *)      HOST_TAG="linux-x86_64" ;;
esac
TOOLCHAIN="$NDK/toolchains/llvm/prebuilt/$HOST_TAG"
if [ ! -d "$TOOLCHAIN" ]; then
  echo "ERROR: NDK toolchain not found at $TOOLCHAIN" >&2
  exit 1
fi

OUT="packaging/android/src/main/jniLibs"
rm -rf "$OUT"

CFLAGS_COMMON=(
  -O2 -fPIC -DNDEBUG
  -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_THREADSAFE=1
  -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION="\"v${VERSION}\""
  -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_DBSTAT_VTAB
  -Wno-comment
)

# abi : clang target triple prefix (the NDK clang wrapper is <triple><api>-clang)
ABIS=(
  "arm64-v8a:aarch64-linux-android"
  "armeabi-v7a:armv7a-linux-androideabi"
  "x86_64:x86_64-linux-android"
  "x86:i686-linux-android"
)

for entry in "${ABIS[@]}"; do
  IFS=: read -r abi triple <<< "$entry"
  cc="$TOOLCHAIN/bin/${triple}${MIN_API}-clang"
  if [ ! -x "$cc" ]; then
    echo "ERROR: compiler not found for $abi: $cc" >&2
    exit 1
  fi
  mkdir -p "$OUT/$abi"
  # zlib (libz) ships in the NDK sysroot; SQLITE_HAVE_ZLIB stays on.
  "$cc" "${CFLAGS_COMMON[@]}" -DSQLITE_HAVE_ZLIB=1 \
    -shared sqlite3.c -lz -o "$OUT/$abi/libdoltlite.so"
  echo "built $abi -> $OUT/$abi/libdoltlite.so"
done

echo "jniLibs:"
find "$OUT" -name '*.so' | sort
