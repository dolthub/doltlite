#!/usr/bin/env bash
# install.sh must match Deb/Homebrew: doltlite.h and doltlite_remotesrv.h,
# never sqlite3.h. A lib zip that still contains sqlite3.h is the
# regression — older releases did, and copying it collides with system
# SQLite. Uses DOLTLITE_BASE_URL so this does not hit the network.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_SH="$REPO_ROOT/install.sh"

if [ ! -x "$INSTALL_SH" ] && [ ! -f "$INSTALL_SH" ]; then
  echo "FAIL: $INSTALL_SH not found"
  exit 1
fi

for cmd in curl unzip zip; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "SKIP: $cmd is required to exercise install.sh"
    exit 0
  fi
done

OS="$(uname -s)"
case "$OS" in
  Linux)  PLATFORM="linux" ;;
  Darwin) PLATFORM="osx" ;;
  *) echo "SKIP: install.sh does not support OS '$OS'"; exit 0 ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) PKG_ARCH="x64" ;;
  arm64|aarch64) PKG_ARCH="arm64" ;;
  *) echo "SKIP: install.sh does not support architecture '$ARCH'"; exit 0 ;;
esac
TARGET="${PLATFORM}-${PKG_ARCH}"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-install-sh.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0
ok()  { echo "  PASS: $1"; pass=$((pass+1)); }
bad() { echo "  FAIL: $1"; fail=$((fail+1)); }

VERSION="v0.0.0-test"
VERSION_NUM="${VERSION#v}"
ASSETS="$TMP/assets"
PREFIX="$TMP/prefix"
TOOLS_NAME="doltlite-tools-${TARGET}-${VERSION_NUM}"
LIB_NAME="doltlite-lib-${TARGET}-${VERSION_NUM}"
TOOLS_DIR="$ASSETS/$TOOLS_NAME"
LIB_DIR="$ASSETS/$LIB_NAME"

mkdir -p "$TOOLS_DIR" "$LIB_DIR"
printf '#!/bin/sh\necho dummy\n' >"$TOOLS_DIR/doltlite"
printf '#!/bin/sh\necho dummy\n' >"$TOOLS_DIR/doltlite-remotesrv"
chmod +x "$TOOLS_DIR/doltlite" "$TOOLS_DIR/doltlite-remotesrv"

printf 'SQLITE3_COLLISION\n' >"$LIB_DIR/sqlite3.h"
printf 'DOLTLITE_HEADER\n' >"$LIB_DIR/doltlite.h"
printf 'REMOTESRV_HEADER\n' >"$LIB_DIR/doltlite_remotesrv.h"
printf 'static\n' >"$LIB_DIR/libdoltlite.a"
case "$PLATFORM" in
  linux) printf 'shared\n' >"$LIB_DIR/libdoltlite.so" ;;
  osx)   printf 'shared\n' >"$LIB_DIR/libdoltlite.dylib" ;;
esac

( cd "$ASSETS" && zip -qr "${TOOLS_NAME}.zip" "$TOOLS_NAME" \
  && zip -qr "${LIB_NAME}.zip" "$LIB_NAME" ) || {
  echo "FAIL: could not write fixture zips"
  exit 1
}

echo "=== install.sh layout (${TARGET}) ==="
if ! DOLTLITE_INSTALL_DIR="$PREFIX/bin" \
     DOLTLITE_VERSION="$VERSION" \
     DOLTLITE_BASE_URL="file://${ASSETS}" \
     bash "$INSTALL_SH" >"$TMP/install.log" 2>&1; then
  echo "FAIL: install.sh exited non-zero"
  sed 's/^/    /' "$TMP/install.log" | head -40
  exit 1
fi

have() { if [ -e "$2" ]; then ok "$1"; else bad "$1 (missing $2)"; fi; }
absent() { if [ -e "$2" ]; then bad "$1 (unexpected $2)"; else ok "$1"; fi; }

have "bin/doltlite"                 "$PREFIX/bin/doltlite"
have "bin/doltlite-remotesrv"       "$PREFIX/bin/doltlite-remotesrv"
have "include/doltlite.h"           "$PREFIX/include/doltlite.h"
have "include/doltlite_remotesrv.h" "$PREFIX/include/doltlite_remotesrv.h"
have "lib/libdoltlite.a"            "$PREFIX/lib/libdoltlite.a"
case "$PLATFORM" in
  linux) have "lib/libdoltlite.so"    "$PREFIX/lib/libdoltlite.so" ;;
  osx)   have "lib/libdoltlite.dylib" "$PREFIX/lib/libdoltlite.dylib" ;;
esac
absent "include/sqlite3.h is not installed" "$PREFIX/include/sqlite3.h"

if [ -f "$PREFIX/include/doltlite.h" ]; then
  got="$(cat "$PREFIX/include/doltlite.h")"
  if [ "$got" = "DOLTLITE_HEADER" ]; then
    ok "include/doltlite.h is the DoltLite header, not sqlite3.h"
  else
    bad "include/doltlite.h is the DoltLite header, not sqlite3.h (got: $got)"
  fi
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
