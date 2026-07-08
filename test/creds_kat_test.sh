#!/bin/bash
set -u
set -o pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
CC="${CC:-cc}"
DOLTLITE_EXTRA_LIBS=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DOLTLITE_EXTRA_LIBS="-lws2_32 -lbcrypt -lcrypt32";; esac
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

BIN="$TMP/creds_kat"

echo "=== doltlite creds KAT ==="
"$CC" -O2 -Wall \
  -I "$HERE/src" -I "$HERE/ext/ed25519" \
  "$HERE/test/doltlite_creds_kat.c" \
  "$HERE/src/doltlite_creds.c" \
  "$HERE/ext/ed25519/fe.c" \
  "$HERE/ext/ed25519/ge.c" \
  "$HERE/ext/ed25519/sc.c" \
  "$HERE/ext/ed25519/sha512.c" \
  "$HERE/ext/ed25519/keypair.c" \
  "$HERE/ext/ed25519/sign.c" \
  "$HERE/ext/ed25519/verify.c" \
  "$HERE/ext/ed25519/add_scalar.c" \
  $DOLTLITE_EXTRA_LIBS \
  -o "$BIN" 2>"$TMP/build.err" || {
  echo "  build failed:"
  cat "$TMP/build.err"
  exit 1
}

DOLTLITE_CREDS_DIR="$TMP/creds" "$BIN" "$TMP/creds"
