#!/bin/bash
set -u
set -o pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
. "$(dirname "$0")/lib/openssl_test_cert.sh"
CC="${CC:-cc}"
DOLTLITE_EXTRA_LIBS=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DOLTLITE_EXTRA_LIBS="-lws2_32 -lbcrypt -lcrypt32";; esac
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

FAKE=""
if command -v openssl >/dev/null 2>&1; then
  if openssl_test_cert /dev/null "$TMP/fake_ca.pem" "/CN=doltlite-fake-ca" \
       >/dev/null 2>&1; then
    FAKE="$TMP/fake_ca.pem"
  fi
fi

echo "=== doltlite TLS test ==="
BIN="${DOLTLITE_TLS_TEST_BIN:-$TMP/tls_test}"
if [ -z "${DOLTLITE_TLS_TEST_BIN:-}" ]; then
  "$CC" -O2 -I "$HERE/src" -I "$HERE/ext/mbedtls/include" \
    "$HERE/test/doltlite_tls_test_main.c" \
    "$HERE/test/sqlite3_alloc_stub.c" \
    "$HERE/src/doltlite_tls.c" \
    "$HERE"/ext/mbedtls/library/*.c \
    $DOLTLITE_EXTRA_LIBS \
    -o "$BIN" 2>"$TMP/build.err" || {
    echo "  build failed:"
    cat "$TMP/build.err"
    exit 1
  }
fi
if [ ! -x "$BIN" ]; then
  echo "TLS test binary not found at $BIN"
  exit 1
fi

"$BIN" ${FAKE:+"$FAKE"}
