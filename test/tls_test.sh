#!/bin/bash
#
# Live test for doltlite_tls: compiles the TLS module against the vendored
# mbedTLS with a plain C compiler and runs an HTTPS GET (+ a wrong-CA negative
# check). Network-dependent; the program SKIPs when the host is unreachable.
#
set -u
set -o pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
CC="${CC:-cc}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Best-effort self-signed CA for the negative test; skipped if openssl absent.
FAKE=""
if command -v openssl >/dev/null 2>&1; then
  if openssl req -x509 -newkey rsa:2048 -keyout /dev/null \
       -out "$TMP/fake_ca.pem" -days 1 -nodes \
       -subj "/CN=doltlite-fake-ca" >/dev/null 2>&1; then
    FAKE="$TMP/fake_ca.pem"
  fi
fi

echo "=== doltlite TLS test ==="
"$CC" -O2 -I "$HERE/src" -I "$HERE/ext/mbedtls/include" \
  "$HERE/test/doltlite_tls_test_main.c" \
  "$HERE/src/doltlite_tls.c" \
  "$HERE"/ext/mbedtls/library/*.c \
  -o "$TMP/tls_test" 2>"$TMP/build.err" || {
  echo "  build failed:"
  cat "$TMP/build.err"
  exit 1
}

"$TMP/tls_test" ${FAKE:+"$FAKE"}
