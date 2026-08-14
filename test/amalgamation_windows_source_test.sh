#!/usr/bin/env bash
set -euo pipefail

amalgamation="${1:-sqlite3.c}"

if [ ! -f "$amalgamation" ]; then
  echo "FAIL: amalgamation not found: $amalgamation"
  exit 1
fi

if grep -Eq 'Begin file doltlite_remotesrv\.c|doltliteServe' "$amalgamation"; then
  echo "FAIL: remote server implementation present in $amalgamation"
  exit 1
fi

if ! grep -q 'Begin file platform\.c' "$amalgamation"; then
  echo "FAIL: Mbed TLS Windows platform wrappers missing from $amalgamation"
  exit 1
fi

auth_config_count="$(grep -c 'Begin file doltlite_mbedtls_config\.h' "$amalgamation")"
if [ "$auth_config_count" -ne 1 ]; then
  echo "FAIL: expected one authentication configuration; found $auth_config_count"
  exit 1
fi
mbed_config_count="$(grep -c 'Begin file mbedtls_config\.h' "$amalgamation")"
if [ "$mbed_config_count" -ne 1 ]; then
  echo "FAIL: expected one Mbed TLS configuration; found $mbed_config_count"
  exit 1
fi
for header in ed25519.h config_psa.h psa_util.h psa_util_internal.h; do
  header_count="$(grep -c "Begin file $header" "$amalgamation")"
  if [ "$header_count" -ne 1 ]; then
    echo "FAIL: expected one $header section; found $header_count"
    exit 1
  fi
done

winsock_line="$(grep -n -m1 '^# *include <winsock2.h>' "$amalgamation" | cut -d: -f1 || true)"
windows_line="$(grep -n -m1 '^# *include [<\"]windows.h[>\"]' "$amalgamation" | cut -d: -f1 || true)"

if [ -z "$winsock_line" ] || [ -z "$windows_line" ]; then
  echo "FAIL: expected winsock2.h and windows.h includes in $amalgamation"
  exit 1
fi
if [ "$winsock_line" -ge "$windows_line" ]; then
  echo "FAIL: winsock2.h must precede windows.h in $amalgamation"
  exit 1
fi

seh_defs="$(grep -Ec '^(SQLITE_PRIVATE )?int sqlite3PagerWalSystemErrno\(Pager \*pPager\)\{' "$amalgamation")"
if [ "$seh_defs" -ne 2 ]; then
  echo "FAIL: expected the prefixed original and pager-shim SEH definitions; found $seh_defs"
  exit 1
fi

echo "Windows amalgamation source invariants: PASS"
