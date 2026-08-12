#!/usr/bin/env bash
set -euo pipefail

amalgamation="${1:-sqlite3.c}"

if [ ! -f "$amalgamation" ]; then
  echo "FAIL: amalgamation not found: $amalgamation"
  exit 1
fi

if grep -Eq '^# *include <(winsock2|ws2tcpip)\.h>' "$amalgamation"; then
  echo "FAIL: remote networking headers present in $amalgamation"
  exit 1
fi

seh_defs="$(grep -Ec '^(SQLITE_PRIVATE )?int sqlite3PagerWalSystemErrno\(Pager \*pPager\)\{' "$amalgamation")"
if [ "$seh_defs" -ne 2 ]; then
  echo "FAIL: expected the prefixed original and pager-shim SEH definitions; found $seh_defs"
  exit 1
fi

echo "Windows amalgamation source invariants: PASS"
