#!/usr/bin/env bash
set -euo pipefail

amalgamation="${1:-sqlite3.c}"

if [ ! -f "$amalgamation" ]; then
  echo "FAIL: amalgamation not found: $amalgamation"
  exit 1
fi

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
