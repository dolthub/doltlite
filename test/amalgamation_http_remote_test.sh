#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cd "$build_dir"

cc_bin="${CC:-cc}"
if [ ! -f ./sqlite3.c ] || [ ! -f ./sqlite3.h ]; then
  echo "SKIP: sqlite3.c/sqlite3.h not found in $PWD"
  exit 0
fi
tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-local.XXXXXX")"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

if grep -Eq 'Begin file doltlite_(remote|remote_sql|http_remote|remotesrv)\.c|DoltliteRemote|doltliteServe' ./sqlite3.c; then
  echo "FAIL: remote implementation present in amalgamation"
  exit 1
fi

probe_libs=(-lz -lpthread -lm)
case "$(uname -s)" in
  Linux*) probe_libs+=(-ldl) ;;
  MINGW*|MSYS*|CYGWIN*) probe_libs+=(-lws2_32 -lbcrypt -lcrypt32) ;;
esac

probe="${DOLTLITE_AMALG_HTTP_PROBE:-}"
if [ -z "$probe" ]; then
  probe="$tmp/amalg_http_probe"
  "$cc_bin" -Wno-comment -I. ../test/amalgamation_http_probe.c \
    ./sqlite3.c "${probe_libs[@]}" -o "$probe"
fi
if [ ! -x "$probe" ]; then
  echo "FAIL: amalgamation local probe not found at $probe"
  exit 1
fi

"$probe" "$tmp/local.db"
echo "amalgamation excludes remote implementation: PASS"
