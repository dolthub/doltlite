#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cd "$build_dir"

cc_bin="${CC:-cc}"
remotesrv="${REMOTESRV:-./doltlite-remotesrv}"
if [ ! -f ./sqlite3.c ] || [ ! -f ./sqlite3.h ]; then
  echo "SKIP: sqlite3.c/sqlite3.h not found in $PWD"
  exit 0
fi
if [ ! -x "$remotesrv" ]; then
  echo "SKIP: doltlite-remotesrv not found at $remotesrv"
  exit 0
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-http.XXXXXX")"
srv_pid=""
cleanup() {
  # Wait for exit so cached open DB handles are released before rm (Windows
  # returns "Device or resource busy" if the process still holds the file).
  if [ -n "$srv_pid" ]; then
    kill "$srv_pid" 2>/dev/null || true
    wait "$srv_pid" 2>/dev/null || true
    srv_pid=""
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

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
  echo "FAIL: amalgamation HTTP probe not found at $probe"
  exit 1
fi

mkdir -p "$tmp/srv"
"$remotesrv" -p 0 --bind 127.0.0.1 "$tmp/srv" >"$tmp/srv.log" 2>&1 &
srv_pid=$!

port=""
for _ in $(seq 1 50); do
  port="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$tmp/srv.log" | head -1)"
  [ -n "$port" ] && break
  sleep 0.1
done
if [ -z "$port" ]; then
  echo "FAIL: server did not start"
  cat "$tmp/srv.log"
  exit 1
fi

"$probe" "$tmp/src.db" "$tmp/clone.db" "http://127.0.0.1:$port/repo.db"
echo "amalgamation plaintext HTTP remote: PASS"
