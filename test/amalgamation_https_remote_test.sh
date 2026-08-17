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
if ! command -v openssl >/dev/null 2>&1; then
  echo "SKIP: openssl not available"
  exit 0
fi

for source in ssl_tls13_client.c ssl_tls13_generic.c ssl_tls13_keys.c; do
  if ! grep -q "Begin file $source" ./sqlite3.c; then
    echo "FAIL: TLS 1.3 client source missing from amalgamation: $source"
    exit 1
  fi
done

tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-https.XXXXXX")"
srv_pid=""
cleanup() {
  if [ -n "$srv_pid" ]; then
    kill "$srv_pid" 2>/dev/null || true
    wait "$srv_pid" 2>/dev/null || true
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
  probe="$tmp/amalg_https_probe"
  "$cc_bin" -Wno-comment -I. ../test/amalgamation_http_probe.c \
    ./sqlite3.c "${probe_libs[@]}" -o "$probe"
fi
if [ ! -x "$probe" ]; then
  echo "FAIL: amalgamation HTTPS probe not found at $probe"
  exit 1
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$tmp/key.pem" -out "$tmp/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1

mkdir -p "$tmp/srv"
"$remotesrv" --cert "$tmp/cert.pem" --key "$tmp/key.pem" \
  -p 0 --bind 127.0.0.1 "$tmp/srv" >"$tmp/srv.log" 2>&1 &
srv_pid=$!

port=""
for _ in $(seq 1 50); do
  port="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$tmp/srv.log" | head -1)"
  [ -n "$port" ] && break
  sleep 0.1
done
if [ -z "$port" ]; then
  echo "FAIL: HTTPS server did not start"
  cat "$tmp/srv.log"
  exit 1
fi

DOLTLITE_CA_FILE="$tmp/cert.pem" DOLTLITE_CREDS_DIR="$tmp/creds" \
  "$probe" "$tmp/src.db" "$tmp/clone.db" \
  "https://localhost:$port/repo.db"

echo "amalgamation TLS 1.3 HTTPS remote: PASS"
