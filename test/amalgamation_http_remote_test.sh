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
  if [ -n "$srv_pid" ]; then
    kill "$srv_pid" 2>/dev/null || true
    wait "$srv_pid" 2>/dev/null || true
    srv_pid=""
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

for invalid_setting in 2 "" "1 0"; do
  if make -n "DOLTLITE_ENABLE_REMOTES=$invalid_setting" doltlite >"$tmp/invalid-option.log" 2>&1; then
    echo "FAIL: invalid DOLTLITE_ENABLE_REMOTES value accepted: '$invalid_setting'"
    exit 1
  fi
  if ! grep -Fq "DOLTLITE_ENABLE_REMOTES must be 0 or 1" "$tmp/invalid-option.log"; then
    echo "FAIL: invalid DOLTLITE_ENABLE_REMOTES value did not produce a clear error: '$invalid_setting'"
    cat "$tmp/invalid-option.log"
    exit 1
  fi
done

for invalid_setting in 2 "" "1 0"; do
  if make -n "DOLTLITE_ENABLE_CHUNK_SOURCE=$invalid_setting" doltlite >"$tmp/invalid-chunk-source-option.log" 2>&1; then
    echo "FAIL: invalid DOLTLITE_ENABLE_CHUNK_SOURCE value accepted: '$invalid_setting'"
    exit 1
  fi
  if ! grep -Fq "DOLTLITE_ENABLE_CHUNK_SOURCE must be 0 or 1" "$tmp/invalid-chunk-source-option.log"; then
    echo "FAIL: invalid DOLTLITE_ENABLE_CHUNK_SOURCE value did not produce a clear error: '$invalid_setting'"
    cat "$tmp/invalid-chunk-source-option.log"
    exit 1
  fi
done

if grep -Eq 'Begin file doltlite_remotesrv\.c|doltliteServe' ./sqlite3.c; then
  echo "FAIL: remote server implementation present in amalgamation"
  exit 1
fi
for source in doltlite_remote.c doltlite_remote_sql.c doltlite_http_remote.c \
              doltlite_creds.c doltlite_tls.c; do
  if ! grep -q "Begin file $source" ./sqlite3.c; then
    echo "FAIL: remote client source missing from amalgamation: $source"
    exit 1
  fi
done
if ! grep -q "Begin file doltlite_chunk_source.c" ./sqlite3.c; then
  echo "FAIL: chunk source implementation missing from amalgamation"
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

DOLTLITE_CREDS_DIR="$tmp/creds" \
  "$probe" "$tmp/src.db" "$tmp/clone.db" "http://127.0.0.1:$port/repo.db"

offline_probe="$tmp/amalg_remotes_disabled_probe"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) offline_probe="${offline_probe}.exe" ;;
esac
"$cc_bin" -Wno-comment -DDOLTLITE_ENABLE_REMOTES=0 -I. \
  ../test/amalgamation_remotes_disabled_probe.c ./sqlite3.c \
  "${probe_libs[@]}" -o "$offline_probe"
"$offline_probe"

chunk_source_probe="$tmp/amalg_chunk_source_probe"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) chunk_source_probe="${chunk_source_probe}.exe" ;;
esac
"$cc_bin" -Wno-comment -I. ../test/amalgamation_chunk_source_probe.c \
  ./sqlite3.c "${probe_libs[@]}" -o "$chunk_source_probe"
"$chunk_source_probe"

chunk_source_off_probe="$tmp/amalg_chunk_source_off_probe"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) chunk_source_off_probe="${chunk_source_off_probe}.exe" ;;
esac
"$cc_bin" -Wno-comment -DDOLTLITE_ENABLE_CHUNK_SOURCE=0 -I. \
  ../test/amalgamation_chunk_source_probe.c ./sqlite3.c \
  "${probe_libs[@]}" -o "$chunk_source_off_probe"
"$chunk_source_off_probe"

if nm "$offline_probe" | grep -Eq \
    'doltlite(HttpRemoteOpen|CredsGenerate|TlsClientNew)|mbedtls_ssl_tls13'; then
  echo "FAIL: networking implementation present in remotes-disabled amalgamation"
  exit 1
fi

echo "amalgamation remote, chunk source, credentials, and offline options: PASS"
