#!/usr/bin/env bash
set -euo pipefail

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 not available"
  exit 0
fi
if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-http-proto.XXXXXX")"
SRV_PID=""
PROXY_PID=""
cleanup() {
  if [ -n "$PROXY_PID" ]; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
  if [ -n "$SRV_PID" ]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0
fail=0

check() {
  local desc="$1" expected="${2//$'\r'/}" actual="${3//$'\r'/}"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$expected|"
    echo "    actual:   |$actual|"
    fail=$((fail+1))
  fi
}

check_ne() {
  local desc="$1" disallowed="${2//$'\r'/}" actual="${3//$'\r'/}"
  if [ "$disallowed" != "$actual" ]; then
    echo "  PASS: $desc"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    disallowed: |$disallowed|"
    echo "    actual:     |$actual|"
    fail=$((fail+1))
  fi
}

set_mode() {
  printf '%s\n' "$1" >"$TMP/proxy-mode"
}

query() {
  "$DOLTLITE" "$1" "$2" 2>&1
}

mkdir -p "$TMP/srv"
"$REMOTESRV" -p 0 --bind 127.0.0.1 "$TMP/srv" >"$TMP/srv.log" 2>&1 &
SRV_PID=$!

SRV_PORT=""
for _ in $(seq 1 50); do
  SRV_PORT="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/srv.log" | head -1)"
  [ -n "$SRV_PORT" ] && break
  sleep 0.1
done
if [ -z "$SRV_PORT" ]; then
  echo "FAIL: server did not start"
  cat "$TMP/srv.log"
  exit 1
fi

cat >"$TMP/proxy.py" <<'PY'
import os
import socket
import socketserver
import sys

upstream_port = int(sys.argv[1])
mode_file = sys.argv[2]
log_file = sys.argv[3]

def response(status, reason, body=b""):
    return (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii") + body

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.request.recv(1)
            if not chunk:
                return
            data += chunk
            if len(data) > 65536:
                return
        head, rest = data.split(b"\r\n\r\n", 1)
        lines = head.decode("iso-8859-1").split("\r\n")
        parts = lines[0].split(" ", 2)
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        with open(log_file, "a") as f:
            f.write(f"{method} {path} {headers.get('content-length','')}\n")

        if method in ("POST", "PUT") and "content-length" not in headers:
            self.request.sendall(response(411, "Length Required", b"Length Required"))
            return

        nbody = int(headers.get("content-length", "0"))
        body = rest
        while len(body) < nbody:
            chunk = self.request.recv(nbody - len(body))
            if not chunk:
                return
            body += chunk

        mode = ""
        try:
            with open(mode_file) as f:
                mode = f.read().strip()
        except FileNotFoundError:
            pass

        if mode == "bad_refs" and method == "GET" and path.endswith("/refs"):
            self.request.sendall(response(200, "OK", b"not-serialized-refs"))
            return
        if mode == "commit_500" and method == "POST" and path.endswith("/commit"):
            self.request.sendall(response(500, "Internal Server Error", b"commit failed"))
            return
        if mode == "refs_if_409" and method == "PUT" and path.endswith("/refs-if"):
            self.request.sendall(response(409, "Conflict", b"Conflict"))
            return
        if mode == "get_chunks_truncated" and method == "POST" and path.endswith("/get-chunks"):
            self.request.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n\x00\x00"
            )
            return

        with socket.create_connection(("127.0.0.1", upstream_port), timeout=5) as s:
            s.sendall(head + b"\r\n\r\n" + body[:nbody])
            s.shutdown(socket.SHUT_WR)
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                self.request.sendall(chunk)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

with Server(("127.0.0.1", 0), Handler) as srv:
    print(srv.server_address[1], flush=True)
    srv.serve_forever()
PY

: >"$TMP/proxy-requests.log"
set_mode "pass"
python3 "$TMP/proxy.py" "$SRV_PORT" "$TMP/proxy-mode" "$TMP/proxy-requests.log" \
  >"$TMP/proxy.log" 2>"$TMP/proxy.err" &
PROXY_PID=$!

PROXY_PORT=""
for _ in $(seq 1 50); do
  PROXY_PORT="$(head -1 "$TMP/proxy.log" 2>/dev/null || true)"
  [ -n "$PROXY_PORT" ] && break
  sleep 0.1
done
if [ -z "$PROXY_PORT" ]; then
  echo "FAIL: proxy did not start"
  cat "$TMP/proxy.err" 2>/dev/null || true
  exit 1
fi

URL="http://127.0.0.1:$PROXY_PORT/repo.db"

echo "=== seed remote ==="
"$DOLTLITE" "$TMP/src.db" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_remote('add','origin','$URL');
SQL
check "initial push succeeds" "0" "$(query "$TMP/src.db" "SELECT dolt_push('origin','main');")"

base_head="$(query "$TMP/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")"

echo "=== clone rejects malformed refs ==="
set_mode "bad_refs"
check_ne "clone with malformed refs fails" "0" \
  "$(query "$TMP/bad_refs_clone.db" "SELECT dolt_clone('$URL');")"
check "failed clone leaves no user tables" "0" \
  "$(query "$TMP/bad_refs_clone.db" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t';")"

echo "=== failed /commit preserves local durable state ==="
set_mode "commit_500"
"$DOLTLITE" "$TMP/src.db" <<'SQL' >/dev/null
INSERT INTO t VALUES(2,'local');
SELECT dolt_commit('-A','-m','local');
SQL
local_head="$(query "$TMP/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")"
check_ne "local head advanced before failed push" "$base_head" "$local_head"
check_ne "push fails when /commit returns 500" "0" \
  "$(query "$TMP/src.db" "SELECT dolt_push('origin','main');")"
check "failed push keeps local head" "$local_head" \
  "$(query "$TMP/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")"
check "failed push keeps local rows" "2" \
  "$(query "$TMP/src.db" "SELECT count(*) FROM t;")"

echo "=== refs-if conflict preserves local durable state ==="
set_mode "refs_if_409"
"$DOLTLITE" "$TMP/src.db" <<'SQL' >/dev/null
INSERT INTO t VALUES(3,'conflict-local');
SELECT dolt_commit('-A','-m','conflict-local');
SQL
conflict_head="$(query "$TMP/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")"
check_ne "push fails when /refs-if returns 409" "0" \
  "$(query "$TMP/src.db" "SELECT dolt_push('origin','main');")"
check "refs-if conflict keeps local head" "$conflict_head" \
  "$(query "$TMP/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")"
check "refs-if conflict keeps local rows" "3" \
  "$(query "$TMP/src.db" "SELECT count(*) FROM t;")"

echo "=== truncated /get-chunks fails fetch without advancing tracking ref ==="
set_mode "pass"
check "clone for fetch test succeeds" "0" \
  "$(query "$TMP/fetch_client.db" "SELECT dolt_clone('$URL');")"
fetch_tracking_before="$(query "$TMP/fetch_client.db" \
  "SELECT hash FROM dolt_branches WHERE name='origin/main';")"
fetch_rows_before="$(query "$TMP/fetch_client.db" "SELECT count(*) FROM t;")"

"$DOLTLITE" "$TMP/advancer.db" <<SQL >/dev/null
SELECT dolt_clone('$URL');
INSERT INTO t VALUES(20,'remote');
SELECT dolt_commit('-A','-m','remote');
SELECT dolt_push('origin','main');
SQL

set_mode "get_chunks_truncated"
check_ne "fetch fails on truncated /get-chunks body" "0" \
  "$(query "$TMP/fetch_client.db" "SELECT dolt_fetch('origin','main');")"
check "failed fetch keeps tracking ref" "$fetch_tracking_before" \
  "$(query "$TMP/fetch_client.db" "SELECT hash FROM dolt_branches WHERE name='origin/main';")"
check "failed fetch keeps working rows" "$fetch_rows_before" \
  "$(query "$TMP/fetch_client.db" "SELECT count(*) FROM t;")"

if grep -Eq '^(POST|PUT) .* $' "$TMP/proxy-requests.log"; then
  echo "  FAIL: observed POST/PUT without Content-Length"
  grep -E '^(POST|PUT) .* $' "$TMP/proxy-requests.log" | sed 's/^/    /'
  fail=$((fail+1))
else
  echo "  PASS: every POST/PUT carried Content-Length"
  pass=$((pass+1))
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ]
