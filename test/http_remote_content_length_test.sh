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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-http-cl.XXXXXX")"
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
marker = sys.argv[2]

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
        if method == "POST" and "content-length" not in headers:
            self.request.sendall(
                b"HTTP/1.1 411 Length Required\r\n"
                b"Content-Length: 15\r\nConnection: close\r\n\r\n"
                b"Length Required"
            )
            return
        nbody = int(headers.get("content-length", "0"))
        body = rest
        while len(body) < nbody:
            chunk = self.request.recv(nbody - len(body))
            if not chunk:
                return
            body += chunk
        if method == "POST" and path.endswith("/commit") and headers.get("content-length") == "0":
            with open(marker, "w") as f:
                f.write("ok\n")
        with socket.create_connection(("127.0.0.1", upstream_port), timeout=5) as s:
            s.sendall(head + b"\r\n\r\n" + body)
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

python3 "$TMP/proxy.py" "$SRV_PORT" "$TMP/commit-content-length-ok" >"$TMP/proxy.log" 2>"$TMP/proxy.err" &
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
"$DOLTLITE" "$TMP/src.db" <<SQL >/dev/null
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES(1,'alice'),(2,'bob'),(3,'charlie');
SELECT dolt_commit('-A','-m','initial');
SELECT dolt_remote('add','origin','$URL');
SQL

result="$("$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('origin','main');" 2>&1)"
if [ "$result" != "0" ]; then
  echo "FAIL: expected push to return 0, got: $result"
  cat "$TMP/proxy.err" 2>/dev/null || true
  exit 1
fi
if [ ! -f "$TMP/commit-content-length-ok" ]; then
  echo "FAIL: proxy did not observe Content-Length: 0 on POST /commit"
  exit 1
fi

echo "http remote empty POST Content-Length: PASS"
