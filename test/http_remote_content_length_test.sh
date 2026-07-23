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

if "$REMOTESRV" -p 12x "$TMP/srv" >"$TMP/bad-cli.out" 2>&1 \
 || ! grep -q "invalid port" "$TMP/bad-cli.out"; then
  echo "FAIL: server accepted a malformed CLI port"
  exit 1
fi
if "$REMOTESRV" --timeout-ms 999999999999 "$TMP/srv" \
    >"$TMP/bad-timeout.out" 2>&1 \
 || ! grep -q "invalid timeout" "$TMP/bad-timeout.out"; then
  echo "FAIL: server accepted an overflowing CLI timeout"
  exit 1
fi
echo "remote server checked CLI numbers: PASS"

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
mode_path = sys.argv[3]

def response_mode():
    try:
        with open(mode_path, "r") as f:
            return f.read().strip()
    except OSError:
        return ""

def alter_response(response, mode):
    head, sep, body = response.partition(b"\r\n\r\n")
    if not sep:
        return response
    lines = head.split(b"\r\n")
    length_index = next(
        (i for i, line in enumerate(lines)
         if line.lower().startswith(b"content-length:")),
        None,
    )
    if length_index is None:
        return response
    length = len(body)
    if mode == "invalid":
        lines[length_index] = f"Content-Length: {length}x".encode()
    elif mode == "duplicate":
        lines.insert(length_index + 1, f"Content-Length: {length}".encode())
    elif mode == "overflow":
        lines[length_index] = b"Content-Length: 999999999999999999999999"
    elif mode == "truncated":
        lines[length_index] = f"Content-Length: {length + 1}".encode()
    elif mode == "transfer":
        lines.append(b"Transfer-Encoding: chunked")
    return b"\r\n".join(lines) + sep + body

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
            response = b""
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                response += chunk
        mode = response_mode()
        if mode and path.endswith("/refs"):
            response = alter_response(response, mode)
        self.request.sendall(response)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

with Server(("127.0.0.1", 0), Handler) as srv:
    print(srv.server_address[1], flush=True)
    srv.serve_forever()
PY

: >"$TMP/response-mode"
python3 "$TMP/proxy.py" "$SRV_PORT" "$TMP/commit-content-length-ok" \
  "$TMP/response-mode" >"$TMP/proxy.log" 2>"$TMP/proxy.err" &
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

bad_url="http://127.0.0.1:${PROXY_PORT}junk/repo.db"
if "$DOLTLITE" "$TMP/bad-port.db" "SELECT dolt_clone('$bad_url');" \
    >"$TMP/bad-port.out" 2>&1; then
  echo "FAIL: URL port with trailing characters was accepted"
  exit 1
fi
echo "http remote checked URL port: PASS"

for mode in invalid duplicate overflow truncated transfer; do
  printf '%s\n' "$mode" >"$TMP/response-mode"
  if "$DOLTLITE" "$TMP/bad-response-$mode.db" \
      "SELECT dolt_clone('$URL');" >"$TMP/bad-response-$mode.out" 2>&1; then
    echo "FAIL: malformed $mode response framing was accepted"
    exit 1
  fi
done
: >"$TMP/response-mode"
echo "http remote strict response framing: PASS"

echo "http remote empty POST Content-Length: PASS"
