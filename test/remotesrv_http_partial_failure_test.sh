#!/usr/bin/env bash
set -uo pipefail

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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-http-partial.XXXXXX")"
SRV_PID=""
PROXY_PID=""
cleanup() {
  [ -n "$PROXY_PID" ] && { kill "$PROXY_PID" 2>/dev/null || true; wait "$PROXY_PID" 2>/dev/null || true; }
  [ -n "$SRV_PID" ] && { kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; }
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0
fail=0
check() {
  local desc="$1" expected="${2//$'\r'/}" actual="${3//$'\r'/}"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$(echo "$expected" | head -5)|"
    echo "    actual:   |$(echo "$actual" | head -5)|"
    fail=$((fail+1))
  fi
}
check_match() {
  local desc="$1" pattern="$2" actual="${3//$'\r'/}"
  if echo "$actual" | grep -qE "$pattern"; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    pattern: |$pattern|"
    echo "    actual:  |$(echo "$actual" | head -5)|"
    fail=$((fail+1))
  fi
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
  echo "FAIL: server did not start"; cat "$TMP/srv.log"; exit 1
fi

cat >"$TMP/proxy.py" <<'PY'
import socket
import socketserver
import sys
import threading

upstream_port = int(sys.argv[1])
fail_path_file = sys.argv[2]
forwarded_path = sys.argv[3]
failure_lock = threading.Lock()
failure_control = ""
failure_count = 0

def current_fail_path():
    try:
        with open(fail_path_file, "r") as f:
            return f.read().strip()
    except OSError:
        return ""

def should_fail(path):
    global failure_control, failure_count
    control = current_fail_path()
    with failure_lock:
        if control != failure_control:
            failure_control = control
            failure_count = 0
        if not control:
            return False
        target, separator, ordinal = control.rpartition("|")
        if not separator or not ordinal.isdigit():
            return path.endswith(control)
        if not path.endswith(target):
            return False
        failure_count += 1
        return failure_count == int(ordinal)

def record_forward(method, path):
    with failure_lock:
        with open(forwarded_path, "a") as f:
            f.write(f"{method} {path}\n")

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.request.recv(4096)
            if not chunk:
                return
            data += chunk
            if len(data) > 1 << 20:
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
        nbody = int(headers.get("content-length", "0"))
        body = rest
        while len(body) < nbody:
            chunk = self.request.recv(min(65536, nbody - len(body)))
            if not chunk:
                return
            body += chunk

        if should_fail(path):
            self.request.sendall(
                b"HTTP/1.1 503 Service Unavailable\r\n"
                b"Content-Length: 0\r\nConnection: close\r\n\r\n"
            )
            return

        record_forward(method, path)
        with socket.create_connection(("127.0.0.1", upstream_port), timeout=60) as s:
            s.sendall(head + b"\r\n\r\n" + body)
            s.shutdown(socket.SHUT_WR)
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                self.request.sendall(chunk)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

with Server(("127.0.0.1", 0), Handler) as srv:
    print(srv.server_address[1], flush=True)
    srv.serve_forever()
PY

: >"$TMP/fail_path"
: >"$TMP/forwarded.log"
python3 "$TMP/proxy.py" "$SRV_PORT" "$TMP/fail_path" "$TMP/forwarded.log" >"$TMP/proxy.log" 2>"$TMP/proxy.err" &
PROXY_PID=$!
PROXY_PORT=""
for _ in $(seq 1 50); do
  PROXY_PORT="$(head -1 "$TMP/proxy.log" 2>/dev/null || true)"
  [ -n "$PROXY_PORT" ] && break
  sleep 0.1
done
if [ -z "$PROXY_PORT" ]; then
  echo "FAIL: proxy did not start"; cat "$TMP/proxy.err" 2>/dev/null || true; exit 1
fi

DB="$DOLTLITE"
URL="http://127.0.0.1:$PROXY_PORT/repo.db"
A="$TMP/a.db"

echo "=== seed remote ==="
"$DB" "$A" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, payload BLOB);
INSERT INTO t VALUES(1,'base',randomblob(512));
SELECT dolt_commit('-A','-m','base');
SELECT dolt_remote('add','origin','$URL');
SQL
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "initial push succeeds" "0" "$result"
base_head=$("$DB" "$A" "SELECT commit_hash FROM dolt_log LIMIT 1;")

echo "=== fail after chunk upload, before refs update ==="
"$DB" "$A" <<'SQL' >/dev/null
INSERT INTO t VALUES(2,'refs-if-fails',randomblob(1048576));
SELECT dolt_commit('-A','-m','refs-if-fails');
SQL
printf '/refs-if\n' >"$TMP/fail_path"
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "push reports refs-if failure" "ERROR|Error|error|failed" "$result"
printf '\n' >"$TMP/fail_path"
result=$("$DB" "$TMP/clone_after_refs_if_fail.db" "SELECT dolt_clone('$URL'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT count(*) FROM t;" 2>&1)
check "remote head unchanged after refs-if failure" "0
$base_head
1" "$result"
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "retry after refs-if failure succeeds" "0" "$result"
retry_head=$("$DB" "$A" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMP/clone_after_refs_if_retry.db" "SELECT dolt_clone('$URL'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT count(*) FROM t;" 2>&1)
check "remote publishes retried refs-if push" "0
$retry_head
2" "$result"

echo "=== fail after refs update, before commit ==="
"$DB" "$A" <<'SQL' >/dev/null
INSERT INTO t VALUES(3,'commit-fails',randomblob(1048576));
SELECT dolt_commit('-A','-m','commit-fails');
SQL
printf '/commit\n' >"$TMP/fail_path"
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "push reports commit failure" "ERROR|Error|error|failed" "$result"
printf '\n' >"$TMP/fail_path"
commit_fail_head=$("$DB" "$A" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMP/clone_after_commit_fail.db" "SELECT dolt_clone('$URL'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT count(*) FROM t;" 2>&1)
check "remote remains readable with refs published after commit failure" "0
$commit_fail_head
3" "$result"
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "retry after commit failure succeeds" "0" "$result"
result=$("$DB" "$TMP/clone_after_commit_retry.db" "SELECT dolt_clone('$URL'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT count(*) FROM t;" 2>&1)
check "remote publishes retried commit push" "0
$commit_fail_head
3" "$result"

echo "=== push more than 128 MiB with a failed second chunk batch ==="
"$DB" "$A" <<'SQL' >/dev/null
CREATE TABLE large_payload(id INTEGER PRIMARY KEY, payload BLOB);
WITH RECURSIVE seq(i) AS (
  VALUES(1) UNION ALL SELECT i+1 FROM seq WHERE i<8704
) INSERT INTO large_payload SELECT i, randomblob(16384) FROM seq;
SELECT dolt_commit('-A','-m','large payload');
SQL
before=$(grep -c 'POST .*/chunks' "$TMP/forwarded.log" || true)
printf '/chunks|2\n' >"$TMP/fail_path"
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "push reports second chunk batch failure" "ERROR|Error|error|failed" "$result"
after=$(grep -c 'POST .*/chunks' "$TMP/forwarded.log" || true)
check "first chunk batch was acknowledged before failure" "1" "$((after-before))"
printf '\n' >"$TMP/fail_path"
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "retry after partial chunk upload succeeds" "0" "$result"
after_retry=$(grep -c 'POST .*/chunks' "$TMP/forwarded.log" || true)
check_match "retry uses multiple bounded chunk batches" "^[4-9]$|^[1-9][0-9]+$" "$((after_retry-after))"
result=$("$DB" "$TMP/clone_large.db" "SELECT dolt_clone('$URL'); SELECT count(*),sum(length(payload)) FROM large_payload;" 2>&1)
check "clone reads the greater-than-128-MiB push" "0
8704|142606336" "$result"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
