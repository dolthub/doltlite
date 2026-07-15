#!/usr/bin/env bash
# Full remote lifecycle over plain HTTP against doltlite-remotesrv, with a
# strict validating proxy in the middle. remotesrv is lenient where real HTTP
# infrastructure is strict — a missing Content-Length passed every
# remotesrv-only test but drew 411s from production proxies — so every
# request in this suite flows through a validator that records a violation
# for malformed framing, unknown endpoints, or 5xx responses, and the suite
# fails if a single one is recorded.
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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-remotesrv-http.XXXXXX")"
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

start_server() {
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
}

restart_server_same_port() {
  "$REMOTESRV" -p "$SRV_PORT" --bind 127.0.0.1 "$TMP/srv" >"$TMP/srv2.log" 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 50); do
    grep -q "$SRV_PORT" "$TMP/srv2.log" 2>/dev/null && break
    sleep 0.1
  done
}

mkdir -p "$TMP/srv"
start_server

cat >"$TMP/proxy.py" <<'PY'
import re
import socket
import socketserver
import sys

upstream_port = int(sys.argv[1])
violations_path = sys.argv[2]
counts_path = sys.argv[3]

GET_RE = re.compile(r"^/[A-Za-z0-9._-]+\.db/(root|refs|chunk/[0-9a-fA-F]+)$")
POST_RE = re.compile(r"^/[A-Za-z0-9._-]+\.db/(has-chunks|get-chunks|chunks|commit|refs)$")
PUT_RE = re.compile(r"^/[A-Za-z0-9._-]+\.db/(refs-if|refs)$")

def violate(msg):
    with open(violations_path, "a") as f:
        f.write(msg + "\n")

def count(method, path):
    ep = path.rsplit("/", 1)[-1] if "/chunk/" not in path else "chunk"
    with open(counts_path, "a") as f:
        f.write(f"{method} {ep}\n")

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.request.recv(4096)
            if not chunk:
                return
            data += chunk
            if len(data) > 1 << 20:
                violate("oversized request head")
                return
        head, rest = data.split(b"\r\n\r\n", 1)
        lines = head.decode("iso-8859-1").split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) < 3 or not parts[2].startswith("HTTP/1."):
            violate(f"malformed request line: {lines[0]!r}")
            return
        method, path = parts[0], parts[1]
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        if method == "GET":
            if not GET_RE.match(path):
                violate(f"unknown GET path: {path}")
        elif method in ("POST", "PUT"):
            body_re = POST_RE if method == "POST" else PUT_RE
            if not body_re.match(path):
                violate(f"unknown {method} path: {path}")
            if "content-length" not in headers:
                violate(f"{method} without Content-Length: {path}")
                self.request.sendall(
                    b"HTTP/1.1 411 Length Required\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n")
                return
        else:
            violate(f"unexpected method: {method} {path}")
        if "host" not in headers:
            violate(f"missing Host header: {method} {path}")

        nbody = int(headers.get("content-length", "0"))
        body = rest
        while len(body) < nbody:
            chunk = self.request.recv(min(65536, nbody - len(body)))
            if not chunk:
                violate(f"body shorter than Content-Length: {method} {path}")
                return
            body += chunk
        if len(body) > nbody:
            violate(f"body longer than Content-Length: {method} {path}")

        count(method, path)
        try:
            with socket.create_connection(("127.0.0.1", upstream_port), timeout=10) as s:
                s.sendall(head + b"\r\n\r\n" + body)
                s.shutdown(socket.SHUT_WR)
                status_checked = False
                while True:
                    chunk = s.recv(65536)
                    if not chunk:
                        break
                    if not status_checked:
                        status_checked = True
                        st = chunk.split(b" ", 2)
                        if len(st) > 1 and st[1][:1] == b"5":
                            violate(f"5xx response: {method} {path} -> {st[1].decode()}")
                    self.request.sendall(chunk)
        except OSError:
            # upstream down: surface a refusal so the client errors cleanly
            try:
                self.request.sendall(
                    b"HTTP/1.1 502 Bad Gateway\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n")
            except OSError:
                pass

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

with Server(("127.0.0.1", 0), Handler) as srv:
    print(srv.server_address[1], flush=True)
    srv.serve_forever()
PY

: >"$TMP/violations.log"
: >"$TMP/counts.log"
python3 "$TMP/proxy.py" "$SRV_PORT" "$TMP/violations.log" "$TMP/counts.log" >"$TMP/proxy.log" 2>"$TMP/proxy.err" &
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

URL="http://127.0.0.1:$PROXY_PORT/repo.db"
DB="$DOLTLITE"
A="$TMP/a.db"
B="$TMP/b.db"

echo "--- 1. initial push over http (schema + index + FK + 2MB blob) ---"
"$DB" "$A" <<SQL >/dev/null
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
INSERT INTO users VALUES(1,'alice',30),(2,'bob',25),(3,'charlie',35);
CREATE INDEX users_age ON users(age);
CREATE TABLE scores(uid INTEGER, score REAL, FOREIGN KEY(uid) REFERENCES users(id));
INSERT INTO scores VALUES(1,95.5),(2,87.3),(3,91.0);
CREATE TABLE blobs(id INTEGER PRIMARY KEY, data BLOB);
INSERT INTO blobs VALUES(1, randomblob(2097152));
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
SELECT dolt_remote('add','origin','$URL');
SQL
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "initial push returns 0" "0" "$result"

echo "--- 2. clone over http, full parity ---"
result=$("$DB" "$B" "SELECT dolt_clone('$URL');" 2>&1)
check "clone returns 0" "0" "$result"
src_state=$("$DB" "$A" "SELECT count(*)||'|'||sum(age) FROM users; SELECT round(sum(score),1) FROM scores; SELECT sum(length(data)) FROM blobs; SELECT count(*) FROM dolt_log; SELECT commit_hash FROM dolt_log LIMIT 1;")
cln_state=$("$DB" "$B" "SELECT count(*)||'|'||sum(age) FROM users; SELECT round(sum(score),1) FROM scores; SELECT sum(length(data)) FROM blobs; SELECT count(*) FROM dolt_log; SELECT commit_hash FROM dolt_log LIMIT 1;")
check "clone content and history match source" "$src_state" "$cln_state"
result=$("$DB" "$B" "SELECT count(*) FROM users WHERE age>26;")
check "clone index query works" "2" "$result"

echo "--- 3. incremental push, fast-forward pull ---"
result=$("$DB" "$A" "UPDATE users SET age=31 WHERE id=1; SELECT dolt_commit('-am','bday');
CREATE TABLE extra(k TEXT PRIMARY KEY, v TEXT); INSERT INTO extra VALUES('x','y');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','extra table'); SELECT dolt_push('origin','main');" 2>&1 | tail -1)
check "incremental push returns 0" "0" "$result"
result=$("$DB" "$B" "SELECT dolt_pull('origin','main');" 2>&1 | tail -1)
check_match "pull succeeds" "^0$|fast" "$result"
a_head=$("$DB" "$A" "SELECT commit_hash FROM dolt_log LIMIT 1;")
b_head=$("$DB" "$B" "SELECT commit_hash FROM dolt_log LIMIT 1; SELECT v FROM extra WHERE k='x'; SELECT age FROM users WHERE id=1;")
check "pull converges to source head with new data" "$a_head
y
31" "$b_head"

echo "--- 4. no-op push uploads no chunks ---"
before=$(grep -ac "POST chunks" "$TMP/counts.log")
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
after=$(grep -ac "POST chunks" "$TMP/counts.log")
check "no-op push returns 0" "0" "$result"
check "no-op push posts no chunk payloads" "$before" "$after"

echo "--- 5. branch push + fetch + checkout ---"
result=$("$DB" "$A" "SELECT dolt_checkout('-b','feature'); INSERT INTO users VALUES(4,'dave',40); SELECT dolt_commit('-am','dave'); SELECT dolt_push('origin','feature'); SELECT dolt_checkout('main');" 2>&1 | grep -c "^0$")
check "feature branch push steps all return 0" "3" "$result"
result=$("$DB" "$B" "SELECT dolt_fetch('origin','feature'); SELECT dolt_checkout('-b','feature_local','origin/feature'); SELECT count(*) FROM users; SELECT dolt_checkout('main');" 2>&1)
check "fetch + tracking checkout of pushed branch sees its rows" "0
0
4
0" "$result"

echo "--- 6. divergent histories: pull merges ---"
result=$("$DB" "$A" "INSERT INTO users VALUES(5,'erin',22); SELECT dolt_commit('-am','erin'); SELECT dolt_push('origin','main');" 2>&1 | tail -1)
check "A-side divergent push returns 0" "0" "$result"
"$DB" "$B" "INSERT INTO users VALUES(6,'frank',50); SELECT dolt_commit('-am','frank');" >/dev/null 2>&1
result=$("$DB" "$B" "SELECT dolt_pull('origin','main');" 2>&1 | tail -1)
check_match "divergent pull merges" "^0$|merge" "$result"
result=$("$DB" "$B" "SELECT count(*) FROM users WHERE id IN (5,6);")
check "merge kept both sides' rows" "2" "$result"
result=$("$DB" "$B" "SELECT dolt_push('origin','main');" 2>&1)
check "merged push returns 0" "0" "$result"
result=$("$DB" "$A" "SELECT dolt_pull('origin','main'); SELECT count(*) FROM users WHERE id IN (5,6);" 2>&1 | tail -1)
check "both clients converge" "2" "$result"

echo "--- 7. stale push rejected until pull ---"
result=$("$DB" "$A" "UPDATE users SET age=23 WHERE id=5; SELECT dolt_commit('-am','a-side'); SELECT dolt_push('origin','main');" 2>&1 | tail -1)
check "a-side push returns 0" "0" "$result"
"$DB" "$B" "UPDATE users SET name='francis' WHERE id=6; SELECT dolt_commit('-am','b-side');" >/dev/null 2>&1
result=$("$DB" "$B" "SELECT dolt_push('origin','main');" 2>&1)
check_match "stale push is rejected" "reject|fetch|behind|fast|stale|ERROR|Error" "$result"
result=$("$DB" "$B" "SELECT dolt_pull('origin','main'); SELECT dolt_push('origin','main');" 2>&1 | tail -1)
check "push succeeds after pull" "0" "$result"

echo "--- 8. second database on the same server ---"
"$DB" "$TMP/c.db" "CREATE TABLE t(a INTEGER PRIMARY KEY); INSERT INTO t VALUES(42); SELECT dolt_commit('-Am','c1'); SELECT dolt_remote('add','origin','http://127.0.0.1:$PROXY_PORT/second.db'); SELECT dolt_push('origin','main');" >/dev/null 2>&1
result=$("$DB" "$TMP/c2.db" "SELECT dolt_clone('http://127.0.0.1:$PROXY_PORT/second.db'); SELECT a FROM t;" 2>&1)
check "second db pushes and clones independently" "0
42" "$result"
origin_users=$("$DB" "$B" "SELECT count(*) FROM users;")
result=$("$DB" "$TMP/c3.db" "SELECT dolt_clone('$URL'); SELECT count(*) FROM users;" 2>&1)
check "first db unaffected by second" "0
$origin_users" "$result"

echo "--- 9. connection refused surfaces an error, local db intact ---"
DEAD_PORT=1
result=$("$DB" "$A" "SELECT dolt_remote('add','dead','http://127.0.0.1:$DEAD_PORT/x.db'); SELECT dolt_push('dead','main');" 2>&1)
check_match "push to dead endpoint errors" "ERROR|Error|error|failed|refused" "$result"
a_users=$("$DB" "$A" "SELECT count(*) FROM users;")
result=$("$DB" "$A" "SELECT count(*) FROM users; SELECT dolt_remote('remove','dead');")
check "local db intact after failed push" "$a_users
0" "$result"

echo "--- 10. server death mid-stream: error now, success after restart ---"
result=$("$DB" "$A" "SELECT dolt_pull('origin','main');" 2>&1 | tail -1)
check "A syncs with origin before the outage" "0" "$result"
kill "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null || true; SRV_PID=""
"$DB" "$A" "INSERT INTO blobs VALUES(2, randomblob(2097152)); SELECT dolt_commit('-am','blob2');" >/dev/null 2>&1
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "push with server down errors" "ERROR|Error|error|failed|refused" "$result"
restart_server_same_port
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "push succeeds after server restart" "0" "$result"
result=$("$DB" "$TMP/d.db" "SELECT dolt_clone('$URL'); SELECT sum(length(data)) FROM blobs;" 2>&1)
check "restarted server serves complete store" "0
4194304" "$result"

echo "--- 11. protocol conformance across the whole run ---"
if [ -s "$TMP/violations.log" ]; then
  echo "  FAIL: HTTP protocol violations recorded:"
  sed 's/^/    /' "$TMP/violations.log" | head -10
  fail=$((fail+1))
else
  echo "  PASS: no HTTP protocol violations across $(grep -ac . "$TMP/counts.log") requests"
  pass=$((pass+1))
fi

echo ""
echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
