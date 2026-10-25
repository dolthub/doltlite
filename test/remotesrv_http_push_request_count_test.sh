#!/usr/bin/env bash
#
# Request counts for pushes from a lazy clone over HTTP. A push of a ref
# whose target the remote already publishes must not walk the commit's
# graph, and chunks a push does read through the lazy origin are fetched
# in batches rather than one request per chunk.
#
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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-http-push-count.XXXXXX")"
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
check_le() {
  local desc="$1" limit="$2" actual="$3"
  if [ "$actual" -le "$limit" ]; then
    echo "  PASS: $desc ($actual <= $limit)"; pass=$((pass+1))
  else
    echo "  FAIL: $desc ($actual > $limit)"
    fail=$((fail+1))
  fi
}
check_ge() {
  local desc="$1" limit="$2" actual="$3"
  if [ "$actual" -ge "$limit" ]; then
    echo "  PASS: $desc ($actual >= $limit)"; pass=$((pass+1))
  else
    echo "  FAIL: $desc ($actual < $limit)"
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
counts_path = sys.argv[2]
lock = threading.Lock()

def record(method, path):
    ep = "chunk" if "/chunk/" in path else path.rsplit("/", 1)[-1]
    with lock:
        with open(counts_path, "a") as f:
            f.write(f"{method} {ep}\n")

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.request.recv(65536)
            if not chunk:
                return
            data += chunk
        head, rest = data.split(b"\r\n\r\n", 1)
        lines = head.decode("iso-8859-1").split("\r\n")
        parts = lines[0].split(" ", 2)
        if len(parts) < 2:
            return
        nbody = 0
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                if k.strip().lower() == "content-length":
                    nbody = int(v.strip())
        body = rest
        while len(body) < nbody:
            chunk = self.request.recv(min(1 << 20, nbody - len(body)))
            if not chunk:
                return
            body += chunk
        record(parts[0], parts[1])
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

: >"$TMP/counts.log"
python3 "$TMP/proxy.py" "$SRV_PORT" "$TMP/counts.log" >"$TMP/proxy.log" 2>"$TMP/proxy.err" &
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
L=""
LURI=""

n_get()   { grep -Eac "^(GET chunk|POST get-chunks)$" "$TMP/counts.log"; }
n_has()   { grep -ac "^POST has-chunks$" "$TMP/counts.log"; }
n_put()   { grep -ac "^POST chunks$" "$TMP/counts.log"; }
n_total() { wc -l <"$TMP/counts.log" | tr -d ' '; }

# measure <label> <sql...>: run sql against the lazy clone, print counts.
measure() {
  local label="$1" sql="$2" g0 h0 p0 t0 out
  g0=$(n_get); h0=$(n_has); p0=$(n_put); t0=$(n_total)
  out=$("$DB" "$LURI" "$sql" 2>&1)
  M_RESULT="$out"
  M_GET=$(( $(n_get) - g0 ))
  M_HAS=$(( $(n_has) - h0 ))
  M_PUT=$(( $(n_put) - p0 ))
  M_TOTAL=$(( $(n_total) - t0 ))
  echo "  [$label] get-chunks=$M_GET has-chunks=$M_HAS put-chunks=$M_PUT total=$M_TOTAL"
}

echo "=== seed remote ==="
"$DB" "$A" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE seq(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM seq WHERE i<40000)
INSERT INTO t SELECT i, printf('row-%08d-%s', i, hex(randomblob(16))) FROM seq;
SELECT dolt_commit('-A','-m','old');
UPDATE t SET v = v || '-new' WHERE id % 97 = 0;
SELECT dolt_commit('-a','-m','head');
SELECT dolt_remote('add','origin','$URL');
SQL
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "seed push succeeds" "0" "$result"
old=$("$DB" "$A" "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;")

# Each scenario starts from a fresh lazy clone so chunks an earlier push
# faulted in do not hide the cost of a later one.
lazy_n=0
fresh_lazy() {
  lazy_n=$((lazy_n+1))
  L="$TMP/lazy$lazy_n.db"
  LURI="file:$L?lazy_origin=1"
  local r
  r=$("$DB" "$L" "SELECT dolt_clone('--lazy','$URL');" 2>&1)
  [ "$r" = "0" ] || { echo "FAIL: lazy clone: $r"; exit 1; }
}

echo "=== branch at the published head ==="
fresh_lazy
"$DB" "$LURI" "SELECT dolt_branch('feat');" >/dev/null
measure "push new branch at head" "SELECT dolt_push('origin','feat');"
check "push new branch at head succeeds" "0" "$M_RESULT"
check "branch push fetches no chunks" "0" "$M_GET"
check "branch push uploads no chunks" "0" "$M_PUT"
check_le "branch push makes O(1) requests" 4 "$M_TOTAL"

echo "=== tag at the published head ==="
fresh_lazy
"$DB" "$LURI" "SELECT dolt_tag('vhead');" >/dev/null
measure "push tag at head" "SELECT dolt_push('origin','vhead');"
check "tag push at head succeeds" "0" "$M_RESULT"
check "tag push at head fetches no chunks" "0" "$M_GET"
check_le "tag push at head makes O(1) requests" 5 "$M_TOTAL"

echo "=== fast-forward a branch to another branch's published head ==="
fresh_lazy
"$DB" "$LURI" "SELECT dolt_branch('edits'); SELECT dolt_checkout('edits'); INSERT INTO t VALUES(800001,'edit'); SELECT dolt_commit('-a','-m','edit'); SELECT dolt_push('origin','edits');" >/dev/null 2>&1
edits_head=$("$DB" "$LURI" "SELECT hash FROM dolt_branches WHERE name='edits';" 2>&1)
fresh_lazy
"$DB" "$LURI" "SELECT dolt_branch('ff','$edits_head');" >/dev/null
measure "push branch at another branch's head" "SELECT dolt_push('origin','ff');"
check "fast-forward push succeeds" "0" "$M_RESULT"
check "fast-forward push fetches no chunks" "0" "$M_GET"
check_le "fast-forward push makes O(1) requests" 4 "$M_TOTAL"

echo "=== tag at an older, unpublished commit ==="
fresh_lazy
"$DB" "$LURI" "SELECT dolt_tag('vold','$old');" >/dev/null
measure "push tag at older commit" "SELECT dolt_push('origin','vold');"
check "tag push at older commit succeeds" "0" "$M_RESULT"
# The older commit is not a published ref target, so its graph is rescanned
# in case an earlier push left it partial. Batched reads keep that bounded.
check_le "older tag rescan batches lazy fetches" 40 "$M_GET"

echo "=== new commit on a branch ==="
fresh_lazy
measure "push new commit" "SELECT dolt_branch('feat2'); SELECT dolt_checkout('feat2'); INSERT INTO t VALUES(900001,'new'); SELECT dolt_commit('-a','-m','feat change'); SELECT dolt_push('origin','feat2');"
check_ge "new commit push uploads chunks" 1 "$M_PUT"
check_le "new commit push fetches few chunks" 20 "$M_GET"
result=$("$DB" "$TMP/verify.db" "SELECT dolt_clone('--branch','feat2','$URL'); SELECT v FROM t WHERE id=900001; SELECT count(*) FROM t;" 2>&1)
check "remote holds the new commit" "0
new
40001" "$result"
result=$("$DB" "$TMP/verify.db" "SELECT count(*) FROM dolt_tags WHERE tag_name IN ('vhead','vold');" 2>&1)
check "remote holds both tags" "2" "$result"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
