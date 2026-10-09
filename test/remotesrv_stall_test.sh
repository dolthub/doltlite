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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-remotesrv-stall.XXXXXX")"
SRV_PID=""
CLIENT_PID=""
BLACKHOLE_PID=""
cleanup() {
  [ -n "$BLACKHOLE_PID" ] && { kill "$BLACKHOLE_PID" 2>/dev/null || true; wait "$BLACKHOLE_PID" 2>/dev/null || true; }
  [ -n "$CLIENT_PID" ] && { kill "$CLIENT_PID" 2>/dev/null || true; wait "$CLIENT_PID" 2>/dev/null || true; }
  [ -n "$SRV_PID" ] && { kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; }
  rm -rf "$TMP"
}
trap cleanup EXIT

start_server() {
  local log="$1"
  shift
  mkdir -p "$TMP/srv"
  "$REMOTESRV" --timeout-ms 2000 -p 0 --bind 127.0.0.1 "$@" \
    "$TMP/srv" >"$log" 2>&1 &
  SRV_PID=$!
  PORT=""
  for _ in $(seq 1 50); do
    PORT="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$log" | head -1)"
    [ -n "$PORT" ] && return
    sleep 0.1
  done
  echo "FAIL: server did not start"
  cat "$log"
  exit 1
}

stop_server_with_stalled_clients() {
  local port="$1"
  python3 - "$port" <<'PY' &
import socket
import sys
import time

sockets = []
for _ in range(24):
    try:
        s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
        s.sendall(b"G")
        sockets.append(s)
    except OSError:
        pass
time.sleep(30)
PY
  CLIENT_PID=$!
  sleep 0.2
  kill -TERM "$SRV_PID"
  for _ in $(seq 1 30); do
    kill -0 "$SRV_PID" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$SRV_PID" 2>/dev/null; then
    echo "FAIL: server shutdown blocked on stalled client"
    exit 1
  fi
  wait "$SRV_PID" 2>/dev/null || true
  SRV_PID=""
  kill "$CLIENT_PID" 2>/dev/null || true
  wait "$CLIENT_PID" 2>/dev/null || true
  CLIENT_PID=""
  echo "  PASS: shutdown interrupts active and queued stalled clients"
}

echo "=== stalled remote server ==="
python3 - "$TMP/blackhole.port" <<'PY' &
import socket
import sys
import time

server = socket.socket()
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("127.0.0.1", 0))
server.listen(1)
with open(sys.argv[1], "w") as f:
    f.write(str(server.getsockname()[1]))

conn, _ = server.accept()
request = b""
while b"\r\n\r\n" not in request:
    data = conn.recv(4096)
    if not data:
        break
    request += data

response = b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n" + b"x" * 20
for byte in response:
    try:
        conn.sendall(bytes([byte]))
    except OSError:
        break
    time.sleep(0.2)
conn.close()
server.close()
PY
BLACKHOLE_PID=$!

BLACKHOLE_PORT=""
for _ in $(seq 1 50); do
  BLACKHOLE_PORT="$(cat "$TMP/blackhole.port" 2>/dev/null || true)"
  [ -n "$BLACKHOLE_PORT" ] && break
  sleep 0.1
done
if [ -z "$BLACKHOLE_PORT" ]; then
  echo "FAIL: stalled remote did not start"
  exit 1
fi

python3 - "$DOLTLITE" "$TMP/client.db" "$BLACKHOLE_PORT" <<'PY'
import os
import subprocess
import sys
import time

env = os.environ.copy()
env["DOLTLITE_HTTP_TIMEOUT_MS"] = "750"
url = "http://127.0.0.1:{}/repo.db".format(sys.argv[3])
start = time.monotonic()
try:
    result = subprocess.run(
        [sys.argv[1], sys.argv[2], "SELECT dolt_clone('{}');".format(url)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=4,
    )
except subprocess.TimeoutExpired as exc:
    raise AssertionError("HTTP remote client ignored its deadline") from exc
elapsed = time.monotonic() - start
assert result.returncode != 0, result.stdout
assert 0.3 < elapsed < 3.0, elapsed
print("  PASS: HTTP remote client cuts a dribbling response at its deadline")
PY
kill "$BLACKHOLE_PID" 2>/dev/null || true
wait "$BLACKHOLE_PID" 2>/dev/null || true
BLACKHOLE_PID=""

echo "=== plaintext stalled clients ==="
start_server "$TMP/plain.log"
python3 - "$PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])

def connect(payload):
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.sendall(payload)
    return s

def healthy_request():
    start = time.monotonic()
    s = connect(b"GET /missing.db/root HTTP/1.1\r\nHost: localhost\r\n\r\n")
    response = b""
    while b"\r\n\r\n" not in response:
        response += s.recv(4096)
    s.close()
    elapsed = time.monotonic() - start
    assert response.startswith(b"HTTP/1.1 404 "), response
    assert elapsed < 1.5, elapsed

def expect_closed(s):
    s.settimeout(4)
    while s.recv(4096):
        pass
    s.close()

def expect_dribble_closed(prefix):
    # Dribble bytes slower than the total request deadline but faster than the
    # per-recv timeout, so every recv succeeds yet the connection must still be
    # cut. Without a total deadline this pins a worker indefinitely.
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.sendall(prefix)
    start = time.monotonic()
    closed_at = None
    for _ in range(20):
        try:
            s.sendall(b"X")
        except OSError:
            closed_at = time.monotonic() - start
            break
        s.settimeout(1.0)
        try:
            if s.recv(4096) == b"":
                closed_at = time.monotonic() - start
                break
        except socket.timeout:
            pass
        except OSError:
            closed_at = time.monotonic() - start
            break
        time.sleep(0.5)
    s.close()
    assert closed_at is not None, "server never cut the dribbling client"
    assert closed_at < 8, closed_at

header = connect(b"G")
body = connect(
    b"POST /repo.db/chunks HTTP/1.1\r\n"
    b"Host: localhost\r\nContent-Length: 10\r\n\r\nx"
)
healthy_request()
expect_closed(header)
expect_closed(body)
print("  PASS: partial headers and bodies do not block healthy requests")
print("  PASS: plaintext read deadlines close stalled clients")

# A never-ending header, one byte at a time.
expect_dribble_closed(b"GET /repo.db/roo")
# A body that never reaches its declared Content-Length.
expect_dribble_closed(
    b"POST /repo.db/chunks HTTP/1.1\r\n"
    b"Host: localhost\r\nContent-Length: 100000\r\n\r\n"
)
healthy_request()
print("  PASS: dribbling header and body clients are cut by the request deadline")
PY
stop_server_with_stalled_clients "$PORT"

echo "=== slow get-chunks reader does not pin the database ==="
rm -rf "$TMP/srv"
mkdir -p "$TMP/srv"
"$REMOTESRV" --timeout-ms 15000 -p 0 --bind 127.0.0.1 \
  "$TMP/srv" >"$TMP/slow.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
  PORT="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/slow.log" | head -1)"
  [ -n "$PORT" ] && break
  sleep 0.1
done
if [ -z "$PORT" ]; then
  echo "FAIL: slow-read server did not start"
  cat "$TMP/slow.log"
  exit 1
fi

"$DOLTLITE" "$TMP/seed_repo.db" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'repo');
SELECT dolt_commit('-A','-m','repo');
SELECT dolt_remote('add','origin','http://127.0.0.1:$PORT/repo.db');
SQL
repo_push="$("$DOLTLITE" "$TMP/seed_repo.db" "SELECT dolt_push('origin','main');" 2>&1 || true)"
if [ "$repo_push" != "0" ]; then
  echo "FAIL: repo push: $repo_push"
  exit 1
fi
"$DOLTLITE" "$TMP/seed_other.db" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'other');
SELECT dolt_commit('-A','-m','other');
SELECT dolt_remote('add','origin','http://127.0.0.1:$PORT/other.db');
SQL
other_push="$("$DOLTLITE" "$TMP/seed_other.db" "SELECT dolt_push('origin','main');" 2>&1 || true)"
if [ "$other_push" != "0" ]; then
  echo "FAIL: other push: $other_push"
  exit 1
fi

python3 - "$DOLTLITE" "$PORT" "$TMP" <<'PY'
import socket
import subprocess
import sys
import threading
import time

doltlite, port_s, tmp = sys.argv[1:]
port = int(port_s)
limit_s = 8.0

def exchange(method, path, body=b"", timeout=10):
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    sock.settimeout(timeout)
    head = (
        "%s %s HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n" % (method, path, len(body))
    ).encode("ascii")
    sock.sendall(head + body)
    data = b""
    while True:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    sock.close()
    return data

def split_response(data):
    head, sep, body = data.partition(b"\r\n\r\n")
    if not sep:
        raise AssertionError("short response %r" % (data[:80],))
    line = head.split(b"\r\n", 1)[0]
    status = int(line.split(b" ")[1])
    headers = {}
    for raw in head.split(b"\r\n")[1:]:
        if b":" not in raw:
            continue
        name, value = raw.split(b":", 1)
        headers[name.strip().lower()] = value.strip()
    return status, headers, body

root = exchange("GET", "/repo.db/root")
status, headers, root_body = split_response(root)
assert status == 200 and len(root_body) == 20, root[:120]

one = exchange("POST", "/repo.db/get-chunks", root_body)
status, headers, one_body = split_response(one)
assert status == 200 and len(one_body) >= 4, one[:120]
chunk_len = int.from_bytes(one_body[:4], "big")
assert chunk_len != 0xFFFFFFFF and chunk_len + 4 == len(one_body), one_body[:16]
unit = 4 + chunk_len
target = 8 * 1024 * 1024
repeats = max(1, (target + unit - 1) // unit)
payload = root_body * repeats

ready = threading.Event()
hold = threading.Event()
hog_error = []
hog_state = {}

def hog():
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        rcvbuf = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        head = (
            "POST /repo.db/get-chunks HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n"
            "\r\n" % len(payload)
        ).encode("ascii")
        sock.sendall(head + payload)
        sock.settimeout(10)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(1)
            if not chunk:
                break
            data += chunk
        status, headers, _ = split_response(data)
        length = int(headers.get(b"content-length", b"0"))
        hog_state["rcvbuf"] = rcvbuf
        hog_state["length"] = length
        hog_state["sock"] = sock
        if status != 200 or length < target or length <= rcvbuf:
            hog_error.append(
                "hog response %s len %s rcvbuf %s" % (status, length, rcvbuf))
        ready.set()
        # Leave the body unread so the server write fills the window.
        hold.wait(timeout=30)
        sock.close()
    except Exception as exc:
        hog_error.append(repr(exc))
        ready.set()

threading.Thread(target=hog, daemon=True).start()
assert ready.wait(timeout=20), "hog never received a response"
if hog_error:
    raise AssertionError(hog_error[0])

results = {}

def run_clone():
    started = time.monotonic()
    try:
        proc = subprocess.run(
            [doltlite, tmp + "/clone_repo.db",
             "SELECT dolt_clone('http://127.0.0.1:%d/repo.db'); SELECT v FROM t;" % port],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20,
        )
        results["clone"] = (proc.returncode, proc.stdout.decode("utf-8", "replace"),
                            time.monotonic() - started)
    except Exception as exc:
        results["clone"] = (1, repr(exc), time.monotonic() - started)

def run_get(name, path):
    started = time.monotonic()
    try:
        data = exchange("GET", path, timeout=12)
        status, _, body = split_response(data)
        results[name] = (status, len(body), time.monotonic() - started)
    except Exception as exc:
        results[name] = (0, repr(exc), time.monotonic() - started)

threads = [
    threading.Thread(target=run_clone),
    threading.Thread(target=run_get, args=("repo_a", "/repo.db/root")),
    threading.Thread(target=run_get, args=("repo_b", "/repo.db/root")),
    threading.Thread(target=run_get, args=("other", "/other.db/root")),
]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
# Probes finished without reading the body. The receive buffer must still
# hold less than the response, which is the unread tail blocking the write.
try:
    import array
    import fcntl
    import termios
    queued = array.array("i", [0])
    fcntl.ioctl(hog_state["sock"].fileno(), termios.FIONREAD, queued, True)
    assert queued[0] < hog_state["length"], (queued[0], hog_state["length"])
except AssertionError:
    raise
except Exception:
    pass
hold.set()

clone_rc, clone_out, clone_s = results["clone"]
clone_lines = [line for line in clone_out.replace("\r", "").split("\n") if line != ""]
assert clone_rc == 0 and clone_lines[-2:] == ["0", "repo"], (clone_rc, clone_out)
assert clone_s < limit_s, clone_s
for name in ("repo_a", "repo_b", "other"):
    status, nbody, elapsed = results[name]
    assert status == 200 and nbody == 20, (name, results[name])
    assert elapsed < limit_s, (name, elapsed)
print("  PASS: unread get-chunks does not block clone (%.2fs, %d byte response)" % (
    clone_s, hog_state["length"]))
print("  PASS: unread get-chunks does not block other readers "
      "(repo %.2fs/%.2fs, other %.2fs)" % (
          results["repo_a"][2], results["repo_b"][2], results["other"][2]))
PY

kill "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
SRV_PID=""

if ! command -v openssl >/dev/null 2>&1; then
  echo "SKIP: openssl not available; TLS stall checks skipped"
  exit 0
fi
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$TMP/key.pem" -out "$TMP/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1 || { echo "SKIP: openssl cert generation failed"; exit 0; }

echo "=== TLS stalled client ==="
rm -rf "$TMP/srv"
start_server "$TMP/tls.log" --cert "$TMP/cert.pem" --key "$TMP/key.pem"
python3 - "$PORT" <<'PY'
import socket
import ssl
import sys
import time

port = int(sys.argv[1])
stalled = socket.create_connection(("127.0.0.1", port), timeout=2)

start = time.monotonic()
context = ssl._create_unverified_context()
healthy = context.wrap_socket(
    socket.create_connection(("127.0.0.1", port), timeout=2),
    server_hostname="localhost",
)
healthy.sendall(b"GET /missing.db/root HTTP/1.1\r\nHost: localhost\r\n\r\n")
response = b""
while b"\r\n\r\n" not in response:
    response += healthy.recv(4096)
healthy.close()
elapsed = time.monotonic() - start
assert response.startswith(b"HTTP/1.1 404 "), response
assert elapsed < 1.5, elapsed

stalled.settimeout(4)
while stalled.recv(4096):
    pass
stalled.close()
print("  PASS: stalled TLS handshake does not block healthy requests")
print("  PASS: TLS handshake deadline closes stalled clients")
PY
stop_server_with_stalled_clients "$PORT"
