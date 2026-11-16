#!/usr/bin/env bash
# With --auth-keys, a request that has no valid bearer token is rejected
# from the headers alone. The declared body is not allocated or read.
set -u
. "$(dirname "$0")/lib/stop_process.sh"

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"
HERE=$(cd "$(dirname "$0")/.." && pwd)
CC="${CC:-cc}"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/doltlite-auth-body.XXXXXX")
SRV_PID=""

cleanup() {
  if [ -n "$SRV_PID" ]; then
    stop_process "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    SRV_PID=""
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0
fail=0
check() {
  if [ "$2" = "$3" ]; then
    echo "  PASS: $1"
    pass=$((pass + 1))
  else
    echo "  FAIL: $1"
    echo "    expected: |$2|"
    echo "    actual:   |$3|"
    fail=$((fail + 1))
  fi
}

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 not available"
  exit 0
fi
if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"
  exit 0
fi

mkdir -p "$TMP/cc" "$TMP/authkeys" "$TMP/srv"
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/throwaway.db" \
  "SELECT dolt_creds_new();" >/dev/null 2>&1
private_jwk=$(find "$TMP/cc" -maxdepth 1 -name '*.jwk' -print | head -1)
if [ -z "$private_jwk" ]; then
  echo "FAIL: no credential generated"
  exit 1
fi
kid=$(basename "$private_jwk" .jwk)
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT dolt_creds('export','$kid','$TMP/authkeys');" >/dev/null 2>&1 || {
  echo "FAIL: public credential export failed"
  exit 1
}

DOLTLITE_EXTRA_LIBS=""
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) DOLTLITE_EXTRA_LIBS="-lws2_32 -lbcrypt -lcrypt32";;
esac
"$CC" -O2 -Wall \
  -I "$HERE/src" -I "$HERE/ext/ed25519" \
  "$HERE/test/creds_token.c" \
  "$HERE/test/sqlite3_alloc_stub.c" \
  "$HERE/src/doltlite_creds.c" \
  "$HERE/ext/ed25519/fe.c" \
  "$HERE/ext/ed25519/ge.c" \
  "$HERE/ext/ed25519/sc.c" \
  "$HERE/ext/ed25519/sha512.c" \
  "$HERE/ext/ed25519/keypair.c" \
  "$HERE/ext/ed25519/sign.c" \
  "$HERE/ext/ed25519/verify.c" \
  "$HERE/ext/ed25519/add_scalar.c" \
  $DOLTLITE_EXTRA_LIBS \
  -o "$TMP/creds_token" 2>"$TMP/token_build.err" || {
  echo "FAIL: bearer-token test helper did not build"
  cat "$TMP/token_build.err"
  exit 1
}
token=$("$TMP/creds_token" "$TMP/cc" localhost) || {
  echo "FAIL: bearer-token test helper failed"
  exit 1
}

"$REMOTESRV" --timeout-ms 8000 --auth-keys "$TMP/authkeys" --audience localhost \
  -p 0 --bind 127.0.0.1 "$TMP/srv" >"$TMP/srv.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
  PORT=$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/srv.log" | head -1)
  [ -n "$PORT" ] && break
  sleep 0.1
done
if [ -z "$PORT" ]; then
  echo "FAIL: server did not start"
  cat "$TMP/srv.log"
  exit 1
fi

py_rc=0
py_out=$(python3 - "$PORT" "$token" "$SRV_PID" <<'PY'
import socket
import subprocess
import sys
import threading
import time

port = int(sys.argv[1])
token = sys.argv[2]
pid = int(sys.argv[3])
big = 64 * 1024 * 1024
results = {}
peak = 0
baseline = None
stop = False

def rss_kb():
    try:
        out = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)],
            text=True, stderr=subprocess.DEVNULL)
        return int(out.split()[0])
    except Exception:
        return None

def sampler():
    global peak
    while not stop:
        reading = rss_kb()
        if reading is not None and reading > peak:
            peak = reading
        time.sleep(0.02)

def status_of(payload):
    sock = socket.create_connection(("127.0.0.1", port), timeout=2)
    sock.settimeout(2)
    try:
        sock.sendall(payload)
        data = b""
        while b"\r\n" not in data:
            chunk = sock.recv(512)
            if not chunk:
                break
            data += chunk
    except socket.timeout:
        return "TIMEOUT"
    finally:
        sock.close()
    parts = data.split(b"\r\n", 1)[0].split()
    if len(parts) < 2:
        return "BAD"
    return parts[1].decode()

def run(name, payload):
    results[name] = status_of(payload)

baseline = rss_kb()
if baseline is not None:
    peak = baseline
watch = threading.Thread(target=sampler)
watch.start()
workers = [
    threading.Thread(target=run, args=("missing", (
        "POST /repo.db/chunks HTTP/1.1\r\n"
        "Host: localhost\r\n"
        f"Content-Length: {big}\r\n"
        "\r\n").encode())),
    threading.Thread(target=run, args=("bogus", (
        "POST /repo.db/chunks HTTP/1.1\r\n"
        "Host: localhost\r\n"
        f"Content-Length: {big}\r\n"
        "Authorization: Bearer not-a-token\r\n"
        "\r\n").encode())),
]
for worker in workers:
    worker.start()
for worker in workers:
    worker.join()
results["oversize"] = status_of(
    b"POST /repo.db/chunks HTTP/1.1\r\n"
    b"Host: localhost\r\n"
    b"Content-Length: 2000000000\r\n"
    b"\r\n")
results["oversize_auth"] = status_of((
    "POST /repo.db/chunks HTTP/1.1\r\n"
    "Host: localhost\r\n"
    "Content-Length: 2000000000\r\n"
    f"Authorization: Bearer {token}\r\n"
    "\r\n").encode())
results["body_read"] = status_of((
    "POST /repo.db/has-chunks HTTP/1.1\r\n"
    "Host: localhost\r\n"
    "Content-Length: 1\r\n"
    f"Authorization: Bearer {token}\r\n"
    "\r\nx").encode())
results["empty_ok"] = status_of((
    "POST /repo.db/has-chunks HTTP/1.1\r\n"
    "Host: localhost\r\n"
    "Content-Length: 0\r\n"
    f"Authorization: Bearer {token}\r\n"
    "\r\n").encode())
stop = True
watch.join()

expect = {
    "missing": "401",
    "bogus": "401",
    "oversize": "401",
    "oversize_auth": "413",
    "body_read": "400",
    "empty_ok": "200",
}
for name, wanted in expect.items():
    got = results.get(name, "MISSING")
    if got == wanted:
        print(f"PASS {name} {got}")
    else:
        print(f"FAIL {name} expected {wanted} got {got}")
if baseline is None:
    print("SKIP rss")
else:
    grew = peak - baseline
    if grew < 16384:
        print(f"PASS rss grew {grew}KB")
    else:
        print(f"FAIL rss grew {grew}KB peak {peak} base {baseline}")
sys.exit(0 if all(results.get(name)==wanted for name, wanted in expect.items())
         and (baseline is None or peak - baseline < 16384) else 1)
PY
) || py_rc=$?

while IFS= read -r line; do
  [ -n "$line" ] || continue
  case "$line" in
    PASS\ *) check "${line#PASS }" "ok" "ok" ;;
    FAIL\ *) check "${line#FAIL }" "pass" "fail: $line" ;;
    SKIP\ *) echo "  SKIP: ${line#SKIP }" ;;
    *) echo "$line" ;;
  esac
done <<EOF
$py_out
EOF

if [ "$py_rc" -ne 0 ] && [ "$fail" -eq 0 ]; then
  check "auth body probe" "0" "$py_rc"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ]
