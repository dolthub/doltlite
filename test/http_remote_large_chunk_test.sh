#!/usr/bin/env bash
set -euo pipefail

DOLTLITE="${1:?DoltLite binary required}"
REMOTESRV="${2:?remote server binary required}"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/doltlite-http-large.XXXXXX")
SRV_PID=""
cleanup() {
  if [ -n "$SRV_PID" ]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT
mkdir "$TMP/srv"
"$REMOTESRV" -p 0 --bind 127.0.0.1 "$TMP/srv" >"$TMP/srv.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
  PORT=$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/srv.log" | head -1)
  [ -z "$PORT" ] || break
  sleep 0.1
done
if [ -z "$PORT" ]; then cat "$TMP/srv.log"; exit 1; fi
URL="http://127.0.0.1:$PORT/large.db"

python3 - "$PORT" <<'PY'
import socket
import sys

with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as sock:
    sock.sendall(
        b"POST /large.db/chunks HTTP/1.1\r\nHost: localhost\r\n"
        b"Content-Length: 1073741825\r\nConnection: close\r\n\r\n"
    )
    response = b""
    while b"\r\n\r\n" not in response:
        part = sock.recv(4096)
        if not part:
            raise SystemExit("FAIL: server closed without an HTTP response")
        response += part
    if response.split(b" ", 2)[1] != b"413":
        raise SystemExit(f"FAIL: request just over 1 GiB was accepted: {response!r}")
print("PASS: request just over 1 GiB is rejected before reading its body")
PY

"$DOLTLITE" -bail "$TMP/source.db" <<SQL >"$TMP/source.out"
CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB);
INSERT INTO t VALUES(1, randomblob(129*1024*1024));
SELECT dolt_commit('-A','-m','large chunk');
SELECT dolt_remote('add','origin','$URL');
SELECT dolt_push('origin','main');
SQL
"$DOLTLITE" -bail "$TMP/clone.db" "SELECT dolt_clone('$URL');" >"$TMP/clone.out"
QUERY="SELECT length(payload), hex(sha3(payload,256)) FROM t; PRAGMA integrity_check;"
"$DOLTLITE" -bail "$TMP/source.db" "$QUERY" >"$TMP/expected"
"$DOLTLITE" -bail "$TMP/clone.db" "$QUERY" >"$TMP/actual"
diff -u "$TMP/expected" "$TMP/actual"
grep -q '^135266304|' "$TMP/actual"
grep -qx 'ok' "$TMP/actual"
echo 'PASS: push and clone preserve a chunk larger than 128 MiB'

"$DOLTLITE" -bail "$TMP/source.db" <<'SQL' >"$TMP/update.out"
UPDATE t SET payload=randomblob(129*1024*1024);
SELECT dolt_commit('-A','-m','replace large chunk');
SELECT dolt_push('origin','main');
SQL
"$DOLTLITE" -bail "$TMP/clone.db" "SELECT dolt_pull('origin','main');" >"$TMP/pull.out"
"$DOLTLITE" -bail "$TMP/source.db" "$QUERY" >"$TMP/expected"
"$DOLTLITE" -bail "$TMP/clone.db" "$QUERY" >"$TMP/actual"
diff -u "$TMP/expected" "$TMP/actual"
echo 'PASS: fetch and pull preserve a replacement chunk larger than 128 MiB'
echo '__SUITE_COMPLETE__'
