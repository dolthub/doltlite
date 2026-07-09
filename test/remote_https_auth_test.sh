#!/bin/bash
set -u

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"
TMP=$(mktemp -d)
SRV_PID=""
cleanup() {
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0
fail=0
check() {
  local a="${2//$'\r'/}" b="${3//$'\r'/}"
  if [ "$a" = "$b" ]; then echo "  PASS: $1"; pass=$((pass+1));
  else echo "  FAIL: $1"; echo "    expected: |$a|"; echo "    actual:   |$b|"; fail=$((fail+1)); fi
}
check_ne() {
  local a="${2//$'\r'/}" b="${3//$'\r'/}"
  if [ "$a" != "$b" ]; then echo "  PASS: $1"; pass=$((pass+1));
  else echo "  FAIL: $1 (got the not-allowed value |$b|)"; fail=$((fail+1)); fi
}

if ! command -v openssl >/dev/null 2>&1; then
  echo "SKIP: openssl not available"; exit 0
fi
if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"; exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$TMP/key.pem" -out "$TMP/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1 || { echo "SKIP: openssl cert generation failed"; exit 0; }
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout /dev/null -out "$TMP/other_ca.pem" -subj "/CN=other" >/dev/null 2>&1

mkdir -p "$TMP/cc" "$TMP/cc_unauth" "$TMP/empty" "$TMP/authkeys" "$TMP/srv" "$TMP/srv2"
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/throwaway.db" "SELECT dolt_creds_new();" >/dev/null 2>&1
cp "$TMP"/cc/*.jwk "$TMP/authkeys/" 2>/dev/null || { echo "FAIL: no credential generated"; exit 1; }
DOLTLITE_CREDS_DIR="$TMP/cc_unauth" "$DOLTLITE" "$TMP/throwaway2.db" "SELECT dolt_creds_new();" >/dev/null 2>&1

"$REMOTESRV" --cert "$TMP/cert.pem" --key "$TMP/key.pem" \
  --auth-keys "$TMP/authkeys" --audience localhost \
  -p 0 --bind 127.0.0.1 "$TMP/srv" >"$TMP/srv.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
  PORT=$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/srv.log" | head -1)
  [ -n "$PORT" ] && break
  sleep 0.1
done
if [ -z "$PORT" ]; then echo "FAIL: server did not start"; cat "$TMP/srv.log"; exit 1; fi
URL="https://localhost:$PORT/repo.db"
echo "server on port $PORT"

export DOLTLITE_CA_FILE="$TMP/cert.pem"
export DOLT_OVERRIDE_GRPC_JWT_AUDIENCE="localhost"

"$DOLTLITE" "$TMP/src.db" <<ENDSQL >/dev/null 2>&1
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES(1,'alice'),(2,'bob'),(3,'charlie');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial');
SELECT dolt_remote('add','origin','$URL');
.quit
ENDSQL

echo "=== 1. Authenticated HTTPS push ==="
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('origin','main');" 2>&1)
check "authenticated push returns 0" "0" "$result"

src_head=$("$DOLTLITE" "$TMP/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>&1)
srv_head=$("$DOLTLITE" "$TMP/srv/repo.db" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>&1)
check "server head matches pushed head" "$src_head" "$srv_head"

echo "=== 2. Authenticated HTTPS clone ==="
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/clone.db" "SELECT dolt_clone('$URL');" 2>&1)
check "authenticated clone returns 0" "0" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/clone.db" "SELECT count(*) FROM users;" 2>&1)
check "clone round-trips 3 users" "3" "$result"

echo "=== 3. Auth is enforced ==="
result=$(DOLTLITE_CREDS_DIR="$TMP/empty" "$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('origin','main');" 2>&1)
check_ne "push without a credential is rejected" "0" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc_unauth" "$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('origin','main');" 2>&1)
check_ne "push with an unauthorized key is rejected" "0" "$result"

echo "=== 4. TLS is enforced ==="
"$DOLTLITE" "$TMP/src.db" "SELECT dolt_remote('add','plain','http://localhost:$PORT/repo.db');" >/dev/null 2>&1
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('plain','main');" 2>&1)
check_ne "plaintext http to the TLS server is rejected" "0" "$result"
result=$(DOLTLITE_CA_FILE="$TMP/other_ca.pem" DOLTLITE_CREDS_DIR="$TMP/cc" \
  "$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('origin','main');" 2>&1)
check_ne "push with the wrong CA is rejected" "0" "$result"

echo "=== 5. Backward-compat: plaintext server, no auth ==="
"$REMOTESRV" -p 0 --bind 127.0.0.1 "$TMP/srv2" >"$TMP/srv2.log" 2>&1 &
SRV2_PID=$!
PORT2=""
for _ in $(seq 1 50); do
  PORT2=$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/srv2.log" | head -1)
  [ -n "$PORT2" ] && break
  sleep 0.1
done
if [ -n "$PORT2" ]; then
  "$DOLTLITE" "$TMP/src.db" "SELECT dolt_remote('add','plainsrv','http://localhost:$PORT2/repo.db');" >/dev/null 2>&1
  result=$("$DOLTLITE" "$TMP/src.db" "SELECT dolt_push('plainsrv','main');" 2>&1)
  check "plaintext push to a plaintext server still works" "0" "$result"
else
  echo "  SKIP: plaintext server did not start"
fi
kill "$SRV2_PID" 2>/dev/null

echo "=== 6. Multi-chunk clone integrity (batched /get-chunks) ==="
# Enough rows with blob payloads to span many chunks, so the pull fans out
# across the batched download path rather than a single chunk. A fresh repo
# path avoids the section-1 history.
BIGURL="https://localhost:$PORT/bigrepo.db"
"$DOLTLITE" "$TMP/big.db" <<ENDSQL >/dev/null 2>&1
CREATE TABLE blobs(id INTEGER PRIMARY KEY, payload BLOB);
WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<2000)
  INSERT INTO blobs SELECT i, randomblob(400) FROM c;
SELECT dolt_commit('-A','-m','big');
SELECT dolt_remote('add','bigorigin','$BIGURL');
ENDSQL
src_hash=$("$DOLTLITE" "$TMP/big.db" "SELECT dolt_hashof_table('blobs');" 2>&1)
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/big.db" "SELECT dolt_push('bigorigin','main');" 2>&1)
check "large push returns 0" "0" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/bigclone.db" "SELECT dolt_clone('$BIGURL');" 2>&1)
check "large clone returns 0" "0" "$result"
clone_rows=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/bigclone.db" "SELECT count(*) FROM blobs;" 2>&1)
check "large clone round-trips 2000 rows" "2000" "$clone_rows"
clone_hash=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/bigclone.db" "SELECT dolt_hashof_table('blobs');" 2>&1)
check "clone table hash matches source (chunks intact)" "$src_hash" "$clone_hash"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
