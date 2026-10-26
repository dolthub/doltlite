#!/bin/bash
set -u

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"
HERE=$(cd "$(dirname "$0")/.." && pwd)
. "$(dirname "$0")/lib/openssl_test_cert.sh"
CC="${CC:-cc}"
TMP=$(mktemp -d)
SRV_PID=""
PRIVATE_PID=""
PUBLIC_PID=""
cleanup() {
  if [ -n "$PUBLIC_PID" ]; then
    kill "$PUBLIC_PID" 2>/dev/null || true
    wait "$PUBLIC_PID" 2>/dev/null || true
  fi
  if [ -n "$PRIVATE_PID" ]; then
    kill "$PRIVATE_PID" 2>/dev/null || true
    wait "$PRIVATE_PID" 2>/dev/null || true
    PRIVATE_PID=""
  fi
  if [ -n "$SRV_PID" ]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    SRV_PID=""
  fi
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
  tls_unavailable "openssl not available"
fi
if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"; exit 0
fi

openssl_test_cert "$TMP/key.pem" "$TMP/cert.pem" "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  >/dev/null 2>&1 || tls_unavailable "openssl cert generation failed"
openssl_test_cert /dev/null "$TMP/other_ca.pem" "/CN=other" >/dev/null 2>&1

mkdir -p "$TMP/cc" "$TMP/cc_unauth" "$TMP/empty" "$TMP/authkeys" \
  "$TMP/private_authkeys" "$TMP/srv" "$TMP/srv2"
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/throwaway.db" "SELECT dolt_creds_new();" >/dev/null 2>&1
private_jwk=$(find "$TMP/cc" -maxdepth 1 -name '*.jwk' -print | head -1)
[ -n "$private_jwk" ] || { echo "FAIL: no credential generated"; exit 1; }
kid=$(basename "$private_jwk" .jwk)
cp "$private_jwk" "$TMP/private_authkeys/"
"$REMOTESRV" --cert "$TMP/cert.pem" --key "$TMP/key.pem" \
  --auth-keys "$TMP/private_authkeys" --audience localhost \
  -p 0 --bind 127.0.0.1 "$TMP/srv" >"$TMP/private.log" 2>&1 &
PRIVATE_PID=$!
# A loaded runner can take a while to reach the check; wait for the refusal
# rather than assuming it lands within a fixed delay.
for _ in $(seq 1 100); do
  kill -0 "$PRIVATE_PID" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$PRIVATE_PID" 2>/dev/null; then
  kill "$PRIVATE_PID" 2>/dev/null || true
  wait "$PRIVATE_PID" 2>/dev/null || true
  echo "FAIL: server accepted a private credential in --auth-keys"
  exit 1
fi
private_rc=0
wait "$PRIVATE_PID" 2>/dev/null || private_rc=$?
PRIVATE_PID=""
if [ "$private_rc" -eq 0 ] \
   || ! grep -q "must contain only public JWK" "$TMP/private.log"; then
  echo "FAIL: server did not refuse the private credential in --auth-keys"
  cat "$TMP/private.log"
  exit 1
fi
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT dolt_creds('export','$kid','$TMP/authkeys');" >/dev/null 2>&1 || {
  echo "FAIL: public credential export failed"; exit 1;
}
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
kill "$SRV2_PID" 2>/dev/null || true
wait "$SRV2_PID" 2>/dev/null || true

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

echo "=== 7. Authorization header names are case-insensitive and exact ==="
DOLTLITE_EXTRA_LIBS=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DOLTLITE_EXTRA_LIBS="-lws2_32 -lbcrypt -lcrypt32";; esac
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

tls_request() {
  local header_name="$1"
  printf 'GET /repo.db/refs HTTP/1.1\r\nHost: localhost\r\n%s: Bearer %s\r\nConnection: close\r\n\r\n' \
    "$header_name" "$token" | \
    openssl s_client -quiet -connect "localhost:$PORT" -servername localhost \
      -CAfile "$TMP/cert.pem" 2>/dev/null || true
}

result=$(tls_request "aUtHoRiZaTiOn" | sed -n '1s/.* \([0-9][0-9][0-9]\) .*/\1/p')
check "mixed-case Authorization is accepted" "200" "$result"
result=$(tls_request "X-Authorization" | sed -n '1s/.* \([0-9][0-9][0-9]\) .*/\1/p')
check "X-Authorization is not treated as Authorization" "401" "$result"

echo "=== 8. Multi-key selection and credential checks ==="
for _ in $(seq 1 84); do
  printf 'SELECT dolt_creds_new();\n'
done | DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: >/dev/null || exit 1
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT count(*),sum(active),max(CASE WHEN active THEN kid END)='$kid' FROM dolt_creds_ls();")
check "85 keys preserve the original active credential" "85|1|1" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT json_extract(dolt_creds_check('origin'),'$.kid')='$kid';")
check "credential check verifies the selected key" "1" "$result"
check "credential check does not create a database" "no" \
  "$(test -e "$TMP/srv/check.db" && echo yes || echo no)"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT json_extract(dolt_creds_check('--endpoint','https://localhost:$PORT/check.db'),'$.can_write');")
check "credential check works before first push" "1" "$result"
check "credential check leaves nonexistent repository absent" "no" \
  "$(test -e "$TMP/srv/check.db" && echo yes || echo no)"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_remote('add','multi','https://localhost:$PORT/multi.db'); SELECT dolt_push('multi','main');")
check "push authenticates with 85 local keys" $'0\n0' "$result"
other=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT kid FROM dolt_creds_ls() WHERE NOT active ORDER BY kid LIMIT 1;")
other=${other//$'\r'/}
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_use('$other'); SELECT dolt_creds_check('origin');" 2>&1)
check_ne "unregistered selected key fails authentication" "0" "$result"
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_bind('origin','$kid');" >/dev/null || exit 1
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT json_extract(dolt_creds_check('origin'),'$.kid')='$kid';")
check "remote binding overrides global default" "1" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" DOLTLITE_CREDS_KID=missing \
  "$DOLTLITE" "$TMP/src.db" "SELECT dolt_creds_check('origin');" 2>&1)
case "$result" in *"selected credential is missing"*) check "invalid env selection fails locally" yes yes;;
  *) check "invalid env selection fails locally" "selected credential error" "$result";; esac
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" DOLTLITE_CREDS_KID="$other" \
  "$DOLTLITE" "$TMP/src.db" "SELECT dolt_creds_check('origin');" 2>&1)
check_ne "env override takes precedence over binding" "0" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" DOLTLITE_CREDS_KID=missing \
  "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_use('$kid','session'); SELECT json_extract(dolt_creds_check('origin'),'$.kid')='$kid';")
check "session selection overrides invalid env selection" $'0\n1' "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_check('--endpoint','$URL','--creds','$other');" 2>&1)
check_ne "explicit check key overrides remote binding" "0" "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_use('$other','session'); SELECT dolt_creds_check('origin');" 2>&1)
check_ne "session selection overrides remote binding" $'0\n0' "$result"
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: "SELECT dolt_creds_use('$kid');" >/dev/null || exit 1

mkdir -p "$TMP/ambiguous"
cp "$TMP/cc/$kid.jwk" "$TMP/cc/$other.jwk" "$TMP/ambiguous/"
result=$(DOLTLITE_CREDS_DIR="$TMP/ambiguous" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_push('origin','main');" 2>&1)
case "$result" in *"multiple credentials found"*) check "legacy ambiguous push fails locally" yes yes;;
  *) check "legacy ambiguous push fails locally" "multiple credentials found" "$result";; esac
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/lazy.db" \
  "SELECT dolt_creds_bind('$URL','$kid'); SELECT dolt_clone('--lazy','$URL');" >"$TMP/lazy.out" 2>&1
if [ "$?" -eq 0 ]; then
  result=$(DOLTLITE_CREDS_DIR="$TMP/cc" \
    "$DOLTLITE" "file:$TMP/lazy.db?lazy_origin=1" \
    "SELECT dolt_creds_use('$kid','session'); SELECT dolt_creds_bind('origin','$other'); SELECT count(*) FROM users; SELECT dolt_creds_bind('origin','$kid');")
  check "lazy reads use the connection credential" $'0\n0\n3\n0' "$result"
else
  check "lazy clone with a remote binding succeeds" "success" "$(cat "$TMP/lazy.out")"
fi

echo "=== 9. Concurrent bindings and explicit anonymous access ==="
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: "SELECT dolt_creds_use('$other');" >/dev/null || exit 1
pids=""
for i in $(seq 1 8); do
  DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
    "SELECT dolt_creds_bind('https://localhost:$PORT/concurrent$i.db','$kid');" >/dev/null &
  pids="$pids $!"
done
bind_rc=0
for pid in $pids; do wait "$pid" || bind_rc=1; done
check "concurrent binding updates succeed" "0" "$bind_rc"
for i in $(seq 1 8); do
  result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
    "SELECT json_extract(dolt_creds_check('--endpoint','https://localhost:$PORT/concurrent$i.db'),'$.kid')='$kid';")
  check "concurrent binding $i survives" "1" "$result"
done
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT dolt_creds_bind('https://LOCALHOST:$PORT/repo.db/','$kid');" >/dev/null || exit 1
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT json_extract(dolt_creds_check('origin'),'$.kid')='$kid';")
check "binding URL normalization preserves host and path identity" "1" "$result"
DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: "SELECT dolt_creds_use('$kid');" >/dev/null || exit 1

mkdir -p "$TMP/public_srv" "$TMP/anon" "$TMP/reader"
"$REMOTESRV" --cert "$TMP/cert.pem" --key "$TMP/key.pem" \
  -p 0 --bind 127.0.0.1 "$TMP/public_srv" >"$TMP/public.log" 2>&1 &
PUBLIC_PID=$!
PUBLIC_PORT=""
for _ in $(seq 1 50); do
  PUBLIC_PORT=$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/public.log" | head -1)
  [ -n "$PUBLIC_PORT" ] && break
  sleep 0.1
done
if [ -z "$PUBLIC_PORT" ]; then echo "FAIL: public HTTPS server did not start"; exit 1; fi
PUBLIC_URL="https://localhost:$PUBLIC_PORT/public.db"
"$DOLTLITE" "$TMP/src.db" "SELECT dolt_remote('add','public','$PUBLIC_URL');" >/dev/null || exit 1
result=$(DOLTLITE_CREDS_DIR="$TMP/anon" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_push('public','main');" 2>&1)
case "$result" in *"no credential selected"*) check "HTTPS writes require an identity or explicit anonymous selection" yes yes;;
  *) check "HTTPS writes require an identity or explicit anonymous selection" "no credential selected" "$result";; esac
check "refused anonymous write creates no database" no \
  "$(test -e "$TMP/public_srv/public.db" && echo yes || echo no)"
result=$(DOLTLITE_CREDS_DIR="$TMP/anon" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_use('anonymous'); SELECT dolt_push('public','main');")
check "explicit anonymous HTTPS push remains available" $'0\n0' "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/reader" "$DOLTLITE" "$TMP/public_clone.db" \
  "SELECT dolt_clone('$PUBLIC_URL'); SELECT count(*) FROM users;")
check "public HTTPS clone works without credentials" $'0\n3' "$result"
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/src.db" \
  "SELECT dolt_creds_check('public');" 2>&1)
case "$result" in *"does not support credential checks"*) check "auth-disabled server does not claim verification" yes yes;;
  *) check "auth-disabled server does not claim verification" "unsupported credential checks" "$result";; esac
result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
  "SELECT dolt_creds_check('--endpoint','http://localhost:$PUBLIC_PORT/public.db');" 2>&1)
case "$result" in *"require HTTPS"*) check "credential check refuses plaintext" yes yes;;
  *) check "credential check refuses plaintext" "HTTPS required" "$result";; esac

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
