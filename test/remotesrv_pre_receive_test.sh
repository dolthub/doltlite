#!/usr/bin/env bash
# doltlite-remotesrv --pre-receive: the program is asked before a push moves
# a branch or tag, sees the ref with its old and new commit under the store
# lock, and anything but exit status 0 refuses the push and leaves the ref
# where it was.
set -uo pipefail
. "$(dirname "$0")/lib/stop_process.sh"
. "$(dirname "$0")/lib/openssl_test_cert.sh"

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"

if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"
  exit 0
fi
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    out=$("$REMOTESRV" --pre-receive hook.cmd . 2>&1); rc=$?
    if [ "$rc" != 0 ] && echo "$out" | grep -q "not supported on Windows"; then
      echo "  PASS: --pre-receive is refused at startup on Windows"
      echo "Results: 1 passed, 0 failed"
      exit 0
    fi
    echo "  FAIL: --pre-receive on Windows: rc=$rc $out"
    echo "Results: 0 passed, 1 failed"
    exit 1
    ;;
esac
DOLTLITE="$(cd "$(dirname "$DOLTLITE")" && pwd)/$(basename "$DOLTLITE")"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-pre-receive.XXXXXX")"
SRV_PID=""
stop_server() {
  [ -n "$SRV_PID" ] && { stop_process "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; }
  SRV_PID=""
}
cleanup() {
  stop_server
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

# The hook appends what it was told to hook.log, then applies the policy
# named in $TMP/policy.
HOOK="$TMP/hook.sh"
cat >"$HOOK" <<HOOKEOF
#!/bin/sh
echo "db=\$DOLTLITE_DB path=\$DOLTLITE_DB_PATH ref=\$DOLTLITE_REF branch=\$DOLTLITE_BRANCH tag=\$DOLTLITE_TAG old=\$DOLTLITE_OLD new=\$DOLTLITE_NEW force=\$DOLTLITE_FORCE kid=\$DOLTLITE_KID" >>"$TMP/hook.log"
case "\$(cat "$TMP/policy" 2>/dev/null)" in
  no-force-main)
    if [ "\$DOLTLITE_BRANCH" = main ] && [ "\$DOLTLITE_FORCE" = 1 ]; then
      echo "main cannot be force-pushed"
      echo "second line is not sent"
      exit 1
    fi ;;
  no-feat)
    [ "\$DOLTLITE_BRANCH" = feat ] && { echo 'feat is "closed" \\ here'; exit 3; } ;;
  no-tags)
    [ -n "\$DOLTLITE_TAG" ] && { echo "tags are frozen"; exit 1; } ;;
  no-delete)
    [ -z "\$DOLTLITE_NEW" ] && { echo "deleting \$DOLTLITE_REF is not allowed"; exit 1; } ;;
  silent) exit 9 ;;
  slow) sleep 30 ;;
  crash) kill -9 \$\$ ;;
  read-new)
    "$DOLTLITE" -readonly "\$DOLTLITE_DB_PATH" \\
      "SELECT 'msg=' || message FROM dolt_log('\$DOLTLITE_NEW') LIMIT 1;" \\
      >>"$TMP/read.log" 2>&1
    tail -1 "$TMP/read.log" | grep -q "^msg=allowed" || { echo "commit message not allowed"; exit 1; } ;;
esac
exit 0
HOOKEOF
chmod +x "$HOOK"

start_server() {  # start_server <log> <args...>
  local log="$1"; shift
  stop_server
  mkdir -p "$TMP/srv"
  "$REMOTESRV" -p 0 --bind 127.0.0.1 "$@" "$TMP/srv" >"$log" 2>&1 &
  SRV_PID=$!
  SRV_PORT=""
  for _ in $(seq 1 50); do
    SRV_PORT="$(sed -n 's#.*://[a-z0-9.]*:\([0-9][0-9]*\).*#\1#p' "$log" | head -1)"
    [ -n "$SRV_PORT" ] && return
    sleep 0.1
  done
  echo "FAIL: server did not start"; cat "$log"; exit 1
}
last_hook() { tail -1 "$TMP/hook.log" 2>/dev/null; }
head_of() { "$DOLTLITE" "$1" "SELECT dolt_hashof('${2:-main}');" 2>&1; }
remote_head() {  # remote_head <branch>: the branch as a fresh clone sees it
  local h
  rm -f "$TMP/probe.db"
  "$DOLTLITE" "$TMP/probe.db" "SELECT dolt_clone('$URL');" >/dev/null 2>&1
  h=$("$DOLTLITE" "$TMP/probe.db" "SELECT dolt_hashof('origin/$1');" 2>/dev/null)
  if echo "$h" | grep -qE '^[0-9a-f]{40}$'; then echo "$h"; else echo absent; fi
}

start_server "$TMP/srv.log" --pre-receive "$HOOK"
URL="http://127.0.0.1:$SRV_PORT/repo.db"
A="$TMP/a.db"

echo "=== 1. first push: the hook sees a new branch ==="
"$DOLTLITE" "$A" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_remote('add','origin','$URL');
SQL
c1=$(head_of "$A")
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "accepted push returns 0" "0" "$result"
check_match "hook saw the new branch" \
  "^db=repo.db path=/.*/srv/repo.db ref=refs/heads/main branch=main tag= old= new=$c1 force=0 kid=\$" \
  "$(last_hook)"
check "remote has the pushed commit" "$c1" "$(remote_head main)"

echo "=== 2. fast-forward: old and new are the replaced and the pushed commit ==="
"$DOLTLITE" "$A" "INSERT INTO t VALUES(2,'two'); SELECT dolt_commit('-A','-m','two');" >/dev/null
c2=$(head_of "$A")
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "fast-forward accepted" "0" "$result"
check_match "hook saw old and new" "branch=main tag= old=$c1 new=$c2 force=0" "$(last_hook)"

echo "=== 3. a push that changes nothing does not ask the hook ==="
n=$(wc -l <"$TMP/hook.log")
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "up-to-date push returns 0" "0" "$result"
check "hook was not run" "$n" "$(wc -l <"$TMP/hook.log")"

echo "=== 4. refused force push: message reaches the client, branch stays ==="
echo no-force-main >"$TMP/policy"
"$DOLTLITE" "$A" "SELECT dolt_reset('--hard','HEAD~1'); INSERT INTO t VALUES(3,'rewrite'); SELECT dolt_commit('-A','-m','rewrite');" >/dev/null
c3=$(head_of "$A")
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main','--force');" 2>&1)
check_match "client sees the hook's first line" "main cannot be force-pushed" "$result"
if echo "$result" | grep -q "second line"; then
  check "only the first line is sent" "absent" "present"
else
  check "only the first line is sent" "absent" "absent"
fi
check_match "hook saw the force flag" "branch=main tag= old=$c2 new=$c3 force=1" "$(last_hook)"
check "remote branch did not move" "$c2" "$(remote_head main)"

echo "=== 5. the same policy lets an ordinary push through ==="
"$DOLTLITE" "$A" "SELECT dolt_reset('--hard','$c2'); INSERT INTO t VALUES(4,'four'); SELECT dolt_commit('-A','-m','four');" >/dev/null
c4=$(head_of "$A")
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "non-force push accepted" "0" "$result"
check "remote advanced" "$c4" "$(remote_head main)"

echo "=== 6. per-branch policy; the message is made safe for the reply ==="
echo no-feat >"$TMP/policy"
"$DOLTLITE" "$A" "SELECT dolt_branch('feat'); SELECT dolt_branch('ok');" >/dev/null
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','feat');" 2>&1)
check_match "refused branch carries the message" "feat is 'closed' / here" "$result"
check "feat is absent on the remote" "absent" "$(remote_head feat)"
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','ok');" 2>&1)
check "another branch is accepted" "0" "$result"
check "other branch is on the remote" "$c4" "$(remote_head ok)"

echo "=== 7. tags ==="
echo no-tags >"$TMP/policy"
"$DOLTLITE" "$A" "SELECT dolt_tag('v1');" >/dev/null
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','--tags');" 2>&1)
check_match "refused tag push carries the message" "tags are frozen" "$result"
check_match "hook saw the tag" "ref=refs/tags/v1 branch= tag=v1 old= new=$c4 force=[01] kid=\$" "$(last_hook)"
: >"$TMP/policy"
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','--tags');" 2>&1)
check "tag push accepted without the policy" "0" "$result"

echo "=== 8. deleting a branch: new is empty ==="
echo no-delete >"$TMP/policy"
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin',':ok');" 2>&1)
check_match "refused delete carries the message" "deleting refs/heads/ok is not allowed" "$result"
check_match "hook saw an empty new commit" "branch=ok tag= old=$c4 new= force=" "$(last_hook)"
check "branch is still on the remote" "$c4" "$(remote_head ok)"

echo "=== 9. a hook that prints nothing still refuses ==="
echo silent >"$TMP/policy"
"$DOLTLITE" "$A" "INSERT INTO t VALUES(5,'five'); SELECT dolt_commit('-A','-m','five');" >/dev/null
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "default refusal message" "push refused by the pre-receive hook" "$result"
check "remote did not move" "$c4" "$(remote_head main)"

echo "=== 10. a hook killed by a signal refuses ==="
echo crash >"$TMP/policy"
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "crashed hook refuses" "push refused by the pre-receive hook" "$result"
check "remote did not move" "$c4" "$(remote_head main)"

echo "=== 11. the hook can read the pushed commit before any ref names it ==="
echo read-new >"$TMP/policy"
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "commit with another message is refused" "commit message not allowed" "$result"
check "the hook read the pushed commit" "msg=five" "$(tail -1 "$TMP/read.log" 2>/dev/null)"
"$DOLTLITE" "$A" "SELECT dolt_commit('--amend','-m','allowed');" >/dev/null
c5=$(head_of "$A")
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "commit the policy allows is accepted" "0" "$result"
check "remote advanced" "$c5" "$(remote_head main)"

echo "=== 12. a hook that outlives the timeout refuses ==="
start_server "$TMP/srv-slow.log" --pre-receive "$HOOK" --pre-receive-timeout-ms 500
URL="http://127.0.0.1:$SRV_PORT/repo.db"
"$DOLTLITE" "$A" "SELECT dolt_remote('remove','origin'); SELECT dolt_remote('add','origin','$URL'); INSERT INTO t VALUES(6,'six'); SELECT dolt_commit('-A','-m','six');" >/dev/null
echo slow >"$TMP/policy"
start=$(date +%s)
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
elapsed=$(( $(date +%s) - start ))
check_match "timed-out hook refuses" "pre-receive hook timed out" "$result"
if [ "$elapsed" -lt 15 ]; then
  check "refusal did not wait for the hook" "fast" "fast"
else
  check "refusal did not wait for the hook" "fast" "${elapsed}s"
fi
check "remote did not move" "$c5" "$(remote_head main)"

echo "=== 13. a program that cannot be run refuses ==="
: >"$TMP/policy"
start_server "$TMP/srv-missing.log" --pre-receive "$TMP/no-such-hook"
URL="http://127.0.0.1:$SRV_PORT/repo.db"
"$DOLTLITE" "$A" "SELECT dolt_remote('remove','origin'); SELECT dolt_remote('add','origin','$URL');" >/dev/null
result=$("$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check_match "missing program refuses" "push refused by the pre-receive hook" "$result"
check "remote did not move" "$c5" "$(remote_head main)"

echo "=== 14. flag validation ==="
out=$("$REMOTESRV" --pre-receive-timeout-ms 500 "$TMP/srv" 2>&1); rc=$?
check "timeout without a program is an error" "1" "$rc"
check_match "the error names the missing flag" "requires --pre-receive" "$out"
out=$("$REMOTESRV" --pre-receive "$HOOK" --pre-receive-timeout-ms 0 "$TMP/srv" 2>&1); rc=$?
check "zero timeout is an error" "1" "$rc"

echo "=== 15. with --auth-keys the hook is told which key pushed ==="
if command -v openssl >/dev/null 2>&1 \
   && openssl_test_cert "$TMP/key.pem" "$TMP/cert.pem" "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1; then
  mkdir -p "$TMP/cc" "$TMP/authkeys"
  created=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$TMP/throwaway.db" \
    "SELECT dolt_creds_new();" 2>/dev/null)
  kid=$(printf '%s\n' "$created" | sed -n \
    's/^Created credential \([0-9a-v][0-9a-v]*\)$/\1/p' | head -1)
  DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" :memory: \
    "SELECT dolt_creds('export','$kid','$TMP/authkeys');" >/dev/null 2>&1
  start_server "$TMP/srv-auth.log" --pre-receive "$HOOK" \
    --cert "$TMP/cert.pem" --key "$TMP/key.pem" \
    --auth-keys "$TMP/authkeys" --audience localhost
  URL="https://localhost:$SRV_PORT/repo.db"
  export DOLTLITE_CA_FILE="$TMP/cert.pem"
  export DOLT_OVERRIDE_GRPC_JWT_AUDIENCE="localhost"
  "$DOLTLITE" "$A" "SELECT dolt_remote('remove','origin'); SELECT dolt_remote('add','origin','$URL');" >/dev/null
  c6=$(head_of "$A")
  result=$(DOLTLITE_CREDS_DIR="$TMP/cc" "$DOLTLITE" "$A" "SELECT dolt_push('origin','main');" 2>&1)
  check "authenticated push accepted" "0" "$result"
  check_match "hook saw the verified key id" "branch=main tag= old=$c5 new=$c6 force=0 kid=$kid\$" "$(last_hook)"
elif [ "${DOLTLITE_REQUIRE_TLS:-0}" = 1 ]; then
  check "openssl available for the key id case" "yes" "no"
else
  echo "  SKIP: openssl unavailable, cannot build a certificate"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ]
