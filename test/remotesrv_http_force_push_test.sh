#!/usr/bin/env bash
set -uo pipefail

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"

if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-http-force.XXXXXX")"
SRV_PID=""
cleanup() {
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

DB="$DOLTLITE"
URL="http://127.0.0.1:$SRV_PORT/repo.db"
A="$TMP/a.db"
B="$TMP/b.db"

echo "=== seed remote and clone stale client ==="
"$DB" "$A" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_remote('add','origin','$URL');
SQL
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "initial push succeeds" "0" "$result"
result=$("$DB" "$B" "SELECT dolt_clone('$URL'); SELECT count(*) FROM t;" 2>&1)
check "stale client clone succeeds" "0
1" "$result"

echo "=== normal stale push rejects, force push rewrites main ==="
"$DB" "$A" <<'SQL' >/dev/null
INSERT INTO t VALUES(2,'from-a');
SELECT dolt_commit('-A','-m','advance a');
SQL
result=$("$DB" "$A" "SELECT dolt_push('origin','main');" 2>&1)
check "a-side push succeeds" "0" "$result"
a_head=$("$DB" "$A" "SELECT commit_hash FROM dolt_log LIMIT 1;")

"$DB" "$B" <<'SQL' >/dev/null
INSERT INTO t VALUES(3,'from-b-force');
SELECT dolt_commit('-A','-m','force b');
SQL
b_head=$("$DB" "$B" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$B" "SELECT dolt_push('origin','main');" 2>&1)
check_match "stale normal push rejects" "reject|fetch|behind|fast|stale|ERROR|Error|push failed" "$result"
result=$("$DB" "$TMP/clone_before_force.db" "SELECT dolt_clone('$URL'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT group_concat(v, ',') FROM t ORDER BY id;" 2>&1)
check "remote still has a-side tip before force" "0
$a_head
base,from-a" "$result"
result=$("$DB" "$B" "SELECT dolt_push('origin','main','--force');" 2>&1)
check "stale force push succeeds" "0" "$result"

echo "=== forced tip is visible and displaced tip is not branch head ==="
result=$("$DB" "$TMP/clone_after_force.db" "SELECT dolt_clone('$URL'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT group_concat(v, ',') FROM t ORDER BY id; SELECT count(*) FROM dolt_log WHERE message='advance a';" 2>&1)
check "fresh clone sees forced main only" "0
$b_head
base,from-b-force
0" "$result"

result=$("$DB" "$A" "SELECT dolt_fetch('origin','main'); SELECT dolt_checkout('-b','forced','origin/main'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT group_concat(v, ',') FROM t ORDER BY id;" 2>&1)
check "fetch updates tracking ref to forced tip" "0
0
$b_head
base,from-b-force" "$result"

echo "=== force push on non-main branch ==="
"$DB" "$A" <<'SQL' >/dev/null
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feature');
INSERT INTO t VALUES(10,'feature-a');
SELECT dolt_commit('-A','-m','feature a');
SELECT dolt_push('origin','feature');
SQL
result=$("$DB" "$TMP/feature_client.db" "SELECT dolt_clone('$URL'); SELECT dolt_fetch('origin','feature'); SELECT dolt_checkout('feature'); SELECT group_concat(v, ',') FROM t ORDER BY id;" 2>&1)
check "feature branch fetch sees original tip" "0
0
0
base,from-a,feature-a" "$result"
"$DB" "$TMP/feature_client.db/feature" <<'SQL' >/dev/null
DELETE FROM t WHERE id IN (2,10);
INSERT INTO t VALUES(11,'feature-forced');
SELECT dolt_commit('-A','-m','feature forced');
SQL
feature_head=$("$DB" "$TMP/feature_client.db/feature" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMP/feature_client.db/feature" "SELECT dolt_push('origin','feature','--force');" 2>&1)
check "feature force push succeeds" "0" "$result"
result=$("$DB" "$TMP/feature_verify.db" "SELECT dolt_clone('$URL'); SELECT dolt_fetch('origin','feature'); SELECT dolt_checkout('feature'); SELECT commit_hash FROM dolt_log LIMIT 1; SELECT group_concat(v, ',') FROM t ORDER BY id;" 2>&1)
check "feature tracking ref moves to forced tip" "0
0
0
$feature_head
base,feature-forced" "$result"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
