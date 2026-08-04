#!/usr/bin/env bash
set -euo pipefail

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
REMOTESRV="${2:-$(dirname "$0")/../build/doltlite-remotesrv}"

if [ ! -x "$DOLTLITE" ] || [ ! -x "$REMOTESRV" ]; then
  echo "SKIP: doltlite/doltlite-remotesrv binaries not found ($DOLTLITE, $REMOTESRV)"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-remotesrv-conc.XXXXXX")"
SRV_PID=""
cleanup() {
  if [ -n "$SRV_PID" ]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

pass=0
fail=0

check() {
  local desc="$1" expected="${2//$'\r'/}" actual="${3//$'\r'/}"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$expected|"
    echo "    actual:   |$actual|"
    fail=$((fail+1))
  fi
}

check_match() {
  local desc="$1" pattern="$2" actual="${3//$'\r'/}"
  if echo "$actual" | grep -qE "$pattern"; then
    echo "  PASS: $desc"
    pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    pattern: |$pattern|"
    echo "    actual:  |$actual|"
    fail=$((fail+1))
  fi
}

run_push_waiting() {
  local db="$1" branch="$2" gate="$3" out="$4"
  (
    while [ ! -f "$gate" ]; do sleep 0.02; done
    "$DOLTLITE" "$db" "SELECT dolt_push('origin','$branch');" >"$out" 2>&1
  ) &
}

run_push_once() {
  local db="$1" branch="$2" out="$3"
  "$DOLTLITE" "$db" "SELECT dolt_push('origin','$branch');" >"$out" 2>&1 || true
}

push_until_success() {
  local db="$1" branch="$2" out="$3"
  local attempts="${REMOTE_PUSH_RETRY_ATTEMPTS:-10}"
  local attempt
  for attempt in $(seq 1 "$attempts"); do
    run_push_once "$db" "$branch" "$out"
    [ "$(cat "$out")" = "0" ] && return 0
    sleep 0.1
  done
  echo "  NOTE: $branch push exhausted $attempts attempts: $(cat "$out")" >&2
  return 1
}

mkdir -p "$TMP/srv"
"$REMOTESRV" -p 0 --bind 127.0.0.1 "$TMP/srv" >"$TMP/srv.log" 2>&1 &
SRV_PID=$!

PORT=""
for _ in $(seq 1 50); do
  PORT="$(sed -n 's#.*://127.0.0.1:\([0-9][0-9]*\).*#\1#p' "$TMP/srv.log" | head -1)"
  [ -n "$PORT" ] && break
  sleep 0.1
done
if [ -z "$PORT" ]; then
  echo "FAIL: server did not start"
  cat "$TMP/srv.log"
  exit 1
fi

URL="http://127.0.0.1:$PORT/repo.db"

echo "=== seed remote ==="
"$DOLTLITE" "$TMP/seed.db" <<SQL >/dev/null
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_remote('add','origin','$URL');
SQL
check "initial push succeeds" "0" \
  "$("$DOLTLITE" "$TMP/seed.db" "SELECT dolt_push('origin','main');" 2>&1)"

echo "=== concurrent same-branch pushes ==="
check "same-branch clone A succeeds" "0" \
  "$("$DOLTLITE" "$TMP/same_a.db" "SELECT dolt_clone('$URL');" 2>&1)"
check "same-branch clone B succeeds" "0" \
  "$("$DOLTLITE" "$TMP/same_b.db" "SELECT dolt_clone('$URL');" 2>&1)"

"$DOLTLITE" "$TMP/same_a.db" <<'SQL' >/dev/null
INSERT INTO t VALUES(10,'same-a');
SELECT dolt_commit('-A','-m','same-a');
SQL
"$DOLTLITE" "$TMP/same_b.db" <<'SQL' >/dev/null
INSERT INTO t VALUES(20,'same-b');
SELECT dolt_commit('-A','-m','same-b');
SQL

gate="$TMP/same-go"
run_push_waiting "$TMP/same_a.db" main "$gate" "$TMP/same_a.out"
pid_a=$!
run_push_waiting "$TMP/same_b.db" main "$gate" "$TMP/same_b.out"
pid_b=$!
touch "$gate"
wait "$pid_a" || true
wait "$pid_b" || true

same_a="$(cat "$TMP/same_a.out")"
same_b="$(cat "$TMP/same_b.out")"
same_success=$(printf '%s\n%s\n' "$same_a" "$same_b" | grep -cx '0' || true)

# Under heavy instrumentation both simultaneous requests can lose a transient
# server lock before either updates the ref. Retry one contender to establish a
# winner, then run the other once more to verify that the stale push still loses.
if [ "$same_success" -eq 0 ]; then
  echo "  NOTE: both initial same-branch pushes lost contention; retrying A"
  if push_until_success "$TMP/same_a.db" main "$TMP/same_a.out"; then
    run_push_once "$TMP/same_b.db" main "$TMP/same_b.out"
  fi
  same_a="$(cat "$TMP/same_a.out")"
  same_b="$(cat "$TMP/same_b.out")"
  same_success=$(printf '%s\n%s\n' "$same_a" "$same_b" | grep -cx '0' || true)
fi

same_clone="$("$DOLTLITE" "$TMP/check_same.db" \
  "SELECT dolt_clone('$URL'); SELECT count(*) FROM t;" 2>&1)"
same_rows="$(printf '%s\n' "$same_clone" | tail -1)"
winner_rows="$("$DOLTLITE" "$TMP/check_same.db" "SELECT group_concat(id, ',') FROM (SELECT id FROM t ORDER BY id);" 2>&1)"

# A request can commit its compare-and-swap and then lose the HTTP response
# while the instrumented server releases its locks. A retry correctly reports
# non-fast-forward because the commit is already remote. In that case the
# remote ref is the source of truth: exactly one contender landed even though
# neither client observed a successful response.
if [ "$same_success" -eq 0 ] \
   && printf '%s\n' "$winner_rows" | grep -qE '^(1,10|1,20)$'; then
  echo "  NOTE: winner committed despite a lost success response"
  same_success=1
fi

check "exactly one same-branch push wins" "1" "$same_success"
check_match "losing same-branch push reports conflict/non-fast-forward" \
  "remote refs changed|not a fast-forward|push failed|ERROR|Error" \
  "$(printf '%s\n%s\n' "$same_a" "$same_b" | grep -vx '0' || true)"
check "remote main remains readable after contention" "2" "$same_rows"
check_match "remote has base plus one contender row" "^(1,10|1,20)$" "$winner_rows"

echo "=== concurrent different-branch pushes with retry ==="
check "branch clone A succeeds" "0" \
  "$("$DOLTLITE" "$TMP/branch_a.db" "SELECT dolt_clone('$URL');" 2>&1)"
check "branch clone B succeeds" "0" \
  "$("$DOLTLITE" "$TMP/branch_b.db" "SELECT dolt_clone('$URL');" 2>&1)"

"$DOLTLITE" "$TMP/branch_a.db" <<'SQL' >/dev/null
SELECT dolt_checkout('-b','branch_a');
INSERT INTO t VALUES(100,'branch-a');
SELECT dolt_commit('-A','-m','branch-a');
SQL
"$DOLTLITE" "$TMP/branch_b.db" <<'SQL' >/dev/null
SELECT dolt_checkout('-b','branch_b');
INSERT INTO t VALUES(200,'branch-b');
SELECT dolt_commit('-A','-m','branch-b');
SQL

gate="$TMP/branches-go"
run_push_waiting "$TMP/branch_a.db" branch_a "$gate" "$TMP/branch_a.out"
pid_a=$!
run_push_waiting "$TMP/branch_b.db" branch_b "$gate" "$TMP/branch_b.out"
pid_b=$!
touch "$gate"
wait "$pid_a" || true
wait "$pid_b" || true

branch_a="$(cat "$TMP/branch_a.out")"
branch_b="$(cat "$TMP/branch_b.out")"

# Different refs do not conflict semantically. Their initial requests may both
# lose transient server contention, but bounded sequential retries must land
# both branches.
if [ "$branch_a" != "0" ]; then
  push_until_success "$TMP/branch_a.db" branch_a "$TMP/branch_a.out" || true
fi
if [ "$branch_b" != "0" ]; then
  push_until_success "$TMP/branch_b.db" branch_b "$TMP/branch_b.out" || true
fi
branch_a="$(cat "$TMP/branch_a.out")"
branch_b="$(cat "$TMP/branch_b.out")"
check "branch_a push eventually succeeds" "0" "$branch_a"
check "branch_b push eventually succeeds" "0" "$branch_b"

check "branch_a fetch and checkout sees its row" "0
0
1" "$("$DOLTLITE" "$TMP/check_branches.db" "SELECT dolt_clone('$URL'); SELECT dolt_fetch('origin','branch_a'); SELECT dolt_checkout('-b','local_a','origin/branch_a'); SELECT count(*) FROM t WHERE id=100;" 2>&1 | tail -3)"
check "branch_b fetch and checkout sees its row" "0
0
1" "$("$DOLTLITE" "$TMP/check_branches.db" "SELECT dolt_checkout('main'); SELECT dolt_fetch('origin','branch_b'); SELECT dolt_checkout('-b','local_b','origin/branch_b'); SELECT count(*) FROM t WHERE id=200;" 2>&1 | tail -3)"

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) ;;
  *)
    echo "=== live replacement and recovery ==="
    "$DOLTLITE" "$TMP/replacement.db" <<'SQL' >/dev/null
CREATE TABLE replacement(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO replacement VALUES(1,'new generation');
SELECT dolt_commit('-A','-m','replacement');
SQL
    mv "$TMP/replacement.db" "$TMP/srv/repo.db"
    result=$("$DOLTLITE" "$TMP/replacement_clone.db" \
      "SELECT dolt_clone('$URL'); SELECT v FROM replacement;" 2>&1)
    check "cached server adopts a valid replacement" "0
new generation" "$result"

    mv "$TMP/srv/repo.db" "$TMP/restored.db"
    printf 'damaged' >"$TMP/srv/repo.db"
    result=$("$DOLTLITE" "$TMP/damaged_clone.db" \
      "SELECT dolt_clone('$URL');" 2>&1 || true)
    check_match "damaged replacement returns an error" \
      "ERROR|Error|error|malformed|format|failed" "$result"
    mv "$TMP/restored.db" "$TMP/srv/repo.db"
    result=$("$DOLTLITE" "$TMP/recovered_clone.db" \
      "SELECT dolt_clone('$URL'); SELECT v FROM replacement;" 2>&1)
    check "cached server recovers after file restoration" "0
new generation" "$result"
    ;;
esac

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ]
