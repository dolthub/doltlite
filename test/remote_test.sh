#!/bin/bash
#
# Remote operations integration test for doltlite.
#
# No set -e: we check results via check() function

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

pass=0
fail=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$(echo "$expected" | head -5)|"
    echo "    actual:   |$(echo "$actual" | head -5)|"
    fail=$((fail+1))
  fi
}

echo "=== Test 1: dolt_remote add/list ==="
"$DOLTLITE" "$TMPDIR/t1.db" <<ENDSQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_remote('add','origin','$TMPDIR/remote.db');
.quit
ENDSQL
result=$("$DOLTLITE" "$TMPDIR/t1.db" "SELECT name, url FROM dolt_remotes;")
check "remote shows in list" "origin|$TMPDIR/remote.db" "$result"

echo "=== Test 2: dolt_push ==="
result=$("$DOLTLITE" "$TMPDIR/t1.db" "SELECT dolt_push('origin','main');")
check "push returns 0" "0" "$result"
result=$("$DOLTLITE" "$TMPDIR/remote.db" "SELECT * FROM t;")
check "remote has pushed data" "1|a" "$result"

echo "=== Test 3: dolt_clone ==="
printf "SELECT dolt_clone('%s');\n.quit\n" "$TMPDIR/remote.db" | "$DOLTLITE" "$TMPDIR/clone.db"
result=$("$DOLTLITE" "$TMPDIR/clone.db" "SELECT count(*) FROM t;")
check "clone has data" "1" "$result"
result=$("$DOLTLITE" "$TMPDIR/clone.db" "SELECT * FROM t;")
check "clone has data" "1|a" "$result"
result=$("$DOLTLITE" "$TMPDIR/clone.db" "SELECT active_branch();")
check "clone on main" "main" "$result"
result=$("$DOLTLITE" "$TMPDIR/clone.db" "SELECT name FROM dolt_remotes;")
check "clone has origin" "origin" "$result"

echo "=== Test 4: push from clone ==="
"$DOLTLITE" "$TMPDIR/clone.db" <<ENDSQL
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add b');
SELECT dolt_push('origin','main');
.quit
ENDSQL
result=$("$DOLTLITE" "$TMPDIR/remote.db" "SELECT * FROM t ORDER BY id;")
check "remote has clone's push" "1|a
2|b" "$result"

echo "=== Test 5: fetch ==="
result=$("$DOLTLITE" "$TMPDIR/t1.db" "SELECT dolt_fetch('origin','main');")
check "fetch returns 0" "0" "$result"
result=$("$DOLTLITE" "$TMPDIR/t1.db" "SELECT * FROM t ORDER BY id;")
check "data unchanged before pull" "1|a" "$result"

echo "=== Test 6: pull (fast-forward) ==="
result=$("$DOLTLITE" "$TMPDIR/t1.db" "SELECT dolt_pull('origin','main'); SELECT * FROM t ORDER BY id;")
check "pull fast-forwards" "0
1|a
2|b" "$result"

echo "=== Test 7: push new branch ==="
"$DOLTLITE" "$TMPDIR/t1.db" <<ENDSQL
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature commit');
SELECT dolt_push('origin','feature');
.quit
ENDSQL
result=$("$DOLTLITE" "$TMPDIR/remote.db" "SELECT dolt_checkout('feature'); SELECT * FROM t ORDER BY id;")
check "remote has feature branch" "0
1|a
2|b
3|c" "$result"

echo ""
echo "Results: $pass passed, $fail failed"
[ "$fail" -eq 0 ] && exit 0 || exit 1
