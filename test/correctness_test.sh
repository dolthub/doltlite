#!/bin/bash
#
# Correctness regression tests for doltlite.
#
# Every test inserts known data, operates on it, and verifies exact results.
# Tests run at 100 AND 2000 rows to catch bugs that only appear when the
# prolly tree has multiple levels (typically >800 rows for small records).
#
# This suite would have caught:
#   - #164: scan-based DELETE with indexes failing at 1000+ rows
#   - #156: diff returning wrong counts
#   - #153: CREATE INDEX corrupting data
#   - The big-endian key encoding bug
#   - The canDefer ephemeral table regression
#

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

pass=0
fail=0
DB="$DOLTLITE"

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$expected|"
    echo "    actual:   |$actual|"
    fail=$((fail+1))
  fi
}

# Run a test at both small (single-level tree) and large (multi-level tree) scale
for_each_scale() {
  local name="$1"
  shift
  for N in 100 2000; do
    "$@" "$N"
  done
}

# ════════════════════════════════════════════════════════════
echo "=== Correctness Regression Tests ==="
echo ""

# ── 1. INSERT + SELECT round-trip ─────────────────────────
echo "--- 1. INSERT + SELECT round-trip ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    SELECT count(*), min(id), max(id), sum(val) FROM t;" 2>/dev/null)
  check "INTKEY round-trip N=$N" "$N|1|$N|$((N*(N+1)/2))" "$result"
done

for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x,'v'||x FROM c;
    SELECT count(*), min(id), max(id) FROM t;" 2>/dev/null)
  check "3-col round-trip N=$N" "$N|1|$N" "$result"
done

# ── 2. UPDATE preserves count, changes values ─────────────
echo ""
echo "--- 2. UPDATE correctness ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    UPDATE t SET val=val+1 WHERE id%2=0;" > /dev/null 2>&1
  count=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
  changed=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t WHERE val!=id;" 2>/dev/null)
  check "UPDATE count N=$N" "$N" "$count"
  check "UPDATE changed N=$N" "$((N/2))" "$changed"
done

# ── 3. Scan-based DELETE ──────────────────────────────────
echo ""
echo "--- 3. Scan DELETE (no index) ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    DELETE FROM t WHERE id%10=0;
    SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "scan DELETE no-idx N=$N" "$((N - N/10))" "$result"
done

# ── 4. Scan DELETE with index ─────────────────────────────
echo ""
echo "--- 4. Scan DELETE with index ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    CREATE INDEX idx ON t(val);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    DELETE FROM t WHERE id%10=0;
    SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "scan DELETE with-idx N=$N" "$((N - N/10))" "$result"
done

# ── 5. Scan DELETE with BLOBKEY (id PRIMARY KEY) ──────────
echo ""
echo "--- 5. BLOBKEY scan DELETE ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    DELETE FROM t WHERE id%10=0;
    SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "BLOBKEY DELETE N=$N" "$((N - N/10))" "$result"
done

# ── 6. Scan UPDATE with index ─────────────────────────────
echo ""
echo "--- 6. Scan UPDATE with index ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    CREATE INDEX idx ON t(val);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    UPDATE t SET val=-1 WHERE id%10=0;" > /dev/null 2>&1
  count=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
  updated=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t WHERE val=-1;" 2>/dev/null)
  check "UPDATE+idx count N=$N" "$N" "$count"
  check "UPDATE+idx changed N=$N" "$((N/10))" "$updated"
done

# ── 7. CREATE INDEX doesn't lose data ─────────────────────
echo ""
echo "--- 7. CREATE INDEX preserves data ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, status TEXT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x,CASE x%3 WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END FROM c;
    UPDATE t SET status='x' WHERE id%2=0;
    CREATE INDEX idx ON t(status);" > /dev/null 2>&1
  result=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
  check "CREATE INDEX count N=$N" "$N" "$result"
done

# ── 8. Diff returns exact counts ──────────────────────────
echo ""
echo "--- 8. Diff accuracy ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x,0 FROM c;
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
    UPDATE t SET b=1 WHERE id%2=0;
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','update');" > /dev/null 2>&1
  result=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" 2>/dev/null)
  check "diff accuracy N=$N" "$((N/2))" "$result"
done

# ── 9. CTE recursion works ────────────────────────────────
echo ""
echo "--- 9. CTE correctness ---"
rm -f "$TMPDIR/t.db"
result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE tree(id INT, name TEXT, parent INT);
  INSERT INTO tree VALUES(1,'root',0),(2,'a',1),(3,'b',1),(4,'c',2);
  WITH RECURSIVE hier(id,name,parent) AS (
    SELECT id,name,parent FROM tree WHERE parent=0
    UNION ALL
    SELECT t.id,t.name,t.parent FROM tree t JOIN hier h ON t.parent=h.id
  ) SELECT count(*) FROM hier;" 2>/dev/null)
check "CTE recursion" "4" "$result"

# ── 10. EXISTS subquery ───────────────────────────────────
echo ""
echo "--- 10. EXISTS subquery ---"
rm -f "$TMPDIR/t.db"
result=$("$DB" :memory: "CREATE TABLE users(name TEXT); CREATE TABLE orders(user_name TEXT);
  INSERT INTO users VALUES('alice'),('bob');
  INSERT INTO orders VALUES('alice');
  SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_name=users.name);" 2>/dev/null)
check "EXISTS subquery" "alice" "$result"

# ── 11. LEFT JOIN ─────────────────────────────────────────
echo ""
echo "--- 11. LEFT JOIN ---"
rm -f "$TMPDIR/t.db"
result=$("$DB" :memory: "CREATE TABLE a(x TEXT); CREATE TABLE b(x TEXT, y TEXT);
  INSERT INTO a VALUES('x'),('y'),('z');
  INSERT INTO b VALUES('x','p'),('x','q'),('y','r');
  SELECT a.x, b.y FROM a LEFT JOIN b ON a.x=b.x ORDER BY a.x, b.y;" 2>/dev/null)
expected="x|p
x|q
y|r
z|"
check "LEFT JOIN" "$expected" "$result"

# ── 12. Branch + merge preserves data ─────────────────────
echo ""
echo "--- 12. Merge correctness ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
    SELECT dolt_branch('feat');
    SELECT dolt_checkout('feat');
    INSERT INTO t VALUES($((N+1)),999);
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat');
    SELECT dolt_checkout('main');
    UPDATE t SET val=0 WHERE id=1;
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','main');
    SELECT dolt_merge('feat');" > /dev/null 2>&1
  count=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
  feat=$("$DB" "$TMPDIR/t.db" "SELECT val FROM t WHERE id=$((N+1));" 2>/dev/null)
  main=$("$DB" "$TMPDIR/t.db" "SELECT val FROM t WHERE id=1;" 2>/dev/null)
  check "merge count N=$N" "$((N+1))" "$count"
  check "merge feat row N=$N" "999" "$feat"
  check "merge main row N=$N" "0" "$main"
done

# ── 13. Clone preserves data ──────────────────────────────
echo ""
echo "--- 13. Clone correctness ---"
for N in 100 2000; do
  rm -f "$TMPDIR/src.db" "$TMPDIR/remote.db" "$TMPDIR/clone.db"
  "$DB" "$TMPDIR/src.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','data');
    SELECT dolt_remote('add','origin','file://$TMPDIR/remote.db');
    SELECT dolt_push('origin','main');" > /dev/null 2>&1
  "$DB" "$TMPDIR/clone.db" "SELECT dolt_clone('file://$TMPDIR/remote.db');" > /dev/null 2>&1
  count=$("$DB" "$TMPDIR/clone.db" "SELECT count(*) FROM t;" 2>/dev/null)
  sum=$("$DB" "$TMPDIR/clone.db" "SELECT sum(val) FROM t;" 2>/dev/null)
  check "clone count N=$N" "$N" "$count"
  check "clone sum N=$N" "$((N*(N+1)/2))" "$sum"
done

# ── 14. Committed data survives reopen ────────────────────
echo ""
echo "--- 14. Persistence ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    SELECT dolt_add('-A'); SELECT dolt_commit('-m','data');" > /dev/null 2>&1
  # Reopen in new session
  count=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
  sum=$("$DB" "$TMPDIR/t.db" "SELECT sum(val) FROM t;" 2>/dev/null)
  check "persist count N=$N" "$N" "$count"
  check "persist sum N=$N" "$((N*(N+1)/2))" "$sum"
done

# ── 15. Mixed operations in single transaction ────────────
echo ""
echo "--- 15. Mixed ops ---"
for N in 100 2000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N)
    INSERT INTO t SELECT x,x FROM c;
    UPDATE t SET val=0 WHERE id%3=0;
    DELETE FROM t WHERE id%5=0;
    INSERT INTO t VALUES($((N+1)),999);
    SELECT count(*) FROM t;" > /dev/null 2>&1
  # N rows - N/5 deleted + 1 inserted = N - N/5 + 1
  expected=$((N - N/5 + 1))
  count=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
  check "mixed ops count N=$N" "$expected" "$count"
done

# ════════════════════════════════════════════════════════════
echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
