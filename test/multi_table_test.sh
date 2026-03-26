#!/bin/bash
#
# Multi-table large-scale test.
# Tests 3 tables with 100K+ rows each, indexes, joins, diffs, merges, clones.
#
# Usage: multi_table_test.sh [doltlite-binary] [--quick]
#

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

QUICK=0
if [ "$2" = "--quick" ]; then QUICK=1; fi

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

ts() { date +%s; }

if [ "$QUICK" = "1" ]; then
  NU=1000; NO=5000; NE=10000
  echo "=== QUICK: ${NU} users, ${NO} orders, ${NE} events ==="
else
  NU=10000; NO=50000; NE=100000
  echo "=== FULL: ${NU} users, ${NO} orders, ${NE} events ==="
fi

# ── 1. Create schema and load data ──────────────────────
echo ""
echo "--- 1. Create 3 tables and load data ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<ENDSQL
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, status TEXT DEFAULT 'active');
CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT, quantity INTEGER, price REAL, status TEXT);
CREATE TABLE events(id INTEGER PRIMARY KEY, user_id INTEGER, event_type TEXT, detail TEXT, ts INTEGER);

WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${NU})
INSERT INTO users SELECT x, 'user_'||x, 'user_'||x||'@test.com', 18+(x%60), 'active' FROM c;

WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${NO})
INSERT INTO orders SELECT x, 1+(x%${NU}),
  CASE x%5 WHEN 0 THEN 'Widget' WHEN 1 THEN 'Gadget' WHEN 2 THEN 'Doohickey' WHEN 3 THEN 'Thingamajig' ELSE 'Gizmo' END,
  1+(x%10), round(9.99+(x%100)*0.5,2),
  CASE x%3 WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped' ELSE 'delivered' END
FROM c;

WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${NE})
INSERT INTO events SELECT x, 1+(x%${NU}),
  CASE x%4 WHEN 0 THEN 'click' WHEN 1 THEN 'purchase' WHEN 2 THEN 'login' ELSE 'view' END,
  'event_'||x, 1700000000+x
FROM c;

SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial load');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM users;")
check "users count" "$NU" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM orders;")
check "orders count" "$NO" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM events;")
check "events count" "$NE" "$result"

# ── 2. Cross-table queries ──────────────────────────────
echo ""
echo "--- 2. Cross-table queries ---"
t0=$(ts)
result=$("$DB" "$TMPDIR/db" "SELECT count(DISTINCT product) FROM orders;")
check "5 products" "5" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT count(DISTINCT event_type) FROM events;")
check "4 event types" "4" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM orders WHERE user_id=42;")
check "orders for user 42" "1" "$([ "$result" -gt 0 ] && echo 1 || echo 0)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM orders o JOIN users u ON o.user_id=u.id WHERE u.age>50;")
check "join: orders for older users" "1" "$([ "$result" -gt 0 ] && echo 1 || echo 0)"

result=$("$DB" "$TMPDIR/db" "SELECT avg(price) > 0 FROM orders;")
check "avg price positive" "1" "$result"
echo "  ($(( $(ts) - t0 ))s)"

# ── 3. Bulk update across tables ────────────────────────
echo ""
echo "--- 3. Bulk update + commit ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<'ENDSQL'
UPDATE orders SET status='shipped' WHERE id % 2 = 0;
UPDATE users SET status='inactive' WHERE id % 100 = 0;
DELETE FROM events WHERE id % 10 = 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','bulk update');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM users WHERE status='inactive';")
check "1% users inactive" "$((NU/100))" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM events;")
check "90% events remain" "$((NE - NE/10))" "$result"

# ── 4. Diff across tables ──────────────────────────────
echo ""
echo "--- 4. Diff ---"
t0=$(ts)
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_diff_orders WHERE diff_type='modified';")
check "diff: orders modified" "1" "$([ "$result" -gt 0 ] && echo 1 || echo 0)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_diff_events WHERE diff_type='removed';")
check "diff: events removed" "$((NE/10))" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dolt_diff_users WHERE diff_type='modified';")
check "diff: users modified" "$((NU/100))" "$result"
echo "  ($(( $(ts) - t0 ))s)"

# ── 5. Branch + merge with multi-table changes ─────────
echo ""
echo "--- 7. Branch + merge ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<ENDSQL
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO users VALUES($((NU+1)), 'feature_user', 'feat@test.com', 30, 'active');
INSERT INTO orders VALUES($((NO+1)), $((NU+1)), 'NewProduct', 1, 49.99, 'pending');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature: new user + order');
SELECT dolt_checkout('main');
UPDATE users SET age=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main: update user 1');
SELECT dolt_merge('feature');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM users;")
check "merged: users count" "$((NU+1))" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM orders;")
check "merged: orders count" "$((NO+1))" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT age FROM users WHERE id=1;")
check "merged: main change kept" "99" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT product FROM orders WHERE id=$((NO+1));")
check "merged: feature order present" "NewProduct" "$result"

# ── 8. Clone multi-table database ──────────────────────
echo ""
echo "--- 8. Clone ---"
t0=$(ts)
"$DB" "$TMPDIR/db" "SELECT dolt_remote('add','origin','file://$TMPDIR/remote'); SELECT dolt_push('origin','main');" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/clone" "SELECT dolt_clone('file://$TMPDIR/remote');")
check "clone ok" "0" "$result"
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/clone" "SELECT count(*) FROM users;")
check "clone: users" "$((NU+1))" "$result"
result=$("$DB" "$TMPDIR/clone" "SELECT count(*) FROM orders;")
check "clone: orders" "$((NO+1))" "$result"
result=$("$DB" "$TMPDIR/clone" "SELECT count(*) FROM events;")
check "clone: events" "$((NE - NE/10))" "$result"

# ── 9. Schema change (ALTER TABLE) ─────────────────────
echo ""
echo "--- 9. Schema change ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<'ENDSQL'
ALTER TABLE users ADD COLUMN phone TEXT;
UPDATE users SET phone='555-'||id WHERE id<=100;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add phone column');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT phone FROM users WHERE id=42;")
check "phone column populated" "555-42" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT phone IS NULL FROM users WHERE id=500;")
check "phone null for unset rows" "1" "$result"

# ── 10. Push schema change + pull ──────────────────────
echo ""
echo "--- 10. Push schema change + pull ---"
t0=$(ts)
"$DB" "$TMPDIR/db" "SELECT dolt_push('origin','main');" > /dev/null 2>&1
"$DB" "$TMPDIR/clone" "SELECT dolt_pull('origin','main');" > /dev/null 2>&1
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/clone" "SELECT phone FROM users WHERE id=42;")
check "clone pulled schema change" "555-42" "$result"

# ── 11. Index creation (after merge, on final data) ────
echo ""
echo "--- 11. Index creation ---"
t0=$(ts)
"$DB" "$TMPDIR/db" <<'ENDSQL'
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_events_user ON events(user_id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add indexes');
.quit
ENDSQL
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%';")
check "2 indexes created" "2" "$result"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM users;")
check "users intact after index" "$((NU+1))" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM orders;")
check "orders intact after index" "$((NO+1))" "$result"

# ── 12. 20 tables ──────────────────────────────────────
echo ""
echo "--- 11. 20 dimension tables ---"
t0=$(ts)
for i in $(seq 1 20); do
  "$DB" "$TMPDIR/db" "CREATE TABLE dim_${i}(id INTEGER PRIMARY KEY, val TEXT, cat INTEGER); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500) INSERT INTO dim_${i} SELECT x,'dim${i}_'||x,x%10 FROM c;" > /dev/null 2>&1
done
"$DB" "$TMPDIR/db" "SELECT dolt_add('-A'); SELECT dolt_commit('-m','20 dim tables');" > /dev/null 2>&1
echo "  ($(( $(ts) - t0 ))s)"

result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'dim_%';")
check "20 dim tables" "20" "$result"
result=$("$DB" "$TMPDIR/db" "SELECT count(*) FROM dim_15;")
check "dim tables have 500 rows" "500" "$result"

# ── 12. File size ──────────────────────────────────────
echo ""
db_size=$(stat -f%z "$TMPDIR/db" 2>/dev/null || stat -c%s "$TMPDIR/db" 2>/dev/null)
db_mb=$((db_size / 1048576))
echo "  Database: ${db_mb}MB"
total=$((NU + NO + NE))
echo "  Rows: ~${total} across 23+ tables"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
