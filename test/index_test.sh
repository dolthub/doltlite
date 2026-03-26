#!/bin/bash
#
# Index operation tests for doltlite.
# Tests scan-based DELETE/UPDATE on tables with indexes at various scales.
# Catches the sort-key-seek scan limit bug (#164).
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

# ── 1. Scan DELETE with INTEGER PRIMARY KEY + index ──────
echo "--- 1. INTKEY + index: scan DELETE ---"
for N in 100 500 1000 5000; do
  rm -f "$TMPDIR/t.db"
  "$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); CREATE INDEX idx ON t(val); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT x,x FROM c; DELETE FROM t WHERE id%10=0; SELECT count(*) FROM t;" 2>/dev/null
  result=$(tail -1 < /dev/stdin)
done
# Can't capture inline. Redo properly:
for N in 100 500 1000 5000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); CREATE INDEX idx ON t(val); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT x,x FROM c; DELETE FROM t WHERE id%10=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "INTKEY+idx delete N=$N" "$((N - N/10))" "$result"
done

# ── 2. Scan DELETE with id PRIMARY KEY (BLOBKEY + autoindex) ─
echo ""
echo "--- 2. BLOBKEY (id PRIMARY KEY): scan DELETE ---"
for N in 100 500 1000 5000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id PRIMARY KEY, val INT); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT x,x FROM c; DELETE FROM t WHERE id%10=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "BLOBKEY delete N=$N" "$((N - N/10))" "$result"
done

# ── 3. TEXT PRIMARY KEY: scan DELETE ─────────────────────
echo ""
echo "--- 3. TEXT PK: scan DELETE ---"
for N in 100 500 1000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id TEXT PRIMARY KEY, val INT); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT 'k'||x,x FROM c; DELETE FROM t WHERE val%10=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "TEXT PK delete N=$N" "$((N - N/10))" "$result"
done

# ── 4. Scan UPDATE with index ────────────────────────────
echo ""
echo "--- 4. INTKEY + index: scan UPDATE ---"
for N in 100 500 1000 5000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); CREATE INDEX idx ON t(val); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT x,x FROM c; UPDATE t SET val=val+1 WHERE id%2=0; SELECT count(*) FROM t WHERE val%2=1;" 2>/dev/null | tail -1)
  # After update: even ids have odd vals, odd ids have odd vals → all odd
  check "INTKEY+idx update N=$N" "$N" "$result"
done

# ── 5. Multiple indexes ──────────────────────────────────
echo ""
echo "--- 5. Multiple indexes: scan DELETE ---"
for N in 100 500 1000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT); CREATE INDEX idx_a ON t(a); CREATE INDEX idx_b ON t(b); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT x,x,'v'||x FROM c; DELETE FROM t WHERE id%10=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "multi-idx delete N=$N" "$((N - N/10))" "$result"
done

# ── 6. Index + dolt_commit + DELETE ──────────────────────
echo ""
echo "--- 6. Committed index: scan DELETE ---"
rm -f "$TMPDIR/t.db"
"$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); CREATE INDEX idx ON t(val); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1000) INSERT INTO t SELECT x,x FROM c; SELECT dolt_add('-A'); SELECT dolt_commit('-m','data');" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/t.db" "DELETE FROM t WHERE id%10=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
check "committed idx delete" "900" "$result"

# ── 7. Index + DELETE + verify index integrity ───────────
echo ""
echo "--- 7. Index integrity after DELETE ---"
rm -f "$TMPDIR/t.db"
"$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); CREATE INDEX idx ON t(val); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1000) INSERT INTO t SELECT x,x FROM c; DELETE FROM t WHERE id%10=0;" > /dev/null 2>&1
# Index lookup should work after delete
result=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t WHERE val=500;" 2>/dev/null)
check "idx lookup after delete" "1" "$result"
result=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t WHERE val=10;" 2>/dev/null)
check "deleted val not in idx" "0" "$result"

# ── 8. Create index AFTER data + update ──────────────────
echo ""
echo "--- 8. CREATE INDEX after data ---"
rm -f "$TMPDIR/t.db"
"$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1000) INSERT INTO t SELECT x,x FROM c; SELECT dolt_add('-A'); SELECT dolt_commit('-m','data');" > /dev/null 2>&1
"$DB" "$TMPDIR/t.db" "UPDATE t SET val=val+1 WHERE id%2=0; SELECT dolt_add('-A'); SELECT dolt_commit('-m','update');" > /dev/null 2>&1
"$DB" "$TMPDIR/t.db" "CREATE INDEX idx ON t(val);" > /dev/null 2>&1
result=$("$DB" "$TMPDIR/t.db" "SELECT count(*) FROM t;" 2>/dev/null)
check "count after idx on updated table" "1000" "$result"

# ── 9. Delete percentage sweep ───────────────────────────
echo ""
echo "--- 9. Delete percentages (INTKEY+idx, N=2000) ---"
for pct in 1 10 25 50 90; do
  rm -f "$TMPDIR/t.db"
  mod=$((100/pct))
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); CREATE INDEX idx ON t(val); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<2000) INSERT INTO t SELECT x,x FROM c; DELETE FROM t WHERE id%$mod=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  expected=$((2000 - 2000/mod))
  check "delete ${pct}%" "$expected" "$result"
done

# ── 10. No index baseline (should always work) ───────────
echo ""
echo "--- 10. No index baseline ---"
for N in 100 1000 5000; do
  rm -f "$TMPDIR/t.db"
  result=$("$DB" "$TMPDIR/t.db" "CREATE TABLE t(id INTEGER PRIMARY KEY, val INT); WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<$N) INSERT INTO t SELECT x,x FROM c; DELETE FROM t WHERE id%10=0; SELECT count(*) FROM t;" 2>/dev/null | tail -1)
  check "no-idx delete N=$N" "$((N - N/10))" "$result"
done

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
