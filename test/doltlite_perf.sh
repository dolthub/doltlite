#!/bin/bash
#
# Performance tests for Doltlite.
#
# Tests that point operations (SELECT, UPDATE, DELETE) scale as O(log n)
# with table size, and that dolt_diff after a single-row update is
# approximately constant time regardless of table size.
#
# Sizes: 1K, 3K, 10K (larger sizes need the bulk insert perf fix).
# Assertions use generous ratios to avoid flaky CI.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

echo "=== Doltlite Performance Tests ==="
echo ""

time_ms() {
  local start end
  start=$(python3 -c 'import time; print(int(time.time()*1000))')
  eval "$@" > /dev/null 2>&1
  end=$(python3 -c 'import time; print(int(time.time()*1000))')
  echo $((end - start))
}

assert_ratio() {
  local name="$1" small="$2" large="$3" max_ratio="$4"
  if [ "$small" -le 0 ]; then small=1; fi
  local ratio=$((large * 100 / small))
  local limit=$((max_ratio * 100))
  if [ "$ratio" -le "$limit" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name — ${small}ms → ${large}ms (${ratio}%/${limit}%)"
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  ${small}ms → ${large}ms (ratio ${ratio}% > limit ${limit}%)"
    echo "  FAIL: $name — ${small}ms → ${large}ms (${ratio}%/${limit}%)"
  fi
}

# ============================================================
# Setup: create committed tables at each size
# ============================================================

echo "Setting up databases..."

for SIZE in 1000 3000 10000; do
  DB="/tmp/perf_${SIZE}_$$.db"; rm -f "$DB"
  python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('BEGIN;')
for i in range($SIZE):
    print(f'INSERT INTO t VALUES({i}, \"row_{i}\");')
print('COMMIT;')
print(\"SELECT dolt_commit('-A','-m','init');\")
" | $DOLTLITE "$DB" > /dev/null 2>&1
  echo "  ${SIZE} rows: done"
done

DB_1K="/tmp/perf_1000_$$.db"
DB_3K="/tmp/perf_3000_$$.db"
DB_10K="/tmp/perf_10000_$$.db"

# ============================================================
# Point SELECT by PK (O(log n))
# ============================================================

echo ""
echo "--- Point SELECT (100 iterations) ---"

T_SEL_1K=$(time_ms "for i in \$(seq 1 100); do echo 'SELECT v FROM t WHERE id=500;' | $DOLTLITE '$DB_1K'; done")
echo "  1K: ${T_SEL_1K}ms"

T_SEL_3K=$(time_ms "for i in \$(seq 1 100); do echo 'SELECT v FROM t WHERE id=1500;' | $DOLTLITE '$DB_3K'; done")
echo "  3K: ${T_SEL_3K}ms"

T_SEL_10K=$(time_ms "for i in \$(seq 1 100); do echo 'SELECT v FROM t WHERE id=5000;' | $DOLTLITE '$DB_10K'; done")
echo "  10K: ${T_SEL_10K}ms"

assert_ratio "select_1k_to_3k" "$T_SEL_1K" "$T_SEL_3K" 5
assert_ratio "select_3k_to_10k" "$T_SEL_3K" "$T_SEL_10K" 5

# ============================================================
# Single-row UPDATE (O(log n))
# ============================================================

echo ""
echo "--- Single-row UPDATE ---"

T_UPD_1K=$(time_ms "echo 'UPDATE t SET v=\"updated\" WHERE id=500;' | $DOLTLITE '$DB_1K'")
echo "  1K: ${T_UPD_1K}ms"

T_UPD_3K=$(time_ms "echo 'UPDATE t SET v=\"updated\" WHERE id=1500;' | $DOLTLITE '$DB_3K'")
echo "  3K: ${T_UPD_3K}ms"

T_UPD_10K=$(time_ms "echo 'UPDATE t SET v=\"updated\" WHERE id=5000;' | $DOLTLITE '$DB_10K'")
echo "  10K: ${T_UPD_10K}ms"

assert_ratio "update_1k_to_3k" "$T_UPD_1K" "$T_UPD_3K" 5
assert_ratio "update_3k_to_10k" "$T_UPD_3K" "$T_UPD_10K" 5

# ============================================================
# Single-row DELETE (O(log n))
# ============================================================

echo ""
echo "--- Single-row DELETE ---"

T_DEL_1K=$(time_ms "echo 'DELETE FROM t WHERE id=999;' | $DOLTLITE '$DB_1K'")
echo "  1K: ${T_DEL_1K}ms"

T_DEL_3K=$(time_ms "echo 'DELETE FROM t WHERE id=2999;' | $DOLTLITE '$DB_3K'")
echo "  3K: ${T_DEL_3K}ms"

T_DEL_10K=$(time_ms "echo 'DELETE FROM t WHERE id=9999;' | $DOLTLITE '$DB_10K'")
echo "  10K: ${T_DEL_10K}ms"

assert_ratio "delete_1k_to_3k" "$T_DEL_1K" "$T_DEL_3K" 5
assert_ratio "delete_3k_to_10k" "$T_DEL_3K" "$T_DEL_10K" 5

# ============================================================
# dolt_diff after single-row UPDATE (~constant time)
# ============================================================

echo ""
echo "--- dolt_diff after single-row UPDATE ---"

echo "SELECT dolt_commit('-A','-m','baseline');" | $DOLTLITE "$DB_1K" > /dev/null 2>&1
echo "SELECT dolt_commit('-A','-m','baseline');" | $DOLTLITE "$DB_3K" > /dev/null 2>&1
echo "SELECT dolt_commit('-A','-m','baseline');" | $DOLTLITE "$DB_10K" > /dev/null 2>&1

echo "UPDATE t SET v='diffme' WHERE id=0;" | $DOLTLITE "$DB_1K" > /dev/null 2>&1
echo "UPDATE t SET v='diffme' WHERE id=0;" | $DOLTLITE "$DB_3K" > /dev/null 2>&1
echo "UPDATE t SET v='diffme' WHERE id=0;" | $DOLTLITE "$DB_10K" > /dev/null 2>&1

T_DIFF_1K=$(time_ms "echo \"SELECT count(*) FROM dolt_diff('t');\" | $DOLTLITE '$DB_1K'")
echo "  1K: ${T_DIFF_1K}ms"

T_DIFF_3K=$(time_ms "echo \"SELECT count(*) FROM dolt_diff('t');\" | $DOLTLITE '$DB_3K'")
echo "  3K: ${T_DIFF_3K}ms"

T_DIFF_10K=$(time_ms "echo \"SELECT count(*) FROM dolt_diff('t');\" | $DOLTLITE '$DB_10K'")
echo "  10K: ${T_DIFF_10K}ms"

assert_ratio "diff_1k_to_3k" "$T_DIFF_1K" "$T_DIFF_3K" 3
assert_ratio "diff_3k_to_10k" "$T_DIFF_3K" "$T_DIFF_10K" 3

# ============================================================
# Diff correctness: exactly 1 change detected
# ============================================================

echo ""
echo "--- Diff correctness ---"

for pair in "1K:$DB_1K" "3K:$DB_3K" "10K:$DB_10K"; do
  name="${pair%%:*}"; db="${pair#*:}"
  val=$(echo "SELECT count(*) FROM dolt_diff('t');" | $DOLTLITE "$db" 2>&1)
  if [ "$val" = "1" ]; then
    PASS=$((PASS+1)); echo "  PASS: diff_correct_$name — 1 change detected"
  else
    FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: diff_correct_$name\n  expected: 1\n  got: $val"
    echo "  FAIL: diff_correct_$name — expected 1, got $val"
  fi
done

# ============================================================
# Cleanup
# ============================================================

rm -f "$DB_1K" "$DB_3K" "$DB_10K"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
