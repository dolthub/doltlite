#!/bin/bash
#
# File persistence tests for Doltlite.
#
# Every test writes data, closes the DB, reopens it, and verifies
# the data survived. Tests at multiple scales (3 rows, 100 rows,
# 1000 rows) to catch size-dependent bugs.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite File Persistence Tests ==="
echo ""

# ============================================================
# Basic: INSERT + commit persists across reopen
# ============================================================

echo "--- Basic persistence ---"

DB=/tmp/test_persist_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello');
INSERT INTO t VALUES(2,'world');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "basic_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "basic_val1" "SELECT v FROM t WHERE id=1;" "hello" "$DB"
run_test "basic_val2" "SELECT v FROM t WHERE id=2;" "world" "$DB"
run_test "basic_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"
run_test_match "basic_msg" "SELECT message FROM dolt_log;" "init" "$DB"

rm -f "$DB"

# ============================================================
# Multiple commits persist
# ============================================================

echo ""
echo "--- Multiple commits ---"

DB=/tmp/test_persist_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "multi_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test_match "multi_latest" "SELECT message FROM dolt_log LIMIT 1;" "c3" "$DB"

rm -f "$DB"

# ============================================================
# Bulk INSERT via BEGIN/COMMIT persists (100 rows)
# ============================================================

echo ""
echo "--- Bulk 100 rows ---"

DB=/tmp/test_persist_100_$$.db; rm -f "$DB"
python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('BEGIN;')
for i in range(100):
    print(f'INSERT INTO t VALUES({i}, \"row_{i}\");')
print('COMMIT;')
print(\"SELECT dolt_commit('-A','-m','100 rows');\")
" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bulk100_count" "SELECT count(*) FROM t;" "100" "$DB"
run_test "bulk100_first" "SELECT v FROM t WHERE id=0;" "row_0" "$DB"
run_test "bulk100_last" "SELECT v FROM t WHERE id=99;" "row_99" "$DB"
run_test "bulk100_mid" "SELECT v FROM t WHERE id=50;" "row_50" "$DB"
run_test "bulk100_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"

rm -f "$DB"

# ============================================================
# Bulk INSERT 1000 rows persists
# ============================================================

echo ""
echo "--- Bulk 1000 rows ---"

DB=/tmp/test_persist_1k_$$.db; rm -f "$DB"
python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('BEGIN;')
for i in range(1000):
    print(f'INSERT INTO t VALUES({i}, \"row_{i}\");')
print('COMMIT;')
print(\"SELECT dolt_commit('-A','-m','1k rows');\")
" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bulk1k_count" "SELECT count(*) FROM t;" "1000" "$DB"
run_test "bulk1k_first" "SELECT v FROM t WHERE id=0;" "row_0" "$DB"
run_test "bulk1k_last" "SELECT v FROM t WHERE id=999;" "row_999" "$DB"
run_test "bulk1k_mid" "SELECT v FROM t WHERE id=500;" "row_500" "$DB"

rm -f "$DB"

# ============================================================
# UPDATE persists
# ============================================================

echo ""
echo "--- UPDATE persistence ---"

DB=/tmp/test_persist_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='changed' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "update_val" "SELECT v FROM t WHERE id=1;" "changed" "$DB"
run_test "update_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

rm -f "$DB"

# ============================================================
# DELETE persists
# ============================================================

echo ""
echo "--- DELETE persistence ---"

DB=/tmp/test_persist_delete_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c1');
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "delete_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "delete_no2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test "delete_has1" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test "delete_has3" "SELECT v FROM t WHERE id=3;" "c" "$DB"

rm -f "$DB"

# ============================================================
# Branch persists
# ============================================================

echo ""
echo "--- Branch persistence ---"

DB=/tmp/test_persist_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_data');
SELECT dolt_commit('-A','-m','feat work');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_count" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test "branch_main" "SELECT count(*) FROM t;" "1" "$DB"

# Switch to feat and verify
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "branch_feat" "SELECT count(*) FROM t;" "2" "$DB"
run_test "branch_feat_val" "SELECT v FROM t WHERE id=2;" "feat_data" "$DB"

rm -f "$DB"

# ============================================================
# Tag persists
# ============================================================

echo ""
echo "--- Tag persistence ---"

DB=/tmp/test_persist_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tag_count" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test_match "tag_v1" "SELECT name FROM dolt_tags WHERE name='v1.0';" "v1.0" "$DB"
run_test_match "tag_v2" "SELECT name FROM dolt_tags WHERE name='v2.0';" "v2.0" "$DB"

rm -f "$DB"

# ============================================================
# Merge result persists
# ============================================================

echo ""
echo "--- Merge persistence ---"

DB=/tmp/test_persist_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "merge_has1" "SELECT v FROM t WHERE id=1;" "init" "$DB"
run_test "merge_has2" "SELECT v FROM t WHERE id=2;" "feat" "$DB"
run_test "merge_has3" "SELECT v FROM t WHERE id=3;" "main" "$DB"
run_test_match "merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"

rm -f "$DB"

# ============================================================
# Schema changes persist (ALTER TABLE)
# ============================================================

echo ""
echo "--- Schema persistence ---"

DB=/tmp/test_persist_schema_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra='added' WHERE id=1;
SELECT dolt_commit('-A','-m','schema change');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_extra" "SELECT extra FROM t WHERE id=1;" "added" "$DB"
run_test "schema_v" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test_match "schema_cols" "PRAGMA table_info(t);" "extra" "$DB"

rm -f "$DB"

# ============================================================
# Multiple tables persist
# ============================================================

echo ""
echo "--- Multi-table persistence ---"

DB=/tmp/test_persist_multitable_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
CREATE TABLE products(id INTEGER PRIMARY KEY, price INTEGER);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO users VALUES(2,'Bob');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO products VALUES(1,999);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "mt_users" "SELECT count(*) FROM users;" "2" "$DB"
run_test "mt_orders" "SELECT count(*) FROM orders;" "1" "$DB"
run_test "mt_products" "SELECT price FROM products WHERE id=1;" "999" "$DB"
run_test "mt_alice" "SELECT name FROM users WHERE id=1;" "Alice" "$DB"

rm -f "$DB"

# ============================================================
# GC result persists
# ============================================================

echo ""
echo "--- GC persistence ---"

DB=/tmp/test_persist_gc_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('temp');
SELECT dolt_checkout('temp');
INSERT INTO t VALUES(2,'temp');
SELECT dolt_commit('-A','-m','temp');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d','temp');
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_data" "SELECT v FROM t WHERE id=1;" "keep" "$DB"
run_test "gc_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_branches" "SELECT count(*) FROM dolt_branches;" "1" "$DB"

rm -f "$DB"

# ============================================================
# Uncommitted data does NOT persist (no dolt_commit)
# ============================================================

echo ""
echo "--- Uncommitted data lost on reopen ---"

DB=/tmp/test_persist_uncommitted_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'committed');
SELECT dolt_commit('-A','-m','init');
INSERT INTO t VALUES(2,'uncommitted');" | $DOLTLITE "$DB" > /dev/null 2>&1

# The uncommitted row should survive (SQL autocommit persists to chunk store)
# but dolt_status should show it as uncommitted
run_test "uncommit_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test_match "uncommit_status" "SELECT count(*) FROM dolt_status;" "^[1-9]" "$DB"

rm -f "$DB"

# ============================================================
# Bulk 1000 rows with UPDATE and DELETE persists
# ============================================================

echo ""
echo "--- Bulk with modifications ---"

DB=/tmp/test_persist_bulkmod_$$.db; rm -f "$DB"
python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('BEGIN;')
for i in range(1000):
    print(f'INSERT INTO t VALUES({i}, \"row_{i}\");')
print('COMMIT;')
print(\"SELECT dolt_commit('-A','-m','init');\")
print(\"UPDATE t SET v='MODIFIED' WHERE id=500;\")
print(\"DELETE FROM t WHERE id=999;\")
print(\"SELECT dolt_commit('-A','-m','mods');\")
" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bulkmod_count" "SELECT count(*) FROM t;" "999" "$DB"
run_test "bulkmod_modified" "SELECT v FROM t WHERE id=500;" "MODIFIED" "$DB"
run_test "bulkmod_deleted" "SELECT count(*) FROM t WHERE id=999;" "0" "$DB"
run_test "bulkmod_unchanged" "SELECT v FROM t WHERE id=1;" "row_1" "$DB"
run_test "bulkmod_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Rapid open/close cycles
# ============================================================

echo ""
echo "--- Rapid open/close ---"

DB=/tmp/test_persist_rapid_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 10); do
  echo "INSERT INTO t VALUES($i,'val_$i');
SELECT dolt_commit('-A','-m','commit $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

run_test "rapid_count" "SELECT count(*) FROM t;" "10" "$DB"
run_test "rapid_log" "SELECT count(*) FROM dolt_log;" "11" "$DB"
run_test "rapid_val5" "SELECT v FROM t WHERE id=5;" "val_5" "$DB"

rm -f "$DB"

# ============================================================
# File size is reasonable (not empty, not corrupt)
# ============================================================

echo ""
echo "--- File size sanity ---"

DB=/tmp/test_persist_size_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'data');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Check total size of DB + WAL files
SIZE=0
for f in "$DB" "${DB}-wal"; do
  if [ -f "$f" ]; then
    S=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null)
    SIZE=$((SIZE + S))
  fi
done
if [ "$SIZE" -gt 100 ]; then
  PASS=$((PASS+1)); echo "  PASS: size_nonzero — $SIZE bytes"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: size_nonzero\n  size=$SIZE (too small)"
  echo "  FAIL: size_nonzero — $SIZE bytes (too small)"
fi

rm -f "$DB" "${DB}-wal"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
