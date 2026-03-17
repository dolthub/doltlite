#!/bin/bash
#
# Tests for dolt_at('table', 'ref') — point-in-time table queries.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite dolt_at() Point-in-Time Query Tests ==="
echo ""

# ============================================================
# Basic: query table at different commits
# ============================================================

DB=/tmp/test_at_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# At c1: 1 row
run_test "basic_c1" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2));" \
  "1" "$DB"

# At c2: 2 rows
run_test "basic_c2" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "2" "$DB"

# At c3 (HEAD): 3 rows
run_test "basic_c3" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

rm -f "$DB"

# ============================================================
# Rowid values correct at each commit
# ============================================================

DB=/tmp/test_at_rowid_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(20,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# At c1: only rowid 10
run_test "rowid_c1_10" \
  "SELECT rowid_val FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "10" "$DB"

# At c2: both rowids
run_test "rowid_c2_count" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "2" "$DB"

rm -f "$DB"

# ============================================================
# Resolve by branch name
# ============================================================

DB=/tmp/test_at_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main_row');
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# At branch 'feat': rows 1,2
run_test "branch_feat" "SELECT count(*) FROM dolt_at('t', 'feat');" "2" "$DB"

# At branch 'main': rows 1,3
run_test "branch_main" "SELECT count(*) FROM dolt_at('t', 'main');" "2" "$DB"

# Specific rowids per branch
run_test "branch_feat_has2" \
  "SELECT count(*) FROM dolt_at('t', 'feat') WHERE rowid_val=2;" "1" "$DB"
run_test "branch_feat_no3" \
  "SELECT count(*) FROM dolt_at('t', 'feat') WHERE rowid_val=3;" "0" "$DB"
run_test "branch_main_has3" \
  "SELECT count(*) FROM dolt_at('t', 'main') WHERE rowid_val=3;" "1" "$DB"
run_test "branch_main_no2" \
  "SELECT count(*) FROM dolt_at('t', 'main') WHERE rowid_val=2;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Resolve by tag name
# ============================================================

DB=/tmp/test_at_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','release 1');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','release 2');
SELECT dolt_tag('v2.0');
INSERT INTO t VALUES(3,'v3');
SELECT dolt_commit('-A','-m','release 3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tag_v1" "SELECT count(*) FROM dolt_at('t', 'v1.0');" "1" "$DB"
run_test "tag_v2" "SELECT count(*) FROM dolt_at('t', 'v2.0');" "2" "$DB"

rm -f "$DB"

# ============================================================
# Updated rows show old values at old commits
# ============================================================

DB=/tmp/test_at_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='changed' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Both commits have 1 row, but with different values (blobs)
run_test "update_c1_count" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "1" "$DB"
run_test "update_c2_count" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

# Values should be different blobs
run_test_match "update_diff_vals" \
  "SELECT CASE WHEN a.value != b.value THEN 'different' ELSE 'same' END FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1)) a, dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1)) b WHERE a.rowid_val=b.rowid_val;" \
  "different" "$DB"

rm -f "$DB"

# ============================================================
# Deleted rows not present at later commits
# ============================================================

DB=/tmp/test_at_delete_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "delete_c1" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "2" "$DB"
run_test "delete_c2" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

rm -f "$DB"

# ============================================================
# Non-existent table returns empty
# ============================================================

DB=/tmp/test_at_notable_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "notable" \
  "SELECT count(*) FROM dolt_at('nonexistent', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "0" "$DB"

rm -f "$DB"

# ============================================================
# Non-existent ref returns empty
# ============================================================

DB=/tmp/test_at_noref_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "noref" \
  "SELECT count(*) FROM dolt_at('t', 'nonexistent_branch');" \
  "0" "$DB"

rm -f "$DB"

# ============================================================
# Multiple tables at same commit
# ============================================================

DB=/tmp/test_at_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO orders VALUES(2,'Gadget');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)

run_test "multi_users" "SELECT count(*) FROM dolt_at('users', '$HASH');" "1" "$DB"
run_test "multi_orders" "SELECT count(*) FROM dolt_at('orders', '$HASH');" "2" "$DB"

rm -f "$DB"

# ============================================================
# Persistence: works after reopen
# ============================================================

DB=/tmp/test_at_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Query from new session
run_test "persist_c1" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "1" "$DB"
run_test "persist_c2" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "2" "$DB"

rm -f "$DB"

# ============================================================
# Table created in later commit not visible at earlier commit
# ============================================================

DB=/tmp/test_at_late_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE t2(id INTEGER PRIMARY KEY);
INSERT INTO t2 VALUES(1);
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# t2 doesn't exist at c1
run_test "late_no_t2" \
  "SELECT count(*) FROM dolt_at('t2', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "0" "$DB"

# t2 exists at c2
run_test "late_has_t2" \
  "SELECT count(*) FROM dolt_at('t2', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

rm -f "$DB"

# ============================================================
# Works after merge
# ============================================================

DB=/tmp/test_at_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('before_merge');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Before merge: 1 row
run_test "merge_before" "SELECT count(*) FROM dolt_at('t', 'before_merge');" "1" "$DB"

# At feat: 2 rows
run_test "merge_feat" "SELECT count(*) FROM dolt_at('t', 'feat');" "2" "$DB"

# At HEAD (after merge): 3 rows
run_test "merge_head" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

rm -f "$DB"

# ============================================================
# Works after GC
# ============================================================

DB=/tmp/test_at_gc_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_c1" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "1" "$DB"
run_test "gc_c2" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "2" "$DB"

rm -f "$DB"

# ============================================================
# Compare current table vs historical version
# ============================================================

DB=/tmp/test_at_compare_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'old');
INSERT INTO t VALUES(2,'old');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='new' WHERE id=1;
INSERT INTO t VALUES(3,'added');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Current has 3 rows
run_test "compare_current" "SELECT count(*) FROM t;" "3" "$DB"

# Historical has 2 rows
run_test "compare_old" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "2" "$DB"

rm -f "$DB"

# ============================================================
# Empty table at commit
# ============================================================

DB=/tmp/test_at_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','with data');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_at_c1" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "0" "$DB"
run_test "empty_at_c2" \
  "SELECT count(*) FROM dolt_at('t', (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
