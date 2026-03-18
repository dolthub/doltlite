#!/bin/bash
#
# Tests for dolt_history_<tablename> time-travel virtual tables.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite dolt_history_<table> Tests ==="
echo ""

# ============================================================
# Single commit: one version of each row
# ============================================================

DB=/tmp/test_hist_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Alice');
INSERT INTO t VALUES(2,'Bob');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "basic_count" "SELECT count(*) FROM dolt_history_t;" "2" "$DB"
run_test_match "basic_hash" "SELECT commit_hash FROM dolt_history_t LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
run_test_match "basic_date" "SELECT commit_date FROM dolt_history_t LIMIT 1;" "^[0-9]{4}-" "$DB"
run_test "basic_committer" "SELECT committer FROM dolt_history_t LIMIT 1;" "doltlite" "$DB"

rm -f "$DB"

# ============================================================
# Multiple commits: rows appear once per commit they exist in
# ============================================================

DB=/tmp/test_hist_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# c1: row 1 (1 row)
# c2: rows 1,2 (2 rows)
# c3: rows 1,2,3 (3 rows)
# Total: 1+2+3 = 6
run_test "multi_count" "SELECT count(*) FROM dolt_history_t;" "6" "$DB"

# Row 1 appears in all 3 commits
run_test "multi_row1" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=1;" "3" "$DB"

# Row 2 appears in 2 commits
run_test "multi_row2" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=2;" "2" "$DB"

# Row 3 appears in 1 commit
run_test "multi_row3" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=3;" "1" "$DB"

# 3 distinct commits
run_test "multi_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Updated row shows different values at different commits
# ============================================================

DB=/tmp/test_hist_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='v2' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='v3' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Row 1 appears 3 times (once per commit), each with different value blob
run_test "update_count" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=1;" "3" "$DB"

# All are for the same rowid but with different commit hashes
run_test "update_distinct" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "3" "$DB"

# value column should be decoded text
run_test_match "update_type" "SELECT typeof(value) FROM dolt_history_t LIMIT 1;" "text" "$DB"

rm -f "$DB"

# ============================================================
# Persistence across reopen
# ============================================================

DB=/tmp/test_hist_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "persist_count" "SELECT count(*) FROM dolt_history_t;" "3" "$DB"
run_test "persist_row1" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=1;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Multiple tables have separate histories
# ============================================================

DB=/tmp/test_hist_tables_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO orders VALUES(2,'Gadget');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tables_users" "SELECT count(*) FROM dolt_history_users;" "1" "$DB"
run_test "tables_orders" "SELECT count(*) FROM dolt_history_orders;" "2" "$DB"

rm -f "$DB"

# ============================================================
# History with --author commit
# ============================================================

DB=/tmp/test_hist_author_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init','--author','TestUser <test@test.com>');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "author_name" "SELECT committer FROM dolt_history_t LIMIT 1;" "TestUser" "$DB"

rm -f "$DB"

# ============================================================
# History after merge
# ============================================================

DB=/tmp/test_hist_merge_$$.db; rm -f "$DB"
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

# After merge: 4 commits (init, feat, main, merge)
# init: row 1
# main: rows 1,3
# merge: rows 1,2,3
# (feat branch not in HEAD history unless followed via merge parent)
# At minimum: init(1) + main(2) + merge(3) = 6
run_test_match "merge_count" "SELECT count(*) FROM dolt_history_t;" "^[6-9]" "$DB"

rm -f "$DB"

# ============================================================
# Empty table has no history
# ============================================================

DB=/tmp/test_hist_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_count" "SELECT count(*) FROM dolt_history_t;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Long history
# ============================================================

DB=/tmp/test_hist_long_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 5); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

# Row 1 appears in all 6 commits
run_test "long_count" "SELECT count(*) FROM dolt_history_t;" "6" "$DB"
run_test "long_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_t;" "6" "$DB"

rm -f "$DB"

# ============================================================
# Table created later doesn't appear in earlier commits
# ============================================================

DB=/tmp/test_hist_late_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# t2 only exists in c2, so history has 1 row
run_test "late_t2" "SELECT count(*) FROM dolt_history_t2;" "1" "$DB"

# t exists in both commits
run_test "late_t" "SELECT count(*) FROM dolt_history_t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# History query with WHERE filter
# ============================================================

DB=/tmp/test_hist_filter_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Filter by rowid
run_test "filter_row1" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=1;" "2" "$DB"
run_test "filter_row3" "SELECT count(*) FROM dolt_history_t WHERE rowid_val=3;" "1" "$DB"

# Filter by commit
run_test_match "filter_commit" \
  "SELECT count(*) FROM dolt_history_t WHERE commit_hash=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "^3$" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
