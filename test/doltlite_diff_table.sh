#!/bin/bash
#
# Tests for dolt_diff_<tablename> audit log virtual tables.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite dolt_diff_<table> Audit Log Tests ==="
echo ""

# ============================================================
# Basic: single commit shows inserts as "added"
# ============================================================

DB=/tmp/test_dt_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Alice');
INSERT INTO t VALUES(2,'Bob');
SELECT dolt_commit('-A','-m','initial');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "basic_count" "SELECT count(*) FROM dolt_diff_t;" "2" "$DB"
run_test_match "basic_type" "SELECT diff_type FROM dolt_diff_t LIMIT 1;" "added" "$DB"
run_test_match "basic_rowid" "SELECT rowid_val FROM dolt_diff_t WHERE rowid_val=1;" "1" "$DB"
run_test_match "basic_to_commit" "SELECT length(to_commit) FROM dolt_diff_t LIMIT 1;" "40" "$DB"
run_test_match "basic_to_date" "SELECT to_commit_date FROM dolt_diff_t LIMIT 1;" "^[0-9]" "$DB"

rm -f "$DB"

# ============================================================
# Multiple commits: full history
# ============================================================

DB=/tmp/test_dt_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='A' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# c1: added row 1 (1 row)
# c2: added row 2 (1 row)
# c3: modified row 1 (1 row)
# Total: 3 audit rows
run_test "multi_count" "SELECT count(*) FROM dolt_diff_t;" "3" "$DB"

# Check diff types present
run_test_match "multi_has_added" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" "^2$" "$DB"
run_test_match "multi_has_modified" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" "^1$" "$DB"

# Each row has a different to_commit
run_test "multi_commits" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Commit context columns
# ============================================================

DB=/tmp/test_dt_context_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# from_commit and to_commit should be valid 40-char hashes
run_test_match "ctx_to_hash" "SELECT to_commit FROM dolt_diff_t LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
run_test_match "ctx_from_hash" "SELECT from_commit FROM dolt_diff_t WHERE diff_type='added' AND rowid_val=2;" "^[0-9a-f]{40}$" "$DB"

# to_commit_date should be a reasonable unix timestamp
run_test_match "ctx_to_date" "SELECT to_commit_date FROM dolt_diff_t LIMIT 1;" "^[0-9]{9,}" "$DB"

# For the initial commit, from_commit is all zeros
run_test_match "ctx_initial_from" \
  "SELECT from_commit FROM dolt_diff_t WHERE rowid_val=1 AND diff_type='added';" \
  "^0{40}$" "$DB"

rm -f "$DB"

# ============================================================
# Persistence across reopen
# ============================================================

DB=/tmp/test_dt_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Query from a new session
run_test "persist_count" "SELECT count(*) FROM dolt_diff_t;" "2" "$DB"
run_test_match "persist_type" "SELECT diff_type FROM dolt_diff_t WHERE rowid_val=2;" "added" "$DB"

rm -f "$DB"

# ============================================================
# Multiple tables have separate audit logs
# ============================================================

DB=/tmp/test_dt_tables_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO orders VALUES(2,'Gadget');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tables_users" "SELECT count(*) FROM dolt_diff_users;" "1" "$DB"
run_test "tables_orders" "SELECT count(*) FROM dolt_diff_orders;" "2" "$DB"

# Add more data
echo "INSERT INTO users VALUES(2,'Bob');
SELECT dolt_commit('-A','-m','add bob');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tables_users_after" "SELECT count(*) FROM dolt_diff_users;" "2" "$DB"
run_test "tables_orders_after" "SELECT count(*) FROM dolt_diff_orders;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Audit log after merge
# ============================================================

DB=/tmp/test_dt_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main add');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Should have audit rows from: init commit + main add + merge (which adds row 2)
run_test_match "merge_count" "SELECT count(*) FROM dolt_diff_t;" "^[3-9]" "$DB"
run_test_match "merge_has_merge_commit" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "^[3-9]" "$DB"

rm -f "$DB"

# ============================================================
# Audit log after cherry-pick
# ============================================================

DB=/tmp/test_dt_cp_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick((SELECT hash FROM dolt_branches WHERE name='feat'));" | $DOLTLITE "$DB" > /dev/null 2>&1

# Should show the cherry-picked row as added
run_test_match "cp_count" "SELECT count(*) FROM dolt_diff_t;" "^[2-9]" "$DB"
run_test_match "cp_has_row2" "SELECT count(*) FROM dolt_diff_t WHERE rowid_val=2;" "^[1-9]" "$DB"

rm -f "$DB"

# ============================================================
# Audit log after revert
# ============================================================

DB=/tmp/test_dt_revert_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
INSERT INTO t VALUES(2,'to_revert');
SELECT dolt_commit('-A','-m','add row 2');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

# Should show: init (added 1), add row 2 (added 2), revert (removed 2)
run_test_match "revert_count" "SELECT count(*) FROM dolt_diff_t;" "^[3-9]" "$DB"
run_test_match "revert_has_removed" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='removed';" "^[1-9]" "$DB"

rm -f "$DB"

# ============================================================
# Empty table has no audit rows
# ============================================================

DB=/tmp/test_dt_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty table');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_count" "SELECT count(*) FROM dolt_diff_t;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Long history (10 commits)
# ============================================================

DB=/tmp/test_dt_history_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 9); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

# 1 "added" + 9 "modified" = 10 audit rows
run_test "history_count" "SELECT count(*) FROM dolt_diff_t;" "10" "$DB"
run_test "history_added" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" "1" "$DB"
run_test "history_modified" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" "9" "$DB"
run_test "history_commits" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "10" "$DB"

rm -f "$DB"

# ============================================================
# Table not present before a commit
# ============================================================

DB=/tmp/test_dt_newt_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init t');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','add t2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# t2 was only in the second commit
run_test "newt_t2_count" "SELECT count(*) FROM dolt_diff_t2;" "1" "$DB"
run_test "newt_t2_type" "SELECT diff_type FROM dolt_diff_t2;" "added" "$DB"

rm -f "$DB"

# ============================================================
# from_value and to_value are blobs
# ============================================================

DB=/tmp/test_dt_vals_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='world' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# For the initial add, from_value is null, to_value is blob
run_test_match "vals_add_from" "SELECT typeof(from_value) FROM dolt_diff_t WHERE diff_type='added';" "null" "$DB"
run_test_match "vals_add_to" "SELECT typeof(to_value) FROM dolt_diff_t WHERE diff_type='added';" "blob" "$DB"

# For the modify, both are blobs
run_test_match "vals_mod_from" "SELECT typeof(from_value) FROM dolt_diff_t WHERE diff_type='modified';" "blob" "$DB"
run_test_match "vals_mod_to" "SELECT typeof(to_value) FROM dolt_diff_t WHERE diff_type='modified';" "blob" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
