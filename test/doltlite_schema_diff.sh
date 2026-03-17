#!/bin/bash
#
# Tests for dolt_schema_diff('from_ref', 'to_ref')
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Schema Diff Tests ==="
echo ""

# ============================================================
# Table added between commits
# ============================================================

DB=/tmp/test_sd_add_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'b');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "add_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "1" "$DB"
run_test "add_name" "SELECT table_name FROM dolt_schema_diff('v1','v2');" "t2" "$DB"
run_test "add_type" "SELECT diff_type FROM dolt_schema_diff('v1','v2');" "added" "$DB"
run_test_match "add_to_sql" \
  "SELECT to_create_stmt FROM dolt_schema_diff('v1','v2');" "CREATE TABLE t2" "$DB"
run_test "add_from_null" \
  "SELECT from_create_stmt IS NULL FROM dolt_schema_diff('v1','v2');" "1" "$DB"

rm -f "$DB"

# ============================================================
# Multiple tables added
# ============================================================

DB=/tmp/test_sd_multi_$$.db; rm -f "$DB"
echo "SELECT dolt_commit('-A','-m','empty');
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','add 3 tables');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_count" \
  "SELECT count(*) FROM dolt_schema_diff((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1),(SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

rm -f "$DB"

# ============================================================
# No schema changes between commits
# ============================================================

DB=/tmp/test_sd_none_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Only data changes, no schema changes
run_test "none_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "0" "$DB"

rm -f "$DB"

# ============================================================
# Resolve by branch name
# ============================================================

DB=/tmp/test_sd_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-A','-m','feat add table');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_count" "SELECT count(*) FROM dolt_schema_diff('main','feat');" "1" "$DB"
run_test "branch_name" "SELECT table_name FROM dolt_schema_diff('main','feat');" "feat_tbl" "$DB"
run_test "branch_type" "SELECT diff_type FROM dolt_schema_diff('main','feat');" "added" "$DB"

rm -f "$DB"

# ============================================================
# Reverse direction shows "dropped"
# ============================================================

DB=/tmp/test_sd_reverse_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE TABLE extra(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# v1→v2: extra is added
run_test "reverse_fwd" "SELECT diff_type FROM dolt_schema_diff('v1','v2');" "added" "$DB"

# v2→v1: extra is dropped
run_test "reverse_rev" "SELECT diff_type FROM dolt_schema_diff('v2','v1');" "dropped" "$DB"
run_test "reverse_rev_name" "SELECT table_name FROM dolt_schema_diff('v2','v1');" "extra" "$DB"

# When dropped, from_create_stmt is set, to_create_stmt is null
run_test_match "reverse_from" \
  "SELECT from_create_stmt FROM dolt_schema_diff('v2','v1');" "CREATE TABLE" "$DB"
run_test "reverse_to_null" \
  "SELECT to_create_stmt IS NULL FROM dolt_schema_diff('v2','v1');" "1" "$DB"

rm -f "$DB"

# ============================================================
# Same commit returns empty
# ============================================================

DB=/tmp/test_sd_same_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('v1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "same_empty" "SELECT count(*) FROM dolt_schema_diff('v1','v1');" "0" "$DB"

rm -f "$DB"

# ============================================================
# Schema diff after merge (new table from feature branch)
# ============================================================

DB=/tmp/test_sd_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('before');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, x TEXT);
INSERT INTO feat_tbl VALUES(1,'a');
SELECT dolt_commit('-A','-m','feat table');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-A','-m','main work');
SELECT dolt_merge('feat');
SELECT dolt_tag('after');" | $DOLTLITE "$DB" > /dev/null 2>&1

# before→after should show feat_tbl as added
run_test_match "merge_added" \
  "SELECT table_name FROM dolt_schema_diff('before','after') WHERE diff_type='added';" \
  "feat_tbl" "$DB"

rm -f "$DB"

# ============================================================
# Index creation shows up as schema change
# ============================================================

DB=/tmp/test_sd_index_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE INDEX idx_name ON t(name);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Index creation adds an entry to sqlite_master
run_test_match "index_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "^[1-9]" "$DB"
run_test_match "index_name" \
  "SELECT table_name FROM dolt_schema_diff('v1','v2') LIMIT 1;" "idx_name" "$DB"

rm -f "$DB"

# ============================================================
# View creation shows up
# ============================================================

DB=/tmp/test_sd_view_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE VIEW v AS SELECT * FROM t;
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "view_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "^[1-9]" "$DB"
run_test_match "view_name" \
  "SELECT table_name FROM dolt_schema_diff('v1','v2') WHERE table_name='v';" "v" "$DB"

rm -f "$DB"

# ============================================================
# Persistence
# ============================================================

DB=/tmp/test_sd_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
CREATE TABLE t2(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "persist_count" "SELECT count(*) FROM dolt_schema_diff('v1','v2');" "1" "$DB"

rm -f "$DB"

# ============================================================
# CREATE TABLE statement content
# ============================================================

DB=/tmp/test_sd_stmt_$$.db; rm -f "$DB"
echo "SELECT dolt_commit('-A','-m','empty');
SELECT dolt_tag('v1');
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);
SELECT dolt_commit('-A','-m','add users');
SELECT dolt_tag('v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "stmt_content" \
  "SELECT to_create_stmt FROM dolt_schema_diff('v1','v2');" \
  "CREATE TABLE users" "$DB"
run_test_match "stmt_cols" \
  "SELECT to_create_stmt FROM dolt_schema_diff('v1','v2');" \
  "name TEXT" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
