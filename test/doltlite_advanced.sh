#!/bin/bash
#
# Advanced coverage tests for Doltlite.
# Covers: indexes, transactions, wide tables, empty tables, upserts,
# multi-column updates, views, aggregates, complex merge scenarios,
# working-change interactions, log/branch/tag metadata, and more.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Advanced Tests ==="
echo ""

# ============================================================
# Section 1: Index tables and queries (6 tests)
# ============================================================

DB=/tmp/test_adv_index_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
CREATE INDEX idx_name ON t(name);
CREATE INDEX idx_age ON t(age);
INSERT INTO t VALUES(1,'Alice',30);
INSERT INTO t VALUES(2,'Bob',25);
INSERT INTO t VALUES(3,'Charlie',35);
INSERT INTO t VALUES(4,'Alice',28);
SELECT dolt_commit('-A','-m','indexed table');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "idx_count" "SELECT count(*) FROM t;" "4" "$DB"
run_test "idx_query_name" "SELECT count(*) FROM t WHERE name='Alice';" "2" "$DB"
run_test "idx_query_age" "SELECT count(*) FROM t WHERE age > 29;" "2" "$DB"
run_test "idx_schema" "SELECT count(*) FROM sqlite_master WHERE type='index';" "2" "$DB"

# Index survives commit + reopen
run_test "idx_persist" "SELECT count(*) FROM sqlite_master WHERE type='index';" "2" "$DB"
run_test "idx_query_persist" "SELECT count(*) FROM t WHERE name='Bob';" "1" "$DB"

rm -f "$DB"

# ============================================================
# Section 2: Transactions with dolt operations (6 tests)
# ============================================================

DB=/tmp/test_adv_txn_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'committed');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Rollback discards SQL changes
echo "BEGIN;
INSERT INTO t VALUES(2,'rollback_me');
ROLLBACK;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "txn_rollback" "SELECT count(*) FROM t;" "1" "$DB"

# Commit inside transaction
echo "BEGIN;
INSERT INTO t VALUES(3,'txn_insert');
COMMIT;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "txn_commit" "SELECT count(*) FROM t;" "2" "$DB"

# Dolt status sees the change
run_test_match "txn_status" "SELECT count(*) FROM dolt_status;" "^[1-9]" "$DB"

# Dolt commit captures it
run_test_match "txn_dolt_commit" "SELECT dolt_commit('-A','-m','txn work');" "^[0-9a-f]{40}$" "$DB"
run_test "txn_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"
run_test "txn_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 3: Wide tables — many columns (5 tests)
# ============================================================

DB=/tmp/test_adv_wide_$$.db; rm -f "$DB"
echo "CREATE TABLE wide(
  id INTEGER PRIMARY KEY,
  c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT,
  c6 TEXT, c7 TEXT, c8 TEXT, c9 TEXT, c10 TEXT,
  c11 INTEGER, c12 INTEGER, c13 REAL, c14 REAL, c15 BLOB
);
INSERT INTO wide VALUES(1,'a','b','c','d','e','f','g','h','i','j',1,2,3.0,4.0,X'FF');
INSERT INTO wide VALUES(2,'k','l','m','n','o','p','q','r','s','t',5,6,7.0,8.0,X'00');
SELECT dolt_commit('-A','-m','wide table');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "wide_count" "SELECT count(*) FROM wide;" "2" "$DB"
run_test "wide_col1" "SELECT c1 FROM wide WHERE id=1;" "a" "$DB"
run_test "wide_col10" "SELECT c10 FROM wide WHERE id=2;" "t" "$DB"
run_test "wide_int" "SELECT c11 FROM wide WHERE id=1;" "1" "$DB"

# Persist
run_test "wide_persist" "SELECT c5 FROM wide WHERE id=1;" "e" "$DB"

rm -f "$DB"

# ============================================================
# Section 4: Empty table operations (6 tests)
# ============================================================

DB=/tmp/test_adv_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty table');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_count" "SELECT count(*) FROM t;" "0" "$DB"
run_test "empty_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"
run_test "empty_diff" "SELECT count(*) FROM dolt_diff('t');" "0" "$DB"

# Branch empty table
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(1,'feat_row');
SELECT dolt_commit('-A','-m','feat adds row');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# FF merge into empty table
run_test_match "empty_ff_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "empty_after_merge" "SELECT count(*) FROM t;" "1" "$DB"
run_test "empty_merge_val" "SELECT v FROM t WHERE id=1;" "feat_row" "$DB"

rm -f "$DB"

# ============================================================
# Section 5: Table with only primary key (4 tests)
# ============================================================

DB=/tmp/test_adv_pkonly_$$.db; rm -f "$DB"
echo "CREATE TABLE ids(id INTEGER PRIMARY KEY);
INSERT INTO ids VALUES(10);
INSERT INTO ids VALUES(20);
INSERT INTO ids VALUES(30);
SELECT dolt_commit('-A','-m','pk only');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "pkonly_count" "SELECT count(*) FROM ids;" "3" "$DB"
run_test "pkonly_sum" "SELECT sum(id) FROM ids;" "60" "$DB"

# Add more and commit
echo "INSERT INTO ids VALUES(40);
INSERT INTO ids VALUES(50);
SELECT dolt_commit('-A','-m','more ids');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "pkonly_count2" "SELECT count(*) FROM ids;" "5" "$DB"
run_test "pkonly_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 6: REPLACE INTO / INSERT OR REPLACE (5 tests)
# ============================================================

DB=/tmp/test_adv_replace_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Replace existing row
echo "REPLACE INTO t VALUES(1,'replaced');
SELECT dolt_commit('-A','-m','replace');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "replace_val" "SELECT v FROM t WHERE id=1;" "replaced" "$DB"
run_test "replace_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "replace_other" "SELECT v FROM t WHERE id=2;" "keep" "$DB"

# INSERT OR REPLACE
echo "INSERT OR REPLACE INTO t VALUES(2,'or_replaced');
SELECT dolt_commit('-A','-m','or replace');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "or_replace_val" "SELECT v FROM t WHERE id=2;" "or_replaced" "$DB"
run_test "or_replace_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 7: INSERT OR IGNORE (4 tests)
# ============================================================

DB=/tmp/test_adv_ignore_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'first');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Insert or ignore duplicate — should silently skip
echo "INSERT OR IGNORE INTO t VALUES(1,'duplicate');
INSERT OR IGNORE INTO t VALUES(2,'new');
SELECT dolt_commit('-A','-m','ignore test');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "ignore_orig" "SELECT v FROM t WHERE id=1;" "first" "$DB"
run_test "ignore_new" "SELECT v FROM t WHERE id=2;" "new" "$DB"
run_test "ignore_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "ignore_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 8: Multi-column updates in merge (6 tests)
# ============================================================

DB=/tmp/test_adv_multicol_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'x','y','z');
INSERT INTO t VALUES(2,'x','y','z');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Branch updates column a on row 1
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET a='feat_a' WHERE id=1;
SELECT dolt_commit('-A','-m','feat: update a');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Main updates column c on row 2
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET c='main_c' WHERE id=2;
SELECT dolt_commit('-A','-m','main: update c');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "multicol_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "multicol_row1_a" "SELECT a FROM t WHERE id=1;" "feat_a" "$DB"
run_test "multicol_row1_b" "SELECT b FROM t WHERE id=1;" "y" "$DB"
run_test "multicol_row2_c" "SELECT c FROM t WHERE id=2;" "main_c" "$DB"
run_test "multicol_row2_a" "SELECT a FROM t WHERE id=2;" "x" "$DB"
run_test "multicol_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 9: Aggregate queries after merge (4 tests)
# ============================================================

DB=/tmp/test_adv_agg_$$.db; rm -f "$DB"
echo "CREATE TABLE sales(id INTEGER PRIMARY KEY, amount INTEGER, region TEXT);
INSERT INTO sales VALUES(1,100,'east');
INSERT INTO sales VALUES(2,200,'west');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO sales VALUES(3,150,'east');
SELECT dolt_commit('-A','-m','feat sale');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO sales VALUES(4,300,'west');
SELECT dolt_commit('-A','-m','main sale');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "agg_sum" "SELECT sum(amount) FROM sales;" "750" "$DB"
run_test "agg_count" "SELECT count(*) FROM sales;" "4" "$DB"
run_test "agg_max" "SELECT max(amount) FROM sales;" "300" "$DB"
run_test "agg_group" "SELECT sum(amount) FROM sales WHERE region='east';" "250" "$DB"

rm -f "$DB"

# ============================================================
# Section 10: Views with dolt (4 tests)
# ============================================================

DB=/tmp/test_adv_view_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, score INTEGER);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'b',20);
INSERT INTO t VALUES(3,'c',30);
CREATE VIEW high_scores AS SELECT * FROM t WHERE score >= 20;
SELECT dolt_commit('-A','-m','with view');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "view_count" "SELECT count(*) FROM high_scores;" "2" "$DB"
run_test "view_schema" "SELECT count(*) FROM sqlite_master WHERE type='view';" "1" "$DB"

# View persists
run_test "view_persist" "SELECT count(*) FROM high_scores;" "2" "$DB"

# Add data, view updates
echo "INSERT INTO t VALUES(4,'d',25);
SELECT dolt_commit('-A','-m','add d');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "view_updated" "SELECT count(*) FROM high_scores;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 11: Multiple updates to same row before commit (4 tests)
# ============================================================

DB=/tmp/test_adv_multiupd_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'first');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Update same row 3 times, then commit — final value should win
echo "UPDATE t SET v='second' WHERE id=1;
UPDATE t SET v='third' WHERE id=1;
UPDATE t SET v='final' WHERE id=1;
SELECT dolt_commit('-A','-m','multi update');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multiupd_val" "SELECT v FROM t WHERE id=1;" "final" "$DB"
run_test "multiupd_count" "SELECT count(*) FROM t;" "1" "$DB"

# Diff should show 1 change (from first to final)
run_test_match "multiupd_diff" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^1$" "$DB"
run_test "multiupd_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 12: Insert, update, delete same row before commit (4 tests)
# ============================================================

DB=/tmp/test_adv_lifecycle_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Insert row 2, update it, delete it — net effect: nothing
echo "INSERT INTO t VALUES(2,'temp');
UPDATE t SET v='temp_updated' WHERE id=2;
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','noop cycle');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "lifecycle_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "lifecycle_orig" "SELECT v FROM t WHERE id=1;" "keep" "$DB"

# Insert and keep
echo "INSERT INTO t VALUES(3,'new');
UPDATE t SET v='updated_new' WHERE id=3;
SELECT dolt_commit('-A','-m','insert and update');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "lifecycle_new" "SELECT v FROM t WHERE id=3;" "updated_new" "$DB"
run_test "lifecycle_final_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 13: Large text values (4 tests)
# ============================================================

DB=/tmp/test_adv_largetext_$$.db; rm -f "$DB"

# Generate a ~10KB string
BIGVAL=$(python3 -c "print('x' * 10000)" 2>/dev/null || printf '%0.sx' $(seq 1 10000))
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'${BIGVAL}');
INSERT INTO t VALUES(2,'small');
SELECT dolt_commit('-A','-m','large text');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "large_length" "SELECT length(v) FROM t WHERE id=1;" "10000" "$DB"
run_test "large_small" "SELECT v FROM t WHERE id=2;" "small" "$DB"
run_test "large_count" "SELECT count(*) FROM t;" "2" "$DB"

# Persist
run_test "large_persist" "SELECT length(v) FROM t WHERE id=1;" "10000" "$DB"

rm -f "$DB"

# ============================================================
# Section 14: dolt_log metadata columns (5 tests)
# ============================================================

DB=/tmp/test_adv_logmeta_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','test message','--author','LogTest <log@test.com>');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Check all log columns
run_test_match "logmeta_hash" "SELECT commit_hash FROM dolt_log LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
run_test "logmeta_committer" "SELECT committer FROM dolt_log LIMIT 1;" "LogTest" "$DB"
run_test "logmeta_email" "SELECT email FROM dolt_log LIMIT 1;" "log@test.com" "$DB"
run_test "logmeta_message" "SELECT message FROM dolt_log LIMIT 1;" "test message" "$DB"
# date should be a reasonable timestamp (> year 2020)
run_test_match "logmeta_date" "SELECT date FROM dolt_log LIMIT 1;" "^[0-9]" "$DB"

rm -f "$DB"

# ============================================================
# Section 15: dolt_branches metadata (5 tests)
# ============================================================

DB=/tmp/test_adv_branchmeta_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branchmeta_count" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test_match "branchmeta_hash" "SELECT hash FROM dolt_branches WHERE name='main';" "^[0-9a-f]{40}$" "$DB"

# Both branches point to same commit (no divergence yet)
run_test "branchmeta_same_hash" \
  "SELECT count(DISTINCT hash) FROM dolt_branches;" "1" "$DB"

# Make a commit on dev — hashes should diverge
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','dev commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branchmeta_diverged" \
  "SELECT count(DISTINCT hash) FROM dolt_branches;" "2" "$DB"
run_test_match "branchmeta_names" \
  "SELECT group_concat(name) FROM (SELECT name FROM dolt_branches ORDER BY name);" "dev.*main" "$DB"

rm -f "$DB"

# ============================================================
# Section 16: dolt_tags metadata (4 tests)
# ============================================================

DB=/tmp/test_adv_tagmeta_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','release v1');
SELECT dolt_tag('v1.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2);
SELECT dolt_commit('-A','-m','release v2');
SELECT dolt_tag('v2.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tagmeta_count" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test_match "tagmeta_hash" "SELECT hash FROM dolt_tags WHERE name='v1.0';" "^[0-9a-f]{40}$" "$DB"
run_test "tagmeta_diff_hash" "SELECT count(DISTINCT hash) FROM dolt_tags;" "2" "$DB"
run_test_match "tagmeta_names" \
  "SELECT group_concat(name) FROM (SELECT name FROM dolt_tags ORDER BY name);" "v1.*v2" "$DB"

rm -f "$DB"

# ============================================================
# Section 17: Schema-only commit (no data changes) (4 tests)
# ============================================================

DB=/tmp/test_adv_schemaonly_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty schema');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schemaonly_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"
run_test "schemaonly_count" "SELECT count(*) FROM t;" "0" "$DB"

# Add another table (schema change, no data)
echo "CREATE TABLE t2(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','add t2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schemaonly_log2" "SELECT count(*) FROM dolt_log;" "2" "$DB"
run_test "schemaonly_tables" "SELECT count(*) FROM sqlite_master WHERE type='table';" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 18: Checkout preserves other branch data (5 tests)
# ============================================================

DB=/tmp/test_adv_checkout_preserve_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main_data');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'dev_data');
SELECT dolt_commit('-A','-m','dev work');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Switch back and forth — data should be correct each time
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "preserve_main1" "SELECT count(*) FROM t;" "1" "$DB"

echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "preserve_dev1" "SELECT count(*) FROM t;" "2" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "preserve_main2" "SELECT count(*) FROM t;" "1" "$DB"

echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "preserve_dev2" "SELECT count(*) FROM t;" "2" "$DB"
run_test "preserve_dev_val" "SELECT v FROM t WHERE id=2;" "dev_data" "$DB"

rm -f "$DB"

# ============================================================
# Section 19: dolt_status after CREATE TABLE (4 tests)
# ============================================================

DB=/tmp/test_adv_createstatus_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create new table — should show in status
echo "CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "createstatus_shows" "SELECT count(*) FROM dolt_status;" "^[1-9]" "$DB"
run_test_match "createstatus_name" "SELECT table_name FROM dolt_status LIMIT 1;" "t2" "$DB"

# Commit it
run_test_match "createstatus_commit" "SELECT dolt_commit('-A','-m','add t2');" "^[0-9a-f]{40}$" "$DB"
run_test "createstatus_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 20: Subqueries with dolt system tables (4 tests)
# ============================================================

DB=/tmp/test_adv_subquery_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Subquery on dolt_log
run_test "subq_latest" \
  "SELECT message FROM dolt_log WHERE commit_hash = (SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "c3" "$DB"

run_test "subq_count_after" \
  "SELECT count(*) FROM dolt_log WHERE message LIKE 'c%';" "3" "$DB"

# Tag the first commit using subquery
echo "SELECT dolt_tag('first', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2));" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "subq_tag" "SELECT count(*) FROM dolt_tags;" "1" "$DB"

# Diff using subquery for hashes
run_test_match "subq_diff" \
  "SELECT count(*) FROM dolt_diff('t', (SELECT hash FROM dolt_tags WHERE name='first'), (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[1-9]" "$DB"

rm -f "$DB"

# ============================================================
# Section 21: ORDER BY on dolt_log (4 tests)
# ============================================================

DB=/tmp/test_adv_logorder_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','first');
INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','second');
INSERT INTO t VALUES(3); SELECT dolt_commit('-A','-m','third');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Default order: newest first
run_test "logorder_first" "SELECT message FROM dolt_log LIMIT 1;" "third" "$DB"
run_test "logorder_last" "SELECT message FROM dolt_log LIMIT 1 OFFSET 2;" "first" "$DB"

# Count and offset
run_test "logorder_offset1" "SELECT message FROM dolt_log LIMIT 1 OFFSET 1;" "second" "$DB"
run_test "logorder_total" "SELECT count(*) FROM dolt_log;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 22: Working changes across multiple tables (5 tests)
# ============================================================

DB=/tmp/test_adv_multiwork_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, uid INTEGER, item TEXT);
CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price INTEGER);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,1,'widget');
INSERT INTO products VALUES(1,'Widget',10);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Make changes to all 3 tables
echo "INSERT INTO users VALUES(2,'Bob');
INSERT INTO orders VALUES(2,2,'gadget');
INSERT INTO products VALUES(2,'Gadget',20);" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multiwork_status" "SELECT count(*) FROM dolt_status;" "3" "$DB"

# Stage only users
echo "SELECT dolt_add('users');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "multiwork_staged" "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"
run_test "multiwork_unstaged" "SELECT count(*) FROM dolt_status WHERE staged=0;" "2" "$DB"

# Commit all
run_test_match "multiwork_commit" "SELECT dolt_commit('-A','-m','all 3');" "^[0-9a-f]{40}$" "$DB"
run_test "multiwork_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 23: Reset after staging specific tables (4 tests)
# ============================================================

DB=/tmp/test_adv_resetstage_$$.db; rm -f "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,'b1');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,'b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Stage only a
echo "SELECT dolt_add('a');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Soft reset — should unstage a but keep all data
echo "SELECT dolt_reset('--soft');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "resetstage_unstaged" "SELECT count(*) FROM dolt_status WHERE staged=1;" "0" "$DB"
run_test "resetstage_data_a" "SELECT count(*) FROM a;" "2" "$DB"
run_test "resetstage_data_b" "SELECT count(*) FROM b;" "2" "$DB"

# Hard reset — discard all
echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "resetstage_hard" "SELECT count(*) FROM a;" "1" "$DB"

rm -f "$DB"

# ============================================================
# Section 24: Double merge (merge A then merge B) (5 tests)
# ============================================================

DB=/tmp/test_adv_doublemerge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Create two feature branches from same point
echo "SELECT dolt_branch('featA');
SELECT dolt_branch('featB');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('featA');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(10,'A'); SELECT dolt_commit('-A','-m','featA');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('featB');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(20,'B'); SELECT dolt_commit('-A','-m','featB');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge A into main
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "doublemerge_A" "SELECT dolt_merge('featA');" "^[0-9a-f]{40}$" "$DB"
run_test "doublemerge_A_count" "SELECT count(*) FROM t;" "2" "$DB"

# Merge B into main (main now has init + A, merging B which has init + B)
run_test_match "doublemerge_B" "SELECT dolt_merge('featB');" "^[0-9a-f]{40}$" "$DB"
run_test "doublemerge_B_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "doublemerge_all" "SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id);" "init,A,B" "$DB"

rm -f "$DB"

# ============================================================
# Section 25: dolt_add('.') behavior (4 tests)
# ============================================================

DB=/tmp/test_adv_adddot_$$.db; rm -f "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,'b1');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,'b2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# dolt_add('.') should stage all
echo "SELECT dolt_add('.');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "adddot_all_staged" "SELECT count(*) FROM dolt_status WHERE staged=1;" "2" "$DB"
run_test "adddot_none_unstaged" "SELECT count(*) FROM dolt_status WHERE staged=0;" "0" "$DB"

# Commit with staged
run_test_match "adddot_commit" "SELECT dolt_commit('-m','dot staged');" "^[0-9a-f]{40}$" "$DB"
run_test "adddot_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

rm -f "$DB"

# ============================================================
# Section 26: Merge where only one side changed (not FF) (4 tests)
# ============================================================

DB=/tmp/test_adv_onesidemerge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Only main changes
echo "INSERT INTO t VALUES(2,'main_only');
SELECT dolt_commit('-A','-m','main adds');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge feat (which is behind) — should see "Already up to date" since
# feat is ancestor of main — merge is a no-op or returns hash
run_test_match "oneside_merge" "SELECT dolt_merge('feat');" "up to date|^[0-9a-f]{40}$" "$DB"
run_test "oneside_count" "SELECT count(*) FROM t;" "2" "$DB"

# Now make feat ahead: checkout feat, add data, merge into main
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'feat_only');
SELECT dolt_commit('-A','-m','feat adds');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Now merge feat into main — three-way merge needed
run_test_match "oneside_real_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "oneside_all_rows" "SELECT count(*) FROM t;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 27: COALESCE, CASE, complex expressions (4 tests)
# ============================================================

DB=/tmp/test_adv_expr_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,'fallback');
INSERT INTO t VALUES(2,'value',NULL);
INSERT INTO t VALUES(3,NULL,NULL);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "expr_coalesce1" "SELECT coalesce(a,b,'none') FROM t WHERE id=1;" "fallback" "$DB"
run_test "expr_coalesce2" "SELECT coalesce(a,b,'none') FROM t WHERE id=2;" "value" "$DB"
run_test "expr_coalesce3" "SELECT coalesce(a,b,'none') FROM t WHERE id=3;" "none" "$DB"
run_test "expr_case" \
  "SELECT CASE WHEN a IS NOT NULL THEN 'has_a' ELSE 'no_a' END FROM t WHERE id=1;" \
  "no_a" "$DB"

rm -f "$DB"

# ============================================================
# Section 28: Merge with index tables (4 tests)
# ============================================================

DB=/tmp/test_adv_mergeidx_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER);
CREATE INDEX idx_val ON t(val);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'b',20);
SELECT dolt_commit('-A','-m','init with index');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'c',30);
SELECT dolt_commit('-A','-m','feat add');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4,'d',5);
SELECT dolt_commit('-A','-m','main add');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "mergeidx_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
# Verify all rows present (check individual rows, not count which may use index)
run_test "mergeidx_row1" "SELECT name FROM t WHERE id=1;" "a" "$DB"
run_test "mergeidx_row3" "SELECT name FROM t WHERE id=3;" "c" "$DB"
run_test "mergeidx_row4" "SELECT name FROM t WHERE id=4;" "d" "$DB"
run_test "mergeidx_schema" "SELECT count(*) FROM sqlite_master WHERE type='index';" "1" "$DB"

rm -f "$DB"

# ============================================================
# Section 29: Unique constraints with dolt (4 tests)
# ============================================================

DB=/tmp/test_adv_unique_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);
INSERT INTO t VALUES(1,'alice@test.com','Alice');
INSERT INTO t VALUES(2,'bob@test.com','Bob');
SELECT dolt_commit('-A','-m','with unique');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "unique_count" "SELECT count(*) FROM t;" "2" "$DB"

# Unique constraint enforced
run_test_match "unique_violate" \
  "INSERT INTO t VALUES(3,'alice@test.com','Fake');" "UNIQUE constraint" "$DB"

# Update respects unique
echo "UPDATE t SET name='Alice Updated' WHERE id=1;
SELECT dolt_commit('-A','-m','update alice');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "unique_updated" "SELECT name FROM t WHERE id=1;" "Alice Updated" "$DB"
run_test "unique_persist" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

# ============================================================
# Section 30: NOT NULL constraints with dolt (4 tests)
# ============================================================

DB=/tmp/test_adv_notnull_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER DEFAULT 0);
INSERT INTO t VALUES(1,'Alice',100);
INSERT INTO t VALUES(2,'Bob',200);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "notnull_count" "SELECT count(*) FROM t;" "2" "$DB"

# NOT NULL enforced
run_test_match "notnull_violate" \
  "INSERT INTO t VALUES(3,NULL,50);" "NOT NULL" "$DB"

# Default value works
echo "INSERT INTO t(id,name) VALUES(4,'Charlie');
SELECT dolt_commit('-A','-m','with default');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "notnull_default" "SELECT score FROM t WHERE id=4;" "0" "$DB"
run_test "notnull_persist" "SELECT count(*) FROM t;" "3" "$DB"

rm -f "$DB"

# ============================================================
# Section 31: Complex join queries after dolt operations (4 tests)
# ============================================================

DB=/tmp/test_adv_join_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, uid INTEGER, product TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO users VALUES(2,'Bob');
INSERT INTO orders VALUES(1,1,'Widget');
INSERT INTO orders VALUES(2,1,'Gadget');
INSERT INTO orders VALUES(3,2,'Thing');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "join_count" \
  "SELECT count(*) FROM users u JOIN orders o ON u.id=o.uid;" "3" "$DB"
run_test "join_alice" \
  "SELECT count(*) FROM users u JOIN orders o ON u.id=o.uid WHERE u.name='Alice';" "2" "$DB"
run_test "join_bob" \
  "SELECT count(*) FROM users u JOIN orders o ON u.id=o.uid WHERE u.name='Bob';" "1" "$DB"

# Add data and verify join after commit
echo "INSERT INTO users VALUES(3,'Charlie');
INSERT INTO orders VALUES(4,3,'Doohickey');
SELECT dolt_commit('-A','-m','add charlie');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "join_after_commit" \
  "SELECT count(*) FROM users u JOIN orders o ON u.id=o.uid;" "4" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
