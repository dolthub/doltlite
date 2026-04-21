#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Conflicts Tests ==="
echo ""

# Test 1: Autocommit merge with conflict surfaces conflict state in-session
DB=/tmp/test_cf_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'orig'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "conflicts_table" \
  "SELECT dolt_merge('feature'); SELECT 'CT|' || \"table\" FROM dolt_conflicts;" \
  "^CT\\|t$" "$DB"
run_test_match "conflicts_count" \
  "SELECT dolt_merge('feature'); SELECT 'CC|' || num_conflicts FROM dolt_conflicts;" \
  "^CC\\|1$" "$DB"

# Test 2: Commit blocked with conflicts in the same SQL session
run_test_match "commit_blocked" \
  "SELECT dolt_merge('feature'); SELECT dolt_commit('-A','-m','fail');" \
  "unresolved merge conflicts" "$DB"

# Test 3: Resolve --ours keeps our value in-session
run_test_match "resolved_no_conflicts" \
  "SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT 'RC|' || count(*) FROM dolt_conflicts; SELECT 'RV|' || v FROM t;" \
  "^RC\\|0$" "$DB"
run_test_match "ours_value_kept" \
  "SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT 'RV|' || v FROM t;" \
  "^RV\\|main$" "$DB"

# Test 4: Resolve --theirs (new scenario)
DB2=/tmp/test_cf2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'orig'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "UPDATE t SET v='main2'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "UPDATE t SET v='feat2'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test_match "theirs_has_conflict" \
  "SELECT dolt_merge('feature'); SELECT 'TC|' || num_conflicts FROM dolt_conflicts;" \
  "^TC\\|1$" "$DB2"
run_test_match "theirs_resolved" \
  "SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TR|' || count(*) FROM dolt_conflicts;" \
  "^TR\\|0$" "$DB2"

# Test 5: No conflict when different rows modified
DB3=/tmp/test_cf3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='MAIN' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='FEAT' WHERE id=2; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test_match "no_conflict_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB3"
run_test "no_conflicts_table" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB3"
run_test "auto_merge_row1" "SELECT v FROM t WHERE id=1;" "MAIN" "$DB3"
run_test "auto_merge_row2" "SELECT v FROM t WHERE id=2;" "FEAT" "$DB3"

# Test 6: Mixed — some conflict, some auto-merge
DB4=/tmp/test_cf4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "UPDATE t SET v='main1' WHERE id=1; UPDATE t SET v='main3' WHERE id=3; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "UPDATE t SET v='feat1' WHERE id=1; INSERT INTO t VALUES(4,'feat4'); SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test_match "mixed_conflict" "SELECT dolt_merge('feature');" "conflict" "$DB4"
run_test_match "mixed_conflict_count" \
  "SELECT dolt_merge('feature'); SELECT 'MC|' || num_conflicts FROM dolt_conflicts;" \
  "^MC\\|1$" "$DB4"
run_test_match "mixed_auto_row3" \
  "SELECT dolt_merge('feature'); SELECT 'MR3|' || v FROM t WHERE id=3;" \
  "^MR3\\|main3$" "$DB4"
run_test_match "mixed_auto_row4" \
  "SELECT dolt_merge('feature'); SELECT 'MR4|' || count(*) FROM t WHERE id=4;" \
  "^MR4\\|0$" "$DB4"

# --- Cell-level merge: non-overlapping column changes auto-merge ---
DB5=/tmp/test_conflicts5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER); INSERT INTO t VALUES(1,'alice',100); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_branch('a'); SELECT dolt_checkout('a'); UPDATE t SET name='ALICE' WHERE id=1; SELECT dolt_commit('-A','-m','a');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_branch('b'); SELECT dolt_checkout('b'); UPDATE t SET val=999 WHERE id=1; SELECT dolt_commit('-A','-m','b');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_merge('a');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test_match "cell_merge_no_conflict" "SELECT dolt_merge('b');" "^[0-9a-f]" "$DB5"
run_test "cell_merge_name" "SELECT name FROM t WHERE id=1;" "ALICE" "$DB5"
run_test "cell_merge_val" "SELECT val FROM t WHERE id=1;" "999" "$DB5"
run_test "cell_merge_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB5"

# --- Cell-level merge: schema change + data change auto-merge ---
DB6=/tmp/test_conflicts6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO t VALUES(1,'alice'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('schema_br'); SELECT dolt_checkout('schema_br'); ALTER TABLE t ADD COLUMN extra TEXT; UPDATE t SET extra='x'; SELECT dolt_commit('-A','-m','schema');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_branch('data_br'); SELECT dolt_checkout('data_br'); UPDATE t SET name='ALICE' WHERE id=1; SELECT dolt_commit('-A','-m','data');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_merge('schema_br');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test_match "schema_data_merge" "SELECT dolt_merge('data_br');" "^[0-9a-f]" "$DB6"
run_test "schema_data_name" "SELECT name FROM t WHERE id=1;" "ALICE" "$DB6"
run_test "schema_data_extra" "SELECT extra FROM t WHERE id=1;" "x" "$DB6"

# --- Real conflict: same column changed on both sides ---
DB7=/tmp/test_conflicts7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER); INSERT INTO t VALUES(1,'alice',100); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('c'); SELECT dolt_checkout('c'); UPDATE t SET name='BOB' WHERE id=1; SELECT dolt_commit('-A','-m','c');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET name='CHARLIE' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test_match "real_conflict" "SELECT dolt_merge('c');" "conflict" "$DB7"
run_test_match "real_conflict_count" \
  "SELECT dolt_merge('c'); SELECT 'RC|' || num_conflicts FROM dolt_conflicts;" \
  "^RC\\|1$" "$DB7"

# User columns are now projected individually (Dolt-compatible schema).
run_test_match "conflict_base_decoded" \
  "SELECT dolt_merge('c'); SELECT 'BASE|' || base_name FROM dolt_conflicts_t;" \
  "^BASE\\|alice$" "$DB7"
run_test_match "conflict_our_decoded" \
  "SELECT dolt_merge('c'); SELECT 'OUR|' || our_name FROM dolt_conflicts_t;" \
  "^OUR\\|CHARLIE$" "$DB7"
run_test_match "conflict_their_decoded" \
  "SELECT dolt_merge('c'); SELECT 'THEIR|' || their_name FROM dolt_conflicts_t;" \
  "^THEIR\\|BOB$" "$DB7"

# Temp table shadowing the user table must not affect dolt_conflicts_<table>
# projection. The conflict view should derive its schema from main.t.
run_test_match "conflict_temp_shadow_base_ignored" \
  "SELECT dolt_merge('c'); CREATE TEMP TABLE t(fake TEXT PRIMARY KEY); SELECT 'TSB|' || base_name FROM dolt_conflicts_t;" \
  "^TSB\\|alice$" "$DB7"
run_test_match "conflict_temp_shadow_our_ignored" \
  "SELECT dolt_merge('c'); CREATE TEMP TABLE t(fake TEXT PRIMARY KEY); SELECT 'TSO|' || our_name FROM dolt_conflicts_t;" \
  "^TSO\\|CHARLIE$" "$DB7"
run_test_match "conflict_temp_shadow_their_ignored" \
  "SELECT dolt_merge('c'); CREATE TEMP TABLE t(fake TEXT PRIMARY KEY); SELECT 'TST|' || their_name FROM dolt_conflicts_t;" \
  "^TST\\|BOB$" "$DB7"

# --- Multiple conflicting rows in one table ---
DB8=/tmp/test_conflicts8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET name='A' WHERE id=1; UPDATE t SET name='B' WHERE id=2; UPDATE t SET name='C' WHERE id=3; SELECT dolt_commit('-A','-m','other');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET name='a2' WHERE id=1; UPDATE t SET name='b2' WHERE id=2; UPDATE t SET name='c2' WHERE id=3; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB8" > /dev/null 2>&1

run_test_match "multi_row_conflict" "SELECT dolt_merge('other');" "Merge conflict detected" "$DB8"
run_test_match "multi_row_conflict_count" \
  "SELECT dolt_merge('other'); SELECT 'MRC|' || num_conflicts FROM dolt_conflicts;" \
  "^MRC\\|3$" "$DB8"
run_test_match "multi_row_all_rows" \
  "SELECT dolt_merge('other'); SELECT 'MRA|' || count(*) FROM dolt_conflicts_t;" \
  "^MRA\\|3$" "$DB8"
run_test_match "multi_row_has_row1" \
  "SELECT dolt_merge('other'); SELECT 'MR1|' || their_name FROM dolt_conflicts_t WHERE base_id=1;" \
  "^MR1\\|A$" "$DB8"
run_test_match "multi_row_has_row2" \
  "SELECT dolt_merge('other'); SELECT 'MR2|' || their_name FROM dolt_conflicts_t WHERE base_id=2;" \
  "^MR2\\|B$" "$DB8"
run_test_match "multi_row_has_row3" \
  "SELECT dolt_merge('other'); SELECT 'MR3|' || their_name FROM dolt_conflicts_t WHERE base_id=3;" \
  "^MR3\\|C$" "$DB8"

# Test 9: triggers on the target table do NOT fire during conflict
# resolution. Matches Dolt's semantics: merge-resolve writes go to the
# prolly tree directly; triggers already ran on the original commits
# and re-firing them during resolve would be wrong. Earlier versions
# of doltlite accidentally fired triggers because the resolve path
# ran through sqlite3_exec(INSERT/DELETE) — that was incorrect, and
# the test cases that enforced it have been removed.
DB9=/tmp/test_conflicts9_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE trig_log(note TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-A','-m','feat delete');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_commit('-A','-m','main update');" | $DOLTLITE "$DB9" > /dev/null 2>&1
run_test_match "theirs_delete_conflict_present" \
  "SELECT dolt_merge('feature'); SELECT 'TDC|' || count(*) FROM dolt_conflicts;" \
  "^TDC\\|1$" "$DB9"
run_test_match "theirs_delete_clears_conflict" \
  "SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TDR|' || count(*) FROM dolt_conflicts;" \
  "^TDR\\|0$" "$DB9"
run_test_match "theirs_delete_removes_row" \
  "SELECT dolt_merge('feature'); DROP TRIGGER IF EXISTS audit_delete; CREATE TRIGGER audit_delete BEFORE DELETE ON t BEGIN INSERT INTO trig_log VALUES('fired'); END; SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TDD|' || count(*) FROM t WHERE id=1;" \
  "^TDD\\|0$" "$DB9"
run_test_match "theirs_delete_trigger_skipped" \
  "SELECT dolt_merge('feature'); DROP TRIGGER IF EXISTS audit_delete; CREATE TRIGGER audit_delete BEFORE DELETE ON t BEGIN INSERT INTO trig_log VALUES('fired'); END; SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TDT|' || count(*) FROM trig_log;" \
  "^TDT\\|0$" "$DB9"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
