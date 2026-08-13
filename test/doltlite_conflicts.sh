#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Conflicts Tests ==="
echo ""

DB=/tmp/test_cf_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'orig'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='main'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "conflicts_table" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'CT|' || \"table\" FROM dolt_conflicts; ROLLBACK;" \
  "^CT\\|t$" "$DB"
run_test_match "conflicts_count" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'CC|' || num_conflicts FROM dolt_conflicts; ROLLBACK;" \
  "^CC\\|1$" "$DB"

run_test_match "commit_blocked" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_commit('-A','-m','fail');" \
  "cannot commit: unresolved merge conflicts|Use dolt_conflicts_resolve" "$DB"

run_test_match "resolved_no_conflicts" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT 'RC|' || count(*) FROM dolt_conflicts; SELECT 'RV|' || v FROM t; ROLLBACK;" \
  "^RC\\|0$" "$DB"
run_test_match "ours_value_kept" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT 'RV|' || v FROM t; ROLLBACK;" \
  "^RV\\|main$" "$DB"

DB2=/tmp/test_cf2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'orig'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "UPDATE t SET v='main2'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "UPDATE t SET v='feat2'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB2/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test_match "theirs_has_conflict" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'TC|' || num_conflicts FROM dolt_conflicts; ROLLBACK;" \
  "^TC\\|1$" "$DB2"
run_test_match "theirs_resolved" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TR|' || count(*) FROM dolt_conflicts; ROLLBACK;" \
  "^TR\\|0$" "$DB2"

DB3=/tmp/test_cf3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='MAIN' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='FEAT' WHERE id=2; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB3/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test_match "no_conflict_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB3"
run_test "no_conflicts_table" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB3"
run_test "auto_merge_row1" "SELECT v FROM t WHERE id=1;" "MAIN" "$DB3"
run_test "auto_merge_row2" "SELECT v FROM t WHERE id=2;" "FEAT" "$DB3"

DB4=/tmp/test_cf4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "UPDATE t SET v='main1' WHERE id=1; UPDATE t SET v='main3' WHERE id=3; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "UPDATE t SET v='feat1' WHERE id=1; INSERT INTO t VALUES(4,'feat4'); SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB4/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test_match "mixed_conflict" "SELECT dolt_merge('feature');" "conflict" "$DB4"
run_test_match "mixed_conflict_count" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'MC|' || num_conflicts FROM dolt_conflicts; ROLLBACK;" \
  "^MC\\|1$" "$DB4"
run_test_match "mixed_auto_row3" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'MR3|' || v FROM t WHERE id=3; ROLLBACK;" \
  "^MR3\\|main3$" "$DB4"
run_test_match "mixed_auto_row4" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'MR4|' || count(*) FROM t WHERE id=4; ROLLBACK;" \
  "^MR4\\|1$" "$DB4"

DB5=/tmp/test_conflicts5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER); INSERT INTO t VALUES(1,'alice',100); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_branch('a'); SELECT dolt_checkout('a'); UPDATE t SET name='ALICE' WHERE id=1; SELECT dolt_commit('-A','-m','a');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_branch('b'); SELECT dolt_checkout('b'); UPDATE t SET val=999 WHERE id=1; SELECT dolt_commit('-A','-m','b');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_merge('a');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test_match "cell_merge_no_conflict" "SELECT dolt_merge('b');" "^[0-9a-f]" "$DB5"
run_test "cell_merge_name" "SELECT name FROM t WHERE id=1;" "ALICE" "$DB5"
run_test "cell_merge_val" "SELECT val FROM t WHERE id=1;" "999" "$DB5"
run_test "cell_merge_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB5"

DB6=/tmp/test_conflicts6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO t VALUES(1,'alice'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('schema_br'); SELECT dolt_checkout('schema_br'); ALTER TABLE t ADD COLUMN extra TEXT; UPDATE t SET extra='x'; SELECT dolt_commit('-A','-m','schema');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_branch('data_br'); SELECT dolt_checkout('data_br'); UPDATE t SET name='ALICE' WHERE id=1; SELECT dolt_commit('-A','-m','data');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); SELECT dolt_merge('schema_br');" | $DOLTLITE "$DB6" > /dev/null 2>&1

run_test_match "schema_data_merge" "SELECT dolt_merge('data_br');" "^[0-9a-f]" "$DB6"
run_test "schema_data_name" "SELECT name FROM t WHERE id=1;" "ALICE" "$DB6"
run_test "schema_data_extra" "SELECT extra FROM t WHERE id=1;" "x" "$DB6"

DB7=/tmp/test_conflicts7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val INTEGER); INSERT INTO t VALUES(1,'alice',100); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('c'); SELECT dolt_checkout('c'); UPDATE t SET name='BOB' WHERE id=1; SELECT dolt_commit('-A','-m','c');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET name='CHARLIE' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test_match "real_conflict" "SELECT dolt_merge('c');" "conflict" "$DB7"
run_test_match "real_conflict_count" \
  "BEGIN; SELECT dolt_merge('c'); SELECT 'RC|' || num_conflicts FROM dolt_conflicts; ROLLBACK;" \
  "^RC\\|1$" "$DB7"

run_test_match "conflict_base_decoded" \
  "BEGIN; SELECT dolt_merge('c'); SELECT 'BASE|' || base_name FROM dolt_conflicts_t; ROLLBACK;" \
  "^BASE\\|alice$" "$DB7"
run_test_match "conflict_our_decoded" \
  "BEGIN; SELECT dolt_merge('c'); SELECT 'OUR|' || our_name FROM dolt_conflicts_t; ROLLBACK;" \
  "^OUR\\|CHARLIE$" "$DB7"
run_test_match "conflict_their_decoded" \
  "BEGIN; SELECT dolt_merge('c'); SELECT 'THEIR|' || their_name FROM dolt_conflicts_t; ROLLBACK;" \
  "^THEIR\\|BOB$" "$DB7"

run_test_match "conflict_temp_shadow_base_ignored" \
  "BEGIN; SELECT dolt_merge('c'); CREATE TEMP TABLE t(fake TEXT PRIMARY KEY); SELECT 'TSB|' || base_name FROM dolt_conflicts_t; ROLLBACK;" \
  "^TSB\\|alice$" "$DB7"
run_test_match "conflict_temp_shadow_our_ignored" \
  "BEGIN; SELECT dolt_merge('c'); CREATE TEMP TABLE t(fake TEXT PRIMARY KEY); SELECT 'TSO|' || our_name FROM dolt_conflicts_t; ROLLBACK;" \
  "^TSO\\|CHARLIE$" "$DB7"
run_test_match "conflict_temp_shadow_their_ignored" \
  "BEGIN; SELECT dolt_merge('c'); CREATE TEMP TABLE t(fake TEXT PRIMARY KEY); SELECT 'TST|' || their_name FROM dolt_conflicts_t; ROLLBACK;" \
  "^TST\\|BOB$" "$DB7"

DB8=/tmp/test_conflicts8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET name='A' WHERE id=1; UPDATE t SET name='B' WHERE id=2; UPDATE t SET name='C' WHERE id=3; SELECT dolt_commit('-A','-m','other');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET name='a2' WHERE id=1; UPDATE t SET name='b2' WHERE id=2; UPDATE t SET name='c2' WHERE id=3; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB8" > /dev/null 2>&1

run_test_match "multi_row_conflict" "SELECT dolt_merge('other');" "cannot merge: conflicts detected" "$DB8"
run_test_match "multi_row_conflict_count" \
  "BEGIN; SELECT dolt_merge('other'); SELECT 'MRC|' || num_conflicts FROM dolt_conflicts; ROLLBACK;" \
  "^MRC\\|3$" "$DB8"
run_test_match "multi_row_all_rows" \
  "BEGIN; SELECT dolt_merge('other'); SELECT 'MRA|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" \
  "^MRA\\|3$" "$DB8"
run_test_match "multi_row_has_row1" \
  "BEGIN; SELECT dolt_merge('other'); SELECT 'MR1|' || their_name FROM dolt_conflicts_t WHERE base_id=1; ROLLBACK;" \
  "^MR1\\|A$" "$DB8"
run_test_match "multi_row_has_row2" \
  "BEGIN; SELECT dolt_merge('other'); SELECT 'MR2|' || their_name FROM dolt_conflicts_t WHERE base_id=2; ROLLBACK;" \
  "^MR2\\|B$" "$DB8"
run_test_match "multi_row_has_row3" \
  "BEGIN; SELECT dolt_merge('other'); SELECT 'MR3|' || their_name FROM dolt_conflicts_t WHERE base_id=3; ROLLBACK;" \
  "^MR3\\|C$" "$DB8"

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
  "BEGIN; SELECT dolt_merge('feature'); SELECT 'TDC|' || count(*) FROM dolt_conflicts; ROLLBACK;" \
  "^TDC\\|1$" "$DB9"
run_test_match "theirs_delete_clears_conflict" \
  "BEGIN; SELECT dolt_merge('feature'); SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TDR|' || count(*) FROM dolt_conflicts; ROLLBACK;" \
  "^TDR\\|0$" "$DB9"
run_test_match "theirs_delete_removes_row" \
  "BEGIN; SELECT dolt_merge('feature'); DROP TRIGGER IF EXISTS audit_delete; CREATE TRIGGER audit_delete BEFORE DELETE ON t BEGIN INSERT INTO trig_log VALUES('fired'); END; SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TDD|' || count(*) FROM t WHERE id=1; ROLLBACK;" \
  "^TDD\\|0$" "$DB9"
run_test_match "theirs_delete_trigger_skipped" \
  "BEGIN; SELECT dolt_merge('feature'); DROP TRIGGER IF EXISTS audit_delete; CREATE TRIGGER audit_delete BEFORE DELETE ON t BEGIN INSERT INTO trig_log VALUES('fired'); END; SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TDT|' || count(*) FROM trig_log; ROLLBACK;" \
  "^TDT\\|0$" "$DB9"


# A commit refused for unresolved conflicts must leave the transaction the
# caller opened intact. Committing the transaction first and only then
# checking left the session in autocommit with live merge state, so the next
# resolve wrote the still-conflicted working set to disk -- the one thing a
# conflicted merge must never do -- and reported success without applying.
DB10=/tmp/test_conf10_$$.db; rm -f "$DB10"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base'); INSERT INTO u VALUES(1,'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('br');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "UPDATE t SET v='theirs'; UPDATE u SET v='theirs'; SELECT dolt_commit('-Am','br');" | $DOLTLITE "$DB10/br" > /dev/null 2>&1
echo "UPDATE t SET v='ours'; UPDATE u SET v='ours'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB10" > /dev/null 2>&1

echo "BEGIN;
SELECT dolt_merge('br');
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT dolt_commit('-A','-m','partial');
SELECT dolt_conflicts_resolve('--ours','u');" | $DOLTLITE "$DB10" > /dev/null 2>&1
run_test "refused_commit_leaves_nothing_merging" \
  "SELECT count(*) FROM dolt_merge_status WHERE is_merging=1;" "0" "$DB10"
run_test "refused_commit_leaves_no_conflicts" \
  "SELECT count(*) FROM dolt_conflicts_u;" "0" "$DB10"
run_test "refused_commit_keeps_our_rows" \
  "SELECT v FROM t;" "ours" "$DB10"

# The same refusal with nothing resolved first must also leave clean state,
# and the surfaces that read merge state must stay readable.
DB11=/tmp/test_conf11_$$.db; rm -f "$DB11"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('br');" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "UPDATE t SET v='theirs'; SELECT dolt_commit('-Am','br');" | $DOLTLITE "$DB11/br" > /dev/null 2>&1
echo "UPDATE t SET v='ours'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "BEGIN;
SELECT dolt_merge('br');
SELECT dolt_commit('-A','-m','nope');" | $DOLTLITE "$DB11" > /dev/null 2>&1
run_test "refused_commit_no_prior_resolve_clean" \
  "SELECT count(*) FROM dolt_merge_status WHERE is_merging=1;" "0" "$DB11"
run_test "refused_commit_conflicts_readable" \
  "SELECT count(*) FROM dolt_conflicts_t;" "0" "$DB11"

# Resolving everything inside the transaction still commits the merge.
DB12=/tmp/test_conf12_$$.db; rm -f "$DB12"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base'); INSERT INTO u VALUES(1,'base');
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('br');" | $DOLTLITE "$DB12" > /dev/null 2>&1
echo "UPDATE t SET v='theirs'; UPDATE u SET v='theirs'; SELECT dolt_commit('-Am','br');" | $DOLTLITE "$DB12/br" > /dev/null 2>&1
echo "UPDATE t SET v='ours'; UPDATE u SET v='ours'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB12" > /dev/null 2>&1
echo "BEGIN;
SELECT dolt_merge('br');
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT dolt_conflicts_resolve('--ours','u');
SELECT dolt_commit('-A','-m','resolved');" | $DOLTLITE "$DB12" > /dev/null 2>&1
run_test "resolve_both_then_commit_lands" \
  "SELECT (SELECT v FROM t) || '/' || (SELECT v FROM u);" "theirs/ours" "$DB12"
run_test "resolve_both_then_commit_not_merging" \
  "SELECT count(*) FROM dolt_merge_status WHERE is_merging=1;" "0" "$DB12"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11" "$DB12"
dltest_finish
