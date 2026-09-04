#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Reset Tests ==="
echo ""

DB=/tmp/test_reset_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_add('-A');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "staged_before_soft_reset" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB"

echo "SELECT dolt_reset('--soft');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "still_staged_after_soft_noop" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB"

run_test "no_unstaged_after_soft_noop" \
  "SELECT count(*) FROM dolt_status WHERE staged=0;" \
  "0" "$DB"

run_test "data_preserved_soft_noop" \
  "SELECT count(*) FROM t;" \
  "2" "$DB"

echo "SELECT dolt_reset();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "unstaged_after_no_args_reset" \
  "SELECT count(*) FROM dolt_status WHERE staged=0;" \
  "1" "$DB"

run_test "no_staged_after_no_args_reset" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "0" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "clean_after_hard_reset" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB"

run_test "data_reverted_hard_reset" \
  "SELECT count(*) FROM t;" \
  "1" "$DB"

run_test "correct_data_after_hard" \
  "SELECT v FROM t;" \
  "a" "$DB"

DB2=/tmp/test_reset2_$$.db; rm -f "$DB2"
echo "CREATE TABLE a(x); CREATE TABLE b(y); INSERT INTO a VALUES(1); INSERT INTO b VALUES(2); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO a VALUES(10); INSERT INTO b VALUES(20); CREATE TABLE c(z);" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "changes_before_hard" \
  "SELECT count(*) FROM dolt_status;" \
  "3" "$DB2"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "untracked_preserved_after_multi_hard" \
  "SELECT count(*) FROM dolt_status;" \
  "1" "$DB2"

run_test "untracked_table_still_present_after_hard" \
  "SELECT table_name FROM dolt_status;" \
  "c" "$DB2"

run_test "a_reverted" \
  "SELECT * FROM a;" \
  "1" "$DB2"

run_test "b_reverted" \
  "SELECT * FROM b;" \
  "2" "$DB2"

DB3=/tmp/test_reset3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_add('t');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "staged_before_reset" \
  "SELECT staged FROM dolt_status;" \
  "1" "$DB3"

echo "SELECT dolt_reset();" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "default_is_soft" \
  "SELECT staged FROM dolt_status;" \
  "0" "$DB3"

DB3B=/tmp/test_reset3b_$$.db; rm -f "$DB3B"
echo "CREATE TABLE a(x); CREATE TABLE b(y); INSERT INTO a VALUES(1); INSERT INTO b VALUES(1); SELECT dolt_commit('-A','-m','init'); INSERT INTO a VALUES(2); INSERT INTO b VALUES(2); SELECT dolt_add('-A'); SELECT dolt_reset('a','nope');" | $DOLTLITE "$DB3B" > /dev/null 2>&1
run_test "multipath_reset_with_missing_unstages_all" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "0" "$DB3B"
run_test "multipath_reset_with_missing_leaves_both_unstaged" \
  "SELECT count(*) FROM dolt_status WHERE staged=0;" \
  "2" "$DB3B"

DB3C=/tmp/test_reset3c_$$.db; rm -f "$DB3C"
echo "CREATE TABLE a(x); INSERT INTO a VALUES(1); SELECT dolt_commit('-A','-m','init'); INSERT INTO a VALUES(2); SELECT dolt_add('-A'); SELECT dolt_reset('nope','nope2');" | $DOLTLITE "$DB3C" > /dev/null 2>&1
run_test "multipath_reset_all_missing_unstages_all" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "0" "$DB3C"

DB3D=/tmp/test_reset3d_$$.db; rm -f "$DB3D"
echo "CREATE TABLE Camel(id INTEGER PRIMARY KEY, v INT); CREATE INDEX Camel_v ON Camel(v); INSERT INTO Camel VALUES(1,10); SELECT dolt_commit('-Am','base'); UPDATE Camel SET v=20; SELECT dolt_add('Camel'); SELECT dolt_reset('cAmEl');" | $DOLTLITE "$DB3D" > /dev/null 2>&1
run_test "path_reset_is_case_insensitive" \
  "SELECT staged, status FROM dolt_status WHERE table_name='Camel';" \
  "0|modified" "$DB3D"
run_test "case_insensitive_reset_keeps_working_change" \
  "SELECT v FROM Camel WHERE id=1;" \
  "20" "$DB3D"
run_test_match "case_insensitive_reset_clears_staged_indexes" \
  "SELECT dolt_commit('-m','unexpected');" \
  "nothing to commit" "$DB3D"

DB4=/tmp/test_reset4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB4" > /dev/null 2>&1

run_test "hard_reset_persists" \
  "SELECT * FROM t;" \
  "1" "$DB4"

DB5=/tmp/test_reset5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'v1'); SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB5" > /dev/null 2>&1
C1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB5" 2>/dev/null)
echo "UPDATE t SET v='v2' WHERE x=1; SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'new'); SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "pre_reset_count" "SELECT count(*) FROM t;" "2" "$DB5"
run_test "pre_reset_commits" "SELECT count(*) FROM dolt_log;" "4" "$DB5"

echo "SELECT dolt_reset('--hard','$C1');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test "reset_to_hash_data" "SELECT v FROM t;" "v1" "$DB5"
run_test "reset_to_hash_count" "SELECT count(*) FROM t;" "1" "$DB5"
run_test "reset_to_hash_log" "SELECT count(*) FROM dolt_log;" "2" "$DB5"
run_test "reset_to_hash_head" "SELECT commit_hash FROM dolt_log LIMIT 1;" "$C1" "$DB5"
run_test "reset_to_hash_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB5"

run_test "reset_to_hash_reopen_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB5"
run_test "reset_to_hash_reopen_rows" "SELECT v FROM t;" "v1" "$DB5"

DB5B=/tmp/test_reset5b_$$.db; rm -f "$DB5B"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); ALTER TABLE a ADD COLUMN extra INTEGER; UPDATE a SET extra=99 WHERE id=1; SELECT dolt_commit('-A','-m','c2'); SELECT dolt_reset('--hard','HEAD^1');" | $DOLTLITE "$DB5B" > /dev/null 2>&1
run_test "reset_head_parent_schema" \
  "SELECT group_concat(name || ':' || lower(type), '|') FROM pragma_table_info('a');" \
  "id:integer|s:text" "$DB5B"
run_test "reset_head_parent_rows" \
  "SELECT s FROM a;" \
  "base" "$DB5B"

DB5C=/tmp/test_reset5c_$$.db; rm -f "$DB5C"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO a VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main'); INSERT INTO a VALUES(3,'main'); SELECT dolt_commit('-A','-m','main'); SELECT dolt_merge('feat');" | $DOLTLITE "$DB5C" > /dev/null 2>&1
run_test "reset_head_second_parent" \
  "SELECT dolt_reset('--hard','HEAD^2'); SELECT group_concat(s, '|') FROM (SELECT s FROM a ORDER BY id);" \
  "0
base|feat" "$DB5C"

echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO a VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main'); INSERT INTO a VALUES(3,'main'); SELECT dolt_commit('-A','-m','main'); SELECT dolt_merge('feat');" | $DOLTLITE "$DB5C.hash" > /dev/null 2>&1
H2=$(echo "SELECT dolt_hashof('HEAD^2');" | $DOLTLITE "$DB5C.hash" 2>/dev/null)
run_test "reset_raw_second_parent_hash" \
  "SELECT dolt_reset('--hard','$H2'); SELECT group_concat(s, '|') FROM (SELECT s FROM a ORDER BY id);" \
  "0
base|feat" "$DB5C.hash"

DB6=/tmp/test_reset6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
C_INIT=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB6" 2>/dev/null)
echo "SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET v='OTHER'; SELECT dolt_commit('-A','-m','other');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET v='MAIN'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test_match "merge_has_conflicts" \
  "BEGIN; SELECT dolt_merge('other'); SELECT 'MC|' || count(*) FROM dolt_conflicts; ROLLBACK;" \
  "^MC\\|1$" "$DB6"

run_test_match "reset_clears_conflicts" \
  "BEGIN; SELECT dolt_merge('other'); SELECT dolt_reset('--hard','$C_INIT'); SELECT 'RC|' || count(*) FROM dolt_conflicts; SELECT 'RV|' || v FROM t; ROLLBACK;" \
  "^RC\\|0$" "$DB6"
run_test_match "reset_restores_init" \
  "BEGIN; SELECT dolt_merge('other'); SELECT dolt_reset('--hard','$C_INIT'); SELECT 'RV|' || v FROM t; ROLLBACK;" \
  "^RV\\|a$" "$DB6"

run_test_match "reset_bad_ref" \
  "SELECT dolt_reset('--hard','not_a_real_ref');" \
  "not found" "$DB6"

DB7=/tmp/test_reset7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET v='feat'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET v='main'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test_match "merge_conflicts_present_before_hard_reset" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT 'HC|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" \
  "^HC\\|1$" "$DB7"

run_test_match "hard_reset_clears_conflicts_without_ref" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_reset('--hard'); SELECT 'HR|' || count(*) FROM dolt_conflicts_t; SELECT 'HV|' || v FROM t; ROLLBACK;" \
  "^HR\\|0$" "$DB7"

run_test_match "hard_reset_restores_head_row_after_conflict" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_reset('--hard'); SELECT 'HV|' || v FROM t; ROLLBACK;" \
  "^HV\\|main$" "$DB7"

DB8=/tmp/test_reset8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET v='feat'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET v='main'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB8" > /dev/null 2>&1
run_test_match "merge_conflicts_present_before_reset_guard" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT 'GC|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" \
  "^GC\\|1$" "$DB8"

run_test_match "no_arg_reset_rejected_during_merge_conflict" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_reset(); SELECT 'GC2|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" \
  "cannot merge: conflicts detected" "$DB8"

run_test_match "soft_reset_rejected_during_merge_conflict" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_reset('--soft'); SELECT 'GS|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" \
  "cannot merge: conflicts detected" "$DB8"

run_test_match "reset_guard_preserves_conflicts" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_reset('--soft'); SELECT 'GP|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" \
  "^GP\\|1$" "$DB8"

run_test_match "reset_guard_preserves_working_row" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_reset('--soft'); SELECT 'GV|' || v FROM t; ROLLBACK;" \
  "^GV\\|main$" "$DB8"

DB9=/tmp/test_reset9_$$.db; rm -f "$DB9"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); DROP TABLE a; SELECT dolt_reset('a');" | $DOLTLITE "$DB9" > /dev/null 2>&1
run_test "path_reset_dropped_table_stays_dropped" \
  "SELECT count(*) FROM dolt_status WHERE table_name='a' AND staged=0 AND status='deleted';" \
  "1" "$DB9"
run_test "path_reset_dropped_table_not_restored_on_reopen" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='a';" \
  "0" "$DB9"

DB10=/tmp/test_reset10_$$.db; rm -f "$DB10"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); DROP TABLE a; CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER); INSERT INTO a VALUES(7,70); SELECT dolt_reset('a');" | $DOLTLITE "$DB10" > /dev/null 2>&1
run_test "path_reset_recreated_table_keeps_live_schema" \
  "SELECT group_concat(name || ':' || type, '|') FROM pragma_table_info('a');" \
  "k:INTEGER|n:INTEGER" "$DB10"
run_test "path_reset_recreated_table_keeps_live_row" \
  "SELECT k || '|' || n FROM a;" \
  "7|70" "$DB10"

# Hard reset with untracked tables: restore tracked (incl. dropped), keep untracked indexes across numbering domains.
DB11=/tmp/test_reset11_$$.db; rm -f "$DB11"
$DOLTLITE "$DB11" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t1(a INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(a INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t3(a INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX i_t2 ON t2(v);
INSERT INTO t1 VALUES(1,'one'); INSERT INTO t2 VALUES(2,'two'); INSERT INTO t3 VALUES(3,'three');
SELECT dolt_commit('-A','-m','seed');
DROP TABLE t1;
UPDATE t2 SET v='MODIFIED';
CREATE TABLE u1(x INTEGER PRIMARY KEY, w TEXT);
CREATE INDEX i_u1 ON u1(w);
INSERT INTO u1 VALUES(9,'kept');
SELECT dolt_reset('--hard');
SQL
run_test "hard_reset_untracked_restores_dropped_table" \
  "SELECT v FROM t1;" "one" "$DB11"
run_test "hard_reset_untracked_reverts_modification" \
  "SELECT v FROM t2;" "two" "$DB11"
run_test "hard_reset_untracked_table_survives" \
  "SELECT w FROM u1 INDEXED BY i_u1 WHERE w='kept';" "kept" "$DB11"
run_test "hard_reset_untracked_status" \
  "SELECT group_concat(table_name || ':' || status) FROM dolt_status;" \
  "u1:new table" "$DB11"
run_test "hard_reset_untracked_integrity" \
  "PRAGMA integrity_check;" "ok" "$DB11"

DB12=/tmp/test_reset12_$$.db; rm -f "$DB12"
$DOLTLITE "$DB12" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX idx ON t(v);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
DELETE FROM t;
INSERT INTO t VALUES(2,'replacement');
ALTER TABLE t RENAME TO u;
SELECT dolt_reset('--hard');
SQL
run_test "hard_reset_drops_tracked_rename" \
  "SELECT group_concat(name || ':' || tbl_name, '|') FROM sqlite_schema WHERE name IN ('idx','t','u') ORDER BY name;" \
  "t:t|idx:t" "$DB12"
run_test "hard_reset_tracked_rename_integrity" \
  "PRAGMA integrity_check;" "ok" "$DB12"

DB13=/tmp/test_reset13_$$.db; rm -f "$DB13"
$DOLTLITE "$DB13" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','base');
CREATE TABLE s(id INTEGER PRIMARY KEY);
INSERT INTO s VALUES(2);
SELECT dolt_add('s');
CREATE TABLE u(id INTEGER PRIMARY KEY);
INSERT INTO u VALUES(3);
SELECT dolt_reset('--hard');
SQL
run_test "hard_reset_drops_staged_new_table" \
  "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='s';" \
  "0" "$DB13"
run_test "hard_reset_preserves_only_untracked_new_table" \
  "SELECT table_name || ':' || staged || ':' || status FROM dolt_status;" \
  "u:0:new table" "$DB13"
run_test "hard_reset_mixed_new_tables_integrity" \
  "PRAGMA integrity_check;" "ok" "$DB13"

DB14=/tmp/test_reset14_$$.db; rm -f "$DB14"
$DOLTLITE "$DB14" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','base');
INSERT INTO dolt_ignore VALUES('scratch_%',1);
INSERT INTO dolt_docs VALUES('README.md','hello');
INSERT INTO dolt_tests VALUES('t1','g','SELECT 1','expected_single_value','==','1');
CREATE TABLE newt(id INTEGER PRIMARY KEY);
CREATE TABLE dolt_custom(a INTEGER PRIMARY KEY);
INSERT INTO dolt_custom VALUES(7);
SELECT dolt_reset('--hard');
SQL
run_test "hard_reset_keeps_untracked_lazy_system_tables" \
  "SELECT group_concat(table_name,'|') FROM (SELECT table_name FROM dolt_status ORDER BY table_name);" \
  "dolt_custom|dolt_docs|dolt_ignore|dolt_tests|newt" "$DB14"
run_test "hard_reset_keeps_untracked_lazy_system_table_rows" \
  "SELECT (SELECT count(*) FROM dolt_ignore) || '|' || (SELECT count(*) FROM dolt_docs WHERE doc_name='README.md') || '|' || (SELECT count(*) FROM dolt_tests) || '|' || (SELECT count(*) FROM dolt_custom);" \
  "1|1|1|1" "$DB14"

DB15=/tmp/test_reset15_$$.db; rm -f "$DB15"
$DOLTLITE "$DB15" > /dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO dolt_ignore VALUES('keep_%',1);
INSERT INTO dolt_docs VALUES('README.md','committed');
SELECT dolt_commit('-A','-m','base');
INSERT INTO dolt_ignore VALUES('extra_%',0);
UPDATE dolt_docs SET doc_text='edited' WHERE doc_name='README.md';
SELECT dolt_reset('--hard');
SQL
run_test "hard_reset_reverts_tracked_lazy_system_tables" \
  "SELECT (SELECT count(*) FROM dolt_ignore) || '|' || (SELECT doc_text FROM dolt_docs WHERE doc_name='README.md') || '|' || (SELECT count(*) FROM dolt_status);" \
  "1|committed|0" "$DB15"

rm -f "$DB" "$DB2" "$DB3" "$DB3B" "$DB3C" "$DB4" "$DB5" "$DB5B" "$DB5C" "$DB5C.hash" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11" "$DB12" "$DB13" "$DB14" "$DB15"

dltest_finish
