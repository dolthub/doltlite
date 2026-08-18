#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Cherry-Pick & Revert Tests ==="
echo ""

DB=/tmp/test_cp_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','add feat_row');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_basic_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_basic_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_basic_val" "SELECT v FROM t WHERE id=2;" "feat_row" "$DB"
run_test_match "cp_basic_msg" "SELECT message FROM dolt_log LIMIT 1;" "^add feat_row$" "$DB"
run_test "cp_basic_branch" "SELECT active_branch();" "main" "$DB"
run_test "cp_basic_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_middle_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(10,'feat1');
SELECT dolt_commit('-A','-m','feat commit 1');
INSERT INTO t VALUES(11,'feat2');
SELECT dolt_commit('-A','-m','feat commit 2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
CP_HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB/feat" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_middle_pick" \
  "SELECT dolt_cherry_pick('$CP_HASH');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "cp_middle_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_middle_has10" "SELECT v FROM t WHERE id=10;" "feat1" "$DB"
run_test "cp_middle_no11" "SELECT count(*) FROM t WHERE id=11;" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_conflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE id=1;
SELECT dolt_commit('-A','-m','feat modifies row 1');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main modifies row 1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_conflict_msg" \
  "SELECT dolt_cherry_pick('feat');" \
  "conflict|rolled back" "$DB"
run_test "cp_conflict_resolved" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "cp_conflict_ours" "SELECT v FROM t WHERE id=1;" "main_val" "$DB"

DB=/tmp/test_cp_conflict_same_session_$$.db; rm -f "$DB"
TX_OUT=$({
cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_val' WHERE id=1;
SELECT dolt_commit('-A','-m','feat modifies row 1');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main modifies row 1');
SELECT dolt_cherry_pick('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_conflicts) || '|' ||
       (SELECT v FROM t WHERE id=1);
SQL
} | $DOLTLITE "$DB" 2>&1 | grep '^TX|')
if [ "$TX_OUT" = "TX|0|main_val" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: cp_conflict_same_session_summary_cleared\n  expected: TX|0|main_val\n  got:      $TX_OUT"
fi

rm -f "$DB"

DB=/tmp/test_cp_noconflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','feat adds row 3');
SELECT dolt_checkout('main');
UPDATE t SET v='A' WHERE id=1;
SELECT dolt_commit('-A','-m','main updates row 1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_noc_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "cp_noc_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_noc_row1" "SELECT v FROM t WHERE id=1;" "A" "$DB"
run_test "cp_noc_row3" "SELECT v FROM t WHERE id=3;" "c" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_errors_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_err_noarg" "SELECT dolt_cherry_pick();" "usage" "$DB"

run_test_match "cp_err_badhash" "SELECT dolt_cherry_pick('not_a_hash');" "invalid" "$DB"

run_test_match "cp_err_initial" \
  "SELECT dolt_cherry_pick((SELECT commit_hash FROM dolt_log WHERE message='Initialize data repository'));" \
  "initial commit" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_data');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cp_persist_count" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_persist_val" "SELECT v FROM t WHERE id=2;" "feat_data" "$DB"
run_test_match "cp_persist_log" "SELECT message FROM dolt_log LIMIT 1;" "^feat add$" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'added');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_before_count" "SELECT count(*) FROM t;" "2" "$DB"

run_test_match "rv_basic_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "rv_basic_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "rv_basic_val" "SELECT v FROM t WHERE id=1;" "init" "$DB"
run_test "rv_basic_no2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test_match "rv_basic_msg" "SELECT message FROM dolt_log LIMIT 1;" "^Revert \"add row 2\"$" "$DB"
run_test "rv_basic_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='changed' WHERE id=1;
SELECT dolt_commit('-A','-m','update row 1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_upd_before" "SELECT v FROM t WHERE id=1;" "changed" "$DB"

run_test_match "rv_upd_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "rv_upd_reverted" "SELECT v FROM t WHERE id=1;" "original" "$DB"
run_test "rv_upd_other" "SELECT v FROM t WHERE id=2;" "keep" "$DB"
run_test "rv_upd_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_middle_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'second');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'third');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_mid_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "rv_mid_no2" "SELECT count(*) FROM t WHERE id=2;" "0" "$DB"
run_test "rv_mid_has3" "SELECT v FROM t WHERE id=3;" "third" "$DB"
run_test "rv_mid_count" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_conflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='v2' WHERE id=1;
SELECT dolt_commit('-A','-m','update to v2');
UPDATE t SET v='v3' WHERE id=1;
SELECT dolt_commit('-A','-m','update to v3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_conf_msg" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "conflict|rolled back" "$DB"
run_test "rv_conf_count" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "rv_conf_ours" "SELECT v FROM t WHERE id=1;" "v3" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_errors_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_noarg_noop" "SELECT dolt_revert();" "0" "$DB"
run_test_match "rv_err_badhash" "SELECT dolt_revert('bad');" "invalid" "$DB"
run_test_match "rv_err_initial" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='Initialize data repository'));" \
  "initial commit" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'to_revert');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_persist_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test_match "rv_persist_log" "SELECT message FROM dolt_log LIMIT 1;" "Revert" "$DB"
run_test "rv_persist_log_count" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_rv_combo_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "combo_after_cp" "SELECT count(*) FROM t;" "2" "$DB"

run_test_match "combo_revert" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "combo_after_rv" "SELECT count(*) FROM t;" "1" "$DB"
run_test "combo_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(10,'cp1');
SELECT dolt_commit('-A','-m','feat1');
INSERT INTO t VALUES(11,'cp2');
SELECT dolt_commit('-A','-m','feat2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH1=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB/feat" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_cherry_pick('$HASH1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cp_multi_first" "SELECT count(*) FROM t;" "2" "$DB"
run_test "cp_multi_has10" "SELECT v FROM t WHERE id=10;" "cp1" "$DB"

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH2=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB/feat" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_cherry_pick('$HASH2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "cp_multi_both" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_multi_has11" "SELECT v FROM t WHERE id=11;" "cp2" "$DB"
run_test "cp_multi_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_double_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'added');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "rv_double_after1" "SELECT count(*) FROM t;" "1" "$DB"

run_test_match "rv_double_revert2" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_double_after2" "SELECT count(*) FROM t;" "2" "$DB"
run_test "rv_double_val" "SELECT v FROM t WHERE id=2;" "added" "$DB"
run_test "rv_double_log" "SELECT count(*) FROM dolt_log;" "5" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_diverged_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(10,'feat_row');
SELECT dolt_commit('-A','-m','feat commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(20,'main_row');
SELECT dolt_commit('-A','-m','main commit');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB/feat" 2>&1)
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_div_hash" "SELECT dolt_cherry_pick('$HASH');" "^[0-9a-f]{40}$" "$DB"
run_test "cp_div_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "cp_div_has10" "SELECT v FROM t WHERE id=10;" "feat_row" "$DB"
run_test "cp_div_has20" "SELECT v FROM t WHERE id=20;" "main_row" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_multirow_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'a');
INSERT INTO t VALUES(3,'b');
INSERT INTO t VALUES(4,'c');
SELECT dolt_commit('-A','-m','add 3 rows');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rv_multi_before" "SELECT count(*) FROM t;" "4" "$DB"

run_test_match "rv_multi_hash" \
  "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "^[0-9a-f]{40}$" "$DB"

run_test "rv_multi_after" "SELECT count(*) FROM t;" "1" "$DB"
run_test "rv_multi_only_init" "SELECT v FROM t WHERE id=1;" "init" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_newtable_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'new_table');
SELECT dolt_commit('-A','-m','feat: add t2');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_newtbl_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "cp_newtbl_t" "SELECT count(*) FROM t;" "1" "$DB"
run_test "cp_newtbl_t2" "SELECT count(*) FROM t2;" "1" "$DB"
run_test "cp_newtbl_val" "SELECT w FROM t2 WHERE id=1;" "new_table" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_violation_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1,1,'base1'),(2,2,'base2');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET u=9, v='feat2' WHERE id=2;
SELECT dolt_commit('-A','-m','feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main1' WHERE id=1;
SELECT dolt_commit('-A','-m','main_unique');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_violation_err" \
  "SELECT dolt_cherry_pick('feat');" \
  "constraint violations|rolled back" "$DB"
run_test "cp_violation_none" "SELECT count(*) FROM dolt_constraint_violations;" "0" "$DB"
run_test "cp_violation_state" \
  "SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id);" \
  "1:9:main1,2:2:base2" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_fk_tables_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_check');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_fk_tables_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_fk_tables_parent" "SELECT count(*) FROM p;" "1" "$DB"
run_test "cp_fk_tables_child" "SELECT count(*) FROM c;" "1" "$DB"
run_test "cp_fk_tables_fk" "SELECT count(*) FROM pragma_foreign_key_list('c');" "1" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_recreate_fk_family_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE c;
DROP TABLE p;
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE, label TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES(2,200,'x');
INSERT INTO c VALUES(2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_recreate_fk_family');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_check');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_recreate_fk_family_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_recreate_fk_family_parent" "SELECT count(*) FROM p;" "1" "$DB"
run_test "cp_recreate_fk_family_child" "SELECT count(*) FROM c;" "1" "$DB"
run_test "cp_recreate_fk_family_fk" "SELECT count(*) FROM pragma_foreign_key_list('c');" "1" "$DB"
run_test "cp_recreate_fk_family_schema" "SELECT instr(sql,'label TEXT')>0 FROM sqlite_master WHERE type='table' AND name='p';" "1" "$DB"
run_test "cp_recreate_fk_family_parent_unique_index_live" "SELECT count(*) FROM p INDEXED BY sqlite_autoindex_p_1 WHERE u=200;" "1" "$DB"
run_test "cp_recreate_fk_family_fk_check_clean" "SELECT count(*) FROM pragma_foreign_key_check;" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_self_ref_fk_$$.db; rm -f "$DB"
echo "PRAGMA foreign_keys=ON;
CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INT, FOREIGN KEY(parent_id) REFERENCES t(id) ON DELETE CASCADE);
INSERT INTO t VALUES(1,NULL),(2,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_add_descendant');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_add_root');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_self_ref_fk_hash" \
  "PRAGMA foreign_keys=ON; SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_self_ref_fk_delete_cascades" \
  "PRAGMA foreign_keys=ON; DELETE FROM t WHERE id=1; SELECT group_concat(id || ':' || ifnull(parent_id,-1), ',') FROM (SELECT id,parent_id FROM t ORDER BY id);" \
  "10:-1" "$DB"
run_test "cp_self_ref_fk_reopen_state" \
  "PRAGMA foreign_keys=ON; SELECT group_concat(id || ':' || ifnull(parent_id,-1), ',') FROM (SELECT id,parent_id FROM t ORDER BY id);" \
  "10:-1" "$DB"

rm -f "$DB"

DB=/tmp/test_cp_fk_chain_$$.db; rm -f "$DB"
echo "PRAGMA foreign_keys=ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(id INTEGER PRIMARY KEY, gp_id INT, FOREIGN KEY(gp_id) REFERENCES gp(id) ON DELETE CASCADE);
CREATE TABLE c(id INTEGER PRIMARY KEY, p_id INT, FOREIGN KEY(p_id) REFERENCES p(id) ON DELETE CASCADE);
INSERT INTO gp VALUES(1);
INSERT INTO p VALUES(1,1);
INSERT INTO c VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES(2,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_add_child');
SELECT dolt_checkout('main');
INSERT INTO gp VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_add_root');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_fk_chain_hash" \
  "PRAGMA foreign_keys=ON; SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_fk_chain_delete_cascades" \
  "PRAGMA foreign_keys=ON; DELETE FROM gp WHERE id=1; SELECT (SELECT count(*) FROM gp) || '|' || (SELECT count(*) FROM p) || '|' || (SELECT count(*) FROM c);" \
  "1|0|0" "$DB"
run_test "cp_fk_chain_reopen_state" \
  "PRAGMA foreign_keys=ON; SELECT (SELECT count(*) FROM gp) || '|' || (SELECT count(*) FROM p) || '|' || (SELECT count(*) FROM c);" \
  "1|0|0" "$DB"

rm -f "$DB"

DB=/tmp/test_rv_violation_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1,1,'base1'),(2,2,'base2');
SELECT dolt_commit('-A','-m','init');
UPDATE t SET u=9, v='c1' WHERE id=1;
SELECT dolt_commit('-A','-m','c1_set_9');
UPDATE t SET u=1, v='c2_take_1' WHERE id=2;
SELECT dolt_commit('-A','-m','c2_take_1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "rv_violation_err" \
  "SELECT dolt_revert('HEAD~1');" \
  "constraint violations|rolled back" "$DB"
run_test "rv_violation_none" "SELECT count(*) FROM dolt_constraint_violations;" "0" "$DB"
run_test "rv_violation_state" \
  "SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id);" \
  "1:9:c1,2:1:c2_take_1" "$DB"

rm -f "$DB"

# Cherry-pick / rebase must not treat a commit message starting with
# "Revert" as dolt_revert. That skipped index patches and left a DROP INDEX
# unapplied. Real dolt_revert still prefers ours and restores the index.
DB=/tmp/test_cp_revert_msg_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX t_v ON t(v);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP INDEX t_v;
SELECT dolt_commit('-A','-m','Revert leftover');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','main row');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "cp_revert_msg_hash" \
  "SELECT dolt_cherry_pick('feat');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "cp_revert_msg_drops_index" \
  "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v';" \
  "0" "$DB"
run_test "cp_revert_msg_keeps_rows" \
  "SELECT count(*) FROM t;" "2" "$DB"
rm -f "$DB"

DB=/tmp/test_rv_drop_index_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX t_v ON t(v);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
DROP INDEX t_v;
SELECT dolt_commit('-A','-m','drop idx');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "rv_drop_index_hash" \
  "SELECT dolt_revert('HEAD');" \
  "^[0-9a-f]{40}$" "$DB"
run_test "rv_drop_index_restored" \
  "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='t_v';" \
  "1" "$DB"
rm -f "$DB"

# A conflicted cherry-pick is not a merge: is_merging stays 0,
# dolt_merge('--abort') has nothing to abort, and checkout refuses
# because conflicts cannot be persisted (not because a merge is open).
DB=/tmp/test_cp_conflict_not_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='F';
SELECT dolt_commit('-A','-m','f');
SELECT dolt_checkout('main');
UPDATE t SET v='M';
SELECT dolt_commit('-A','-m','m');
SELECT dolt_branch('other');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "cp_conflict_not_a_merge" \
  "BEGIN;
   SELECT dolt_cherry_pick('feat');
   SELECT is_merging FROM dolt_merge_status;
   SELECT count(*) FROM dolt_conflicts;
   SELECT dolt_checkout('other');
   SELECT dolt_merge('--abort');" \
  "Error near line 2: Cherry-pick has 1 conflict(s). Resolve and then commit with dolt_commit.
0
1
Error near line 5: unresolved conflicts — resolve them or roll back first
Error near line 6: no merge in progress" \
  "$DB"
rm -f "$DB"

DB=/tmp/test_rv_conflict_not_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
UPDATE t SET v='b';
SELECT dolt_commit('-A','-m','b');
UPDATE t SET v='c';
SELECT dolt_commit('-A','-m','c');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "rv_conflict_not_a_merge" \
  "BEGIN;
   SELECT dolt_revert('HEAD~1');
   SELECT is_merging FROM dolt_merge_status;
   SELECT count(*) FROM dolt_conflicts;
   SELECT dolt_merge('--abort');" \
  "Error near line 2: Revert has 1 conflict(s). Resolve and then commit with dolt_commit.
0
1
Error near line 5: no merge in progress" \
  "$DB"
rm -f "$DB"

# A clean cherry-pick / revert is a transaction boundary like dolt_commit:
# it seals the enclosing BEGIN when it advances the ref. Leaving the
# transaction open let a later ROLLBACK revert the working set while the
# advanced ref stayed, splitting HEAD from the data.
DB=/tmp/test_cp_txn_seal_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','add feat_row');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_cherry_pick('feat');
ROLLBACK;
SELECT 'TX|' || (SELECT message FROM dolt_log LIMIT 1) || '|' || (SELECT count(*) FROM t) || '|' || (SELECT count(*) FROM dolt_status);" | $DOLTLITE "$DB" 2>&1)
TX_LINE=$(echo "$TX_OUT" | grep '^TX|')
if [ "$TX_LINE" = "TX|add feat_row|2|0" ]; then
  dltest_pass
else
  dltest_fail "cp_in_txn_seals" "  expected: TX|add feat_row|2|0\n  got:      $TX_LINE"
fi
if echo "$TX_OUT" | grep -q "cannot rollback - no transaction is active"; then
  dltest_pass
else
  dltest_fail "cp_in_txn_rollback_refused" "  expected the post-cherry-pick ROLLBACK to find no open transaction"
fi
rm -f "$DB"

DB=/tmp/test_rv_txn_seal_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'added');
SELECT dolt_commit('-A','-m','add row 2');" | $DOLTLITE "$DB" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));
ROLLBACK;
SELECT 'TX|' || (SELECT message FROM dolt_log LIMIT 1) || '|' || (SELECT count(*) FROM t) || '|' || (SELECT count(*) FROM dolt_status);" | $DOLTLITE "$DB" 2>&1)
TX_LINE=$(echo "$TX_OUT" | grep '^TX|')
if [ "$TX_LINE" = "TX|Revert \"add row 2\"|1|0" ]; then
  dltest_pass
else
  dltest_fail "rv_in_txn_seals" "  expected: TX|Revert \"add row 2\"|1|0\n  got:      $TX_LINE"
fi
if echo "$TX_OUT" | grep -q "cannot rollback - no transaction is active"; then
  dltest_pass
else
  dltest_fail "rv_in_txn_rollback_refused" "  expected the post-revert ROLLBACK to find no open transaction"
fi
rm -f "$DB"

dltest_finish
