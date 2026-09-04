#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Branch Tests (Per-Session) ==="
echo ""
DB=/tmp/test_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "default_branch" "SELECT active_branch();" "main" "$DB"
run_test "create_branch" "SELECT dolt_branch('feature');" "0" "$DB"
run_test "list_branches" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test "main_current" "SELECT active_branch();" "main" "$DB"

run_test "checkout_feature" "SELECT dolt_checkout('feature');" "0" "$DB"
run_test "active_feature" "SELECT active_branch();" "feature" "$DB/feature"

echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','on feature');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1
run_test "data_on_feature" "SELECT count(*) FROM t;" "2" "$DB/feature"

run_test "checkout_main" "SELECT dolt_checkout('main');" "0" "$DB"
run_test "main_one_row" "SELECT count(*) FROM t;" "1" "$DB"

echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "feature_two_rows" "SELECT count(*) FROM t;" "2" "$DB/feature"

run_test_match "dup_branch" "SELECT dolt_branch('feature');" "already exists" "$DB"
run_test_match "del_current" "SELECT dolt_branch('-d','feature');" "cannot delete" "$DB/feature"
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "delete_unmerged_branch" "SELECT dolt_branch('-d','feature');" "not fully merged" "$DB"
run_test "force_delete_branch" "SELECT dolt_branch('-D','feature');" "0" "$DB"
run_test "one_branch" "SELECT count(*) FROM dolt_branches;" "1" "$DB"
run_test_match "checkout_gone" "SELECT dolt_checkout('feature');" "no such branch or table" "$DB"

DB2=/tmp/test_branch2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init'); UPDATE t SET v='dirty' WHERE id=1; SELECT dolt_checkout('t');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "checkout_table_persists_across_reopen" "SELECT v FROM t WHERE id=1;" "base" "$DB2"

DB2B=/tmp/test_branch2b_$$.db; rm -f "$DB2B"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_checkout('-b','feature'); INSERT INTO t VALUES(2,'feature'); SELECT dolt_commit('-A','-m','feature'); SELECT dolt_checkout('main'); SELECT dolt_checkout('feature','t');" | $DOLTLITE "$DB2B" > /dev/null 2>&1
run_test "checkout_table_from_branch_ref_persists_across_reopen" "SELECT count(*) FROM t;" "2" "$DB2B"

DB2C=/tmp/test_branch2c_$$.db; rm -f "$DB2C"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO a VALUES(1,'base_a'); INSERT INTO b VALUES(1,'base_b'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_tag('v1'); UPDATE a SET v='main_a' WHERE id=1; UPDATE b SET v='main_b' WHERE id=1; SELECT dolt_commit('-A','-m','c2'); SELECT dolt_checkout('v1','a','b'); CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t SELECT id, v FROM a UNION ALL SELECT id+10, v FROM b;" | $DOLTLITE "$DB2C" > /dev/null 2>&1
run_test "checkout_table_from_tag_ref_persists_across_reopen" "SELECT group_concat(v, ',') FROM (SELECT v FROM t ORDER BY id);" "base_a,base_b" "$DB2C"

DB2D=/tmp/test_branch2d_$$.db; rm -f "$DB2D"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO a VALUES(1,'base_a'); INSERT INTO b VALUES(1,'base_b'); SELECT dolt_commit('-A','-m','init'); UPDATE a SET v='main_a' WHERE id=1; UPDATE b SET v='main_b' WHERE id=1; SELECT dolt_commit('-A','-m','c2'); SELECT dolt_checkout(dolt_hashof('HEAD~1'),'a','b'); CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t SELECT id, v FROM a UNION ALL SELECT id+10, v FROM b;" | $DOLTLITE "$DB2D" > /dev/null 2>&1
run_test "checkout_table_from_raw_hash_persists_across_reopen" "SELECT group_concat(v, ',') FROM (SELECT v FROM t ORDER BY id);" "base_a,base_b" "$DB2D"

DB2E=/tmp/test_branch2e_$$.db; rm -f "$DB2E"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO a VALUES(1,'base_a'); INSERT INTO b VALUES(1,'base_b'); SELECT dolt_commit('-A','-m','init'); UPDATE a SET v='main_a' WHERE id=1; UPDATE b SET v='main_b' WHERE id=1; SELECT dolt_commit('-A','-m','c2'); SELECT dolt_checkout('HEAD^1','a','b'); CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t SELECT id, v FROM a UNION ALL SELECT id+10, v FROM b;" | $DOLTLITE "$DB2E" > /dev/null 2>&1
run_test "checkout_table_from_first_parent_ref_persists_across_reopen" "SELECT group_concat(v, ',') FROM (SELECT v FROM t ORDER BY id);" "base_a,base_b" "$DB2E"

DB2F=/tmp/test_branch2f_$$.db; rm -f "$DB2F"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO a VALUES(1,'base_a'); INSERT INTO b VALUES(1,'base_b'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_checkout('-b','feature'); INSERT INTO a VALUES(2,'feat_a'); INSERT INTO b VALUES(2,'feat_b'); SELECT dolt_commit('-A','-m','c2f'); SELECT dolt_checkout('main'); INSERT INTO a VALUES(3,'main_a'); INSERT INTO b VALUES(3,'main_b'); SELECT dolt_commit('-A','-m','c2m'); SELECT dolt_merge('feature'); SELECT dolt_checkout('HEAD^2','a','b'); CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t SELECT id, v FROM a UNION ALL SELECT id+10, v FROM b;" | $DOLTLITE "$DB2F" > /dev/null 2>&1
run_test "checkout_table_from_second_parent_ref_persists_across_reopen" "SELECT group_concat(v, ',') FROM (SELECT v FROM t ORDER BY id);" "base_a,feat_a,base_b,feat_b" "$DB2F"

DB2G=/tmp/test_branch2g_$$.db; rm -f "$DB2G"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO a VALUES(1,'base_a'); INSERT INTO b VALUES(1,'base_b'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_checkout('-b','feature'); INSERT INTO a VALUES(2,'feat_a'); INSERT INTO b VALUES(2,'feat_b'); SELECT dolt_commit('-A','-m','c2f'); SELECT dolt_checkout('main'); INSERT INTO a VALUES(3,'main_a'); INSERT INTO b VALUES(3,'main_b'); SELECT dolt_commit('-A','-m','c2m'); SELECT dolt_merge('feature'); SELECT dolt_checkout(dolt_hashof('HEAD^2'),'a','b'); CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t SELECT id, v FROM a UNION ALL SELECT id+10, v FROM b;" | $DOLTLITE "$DB2G" > /dev/null 2>&1
run_test "checkout_table_from_raw_second_parent_hash_persists_across_reopen" "SELECT group_concat(v, ',') FROM (SELECT v FROM t ORDER BY id);" "base_a,feat_a,base_b,feat_b" "$DB2G"

DB2H=/tmp/test_branch2h_$$.db; rm -f "$DB2H"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); DROP TABLE a; SELECT dolt_commit('-A','-m','drop a'); SELECT dolt_checkout('HEAD~1','a');" | $DOLTLITE "$DB2H" > /dev/null 2>&1
run_test "checkout_dropped_table_restores_rows_across_reopen" "SELECT count(*) FROM a;" "1" "$DB2H"
run_test "checkout_dropped_table_restores_schema_across_reopen" "SELECT group_concat(name || ':' || type, '|') FROM pragma_table_info('a');" "id:INTEGER|s:TEXT" "$DB2H"

DB2I=/tmp/test_branch2i_$$.db; rm -f "$DB2I"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO a VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); DROP TABLE a; CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER); INSERT INTO a VALUES(7,70); SELECT dolt_commit('-A','-m','recreate a'); SELECT dolt_checkout('HEAD~1','a');" | $DOLTLITE "$DB2I" > /dev/null 2>&1
run_test "checkout_recreated_table_restores_rows_across_reopen" "SELECT group_concat(id || ':' || s, ',') FROM a;" "1:base" "$DB2I"
run_test "checkout_recreated_table_restores_schema_across_reopen" "SELECT group_concat(name || ':' || type, '|') FROM pragma_table_info('a');" "id:INTEGER|s:TEXT" "$DB2I"

DB3=/tmp/test_branch3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','i');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('b2');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2);" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test "dirty_checkout" "SELECT dolt_checkout('b2');" "0" "$DB3"

DB4=/tmp/test_branch4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1

echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_add('-A'); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test "checkout_after_hard_reset" "SELECT dolt_checkout('feat');" "0" "$DB4"
run_test "active_after_hard_reset" "SELECT active_branch();" "feat" "$DB4/feat"

run_test "checkout_back_after_hard_reset" "SELECT dolt_checkout('main');" "0" "$DB4"

DB5=/tmp/test_branch5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_branch('b2');" | $DOLTLITE "$DB5" > /dev/null 2>&1
run_test "checkout_after_hard_reset_no_stage" "SELECT dolt_checkout('b2');" "0" "$DB5"

DB6=/tmp/test_branch6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO t VALUES(4); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('b3');" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test "checkout_after_multi_hard_reset" "SELECT dolt_checkout('b3');" "0" "$DB6"

DB7=/tmp/test_branch7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "INSERT INTO t VALUES(99);" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('b4');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test "dirty_after_hard_reset_new_changes" "SELECT dolt_checkout('b4');" "0" "$DB7"

DB8=/tmp/test_branch8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "CREATE TABLE extra(y); SELECT dolt_reset('--hard');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('b5');" | $DOLTLITE "$DB8" > /dev/null 2>&1
run_test "checkout_after_schema_change_hard_reset" "SELECT dolt_checkout('b5');" "0" "$DB8"

DB9=/tmp/test_branch9_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB9" > /dev/null 2>&1

run_test "checkout_b_creates" "SELECT dolt_checkout('-b','newbr');" "0" "$DB9"
run_test "checkout_b_active" "SELECT active_branch();" "newbr" "$DB9/newbr"
run_test "checkout_b_listed" "SELECT count(*) FROM dolt_branches;" "2" "$DB9"

echo "INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','on newbr');" | $DOLTLITE "$DB9/newbr" > /dev/null 2>&1
run_test "checkout_b_data" "SELECT count(*) FROM t;" "2" "$DB9/newbr"
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB9" > /dev/null 2>&1
run_test "checkout_b_main_data" "SELECT count(*) FROM t;" "1" "$DB9"

run_test_match "checkout_b_dup" "SELECT dolt_checkout('-b','main');" "already exists" "$DB9"
run_test_match "create_empty_branch_name" "SELECT dolt_branch('');" "branch name required" "$DB9"
run_test_match "copy_empty_source" "SELECT dolt_branch('-c','','copy');" "branch name required" "$DB9"
run_test_match "copy_empty_dest" "SELECT dolt_branch('-c','main','');" "branch name required" "$DB9"
run_test_match "move_empty_source" "SELECT dolt_branch('-m','','renamed');" "branch name required" "$DB9"
run_test_match "move_empty_dest" "SELECT dolt_branch('-m','main','');" "branch name required" "$DB9"
run_test_match "checkout_b_empty_name" "SELECT dolt_checkout('-b','');" "branch name required" "$DB9"

DB10=/tmp/test_branch10_$$.db; rm -f "$DB10"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "SELECT dolt_checkout('-b','other');" | $DOLTLITE "$DB10" > /dev/null 2>&1

run_test_match "delete_default_rejected" "SELECT dolt_branch('-d','main');" "cannot delete the default branch" "$DB10/other"
run_test_match "force_delete_default_rejected" "SELECT dolt_branch('-D','main');" "cannot delete the default branch" "$DB10/other"
run_test "rename_default_allowed" "SELECT dolt_branch('-m','main','trunk');" "0" "$DB10/other"
run_test "renamed_default_listed" "SELECT count(*) FROM dolt_branches WHERE name='trunk';" "1" "$DB10/other"
run_test "default_pointer_followed" "SELECT dolt_default_branch();" "trunk" "$DB10/other"
run_test "old_default_name_gone" "SELECT count(*) FROM dolt_branches WHERE name='main';" "0" "$DB10/other"

run_test "reopen_after_blocked_ops_active" "SELECT active_branch();" "other" "$DB10/other"

DB11=/tmp/test_branch11_$$.db; rm -f "$DB11"
echo "CREATE TABLE t(x INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB11" > /dev/null 2>&1
run_test "rename_non_main_works" "SELECT dolt_branch('-m','feat','renamed');" "0" "$DB11"
run_test "renamed_listed" "SELECT count(*) FROM dolt_branches WHERE name='renamed';" "1" "$DB11"
run_test "delete_non_main_works" "SELECT dolt_branch('-d','renamed');" "0" "$DB11"

run_test "copy_from_main_works" "SELECT dolt_branch('-c','main','snapshot');" "0" "$DB11"

DB12=/tmp/test_branch12_$$.db; rm -f "$DB12"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); INSERT INTO t VALUES(2,'c2'); SELECT dolt_commit('-A','-m','c2'); SELECT dolt_tag('v1','HEAD~1'); SELECT dolt_branch('from_tag','v1');" | $DOLTLITE "$DB12" > /dev/null 2>&1
run_test "branch_from_tag_persists_across_reopen" "SELECT count(*) FROM t;" "1" "$DB12/from_tag"

DB13=/tmp/test_branch13_$$.db; rm -f "$DB13"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','c1'); SELECT dolt_branch('feature'); SELECT dolt_checkout('feature'); INSERT INTO t VALUES(2,'feat'); SELECT dolt_commit('-A','-m','feat'); SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,'main'); SELECT dolt_commit('-A','-m','main'); SELECT dolt_merge('feature'); SELECT dolt_branch('from_p1','HEAD^1'); SELECT dolt_branch('from_p2','HEAD^2'); SELECT dolt_branch('from_hash', dolt_hashof('HEAD^2'));" | $DOLTLITE "$DB13" > /dev/null 2>&1
run_test "branch_from_first_parent_ref_persists_across_reopen" "SELECT count(*) FROM t;" "2" "$DB13/from_p1"
run_test "branch_from_second_parent_ref_persists_across_reopen" "SELECT count(*) FROM t;" "2" "$DB13/from_p2"
run_test "branch_from_second_parent_hash_persists_across_reopen" "SELECT count(*) FROM t;" "2" "$DB13/from_hash"
run_test_match "checkout_raw_hash_refuses_detached_head" "SELECT dolt_checkout(dolt_hashof('HEAD^2'));" "does not support a detached head state" "$DB13"
run_test_match "checkout_raw_hash_suggests_branch" "SELECT dolt_checkout(dolt_hashof('HEAD^2'));" "To create a branch at this commit instead" "$DB13"
run_test "checkout_b_raw_hash_documented_order" "SELECT dolt_checkout(dolt_hashof('HEAD^2'),'-b','checkout_from_hash');" "0" "$DB13"
run_test "checkout_b_raw_hash_documented_order_rows" "SELECT count(*) FROM t;" "2" "$DB13/checkout_from_hash"
run_test "checkout_b_raw_hash_flag_first" "SELECT dolt_checkout('-b','checkout_from_hash_flag_first',dolt_hashof('HEAD^1'));" "0" "$DB13"
run_test "checkout_b_raw_hash_flag_first_rows" "SELECT count(*) FROM t;" "2" "$DB13/checkout_from_hash_flag_first"

DB14=/tmp/test_branch14_$$.db; rm -f "$DB14"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('feature'); SELECT dolt_checkout('feature'); UPDATE t SET v='working' WHERE id=1; SELECT dolt_checkout('main');" | $DOLTLITE "$DB14" > /dev/null 2>&1
run_test "inactive_branch_unstaged_changes_are_dirty" "SELECT dirty FROM dolt_branches WHERE name='feature';" "1" "$DB14"
run_test "inactive_branch_peer_remains_clean" "SELECT dirty FROM dolt_branches WHERE name='main';" "0" "$DB14"
echo "SELECT dolt_checkout('feature'); SELECT dolt_add('-A'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB14" > /dev/null 2>&1
run_test "inactive_branch_staged_changes_are_dirty" "SELECT dirty FROM dolt_branches WHERE name='feature';" "1" "$DB14"
echo "SELECT dolt_checkout('feature'); SELECT dolt_commit('-m','feature change'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB14" > /dev/null 2>&1
run_test "inactive_branch_committed_changes_are_clean" "SELECT dirty FROM dolt_branches WHERE name='feature';" "0" "$DB14"

DB15=/tmp/test_branch15_$$.db; rm -f "$DB15"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('one'); SELECT dolt_branch('two');" | $DOLTLITE "$DB15" > /dev/null 2>&1
run_test "delete_multiple_branches" "SELECT dolt_branch('-d','one','two');" "0" "$DB15"
run_test "delete_multiple_branches_removed" "SELECT count(*) FROM dolt_branches WHERE name IN ('one','two');" "0" "$DB15"
echo "SELECT dolt_branch('one'); SELECT dolt_branch('two');" | $DOLTLITE "$DB15" > /dev/null 2>&1
run_test_match "delete_multiple_missing_is_atomic" "SELECT dolt_branch('-d','one','missing','two');" "not found" "$DB15"
run_test "delete_multiple_missing_keeps_branches" "SELECT count(*) FROM dolt_branches WHERE name IN ('one','two');" "2" "$DB15"
echo "SELECT dolt_checkout('one'); INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','one'); SELECT dolt_checkout('main');" | $DOLTLITE "$DB15" > /dev/null 2>&1
run_test "force_delete_multiple_branches" "SELECT dolt_branch('-D','one','two');" "0" "$DB15"
run_test "force_delete_multiple_branches_removed" "SELECT count(*) FROM dolt_branches WHERE name IN ('one','two');" "0" "$DB15"

DB16=/tmp/test_branch16_$$.db; rm -f "$DB16"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('feat');" | $DOLTLITE "$DB16" > /dev/null 2>&1
run_test_match "create_case_variant_refused" "SELECT dolt_branch('Feat');" "already exists" "$DB16"
run_test_match "copy_to_case_variant_refused" "SELECT dolt_branch('-c','main','MAIN');" "already exists" "$DB16"
run_test_match "force_create_case_variant_refused" "SELECT dolt_branch('-f','Main','main');" "already exists" "$DB16"
run_test_match "checkout_b_case_variant_refused" "SELECT dolt_checkout('-b','MAIN');" "already exists" "$DB16"
run_test_match "move_onto_case_variant_refused" "SELECT dolt_branch('-m','feat','MAIN');" "already exists" "$DB16"
run_test_match "force_move_onto_case_variant_refused" "SELECT dolt_branch('-m','-f','feat','MAIN');" "already exists" "$DB16"
run_test "case_variant_attempts_created_nothing" \
  "SELECT group_concat(name,'|') FROM (SELECT name FROM dolt_branches ORDER BY name);" \
  "feat|main" "$DB16"
run_test "distinct_name_still_creates" "SELECT dolt_branch('feature2');" "0" "$DB16"

DB17=/tmp/test_branch17_$$.db; rm -f "$DB17"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init'); SELECT dolt_branch('feat');" | $DOLTLITE "$DB17" > /dev/null 2>&1
run_test "move_recases_own_name" "SELECT dolt_branch('-m','feat','FEAT');" "0" "$DB17"
run_test "move_recased_branch_listed_once" \
  "SELECT group_concat(name,'|') FROM (SELECT name FROM dolt_branches ORDER BY name);" \
  "FEAT|main" "$DB17"
run_test "move_recases_default_branch" "SELECT dolt_branch('-m','main','MAIN');" "0" "$DB17"
run_test "recased_default_branch_listed_once" \
  "SELECT group_concat(name,'|') FROM (SELECT name FROM dolt_branches ORDER BY name);" \
  "FEAT|MAIN" "$DB17"

rm -f "$DB" "$DB2" "$DB2B" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11" "$DB12" "$DB13" "$DB14" "$DB15" "$DB16" "$DB17"
dltest_finish
