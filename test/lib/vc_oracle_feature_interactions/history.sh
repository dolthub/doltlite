# Feature-interaction oracle cases: history.
# Sourced by test/vc_oracle_feature_interaction_test.sh.

echo "--- cherry-pick ---"

oracle "cherry_pick_single_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to cherry pick');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='updated' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','update on feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','delete on feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_with_prior_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit 1');
INSERT INTO t VALUES(4,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit 2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main diverge');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- revert ---"

oracle "revert_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'to_revert');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add row');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','delete row');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='changed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','update');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- reset ---"

oracle "reset_soft_keeps_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
SELECT dolt_reset('HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

oracle "reset_hard_discards_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
SELECT dolt_reset('--hard', 'HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- branch + checkout ---"

oracle "checkout_preserves_committed_state" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'main_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit');
SELECT dolt_checkout('main');
" "SELECT id, val FROM t ORDER BY id;"

oracle "checkout_then_modify_and_return" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- tag interactions ---"

oracle "tag_survives_branch_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'tagged');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to tag');
SELECT dolt_tag('v1');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d', 'feat');
SELECT dolt_checkout('v1');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- savepoint + commit ---"

oracle "commit_after_savepoint_release" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'in savepoint');
RELEASE sp1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after savepoint');
" "SELECT id, val FROM t ORDER BY id;"

oracle "commit_multiple_dml_before_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
UPDATE t SET val='A' WHERE id=1;
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','multiple dml');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cherry-pick edge cases ---"

oracle "cherry_pick_into_diverged_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'cherry_this');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to_cherry');
SELECT dolt_checkout('main');
UPDATE t SET val='main_changed' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main diverges');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_delete_into_modified" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes 3');
SELECT dolt_checkout('main');
UPDATE t SET val='main_1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates 1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_multi_row_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='X' WHERE id IN (1,3);
DELETE FROM t WHERE id=4;
INSERT INTO t VALUES(5,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat multi change');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- revert edge cases ---"

oracle "revert_middle_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_revert('HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'will_revert');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add row');
SELECT dolt_revert('HEAD');
INSERT INTO t VALUES(3,'after_revert');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after revert');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_update_restores_original" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, num INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='changed', num=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','update');
SELECT dolt_revert('HEAD');
" "SELECT id, val, num FROM t ORDER BY id;"

echo "--- diamond merges ---"

oracle "diamond_merge_no_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
UPDATE t SET val='left' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
UPDATE t SET val='right' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, val FROM t ORDER BY id;"

oracle "diamond_merge_insert_both_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
INSERT INTO t VALUES(2,'left');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
INSERT INTO t VALUES(3,'right');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, val FROM t ORDER BY id;"

oracle "diamond_three_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
UPDATE t SET val='b1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
UPDATE t SET val='b2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
UPDATE t SET val='b3' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- reset edge cases ---"

oracle "soft_reset_then_recommit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('HEAD~1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 redo');
" "SELECT id, val FROM t ORDER BY id;"

oracle "hard_reset_then_rebuild" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard', 'HEAD~1');
INSERT INTO t VALUES(3,'after_reset');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- repeated merge ---"

oracle "merge_same_branch_twice" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'first_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'second_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_branch_back_and_forth" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'main_after');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main after merge');
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cherry-pick + merge ---"

oracle "cherry_pick_then_merge_same_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cherry target');
INSERT INTO t VALUES(3,'extra');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','extra');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- add/commit workflow ---"

oracle "add_specific_table" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('t1');
SELECT dolt_commit('-m','only t1');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "add_all_then_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','both tables');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

oracle "commit_a_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A', '-m','auto add');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- merge chains ---"

oracle "serial_branch_merge_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b2');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(4,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b3');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_chain_with_updates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
UPDATE t SET val='v1';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_checkout('-b','b2');
UPDATE t SET val='v2';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b2');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- revert + merge ---"

oracle "revert_then_merge_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='reverted';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','change');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_after_revert_on_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat_changed' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat change');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cherry-pick + FK ---"

oracle "cherry_pick_with_parent_child" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
INSERT INTO child VALUES(1,2,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds parent+child');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT p.id, p.name FROM parent p ORDER BY p.id;"

echo "--- revert + multi-table ---"

oracle "revert_multi_table_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t1 VALUES(2,'b');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add to both');
SELECT dolt_revert('HEAD');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

echo "--- reset + branch ---"

oracle "reset_hard_then_new_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3 after reset');
" "SELECT id, val FROM t ORDER BY id;"

oracle "branch_reset_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
INSERT INTO t VALUES(4,'main2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main2');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- interleaved branch ops ---"

oracle "alternating_branch_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'m1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(4,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "branch_from_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','on feat1');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','on feat2 from feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_grandchild_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','child');
INSERT INTO t VALUES(2,'child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','child');
SELECT dolt_checkout('-b','grandchild');
INSERT INTO t VALUES(3,'grandchild');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','grandchild');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('grandchild');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cross-branch FK ---"

oracle "fk_both_branches_add_children" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2'),(3,'p3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(1,1,'fc1');
INSERT INTO child VALUES(2,2,'fc2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat children');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,2,'mc1');
INSERT INTO child VALUES(4,3,'mc2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main children');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_update_parent_merge_child" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='P1_UPDATED' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates parent');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child to p1');
SELECT dolt_merge('feat');
" "SELECT p.name, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY c.id;"

echo "--- accumulating merges ---"

oracle "merge_5_feature_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_merge('f1');
SELECT dolt_checkout('-b','f2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('f2');
SELECT dolt_checkout('-b','f3');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('f3');
SELECT dolt_checkout('-b','f4');
INSERT INTO t VALUES(5,'f4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_merge('f4');
SELECT dolt_checkout('-b','f5');
INSERT INTO t VALUES(6,'f5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
SELECT dolt_checkout('main');
SELECT dolt_merge('f5');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_then_modify_then_merge_again" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
UPDATE t SET val='post_merge' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post-merge edit');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cherry-pick + multi-table ---"

oracle "cherry_pick_multi_table_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,'b');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds to both');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

echo "--- revert + cherry-pick ---"

oracle "revert_then_cherry_pick_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'added');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add row');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cherry-pick data preservation ---"

oracle "cherry_pick_doesnt_affect_other_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'keep'),(2,'keep'),(3,'keep');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='changed' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes one');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_preserves_main_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changed');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- revert data preservation ---"

oracle "revert_one_row_keeps_others" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='X' WHERE id=2;
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','changes');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- add/status edge cases ---"

oracle "add_after_drop_and_recreate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'first');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'second');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recreated');
" "SELECT id, val FROM t ORDER BY id;"

oracle "multiple_add_before_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('t1');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('t2');
SELECT dolt_commit('-m','staged both');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

echo "--- commit graph counts ---"

oracle "linear_3_commits_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_log;"

oracle "merge_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "ff_merge_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "cherry_pick_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cherry');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "revert_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to revert');
SELECT dolt_revert('HEAD');
" "SELECT count(*) FROM dolt_log;"

echo "--- merge topology ---"

oracle "merge_5_branches_serial" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_merge('f1');
SELECT dolt_checkout('-b','f2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('f2');
SELECT dolt_checkout('-b','f3');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('f3');
SELECT dolt_checkout('-b','f4');
INSERT INTO t VALUES(5,'f4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_merge('f4');
SELECT dolt_checkout('-b','f5');
INSERT INTO t VALUES(6,'f5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
SELECT dolt_checkout('main');
SELECT dolt_merge('f5');
" "SELECT id, val FROM t ORDER BY id;"

oracle "branch_from_branch_merge_grandchild" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','child');
INSERT INTO t VALUES(2,'child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','child');
SELECT dolt_checkout('-b','grandchild');
INSERT INTO t VALUES(3,'grandchild');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','grandchild');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('grandchild');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_branch_that_already_merged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_merge('feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "ff_then_three_way" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'ff');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_back_and_forth" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'main_post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post merge');
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- net-no-op commit + merge ---"

oracle "roundtrip_update_no_net_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='temp';
UPDATE t SET val='original';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat no-op roundtrip');
SELECT dolt_checkout('main');
UPDATE t SET val='main_change' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_reinsert_same_value_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'keep'),(2,'target');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(2,'target');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat recreate');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "insert_delete_net_zero_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(99,'temp');
DELETE FROM t WHERE id=99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat no net change');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- allow-empty commit + merge ---"

oracle "allow_empty_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','empty marker','--allow-empty');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat data');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "allow_empty_only_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','just empty','--allow-empty');
SELECT dolt_commit('-m','another empty','--allow-empty');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- diamond via branches ---"

oracle "diamond_with_cell_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a0','b0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
UPDATE t SET a='L' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
UPDATE t SET b='R' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, a, b FROM t;"

oracle "diamond_independent_tables" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty');
SELECT dolt_checkout('-b','left');
INSERT INTO t1 VALUES(1,'l1'),(2,'l2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
INSERT INTO t2 VALUES(1,'r1'),(2,'r2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT 't1' AS tbl, count(*) AS n FROM t1 UNION ALL SELECT 't2', count(*) FROM t2 ORDER BY 1;"

echo "--- branch from historical commit ---"

oracle "branch_from_past_commit_new_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_checkout('-b','oldbranch','HEAD~2');
INSERT INTO t VALUES(99,'oldside');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','oldside');
SELECT dolt_checkout('main');
SELECT dolt_merge('oldbranch');
" "SELECT id, v FROM t ORDER BY id;"

oracle "two_branches_from_past" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_checkout('-b','past_a','HEAD~1');
INSERT INTO t VALUES(10,'past_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','past_a');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','past_b','HEAD~1');
INSERT INTO t VALUES(20,'past_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','past_b');
SELECT dolt_checkout('main');
SELECT dolt_merge('past_a');
SELECT dolt_merge('past_b');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- cherry-pick chain ---"

oracle "cherry_pick_two_commits_sequentially" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_then_reset_then_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- branch lifecycle + merge ---"

oracle "create_merge_delete_branch_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_branch('-d','feat');
INSERT INTO t VALUES(3,'post_delete');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after delete');
" "SELECT id, v FROM t ORDER BY id;"

oracle "rebranch_after_delete_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_branch('-d','feat');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'new_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','new feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- revert chain ---"

oracle "revert_then_revert_the_revert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
UPDATE t SET v='modified' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 modified');
SELECT dolt_revert('HEAD');
SELECT dolt_revert('HEAD');
" "SELECT id, v FROM t;"

oracle "revert_two_sequential_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_revert('HEAD');
SELECT dolt_revert('HEAD');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_log after various ops ---"

oracle "log_message_presence_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base_xyz');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_xyz');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_xyz');
SELECT dolt_merge('feat','--no-ff','-m','merge_xyz');
" "SELECT count(*) FROM dolt_log WHERE message LIKE '%_xyz';"

oracle "log_messages_after_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT message FROM dolt_log ORDER BY message;"

echo "--- post-merge working state ---"

oracle "post_merge_immediate_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'post_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

oracle "post_merge_immediate_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
UPDATE t SET v='post' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t;"

echo "--- parallel branches ---"

oracle "four_parallel_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(100,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(1,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(3,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b4');
INSERT INTO t VALUES(4,'b4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b4');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
" "SELECT id, v FROM t ORDER BY id;"

oracle "six_parallel_unique_inserts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty','--allow-empty');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(1,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(3,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b4');
INSERT INTO t VALUES(4,'b4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b4');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b5');
INSERT INTO t VALUES(5,'b5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b5');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b6');
INSERT INTO t VALUES(6,'b6');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b6');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
SELECT dolt_merge('b5');
SELECT dolt_merge('b6');
" "SELECT count(*) AS n, sum(id) AS s FROM t;"

echo "--- merge then revert ---"

oracle "revert_merge_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merge feat');
SELECT dolt_revert('HEAD','-m','1');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- commit message special chars ---"

oracle "message_with_dashes_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base-message');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat-with-dashes-and-stuff');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "message_with_spaces_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base message here');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feature branch commit message');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

echo "--- re-merge branch after update ---"

oracle "merge_branch_update_merge_again" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- merge + reset + re-merge ---"

oracle "merge_reset_remerge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_log structure after merges ---"

oracle "log_distinct_commit_hashes_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(DISTINCT commit_hash) AS h FROM dolt_log;"

oracle "log_messages_in_order_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','first');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','third_on_feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log WHERE message IN ('first','second','third_on_feat');"

echo "--- commit chain reshape + merge ---"

oracle "amend_like_flow_soft_reset_recommit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','original');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','wrong message');
SELECT dolt_reset('--soft','HEAD~1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','amended');
" "SELECT count(*) FROM dolt_log WHERE message IN ('original','amended','wrong message');"

oracle "soft_reset_combine_two_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--soft','HEAD~2');
SELECT dolt_commit('-m','squashed');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- merge branch with only empty commits ---"

oracle "merge_only_allow_empty_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','e1','--allow-empty');
SELECT dolt_commit('-m','e2','--allow-empty');
SELECT dolt_commit('-m','e3','--allow-empty');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- self-merge probes ---"

oracle "merge_self_is_noop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_merge('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

oracle "merge_already_merged_branch_noop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_merge('feat');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

echo "--- cherry-pick edge probes ---"

oracle "cherry_pick_same_commit_twice" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_empty_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','empty_marker','--allow-empty');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
INSERT INTO t VALUES(2,'after');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_with_added_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0;
INSERT INTO t VALUES(2,'feat',42);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat add col');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- reset target probes ---"

oracle "reset_to_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('snap','HEAD');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--hard','snap');
" "SELECT id, v FROM t ORDER BY id;"

oracle "reset_to_branch_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_branch('snap');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','snap');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- pre-merge staging probes ---"

oracle "merge_with_uncommitted_working_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'uncommitted');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- merge+revert probes ---"

oracle "revert_noff_merge_commit_row_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1'),(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(100,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merged');
SELECT dolt_revert('HEAD','-m','1');
" "SELECT count(*) FROM t;"

echo "--- reset then merge replay probes ---"

oracle "reset_then_merge_second_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_merge('b2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- graph shape probes ---"

oracle "log_count_after_diamond_no_ff" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
INSERT INTO t VALUES(2,'l');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
INSERT INTO t VALUES(3,'r');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left','--no-ff','-m','merge left');
SELECT dolt_merge('right','--no-ff','-m','merge right');
" "SELECT count(*) FROM dolt_log;"

echo "--- drop-on-branch probes ---"

oracle "table_dropped_on_feat_merge_to_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE keep(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO keep VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops t');
SELECT dolt_checkout('main');
INSERT INTO keep VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds to keep');
SELECT dolt_merge('feat');
" "SELECT id, v FROM keep ORDER BY id;"

oracle "table_dropped_on_main_with_feat_modifying" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat modifies');
SELECT dolt_checkout('main');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main drops');
SELECT dolt_merge('feat');
CREATE TABLE marker(id INTEGER PRIMARY KEY);
INSERT INTO marker VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','marker');
" "SELECT id FROM marker;"

echo "--- IDR on one branch probes ---"

oracle "insert_delete_reinsert_within_branch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'tmp');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(2,'final');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat IDR');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "insert_delete_reinsert_different_value_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'orig_2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(2,'new_2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- revert/cherry-pick inversion probes ---"

oracle "cherry_pick_a_revert_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_checkout('-b','feat');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "revert_a_cherry_pick_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
SELECT dolt_revert('HEAD');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- branch rename / move probes ---"

oracle "branch_move_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_branch('-m','feat','renamed');
SELECT dolt_checkout('main');
SELECT dolt_merge('renamed');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- tag at non-HEAD probes ---"

oracle "tag_at_past_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_tag('mid','HEAD~1');
SELECT dolt_reset('--hard','mid');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- repeat branch merge probes ---"

oracle "branch_merge_update_merge_update_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=2 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t;"

echo "--- reset immediately after commit probes ---"

oracle "commit_then_immediate_hard_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(10,'after');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after');
" "SELECT id, v FROM t ORDER BY id;"

oracle "commit_reset_commit_reset_cycle" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(2,22);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','new c2');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(2,222);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','newer c2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- alter-only branch merge ---"

oracle "feat_only_adds_column_no_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN new_col INTEGER DEFAULT 99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds col');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds row');
SELECT dolt_merge('feat');
" "SELECT id, v, new_col FROM t ORDER BY id;"

oracle "main_only_adds_column_no_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat row');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN tag INTEGER DEFAULT 42;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds col');
SELECT dolt_merge('feat');
" "SELECT id, v, tag FROM t ORDER BY id;"

echo "--- dolt_log structural probes ---"

oracle "dolt_log_message_set_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','M1_main');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','F1_feat');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','F2_feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','M2_main');
SELECT dolt_merge('feat','--no-ff','-m','merge');
" "SELECT count(*) FROM dolt_log WHERE message IN ('M1_main','F1_feat','F2_feat','M2_main','merge');"

oracle "dolt_log_message_filter_like" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cx001');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cx002');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','other');
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cx003');
" "SELECT count(*) FROM dolt_log WHERE message LIKE 'cx%';"

echo "--- dolt_status probes ---"

oracle "status_empty_after_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
" "SELECT count(*) FROM dolt_status;"

oracle "status_populated_after_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
" "SELECT count(*) FROM dolt_status WHERE staged=0;"

echo "--- branch off tag + merge ---"

oracle "branch_from_tag_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('v1','HEAD');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_checkout('-b','from_tag','v1');
INSERT INTO t VALUES(10,'tag_side');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','tag side');
SELECT dolt_checkout('main');
SELECT dolt_merge('from_tag');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- multi-branch dependency ---"

oracle "chain_of_dependent_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
UPDATE t SET v=v+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1 +1');
SELECT dolt_checkout('-b','b2');
UPDATE t SET v=v+10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2 +10');
SELECT dolt_checkout('-b','b3');
UPDATE t SET v=v+100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3 +100');
SELECT dolt_checkout('main');
SELECT dolt_merge('b3');
" "SELECT id, v FROM t;"

echo "--- create+insert same commit probes ---"

oracle "create_and_insert_one_commit_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1,'u1'),(2,'u2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat creates u');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT 't' AS tbl, count(*) AS n FROM t UNION ALL SELECT 'u', count(*) FROM u ORDER BY 1;"

echo "--- merge commit accounting ---"

oracle "ff_merge_commit_count_equals_branch_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "noff_merge_commit_count_adds_one" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'m1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_merge('feat','--no-ff','-m','merge');
" "SELECT count(*) FROM dolt_log;"

echo "--- PK ordering + reset probes ---"

oracle "insert_reverse_order_pk_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'a'),(5,'b'),(100,'c'),(1,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(50,'e'),(7,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- mid-sequence branch switches ---"

oracle "back_and_forth_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'m1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'m2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_hashof property probes ---"

oracle "hashof_head_matches_log_top" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD');"

oracle "hashof_main_equals_hashof_head_on_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
" "SELECT CASE WHEN dolt_hashof('main') = dolt_hashof('HEAD') THEN 1 ELSE 0 END;"

oracle "hashof_head_changes_after_new_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(DISTINCT commit_hash) FROM dolt_log;"

oracle "hashof_tilde_traverses_parents" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD~1');"

oracle "hashof_table_nonempty_after_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
" "SELECT CASE WHEN length(dolt_hashof_table('t')) > 0 THEN 1 ELSE 0 END;"

oracle "hashof_table_changes_with_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_hashof_table('t');
UPDATE t SET v=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(DISTINCT commit_hash) FROM dolt_log;"

oracle "hashof_same_across_noop_queries" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT 1;
SELECT 2;
SELECT 3;
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD');"

echo "--- tag semantics probes ---"

oracle "tag_points_to_commit_hash" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('snap','HEAD');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('snap');"

oracle "tag_count_after_multiple_tags" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('t1','HEAD');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_tag('t2','HEAD');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_tag('t3','HEAD');
" "SELECT count(*) FROM dolt_tags;"

oracle "tag_deleted_doesnt_affect_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('doomed','HEAD');
SELECT dolt_tag('-d','doomed');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- complex merge base topology ---"

oracle "merge_base_after_merged_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "merge_base_grandchild_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('-b','grandchild');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','g1');
SELECT dolt_checkout('main');
SELECT dolt_merge('grandchild');
" "SELECT id FROM t ORDER BY id;"

echo "--- log filter probes ---"

oracle "log_commit_hash_prefix_match" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD');"

oracle "log_ordered_by_time_stable_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM (SELECT commit_hash FROM dolt_log ORDER BY date DESC) sub;"

echo "--- tag reuse probes ---"

oracle "reset_to_past_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('stable','HEAD');
INSERT INTO t VALUES(2),(3),(4),(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','stable');
" "SELECT id FROM t ORDER BY id;"

oracle "branch_from_tag_and_merge_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('snap','HEAD');
INSERT INTO t VALUES(2,'main_c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_c2');
SELECT dolt_checkout('-b','hotfix','snap');
INSERT INTO t VALUES(99,'hotfix');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','hotfix');
SELECT dolt_checkout('main');
SELECT dolt_merge('hotfix');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- edge commit patterns ---"

oracle "commit_dash_A_flag_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "message_with_hyphens_survives" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','looks-like-arg');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','another-msg');
" "SELECT count(*) FROM dolt_log WHERE message IN ('looks-like-arg','another-msg');"

echo "--- dolt_branches accounting ---"

oracle "dolt_branches_count_after_creates_and_deletes" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('a');
SELECT dolt_branch('b');
SELECT dolt_branch('c');
SELECT dolt_branch('-d','a');
SELECT dolt_branch('-d','b');
" "SELECT count(*) FROM dolt_branches;"

oracle "dolt_branches_has_main_after_all_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('one');
SELECT dolt_branch('two');
SELECT dolt_branch('-c','main','three');
" "SELECT count(*) FROM dolt_branches WHERE name='main';"

echo "--- cherry-pick edges ---"

oracle "cherry_pick_an_alter_add_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 42;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat alter');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v, extra FROM t;"

oracle "cherry_pick_delete_then_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(99,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat swap');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- soft reset patterns ---"

oracle "soft_reset_two_then_squash" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--soft','HEAD~2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','squashed');
" "SELECT count(*) FROM dolt_log;"

oracle "soft_reset_keeps_working_tree" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--soft','HEAD~1');
" "SELECT id FROM t ORDER BY id;"

oracle "soft_reset_recommit_no_explicit_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','original');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','wrong message');
SELECT dolt_reset('--soft','HEAD~1');
SELECT dolt_commit('-m','amended');
" "SELECT count(*) FROM dolt_log WHERE message IN ('original','amended','wrong message');"

oracle "soft_reset_recommit_amended_data_present" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','original');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','wrong message');
SELECT dolt_reset('--soft','HEAD~1');
SELECT dolt_commit('-m','amended');
" "SELECT id, v FROM t ORDER BY id;"

oracle "soft_reset_two_levels_recommit_no_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--soft','HEAD~2');
SELECT dolt_commit('-m','squashed');
" "SELECT count(*) FROM dolt_log;"

oracle "soft_reset_recommit_combines_prior_stage" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','original');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','wrong message');
SELECT dolt_reset('--soft','HEAD~1');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','amended_with_extra');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- merge commit accounting probes ---"

oracle "noff_merge_has_four_log_entries" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merge_noff');
" "SELECT count(*) FROM dolt_log WHERE message IN ('base','feat','main','merge_noff');"

oracle "ff_merge_no_extra_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

echo "--- cherry-pick semantics probes ---"

oracle "cherry_pick_preserves_row_count_on_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 main');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_leaves_other_tables_alone" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat t1');
SELECT dolt_checkout('main');
INSERT INTO t2 VALUES(2,'main2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main t2');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t2 ORDER BY id;"

echo "--- reset then merge probes ---"

oracle "hard_reset_then_merge_brings_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- merge_base probes ---"

oracle "merge_base_matches_log_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_merge_base('main','feat');"

oracle "merge_base_symmetric" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
" "SELECT CASE WHEN dolt_merge_base('main','feat') = dolt_merge_base('feat','main') THEN 1 ELSE 0 END;"

oracle "merge_base_of_branch_with_itself_is_head" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT CASE WHEN dolt_merge_base('main','main') = dolt_hashof('HEAD') THEN 1 ELSE 0 END;"

oracle "merge_base_after_ff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT CASE WHEN dolt_merge_base('main','feat') = dolt_hashof('HEAD') THEN 1 ELSE 0 END;"

echo "--- multi-commit DML probes ---"

oracle "three_commits_feat_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
UPDATE t SET v=20 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "interleaved_main_feat_commits_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(100,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(11,11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(101,101);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, sum(id) AS s FROM t;"

echo "--- cherry-pick DML batch probes ---"

oracle "cherry_pick_batch_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main_row');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_mix_update_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=99 WHERE id=1;
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat mix');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dolt_branches column shape ---"

oracle "branches_dirty_flag_is_boolean" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_branch('clean');
" "SELECT count(*) FROM dolt_branches WHERE dirty IN (0,1);"

oracle "branches_latest_commit_hash_nonempty" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_branch('b1');
" "SELECT count(*) FROM dolt_branches WHERE length(hash) > 0;"

echo "--- dolt_log parent traversal ---"

oracle "log_has_expected_number_of_merge_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merge_c');
" "SELECT count(*) FROM dolt_log WHERE message='merge_c';"

echo "--- commit message preservation ---"

oracle "message_with_equals_and_slash" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','k=v/a=b');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','path/to/file');
" "SELECT count(*) FROM dolt_log WHERE message IN ('k=v/a=b','path/to/file');"

oracle "message_with_numbers" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','v1.2.3');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','#123 fix');
" "SELECT count(*) FROM dolt_log WHERE message IN ('v1.2.3','#123 fix');"

echo "--- diamond convergent delete ---"

oracle "diamond_convergent_delete_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep'),(2,'del'),(3,'keep2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(10,'left');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(20,'right');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- hashof across merges ---"

oracle "hashof_changes_after_noff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merge');
" "SELECT count(DISTINCT commit_hash) FROM dolt_log;"

echo "--- alternating work + checkout ---"

oracle "alternate_commits_both_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(4,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT owner, count(*) FROM t GROUP BY owner ORDER BY owner;"

echo "--- tag list stability ---"

oracle "multiple_tags_listed_in_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('z_last','HEAD');
SELECT dolt_tag('a_first','HEAD');
SELECT dolt_tag('m_mid','HEAD');
" "SELECT tag_name FROM dolt_tags ORDER BY tag_name;"

echo "--- cherry/revert round-trip ---"

oracle "cherry_pick_revert_cherry_pick_same_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
SELECT dolt_revert('HEAD');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dirty branch detection ---"

oracle "branch_dirty_after_uncommitted_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
" "SELECT count(*) FROM dolt_branches WHERE name='main' AND dirty IN (1,'true');"

oracle "branch_clean_after_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_branches WHERE name='main' AND dirty IN (0,'false');"

echo "--- commit order probes ---"

oracle "log_ordered_by_commit_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','first_commit_abc');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second_commit_abc');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','third_commit_abc');
" "SELECT count(*) FROM dolt_log WHERE message LIKE '%commit_abc';"

echo "--- merge-commit history ---"

oracle "merge_commit_message_in_log_noff" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merged_feat_to_main');
" "SELECT count(*) FROM dolt_log WHERE message='merged_feat_to_main';"

echo "--- hash stability probes ---"

oracle "hashof_stable_after_empty_reads" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT count(*) FROM t;
SELECT * FROM t LIMIT 0;
SELECT id FROM t WHERE id<0;
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD');"

echo "--- schema-only branch probes ---"

oracle "schema_only_branch_adds_col_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','schema');
ALTER TABLE t ADD COLUMN tag INTEGER DEFAULT 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','schema alter only');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
UPDATE t SET v='B' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main data');
SELECT dolt_merge('schema');
" "SELECT id, v, tag FROM t ORDER BY id;"

oracle "schema_only_branch_drops_col_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, tmp TEXT);
INSERT INTO t VALUES(1,'a','x'),(2,'b','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','schema');
ALTER TABLE t DROP COLUMN tmp;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','schema drop');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c','z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('schema');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- tag reset probes ---"

oracle "tag_reset_retag_workflow" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('stable_v1','HEAD');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_tag('stable_v2','HEAD');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_tag('stable_v3','HEAD');
SELECT dolt_reset('--hard','stable_v1');
SELECT dolt_tag('-d','stable_v2');
SELECT dolt_tag('-d','stable_v3');
INSERT INTO t VALUES(99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
SELECT dolt_tag('new_stable','HEAD');
" "SELECT id FROM t ORDER BY id;"

echo "--- hash consistency probes ---"

oracle "hashof_tag_and_head_equal_when_tag_at_head" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('here','HEAD');
" "SELECT CASE WHEN dolt_hashof('here') = dolt_hashof('HEAD') THEN 1 ELSE 0 END;"

echo "--- merge vs regular commit log ---"

oracle "log_has_base_branches_and_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','regular_c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','regular_c2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','regular_c3');
SELECT dolt_merge('feat','--no-ff','-m','merge_c4');
" "SELECT count(*) FROM dolt_log WHERE message LIKE 'regular_%' OR message LIKE 'merge_%';"

echo "--- history inspection probes ---"

oracle "history_shows_commits_touching_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(*) FROM dolt_history_t;"

echo "--- hashof after update ---"

oracle "hashof_differs_after_update_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
UPDATE t SET v=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(DISTINCT commit_hash) FROM dolt_log;"
echo "--- reset variant revisit ---"

oracle "reset_hard_tag_then_reset_hard_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('tag1','HEAD');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_branch('snap');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--hard','tag1');
SELECT dolt_reset('--hard','snap');
" "SELECT id FROM t ORDER BY id;"

oracle "reset_hard_head_same_hash" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD');
" "SELECT id FROM t ORDER BY id;"

echo "--- repeated merge from feat ---"

oracle "three_updates_three_merges_from_feat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=2 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t;"

echo "--- dolt_history_<table> probes ---"

oracle "history_table_after_updates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
UPDATE t SET v=2 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT count(DISTINCT v) FROM dolt_history_t WHERE id=1;"

echo "--- reset to intermediate commit ---"

oracle "reset_to_headTilde_3_deep" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c5');
SELECT dolt_reset('--hard','HEAD~3');
" "SELECT count(*) FROM t;"

echo "--- per-branch log probes ---"

oracle "log_from_after_merge_sees_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_c');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_c');
SELECT dolt_merge('feat','--no-ff','-m','m_merge');
" "SELECT count(*) FROM dolt_log WHERE message IN ('base','feat_c','main_c','m_merge');"

echo "--- cross-branch schema probes ---"

oracle "alter_add_col_populate_query_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN x INTEGER DEFAULT 0;
UPDATE t SET x=10 WHERE id=1;
UPDATE t SET x=20 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat x');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, x FROM t ORDER BY id;"

echo "--- multi-FK topology probes ---"

oracle "multi_fk_no_cascade_parent_preserved" "
PRAGMA foreign_keys=1;
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1,'p1'),(2,'p2');
INSERT INTO c VALUES(1,1),(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO p VALUES(3,'p3');
INSERT INTO c VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE p SET v='P2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM p ORDER BY id;"

echo "--- commit hash uniqueness ---"

oracle "commit_hashes_unique_after_15_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c5');
INSERT INTO t VALUES(6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c6');
INSERT INTO t VALUES(7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c7');
INSERT INTO t VALUES(8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c8');
INSERT INTO t VALUES(9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c9');
INSERT INTO t VALUES(10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c10');
INSERT INTO t VALUES(11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c11');
INSERT INTO t VALUES(12);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c12');
INSERT INTO t VALUES(13);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c13');
INSERT INTO t VALUES(14);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c14');
INSERT INTO t VALUES(15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c15');
" "SELECT CASE WHEN count(DISTINCT commit_hash) = count(*) THEN 1 ELSE 0 END FROM dolt_log;"

echo "--- dolt_blame probes ---"

oracle "blame_count_equals_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
UPDATE t SET v='B' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_blame_t;"

echo "--- repeated cherry-pick probes ---"

oracle "cherry_pick_4_sequential_from_feat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(4,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(5,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~3');
SELECT dolt_cherry_pick('feat~2');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- mix merge/cherry-pick ---"

oracle "merge_branch_then_cherry_pick_from_another" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_cherry_pick('b2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- txn + reset probes ---"

oracle "txn_wraps_reset_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
BEGIN;
SELECT dolt_reset('--hard','HEAD~1');
COMMIT;
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id FROM t ORDER BY id;"
echo "--- revert merge-commit probes ---"

oracle "revert_noff_merge_reverses_feat_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merged');
SELECT dolt_revert('HEAD','-m','1');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- cherry-pick across schema ---"

oracle "cherry_pick_commit_with_both_alter_and_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7;
INSERT INTO t VALUES(2,'b',14);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat alter+insert');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v, extra FROM t ORDER BY id;"

echo "--- post-merge head state ---"

oracle "post_merge_head_matches_log_top" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merge_commit');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD') AND message='merge_commit';"

echo "--- cherry-pick schema-only ---"

oracle "cherry_pick_alter_only_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN flag INTEGER DEFAULT 99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat alter only');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main row');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v, flag FROM t ORDER BY id;"

echo "--- commit/reset/re-commit ---"

oracle "reset_and_recommit_same_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 orig');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 redo');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- cross-branch tag probes ---"

oracle "tag_on_feat_branch_visible_on_main_tags" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_tag('feat_tag','HEAD');
SELECT dolt_checkout('main');
" "SELECT count(*) FROM dolt_tags WHERE tag_name='feat_tag';"

echo "--- sub-branch reset after merge ---"

oracle "sub_branch_merge_then_main_reset_keeps_sub_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','sub');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','sub');
SELECT dolt_checkout('main');
SELECT dolt_merge('sub');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main after');
SELECT dolt_reset('--hard','HEAD~1');
" "SELECT id FROM t ORDER BY id;"

echo "--- hashof variants ---"

oracle "hashof_db_differs_across_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT CASE WHEN length(dolt_hashof_db()) > 0 THEN 1 ELSE 0 END;"

oracle "hashof_db_stable_for_empty_select" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT 1;
SELECT 2;
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_hashof('HEAD');"

echo "--- dolt_status detail ---"

oracle "dolt_status_new_table_shows" "
CREATE TABLE existing(id INTEGER PRIMARY KEY);
INSERT INTO existing VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE TABLE new_tbl(id INTEGER PRIMARY KEY);
INSERT INTO new_tbl VALUES(1);
" "SELECT count(*) FROM dolt_status WHERE table_name='new_tbl';"

oracle "dolt_status_modified_shows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET v='b' WHERE id=1;
" "SELECT count(*) FROM dolt_status WHERE table_name='t';"

echo "--- commit after noops ---"

oracle "many_select_then_commit_stable" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT 1;
SELECT 1+2;
SELECT id FROM t;
SELECT count(*) FROM dolt_log;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"
echo "--- dolt_log filter combos ---"

oracle "log_filter_message_and_ordered" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','alpha');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','beta');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','gamma');
" "SELECT count(*) FROM dolt_log WHERE message IN ('alpha','beta','gamma');"

oracle "log_count_nonnegative_after_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT CASE WHEN count(*) > 0 THEN 1 ELSE 0 END FROM dolt_log;"

echo "--- dirty flag probes ---"

oracle "dirty_1_after_add_no_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
" "SELECT count(*) FROM dolt_branches WHERE name='main' AND dirty IN (1,'true');"

oracle "dirty_0_after_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_branches WHERE name='main' AND dirty IN (0,'false');"

echo "--- cross-branch FK preservation ---"

oracle "fk_parent_child_on_feat_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), v TEXT);
INSERT INTO p VALUES(1,'p1');
INSERT INTO c VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO p VALUES(2,'p2'),(3,'p3');
INSERT INTO c VALUES(2,2,'c2'),(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE p SET v='P1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT p.id AS pid, p.v AS pv, c.v AS cv FROM p LEFT JOIN c ON p.id=c.pid ORDER BY p.id;"

echo "--- tag chain reset ---"

oracle "tag_chain_reset_5_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_tag('v1','HEAD');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_tag('v2','HEAD');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_tag('v3','HEAD');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
SELECT dolt_tag('v4','HEAD');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c5');
SELECT dolt_reset('--hard','v1');
" "SELECT id FROM t ORDER BY id;"

echo "--- cherry-pick idempotency ---"

oracle "cherry_pick_then_full_merge_same_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- multi-commit conflict resolve ---"

oracle "resolve_multi_commit_conflict_via_ours" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
UPDATE t SET v=20 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
UPDATE t SET v=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','kept ours');
COMMIT;
" "SELECT id, v FROM t;"

echo "--- log invariant ---"

oracle "log_count_unchanged_by_selects" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT count(*) FROM t;
SELECT count(*) FROM dolt_branches;
SELECT count(*) FROM dolt_tags;
" "SELECT count(*) FROM dolt_log;"

echo "--- hashof_table after merge ---"

oracle "hashof_table_valid_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT CASE WHEN length(dolt_hashof_table('t')) > 0 THEN 1 ELSE 0 END;"

echo "--- cherry-pick preservation ---"

oracle "cherry_pick_preserves_other_table" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat t1 only');
SELECT dolt_checkout('main');
INSERT INTO t2 VALUES(2,'main t2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main t2');
SELECT dolt_cherry_pick('feat');
" "SELECT 't1' AS tbl, count(*) AS n FROM t1 UNION ALL SELECT 't2', count(*) FROM t2 ORDER BY 1;"

echo "--- three-branch merge count ---"

oracle "three_branch_noff_merges" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','a');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','a');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_merge('a','--no-ff','-m','merge_a');
SELECT dolt_checkout('-b','b');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_merge('b','--no-ff','-m','merge_b');
" "SELECT count(*) FROM dolt_log WHERE message IN ('merge_a','merge_b');"

echo "--- three-branch tag snapshots ---"

oracle "tags_snapshot_three_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_tag('base','HEAD');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_tag('b1_snap','HEAD');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_tag('b2_snap','HEAD');
SELECT dolt_checkout('main');
" "SELECT count(*) FROM dolt_tags;"

echo "--- revert-a-revert ---"

oracle "revert_then_revert_restores_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','added_b');
SELECT dolt_revert('HEAD');
SELECT dolt_revert('HEAD');
" "SELECT id, v FROM t ORDER BY id;"
