# Feature-interaction oracle cases: merge.
# Sourced by test/vc_oracle_feature_interaction_test.sh.

echo "--- merge + DML combinations ---"

oracle "merge_insert_both_different_keys" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat insert');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_update_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_update_different_cols_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='FEAT_A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update a');
SELECT dolt_checkout('main');
UPDATE t SET b='MAIN_B' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update b');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "merge_delete_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_delete_both_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main delete');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_insert_update_delete_mix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d');
UPDATE t SET val='B_FEAT' WHERE id=2;
DELETE FROM t WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat mix');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'e');
UPDATE t SET val='A_MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main mix');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- multi-table merges ---"

oracle "merge_two_tables_independent" "
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
INSERT INTO t1 VALUES(3,'c');
INSERT INTO t2 VALUES(3,'z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds to both');
SELECT dolt_merge('feat');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

oracle "merge_new_table_on_branch" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t2 VALUES(1,'new_table');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat creates t2');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies t1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "merge_drop_table_on_branch" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP TABLE t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops t2');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies t1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

echo "--- fast-forward vs three-way ---"

oracle "ff_merge_no_divergence" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat only');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "noff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat', '--no-ff', '-m', 'merge feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- sequential merges ---"

oracle "two_merges_same_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat1');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_after_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to cherry');
INSERT INTO t VALUES(3,'not cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','extra');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- empty/boundary conditions ---"

oracle "merge_empty_table_both_add_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds to empty');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds to empty');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_one_side_empty_other_inserts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat inserts');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_empty_diff" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','noop commit', '--allow-empty');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- convergent modifications ---"

oracle "both_sides_same_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'old');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='same_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='same_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "both_sides_insert_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "both_sides_delete_and_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,'reinserted');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main_changed' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- negative numbers and zero ---"

oracle "merge_negative_ids" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(-1,'neg'),(0,'zero'),(1,'pos');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=-1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_zero_and_null_distinction" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
INSERT INTO t VALUES(1, 0, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=0 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- multi-table complex ---"

oracle "merge_3_tables_mixed_ops" "
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, uid INTEGER, amount INTEGER);
CREATE TABLE items(id INTEGER PRIMARY KEY, oid INTEGER, name TEXT);
INSERT INTO users VALUES(1,'alice'),(2,'bob');
INSERT INTO orders VALUES(1,1,100),(2,2,200);
INSERT INTO items VALUES(1,1,'widget'),(2,2,'gadget');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO users VALUES(3,'charlie');
INSERT INTO orders VALUES(3,3,300);
UPDATE items SET name='WIDGET' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds user+order, updates item');
SELECT dolt_checkout('main');
UPDATE users SET name='ALICE' WHERE id=1;
DELETE FROM orders WHERE id=2;
INSERT INTO items VALUES(3,1,'accessory');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates user, deletes order, adds item');
SELECT dolt_merge('feat');
" "SELECT 'u' AS t, id, name FROM users UNION ALL SELECT 'i', id, name FROM items ORDER BY 1, 2;"

oracle "merge_with_unrelated_table_insert" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a'),(2,'b');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t1 SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates t1');
SELECT dolt_checkout('main');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main inserts t2');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

echo "--- idempotent operations ---"

oracle "update_to_same_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='same' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','noop update');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_nonexistent_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE id=999;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','noop delete');
" "SELECT id, val FROM t ORDER BY id;"

oracle "insert_delete_same_row_before_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'temp');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','insert then delete');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- complex row operations merge ---"

oracle "feat_inserts_main_deletes_no_overlap" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(6,'f'),(7,'g'),(8,'h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat inserts');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id IN (4,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main deletes');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "both_update_all_rows_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1'),(2,'a2','b2'),(3,'a3','b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='FA';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates all a');
SELECT dolt_checkout('main');
UPDATE t SET b='MB';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates all b');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "interleaved_insert_ids" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f'),(4,'f'),(6,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat even ids');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'m'),(5,'m'),(7,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main odd ids');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "update_same_rows_different_values_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, z INTEGER);
INSERT INTO t VALUES(1,0,0,0),(2,0,0,0),(3,0,0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x=1 WHERE id=1;
UPDATE t SET x=2 WHERE id=2;
UPDATE t SET x=3 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat x');
SELECT dolt_checkout('main');
UPDATE t SET y=10 WHERE id=1;
UPDATE t SET y=20 WHERE id=2;
UPDATE t SET z=30 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main y,z');
SELECT dolt_merge('feat');
" "SELECT id, x, y, z FROM t ORDER BY id;"

echo "--- auto-increment patterns ---"

oracle "merge_with_max_id_pattern" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'feat4'),(5,'feat5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat auto ids');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,'main6'),(7,'main7');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main auto ids');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- merge row dedup ---"

oracle "no_duplicates_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'shared'),(2,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "no_duplicates_convergent_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'same'),(2,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,'same'),(2,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- merge data integrity ---"

oracle "merge_sum_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount INTEGER);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base sum=600');
SELECT dolt_checkout('-b','feat');
UPDATE t SET amount=150 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes 1');
SELECT dolt_checkout('main');
UPDATE t SET amount=250 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes 2');
SELECT dolt_merge('feat');
" "SELECT sum(amount) FROM t;"

oracle "merge_min_max_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=5 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat lowers min');
SELECT dolt_checkout('main');
UPDATE t SET val=60 WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main raises max');
SELECT dolt_merge('feat');
" "SELECT min(val), max(val) FROM t;"

echo "--- batch operations + merge ---"

oracle "batch_insert_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'k'),(12,'l'),(13,'m'),(14,'n'),(15,'o');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main batch');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_delete_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch delete odds');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main batch delete evens');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_update_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','x'),(2,'a','x'),(3,'a','x'),(4,'a','x'),(5,'a','x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id IN (1,2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id IN (3,4,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- conflict boundaries ---"

oracle "convergent_same_field_same_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='converge' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='converge' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "convergent_both_set_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'notnull');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cell_merge_null_to_value_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1, NULL, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='feat_a' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "cell_merge_value_to_null_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'val_a','val_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='new_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "conflict_same_field_safe_rows_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'conflict_target'),(2,'safe'),(3,'safe2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t WHERE id>=2 ORDER BY id;"

oracle "conflict_null_vs_value_same_field" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, other TEXT);
INSERT INTO t VALUES(1,'orig','keep');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='different' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, other FROM t WHERE id=1;"

oracle "delete_modify_conflict_safe_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'target'),(2,'safe'),(3,'safe2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='modified' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t WHERE id>=2 ORDER BY id;"

echo "--- undo patterns ---"

echo "--- batch operations ---"

oracle "batch_insert_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f'),(3,'f'),(4,'f'),(5,'f'),(6,'f'),(7,'f'),(8,'f'),(9,'f'),(10,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat 9 rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'m'),(12,'m'),(13,'m'),(14,'m'),(15,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main 5 rows');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_delete_disjoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_update_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','x'),(2,'a','x'),(3,'a','x'),(4,'a','x'),(5,'a','x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id<=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id>=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- HEAD~N refs ---"

oracle "reset_to_head_tilde_2" "
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
SELECT dolt_reset('--hard','HEAD~2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "merge_branch_after_reset_head_tilde" "
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
SELECT dolt_checkout('-b','side','HEAD~1');
INSERT INTO t VALUES(99,'side');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','side commit');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- LIMIT/OFFSET after merge ---"

oracle "select_limit_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id LIMIT 3;"

oracle "select_limit_offset_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id LIMIT 2 OFFSET 2;"

echo "--- EXISTS/NOT EXISTS after merge ---"

oracle "exists_subquery_after_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref_id INTEGER);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t2 VALUES(2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t1 WHERE EXISTS(SELECT 1 FROM t2 WHERE t2.ref_id=t1.id) ORDER BY id;"

oracle "not_exists_after_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref_id INTEGER);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t2 VALUES(2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t1 WHERE NOT EXISTS(SELECT 1 FROM t2 WHERE t2.ref_id=t1.id) ORDER BY id;"

oracle "update_where_not_in_subquery_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE exclude(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');
INSERT INTO exclude VALUES(2),(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='FLAGGED' WHERE id NOT IN (SELECT id FROM exclude);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO exclude VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- self-referential merge ---"

oracle "self_ref_fk_new_hierarchy_merge" "
CREATE TABLE n(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES n(id), v TEXT);
INSERT INTO n VALUES(1,NULL,'root'),(2,1,'a'),(3,1,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO n VALUES(4,2,'a-a'),(5,2,'a-b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO n VALUES(6,3,'b-a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, parent_id, v FROM n ORDER BY id;"

echo "--- convergent conflict probes ---"

oracle "convergent_update_same_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig'),(2,'keep');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='SAME' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat same');
SELECT dolt_checkout('main');
UPDATE t SET v='SAME' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main same');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "convergent_delete_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat del');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main del');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "convergent_insert_identical_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(5,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat ins');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main ins');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "delete_modify_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat del');
SELECT dolt_checkout('main');
UPDATE t SET v='mod' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main mod');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t WHERE id=2;"

oracle "update_different_cols_same_row_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'a0','b0','c0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='FEAT_A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='MAIN_B', c='MAIN_C' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t;"

oracle "conflicting_update_same_col_different_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig'),(2,'unaffected');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t WHERE id=2;"

echo "--- empty/no-op merge probes ---"

oracle "merge_branch_with_no_new_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'main_only');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- cell merge fallback probes ---"

oracle "three_cols_updated_across_sides_no_overlap" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT);
INSERT INTO t VALUES(1,'a0','b0','c0','d0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='A_FEAT', b='B_FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET c='C_MAIN', d='D_MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c, d FROM t;"

oracle "overlapping_col_update_same_new_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a0','b0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='SAME' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET a='SAME', b='B_MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t;"

echo "--- batch + partial conflict probes ---"

oracle "batch_50_with_one_conflict_elsewhere" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'conflict_target');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='FEAT' WHERE id=1;
INSERT INTO t VALUES(100,'f'),(101,'f'),(102,'f'),(103,'f'),(104,'f'),(105,'f'),(106,'f'),(107,'f'),(108,'f'),(109,'f');
INSERT INTO t VALUES(110,'f'),(111,'f'),(112,'f'),(113,'f'),(114,'f'),(115,'f'),(116,'f'),(117,'f'),(118,'f'),(119,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t WHERE id BETWEEN 100 AND 119;"

echo "--- stale working set probes ---"

oracle "insert_commit_reset_hard_uncommitted_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'uncommitted');
SELECT dolt_reset('--hard','HEAD~1');
" "SELECT id, v FROM t ORDER BY id;"

oracle "stage_insert_reset_hard_wipes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'staged');
SELECT dolt_add('-A');
SELECT dolt_reset('--hard','HEAD');
INSERT INTO t VALUES(3,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- dialect edge probes ---"

oracle "integer_stored_text_update_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, num TEXT);
INSERT INTO t VALUES(1,'100'),(2,'200');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET num='300' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET num='999' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, num FROM t ORDER BY id;"

oracle "negative_int_update_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,-10),(2,-20),(3,-30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=-100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=-200 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- multi-col CHECK probes ---"

oracle "check_on_two_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, CHECK(a <= b));
INSERT INTO t VALUES(1,1,10),(2,2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=50 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- dolt_diff stability probes ---"

oracle "dolt_diff_summary_row_count_stable" "
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
" "SELECT count(*) FROM dolt_diff WHERE table_name='t';"

echo "--- explicit transaction probes ---"

oracle "begin_commit_across_dolt_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2,'txn');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','inside txn');
COMMIT;
INSERT INTO t VALUES(3,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- complex flow final state probes ---"

oracle "complex_flow_final_state" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
UPDATE t SET v='UPD' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c3');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- repeated ff / noff probes ---"

oracle "ff_same_branch_many_times" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
UPDATE t SET v=2 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t;"

echo "--- conflict resolution in txn probes ---"

oracle "resolve_conflict_via_update_their_in_txn" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
UPDATE t SET v='resolved' WHERE id=1;
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','resolved');
COMMIT;
" "SELECT id, v FROM t;"

oracle "resolve_conflict_commit_conflicts_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SET @@dolt_allow_commit_conflicts=1;
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard','HEAD');
INSERT INTO t VALUES(2,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','clean post');
" "SELECT id, v FROM t ORDER BY id;"

oracle "txn_merge_rollback_leaves_clean_state" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
ROLLBACK;
INSERT INTO t VALUES(2,'post_rollback');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- unicode sort probes ---"

oracle "unicode_text_sort_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(32));
INSERT INTO t VALUES(1,'alpha'),(2,'ALPHA'),(3,'beta');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'gamma'),(5,'GAMMA');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY v, id;"

oracle "ascii_punctuation_sort_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(32));
INSERT INTO t VALUES(1,'a-b'),(2,'a_b'),(3,'ab');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a!b'),(5,'a.b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY v;"

echo "--- post-merge invariant probes ---"

oracle "row_count_after_ff_merge_unchanged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'c'),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "row_count_after_three_way_merge_disjoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(100),(101);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10),(11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- multi-hop merge-base probes ---"

oracle "two_feature_branches_merged_sequentially" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
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
" "SELECT id, v FROM t ORDER BY id;"

oracle "merge_chain_triangle_resolution" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','a');
UPDATE t SET v=1 WHERE id=1;
INSERT INTO t VALUES(2,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','a_c');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_c');
SELECT dolt_merge('a');
SELECT dolt_checkout('-b','b');
INSERT INTO t VALUES(4,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b_c');
SELECT dolt_checkout('main');
SELECT dolt_merge('b');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

echo "--- row manipulation edges ---"

oracle "swap_pks_via_temp_sentinel" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET id=99 WHERE id=1;
UPDATE t SET id=1 WHERE id=2;
UPDATE t SET id=2 WHERE id=99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat swaps');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN_1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "pk_change_one_side_value_change_other_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET id=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes pk');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes value');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "one_sided_table_update_indexed_col_stays_queryable" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER);
CREATE INDEX idx_x ON t(x);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x=99 WHERE id=1;
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates indexed col and inserts');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, x FROM t WHERE x IN (99,40,20,30) ORDER BY id;"

oracle "one_sided_table_modify_unique_enforced_post_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER UNIQUE);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds row with x=30');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT OR IGNORE INTO t VALUES(4,30);
" "SELECT id, x FROM t ORDER BY id;"

oracle "one_sided_table_modify_multi_col_index_lookup" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
CREATE INDEX idx_ab ON t(a,b);
INSERT INTO t VALUES(1,10,100),(2,20,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=99,b=999 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates a and b');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE a=99 AND b=999;"

oracle "update_pk_equiv_delete_then_insert_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(2,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat explicit delete plus insert');
SELECT dolt_checkout('main');
UPDATE t SET id=2 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update pk');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "update_back_to_original_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='middle' WHERE id=1;
UPDATE t SET v='final' WHERE id=1;
UPDATE t SET v='original' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat no-net-change');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t;"

echo "--- SAVEPOINT probes ---"

oracle "savepoint_rollback_keeps_outer_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
SAVEPOINT sp1;
INSERT INTO t VALUES(3);
ROLLBACK TO sp1;
INSERT INTO t VALUES(4);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after sp');
" "SELECT id FROM t ORDER BY id;"

oracle "savepoint_release_commits_inner" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2);
INSERT INTO t VALUES(3);
RELEASE sp1;
INSERT INTO t VALUES(4);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after sp');
" "SELECT count(*) FROM t;"

oracle "nested_savepoints_both_rolled_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
INSERT INTO t VALUES(2);
SAVEPOINT sp1;
INSERT INTO t VALUES(3);
SAVEPOINT sp2;
INSERT INTO t VALUES(4);
ROLLBACK TO sp2;
ROLLBACK TO sp1;
INSERT INTO t VALUES(5);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after nested sp');
" "SELECT id FROM t ORDER BY id;"

oracle "savepoint_rollback_before_dolt_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(99,'discarded');
ROLLBACK TO sp1;
INSERT INTO t VALUES(2,'kept');
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after sp rollback');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- empty-table merge probes ---"

oracle "empty_table_created_one_side_merge" "
CREATE TABLE keep(id INTEGER PRIMARY KEY);
INSERT INTO keep VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE empty_tbl(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat empty');
SELECT dolt_checkout('main');
INSERT INTO keep VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM empty_tbl;"

oracle "table_created_empty_data_added_post_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat empty b');
SELECT dolt_checkout('main');
INSERT INTO a VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main a');
SELECT dolt_merge('feat');
INSERT INTO b VALUES(1,'post_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post b');
" "SELECT id, v FROM b;"

echo "--- conflict resolve multi-row probes ---"

oracle "resolve_multiple_conflicts_via_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base1'),(2,'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat1' WHERE id=1;
UPDATE t SET v='feat2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main1' WHERE id=1;
UPDATE t SET v='main2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
UPDATE t SET v='resolved1' WHERE id=1;
UPDATE t SET v='resolved2' WHERE id=2;
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','resolved');
COMMIT;
" "SELECT id, v FROM t ORDER BY id;"

echo "--- set ops after merge ---"

oracle "union_distinct_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,10),(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,30),(5,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT v FROM t UNION SELECT v FROM t ORDER BY v;"

oracle "intersect_across_tables_after_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t1 VALUES(1,10),(2,20);
INSERT INTO t2 VALUES(1,20),(2,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(3,40);
INSERT INTO t2 VALUES(3,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT v FROM t1 INTERSECT SELECT v FROM t2 ORDER BY v;"

oracle "except_across_tables_after_merge" "
CREATE TABLE a(v INTEGER);
CREATE TABLE b(v INTEGER);
INSERT INTO a VALUES(1),(2),(3);
INSERT INTO b VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(4);
INSERT INTO b VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT v FROM a EXCEPT SELECT v FROM b ORDER BY v;"

echo "--- case-insensitive lookup ---"

oracle "lower_lookup_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Alpha'),(2,'beta'),(3,'GAMMA');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'Delta');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE LOWER(v) IN ('alpha','beta','delta') ORDER BY id;"

oracle "upper_filter_with_like_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'red apple'),(2,'RED BERRY'),(3,'Green apple');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'RED GRAPE');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE UPPER(v) LIKE 'RED%' ORDER BY id;"

echo "--- long messages ---"

oracle "long_message_survives_commit_and_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','this is a fairly long commit message that describes several things about the change and has some detail');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log WHERE length(message) > 50;"

echo "--- max/min tie-breaking ---"

oracle "min_tie_break_by_id_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER);
INSERT INTO t VALUES(1,50),(2,50),(3,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE score = (SELECT min(score) FROM t) ORDER BY id;"

echo "--- count variants ---"

oracle "count_star_vs_count_col_with_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,NULL),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,NULL),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS c_all, count(v) AS c_nn, count(DISTINCT v) AS c_dist FROM t;"

echo "--- row-level integrity ---"

oracle "sum_preserved_across_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=n+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n+10 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT sum(n) FROM t;"

echo "--- multi-conflict txn resolve ---"

oracle "resolve_three_conflicts_via_ours" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base1'),(2,'base2'),(3,'base3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f1' WHERE id=1;
UPDATE t SET v='f2' WHERE id=2;
UPDATE t SET v='f3' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m1' WHERE id=1;
UPDATE t SET v='m2' WHERE id=2;
UPDATE t SET v='m3' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','kept ours');
COMMIT;
" "SELECT id, v FROM t ORDER BY id;"
echo "--- conflict resolve variations ---"

oracle "resolve_with_mixed_take_ours_take_theirs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base1'),(2,'base2'),(3,'base3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f1' WHERE id=1;
UPDATE t SET v='f2' WHERE id=2;
UPDATE t SET v='f3' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m1' WHERE id=1;
UPDATE t SET v='m2' WHERE id=2;
UPDATE t SET v='m3' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
UPDATE t SET v='their_1' WHERE id=1;
UPDATE t SET v='our_2' WHERE id=2;
UPDATE t SET v='custom_3' WHERE id=3;
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','resolved mixed');
COMMIT;
" "SELECT id, v FROM t ORDER BY id;"

echo "--- batch conflict + resolve probes ---"

oracle "ten_rows_conflict_resolve_via_ours" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'x'),(2,'x'),(3,'x'),(4,'x'),(5,'x');
INSERT INTO t VALUES(6,'x'),(7,'x'),(8,'x'),(9,'x'),(10,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f' WHERE id<=10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m' WHERE id<=10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','keep ours');
COMMIT;
" "SELECT count(*) FROM t WHERE v='m';"

echo "--- CTAS probes ---"

oracle "ctas_after_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO src VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO src VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
CREATE TABLE dst AS SELECT id, n*2 AS n2 FROM src;
" "SELECT id, n2 FROM dst ORDER BY id;"

echo "--- correlated count probes ---"

oracle "correlated_count_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE related(id INTEGER PRIMARY KEY, ref INTEGER);
INSERT INTO t VALUES(1),(2),(3);
INSERT INTO related VALUES(10,1),(11,1),(12,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO related VALUES(13,3),(14,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT t.id, (SELECT count(*) FROM related WHERE related.ref=t.id) AS c FROM t ORDER BY t.id;"

echo "--- multi-col CHECK probes ---"

oracle "check_on_multi_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, CHECK(a+b <= 100));
INSERT INTO t VALUES(1,10,20),(2,30,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,25,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,5,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- conflict resolve patterns ---"

oracle "conflict_inspect_via_dolt_conflicts_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
DELETE FROM dolt_conflicts_t;
UPDATE t SET v='resolved' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','resolved');
COMMIT;
" "SELECT id, v FROM t;"

oracle "merge_then_resolve_with_delete_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base1'),(2,'keep');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='f1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='m1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
BEGIN;
SELECT dolt_merge('feat');
DELETE FROM t WHERE id=1;
DELETE FROM dolt_conflicts_t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','resolved by delete');
COMMIT;
" "SELECT id, v FROM t ORDER BY id;"

echo "--- empty row edges ---"

oracle "all_columns_null_except_pk_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER);
INSERT INTO t VALUES(1,NULL,NULL,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,NULL,NULL,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET a='alpha' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- multi-table agg ---"

oracle "sum_across_join_after_merge" "
CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER);
CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO customers VALUES(1,'alice'),(2,'bob');
INSERT INTO orders VALUES(1,1,100),(2,1,200),(3,2,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO orders VALUES(4,1,300),(5,2,75);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT c.name, sum(o.amount) AS total FROM customers c JOIN orders o ON c.id=o.customer_id GROUP BY c.name ORDER BY c.name;"

echo "--- savepoint + dolt_add parity ---"

oracle "savepoint_then_dolt_add_rollback_is_noop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('.');
ROLLBACK TO sp1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after sp sealed');
" "SELECT count(*) FROM t;"

oracle "savepoint_without_dolt_add_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'dirty');
ROLLBACK TO sp1;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after sp rollback');
" "SELECT count(*) FROM t;"

oracle "savepoint_error_seals_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('nonexistent_table');
ROLLBACK TO sp1;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT count(*) FROM t;"

echo "--- LIMIT/OFFSET probes ---"

oracle "limit_offset_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2),(3),(4),(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(6),(7),(8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2;"

echo "--- txn + dolt_add probes ---"

oracle "begin_insert_dolt_add_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
BEGIN;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
COMMIT;
SELECT dolt_commit('-m','post');
" "SELECT count(*) FROM dolt_log;"

oracle "begin_insert_add_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
BEGIN;
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
ROLLBACK;
" "SELECT count(*) FROM t;"

echo "--- row survival probes ---"

oracle "row_survives_many_ops_on_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=1 WHERE id=1;
UPDATE t SET v=2 WHERE id=1;
UPDATE t SET v=3 WHERE id=1;
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat ops');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t;"
echo "--- ROW_NUMBER probes ---"

oracle "row_number_per_group_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, score INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',5),(5,'b',25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn FROM t ORDER BY id;"

echo "--- triple parallel merges ---"

oracle "three_branch_parallel_then_merge_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(1,'f1a'),(2,'f1b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','f2');
INSERT INTO t VALUES(3,'f2a'),(4,'f2b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','f3');
INSERT INTO t VALUES(5,'f3a'),(6,'f3b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('f1');
SELECT dolt_merge('f2');
SELECT dolt_merge('f3');
" "SELECT count(*) FROM t;"

echo "--- multi-merge from same feat ---"

oracle "merge_feat_twice_after_update" "
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
" "SELECT id, v FROM t;"

echo "--- repeated add ---"

oracle "add_same_table_multiple_times" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2);
SELECT dolt_add('t');
SELECT dolt_add('t');
SELECT dolt_add('-A');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"
