# Feature-interaction oracle cases: stress.
# Sourced by test/vc_oracle_feature_interaction_test.sh.

echo "--- large row counts ---"

oracle "merge_many_inserts_each_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'f10'),(11,'f11'),(12,'f12'),(13,'f13'),(14,'f14');
INSERT INTO t VALUES(15,'f15'),(16,'f16'),(17,'f17'),(18,'f18'),(19,'f19');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds 10');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(20,'m20'),(21,'m21'),(22,'m22'),(23,'m23'),(24,'m24');
INSERT INTO t VALUES(25,'m25'),(26,'m26'),(27,'m27'),(28,'m28'),(29,'m29');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds 10');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "merge_update_disjoint_ranges" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0),(8,0),(9,0),(10,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=val+1 WHERE id<=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates 1-5');
SELECT dolt_checkout('main');
UPDATE t SET val=val+10 WHERE id>5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates 6-10');
SELECT dolt_merge('feat');
" "SELECT sum(val) FROM t;"

echo "--- wide tables ---"

oracle "merge_wide_table_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='FEAT', c3='FEAT3' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates c1,c3');
SELECT dolt_checkout('main');
UPDATE t SET c5='MAIN', c7='MAIN7' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates c5,c7');
SELECT dolt_merge('feat');
" "SELECT id, c1, c2, c3, c4, c5, c6, c7, c8 FROM t ORDER BY id;"

echo "--- deep branch history merge ---"

oracle "merge_after_3_commits_each" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'m1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
INSERT INTO t VALUES(6,'m2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main2');
INSERT INTO t VALUES(7,'m3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main3');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_with_updates_across_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'v0'),(2,'v0'),(3,'v0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='f1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
UPDATE t SET val='f2' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
UPDATE t SET val='m1' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main c1');
UPDATE t SET val='m2' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main c2');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- many data types in merge ---"

oracle "merge_bool_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, active INTEGER);
INSERT INTO t VALUES(1,1),(2,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET active=0 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deactivates 1');
SELECT dolt_checkout('main');
UPDATE t SET active=1 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main activates 2');
SELECT dolt_merge('feat');
" "SELECT id, active FROM t ORDER BY id;"

oracle "merge_very_long_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO t VALUES(1,'short');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET body='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat long text');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, length(body) FROM t ORDER BY id;"

oracle "merge_large_integer_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, big INTEGER);
INSERT INTO t VALUES(1, 2147483647);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base large int');
SELECT dolt_checkout('-b','feat');
UPDATE t SET big=-2147483648 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat neg int');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, big FROM t ORDER BY id;"

oracle "merge_float_precision" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
INSERT INTO t VALUES(1, 0.1),(2, 0.2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=0.3 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val=0.4 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- stress cell merge ---"

oracle "cell_merge_8_cols_interleaved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='F1', c3='F3', c5='F5', c7='F7' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
UPDATE t SET c2='M2', c4='M4', c6='M6', c8='M8' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT id, c1, c2, c3, c4, c5, c6, c7, c8 FROM t ORDER BY id;"

oracle "cell_merge_10_rows_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','b'),(2,'a','b'),(3,'a','b'),(4,'a','b'),(5,'a','b');
INSERT INTO t VALUES(6,'a','b'),(7,'a','b'),(8,'a','b'),(9,'a','b'),(10,'a','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat a on odds');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main b on evens');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- large delete + merge ---"

oracle "delete_half_merge_other_half" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id <= 4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes first half');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id > 4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates second half');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_all_one_side_insert_other" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes all');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c'),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main inserts');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- cherry-pick from deep history ---"

oracle "cherry_pick_2nd_of_3_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
INSERT INTO t VALUES(3,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
INSERT INTO t VALUES(4,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- rapid updates before merge ---"

oracle "multiple_updates_before_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='v1';
UPDATE t SET val='v2';
UPDATE t SET val='v3';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat triple update');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "insert_update_delete_insert_same_id" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(1,'first');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,'second');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recreate row');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- multi-commit branch stress ---"

oracle "five_commits_per_branch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=val+1 WHERE id=1;
INSERT INTO t VALUES(2,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
UPDATE t SET val=val+1 WHERE id=1;
INSERT INTO t VALUES(3,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
UPDATE t SET val=val+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(11,11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- merge topology stress ---"

oracle "merge_into_already_merged_branch" "
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
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
SELECT dolt_checkout('main');
" "SELECT id, val FROM t ORDER BY id;"

oracle "parallel_branches_same_base_merge_both" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
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
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(4,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_branch_that_merged_another" "
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
SELECT dolt_merge('feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "ff_then_three_way_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'ff');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat ff');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main diverge');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "long_linear_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(4,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(5,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- stress cell merge ---"

oracle "cell_merge_8_cols_interleaved_single_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='F1', c3='F3', c5='F5', c7='F7' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
UPDATE t SET c2='M2', c4='M4', c6='M6', c8='M8' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT c1,c2,c3,c4,c5,c6,c7,c8 FROM t WHERE id=1;"

oracle "cell_merge_10_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT, c9 TEXT, c10 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h','i','j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
UPDATE t SET c10='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main c10');
SELECT dolt_merge('feat');
" "SELECT c1, c10 FROM t WHERE id=1;"

oracle "cell_merge_many_rows_alternating" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','b'),(2,'a','b'),(3,'a','b'),(4,'a','b'),(5,'a','b');
INSERT INTO t VALUES(6,'a','b'),(7,'a','b'),(8,'a','b'),(9,'a','b'),(10,'a','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "cell_merge_20_rows_all_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, y TEXT);
INSERT INTO t VALUES(1,'x','y'),(2,'x','y'),(3,'x','y'),(4,'x','y'),(5,'x','y');
INSERT INTO t VALUES(6,'x','y'),(7,'x','y'),(8,'x','y'),(9,'x','y'),(10,'x','y');
INSERT INTO t VALUES(11,'x','y'),(12,'x','y'),(13,'x','y'),(14,'x','y'),(15,'x','y');
INSERT INTO t VALUES(16,'x','y'),(17,'x','y'),(18,'x','y'),(19,'x','y'),(20,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x='F';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat all x');
SELECT dolt_checkout('main');
UPDATE t SET y='M';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main all y');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, count(CASE WHEN x='F' THEN 1 END) AS xf, count(CASE WHEN y='M' THEN 1 END) AS ym FROM t;"

echo "--- FK stress ---"

oracle "fk_self_ref_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id), val TEXT);
INSERT INTO t VALUES(1, NULL, 'root');
INSERT INTO t VALUES(2, 1, 'child1');
INSERT INTO t VALUES(3, 1, 'child2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4, 2, 'grandchild_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5, 3, 'grandchild_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM t ORDER BY id;"

oracle "fk_multiple_tables_merge" "
CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE emp(id INTEGER PRIMARY KEY, did INTEGER REFERENCES dept(id), name TEXT);
INSERT INTO dept VALUES(1,'eng'),(2,'sales');
INSERT INTO emp VALUES(1,1,'alice'),(2,2,'bob');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dept VALUES(3,'ops');
INSERT INTO emp VALUES(3,3,'charlie');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO emp VALUES(4,1,'dave');
UPDATE dept SET name='engineering' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT e.id, e.name, d.name AS dept FROM emp e JOIN dept d ON e.did=d.id ORDER BY e.id;"

oracle "fk_both_add_children_same_parent" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(1,1,'feat_child1');
INSERT INTO child VALUES(2,1,'feat_child2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'main_child1');
INSERT INTO child VALUES(4,1,'main_child2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM child ORDER BY id;"

echo "--- deep history + cherry-pick ---"

oracle "cherry_pick_from_deep_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(5,'f4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~2');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- wide PK patterns + merge ---"

oracle "varchar_pk_merge" "
CREATE TABLE t(k VARCHAR(16) PRIMARY KEY, v TEXT);
INSERT INTO t VALUES('alpha','a'),('beta','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES('gamma','g');
UPDATE t SET v='BETA' WHERE k='beta';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('delta','d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT k, v FROM t ORDER BY k;"

oracle "composite_varchar_pk_merge" "
CREATE TABLE t(a VARCHAR(8), b VARCHAR(8), v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES('x','1','v1'),('x','2','v2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES('y','1','yv1');
UPDATE t SET v='MOD' WHERE a='x' AND b='1';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('z','1','zv1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, v FROM t ORDER BY a, b;"

echo "--- balanced growth merges ---"

oracle "both_sides_add_5_rows_disjoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(100,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'f1'),(2,'f2'),(3,'f3'),(4,'f4'),(5,'f5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'m1'),(11,'m2'),(12,'m3'),(13,'m4'),(14,'m5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "both_sides_delete_5_rows_disjoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id BETWEEN 1 AND 3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete 1-3');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id BETWEEN 8 AND 10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main delete 8-10');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- deep FK scenarios ---"

oracle "fk_update_root_propagates_views_ok" "
CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), val TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id), val TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,1,'b1');
INSERT INTO c VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE a SET val='A_FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE c SET val='C_MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a.val AS aval, b.val AS bval, c.val AS cval FROM c JOIN b ON c.bid=b.id JOIN a ON b.aid=a.id;"

oracle "fk_add_orphan_like_via_null" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,1,'c1'),(2,NULL,'c_orphan');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(3,2,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(4,NULL,'c_orphan2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

echo "--- long linear history + merge ---"

oracle "ten_commits_linear_then_branch_merge" "
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
INSERT INTO t VALUES(4,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
INSERT INTO t VALUES(5,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c5');
INSERT INTO t VALUES(6,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c6');
INSERT INTO t VALUES(7,7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c7');
INSERT INTO t VALUES(8,8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c8');
INSERT INTO t VALUES(9,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c9');
INSERT INTO t VALUES(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c10');
SELECT dolt_checkout('-b','side','HEAD~5');
INSERT INTO t VALUES(99,99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','side');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

echo "--- large payload probes ---"

oracle "blob_different_on_each_side_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB);
INSERT INTO t VALUES(1, X'0000');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET b=X'FFAA' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, X'CCDD');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, hex(b) FROM t ORDER BY id;"

echo "--- cherry-pick and reset deeper probes ---"

oracle "cherry_pick_then_hard_reset_clears_it" "
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
INSERT INTO t VALUES(3,'post_reset');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

oracle "revert_then_hard_reset_clears_revert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_revert('HEAD');
SELECT dolt_reset('--hard','HEAD~1');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_chain_of_three" "
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
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~2');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- no-op commit stress probes ---"

oracle "many_allow_empty_then_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_commit('-m','e1','--allow-empty');
SELECT dolt_commit('-m','e2','--allow-empty');
SELECT dolt_commit('-m','e3','--allow-empty');
SELECT dolt_commit('-m','e4','--allow-empty');
SELECT dolt_commit('-m','e5','--allow-empty');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
" "SELECT count(*) FROM dolt_log;"

echo "--- deep history probes ---"

oracle "twenty_commit_log_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c0');
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
INSERT INTO t VALUES(4,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c4');
INSERT INTO t VALUES(5,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c5');
INSERT INTO t VALUES(6,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c6');
INSERT INTO t VALUES(7,7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c7');
INSERT INTO t VALUES(8,8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c8');
INSERT INTO t VALUES(9,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c9');
INSERT INTO t VALUES(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c10');
INSERT INTO t VALUES(11,11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c11');
INSERT INTO t VALUES(12,12);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c12');
INSERT INTO t VALUES(13,13);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c13');
INSERT INTO t VALUES(14,14);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c14');
INSERT INTO t VALUES(15,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c15');
INSERT INTO t VALUES(16,16);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c16');
INSERT INTO t VALUES(17,17);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c17');
INSERT INTO t VALUES(18,18);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c18');
INSERT INTO t VALUES(19,19);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c19');
" "SELECT count(*) FROM dolt_log;"

oracle "deep_history_reset_to_halfway" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c0');
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
SELECT dolt_reset('--hard','HEAD~5');
" "SELECT count(*) FROM t;"

echo "--- wide table probes ---"

oracle "twenty_col_table_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER);
INSERT INTO t VALUES(1,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c5=500, c10=1000 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET c15=1500, c20=2000 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, c5, c10, c15, c20 FROM t;"

echo "--- many-branch fan-in probes ---"

oracle "ten_branch_fan_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'base');
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
SELECT dolt_checkout('-b','b7');
INSERT INTO t VALUES(7,'b7');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b7');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b8');
INSERT INTO t VALUES(8,'b8');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b8');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b9');
INSERT INTO t VALUES(9,'b9');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b9');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b10');
INSERT INTO t VALUES(10,'b10');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b10');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
SELECT dolt_merge('b5');
SELECT dolt_merge('b6');
SELECT dolt_merge('b7');
SELECT dolt_merge('b8');
SELECT dolt_merge('b9');
SELECT dolt_merge('b10');
" "SELECT count(*) AS n, sum(id) AS s FROM t;"

oracle "fan_in_branch_list_grows" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');
SELECT dolt_branch('b3');
SELECT dolt_branch('b4');
SELECT dolt_branch('b5');
SELECT dolt_branch('b6');
SELECT dolt_branch('b7');
SELECT dolt_branch('b8');
" "SELECT count(*) FROM dolt_branches;"

echo "--- generated column deeper ---"

oracle "two_stored_generated_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s INTEGER GENERATED ALWAYS AS (a+b) STORED, p INTEGER GENERATED ALWAYS AS (a*b) STORED);
INSERT INTO t(id,a,b) VALUES(1,2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,a,b) VALUES(2,4,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET a=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, s, p FROM t ORDER BY id;"

oracle "generated_col_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, doubled INTEGER GENERATED ALWAYS AS (a*2) STORED);
INSERT INTO t(id,a) VALUES(1,5),(2,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,a) VALUES(3,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE doubled >= 20 ORDER BY id;"

echo "--- stress-lite merge probes ---"

oracle "merge_50_rows_disjoint_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);
INSERT INTO t VALUES(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20);
INSERT INTO t VALUES(21,21),(22,22),(23,23),(24,24),(25,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(101,101),(102,102),(103,103),(104,104),(105,105),(106,106),(107,107),(108,108),(109,109),(110,110);
INSERT INTO t VALUES(111,111),(112,112),(113,113),(114,114),(115,115),(116,116),(117,117),(118,118),(119,119),(120,120);
INSERT INTO t VALUES(121,121),(122,122),(123,123),(124,124),(125,125);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, sum(id) AS s FROM t;"

oracle "merge_many_updates_disjoint_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0),(8,0),(9,0),(10,0);
INSERT INTO t VALUES(11,0),(12,0),(13,0),(14,0),(15,0),(16,0),(17,0),(18,0),(19,0),(20,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=1 WHERE id=1;
UPDATE t SET v=2 WHERE id=2;
UPDATE t SET v=3 WHERE id=3;
UPDATE t SET v=4 WHERE id=4;
UPDATE t SET v=5 WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=16 WHERE id=16;
UPDATE t SET v=17 WHERE id=17;
UPDATE t SET v=18 WHERE id=18;
UPDATE t SET v=19 WHERE id=19;
UPDATE t SET v=20 WHERE id=20;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT sum(v) AS s FROM t;"

echo "--- correlated subquery UPDATE deeper ---"

oracle "update_running_total_via_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, total INTEGER);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET total = (SELECT sum(n) FROM t AS t2 WHERE t2.id <= t.id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat totals');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,40,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds');
SELECT dolt_merge('feat');
" "SELECT id, n, total FROM t ORDER BY id;"

oracle "update_set_count_from_other_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, cnt INTEGER);
CREATE TABLE items(id INTEGER PRIMARY KEY, owner INTEGER);
INSERT INTO t VALUES(1,0),(2,0);
INSERT INTO items VALUES(1,1),(2,1),(3,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET cnt = (SELECT count(*) FROM items WHERE owner = t.id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat count');
SELECT dolt_checkout('main');
INSERT INTO items VALUES(4,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, cnt FROM t ORDER BY id;"

echo "--- deep branch probes ---"

oracle "deep_branch_ff_merge_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
INSERT INTO t VALUES(6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
INSERT INTO t VALUES(7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f6');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

echo "--- large multi-table merge probes ---"

oracle "four_tables_each_touched_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t3(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t4(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t1 VALUES(1,1);
INSERT INTO t2 VALUES(1,2);
INSERT INTO t3 VALUES(1,3);
INSERT INTO t4 VALUES(1,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,10);
INSERT INTO t2 VALUES(2,20);
INSERT INTO t3 VALUES(2,30);
INSERT INTO t4 VALUES(2,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(3,100);
INSERT INTO t2 VALUES(3,200);
INSERT INTO t3 VALUES(3,300);
INSERT INTO t4 VALUES(3,400);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT 't1' AS tbl, sum(v) AS s FROM t1 UNION ALL SELECT 't2', sum(v) FROM t2 UNION ALL SELECT 't3', sum(v) FROM t3 UNION ALL SELECT 't4', sum(v) FROM t4 ORDER BY 1;"

echo "--- window deeper ---"

oracle "partition_by_frame_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INTEGER, v INTEGER);
INSERT INTO t VALUES(1,1,10),(2,1,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,2,30),(4,2,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, SUM(v) OVER (PARTITION BY grp ORDER BY id ROWS UNBOUNDED PRECEDING) AS rsum FROM t ORDER BY id;"

oracle "dense_rank_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, s INTEGER);
INSERT INTO t VALUES(1,10),(2,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,20),(4,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, DENSE_RANK() OVER (ORDER BY s) AS r FROM t ORDER BY id;"

oracle "first_value_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,100),(2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,300),(4,400);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, FIRST_VALUE(v) OVER (ORDER BY id) AS fv FROM t ORDER BY id;"

oracle "lead_window_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, COALESCE(LEAD(v) OVER (ORDER BY id), -1) AS nxt FROM t ORDER BY id;"

echo "--- diamond fan-in probes ---"

oracle "diamond_fan_5_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','a');
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','a');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','c');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','d');
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','d');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','e');
INSERT INTO t VALUES(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','e');
SELECT dolt_checkout('main');
SELECT dolt_merge('a');
SELECT dolt_merge('b');
SELECT dolt_merge('c');
SELECT dolt_merge('d');
SELECT dolt_merge('e');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- deep history log queries ---"

oracle "log_distinct_messages_in_deep_history" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m_one');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m_two');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m_three');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m_four');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m_five');
" "SELECT count(DISTINCT message) FROM dolt_log WHERE message LIKE 'm_%';"

oracle "log_count_after_10_commits_and_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
INSERT INTO t VALUES(12);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m3');
SELECT dolt_merge('feat','--no-ff','-m','merged');
" "SELECT count(*) FROM dolt_log;"

echo "--- 100-row merge ---"

oracle "hundred_rows_each_side_disjoint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);
INSERT INTO t VALUES(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20);
INSERT INTO t VALUES(21,21),(22,22),(23,23),(24,24),(25,25),(26,26),(27,27),(28,28),(29,29),(30,30);
INSERT INTO t VALUES(31,31),(32,32),(33,33),(34,34),(35,35),(36,36),(37,37),(38,38),(39,39),(40,40);
INSERT INTO t VALUES(41,41),(42,42),(43,43),(44,44),(45,45),(46,46),(47,47),(48,48),(49,49),(50,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(101,101),(102,102),(103,103),(104,104),(105,105),(106,106),(107,107),(108,108),(109,109),(110,110);
INSERT INTO t VALUES(111,111),(112,112),(113,113),(114,114),(115,115),(116,116),(117,117),(118,118),(119,119),(120,120);
INSERT INTO t VALUES(121,121),(122,122),(123,123),(124,124),(125,125),(126,126),(127,127),(128,128),(129,129),(130,130);
INSERT INTO t VALUES(131,131),(132,132),(133,133),(134,134),(135,135),(136,136),(137,137),(138,138),(139,139),(140,140);
INSERT INTO t VALUES(141,141),(142,142),(143,143),(144,144),(145,145),(146,146),(147,147),(148,148),(149,149),(150,150);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

echo "--- string func deeper ---"

oracle "upper_lower_projection_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Hello'),(2,'World');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'Goodbye');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, UPPER(v) AS u, LOWER(v) AS l FROM t ORDER BY id;"

oracle "substr_and_length_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'abcdef'),(2,'xyz');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'longer_string');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, SUBSTR(v, 1, 3) AS p, length(v) AS L FROM t ORDER BY id;"

echo "--- long-running branch probes ---"

oracle "feat_behind_main_pull_main_via_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'feat_only');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'m2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
INSERT INTO t VALUES(3,'m3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m3');
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
INSERT INTO t VALUES(11,'feat_after_catchup');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- nested subquery deep ---"

oracle "triple_nested_subquery_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40),(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE n > (SELECT avg(n) FROM t WHERE id IN (SELECT id FROM t WHERE n >= 20)) ORDER BY id;"

echo "--- many-col UPDATE probes ---"

oracle "update_5_cols_same_row_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);
INSERT INTO t VALUES(1,1,1,1,1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=100, b=200, c=300, d=400, e=500 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat all cols');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,10,20,30,40,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c, d, e FROM t ORDER BY id;"

echo "--- deep both-side history ---"

oracle "5x5_commits_each_side_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
INSERT INTO t VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
INSERT INTO t VALUES(12);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m3');
INSERT INTO t VALUES(13);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m4');
INSERT INTO t VALUES(14);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m5');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n FROM t;"

echo "--- cell merge many rows ---"

oracle "cell_merge_20_rows_disjoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
INSERT INTO t VALUES(1,0,0),(2,0,0),(3,0,0),(4,0,0),(5,0,0);
INSERT INTO t VALUES(6,0,0),(7,0,0),(8,0,0),(9,0,0),(10,0,0);
INSERT INTO t VALUES(11,0,0),(12,0,0),(13,0,0),(14,0,0),(15,0,0);
INSERT INTO t VALUES(16,0,0),(17,0,0),(18,0,0),(19,0,0),(20,0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=1 WHERE id<=10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat a');
SELECT dolt_checkout('main');
UPDATE t SET b=2 WHERE id>10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main b');
SELECT dolt_merge('feat');
" "SELECT sum(a) AS sa, sum(b) AS sb FROM t;"

echo "--- wide FK graph ---"

oracle "one_parent_three_child_tables_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c1(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id));
CREATE TABLE c2(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id));
CREATE TABLE c3(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id));
INSERT INTO p VALUES(1);
INSERT INTO c1 VALUES(1,1);
INSERT INTO c2 VALUES(1,1);
INSERT INTO c3 VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO p VALUES(2);
INSERT INTO c1 VALUES(2,2);
INSERT INTO c2 VALUES(2,2);
INSERT INTO c3 VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO c1 VALUES(10,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT 'c1' AS tbl, count(*) AS n FROM c1 UNION ALL SELECT 'c2', count(*) FROM c2 UNION ALL SELECT 'c3', count(*) FROM c3 ORDER BY 1;"

echo "--- deep diamond probes ---"

oracle "diamond_with_3_commits_per_side_noff" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
INSERT INTO t VALUES(2,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','l1');
INSERT INTO t VALUES(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','l2');
INSERT INTO t VALUES(4,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','l3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(11,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
INSERT INTO t VALUES(12,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m3');
SELECT dolt_merge('left','--no-ff','-m','merged');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

echo "--- long-chain cherry-pick ---"

oracle "cherry_pick_6_sequential_from_feat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(4,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
INSERT INTO t VALUES(5,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
INSERT INTO t VALUES(6,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f6');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~5');
SELECT dolt_cherry_pick('feat~4');
SELECT dolt_cherry_pick('feat~3');
SELECT dolt_cherry_pick('feat~2');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_cherry_pick('feat');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

echo "--- large delete + merge ---"

oracle "delete_half_rows_update_other_half_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id<=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes half');
SELECT dolt_checkout('main');
UPDATE t SET v=v*10 WHERE id>5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main multiplies other half');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- rapid alternation ---"

oracle "alternating_commits_6_times_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT);
INSERT INTO t VALUES(0,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(12,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m3');
SELECT dolt_merge('feat');
" "SELECT owner, count(*) AS n FROM t GROUP BY owner ORDER BY owner;"

echo "--- rapid commit-reset cycles ---"

oracle "three_commit_reset_cycles" "
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
SELECT dolt_commit('-m','c2b');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(2,222);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2c');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- very deep reset probes ---"

oracle "reset_head_tilde_8" "
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
SELECT dolt_reset('--hard','HEAD~8');
" "SELECT count(*) FROM t;"

echo "--- 4 branch merges ---"

oracle "four_branch_serial_merge_row_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(1),(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3),(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(5),(6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b4');
INSERT INTO t VALUES(7),(8);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b4');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
" "SELECT count(*) FROM t;"

echo "--- 30-col table merge ---"

oracle "thirty_col_table_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY,
  c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER,
  c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER,
  c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER,
  c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER,
  c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER,
  c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER);
INSERT INTO t VALUES(1, 1,2,3,4,5,6,7,8,9,10, 11,12,13,14,15,16,17,18,19,20, 21,22,23,24,25,26,27,28,29,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c15=999 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET c25=888 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, c01, c15, c25, c30 FROM t;"

echo "--- stress log ---"

oracle "thirty_commit_log_hash_uniqueness" "
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
echo "--- final probes ---"

oracle "one_k_update_after_ff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=1000 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t;"

oracle "one_k_distinct_cat_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, cat VARCHAR(8));
INSERT INTO t VALUES(1,'a'),(2,'a'),(3,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'c'),(5,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(DISTINCT cat) AS d FROM t;"

oracle "one_k_nested_or_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
INSERT INTO t VALUES(1,1,1),(2,2,2),(3,3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,4,4),(5,5,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE (a>2 OR b<2) AND id <> 4 ORDER BY id;"

oracle "one_k_varchar_pk_order_after_merge" "
CREATE TABLE t(k VARCHAR(8) PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES('alpha',1),('bravo',2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES('charlie',3),('delta',4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT k FROM t ORDER BY k;"

oracle "one_k_cherry_pick_preserves_log_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "one_k_union_all_merge" "
CREATE TABLE a(v INTEGER);
CREATE TABLE b(v INTEGER);
INSERT INTO a VALUES(1),(2);
INSERT INTO b VALUES(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(4);
INSERT INTO b VALUES(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v;"

oracle "one_k_three_parent_fk_chain_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id));
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id));
CREATE TABLE d(id INTEGER PRIMARY KEY, cid INTEGER REFERENCES c(id));
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(1,1);
INSERT INTO c VALUES(1,1);
INSERT INTO d VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(2);
INSERT INTO b VALUES(2,2);
INSERT INTO c VALUES(2,2);
INSERT INTO d VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM d;"

oracle "one_k_coalesce_aggregate_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,NULL),(2,20),(3,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40),(5,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT sum(COALESCE(v, 0)) AS s FROM t;"

oracle "one_k_reset_branch_forward_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_branch('snap');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--hard','snap');
INSERT INTO t VALUES(99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','alt c2');
" "SELECT id FROM t ORDER BY id;"

oracle "one_k_merge_base_across_diamond" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
INSERT INTO t VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','l');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
INSERT INTO t VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','r');
SELECT dolt_checkout('main');
" "SELECT count(*) FROM dolt_log WHERE commit_hash = dolt_merge_base('left','right');"

oracle "one_k_final_milestone_count_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','milestone');
" "SELECT count(*) AS n FROM t;"
echo ""
