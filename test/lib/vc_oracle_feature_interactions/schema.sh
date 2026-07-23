# Feature-interaction oracle cases: schema.
# Sourced by test/vc_oracle_feature_interaction_test.sh.

echo "--- merge + alter table add column ---"

oracle "merge_add_col_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x';
UPDATE t SET extra='hello' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add col on feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','insert on main');
SELECT dolt_merge('feat');
" "SELECT id, val, extra FROM t ORDER BY id;"

oracle "merge_add_col_both_sides_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN c1 INTEGER DEFAULT 0;
UPDATE t SET c1=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add c1 on feat');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN c1 INTEGER DEFAULT 0;
UPDATE t SET c1=20 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add c1 on main');
SELECT dolt_merge('feat');
" "SELECT id, val, c1 FROM t ORDER BY id;"

oracle "merge_add_col_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN feat_col TEXT DEFAULT 'f';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add feat_col');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN main_col TEXT DEFAULT 'm';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add main_col');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_add_col_with_data_on_both" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'alice'),(2,'bob');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 0;
UPDATE t SET score=100 WHERE id=1;
INSERT INTO t VALUES(3,'charlie',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET name='ALICE' WHERE id=1;
INSERT INTO t VALUES(4,'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes');
SELECT dolt_merge('feat');
" "SELECT id, name FROM t ORDER BY id;"

echo "--- merge + composite PK ---"

oracle "merge_composite_pk_insert" "
CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(10,1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,2,'feat');
INSERT INTO t VALUES(20,1,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat inserts');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT a, b, val FROM t ORDER BY a, b;"

oracle "merge_composite_pk_update" "
CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(10,1,'v1'),(10,2,'v2'),(20,1,'v3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat' WHERE a=10 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update');
SELECT dolt_checkout('main');
UPDATE t SET val='main' WHERE a=20 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT a, b, val FROM t ORDER BY a, b;"

oracle "merge_composite_pk_delete_and_insert" "
CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,'v1'),(1,2,'v2'),(2,1,'v3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE a=1 AND b=2;
INSERT INTO t VALUES(3,1,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET val='updated' WHERE a=2 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT a, b, val FROM t ORDER BY a, b;"

echo "--- default values in merge ---"

oracle "merge_with_default_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT DEFAULT 'default_val', num INTEGER DEFAULT 42);
INSERT INTO t(id) VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id) VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat insert with defaults');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'explicit',99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert explicit');
SELECT dolt_merge('feat');
" "SELECT id, val, num FROM t ORDER BY id;"

echo "--- merge + foreign keys ---"

oracle "fk_insert_child_on_branch" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(2,2,'c2_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds child');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'c3_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_update_parent_and_child" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='P1_FEAT' WHERE id=1;
UPDATE child SET val='c1_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates');
SELECT dolt_checkout('main');
UPDATE parent SET name='P2_MAIN' WHERE id=2;
UPDATE child SET val='c2_main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates');
SELECT dolt_merge('feat');
" "SELECT p.id, p.name, c.id, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY p.id, c.id;"

oracle "fk_delete_parent_no_children" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2'),(3,'p3');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes parentless');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT id, name FROM parent ORDER BY id;"

oracle "fk_add_parent_and_child_on_branch" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
INSERT INTO child VALUES(1,2,'new child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds parent+child');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,1,'main child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT c.id, c.pid, c.val FROM child c ORDER BY c.id;"

echo "--- merge + secondary indexes ---"

oracle "merge_with_indexed_col_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, score INTEGER);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE INDEX idx_score ON t(score);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add index');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val, score FROM t ORDER BY id;"

oracle "merge_with_index_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, score INTEGER);
CREATE INDEX idx_score ON t(score);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'c',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET score=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates score');
SELECT dolt_checkout('main');
UPDATE t SET score=200 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates score');
SELECT dolt_merge('feat');
" "SELECT id, val, score FROM t ORDER BY score;"

oracle "merge_with_unique_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE);
INSERT INTO t VALUES(1,'alice@test'),(2,'bob@test');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET email='alice_new@test' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat renames alice');
SELECT dolt_checkout('main');
UPDATE t SET email='bob_new@test' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main renames bob');
SELECT dolt_merge('feat');
" "SELECT id, email FROM t ORDER BY id;"

oracle "merge_with_composite_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1,10,20,'v1'),(2,10,30,'v2'),(3,20,10,'v3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=30 WHERE id=1;
INSERT INTO t VALUES(4,10,40,'v4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET b=50 WHERE id=3;
INSERT INTO t VALUES(5,20,20,'v5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes');
SELECT dolt_merge('feat');
" "SELECT id, a, b, val FROM t ORDER BY a, b, id;"

echo "--- multi-table FK merge ---"

oracle "fk_cascade_not_triggered_by_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='p1_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates parent');
SELECT dolt_checkout('main');
UPDATE child SET val='c2_main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates child');
SELECT dolt_merge('feat');
" "SELECT p.name, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY p.id;"

oracle "three_table_fk_chain_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), val TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id), val TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,1,'b1');
INSERT INTO c VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE a SET val='a1_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates a');
SELECT dolt_checkout('main');
UPDATE c SET val='c1_main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates c');
SELECT dolt_merge('feat');
" "SELECT a.val, b.val, c.val FROM a JOIN b ON b.aid=a.id JOIN c ON c.bid=b.id;"

echo "--- schema-only changes ---"

oracle "add_col_no_data_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add col');
" "SELECT id, val FROM t ORDER BY id;"

oracle "add_col_then_populate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 0;
UPDATE t SET score=100 WHERE id=1;
INSERT INTO t VALUES(2,'b',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add and populate');
" "SELECT id, val, score FROM t ORDER BY id;"

echo "--- drop table interactions ---"

oracle "drop_table_other_side_untouched" "
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
UPDATE t1 SET val='updated' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates t1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "create_new_table_on_branch_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t2 VALUES(1,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat creates t2');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies t1');
SELECT dolt_merge('feat');
" "SELECT 't1' AS tbl, id FROM t1 UNION ALL SELECT 't2', id FROM t2 ORDER BY 1, 2;"

echo "--- multi-column cell merge ---"

oracle "cell_merge_4_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT);
INSERT INTO t VALUES(1,'a','b','c','d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='A', c='C' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes a,c');
SELECT dolt_checkout('main');
UPDATE t SET b='B', d='D' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes b,d');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c, d FROM t ORDER BY id;"

oracle "cell_merge_many_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, left_col TEXT, right_col TEXT);
INSERT INTO t VALUES(1,'L','R'),(2,'L','R'),(3,'L','R'),(4,'L','R'),(5,'L','R');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET left_col='FEAT' WHERE id IN (1,3,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET right_col='MAIN' WHERE id IN (2,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, left_col, right_col FROM t ORDER BY id;"

oracle "cell_merge_with_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,NULL);
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

echo "--- multi-column PK merge ---"

oracle "three_col_pk_merge" "
CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER, val TEXT, PRIMARY KEY(a,b,c));
INSERT INTO t VALUES(1,1,1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,1,2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,2,1,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, c, val FROM t ORDER BY a, b, c;"

oracle "two_col_pk_cell_merge" "
CREATE TABLE t(a INTEGER, b INTEGER, x TEXT, y TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,'x0','y0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x='xF' WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET y='yM' WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, x, y FROM t ORDER BY a, b;"

echo "--- column defaults in merge ---"

oracle "add_col_with_default_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN flag INTEGER DEFAULT 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds col');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main inserts');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_rows_with_different_defaults" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, status TEXT DEFAULT 'active');
INSERT INTO t VALUES(1,'a','active');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id, val) VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat uses default');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c','inactive');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main explicit');
SELECT dolt_merge('feat');
" "SELECT id, val, status FROM t ORDER BY id;"

echo "--- CHECK constraints + merge ---"

oracle "merge_with_check_constraint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER CHECK(score >= 0));
INSERT INTO t VALUES(1, 50),(2, 75);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET score=90 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET score=80 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, score FROM t ORDER BY id;"

echo "--- rename-like operations ---"

oracle "delete_and_reinsert_different_val" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'old_name');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,'new_name');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat renames');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- UNIQUE constraint + merge ---"

oracle "unique_col_merge_no_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, val TEXT);
INSERT INTO t VALUES(1,'A','first'),(2,'B','second');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code, val FROM t ORDER BY id;"

oracle "unique_col_insert_different_codes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, val TEXT);
INSERT INTO t VALUES(1,'A','base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'B','feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'C','main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code, val FROM t ORDER BY id;"

echo "--- various PK types ---"

oracle "merge_negative_pk" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(-10,'neg10'),(-5,'neg5'),(0,'zero'),(5,'pos5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(-3,'feat');
UPDATE t SET val='FEAT' WHERE id=-10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
UPDATE t SET val='MAIN' WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_sparse_pk_range" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(100,'a'),(200,'b'),(300,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(150,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat between');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(250,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main between');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_pk_at_boundaries" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'first'),(1000000,'last');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'near_start');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(999999,'near_end');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- multiple indexes + merge ---"

oracle "merge_table_with_unique_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT UNIQUE, age INTEGER);
INSERT INTO t VALUES(1,'alice',30),(2,'bob',25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'charlie',35);
UPDATE t SET age=31 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'dave',28);
UPDATE t SET name='BOB' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, name, age FROM t ORDER BY id;"

echo "--- complex FK merge ---"

oracle "fk_insert_parent_one_side_child_other" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds parent');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,1,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT p.id, p.name FROM parent p ORDER BY p.id;"

oracle "fk_merge_update_referenced_parent" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='P1_NEW' WHERE id=1;
UPDATE child SET val='c1_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT c.id, c.pid, c.val FROM child c ORDER BY c.id;"

echo "--- column ordering ---"

oracle "select_cols_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first_col TEXT, second_col TEXT, third_col TEXT);
INSERT INTO t VALUES(1,'a','b','c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET first_col='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET third_col='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT first_col, second_col, third_col FROM t WHERE id=1;"

echo "--- more FK patterns ---"

oracle "fk_both_add_children_to_same_parent" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'shared_parent');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(1,1,'feat_child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,1,'main_child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_delete_child_merge_parent_update" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM child WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes child');
SELECT dolt_checkout('main');
UPDATE parent SET name='P2_UPDATED' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates parent');
SELECT dolt_merge('feat');
" "SELECT c.id, c.val FROM child c ORDER BY c.id;"

oracle "fk_multiple_children_per_parent_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1a'),(2,1,'c1b'),(3,2,'c2a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(4,1,'c1c_feat');
UPDATE child SET val='c2a_feat' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(5,2,'c2b_main');
UPDATE child SET val='c1b_main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_self_referencing_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id), val TEXT);
INSERT INTO t VALUES(1, NULL, 'root');
INSERT INTO t VALUES(2, 1, 'child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3, 1, 'feat_child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4, 2, 'main_grandchild');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM t ORDER BY id;"

oracle "fk_update_both_parent_and_child_same_branch" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='p1_new' WHERE id=1;
UPDATE child SET val='c1_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat both');
SELECT dolt_checkout('main');
UPDATE parent SET name='p2_new' WHERE id=2;
UPDATE child SET val='c2_new' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main both');
SELECT dolt_merge('feat');
" "SELECT p.name, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY p.id;"

echo "--- PK edge cases in merge ---"

oracle "pk_zero_in_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT_ZERO' WHERE id=0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN_ONE' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "pk_negative_in_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(-5,'neg'),(0,'zero'),(5,'pos');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(-10,'very_neg');
UPDATE t SET val='F' WHERE id=-5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'very_pos');
UPDATE t SET val='M' WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "pk_gaps_fill_from_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(10,'a'),(20,'b'),(30,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(15,'f15'),(25,'f25');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'m5'),(35,'m35');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- INT PK (WITHOUT ROWID) ---"

oracle "int_pk_insert_merge" "
CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "int_pk_cell_merge" "
CREATE TABLE t(id INT PRIMARY KEY, x TEXT, y TEXT);
INSERT INTO t VALUES(1,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET y='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, x, y FROM t ORDER BY id;"

oracle "int_pk_delete_merge" "
CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "int_pk_cherry_pick" "
CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cherry');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- multi-level FK + merge ---"

oracle "four_level_fk_chain_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id), v TEXT);
CREATE TABLE d(id INTEGER PRIMARY KEY, cid INTEGER REFERENCES c(id), v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,1,'b1');
INSERT INTO c VALUES(1,1,'c1');
INSERT INTO d VALUES(1,1,'d1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,2,'b2');
INSERT INTO c VALUES(2,2,'c2');
INSERT INTO d VALUES(2,2,'d2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat chain');
SELECT dolt_checkout('main');
UPDATE d SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a.v, b.v, c.v, d.v FROM d JOIN c ON d.cid=c.id JOIN b ON c.bid=b.id JOIN a ON b.aid=a.id ORDER BY d.id;"

echo "--- UNIQUE + merge complex ---"

oracle "unique_col_delete_then_reinsert_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE);
INSERT INTO t VALUES(1,'X'),(2,'Y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(3,'X');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat swap');
SELECT dolt_checkout('main');
UPDATE t SET code='Z' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code FROM t ORDER BY id;"

oracle "multi_unique_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code1 TEXT UNIQUE, code2 TEXT UNIQUE);
INSERT INTO t VALUES(1,'A','X'),(2,'B','Y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'C','Z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET code2='YY' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code1, code2 FROM t ORDER BY id;"

echo "--- explicit column lists + merge ---"

oracle "insert_named_cols_different_order_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t(id,a,b,c) VALUES(1,'a1','b1','c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(c,a,id,b) VALUES('c2','a2',2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,a,b,c) VALUES(3,'a3','b3','c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- CHECK constraint interactions ---"

oracle "check_constraint_multi_row_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK(n > 0));
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n*2 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

oracle "check_with_not_null_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL CHECK(length(v)>0));
INSERT INTO t VALUES(1,'abc');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'def');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='xyz' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- FK delete restriction + merge ---"

oracle "fk_delete_parent_with_children_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, n TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), v TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,2,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete unreferenced');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main more children');
SELECT dolt_merge('feat');
" "SELECT id, v, pid FROM child ORDER BY id;"

echo "--- FK cascade-ish behavior + merge ---"

oracle "fk_orphan_possible_when_no_action_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, n TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1,'p'),(2,'q');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(3,'r');
INSERT INTO child VALUES(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE child SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

echo "--- alter + populate + merge ---"

oracle "alter_add_col_populate_on_branch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0;
UPDATE t SET extra=100 WHERE id=1;
UPDATE t SET extra=200 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat added col and populated');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main new row');
SELECT dolt_merge('feat');
" "SELECT id, v, extra FROM t ORDER BY id;"

oracle "alter_two_cols_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN x INTEGER DEFAULT 1;
ALTER TABLE t ADD COLUMN y INTEGER DEFAULT 2;
INSERT INTO t VALUES(2,'b',10,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, x, y FROM t ORDER BY id;"

echo "--- multi-column indexes + merge ---"

oracle "merge_table_with_multi_col_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
CREATE INDEX idx_ab ON t(a,b);
INSERT INTO t VALUES(1,1,10),(2,2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,3,30);
UPDATE t SET b=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY a, b;"

echo "--- drop + recreate + merge ---"

oracle "drop_recreate_same_name_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'new_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat recreate');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- PK/UNIQUE conflict probes ---"

oracle "both_sides_insert_same_pk_different_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t WHERE id=1;"

oracle "unique_col_same_value_both_sides_different_pk" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE id=1;"

echo "--- FK merge dependency probes ---"

oracle "fk_parent_created_one_side_child_other_same_id" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), v TEXT);
INSERT INTO parent VALUES(1,'p1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat parent');
SELECT dolt_checkout('main');
INSERT INTO parent VALUES(3,'main_p3');
INSERT INTO child VALUES(1,1,'c_to_p1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM parent ORDER BY id;"

oracle "fk_new_parent_and_child_on_feat_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), v TEXT);
INSERT INTO parent VALUES(1,'p1');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
INSERT INTO child VALUES(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO parent VALUES(3,'p3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

echo "--- repeated ALTER probes ---"

oracle "alter_drop_recreate_col_through_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, keep TEXT, toss TEXT);
INSERT INTO t VALUES(1,'k1','t1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t DROP COLUMN toss;
INSERT INTO t VALUES(2,'k2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drop');
SELECT dolt_checkout('main');
UPDATE t SET keep='KMAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, keep FROM t ORDER BY id;"

echo "--- schema mutation across merge ---"

oracle "add_col_on_feat_insert_main_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds col');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds row');
SELECT dolt_merge('feat');
" "SELECT id, v, extra FROM t ORDER BY id;"

oracle "add_different_cols_each_side_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN x INTEGER DEFAULT 10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat x');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN y INTEGER DEFAULT 20;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main y');
SELECT dolt_merge('feat');
" "SELECT id, v, x, y FROM t;"

echo "--- convergent schema probes ---"

oracle "both_sides_drop_same_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, toss TEXT);
INSERT INTO t VALUES(1,'a1','t1'),(2,'a2','t2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t DROP COLUMN toss;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drop');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN toss;
INSERT INTO t VALUES(3,'a3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main drop');
SELECT dolt_merge('feat');
" "SELECT id, a FROM t ORDER BY id;"

echo "--- FK violation on merge probes ---"

oracle "fk_merge_preserves_both_parent_insertions" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), v TEXT);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2);
INSERT INTO child VALUES(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO parent VALUES(3);
INSERT INTO child VALUES(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS parents FROM parent;"

echo "--- implicit column behavior probes ---"

oracle "insert_integer_into_text_col_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'100'),(2,'200');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='300' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'abc');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t WHERE v LIKE '%0%' ORDER BY id;"

echo "--- NULL + UNIQUE merge probes ---"

oracle "unique_allows_multiple_nulls_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code VARCHAR(32) UNIQUE);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code FROM t ORDER BY id;"

oracle "unique_multi_col_with_one_null_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a VARCHAR(8), b VARCHAR(8), UNIQUE(a,b));
INSERT INTO t VALUES(1,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'x',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'x',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- FK delete-restrict probes ---"

oracle "fk_child_referencing_deleted_parent_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, n TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM child WHERE id=1;
DELETE FROM parent WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete pair');
SELECT dolt_checkout('main');
UPDATE child SET v='MAIN' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS np, count(*) FROM parent;"

echo "--- two-col PK cell merge probes ---"

oracle "two_col_int_pk_disjoint_updates" "
CREATE TABLE t(a INTEGER, b INTEGER, v INTEGER, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,10),(1,2,20),(2,1,30),(2,2,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=99 WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=88 WHERE a=2 AND b=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, v FROM t ORDER BY a, b;"

oracle "two_col_pk_partial_overlap_different_cols" "
CREATE TABLE t(a INTEGER, b INTEGER, v1 INTEGER, v2 INTEGER, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,10,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v1=99 WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v2=999 WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, v1, v2 FROM t;"

echo "--- views + VC ---"

oracle "view_created_on_feat_queried_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW vbig AS SELECT id, v FROM t WHERE v >= 20;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat view');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main row');
SELECT dolt_merge('feat');
" "SELECT id, v FROM vbig ORDER BY id;"

oracle "view_both_sides_same_definition" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW vpos AS SELECT id, v FROM t WHERE v > 0;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat view + row');
SELECT dolt_checkout('main');
CREATE VIEW vpos AS SELECT id, v FROM t WHERE v > 0;
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main view + row');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM vpos;"

oracle "view_on_multi_table_after_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER, w INTEGER);
INSERT INTO a VALUES(1,10),(2,20);
INSERT INTO b VALUES(1,1,100),(2,2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW ab AS SELECT a.id AS aid, a.v AS av, b.w AS bw FROM a JOIN b ON a.id=b.aid;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat view');
SELECT dolt_checkout('main');
INSERT INTO a VALUES(3,30);
INSERT INTO b VALUES(3,3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main data');
SELECT dolt_merge('feat');
" "SELECT aid, av, bw FROM ab ORDER BY aid;"

oracle "drop_view_convergent_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE VIEW vdoomed AS SELECT id, v FROM t;
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base with view');
SELECT dolt_checkout('-b','feat');
DROP VIEW vdoomed;
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops view');
SELECT dolt_checkout('main');
DROP VIEW vdoomed;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main drops view');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "view_created_main_row_added_feat_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'a',15),(3,'b',25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat rows');
SELECT dolt_checkout('main');
CREATE VIEW agg AS SELECT grp, count(*) AS c FROM t GROUP BY grp;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main view');
SELECT dolt_merge('feat');
" "SELECT grp, c FROM agg ORDER BY grp;"

oracle "view_references_dropped_table_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE keep(id INTEGER PRIMARY KEY);
CREATE VIEW vt AS SELECT id, v FROM t;
INSERT INTO t VALUES(1,'a');
INSERT INTO keep VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP VIEW vt;
DROP TABLE t;
INSERT INTO keep VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops both');
SELECT dolt_checkout('main');
INSERT INTO keep VALUES(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main keeps');
SELECT dolt_merge('feat');
" "SELECT id FROM keep ORDER BY id;"

oracle "view_with_aggregation_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW totals AS SELECT grp, sum(n) AS s FROM t GROUP BY grp;
INSERT INTO t VALUES(3,'b',5),(4,'a',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'c',100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT grp, s FROM totals ORDER BY grp;"

oracle "view_with_where_and_order_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flag INTEGER, v TEXT);
INSERT INTO t VALUES(1,1,'a'),(2,0,'b'),(3,1,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW active AS SELECT id, v FROM t WHERE flag=1 ORDER BY id DESC;
INSERT INTO t VALUES(4,1,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,1,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM active;"

echo "--- generated columns + merge ---"

oracle "stored_generated_col_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, doubled INTEGER GENERATED ALWAYS AS (a*2) STORED);
INSERT INTO t(id,a) VALUES(1,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,a) VALUES(2,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,a) VALUES(3,7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, doubled FROM t ORDER BY id;"

oracle "virtual_generated_col_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, sum INTEGER GENERATED ALWAYS AS (a+b) VIRTUAL);
INSERT INTO t(id,a,b) VALUES(1,1,2),(2,3,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=20 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, sum FROM t ORDER BY id;"

oracle "generated_col_referencing_col_updated_other_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, base_val INTEGER, plus_ten INTEGER GENERATED ALWAYS AS (base_val+10) STORED);
INSERT INTO t(id,base_val) VALUES(1,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,base_val) VALUES(2,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds');
SELECT dolt_checkout('main');
UPDATE t SET base_val=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates');
SELECT dolt_merge('feat');
" "SELECT id, base_val, plus_ten FROM t ORDER BY id;"

oracle "generated_string_col_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first TEXT, last TEXT, full TEXT GENERATED ALWAYS AS (first) STORED);
INSERT INTO t(id,first,last) VALUES(1,'Ada','L');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,first,last) VALUES(2,'Grace','H');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,first,last) VALUES(3,'Linus','T');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, first, last, full FROM t ORDER BY id;"

oracle "generated_col_in_where_clause_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, squared INTEGER GENERATED ALWAYS AS (a*a) STORED);
INSERT INTO t(id,a) VALUES(1,2),(2,3),(3,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,a) VALUES(4,5),(5,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE squared > 10 ORDER BY id;"

echo "--- FK cascade actions + merge ---"

oracle "on_delete_cascade_parent_delete_one_side" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2'),(3,2,'c2b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat cascades delete');
SELECT dolt_checkout('main');
UPDATE child SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

oracle "on_delete_set_null_one_side" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat set null');
SELECT dolt_checkout('main');
INSERT INTO parent VALUES(3);
INSERT INTO child VALUES(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

oracle "on_update_cascade_parent_id_change" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id) ON UPDATE CASCADE);
INSERT INTO parent VALUES(1,'p1');
INSERT INTO child VALUES(1,1,'c1'),(2,1,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET id=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update id');
SELECT dolt_checkout('main');
UPDATE child SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

oracle "cascade_delete_with_unaffected_siblings_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1),(2),(3);
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2'),(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes p2');
SELECT dolt_checkout('main');
UPDATE child SET v='MAIN' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates c3');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

oracle "cascade_delete_nested_child_layers" "
PRAGMA foreign_keys=1;
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER, FOREIGN KEY(aid) REFERENCES a(id) ON DELETE CASCADE);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER, v TEXT, FOREIGN KEY(bid) REFERENCES b(id) ON DELETE CASCADE);
INSERT INTO a VALUES(1),(2);
INSERT INTO b VALUES(1,1),(2,1),(3,2);
INSERT INTO c VALUES(1,1,'c1'),(2,2,'c2'),(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM a WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat cascades through');
SELECT dolt_checkout('main');
UPDATE c SET v='MAIN' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, bid, v FROM c ORDER BY id;"

oracle "cherry_pick_cascade_delete" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,1),(2,2),(3,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat cascade');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT count(*) FROM child;"

oracle "set_null_then_update_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=1;
UPDATE child SET v='post_null' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE child SET v='main_keep' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

echo "--- schema merge corner probes ---"

oracle "type_widen_int_to_bigint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2, 2000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=999 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

oracle "add_col_with_default_then_merge_into_branch_with_more_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b'),(3,'c'),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat rows');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN flag INTEGER DEFAULT 7;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds col');
SELECT dolt_merge('feat');
" "SELECT id, v, flag FROM t ORDER BY id;"

oracle "drop_nullable_col_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, notes TEXT);
INSERT INTO t VALUES(1,'a','n1'),(2,'b','n2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t DROP COLUMN notes;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c','n3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds row');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- view + changes on both sides ---"

oracle "view_on_feat_data_on_main_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW small AS SELECT id, v FROM t WHERE v < 25;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat view');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,15);
UPDATE t SET v=5 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main data');
SELECT dolt_merge('feat');
" "SELECT id, v FROM small ORDER BY id;"

oracle "view_dropped_and_readded_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE VIEW posv AS SELECT id, v FROM t WHERE v > 0;
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP VIEW posv;
CREATE VIEW posv AS SELECT id, v FROM t WHERE v >= 0;
INSERT INTO t VALUES(2,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat view swap');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id FROM posv ORDER BY id;"

echo "--- schema+data mix probes ---"

oracle "add_col_on_feat_and_insert_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'x';
INSERT INTO t VALUES(2,'feat','y');
UPDATE t SET tag='z' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat full');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main row');
SELECT dolt_merge('feat');
" "SELECT id, v, tag FROM t ORDER BY id;"

oracle "both_branches_add_col_different_names" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN feat_col TEXT;
UPDATE t SET feat_col='fa' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds feat_col');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN main_col INTEGER DEFAULT 0;
UPDATE t SET main_col=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds main_col');
SELECT dolt_merge('feat');
" "SELECT id, v, feat_col, main_col FROM t;"

echo "--- FK null-parent edge ---"

oracle "fk_nullable_pid_on_both_sides_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,NULL,'orphan1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(2,1,'feat_c2');
INSERT INTO child VALUES(3,NULL,'feat_orphan');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(4,2,'main_c4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, v FROM child ORDER BY id;"

echo "--- default expression probes ---"

oracle "default_literal_used_in_both_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flag INTEGER DEFAULT 42, v TEXT);
INSERT INTO t(id,v) VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,v) VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,v) VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, flag, v FROM t ORDER BY id;"

oracle "default_not_supplied_different_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT DEFAULT 'unset', b TEXT);
INSERT INTO t(id,b) VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,b) VALUES(2,'feat-b');
INSERT INTO t(id,a,b) VALUES(3,'explicit','feat-b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,b) VALUES(10,'main-b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- rename column probes ---"

oracle "rename_column_on_feat_query_works_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t RENAME COLUMN v TO val;
INSERT INTO t(id, val) VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat rename');
SELECT dolt_checkout('main');
INSERT INTO t(id, v) VALUES(4,'main_d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "rename_column_on_main_and_insert_feat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME COLUMN v TO val;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main rename');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- rename table probes ---"

oracle "rename_table_on_feat_merge_to_main" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE other(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
INSERT INTO other VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t RENAME TO renamed;
INSERT INTO renamed VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat rename');
SELECT dolt_checkout('main');
INSERT INTO other VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id FROM other ORDER BY id;"

echo "--- view-join probes ---"

oracle "view_on_left_join_after_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER, n INTEGER);
INSERT INTO a VALUES(1,'x'),(2,'y');
INSERT INTO b VALUES(1,1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW a_with_b AS SELECT a.id, a.v, b.n FROM a LEFT JOIN b ON a.id=b.aid;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat view');
SELECT dolt_checkout('main');
INSERT INTO b VALUES(2,2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, n FROM a_with_b ORDER BY id;"

oracle "view_with_aggregate_across_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW totals AS SELECT grp, sum(amt) AS total FROM t GROUP BY grp;
INSERT INTO t VALUES(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'a',100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT grp, total FROM totals ORDER BY grp;"

echo "--- post-merge FK query probes ---"

oracle "three_table_join_filter_after_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE a(id INTEGER PRIMARY KEY, cat TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), tag TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id), score INTEGER);
INSERT INTO a VALUES(1,'x'),(2,'y');
INSERT INTO b VALUES(1,1,'t1'),(2,2,'t2');
INSERT INTO c VALUES(1,1,50),(2,2,75);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(3,'z');
INSERT INTO b VALUES(3,3,'t3');
INSERT INTO c VALUES(3,3,90);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE c SET score=80 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a.cat FROM c JOIN b ON c.bid=b.id JOIN a ON b.aid=a.id WHERE c.score > 60 ORDER BY a.cat;"

echo "--- multi-index probes ---"

oracle "two_indexes_both_queried_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30,300),(4,40,400);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE a > 15 AND b < 350 ORDER BY id;"

oracle "unique_index_distinct_cols_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT);
CREATE UNIQUE INDEX idx_code ON t(code);
INSERT INTO t VALUES(1,'X'),(2,'Y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'Z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(DISTINCT code) FROM t;"

echo "--- schema-only modifications ---"

oracle "add_then_drop_col_same_branch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN tmp INTEGER DEFAULT 0;
ALTER TABLE t DROP COLUMN tmp;
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat noop schema');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- alter after merge ---"

oracle "alter_add_col_after_merge" "
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
ALTER TABLE t ADD COLUMN tag INTEGER DEFAULT 99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post-merge alter');
" "SELECT id, v, tag FROM t ORDER BY id;"

echo "--- view chain probes ---"

oracle "view_on_view_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE VIEW v1 AS SELECT id, v FROM t WHERE v >= 15;
CREATE VIEW v2 AS SELECT id FROM v1 WHERE v >= 25;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,35);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM v2 ORDER BY id;"

oracle "view_created_on_top_of_existing_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE VIEW bigs AS SELECT id, v FROM t WHERE v >= 50;
INSERT INTO t VALUES(1,10),(2,60);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE VIEW very_bigs AS SELECT id FROM bigs WHERE v >= 75;
INSERT INTO t VALUES(3,80);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat chain view');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id FROM very_bigs ORDER BY id;"

echo "--- multi-unique cols probes ---"

oracle "two_unique_cols_inserts_disjoint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code VARCHAR(16) UNIQUE, tag VARCHAR(16) UNIQUE);
INSERT INTO t VALUES(1,'c1','t1'),(2,'c2','t2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'c3','t3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'c4','t4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(DISTINCT code) AS d_code, count(DISTINCT tag) AS d_tag FROM t;"
echo "--- DATE column probes ---"

oracle "date_col_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, d DATE);
INSERT INTO t VALUES(1,'2024-01-15'),(2,'2024-06-01');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'2024-12-25');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'2024-02-14');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE d > '2024-03-01' ORDER BY id;"

oracle "date_update_both_sides_disjoint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, d DATE);
INSERT INTO t VALUES(1,'2024-01-01'),(2,'2024-01-01'),(3,'2024-01-01');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET d='2024-06-15' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET d='2024-12-31' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, d FROM t ORDER BY id;"

echo "--- self-ref FK probes ---"

oracle "self_ref_fk_with_pragma_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE n(id INTEGER PRIMARY KEY, parent_id INTEGER, v TEXT, FOREIGN KEY(parent_id) REFERENCES n(id));
INSERT INTO n VALUES(1,NULL,'root'),(2,1,'a'),(3,1,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO n VALUES(4,2,'a1'),(5,3,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO n VALUES(6,2,'a2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, parent_id, v FROM n ORDER BY id;"

echo "--- NULL unique alt probes ---"

oracle "unique_col_with_null_then_nonnull_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code VARCHAR(16) UNIQUE);
INSERT INTO t VALUES(1,'A'),(2,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'B');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'C');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- multi-table FK merge ---"

oracle "three_table_fk_chain_add_leaves_both_sides" "
PRAGMA foreign_keys=1;
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id));
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id));
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(1,1);
INSERT INTO c VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO c VALUES(2,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat leaf');
SELECT dolt_checkout('main');
INSERT INTO c VALUES(3,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main leaf');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM c;"

echo "--- index usage probes ---"

oracle "unique_index_lookup_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code VARCHAR(32));
CREATE UNIQUE INDEX uc ON t(code);
INSERT INTO t VALUES(1,'A'),(2,'B');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'C'),(4,'D');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE code IN ('B','D') ORDER BY id;"

echo "--- REPLACE + UNIQUE probes ---"

oracle "replace_with_unique_col_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code VARCHAR(16) UNIQUE, v TEXT);
INSERT INTO t VALUES(1,'A','a1'),(2,'B','b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'A','a2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'C','c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code, v FROM t ORDER BY id;"

echo "--- convergent ALTER probes ---"

oracle "both_sides_rename_same_col_same_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t RENAME COLUMN v TO val;
INSERT INTO t(id,val) VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat rename');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME COLUMN v TO val;
INSERT INTO t(id,val) VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main rename');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- drop + recreate + merge ---"

oracle "drop_recreate_different_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(10,100),(20,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat recreated');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- mixed index types ---"

oracle "unique_plus_non_unique_index_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code VARCHAR(16), category TEXT);
CREATE UNIQUE INDEX uc ON t(code);
CREATE INDEX idx_cat ON t(category);
INSERT INTO t VALUES(1,'X','a'),(2,'Y','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'Z','a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'W','c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT category, count(*) AS n FROM t GROUP BY category ORDER BY category;"

echo "--- schema diff count after merge ---"

oracle "schema_diff_commit_count_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN c1 INTEGER;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1 added');
ALTER TABLE t ADD COLUMN c2 INTEGER;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 added');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n FROM dolt_log WHERE message IN ('c1 added','c2 added');"

echo "--- NULL default probes ---"

oracle "null_default_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT DEFAULT NULL);
INSERT INTO t(id) VALUES(1),(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,v) VALUES(3,'explicit');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id) VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- FK update through merge ---"

oracle "fk_update_parent_attribute_merge" "
PRAGMA foreign_keys=1;
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), v TEXT);
INSERT INTO p VALUES(1,'P1'),(2,'P2');
INSERT INTO c VALUES(1,1,'C1'),(2,2,'C2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE p SET v='P1_NEW' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat parent');
SELECT dolt_checkout('main');
UPDATE c SET v='C2_NEW' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main child');
SELECT dolt_merge('feat');
" "SELECT p.v AS pv, c.v AS cv FROM p JOIN c ON p.id=c.pid ORDER BY p.id;"
