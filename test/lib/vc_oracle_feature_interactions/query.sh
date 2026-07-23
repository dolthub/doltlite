# Feature-interaction oracle cases: query.
# Sourced by test/vc_oracle_feature_interaction_test.sh.

echo "--- merge + NULL values ---"

oracle "merge_null_to_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base with null');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='hello' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat sets value');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_value_to_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'hello');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat nulls');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_null_in_multiple_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,NULL,'b',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='filled_a' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills a');
SELECT dolt_checkout('main');
UPDATE t SET c='filled_c' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main fills c');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- upsert + version control ---"

oracle "upsert_replace_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'replaced');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat replaces');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "update_as_upsert_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, count INTEGER DEFAULT 0);
INSERT INTO t VALUES(1,'a',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET count=count+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat increment');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val, count FROM t ORDER BY id;"

echo "--- text/blob in merge ---"

oracle "merge_long_text_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO t VALUES(1,'short');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET body='this is a much longer text value that spans many bytes and tests whether large text fields merge correctly across branches';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat long text');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'another row');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, length(body) FROM t ORDER BY id;"

oracle "merge_blob_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
INSERT INTO t VALUES(1, X'DEADBEEF');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET data=X'CAFEBABE' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat blob update');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, X'0102030405');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, hex(data) FROM t ORDER BY id;"

echo "--- numeric types in merge ---"

oracle "merge_integer_types" "
CREATE TABLE t(id INTEGER PRIMARY KEY, small INTEGER, big INTEGER);
INSERT INTO t VALUES(1, 42, 999999);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET small=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update small');
SELECT dolt_checkout('main');
UPDATE t SET big=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update big');
SELECT dolt_merge('feat');
" "SELECT id, small, big FROM t ORDER BY id;"

oracle "merge_real_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
INSERT INTO t VALUES(1, 3.14159);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=2.71828 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 1.41421);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- type coercion ---"

oracle "merge_int_stored_as_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'100');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='200' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'300');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_mixed_null_types" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL);
INSERT INTO t VALUES(1, NULL, NULL, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base all null');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=42 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills int');
SELECT dolt_checkout('main');
UPDATE t SET c=3.14 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main fills real');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- empty string vs NULL ---"

oracle "merge_empty_string_vs_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty string');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='filled' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main null');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- row count after merge ---"

oracle "merge_preserves_total_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=5;
INSERT INTO t VALUES(6,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- NOT NULL in merge ---"

oracle "merge_not_null_col_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT NOT NULL);
INSERT INTO t VALUES(1,'a'),(2,'b');
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
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_not_null_with_default" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT NOT NULL DEFAULT 'none');
INSERT INTO t(id) VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id) VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='updated' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- row ordering after merge ---"

oracle "merge_preserves_pk_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(10,'ten'),(20,'twenty'),(30,'thirty');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(15,'fifteen');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(25,'twentyfive');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- empty string handling ---"

oracle "merge_empty_string_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='notempty' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds empty');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_null_vs_empty_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,''),(2,'','hello');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='filled' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='world' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

echo "--- value patterns in merge ---"

oracle "merge_special_chars_in_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'hello world'),(2,'line1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='has ''quotes''' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='has,commas' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_unicode_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'ascii');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='hello' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'world');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_with_spaces_in_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'  leading'),(2,'trailing  '),(3,' both ');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='no_spaces' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='also_no_spaces' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_zero_length_blob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
INSERT INTO t VALUES(1, X''),(2, X'FF');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET data=X'AABB' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET data=X'CCDD' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, hex(data) FROM t ORDER BY id;"

oracle "merge_numeric_text_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'100'),(2,'200');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='150' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='250' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

echo "--- aggregate verification ---"

oracle "merge_preserves_sum" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount INTEGER);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET amount=150 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET amount=250 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT sum(amount) FROM t;"

oracle "merge_preserves_count_with_mixed_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base 5');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=5;
INSERT INTO t VALUES(6,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat -1+1');
SELECT dolt_checkout('main');
UPDATE t SET val='A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "merge_group_by_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'X',10),(2,'X',20),(3,'Y',30),(4,'Y',40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=15 WHERE id=1;
INSERT INTO t VALUES(5,'X',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val=35 WHERE id=3;
INSERT INTO t VALUES(6,'Y',60);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;"

echo "--- UPDATE CASE + merge ---"

oracle "update_case_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'c',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=CASE WHEN n>15 THEN 'big' ELSE 'small' END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat case');
SELECT dolt_checkout('main');
UPDATE t SET n=n+100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val, n FROM t ORDER BY id;"

oracle "case_in_select_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,5),(2,15),(3,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,35);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, CASE WHEN n<10 THEN 's' WHEN n<20 THEN 'm' ELSE 'l' END AS sz FROM t ORDER BY id;"

echo "--- subquery WHERE + merge ---"

oracle "update_where_subquery_then_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, threshold INTEGER);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t1 SET v=0 WHERE v < (SELECT threshold FROM t2 WHERE id=1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t1 ORDER BY id;"

oracle "delete_where_subquery_then_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, cutoff INTEGER);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t1 WHERE v > (SELECT cutoff FROM t2 WHERE id=1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat del');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t1 ORDER BY id;"

echo "--- INSERT SELECT + merge ---"

oracle "insert_select_from_other_table_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE dst(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO src VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dst SELECT id, v FROM src WHERE id <= 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat copy');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main src++');
SELECT dolt_merge('feat');
" "SELECT id, v FROM dst ORDER BY id;"

oracle "insert_select_same_table_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, grp INTEGER);
INSERT INTO t VALUES(1,'a',1),(2,'b',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t SELECT id+10, v, grp+1 FROM t WHERE id<=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat self copy');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, grp FROM t WHERE id>=10 ORDER BY id;"

echo "--- LIKE/IN/BETWEEN + merge ---"

oracle "update_where_like_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'apple'),(2,'apricot'),(3,'banana'),(4,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET name='FRUIT_A' WHERE name LIKE 'ap%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET name='MAIN_B' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, name FROM t ORDER BY id;"

oracle "update_where_in_list_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='X' WHERE id IN (1,3,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='M' WHERE id IN (2,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "delete_where_between_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id BETWEEN 2 AND 4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='M' WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- aggregates after merge ---"

oracle "sum_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n+1 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT sum(n) AS s FROM t;"

oracle "group_by_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',100),(5,'b',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, sum(n) AS total FROM t GROUP BY grp ORDER BY grp;"

oracle "avg_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT sum(n)/count(*) AS a FROM t;"

echo "--- conditional UPDATE + cell merge ---"

oracle "update_coalesce_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,'b0'),(2,'a0',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=COALESCE(a,'fallback') WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=COALESCE(b,'mfallback') WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "update_different_cols_disjoint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='x_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='y_main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t;"

echo "--- DISTINCT/UNION after merge ---"

oracle "distinct_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, cat TEXT);
INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'z'),(5,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT DISTINCT cat FROM t ORDER BY cat;"

oracle "union_all_from_merged" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a1');
INSERT INTO t2 VALUES(1,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,'a2');
INSERT INTO t2 VALUES(2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT v FROM t1 UNION ALL SELECT v FROM t2 ORDER BY v;"

echo "--- multiple inserts then merge ---"

oracle "many_inserts_same_batch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,10);
INSERT INTO t VALUES(11,11);
INSERT INTO t VALUES(12,12);
INSERT INTO t VALUES(13,13);
INSERT INTO t VALUES(14,14);
INSERT INTO t VALUES(15,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

oracle "many_updates_same_batch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=10 WHERE id=1;
UPDATE t SET v=20 WHERE id=2;
UPDATE t SET v=30 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=99 WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- NULL handling edge cases ---"

oracle "null_to_value_both_sides_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,NULL),(2,NULL),(3,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "is_null_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,NULL),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS null_count FROM t WHERE v IS NULL;"

echo "--- chained updates same row + merge ---"

oracle "many_updates_same_row_feat_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat +5');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main new row');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- HAVING after merge ---"

oracle "having_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'c',100),(5,'a',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, sum(n) AS total FROM t GROUP BY grp HAVING sum(n) > 10 ORDER BY grp;"

echo "--- CTE + merge ---"

oracle "cte_select_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH big AS (SELECT id, n FROM t WHERE n >= 20) SELECT id, n FROM big ORDER BY id;"

oracle "cte_with_count_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',1),(2,'a',2),(3,'b',3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',4),(5,'c',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH gc AS (SELECT grp, count(*) AS c FROM t GROUP BY grp) SELECT grp, c FROM gc ORDER BY grp;"

echo "--- REPLACE patterns + merge ---"

oracle "replace_on_both_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(2,'feat_replace');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat replace');
SELECT dolt_checkout('main');
REPLACE INTO t VALUES(3,'main_replace');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main replace');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "replace_then_delete_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'feat_replaced');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- multi-row UPDATE + merge ---"

oracle "update_per_id_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=10 WHERE id=1;
UPDATE t SET v=20 WHERE id=2;
UPDATE t SET v=30 WHERE id=3;
UPDATE t SET v=40 WHERE id=4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=999 WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- REAL/float merge ---"

oracle "float_merge_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x REAL);
INSERT INTO t VALUES(1, 1.5),(2, 2.5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x=3.75 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET x=4.25 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, x FROM t ORDER BY id;"

oracle "float_negative_values_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x REAL);
INSERT INTO t VALUES(1, -1.5),(2, 0.5),(3, 100.25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4, -0.125);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, x FROM t ORDER BY id;"

echo "--- multi-VALUES INSERT + merge ---"

oracle "insert_10_rows_one_stmt_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,10),(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat 10 rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

echo "--- post-merge aggregate invariants ---"

oracle "min_max_span_unchanged_by_convergent_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=99 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=5 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT min(n) AS lo, max(n) AS hi FROM t;"

oracle "count_distinct_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, cat TEXT);
INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'z'),(5,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(DISTINCT cat) AS distinct_cats FROM t;"

echo "--- string funcs in UPDATE + merge ---"

oracle "update_lower_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'ABC'),(2,'DEF');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=LOWER(v) WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=UPPER(v) WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "length_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'bb'),(3,'ccc');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'dddd'),(5,'eeeee');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t WHERE length(v)>=3 ORDER BY id;"

echo "--- arithmetic UPDATE + merge ---"

oracle "update_multiply_disjoint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=n*2 WHERE id IN (1,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n+100 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

oracle "update_mod_op_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,7),(2,13),(3,22);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=n%5 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n-1 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- UPDATE with correlated subquery + merge ---"

oracle "update_via_correlated_subquery_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE lookup(id INTEGER PRIMARY KEY, mult INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
INSERT INTO lookup VALUES(1,2),(2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v = v * (SELECT mult FROM lookup WHERE lookup.id=t.id) WHERE id IN (1,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- multi-table inner join + merge ---"

oracle "three_way_join_after_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER, v TEXT);
INSERT INTO a VALUES(1,'A'),(2,'B');
INSERT INTO b VALUES(1,1,'b1'),(2,2,'b2');
INSERT INTO c VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(3,'C');
INSERT INTO b VALUES(3,3,'b3');
INSERT INTO c VALUES(3,3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT a.v AS av, b.v AS bv, c.v AS cv FROM c JOIN b ON c.bid=b.id JOIN a ON b.aid=a.id ORDER BY a.id;"

oracle "left_join_counts_after_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1id INTEGER, v TEXT);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t2 VALUES(2,2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT t1.id, CASE WHEN t2.id IS NULL THEN 'none' ELSE t2.v END AS got FROM t1 LEFT JOIN t2 ON t1.id=t2.t1id ORDER BY t1.id;"

echo "--- type behavior through merge ---"

oracle "int_column_preserved_numeric_equality" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,42);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t WHERE n > 10;"

oracle "text_column_equality_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO t VALUES(1,'hello'),(2,'world');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET s='updated' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE s='updated';"

echo "--- REPLACE vs merge probes ---"

oracle "replace_then_other_side_replaces_same_pk" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'feat_replaced');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
REPLACE INTO t VALUES(1,'main_replaced');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- NULL aggregation probes ---"

oracle "sum_ignores_null_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,NULL),(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,NULL),(5,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT sum(n) AS s, count(n) AS c_nn, count(*) AS c_all FROM t;"

oracle "min_max_skip_null_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,NULL),(2,5),(3,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,100),(5,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT min(n) AS lo, max(n) AS hi FROM t;"

echo "--- update pattern probes ---"

oracle "update_then_update_back_same_value_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='intermediate' WHERE id=1;
UPDATE t SET v='orig' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat roundtrip');
SELECT dolt_checkout('main');
UPDATE t SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t;"

oracle "update_set_null_then_value_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=NULL WHERE id=1;
UPDATE t SET v='feat_final' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- NULL ordering probes ---"

oracle "nulls_in_order_by_asc_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,NULL),(3,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,NULL),(5,7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE n IS NOT NULL ORDER BY n;"

echo "--- one-sided delete probes ---"

oracle "delete_on_feat_untouched_main_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep'),(2,'del'),(3,'keep2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat del');
SELECT dolt_checkout('main');
UPDATE t SET v='modified' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- whitespace/text probes ---"

oracle "trailing_space_text_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a '),(2,' b'),(3,' c ');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d  ');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, length(v) AS L FROM t ORDER BY id;"

oracle "tab_newline_in_text_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'with	tab'),(2,'with newline embedded');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'plain');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, length(v) AS L FROM t ORDER BY id;"

echo "--- recursive CTE probes ---"

oracle "recursive_cte_count_to_n_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=8 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < (SELECT v FROM t WHERE id=1)) SELECT count(*) AS c FROM nums;"

oracle "recursive_cte_hierarchy_walk_after_merge" "
CREATE TABLE tree(id INTEGER PRIMARY KEY, pid INTEGER, v TEXT);
INSERT INTO tree VALUES(1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'aa');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO tree VALUES(5,3,'ba'),(6,4,'aaa');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH RECURSIVE descend(id, depth) AS (SELECT id, 0 FROM tree WHERE pid IS NULL UNION ALL SELECT t.id, d.depth+1 FROM tree t JOIN descend d ON t.pid=d.id) SELECT depth, count(*) AS n FROM descend GROUP BY depth ORDER BY depth;"

echo "--- window function probes ---"

oracle "row_number_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',30),(5,'b',15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY n) AS rn FROM t ORDER BY id;"

oracle "sum_running_window_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, SUM(n) OVER (ORDER BY id) AS running FROM t ORDER BY id;"

oracle "rank_window_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,30),(5,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, RANK() OVER (ORDER BY score DESC) AS r FROM t ORDER BY id;"

oracle "lag_window_after_merge" "
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
" "SELECT id, COALESCE(LAG(v) OVER (ORDER BY id), 0) AS prev FROM t ORDER BY id;"

echo "--- complex WHERE probes ---"

oracle "and_or_not_combined_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'c',40),(5,'a',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE (grp='a' AND n>15) OR (grp='b' AND NOT (n<20)) ORDER BY id;"

oracle "nested_in_subquery_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE allow(v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO allow VALUES('a'),('c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d');
INSERT INTO allow VALUES('d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE v IN (SELECT v FROM allow) ORDER BY id;"

echo "--- multi-col UPDATE probes ---"

oracle "update_set_multiple_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);
INSERT INTO t VALUES(1,1,2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=10, b=20, c=30 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,4,5,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- JOIN UPDATE probes ---"

oracle "update_with_inner_join_lookup_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE lookup(id INTEGER PRIMARY KEY, bonus INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
INSERT INTO lookup VALUES(1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v = v + (SELECT bonus FROM lookup WHERE lookup.id=t.id) WHERE EXISTS (SELECT 1 FROM lookup WHERE lookup.id=t.id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- CASE projection probes ---"

oracle "nested_case_projection_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,5),(2,15),(3,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,35),(5,45);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, CASE WHEN n<10 THEN 'xs' WHEN n<20 THEN 's' WHEN n<30 THEN 'm' WHEN n<40 THEN 'l' ELSE 'xl' END AS sz FROM t ORDER BY id;"

echo "--- INSERT SELECT agg probes ---"

oracle "insert_select_from_agg_after_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO src VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO src VALUES(4,'a',30),(5,'b',15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat rows');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(6,'c',100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main new grp');
SELECT dolt_merge('feat');
" "SELECT grp, sum(n) AS s FROM src GROUP BY grp ORDER BY grp;"

echo "--- upsert-like probes ---"

oracle "on_conflict_replace_equivalent_via_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'a_new',11);
REPLACE INTO t VALUES(3,'c_new',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
REPLACE INTO t VALUES(2,'b_new',22);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, n FROM t ORDER BY id;"

echo "--- aggregate on empty table probes ---"

oracle "delete_all_sum_null_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat del all');
SELECT dolt_checkout('main');
UPDATE t SET n=n*10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS c FROM t;"

echo "--- string function probes ---"

oracle "substr_in_select_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'abcdef'),(2,'ghijkl');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'mnopqr');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, SUBSTR(v,2,3) AS s FROM t ORDER BY id;"

oracle "replace_string_in_select_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello world'),(2,'goodbye world');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'hello universe');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, REPLACE(v,'world','WORLD') AS r FROM t ORDER BY id;"

echo "--- multi-key GROUP BY probes ---"

oracle "multi_key_group_by_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER);
INSERT INTO t VALUES(1,'x','1',10),(2,'x','2',20),(3,'y','1',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'x','1',100),(5,'y','1',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT a, b, sum(n) AS s FROM t GROUP BY a, b ORDER BY a, b;"

echo "--- NULL-handling funcs after merge ---"

oracle "ifnull_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,'b1'),(2,'a2',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,NULL,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, IFNULL(a,'none') AS a_safe, IFNULL(b,'none') AS b_safe FROM t ORDER BY id;"

oracle "nullif_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'x'),(2,'sentinel'),(3,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'sentinel'),(5,'z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n_null FROM (SELECT NULLIF(v,'sentinel') AS masked FROM t) sub WHERE masked IS NULL;"

echo "--- math function probes ---"

oracle "abs_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,-5),(2,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,-20),(4,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, abs(n) AS a FROM t ORDER BY id;"

oracle "mod_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,17),(2,22),(3,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,49);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, n % 7 AS r FROM t ORDER BY id;"

oracle "negate_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,17),(2,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, -n AS neg FROM t ORDER BY id;"

echo "--- complex WHERE projection ---"

oracle "where_multi_pred_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);
INSERT INTO t VALUES(1,10,'x',100),(2,20,'y',200),(3,30,'x',300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,15,'x',150),(5,25,'y',250);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE a > 10 AND b='x' AND c < 300 ORDER BY id;"

oracle "in_subquery_after_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, ref INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, flag INTEGER);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(10,1),(20,0),(30,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(4,40);
INSERT INTO t2 VALUES(40,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t1 WHERE ref IN (SELECT id FROM t2 WHERE flag=1) ORDER BY id;"

echo "--- JSON function probes ---"

oracle "json_extract_simple_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, j JSON);
INSERT INTO t VALUES(1,'{\"a\":1,\"b\":2}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'{\"a\":10,\"b\":20}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, json_extract(j,'\$.a') AS a FROM t ORDER BY id;"

oracle "json_nested_extract_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, j JSON);
INSERT INTO t VALUES(1,'{\"inner\":{\"x\":42}}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'{\"inner\":{\"x\":99}}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, json_extract(j,'\$.inner.x') AS x FROM t ORDER BY id;"

oracle "json_col_updated_on_one_side_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, j JSON);
INSERT INTO t VALUES(1,'{\"v\":1}'),(2,'{\"v\":2}');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET j='{\"v\":100}' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET j='{\"v\":200}' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, json_extract(j,'\$.v') AS v FROM t ORDER BY id;"

echo "--- REPLACE vs UPDATE probes ---"

oracle "replace_on_existing_pk_merges_like_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'feat_replaced');
REPLACE INTO t VALUES(2,'feat_new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "replace_many_times_same_pk_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,1);
REPLACE INTO t VALUES(1,2);
REPLACE INTO t VALUES(1,3);
REPLACE INTO t VALUES(1,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat final');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- delete-insert ordering probes ---"

oracle "delete_insert_same_id_same_branch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,100),(2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,999);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=888 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "delete_all_then_reinsert_subset_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t;
INSERT INTO t VALUES(1,1000);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=v*2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- truncate-like probes ---"

oracle "delete_all_one_side_insert_other_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1),(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat clears');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4),(5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

echo "--- mixed type merge probes ---"

oracle "mixed_int_text_real_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, s TEXT, r REAL);
INSERT INTO t VALUES(1,10,'alpha',1.5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET s='ALPHA' WHERE id=1;
UPDATE t SET r=2.75 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n, s, r FROM t;"

echo "--- ordering probes ---"

oracle "order_by_desc_with_nulls_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,NULL),(3,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,20),(5,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE n IS NOT NULL ORDER BY n DESC, id;"

oracle "order_by_multiple_keys_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
INSERT INTO t VALUES(1,'x',10),(2,'x',20),(3,'y',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'x',15),(5,'y',25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY a, b DESC;"

echo "--- replace stability probes ---"

oracle "replace_every_row_both_sides_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'f1'),(2,'f2'),(3,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat replaces');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"
echo "--- complex UPDATE probes ---"

oracle "update_with_min_subquery_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,100),(2,50),(3,75);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n = n - (SELECT min(n) FROM t);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat normalize');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

oracle "update_exists_filter_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, active INTEGER);
CREATE TABLE ids(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a',0),(2,'b',0),(3,'c',0);
INSERT INTO ids VALUES(1),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET active=1 WHERE EXISTS (SELECT 1 FROM ids WHERE ids.id=t.id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat activate');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'d',0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, active FROM t ORDER BY id;"

echo "--- boolean-like flags through merge ---"

oracle "zero_one_flags_toggled_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, active INTEGER);
INSERT INTO t VALUES(1,0),(2,1),(3,0),(4,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET active = 1 - active WHERE id IN (1,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat toggled');
SELECT dolt_checkout('main');
UPDATE t SET active = 0 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, active FROM t ORDER BY id;"

echo "--- INSERT col subset probes ---"

oracle "insert_subset_cols_different_on_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT DEFAULT 'da', b TEXT DEFAULT 'db', c TEXT DEFAULT 'dc');
INSERT INTO t(id) VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id,a) VALUES(2,'fa2');
INSERT INTO t(id,b) VALUES(3,'fb3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,c) VALUES(10,'mc10');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- long string payload probes ---"

oracle "long_string_update_one_side_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'short');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat long');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, length(v) AS L FROM t ORDER BY id;"
echo "--- multi-CTE probes ---"

oracle "chained_ctes_across_merge" "
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
" "WITH low AS (SELECT id, n FROM t WHERE n < 25), high AS (SELECT id, n FROM t WHERE n >= 25) SELECT id, n FROM low UNION ALL SELECT id, n FROM high ORDER BY id;"

oracle "cte_referencing_another_cte" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,5),(2,10),(3,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH doubled AS (SELECT id, n*2 AS d FROM t), filtered AS (SELECT id, d FROM doubled WHERE d > 15) SELECT id, d FROM filtered ORDER BY id;"

echo "--- RIGHT JOIN probes ---"

oracle "right_join_after_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, tag TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, tag TEXT);
INSERT INTO a VALUES(1,'a1'),(2,'a2');
INSERT INTO b VALUES(2,'b2'),(3,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO b VALUES(4,'b4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT COALESCE(a.tag, 'none') AS at, b.tag AS bt FROM a RIGHT JOIN b ON a.id=b.id ORDER BY b.tag;"

oracle "left_join_count_after_merge" "
CREATE TABLE owners(id INTEGER PRIMARY KEY);
CREATE TABLE items(id INTEGER PRIMARY KEY, owner_id INTEGER);
INSERT INTO owners VALUES(1),(2),(3);
INSERT INTO items VALUES(1,1),(2,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO items VALUES(3,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT owners.id, count(items.id) AS cnt FROM owners LEFT JOIN items ON owners.id=items.owner_id GROUP BY owners.id ORDER BY owners.id;"

echo "--- boolean expressions in SELECT ---"

oracle "comparison_result_as_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,5),(2,15),(3,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,35);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, CASE WHEN n > 20 THEN 1 ELSE 0 END AS big FROM t ORDER BY id;"

echo "--- computed-value INSERT probes ---"

oracle "insert_values_with_subquery_after_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, n INTEGER);
CREATE TABLE counts(label VARCHAR(32) PRIMARY KEY, n INTEGER);
INSERT INTO src VALUES(1,10),(2,20),(3,30);
INSERT INTO counts VALUES('seed', 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO counts VALUES('feat_cnt', (SELECT count(*) FROM src));
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat counts');
SELECT dolt_checkout('main');
INSERT INTO counts VALUES('main_cnt', 99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT label, n FROM counts ORDER BY label;"

echo "--- DELETE with subquery ---"

oracle "delete_where_in_select_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE blocklist(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');
INSERT INTO blocklist VALUES(2),(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (SELECT id FROM blocklist);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes blocked');
SELECT dolt_checkout('main');
INSERT INTO blocklist VALUES(5);
INSERT INTO t VALUES(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- multi-col computed UPDATE ---"

oracle "update_ab_from_c_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);
INSERT INTO t VALUES(1,0,0,5),(2,0,0,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a = c*2, b = c+1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat compute');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,7,8,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

echo "--- self-join probes ---"

oracle "self_join_parent_child_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INTEGER);
INSERT INTO t VALUES(1,NULL),(2,1),(3,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,2),(5,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT c.id AS child, p.id AS parent FROM t c LEFT JOIN t p ON c.parent_id=p.id ORDER BY c.id;"

oracle "self_join_sibling_count_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, gid INTEGER, v TEXT);
INSERT INTO t VALUES(1,1,'a'),(2,1,'b'),(3,2,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,2,'d'),(5,1,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT a.id, count(b.id) AS siblings FROM t a LEFT JOIN t b ON a.gid=b.gid AND a.id<>b.id GROUP BY a.id ORDER BY a.id;"

echo "--- GROUP_CONCAT probes ---"

oracle "group_concat_default_separator_after_merge" "
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
" "SELECT GROUP_CONCAT(v) AS g FROM (SELECT v FROM t ORDER BY id) sub;"

oracle "group_concat_per_group_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, v TEXT);
INSERT INTO t VALUES(1,'a','aa'),(2,'a','ab'),(3,'b','ba');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'b','bb'),(5,'a','ac');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp;"

echo "--- complex subquery probes ---"

oracle "scalar_subquery_in_projection_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v, (SELECT sum(v) FROM t) AS total FROM t ORDER BY id;"

oracle "scalar_subquery_bound_by_id_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
CREATE TABLE refs(id INTEGER PRIMARY KEY, target INTEGER);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
INSERT INTO refs VALUES(10,1),(11,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO refs VALUES(12,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT r.id, (SELECT n FROM t WHERE t.id=r.target) AS tgt_n FROM refs r ORDER BY r.id;"

echo "--- ordered UPDATE probes ---"

oracle "update_value_via_row_position_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pos INTEGER);
INSERT INTO t VALUES(1,50,0),(2,100,0),(3,25,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET pos = (SELECT count(*) FROM t AS t2 WHERE t2.v > t.v) + 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat ranks');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,75,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, pos FROM t ORDER BY id;"

echo "--- cross-table DELETE probes ---"

oracle "delete_where_id_in_join_result_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE blocked(id INTEGER PRIMARY KEY, reason TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');
INSERT INTO blocked VALUES(2,'r1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (SELECT b.id FROM blocked b);
INSERT INTO blocked VALUES(4,'r4');
DELETE FROM t WHERE id IN (SELECT b.id FROM blocked b);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- INSERT SELECT probes ---"

oracle "insert_select_filtered_after_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE dst(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO src VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dst SELECT id, v FROM src WHERE v >= 20;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat copy');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM dst ORDER BY id;"

oracle "insert_select_double_then_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE dst(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO src VALUES(1,5),(2,10),(3,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dst SELECT id, v*2 FROM src;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(4,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM dst ORDER BY id;"

echo "--- UPDATE-from subquery probes ---"

oracle "update_from_lookup_table_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE upd(id INTEGER PRIMARY KEY, new_v INTEGER);
INSERT INTO t VALUES(1,0),(2,0);
INSERT INTO upd VALUES(1,100),(2,200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v = (SELECT new_v FROM upd WHERE upd.id=t.id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat apply');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,999);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- UPDATE CASE probes ---"

oracle "update_case_multi_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, tier TEXT);
INSERT INTO t VALUES(1,5,''),(2,15,''),(3,25,''),(4,50,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET tier = CASE
  WHEN n < 10 THEN 'S'
  WHEN n < 20 THEN 'M'
  WHEN n < 40 THEN 'L'
  ELSE 'XL'
END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,100,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n, tier FROM t ORDER BY id;"

echo "--- INSERT SELECT computed probes ---"

oracle "insert_select_arithmetic_row_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES(1,10);
INSERT INTO b VALUES(1),(2),(3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a SELECT id+10, id*100 FROM b;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO b VALUES(4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM a ORDER BY id;"

echo "--- CTE aggregation ---"

oracle "cte_with_having_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',30),(5,'b',100),(6,'c',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH totals AS (SELECT grp, sum(n) AS s FROM t GROUP BY grp) SELECT grp, s FROM totals WHERE s > 10 ORDER BY grp;"

echo "--- computed WHERE probes ---"

oracle "where_by_mod_expression_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,15),(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,25),(5,33);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE n % 5 = 0 ORDER BY id;"

echo "--- INSERT partial cols + subquery ---"

oracle "insert_partial_then_subquery_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER DEFAULT 10, s INTEGER);
INSERT INTO t(id, s) VALUES(1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id, v, s) VALUES(2, (SELECT sum(v) FROM t) + 5, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id, s) VALUES(3, 300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, s FROM t ORDER BY id;"
echo "--- subquery in WHERE with NULL ---"

oracle "in_subquery_with_null_after_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE b(id INTEGER PRIMARY KEY, ref INTEGER);
INSERT INTO a VALUES(1,10),(2,20);
INSERT INTO b VALUES(10,1),(11,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO b VALUES(12,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM a WHERE id IN (SELECT ref FROM b) ORDER BY id;"

echo "--- nested UPDATE probes ---"

oracle "update_set_based_on_avg_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET n = n - (SELECT n FROM t WHERE id=2) WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

echo "--- WITH + DELETE probes ---"

oracle "with_cte_as_delete_filter" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5),(4,'a',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (WITH top AS (SELECT id FROM t WHERE grp='a' AND n > 15) SELECT id FROM top);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'c',100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, grp, n FROM t ORDER BY id;"

echo "--- sparse update probes ---"

oracle "sparse_updates_across_ids_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=100 WHERE id=1;
UPDATE t SET v=100 WHERE id=5;
UPDATE t SET v=100 WHERE id=9;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat sparse');
SELECT dolt_checkout('main');
UPDATE t SET v=200 WHERE id=2;
UPDATE t SET v=200 WHERE id=6;
UPDATE t SET v=200 WHERE id=10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main sparse');
SELECT dolt_merge('feat');
" "SELECT sum(v) FROM t;"

echo "--- aggregate pruning probes ---"

oracle "sum_filtered_by_where_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'a',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',100),(5,'c',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, sum(n) AS s FROM t WHERE n >= 20 GROUP BY grp HAVING sum(n) > 40 ORDER BY grp;"

echo "--- boolean operator edge ---"

oracle "not_null_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,NULL),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,NULL),(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE v IS NOT NULL AND v > 20 ORDER BY id;"

echo "--- bit flag patterns ---"

oracle "flags_with_bitwise_and_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flags INTEGER);
INSERT INTO t VALUES(1,5),(2,6);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,7),(4,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, flags & 1 AS bit0 FROM t ORDER BY id;"

echo "--- recursive CTE generator ---"

oracle "recursive_cte_series_join_post_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO t VALUES(3,'three'),(5,'five');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(7,'seven');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<10) SELECT n, COALESCE((SELECT label FROM t WHERE t.id=nums.n),'none') AS lbl FROM nums ORDER BY n;"

echo "--- update affecting nothing ---"

oracle "update_where_false_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=999 WHERE id=999;
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=v WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- order+limit probes ---"

oracle "max_via_order_limit_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,30),(2,10),(3,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY v DESC LIMIT 1;"

echo "--- agg in subquery ---"

oracle "subquery_with_order_in_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, v INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',30),(3,'b',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',25),(5,'b',40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, max(v) AS mx FROM t GROUP BY grp ORDER BY grp;"

echo "--- text encoding probes ---"

oracle "unicode_accented_text_through_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'café'),(2,'naïve');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'résumé');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'über');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "emoji_in_text_through_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello 👋');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'party 🎉');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- BLOB through merge ---"

oracle "blob_update_one_side_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB, tag TEXT);
INSERT INTO t VALUES(1, X'DEADBEEF', 'initial');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET b=X'CAFEBABE' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat blob');
SELECT dolt_checkout('main');
UPDATE t SET tag='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main tag');
SELECT dolt_merge('feat');
" "SELECT id, hex(b), tag FROM t;"

echo "--- UPDATE cte source ---"

oracle "update_via_cte_subquery_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v = (WITH c AS (SELECT max(id) AS mx FROM t) SELECT mx+id FROM c);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- convergent update-same-col same-value ---"

oracle "both_sides_set_same_col_same_value_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='SHARED' WHERE id=1;
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='SHARED' WHERE id=1;
INSERT INTO t VALUES(3,'main3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- UPDATE subquery 2 tables ---"

oracle "update_select_from_join_merge" "
CREATE TABLE items(id INTEGER PRIMARY KEY, category_id INTEGER, price INTEGER);
CREATE TABLE categories(id INTEGER PRIMARY KEY, multiplier INTEGER);
INSERT INTO items VALUES(1,1,10),(2,2,20),(3,1,30);
INSERT INTO categories VALUES(1,2),(2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE items SET price = price * (SELECT multiplier FROM categories WHERE categories.id=items.category_id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat compute');
SELECT dolt_checkout('main');
INSERT INTO items VALUES(4,1,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, category_id, price FROM items ORDER BY id;"

echo "--- rebase-like flow ---"

oracle "branch_reset_to_main_then_merge" "
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
SELECT dolt_checkout('feat');
SELECT dolt_reset('--hard','main');
INSERT INTO t VALUES(99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat post-reset');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY id;"

echo "--- nested WHERE joins ---"

oracle "two_join_with_agg_filter_after_merge" "
CREATE TABLE customers(id INTEGER PRIMARY KEY, region TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, cid INTEGER, amount INTEGER);
INSERT INTO customers VALUES(1,'east'),(2,'west'),(3,'east');
INSERT INTO orders VALUES(1,1,100),(2,2,200),(3,3,150);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO orders VALUES(4,1,50),(5,3,250);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT c.region, sum(o.amount) AS total FROM customers c JOIN orders o ON c.id=o.cid GROUP BY c.region ORDER BY c.region;"

echo "--- INSERT SELECT LIMIT ---"

oracle "insert_select_limit_after_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE dst(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO src VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dst SELECT id, v FROM src ORDER BY v DESC LIMIT 3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat copy top3');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(6,60);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM dst ORDER BY id;"

echo "--- UPDATE expression ---"

oracle "update_case_increment_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, s TEXT);
INSERT INTO t VALUES(1,5,''),(2,15,''),(3,25,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET s = CASE WHEN n < 10 THEN 'low' WHEN n < 20 THEN 'mid' ELSE 'high' END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat categorized');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,50,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n, s FROM t ORDER BY id;"

echo "--- LIKE anchored ---"

oracle "like_anchored_prefix_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'apple'),(2,'banana'),(3,'apricot');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'avocado'),(5,'berry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE name LIKE 'a%' ORDER BY id;"

echo "--- BETWEEN edges ---"

oracle "between_inclusive_edges_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40),(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t WHERE v BETWEEN 20 AND 40 ORDER BY id;"

echo "--- INSERT SELECT GROUP BY ---"

oracle "insert_select_group_by_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, grp VARCHAR(8), n INTEGER);
CREATE TABLE totals(grp VARCHAR(8) PRIMARY KEY, total INTEGER);
INSERT INTO src VALUES(1,'a',10),(2,'a',20),(3,'b',5);
INSERT INTO totals VALUES('seed',0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO totals SELECT grp, sum(n) FROM src GROUP BY grp;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat totals');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(4,'c',100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT grp, total FROM totals ORDER BY grp;"

echo "--- multi-row REPLACE ---"

oracle "multi_row_replace_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,10),(2,20),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat replaces');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

echo "--- tie-break ORDER BY ---"

oracle "order_by_with_tiebreak_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',10),(3,'a',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'b',20),(5,'a',10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM t ORDER BY n, grp, id;"
