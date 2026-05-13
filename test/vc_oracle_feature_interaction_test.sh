#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize() {
  tr -d '\r' | grep -v '^$' | sort
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  printf "%s\n" "$setup" | "$DOLTLITE" "$dir/dl/db" \
      >/dev/null 2>"$dir/dl.err"
  local dl_out
  dl_out=$(printf ".headers off\n.mode csv\n%s\n" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
           | grep -vi 'already up to date' \
           | grep -vi 'Fast-forward' \
           | tr -d '"' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dolt_query
  dolt_query=$(vc_oracle_translate_for_dolt "$query")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n" "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>>"$dir/dt.err" \
      | tail -n +2 | tr -d '"'
  ) 2>/dev/null
  dt_out=$(echo "$dt_out" | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Feature Interaction Oracle Tests ==="
echo ""

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

echo "--- undo patterns ---"

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
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
