#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_at() Point-in-Time Query Tests ==="
echo ""

DB=/tmp/test_at_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "basic_c1" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 2));" \
  "1" "$DB"

run_test "basic_c2" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "2" "$DB"

run_test "basic_c3" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

run_test "basic_commit_ref_hash" \
  "SELECT commit_ref FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1)) LIMIT 1;" \
  "$(echo "SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1;" | $DOLTLITE "$DB" 2>/dev/null)" "$DB"

rm -f "$DB"

DB=/tmp/test_at_quoted_$$.db; rm -f "$DB"
echo "CREATE TABLE \"odd\"\"name\"(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO \"odd\"\"name\" VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "quoted_name_count" \
  "SELECT count(*) FROM \"dolt_at_odd\"\"name\"( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

run_test "quoted_name_value" \
  "SELECT v FROM \"dolt_at_odd\"\"name\"( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "a" "$DB"

rm -f "$DB"

DB=/tmp/test_at_rowid_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(20,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "rowid_c1_10" \
  "SELECT id FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "10" "$DB"

run_test "rowid_c2_count" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_branch_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main_row');
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_feat" "SELECT count(*) FROM dolt_at_t( 'feat');" "2" "$DB"

run_test "branch_main" "SELECT count(*) FROM dolt_at_t( 'main');" "2" "$DB"

run_test "branch_commit_ref_name" \
  "SELECT commit_ref FROM dolt_at_t( 'feat') LIMIT 1;" "feat" "$DB"

run_test "branch_feat_has2" \
  "SELECT count(*) FROM dolt_at_t( 'feat') WHERE id=2;" "1" "$DB"
run_test "branch_feat_no3" \
  "SELECT count(*) FROM dolt_at_t( 'feat') WHERE id=3;" "0" "$DB"
run_test "branch_main_has3" \
  "SELECT count(*) FROM dolt_at_t( 'main') WHERE id=3;" "1" "$DB"
run_test "branch_main_no2" \
  "SELECT count(*) FROM dolt_at_t( 'main') WHERE id=2;" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_at_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','release 1');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','release 2');
SELECT dolt_tag('v2.0');
INSERT INTO t VALUES(3,'v3');
SELECT dolt_commit('-A','-m','release 3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tag_v1" "SELECT count(*) FROM dolt_at_t( 'v1.0');" "1" "$DB"
run_test "tag_v2" "SELECT count(*) FROM dolt_at_t( 'v2.0');" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_parents_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "parent_first_ref" "SELECT count(*) FROM dolt_at_t( 'HEAD^1');" "2" "$DB"
run_test "parent_second_ref" "SELECT count(*) FROM dolt_at_t( 'HEAD^2');" "2" "$DB"
run_test "parent_first_has_main_row" "SELECT count(*) FROM dolt_at_t( 'HEAD^1') WHERE id=3;" "1" "$DB"
run_test "parent_second_has_feat_row" "SELECT count(*) FROM dolt_at_t( 'HEAD^2') WHERE id=2;" "1" "$DB"

HASH=$(echo "SELECT dolt_hashof('HEAD^2');" | $DOLTLITE "$DB" 2>&1)
run_test "parent_second_hash" "SELECT count(*) FROM dolt_at_t( '$HASH');" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_branch_from_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_tag('v1.0','HEAD~1');
SELECT dolt_branch('from_tag','v1.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "branch_from_tag_reopen_at_head" "SELECT count(*) FROM dolt_at_t( 'HEAD');" "1" "$DB/from_tag"
run_test "branch_from_tag_reopen_at_main" "SELECT count(*) FROM dolt_at_t( 'main');" "2" "$DB/from_tag"

rm -f "$DB"

DB=/tmp/test_at_update_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='changed' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "update_c1_count" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "1" "$DB"
run_test "update_c2_count" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

run_test_match "update_diff_vals" \
  "SELECT CASE WHEN a.v != b.v THEN 'different' ELSE 'same' END FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1)) a, dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1)) b WHERE a.id=b.id;" \
  "different" "$DB"

rm -f "$DB"

DB=/tmp/test_at_delete_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c1');
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "delete_c1" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "2" "$DB"
run_test "delete_c2" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

rm -f "$DB"

DB=/tmp/test_at_notable_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "notable" \
  "SELECT count(*) FROM dolt_at_nonexistent( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "no such table" "$DB"

rm -f "$DB"

DB=/tmp/test_at_noref_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "noref" \
  "SELECT count(*) FROM dolt_at_t( 'nonexistent_branch');" \
  "ref not found" "$DB"

rm -f "$DB"

DB=/tmp/test_at_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO orders VALUES(2,'Gadget');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

HASH=$(echo "SELECT commit_hash FROM dolt_log LIMIT 1;" | $DOLTLITE "$DB" 2>&1)

run_test "multi_users" "SELECT count(*) FROM dolt_at_users( '$HASH');" "1" "$DB"
run_test "multi_orders" "SELECT count(*) FROM dolt_at_orders( '$HASH');" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "persist_c1" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "1" "$DB"
run_test "persist_c2" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_late_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','c1');
CREATE TABLE t2(id INTEGER PRIMARY KEY);
INSERT INTO t2 VALUES(1);
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "late_no_t2" \
  "SELECT count(*) FROM dolt_at_t2( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "0" "$DB"

run_test "late_has_t2" \
  "SELECT count(*) FROM dolt_at_t2( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

rm -f "$DB"

DB=/tmp/test_at_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_tag('before_merge');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_before" "SELECT count(*) FROM dolt_at_t( 'before_merge');" "1" "$DB"

run_test "merge_feat" "SELECT count(*) FROM dolt_at_t( 'feat');" "2" "$DB"

run_test "merge_head" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "3" "$DB"

rm -f "$DB"

DB=/tmp/test_at_gc_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_c1" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "1" "$DB"
run_test "gc_c2" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_compare_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'old');
INSERT INTO t VALUES(2,'old');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='new' WHERE id=1;
INSERT INTO t VALUES(3,'added');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "compare_current" "SELECT count(*) FROM t;" "3" "$DB"

run_test "compare_old" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "2" "$DB"

rm -f "$DB"

DB=/tmp/test_at_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','with data');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_at_c1" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" \
  "0" "$DB"
run_test "empty_at_c2" \
  "SELECT count(*) FROM dolt_at_t( (SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"

rm -f "$DB"

# A historical root whose key shape differs from the declared schema (table
# dropped and recreated across a rowid/WITHOUT ROWID boundary) must render
# the historical row values -- matching Dolt's AS OF -- and pushed-down PK
# constraints must still filter (they were silently dropped for such roots).
DB=/tmp/test_at_keyshape_$$.db; rm -f "$DB"
echo "CREATE TABLE s(a TEXT PRIMARY KEY, b TEXT) WITHOUT ROWID;
INSERT INTO s VALUES('x','one'),('y','two');
SELECT dolt_commit('-Am','v1');
SELECT dolt_tag('oldtag');
DROP TABLE s;
CREATE TABLE s(a INTEGER PRIMARY KEY, b TEXT);
INSERT INTO s VALUES(1,'one'),(2,'two'),(7,'seven');
SELECT dolt_commit('-Am','v2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "keyshape_values_render" \
  "SELECT a, b FROM dolt_at_s WHERE commit_ref='oldtag' ORDER BY a;" \
  "x|one
y|two" "$DB"
run_test "keyshape_eq_filters" \
  "SELECT count(*) FROM dolt_at_s WHERE commit_ref='oldtag' AND a=7;" \
  "0" "$DB"
run_test "keyshape_eq_matches_text" \
  "SELECT b FROM dolt_at_s WHERE commit_ref='oldtag' AND a='y';" \
  "two" "$DB"
run_test "keyshape_current_seek_intact" \
  "SELECT b FROM dolt_at_s WHERE commit_ref='HEAD' AND a=7;" \
  "seven" "$DB"
run_test "keyshape_upper_bound_scans_all" \
  "SELECT count(*) FROM dolt_at_s WHERE commit_ref='oldtag' AND a<'z';" \
  "2" "$DB"

rm -f "$DB"

dltest_finish