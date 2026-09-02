#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_diff_<table> Audit Log Tests ==="
echo ""

DB=/tmp/test_dt_basic_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Alice');
INSERT INTO t VALUES(2,'Bob');
SELECT dolt_commit('-A','-m','initial');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "basic_count" "SELECT count(*) FROM dolt_diff_t;" "2" "$DB"
run_test_match "basic_type" "SELECT diff_type FROM dolt_diff_t LIMIT 1;" "added" "$DB"
run_test_match "basic_rowid" "SELECT rowid_val FROM dolt_diff_t WHERE to_v IS NOT NULL;" "1" "$DB"
run_test_match "basic_to_commit" "SELECT length(to_commit) FROM dolt_diff_t LIMIT 1;" "40" "$DB"
run_test_match "basic_to_date" "SELECT to_commit_date FROM dolt_diff_t LIMIT 1;" "^[0-9]" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_multi_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='A' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "multi_count" "SELECT count(*) FROM dolt_diff_t;" "3" "$DB"

run_test_match "multi_has_added" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" "^2$" "$DB"
run_test_match "multi_has_modified" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" "^1$" "$DB"

run_test "multi_commits" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "3" "$DB"
run_test "multi_to_commit_count" "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='c2');" "1" "$DB"
run_test "multi_to_commit_row" "SELECT diff_type || ':' || coalesce(to_id, from_id) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='c2');" "added:2" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_context_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "ctx_to_hash" "SELECT to_commit FROM dolt_diff_t LIMIT 1;" "^[0-9a-f]{40}$" "$DB"
run_test_match "ctx_from_hash" "SELECT from_commit FROM dolt_diff_t WHERE diff_type='added' ;" "^[0-9a-f]{40}$" "$DB"

run_test_match "ctx_to_date" "SELECT to_commit_date FROM dolt_diff_t LIMIT 1;" "^[0-9]{4}-" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_persist_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "persist_count" "SELECT count(*) FROM dolt_diff_t;" "2" "$DB"
run_test_match "persist_type" "SELECT diff_type FROM dolt_diff_t WHERE to_v IS NOT NULL;" "added" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_tables_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT);
INSERT INTO users VALUES(1,'Alice');
INSERT INTO orders VALUES(1,'Widget');
INSERT INTO orders VALUES(2,'Gadget');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tables_users" "SELECT count(*) FROM dolt_diff_users;" "1" "$DB"
run_test "tables_orders" "SELECT count(*) FROM dolt_diff_orders;" "2" "$DB"

echo "INSERT INTO users VALUES(2,'Bob');
SELECT dolt_commit('-A','-m','add bob');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tables_users_after" "SELECT count(*) FROM dolt_diff_users;" "2" "$DB"
run_test "tables_orders_after" "SELECT count(*) FROM dolt_diff_orders;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main add');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "merge_count" "SELECT count(*) FROM dolt_diff_t;" "^[3-9]" "$DB"
run_test_match "merge_has_merge_commit" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "^[3-9]" "$DB"
run_test "merge_to_commit_eq_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "2" "$DB"
run_test "merge_to_commit_eq_matches_plus" \
  "SELECT (SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1)) = (SELECT count(*) FROM dolt_diff_t WHERE +to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DB"
run_test "merge_to_commit_head_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='HEAD';" \
  "2" "$DB"
run_test "merge_to_commit_distinct_parents" \
  "SELECT count(DISTINCT from_commit) FROM dolt_diff_t WHERE to_commit='HEAD';" \
  "2" "$DB"
run_test "merge_to_commit_head_ids" \
  "SELECT group_concat(coalesce(to_id, from_id), ',') FROM (SELECT to_id, from_id FROM dolt_diff_t WHERE to_commit='HEAD' ORDER BY coalesce(to_id, from_id));" \
  "2,3" "$DB"

rm -f "$DB"

# Two-parent merge whose parents each change a different row: the to_commit
# EQ pushdown must emit both parent pairs, matching the unconstrained scan.
DBMTC=/tmp/test_dt_merge_to_commit_$$.db; rm -f "$DBMTC"
echo "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(100,'a'),(200,'b');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('side');
UPDATE t SET v='main' WHERE k=100;
SELECT dolt_commit('-Am','mainc');
SELECT dolt_checkout('side');
UPDATE t SET v='side' WHERE k=200;
SELECT dolt_commit('-Am','sidec');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');" | $DOLTLITE "$DBMTC" > /dev/null 2>&1

run_test "merge_to_commit_both_rows" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "2" "$DBMTC"
run_test "merge_to_commit_both_keys" \
  "SELECT group_concat(coalesce(to_k, from_k), ',') FROM (SELECT to_k, from_k FROM dolt_diff_t WHERE to_commit='HEAD' ORDER BY coalesce(to_k, from_k));" \
  "100,200" "$DBMTC"
run_test "merge_to_commit_plus_agrees" \
  "SELECT (SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1)) = (SELECT count(*) FROM dolt_diff_t WHERE +to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "1" "$DBMTC"

rm -f "$DBMTC"

DB=/tmp/test_dt_cp_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "cp_count" "SELECT count(*) FROM dolt_diff_t;" "^[2-9]" "$DB"
run_test_match "cp_has_row2" "SELECT count(*) FROM dolt_diff_t WHERE to_v IS NOT NULL;" "^[1-9]" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_revert_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
INSERT INTO t VALUES(2,'to_revert');
SELECT dolt_commit('-A','-m','add row 2');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "revert_count" "SELECT count(*) FROM dolt_diff_t;" "^[3-9]" "$DB"
run_test_match "revert_has_removed" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='removed';" "^[1-9]" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_empty_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','empty table');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "empty_count" "SELECT count(*) FROM dolt_diff_t;" "0" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_history_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 9); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

run_test "history_count" "SELECT count(*) FROM dolt_diff_t;" "10" "$DB"
run_test "history_added" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='added';" "1" "$DB"
run_test "history_modified" "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" "9" "$DB"
run_test "history_commits" "SELECT count(DISTINCT to_commit) FROM dolt_diff_t;" "10" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_unchanged_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'keep');
INSERT INTO u VALUES(1,'u0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 25); do
  echo "UPDATE u SET v='u$i' WHERE id=1;
SELECT dolt_commit('-A','-m','u$i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

run_test "unchanged_table_diff_count" "SELECT count(*) FROM dolt_diff_t;" "1" "$DB"
run_test "unchanged_table_diff_type" "SELECT diff_type FROM dolt_diff_t;" "added" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_quoted_$$.db; rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE "odd""name"(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO "odd""name" VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
EOF

run_test "quoted_diff_count" 'SELECT count(*) FROM "dolt_diff_odd""name";' "1" "$DB"
run_test_match "quoted_diff_type" 'SELECT diff_type FROM "dolt_diff_odd""name";' "added" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_newt_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init t');
CREATE TABLE t2(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_commit('-A','-m','add t2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "newt_t2_count" "SELECT count(*) FROM dolt_diff_t2;" "1" "$DB"
run_test "newt_t2_type" "SELECT diff_type FROM dolt_diff_t2;" "added" "$DB"

rm -f "$DB"

DB=/tmp/test_dt_vals_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='world' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "vals_add_from" "SELECT typeof(from_v) FROM dolt_diff_t WHERE diff_type='added';" "null" "$DB"
run_test_match "vals_add_to" "SELECT typeof(to_v) FROM dolt_diff_t WHERE diff_type='added';" "text" "$DB"

run_test_match "vals_mod_from" "SELECT typeof(from_v) FROM dolt_diff_t WHERE diff_type='modified';" "text" "$DB"
run_test_match "vals_mod_to" "SELECT typeof(to_v) FROM dolt_diff_t WHERE diff_type='modified';" "text" "$DB"

rm -f "$DB"

# Recreate across key shapes pairs nothing; a raw-key collision is remove+add.
DBS=/tmp/test_dt_shape_$$.db; rm -f "$DBS"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;
INSERT INTO t VALUES('AAAAA', 1);
SELECT dolt_commit('-Am','c1');
DROP TABLE t;
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(-5385951930834944000, 2);
SELECT dolt_commit('-Am','c2');" | $DOLTLITE "$DBS" > /dev/null 2>&1

run_test "shape_from_pk_text" \
  "SELECT from_pk || '=' || from_v FROM dolt_diff_t WHERE diff_type='removed' AND to_commit=(SELECT commit_hash FROM dolt_log WHERE message='c2');" \
  "AAAAA=1" "$DBS"
run_test "shape_to_pk_int" \
  "SELECT to_pk || '=' || to_v FROM dolt_diff_t WHERE diff_type='added' AND to_commit=(SELECT commit_hash FROM dolt_log WHERE message='c2');" \
  "-5385951930834944000=2" "$DBS"
run_test "shape_no_modified_pairing" \
  "SELECT count(*) FROM dolt_diff_t WHERE diff_type='modified';" \
  "0" "$DBS"
rm -f "$DBS"


# Historical sides render by column name at that commit, not current positions.
DBN=/tmp/test_dt_namemap_$$.db; rm -f "$DBN"
echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'BEE','CEE');
SELECT dolt_commit('-Am','base');
ALTER TABLE t DROP COLUMN b;
SELECT dolt_commit('-Am','drop_b');" | $DOLTLITE "$DBN" > /dev/null 2>&1

run_test "namemap_from_after_drop" \
  "SELECT from_a || '=' || from_c FROM dolt_diff_t WHERE diff_type='modified';" \
  "1=CEE" "$DBN"
run_test "namemap_to_at_old_commit" \
  "SELECT to_a || '=' || to_c FROM dolt_diff_t WHERE diff_type='added';" \
  "1=CEE" "$DBN"

# Re-adding a dropped column moves it to the declared end.
DBR=/tmp/test_dt_readd_$$.db; rm -f "$DBR"
echo "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c TEXT);
INSERT INTO t VALUES(1,'BEE','CEE');
SELECT dolt_commit('-Am','base');
ALTER TABLE t DROP COLUMN b;
ALTER TABLE t ADD COLUMN b TEXT;
UPDATE t SET b='NEWBEE';
SELECT dolt_commit('-Am','moved_b');" | $DOLTLITE "$DBR" > /dev/null 2>&1

run_test "namemap_moved_col_from" \
  "SELECT from_b || '/' || from_c FROM dolt_diff_t WHERE diff_type='modified';" \
  "BEE/CEE" "$DBR"
run_test "namemap_moved_col_to" \
  "SELECT to_b || '/' || to_c FROM dolt_diff_t WHERE diff_type='modified';" \
  "NEWBEE/CEE" "$DBR"

# Decode each side's key with that side's PK definition.
DBK=/tmp/test_dt_pkswap_$$.db; rm -f "$DBK"
echo "CREATE TABLE t(a TEXT, b TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t VALUES('k1','k2');
SELECT dolt_commit('-Am','base');
DROP TABLE t;
CREATE TABLE t(b TEXT, a TEXT, PRIMARY KEY(b,a)) WITHOUT ROWID;
INSERT INTO t VALUES('k2','k1');
SELECT dolt_commit('-Am','swap');" | $DOLTLITE "$DBK" > /dev/null 2>&1

run_test "namemap_pkswap_from" \
  "SELECT from_a || '/' || from_b FROM dolt_diff_t WHERE diff_type='removed';" \
  "k1/k2" "$DBK"

rm -f "$DBN" "$DBR" "$DBK"
# Unreachable to_commit is empty; +to_commit must agree with the unfiltered scan.
DBG=/tmp/test_dt_ancgate_$$.db; rm -f "$DBG"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feat');" | $DOLTLITE "$DBG" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'feat-only');
SELECT dolt_commit('-Am','feat_commit');" | $DOLTLITE "$DBG/feat" > /dev/null 2>&1

run_test "ancestry_gate_foreign_commit" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log('feat') LIMIT 1);" \
  "0" "$DBG"
run_test "ancestry_gate_subset_of_full_scan" \
  "SELECT (SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log('feat') LIMIT 1)) = (SELECT count(*) FROM dolt_diff_t WHERE +to_commit=(SELECT commit_hash FROM dolt_log('feat') LIMIT 1));" \
  "1" "$DBG"
run_test "ancestry_gate_reachable_commit" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log LIMIT 1);" \
  "1" "$DBG"

rm -f "$DBG"


# STORED generated columns occupy a record slot; VIRTUAL ones do not.
DBG1=/tmp/test_dt_gencol_$$.db; rm -f "$DBG1"
echo "CREATE TABLE g(a INTEGER PRIMARY KEY, b TEXT, gs TEXT GENERATED ALWAYS AS (b||'S') STORED, gv TEXT AS (b||'V') VIRTUAL, c TEXT);
INSERT INTO g(a,b,c) VALUES(1,'x','c1');
SELECT dolt_commit('-Am','base','--date','2020-01-01T00:00:00');
UPDATE g SET c='c2';
SELECT dolt_commit('-am','upd','--date','2020-01-02T00:00:00');" | $DOLTLITE "$DBG1" > /dev/null 2>&1

run_test "gencol_diff_sides" \
  "SELECT to_c || '/' || from_c FROM dolt_diff_g WHERE diff_type='modified';" \
  "c2/c1" "$DBG1"
run_test "gencol_live_matches_diff" "SELECT c FROM g;" "c2" "$DBG1"
run_test "gencol_history" \
  "SELECT group_concat(c,',') FROM (SELECT c FROM dolt_history_g ORDER BY commit_date);" \
  "c1,c2" "$DBG1"
run_test "gencol_at" "SELECT c FROM dolt_at_g WHERE commit_ref='HEAD~1';" "c1" "$DBG1"

# Clustered layout puts keys first, generated included.
DBG2=/tmp/test_dt_gencol_wr_$$.db; rm -f "$DBG2"
echo "CREATE TABLE w(k TEXT, b TEXT, gs TEXT GENERATED ALWAYS AS (b||'S') STORED, c TEXT, PRIMARY KEY(k)) WITHOUT ROWID;
INSERT INTO w(k,b,c) VALUES('k1','x','c1');
SELECT dolt_commit('-Am','base','--date','2020-01-01T00:00:00');
UPDATE w SET c='c2';
SELECT dolt_commit('-am','upd','--date','2020-01-02T00:00:00');" | $DOLTLITE "$DBG2" > /dev/null 2>&1

run_test "gencol_clustered_diff_sides" \
  "SELECT to_c || '/' || from_c FROM dolt_diff_w WHERE diff_type='modified';" \
  "c2/c1" "$DBG2"
run_test "gencol_clustered_history" \
  "SELECT group_concat(c,',') FROM (SELECT c FROM dolt_history_w ORDER BY commit_date);" \
  "c1,c2" "$DBG2"

DBG3=/tmp/test_dt_gencol_nlpk_$$.db; rm -f "$DBG3"
echo "CREATE TABLE z(a TEXT, k TEXT PRIMARY KEY, gs TEXT GENERATED ALWAYS AS (a||'S') STORED, c TEXT) WITHOUT ROWID;
INSERT INTO z(a,k,c) VALUES('av','k1','c1');
SELECT dolt_commit('-Am','base');
UPDATE z SET c='c2';
SELECT dolt_commit('-am','upd');" | $DOLTLITE "$DBG3" > /dev/null 2>&1

run_test "gencol_nonleading_pk_diff" \
  "SELECT to_a || '/' || to_c FROM dolt_diff_z WHERE diff_type='modified';" \
  "av/c2" "$DBG3"

rm -f "$DBG1" "$DBG2" "$DBG3"

# Revision specs in from_commit/to_commit constraints resolve like the
# function form; both-ends-named is the arbitrary-pair diff.
DBRS=/tmp/test_dt_revspec_$$.db; rm -f "$DBRS"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='A' WHERE id=1;
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='B' WHERE id=2;
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DBRS" > /dev/null 2>&1

run_test "revspec_from_to_adjacent" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='HEAD~1' AND to_commit='HEAD';" \
  "1" "$DBRS"
run_test "revspec_from_to_nonadjacent_slice" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='HEAD~2' AND to_commit='HEAD';" \
  "2" "$DBRS"
run_test "revspec_to_only" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='HEAD';" \
  "1" "$DBRS"
run_test "revspec_from_only" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='HEAD~1';" \
  "1" "$DBRS"
run_test "revspec_from_only_row" \
  "SELECT diff_type || ':' || to_v FROM dolt_diff_t WHERE from_commit='HEAD~1';" \
  "modified:B" "$DBRS"
run_test "revspec_hash_pair_still_works" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit=(SELECT commit_hash FROM dolt_log WHERE message='c2') AND to_commit=(SELECT commit_hash FROM dolt_log WHERE message='c3');" \
  "1" "$DBRS"
run_test_match "revspec_garbage_from_rejected" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='nosuchref' AND to_commit='HEAD';" \
  "dolt_diff_t: ref not found: nosuchref" "$DBRS"
run_test_match "revspec_garbage_to_rejected" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='garbage';" \
  "dolt_diff_t: ref not found: garbage" "$DBRS"
run_test_match "revspec_garbage_from_only_rejected" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='nosuchref';" \
  "dolt_diff_t: ref not found: nosuchref" "$DBRS"
run_test_match "revspec_invalid_ancestor_from_only_rejected" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='HEAD~99';" \
  "dolt_diff_t: invalid ref: HEAD~99" "$DBRS"
run_test_match "revspec_invalid_ancestor_to_only_rejected" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit='HEAD~99';" \
  "dolt_diff_t: invalid ref: HEAD~99" "$DBRS"
run_test_match "revspec_tvf_garbage_from_rejected" \
  "SELECT count(*) FROM dolt_diff_t('nosuchref','HEAD');" \
  "dolt_diff_t: ref not found: nosuchref" "$DBRS"
run_test_match "revspec_tvf_garbage_to_rejected" \
  "SELECT count(*) FROM dolt_diff_t('HEAD','nosuchref');" \
  "dolt_diff_t: ref not found: nosuchref" "$DBRS"
run_test_match "revspec_tvf_hash_prefix_rejected" \
  "SELECT count(*) FROM dolt_diff_t('abc123','HEAD');" \
  "dolt_diff_t: ref not found: abc123" "$DBRS"
run_test_match "revspec_tvf_invalid_ancestor_rejected" \
  "SELECT count(*) FROM dolt_diff_t('HEAD~99','HEAD');" \
  "dolt_diff_t: invalid ref: HEAD~99" "$DBRS"
run_test_match "diff_stat_garbage_ref_descriptive" \
  "SELECT count(*) FROM dolt_diff_stat('nosuchref','HEAD','t');" \
  "dolt_diff_stat: ref not found: nosuchref" "$DBRS"
run_test_match "diff_stat_invalid_ancestor_descriptive" \
  "SELECT count(*) FROM dolt_diff_stat('HEAD~99','HEAD','t');" \
  "dolt_diff_stat: invalid ref: HEAD~99" "$DBRS"
run_test "revspec_to_working" \
  "INSERT INTO t VALUES(3,'dirty');
   SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" \
  "1" "$DBRS"
run_test "revspec_from_head_to_working" \
  "SELECT count(*) FROM dolt_diff_t WHERE from_commit='HEAD' AND to_commit='WORKING';" \
  "1" "$DBRS"
run_test "revspec_to_staged" \
  "SELECT dolt_add('t');
   SELECT count(*) FROM dolt_diff_t WHERE to_commit='STAGED';" \
  "0
1" "$DBRS"
run_test "revspec_unconstrained_unchanged" \
  "SELECT count(*) FROM dolt_diff_t;" \
  "5" "$DBRS"

rm -f "$DBRS"

# Deep multi-level trees: the diff iterator skips shared subtrees, so pin
# sparse edits, contiguous range deletes, and height-mismatched pairs.
DBDEEP=/tmp/test_dt_deep_$$.db; rm -f "$DBDEEP"
echo "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<2500)
INSERT INTO t SELECT x, printf('v%05d', x) FROM c;
SELECT dolt_commit('-Am','big');
UPDATE t SET v='edit' WHERE k IN (3, 1250, 2498);
SELECT dolt_commit('-Am','sparse');
DELETE FROM t WHERE k BETWEEN 800 AND 899;
SELECT dolt_commit('-Am','range-del');" | $DOLTLITE "$DBDEEP" > /dev/null 2>&1

run_test "deep_sparse_edit_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='sparse');" \
  "3" "$DBDEEP"
run_test "deep_sparse_edit_rows" \
  "SELECT group_concat(to_k, ',') FROM (SELECT to_k FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='sparse') ORDER BY to_k);" \
  "3,1250,2498" "$DBDEEP"
run_test "deep_range_delete_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='range-del') AND diff_type='removed';" \
  "100" "$DBDEEP"
run_test "deep_range_delete_edges" \
  "SELECT min(from_k) || '-' || max(from_k) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='range-del');" \
  "800-899" "$DBDEEP"
run_test "deep_slice_spans_heights" \
  "SELECT count(*) FROM dolt_diff_t('HEAD~2','HEAD');" \
  "103" "$DBDEEP"

rm -f "$DBDEEP"

DBMIX=/tmp/test_dt_mixheight_$$.db; rm -f "$DBMIX"
echo "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-Am','tiny');
WITH RECURSIVE c(x) AS (VALUES(4) UNION ALL SELECT x+1 FROM c WHERE x<3000)
INSERT INTO t SELECT x, printf('g%05d', x) FROM c;
SELECT dolt_commit('-Am','grown');" | $DOLTLITE "$DBMIX" > /dev/null 2>&1

run_test "mixheight_added_count" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='grown');" \
  "2997" "$DBMIX"
run_test "mixheight_originals_untouched" \
  "SELECT count(*) FROM dolt_diff_t WHERE to_commit=(SELECT commit_hash FROM dolt_log WHERE message='grown') AND to_k <= 3;" \
  "0" "$DBMIX"

rm -f "$DBMIX"

dltest_finish
