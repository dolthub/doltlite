#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Merge Tests ==="
echo ""

DB=/tmp/test_merge_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT); INSERT INTO users VALUES(1,'Alice'); INSERT INTO orders VALUES(1,'hat'); SELECT dolt_commit('-A','-m','initial');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE users SET name='ALICE' WHERE id=1; SELECT dolt_commit('-A','-m','main updates');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "INSERT INTO orders VALUES(2,'coat'); SELECT dolt_commit('-A','-m','feature adds');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "merge_hash" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB"
run_test "merge_users" "SELECT name FROM users;" "ALICE" "$DB"
run_test "merge_orders" "SELECT count(*) FROM orders;" "2" "$DB"
run_test "merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge branch 'feature' into main" "$DB"
run_test "merge_log_count" "SELECT count(*) FROM dolt_log;" "5" "$DB"

DB2=/tmp/test_merge2_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','feature');" | $DOLTLITE "$DB2/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "ff_before" "SELECT count(*) FROM t;" "1" "$DB2"
run_test_match "ff_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB2"
run_test "ff_after" "SELECT count(*) FROM t;" "2" "$DB2"
run_test "ff_no_merge_commit" "SELECT message FROM dolt_log LIMIT 1;" "feature" "$DB2"
run_test "ff_log_count" "SELECT count(*) FROM dolt_log;" "3" "$DB2"

run_test "up_to_date" "SELECT dolt_merge('feature');" "Already up to date" "$DB2"

DB3=/tmp/test_merge3_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='main'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "UPDATE t SET v='feat'; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB3/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB3" > /dev/null 2>&1
run_test_match "conflict" "SELECT dolt_merge('feature');" "conflict" "$DB3"
run_test "conflict_ours_preserved" "SELECT v FROM t;" "main" "$DB3"

run_test_match "no_branch" "SELECT dolt_merge('nope');" "not found" "$DB3"

DB4=/tmp/test_merge4_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "CREATE TABLE new_t(y); INSERT INTO new_t VALUES(42); SELECT dolt_commit('-A','-m','add table');" | $DOLTLITE "$DB4/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test_match "new_table_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB4"
run_test "new_table_visible" "SELECT y FROM new_t;" "42" "$DB4"
run_test "original_intact" "SELECT x FROM t;" "1" "$DB4"

DB5=/tmp/test_merge5_$$.db; rm -f "$DB5"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "DELETE FROM t WHERE id=2; SELECT dolt_commit('-A','-m','del');" | $DOLTLITE "$DB5/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB5" > /dev/null 2>&1
run_test "pre_merge_rows" "SELECT count(*) FROM t;" "2" "$DB5"
run_test_match "merge_del" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB5"
run_test "post_merge_rows" "SELECT count(*) FROM t;" "1" "$DB5"

run_test "diff_3way_users" \
  "SELECT rows_modified FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 3), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 'users');" \
  "1" "$DB"
run_test "diff_3way_orders" \
  "SELECT rows_added FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 3), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 'orders');" \
  "1" "$DB"

run_test "diff_ff_added" \
  "SELECT rows_added FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 't');" \
  "1" "$DB2"

run_test "diff_conflict_shows_change" \
  "SELECT rows_modified FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 0), 't');" \
  "1" "$DB3"

DB6=/tmp/test_merge6_$$.db; rm -f "$DB6"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "UPDATE t SET v='MAIN' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "UPDATE t SET v='FEAT' WHERE id=3; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB6/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test_match "row_merge_succeeds" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB6"
run_test "row_merge_row1" "SELECT v FROM t WHERE id=1;" "MAIN" "$DB6"
run_test "row_merge_row2" "SELECT v FROM t WHERE id=2;" "b" "$DB6"
run_test "row_merge_row3" "SELECT v FROM t WHERE id=3;" "FEAT" "$DB6"

DB7=/tmp/test_merge7_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'orig'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "UPDATE t SET v='main-val' WHERE id=1; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "UPDATE t SET v='feat-val' WHERE id=1; SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB7/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test_match "row_conflict_detected" "SELECT dolt_merge('feature');" "conflict" "$DB7"
run_test "row_conflict_ours_kept" "SELECT v FROM t WHERE id=1;" "main-val" "$DB7"

DB8=/tmp/test_merge8_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "UPDATE t SET v='main1' WHERE id=1; INSERT INTO t VALUES(3,'main3'); SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "SELECT dolt_checkout('feature');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "UPDATE t SET v='feat1' WHERE id=1; INSERT INTO t VALUES(4,'feat4'); SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB8/feature" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB8" > /dev/null 2>&1
run_test_match "mixed_merge" "SELECT dolt_merge('feature');" "conflict|rolled back" "$DB8"
run_test "mixed_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB8"
run_test "mixed_row1_main" "SELECT v FROM t WHERE id=1;" "main1" "$DB8"
run_test "mixed_row2_unchanged" "SELECT v FROM t WHERE id=2;" "b" "$DB8"
run_test "mixed_row3_from_main" "SELECT v FROM t WHERE id=3;" "main3" "$DB8"
run_test "mixed_row4_absent" "SELECT count(*) FROM t WHERE id=4;" "0" "$DB8"

DB8B=/tmp/test_merge8b_$$.db; rm -f "$DB8B"
TX_OUT=$({
cat <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v='feat1' WHERE id=1;
INSERT INTO t VALUES(4,'feat4');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main1' WHERE id=1;
INSERT INTO t VALUES(3,'main3');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feature');
SELECT 'TX|' || (SELECT count(*) FROM dolt_conflicts) || '|' ||
       (SELECT v FROM t WHERE id=1) || '|' ||
       (SELECT count(*) FROM t WHERE id=4);
SQL
} | $DOLTLITE "$DB8B" 2>&1 | grep '^TX|')
if [ "$TX_OUT" = "TX|0|main1|0" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: mixed_merge_same_session_summary_cleared\n  expected: TX|0|main1|0\n  got:      $TX_OUT"
fi

DB9=/tmp/test_merge9_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB9" > /dev/null 2>&1
echo "SELECT dolt_branch('other'); SELECT dolt_checkout('other'); UPDATE t SET v='OTHER'; SELECT dolt_commit('-A','-m','other');" | $DOLTLITE "$DB9" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET v='MAIN'; SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB9" > /dev/null 2>&1

echo "BEGIN; SELECT dolt_merge('other'); SELECT dolt_merge('--abort'); COMMIT;" | $DOLTLITE "$DB9" > /dev/null 2>&1
run_test "abort_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB9"
run_test "abort_data_restored" "SELECT v FROM t WHERE id=1;" "MAIN" "$DB9"

run_test_match "abort_no_merge" "SELECT dolt_merge('--abort');" "no merge in progress" "$DB9"

DB10=/tmp/test_merge10_$$.db; rm -f "$DB10"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','feat');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB10" > /dev/null 2>&1

run_test_match "clean_ff_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]" "$DB10"
run_test "clean_ff_log" "SELECT message FROM dolt_log LIMIT 1;" "feat" "$DB10"
run_test "clean_ff_data" "SELECT count(*) FROM t;" "2" "$DB10"

DB11=/tmp/test_merge11_$$.db; rm -f "$DB11"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT); INSERT INTO t VALUES(1,1,'base1'),(2,2,'base2'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET u=9, v='feat2' WHERE id=2; SELECT dolt_commit('-A','-m','feat_unique');" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET u=9, v='main1' WHERE id=1; SELECT dolt_commit('-A','-m','main_unique');" | $DOLTLITE "$DB11" > /dev/null 2>&1
run_test_match "constraint_violation_merge_errors" "SELECT dolt_merge('feat');" "constraint violations|rolled back" "$DB11"
run_test "constraint_violation_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB11"
run_test "constraint_violation_no_violations" "SELECT count(*) FROM dolt_constraint_violations;" "0" "$DB11"
run_test "constraint_violation_state_restored" "SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id, u, v FROM t ORDER BY id);" "1:9:main1,2:2:base2" "$DB11"

# Same-cell conflict plus unique CV: finish used to report only CVs and skip conflicts.
DB11B=/tmp/test_merge11b_$$.db; rm -f "$DB11B"
cat <<'EOF' | $DOLTLITE "$DB11B" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1,1,'base'),(2,2,'base');
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
UPDATE t SET u=9 WHERE id=2;
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main', u=9 WHERE id=1;
SELECT dolt_commit('-Am','main');
EOF
run_test_match "conflict_and_cv_autocommit_msg" \
  "SELECT dolt_merge('feat');" \
  "conflict.*constraint violations|constraint violations.*conflict|rolled back" \
  "$DB11B"
run_test "conflict_and_cv_autocommit_clean_conflicts" \
  "SELECT count(*) FROM dolt_conflicts;" "0" "$DB11B"
run_test "conflict_and_cv_autocommit_clean_cvs" \
  "SELECT count(*) FROM dolt_constraint_violations;" "0" "$DB11B"

DB11C=/tmp/test_merge11c_$$.db; rm -f "$DB11C"
cat <<'EOF' | $DOLTLITE "$DB11C" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1,1,'base'),(2,2,'base');
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
UPDATE t SET u=9 WHERE id=2;
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main', u=9 WHERE id=1;
SELECT dolt_commit('-Am','main');
EOF
TX_BOTH=$(echo "BEGIN;
SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_conflicts) || '|' ||
  (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
  (SELECT is_merging FROM dolt_merge_status);
ROLLBACK;" | $DOLTLITE "$DB11C" 2>&1)
echo "$TX_BOTH" | grep -E 'conflict\(s\) and constraint violations' >/dev/null
if [ $? -eq 0 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: conflict_and_cv_tx_message\n  expected message containing 'conflict(s) and constraint violations'\n  got:\n$TX_BOTH"
fi
TX_BOTH_COUNTS=$(echo "$TX_BOTH" | grep '^TX|')
if [ "$TX_BOTH_COUNTS" = "TX|1|1|1" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: conflict_and_cv_tx_persists_both\n  expected: TX|1|1|1\n  got:      $TX_BOTH_COUNTS"
fi

DB12=/tmp/test_merge12_$$.db; rm -f "$DB12"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT); INSERT INTO t VALUES(1,1,'base1'),(2,2,'base2'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB12" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); UPDATE t SET u=9, v='feat2' WHERE id=2; SELECT dolt_commit('-A','-m','feat_unique');" | $DOLTLITE "$DB12" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); UPDATE t SET u=9, v='main1' WHERE id=1; SELECT dolt_commit('-A','-m','main_unique');" | $DOLTLITE "$DB12" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id));
ROLLBACK;" | $DOLTLITE "$DB12" 2>&1 | grep '^TX|')
if [ "$TX_OUT" = "TX|0|1|1:9:main1,2:9:feat2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: constraint_violation_merge_tx_persists\n  expected: TX|0|1|1:9:main1,2:9:feat2\n  got:      $TX_OUT"
fi

DB11D=/tmp/test_merge11d_$$.db; rm -f "$DB11D"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT COLLATE NOCASE UNIQUE);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'a'); SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,'A'); SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB11D" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
       COALESCE((SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t'),0) || '|' ||
       COALESCE((SELECT group_concat(id, ',') FROM (SELECT id FROM dolt_constraint_violations_t ORDER BY id)),'');
ROLLBACK;" | $DOLTLITE "$DB11D" 2>&1 | grep '^TX|')
if [ "$TX_OUT" = "TX|1|2|1,2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: constraint_violation_nocase_unique\n  expected: TX|1|2|1,2\n  got:      $TX_OUT"
fi

DB11E=/tmp/test_merge11e_$$.db; rm -f "$DB11E"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, u BLOB UNIQUE);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,x'410042'); SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,x'4100'); SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB11E" > /dev/null 2>&1
run_test_match "constraint_violation_blob_prefix_merge" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB11E"
run_test "constraint_violation_blob_prefix_rows" \
  "SELECT group_concat(hex(u), ',') FROM (SELECT u FROM t ORDER BY id);" \
  "4100,410042" "$DB11E"

DB11F=/tmp/test_merge11f_$$.db; rm -f "$DB11F"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, u TEXT COLLATE NOCASE UNIQUE) WITHOUT ROWID;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
INSERT INTO t VALUES('two','a'); SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('one','A'); SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB11F" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
       COALESCE((SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t'),0) || '|' ||
       COALESCE((SELECT group_concat(pk, ',') FROM (SELECT pk FROM dolt_constraint_violations_t ORDER BY pk)),'');
ROLLBACK;" | $DOLTLITE "$DB11F" 2>&1 | grep '^TX|')
if [ "$TX_OUT" = "TX|1|2|one,two" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: constraint_violation_nocase_unique_without_rowid\n  expected: TX|1|2|one,two\n  got:      $TX_OUT"
fi

DB13=/tmp/test_merge13_$$.db; rm -f "$DB13"
echo "CREATE TABLE anchor(id INTEGER PRIMARY KEY); INSERT INTO anchor VALUES(1); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB13" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO feat_tbl VALUES(1,'f'); SELECT dolt_commit('-A','-m','feat_add_table');" | $DOLTLITE "$DB13" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); CREATE TABLE main_tbl(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO main_tbl VALUES(1,'m'); SELECT dolt_commit('-A','-m','main_add_table');" | $DOLTLITE "$DB13" > /dev/null 2>&1
run_test_match "disjoint_new_tables_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB13"
run_test "disjoint_new_tables_main_present" "SELECT v FROM main_tbl;" "m" "$DB13"
run_test "disjoint_new_tables_feat_present" "SELECT v FROM feat_tbl;" "f" "$DB13"
run_test "disjoint_new_tables_reopen_main" "SELECT v FROM main_tbl;" "m" "$DB13"
run_test "disjoint_new_tables_reopen_feat" "SELECT v FROM feat_tbl;" "f" "$DB13"

DB14=/tmp/test_merge14_$$.db; rm -f "$DB14"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v INT); CREATE TABLE b(id INTEGER PRIMARY KEY, v INT); INSERT INTO a VALUES(1,10); INSERT INTO b VALUES(1,20); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB14" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); CREATE INDEX idx_a_v ON a(v); SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_idx');" | $DOLTLITE "$DB14" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat'); CREATE INDEX idx_b_v ON b(v); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_idx');" | $DOLTLITE "$DB14" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB14" > /dev/null 2>&1
run_test_match "disjoint_indexes_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB14"
run_test "disjoint_indexes_visible" "SELECT count(*) FROM sqlite_master WHERE type='index' AND name IN ('idx_a_v','idx_b_v');" "2" "$DB14"
run_test "disjoint_indexes_reopen_data_a" "SELECT count(*) FROM a;" "1" "$DB14"
run_test "disjoint_indexes_reopen_data_b" "SELECT count(*) FROM b;" "1" "$DB14"

DB15=/tmp/test_merge15_$$.db; rm -f "$DB15"
echo "CREATE TABLE p1(id INTEGER PRIMARY KEY); CREATE TABLE c1(id INTEGER PRIMARY KEY, p1_id INT); CREATE TABLE p2(id INTEGER PRIMARY KEY); CREATE TABLE c2(id INTEGER PRIMARY KEY, p2_id INT); INSERT INTO p1 VALUES(1); INSERT INTO p2 VALUES(1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB15" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); ALTER TABLE c1 RENAME TO c1_old; CREATE TABLE c1(id INTEGER PRIMARY KEY, p1_id INT, CONSTRAINT fk_c1 FOREIGN KEY (p1_id) REFERENCES p1(id)); INSERT INTO c1 SELECT id,p1_id FROM c1_old; DROP TABLE c1_old; SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_fk');" | $DOLTLITE "$DB15" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat'); ALTER TABLE c2 RENAME TO c2_old; CREATE TABLE c2(id INTEGER PRIMARY KEY, p2_id INT, CONSTRAINT fk_c2 FOREIGN KEY (p2_id) REFERENCES p2(id)); INSERT INTO c2 SELECT id,p2_id FROM c2_old; DROP TABLE c2_old; SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_fk');" | $DOLTLITE "$DB15" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB15" > /dev/null 2>&1
run_test_match "disjoint_fks_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB15"
run_test "disjoint_fks_c1" "SELECT count(*) FROM pragma_foreign_key_list('c1');" "1" "$DB15"
run_test "disjoint_fks_c2" "SELECT count(*) FROM pragma_foreign_key_list('c2');" "1" "$DB15"

DB16=/tmp/test_merge16_$$.db; rm -f "$DB16"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB16" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); CREATE INDEX idx_t_v ON t(v); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_idx');" | $DOLTLITE "$DB16" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); CREATE TABLE main_only(id INTEGER PRIMARY KEY, v INT); INSERT INTO main_only VALUES(1,11); SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_add_table');" | $DOLTLITE "$DB16" > /dev/null 2>&1
run_test_match "table_plus_index_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB16"
run_test "table_plus_index_table_visible" "SELECT count(*) FROM main_only;" "1" "$DB16"
run_test "table_plus_index_index_visible" "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_t_v';" "1" "$DB16"

DB17=/tmp/test_merge17_$$.db; rm -f "$DB17"
echo "CREATE TABLE base(id INTEGER PRIMARY KEY, v INT); INSERT INTO base VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB17" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, v INT); INSERT INTO feat_tbl VALUES(1,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add_table');" | $DOLTLITE "$DB17" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); ALTER TABLE base RENAME TO base_old; CREATE TABLE base(id INTEGER PRIMARY KEY, v INT, CONSTRAINT chk_base CHECK (v > 0)); INSERT INTO base SELECT * FROM base_old; DROP TABLE base_old; SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_add_check');" | $DOLTLITE "$DB17" > /dev/null 2>&1
run_test_match "table_plus_check_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB17"
run_test "table_plus_check_table_visible" "SELECT count(*) FROM feat_tbl;" "1" "$DB17"
run_test "table_plus_check_constraint_visible" "SELECT instr(sql,'CHECK')>0 FROM sqlite_master WHERE type='table' AND name='base';" "1" "$DB17"

DB18=/tmp/test_merge18_$$.db; rm -f "$DB18"
echo "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO base VALUES(1,'x'); CREATE TABLE keep_main(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO keep_main VALUES(1,'m'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB18" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); ALTER TABLE keep_main RENAME TO renamed_main; SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_rename');" | $DOLTLITE "$DB18" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat'); CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO feat_tbl VALUES(1,'f'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add_table');" | $DOLTLITE "$DB18" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB18" > /dev/null 2>&1
run_test_match "table_plus_rename_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB18"
run_test "table_plus_rename_feat_visible" "SELECT count(*) FROM feat_tbl;" "1" "$DB18"
run_test "table_plus_rename_new_name_visible" "SELECT count(*) FROM renamed_main;" "1" "$DB18"

DB19=/tmp/test_merge19_$$.db; rm -f "$DB19"
echo "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO base VALUES(1,'x'); CREATE TABLE churn(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO churn VALUES(1,'m'); SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB19" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); DROP TABLE churn; CREATE TABLE churn(k INTEGER PRIMARY KEY, n INT); INSERT INTO churn VALUES(7,70); SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_recreate');" | $DOLTLITE "$DB19" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat'); CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO feat_tbl VALUES(1,'f'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add_table');" | $DOLTLITE "$DB19" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB19" > /dev/null 2>&1
run_test_match "table_plus_recreate_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB19"
run_test "table_plus_recreate_feat_visible" "SELECT count(*) FROM feat_tbl;" "1" "$DB19"
run_test "table_plus_recreate_schema_visible" "SELECT instr(sql,'k INTEGER PRIMARY KEY')>0 FROM sqlite_master WHERE type='table' AND name='churn';" "1" "$DB19"

DB20=/tmp/test_merge20_$$.db; rm -f "$DB20"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,10); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB20" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE); CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u)); INSERT INTO p VALUES(1,100); INSERT INTO c VALUES(1,100); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add_fk_tables');" | $DOLTLITE "$DB20" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK(v > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_check');" | $DOLTLITE "$DB20" > /dev/null 2>&1
run_test_match "fk_tables_plus_check_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB20"
run_test "fk_tables_plus_check_parent_visible" "SELECT count(*) FROM p;" "1" "$DB20"
run_test "fk_tables_plus_check_child_visible" "SELECT count(*) FROM c;" "1" "$DB20"
run_test "fk_tables_plus_check_fk_visible" "SELECT count(*) FROM pragma_foreign_key_list('c');" "1" "$DB20"

DB20B=/tmp/test_merge20b_$$.db; rm -f "$DB20B"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(1,10); CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE); CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u)); INSERT INTO p VALUES(1,100); INSERT INTO c VALUES(1,100); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB20B" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); DROP TABLE c; DROP TABLE p; CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE, label TEXT); CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u)); INSERT INTO p VALUES(2,200,'x'); INSERT INTO c VALUES(2,200); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_recreate_fk_family');" | $DOLTLITE "$DB20B" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK(v > 0)); INSERT INTO t_new SELECT * FROM t; DROP TABLE t; ALTER TABLE t_new RENAME TO t; SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_check');" | $DOLTLITE "$DB20B" > /dev/null 2>&1
run_test_match "recreate_fk_family_merge_hash" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB20B"
run_test "recreate_fk_family_parent_visible" "SELECT count(*) FROM p;" "1" "$DB20B"
run_test "recreate_fk_family_child_visible" "SELECT count(*) FROM c;" "1" "$DB20B"
run_test "recreate_fk_family_fk_visible" "SELECT count(*) FROM pragma_foreign_key_list('c');" "1" "$DB20B"
run_test "recreate_fk_family_parent_schema_visible" "SELECT instr(sql,'label TEXT')>0 FROM sqlite_master WHERE type='table' AND name='p';" "1" "$DB20B"
run_test "recreate_fk_family_parent_unique_index_live" "SELECT count(*) FROM p INDEXED BY sqlite_autoindex_p_1 WHERE u=200;" "1" "$DB20B"
run_test "recreate_fk_family_fk_check_clean" "SELECT count(*) FROM pragma_foreign_key_check;" "0" "$DB20B"

DB21=/tmp/test_merge21_$$.db; rm -f "$DB21"
echo "PRAGMA foreign_keys=ON; CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INT, FOREIGN KEY(parent_id) REFERENCES t(id) ON DELETE CASCADE); INSERT INTO t VALUES(1,NULL),(2,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB21" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO t VALUES(3,2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add_descendant');" | $DOLTLITE "$DB21" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); INSERT INTO t VALUES(10,NULL); SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_add_root');" | $DOLTLITE "$DB21" > /dev/null 2>&1
run_test_match "self_ref_fk_merge_hash" "PRAGMA foreign_keys=ON; SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB21"
run_test "self_ref_fk_delete_cascades_same_session" "PRAGMA foreign_keys=ON; DELETE FROM t WHERE id=1; SELECT group_concat(id || ':' || ifnull(parent_id,-1), ',') FROM (SELECT id,parent_id FROM t ORDER BY id);" "10:-1" "$DB21"
run_test "self_ref_fk_reopen_state" "PRAGMA foreign_keys=ON; SELECT group_concat(id || ':' || ifnull(parent_id,-1), ',') FROM (SELECT id,parent_id FROM t ORDER BY id);" "10:-1" "$DB21"
run_test "self_ref_fk_reopen_delete_last_root" "PRAGMA foreign_keys=ON; DELETE FROM t WHERE id=10; SELECT count(*) FROM t;" "0" "$DB21"

DB22=/tmp/test_merge22_$$.db; rm -f "$DB22"
echo "PRAGMA foreign_keys=ON; CREATE TABLE gp(id INTEGER PRIMARY KEY); CREATE TABLE p(id INTEGER PRIMARY KEY, gp_id INT, FOREIGN KEY(gp_id) REFERENCES gp(id) ON DELETE CASCADE); CREATE TABLE c(id INTEGER PRIMARY KEY, p_id INT, FOREIGN KEY(p_id) REFERENCES p(id) ON DELETE CASCADE); INSERT INTO gp VALUES(1); INSERT INTO p VALUES(1,1); INSERT INTO c VALUES(1,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB22" > /dev/null 2>&1
echo "SELECT dolt_branch('feat'); SELECT dolt_checkout('feat'); INSERT INTO c VALUES(2,1); SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add_child');" | $DOLTLITE "$DB22" > /dev/null 2>&1
echo "SELECT dolt_checkout('main'); INSERT INTO gp VALUES(2); SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_add_root');" | $DOLTLITE "$DB22" > /dev/null 2>&1
run_test_match "fk_chain_merge_hash" "PRAGMA foreign_keys=ON; SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB22"
run_test "fk_chain_delete_cascades_same_session" "PRAGMA foreign_keys=ON; DELETE FROM gp WHERE id=1; SELECT (SELECT count(*) FROM gp) || '|' || (SELECT count(*) FROM p) || '|' || (SELECT count(*) FROM c);" "1|0|0" "$DB22"
run_test "fk_chain_reopen_state" "PRAGMA foreign_keys=ON; SELECT (SELECT count(*) FROM gp) || '|' || (SELECT count(*) FROM p) || '|' || (SELECT count(*) FROM c);" "1|0|0" "$DB22"
run_test "fk_chain_reopen_delete_last_root" "PRAGMA foreign_keys=ON; DELETE FROM gp WHERE id=2; SELECT (SELECT count(*) FROM gp) || '|' || (SELECT count(*) FROM p) || '|' || (SELECT count(*) FROM c);" "0|0|0" "$DB22"

# 300 orphans in one detection pass; the count must include every one.
DB23=/tmp/test_merge23_$$.db; rm -f "$DB23"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat'); SELECT dolt_checkout('feat');
INSERT INTO child(id,pid) WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<300) SELECT i, 1 FROM c;
SELECT dolt_commit('-A','-m','add 300 children');
SELECT dolt_checkout('main'); DELETE FROM parent WHERE id=1; SELECT dolt_commit('-A','-m','drop parent');" | $DOLTLITE "$DB23" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
       (SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='child');
ROLLBACK;" | $DOLTLITE "$DB23" 2>&1 | grep '^TX|')
if [ "$TX_OUT" = "TX|1|300" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: many_fk_violations_all_recorded\n  expected: TX|1|300\n  got:      $TX_OUT"
fi
# Autocommit with the same CVs rolls the whole merge back.
echo "SELECT dolt_merge('feat');" | $DOLTLITE "$DB23" > /dev/null 2>&1
run_test "many_fk_violations_autocommit_rolled_back" \
  "SELECT count(*) FROM dolt_constraint_violations;" "0" "$DB23"
run_test "many_fk_violations_parent_restored" \
  "SELECT count(*) FROM parent;" "0" "$DB23"

# Clean merge seals the enclosing BEGIN; otherwise ROLLBACK split HEAD from data.
DB24=/tmp/test_merge24_$$.db; rm -f "$DB24"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,'c'); SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB24" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
ROLLBACK;
SELECT 'TX|' || (SELECT message FROM dolt_log LIMIT 1) || '|' || (SELECT count(*) FROM t) || '|' || (SELECT count(*) FROM dolt_status);" | $DOLTLITE "$DB24" 2>&1)
TX_LINE=$(echo "$TX_OUT" | grep '^TX|')
if [ "$TX_LINE" = "TX|Merge branch 'feat' into main|3|0" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: clean_merge_in_txn_seals\n  expected: TX|Merge branch 'feat' into main|3|0\n  got:      $TX_LINE"
fi
echo "$TX_OUT" | grep -q "cannot rollback - no transaction is active"
if [ $? -eq 0 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: clean_merge_in_txn_rollback_refused\n  expected the post-merge ROLLBACK to find no open transaction"
fi

DB25=/tmp/test_merge25_$$.db; rm -f "$DB25" "$DB26" "$DB27" "$DB28" "$DB29" "$DB30" "$DB31" "$DB32" "$DB33" "$DB34" "$DB35" "$DB36" "$DB37" "$DB38" "$DB39"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feat'); INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB25" > /dev/null 2>&1
TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
ROLLBACK;
SELECT 'TX|' || (SELECT message FROM dolt_log LIMIT 1) || '|' || (SELECT count(*) FROM t) || '|' || (SELECT count(*) FROM dolt_status);" | $DOLTLITE "$DB25" 2>&1)
TX_LINE=$(echo "$TX_OUT" | grep '^TX|')
if [ "$TX_LINE" = "TX|feat|2|0" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: ff_merge_in_txn_seals\n  expected: TX|feat|2|0\n  got:      $TX_LINE"
fi
echo "$TX_OUT" | grep -q "cannot rollback - no transaction is active"
if [ $? -eq 0 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: ff_merge_in_txn_rollback_refused\n  expected the post-merge ROLLBACK to find no open transaction"
fi


DB26=/tmp/test_merge26_$$.db; rm -f "$DB26"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('AAAAA',1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB26" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB26" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(5,50); SELECT dolt_commit('-A','-m','feat recreate');" | $DOLTLITE "$DB26/feature" > /dev/null 2>&1
echo "INSERT INTO t VALUES('BBBBB',2); SELECT dolt_commit('-A','-m','main rows');" | $DOLTLITE "$DB26" > /dev/null 2>&1
run_test_match "pk_change_refused" "SELECT dolt_merge('feature');" "different primary keys" "$DB26"
run_test "pk_change_local_intact" "SELECT count(*) FROM t;" "2" "$DB26"

DB27=/tmp/test_merge27_$$.db; rm -f "$DB27"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('AAAAA',1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB27" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB27" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT); INSERT INTO t VALUES(5,50); SELECT dolt_commit('-A','-m','feat recreate');" | $DOLTLITE "$DB27/feature" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT); CREATE INDEX tv ON t(v); INSERT INTO t VALUES(7,70); SELECT dolt_commit('-A','-m','main recreate');" | $DOLTLITE "$DB27" > /dev/null 2>&1
run_test_match "pk_change_ancestor_refused" "SELECT dolt_merge('feature');" "different primary keys in its common ancestor" "$DB27"

DB28=/tmp/test_merge28_$$.db; rm -f "$DB28"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('AAAAA',1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB28" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB28" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(pk TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('feat',50); SELECT dolt_commit('-A','-m','feat recreate');" | $DOLTLITE "$DB28/feature" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(pk TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('main',7); SELECT dolt_commit('-A','-m','main recreate');" | $DOLTLITE "$DB28" > /dev/null 2>&1
run_test_match "identical_recreate_merge_ok" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB28"
run_test "identical_recreate_rows" "SELECT group_concat(pk || ':' || v, ',') FROM (SELECT pk,v FROM t ORDER BY pk);" "feat:50,main:7" "$DB28"
run_test "identical_recreate_integrity" "PRAGMA integrity_check;" "ok" "$DB28"


# PK collation/sort-order change makes keyspaces incomparable; used to commit out of order.
DB29=/tmp/test_merge29_$$.db; rm -f "$DB29"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('a',1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB29" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB29" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(pk TEXT COLLATE NOCASE PRIMARY KEY, v INT); INSERT INTO t VALUES('a',1); SELECT dolt_commit('-A','-m','feat collate');" | $DOLTLITE "$DB29/feature" > /dev/null 2>&1
echo "INSERT INTO t VALUES('A',3); SELECT dolt_commit('-A','-m','main row');" | $DOLTLITE "$DB29" > /dev/null 2>&1
run_test_match "pk_collation_change_refused" "SELECT dolt_merge('feature');" "different primary keys" "$DB29"
run_test "pk_collation_change_integrity" "PRAGMA integrity_check;" "ok" "$DB29"

DB30=/tmp/test_merge30_$$.db; rm -f "$DB30"
echo "CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID; INSERT INTO t VALUES(1,1,'x'),(2,1,'y'); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB30" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB30" > /dev/null 2>&1
echo "DROP TABLE t; CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a DESC, b)) WITHOUT ROWID; INSERT INTO t VALUES(1,1,'x'),(2,1,'y'),(9,1,'f'); SELECT dolt_commit('-A','-m','feat desc');" | $DOLTLITE "$DB30/feature" > /dev/null 2>&1
echo "INSERT INTO t VALUES(5,1,'m'); SELECT dolt_commit('-A','-m','main row');" | $DOLTLITE "$DB30" > /dev/null 2>&1
run_test_match "pk_sort_order_change_refused" "SELECT dolt_merge('feature');" "different primary keys" "$DB30"
run_test "pk_sort_order_change_integrity" "PRAGMA integrity_check;" "ok" "$DB30"

# Matching collations must still merge (signature names collation).
DB31=/tmp/test_merge31_$$.db; rm -f "$DB31"
echo "CREATE TABLE t(pk TEXT COLLATE NOCASE PRIMARY KEY, v INT); INSERT INTO t VALUES('a',1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB31" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB31" > /dev/null 2>&1
echo "INSERT INTO t VALUES('b',2); SELECT dolt_commit('-A','-m','feat row');" | $DOLTLITE "$DB31/feature" > /dev/null 2>&1
echo "INSERT INTO t VALUES('c',3); SELECT dolt_commit('-A','-m','main row');" | $DOLTLITE "$DB31" > /dev/null 2>&1
run_test_match "pk_same_collation_merges" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB31"
run_test "pk_same_collation_rows" "SELECT count(*) FROM t;" "3" "$DB31"

# DESC PK on both sides is unchanged, so it merges.
DB32=/tmp/test_merge32_$$.db; rm -f "$DB32"
echo "CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a DESC, b)) WITHOUT ROWID; INSERT INTO t VALUES(1,1,'x'); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB32" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB32" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,1,'y'); SELECT dolt_commit('-A','-m','feat row');" | $DOLTLITE "$DB32/feature" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,1,'z'); SELECT dolt_commit('-A','-m','main row');" | $DOLTLITE "$DB32" > /dev/null 2>&1
run_test_match "pk_same_desc_merges" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB32"
run_test "pk_same_desc_integrity" "PRAGMA integrity_check;" "ok" "$DB32"
# PK-only rows live in the sort key; a value-only lookup skipped them (CVs missed).
DB33=/tmp/test_merge33_$$.db; rm -f "$DB33"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(pid INT REFERENCES parent(id), tag TEXT, PRIMARY KEY(pid,tag)); INSERT INTO parent VALUES(1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB33" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB33" > /dev/null 2>&1
echo "INSERT INTO child VALUES(1,'t1'); SELECT dolt_commit('-A','-m','child row');" | $DOLTLITE "$DB33/feature" > /dev/null 2>&1
echo "DELETE FROM parent WHERE id=1; SELECT dolt_commit('-A','-m','drop parent');" | $DOLTLITE "$DB33" > /dev/null 2>&1
run_test_match "pk_only_fk_violation_detected" "SELECT dolt_merge('feature');" "constraint violations" "$DB33"
TX33=$(echo "BEGIN;
SELECT dolt_merge('feature');
SELECT 'CV|' || (SELECT count(*) FROM dolt_constraint_violations);
ROLLBACK;" | $DOLTLITE "$DB33" 2>&1 | grep '^CV|')
if [ "$TX33" = "CV|1" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: pk_only_fk_violation_recorded\n  expected: CV|1\n  got:      $TX33"
fi

DB34=/tmp/test_merge34_$$.db; rm -f "$DB34"
echo "CREATE TABLE t(a INT, b INT, PRIMARY KEY(a,b), UNIQUE(b)); INSERT INTO t VALUES(1,1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB34" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB34" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,5); SELECT dolt_commit('-A','-m','feat dup');" | $DOLTLITE "$DB34/feature" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,5); SELECT dolt_commit('-A','-m','main dup');" | $DOLTLITE "$DB34" > /dev/null 2>&1
run_test_match "pk_only_unique_violation_detected" "SELECT dolt_merge('feature');" "constraint violations" "$DB34"

# A non-key column still stores a value record; ordinary path must keep working.
DB35=/tmp/test_merge35_$$.db; rm -f "$DB35"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(pid INT REFERENCES parent(id), tag TEXT, note TEXT, PRIMARY KEY(pid,tag)); INSERT INTO parent VALUES(1); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB35" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB35" > /dev/null 2>&1
echo "INSERT INTO child VALUES(1,'t1','n'); SELECT dolt_commit('-A','-m','child row');" | $DOLTLITE "$DB35/feature" > /dev/null 2>&1
echo "DELETE FROM parent WHERE id=1; SELECT dolt_commit('-A','-m','drop parent');" | $DOLTLITE "$DB35" > /dev/null 2>&1
run_test_match "valued_row_fk_violation_detected" "SELECT dolt_merge('feature');" "constraint violations" "$DB35"

# Clean merge on a PK-only table must stay clean.
DB36=/tmp/test_merge36_$$.db; rm -f "$DB36"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(pid INT REFERENCES parent(id), tag TEXT, PRIMARY KEY(pid,tag)); INSERT INTO parent VALUES(1),(2); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB36" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB36" > /dev/null 2>&1
echo "INSERT INTO child VALUES(1,'t1'); SELECT dolt_commit('-A','-m','child one');" | $DOLTLITE "$DB36/feature" > /dev/null 2>&1
echo "INSERT INTO child VALUES(2,'t2'); SELECT dolt_commit('-A','-m','child two');" | $DOLTLITE "$DB36" > /dev/null 2>&1
run_test_match "pk_only_clean_merge_hash" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB36"
run_test "pk_only_clean_merge_rows" "SELECT count(*) FROM child;" "2" "$DB36"


# Dual add-column: rewrite must fill our column with its declared default, not NULL.
DB37=/tmp/test_merge37_$$.db; rm -f "$DB37"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB37" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB37" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN m INT DEFAULT 5; INSERT INTO t VALUES(2,'from-feat',7); INSERT INTO t(id,v,m) VALUES(3,'null-feat',NULL); SELECT dolt_commit('-A','-m','feat col');" | $DOLTLITE "$DB37/feature" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN n INT DEFAULT 9; SELECT dolt_commit('-A','-m','main col');" | $DOLTLITE "$DB37" > /dev/null 2>&1
run_test_match "dual_add_column_merge_hash" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB37"
run_test "dual_add_column_defaults" "SELECT group_concat(id || ':' || quote(n) || ':' || quote(m), ',') FROM (SELECT id,n,m FROM t ORDER BY id);" "1:9:5,2:9:7,3:9:NULL" "$DB37"

# NOT NULL default must not become a CV invented by the rewrite.
DB38=/tmp/test_merge38_$$.db; rm -f "$DB38"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB38" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB38" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN m TEXT DEFAULT 'em'; INSERT INTO t VALUES(2,'from-feat','x'); SELECT dolt_commit('-A','-m','feat col');" | $DOLTLITE "$DB38/feature" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN n TEXT NOT NULL DEFAULT 'en'; SELECT dolt_commit('-A','-m','main col');" | $DOLTLITE "$DB38" > /dev/null 2>&1
run_test_match "dual_add_text_default_merge_hash" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB38"
run_test "dual_add_text_defaults" "SELECT group_concat(id || ':' || n || ':' || m, ',') FROM (SELECT id,n,m FROM t ORDER BY id);" "1:en:em,2:en:x" "$DB38"
run_test "dual_add_not_null_no_violation" "SELECT count(*) FROM dolt_constraint_violations;" "0" "$DB38"

# No default still reads NULL; pre-existing rows stay NULL.
DB39=/tmp/test_merge39_$$.db; rm -f "$DB39"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'base'); SELECT dolt_commit('-A','-m','base');" | $DOLTLITE "$DB39" > /dev/null 2>&1
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB39" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN m INT; INSERT INTO t VALUES(2,'from-feat',7); SELECT dolt_commit('-A','-m','feat col');" | $DOLTLITE "$DB39/feature" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN n INT; SELECT dolt_commit('-A','-m','main col');" | $DOLTLITE "$DB39" > /dev/null 2>&1
run_test_match "dual_add_no_default_merge_hash" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB39"
run_test "dual_add_no_default_nulls" "SELECT group_concat(id || ':' || coalesce(n,'-') || ':' || coalesce(m,'-'), ',') FROM (SELECT id,n,m FROM t ORDER BY id);" "1:-:-,2:-:7" "$DB39"

DB40=/tmp/test_merge40_$$.db; rm -f "$DB40"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feature');" | $DOLTLITE "$DB40" > /dev/null 2>&1
echo "INSERT INTO t VALUES(1,'not-json'); SELECT dolt_commit('-A','-m','invalid json');" | $DOLTLITE "$DB40/feature" > /dev/null 2>&1
echo "CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT CHECK(json_extract(v,'$.ok'))); DROP TABLE t; ALTER TABLE t2 RENAME TO t; SELECT dolt_commit('-A','-m','add json check');" | $DOLTLITE "$DB40" > /dev/null 2>&1
run_test_match "constraint_detector_sql_error_merge_errors" "SELECT dolt_merge('feature');" "malformed JSON" "$DB40"
run_test "constraint_detector_sql_error_restores_rows" "SELECT count(*) FROM t;" "0" "$DB40"
run_test "constraint_detector_sql_error_preserves_head" "SELECT message FROM dolt_log LIMIT 1;" "add json check" "$DB40"

DB41=/tmp/test_merge41_$$.db; rm -f "$DB41"
run_test_match "same_added_table_with_peer_drop_merge" "
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO p VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feature');
CREATE TABLE q(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','main');
SELECT dolt_checkout('feature');
CREATE TABLE q(id INTEGER PRIMARY KEY, v TEXT);
DROP TABLE p;
SELECT dolt_commit('-A','-m','feature');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT 'RESULT|' || group_concat(name, ',') FROM (
  SELECT name FROM sqlite_master
  WHERE type='table' AND name NOT LIKE 'dolt_%' ORDER BY name
);" "^RESULT\|q$" "$DB41"
run_test "same_added_table_with_peer_drop_schema" "SELECT group_concat(name, ',') FROM (SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'dolt_%' ORDER BY name);" "q" "$DB41"
run_test "same_added_table_with_peer_drop_integrity" "PRAGMA integrity_check;" "ok" "$DB41"

DB42=/tmp/test_merge42_$$.db; rm -f "$DB42"
run_test_match "same_added_table_with_our_drop_merge" "
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO p VALUES(1,'a');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feature');
CREATE TABLE q(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO q VALUES(1,'main');
DROP TABLE p;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_checkout('feature');
CREATE TABLE q(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO q VALUES(2,'feature');
SELECT dolt_commit('-A','-m','feature');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT 'RESULT|' || group_concat(id || ':' || v, ',') FROM (
  SELECT id,v FROM q ORDER BY id
);" "^RESULT\|1:main,2:feature$" "$DB42"
run_test "same_added_table_with_our_drop_schema" "SELECT group_concat(name, ',') FROM (SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'dolt_%' ORDER BY name);" "q" "$DB42"
run_test "same_added_table_with_our_drop_integrity" "PRAGMA integrity_check;" "ok" "$DB42"

DB43=/tmp/test_merge43_$$.db; rm -f "$DB43"
run_test_match "same_empty_added_table_with_peer_drop_merge" "
CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feature');
CREATE TABLE q(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-A','-m','main');
SELECT dolt_checkout('feature');
CREATE TABLE q(id INTEGER PRIMARY KEY, v TEXT);
DROP TABLE p;
SELECT dolt_commit('-A','-m','feature');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT 'RESULT|' || group_concat(name, ',') FROM (
  SELECT name FROM sqlite_master
  WHERE type='table' AND name NOT LIKE 'dolt_%' ORDER BY name
);" "^RESULT\|q$" "$DB43"
run_test "same_empty_added_table_with_peer_drop_integrity" "PRAGMA integrity_check;" "ok" "$DB43"

DB44=/tmp/test_merge44_$$.db; rm -f "$DB44"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two'),(3,'three'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feature');" | $DOLTLITE "$DB44" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN added TEXT DEFAULT 'dd'; SELECT dolt_commit('-A','-m','add col');" | $DOLTLITE "$DB44/feature" > /dev/null 2>&1
echo "DELETE FROM t WHERE id=2; SELECT dolt_commit('-A','-m','delete row');" | $DOLTLITE "$DB44" > /dev/null 2>&1
run_test_match "delete_vs_add_column_default_merges" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB44"
run_test "delete_vs_add_column_default_rows" "SELECT group_concat(id || ':' || added, ',') FROM (SELECT id,added FROM t ORDER BY id);" "1:dd,3:dd" "$DB44"

DB45=/tmp/test_merge45_$$.db; rm -f "$DB45"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX tv ON t(v); INSERT INTO t VALUES(1,'one'),(2,'two'),(3,'three'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feature');" | $DOLTLITE "$DB45" > /dev/null 2>&1
echo "DELETE FROM t WHERE id=2; SELECT dolt_commit('-A','-m','delete row');" | $DOLTLITE "$DB45/feature" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN added TEXT NOT NULL DEFAULT 7; SELECT dolt_commit('-A','-m','add col');" | $DOLTLITE "$DB45" > /dev/null 2>&1
run_test_match "add_column_default_vs_delete_merges" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB45"
run_test "add_column_default_vs_delete_rows" "SELECT group_concat(id || ':' || quote(added), ',') FROM (SELECT id,added FROM t INDEXED BY tv ORDER BY v);" "1:'7',3:'7'" "$DB45"
run_test "add_column_default_vs_delete_integrity" "PRAGMA integrity_check;" "ok" "$DB45"

DB46=/tmp/test_merge46_$$.db; rm -f "$DB46"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two'); SELECT dolt_commit('-A','-m','base'); SELECT dolt_branch('feature');" | $DOLTLITE "$DB46" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN added TEXT DEFAULT 'dd'; UPDATE t SET v='edited' WHERE id=2; SELECT dolt_commit('-A','-m','add col and edit');" | $DOLTLITE "$DB46/feature" > /dev/null 2>&1
echo "DELETE FROM t WHERE id=2; SELECT dolt_commit('-A','-m','delete row');" | $DOLTLITE "$DB46" > /dev/null 2>&1
run_test_match "delete_vs_real_edit_with_add_column_conflicts" "SELECT dolt_merge('feature');" "conflict" "$DB46"

DB47=/tmp/test_merge47_$$.db; rm -f "$DB47"
cat <<'EOF' | $DOLTLITE "$DB47" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test_match "ff_nocommit_still_ff" "SELECT dolt_merge('--no-commit','f');" "^[0-9a-f]{40}$" "$DB47"
run_test "ff_nocommit_count" "SELECT count(*) FROM t;" "2" "$DB47"
run_test "ff_nocommit_log" "SELECT message FROM dolt_log LIMIT 1;" "f" "$DB47"
run_test "ff_nocommit_status" "SELECT count(*) FROM dolt_status;" "0" "$DB47"
run_test "ff_nocommit_merging" "SELECT is_merging FROM dolt_merge_status;" "0" "$DB47"

DB48=/tmp/test_merge48_$$.db; rm -f "$DB48"
cat <<'EOF' | $DOLTLITE "$DB48" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test_match "ff_squash_hash" "SELECT dolt_merge('--squash','f');" "^[0-9a-f]{40}$" "$DB48"
run_test "ff_squash_count" "SELECT count(*) FROM t;" "2" "$DB48"
run_test "ff_squash_log" "SELECT message FROM dolt_log LIMIT 1;" "b" "$DB48"
run_test "ff_squash_status" \
  "SELECT table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "t|1|modified" "$DB48"
run_test "ff_squash_merging" "SELECT is_merging FROM dolt_merge_status;" "0" "$DB48"
run_test_match "ff_squash_abort_none" "SELECT dolt_merge('--abort');" "no merge in progress" "$DB48"

DB49=/tmp/test_merge49_$$.db; rm -f "$DB49"
cat <<'EOF' | $DOLTLITE "$DB49" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
INSERT INTO t VALUES(3,'m');
SELECT dolt_commit('-Am','m');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test "threeway_nocommit_ret" "SELECT dolt_merge('--no-commit','f');" "0" "$DB49"
run_test "threeway_nocommit_count" "SELECT count(*) FROM t;" "3" "$DB49"
run_test "threeway_nocommit_log" "SELECT message FROM dolt_log LIMIT 1;" "m" "$DB49"
run_test "threeway_nocommit_status" \
  "SELECT table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "t|1|modified" "$DB49"
run_test "threeway_nocommit_merging" "SELECT is_merging FROM dolt_merge_status;" "1" "$DB49"
run_test_match "threeway_nocommit_blocks_second" \
  "SELECT dolt_merge('f');" "uncommitted" "$DB49"
run_test_match "threeway_nocommit_finish" \
  "SELECT dolt_commit('-m','finished merge');" "^[0-9a-f]{40}$" "$DB49"
run_test "threeway_nocommit_after_log" \
  "SELECT message FROM dolt_log LIMIT 1;" "finished merge" "$DB49"
run_test "threeway_nocommit_feat_ancestor" \
  "SELECT count(*) FROM dolt_log WHERE message='f';" "1" "$DB49"
run_test "threeway_nocommit_after_merging" \
  "SELECT is_merging FROM dolt_merge_status;" "0" "$DB49"
run_test "threeway_nocommit_after_status" "SELECT count(*) FROM dolt_status;" "0" "$DB49"

DB50=/tmp/test_merge50_$$.db; rm -f "$DB50"
cat <<'EOF' | $DOLTLITE "$DB50" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
INSERT INTO t VALUES(3,'m');
SELECT dolt_commit('-Am','m');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test_match "threeway_squash_hash" \
  "SELECT dolt_merge('--squash','f');" "^[0-9a-f]{40}$" "$DB50"
run_test "threeway_squash_count" "SELECT count(*) FROM t;" "3" "$DB50"
run_test "threeway_squash_log" \
  "SELECT message FROM dolt_log LIMIT 1;" "Merge branch 'f' into main" "$DB50"
run_test "threeway_squash_feat_absent" \
  "SELECT count(*) FROM dolt_log WHERE message='f';" "0" "$DB50"
run_test "threeway_squash_status" "SELECT count(*) FROM dolt_status;" "0" "$DB50"
run_test "threeway_squash_merging" "SELECT is_merging FROM dolt_merge_status;" "0" "$DB50"

DB51=/tmp/test_merge51_$$.db; rm -f "$DB51"
cat <<'EOF' | $DOLTLITE "$DB51" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
INSERT INTO t VALUES(3,'m');
SELECT dolt_commit('-Am','m');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test "threeway_squash_nocommit_ret" \
  "SELECT dolt_merge('--squash','--no-commit','f');" "0" "$DB51"
run_test "threeway_squash_nocommit_count" "SELECT count(*) FROM t;" "3" "$DB51"
run_test "threeway_squash_nocommit_log" \
  "SELECT message FROM dolt_log LIMIT 1;" "m" "$DB51"
run_test "threeway_squash_nocommit_status" \
  "SELECT table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "t|1|modified" "$DB51"
run_test "threeway_squash_nocommit_merging" \
  "SELECT is_merging FROM dolt_merge_status;" "0" "$DB51"
run_test_match "threeway_squash_nocommit_commit" \
  "SELECT dolt_commit('-m','squashed');" "^[0-9a-f]{40}$" "$DB51"
run_test "threeway_squash_nocommit_after_log" \
  "SELECT message FROM dolt_log LIMIT 1;" "squashed" "$DB51"
run_test "threeway_squash_nocommit_feat_absent" \
  "SELECT count(*) FROM dolt_log WHERE message='f';" "0" "$DB51"

DB52=/tmp/test_merge52_$$.db; rm -f "$DB52"
cat <<'EOF' | $DOLTLITE "$DB52" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test_match "squash_noff_rejected" \
  "SELECT dolt_merge('--squash','--no-ff','f');" \
  "cannot be used together" "$DB52"
run_test "squash_noff_unchanged" "SELECT count(*) FROM t;" "1" "$DB52"

DB53=/tmp/test_merge53_$$.db; rm -f "$DB53"
cat <<'EOF' | $DOLTLITE "$DB53" > /dev/null 2>&1
CREATE TABLE t(i INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','b');
SELECT dolt_branch('f');
SELECT dolt_checkout('f');
INSERT INTO t VALUES(2,'f');
SELECT dolt_commit('-Am','f');
SELECT dolt_checkout('main');
EOF
run_test "ff_nocommit_noff_ret" \
  "SELECT dolt_merge('--no-commit','--no-ff','f');" "0" "$DB53"
run_test "ff_nocommit_noff_count" "SELECT count(*) FROM t;" "2" "$DB53"
run_test "ff_nocommit_noff_log" "SELECT message FROM dolt_log LIMIT 1;" "b" "$DB53"
run_test "ff_nocommit_noff_status" \
  "SELECT table_name || '|' || staged || '|' || status FROM dolt_status;" \
  "t|1|modified" "$DB53"
run_test "ff_nocommit_noff_merging" \
  "SELECT is_merging FROM dolt_merge_status;" "1" "$DB53"
run_test "ff_nocommit_noff_abort" "SELECT dolt_merge('--abort');" "0" "$DB53"
run_test "ff_nocommit_noff_abort_count" "SELECT count(*) FROM t;" "1" "$DB53"
run_test "ff_nocommit_noff_abort_merging" \
  "SELECT is_merging FROM dolt_merge_status;" "0" "$DB53"

# Persisted --no-commit merge: a refused second merge must not wedge the
# next dolt_commit in that session (#2539).
DB54=/tmp/test_merge54_$$.db; rm -f "$DB54"
echo "CREATE TABLE t(a INT PRIMARY KEY, b INT);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('.'); SELECT dolt_commit('-m','base'); SELECT dolt_branch('feat');" | $DOLTLITE "$DB54" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,2); SELECT dolt_commit('-am','feat1');" | $DOLTLITE "$DB54/feat" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,3); SELECT dolt_commit('-am','main1');" | $DOLTLITE "$DB54" > /dev/null 2>&1
echo "SELECT dolt_merge('--no-commit','feat');" | $DOLTLITE "$DB54" > /dev/null 2>&1
REFUSE_OUT=$(cat <<'EOF' | $DOLTLITE "$DB54" 2>&1
SELECT dolt_merge('feat');
SELECT dolt_commit('-m','after-fail');
EOF
)
if echo "$REFUSE_OUT" | grep -q "uncommitted changes"; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocommit_second_merge_refused\n  got: $REFUSE_OUT"
fi
if echo "$REFUSE_OUT" | grep -q "cannot commit - no transaction is active"; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocommit_refuse_does_not_wedge_commit\n  got: $REFUSE_OUT"
else
  PASS=$((PASS+1))
fi
if echo "$REFUSE_OUT" | grep -Eq '[0-9a-f]{40}'; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocommit_refuse_then_commit_hash\n  got: $REFUSE_OUT"
fi

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB8B" "$DB9" "$DB10" "$DB11" "$DB11D" "$DB11E" "$DB11F" "$DB12" "$DB13" "$DB14" "$DB15" "$DB16" "$DB17" "$DB18" "$DB19" "$DB20" "$DB20B" "$DB21" "$DB22" "$DB23" "$DB24" "$DB25" "$DB40" "$DB41" "$DB42" "$DB43" "$DB44" "$DB45" "$DB46" "$DB47" "$DB48" "$DB49" "$DB50" "$DB51" "$DB52" "$DB53" "$DB54"
dltest_finish
