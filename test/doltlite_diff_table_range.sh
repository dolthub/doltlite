#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite diff revision range tests ==="
echo ""

DB=/tmp/test_dt_range_$$.db; rm -f "$DB"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feature');
INSERT INTO t VALUES(1,'main only');
SELECT dolt_commit('-A','-m','main change');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'feature only');
SELECT dolt_commit('-A','-m','feature change');" \
  | $DOLTLITE "$DB" > /dev/null 2>&1

MAIN=$(echo "SELECT hash FROM dolt_branches WHERE name='main';" | $DOLTLITE "$DB")
FEATURE=$(echo "SELECT hash FROM dolt_branches WHERE name='feature';" | $DOLTLITE "$DB")
BASE=$(echo "SELECT commit_hash FROM dolt_log WHERE message='base';" | $DOLTLITE "$DB")

run_test "two_ref_snapshot_diff" \
  "SELECT group_concat(id || ':' || diff_type, ',') FROM
     (SELECT coalesce(from_id,to_id) AS id, diff_type
        FROM dolt_diff_t('main','feature') ORDER BY id);" \
  "1:removed,2:added" "$DB"

run_test "two_dot_snapshot_diff" \
  "SELECT group_concat(id || ':' || diff_type, ',') FROM
     (SELECT coalesce(from_id,to_id) AS id, diff_type
        FROM dolt_diff_t('main..feature') ORDER BY id);" \
  "1:removed,2:added" "$DB"

run_test_match "two_dot_missing_left_rejected" \
  "SELECT count(*) FROM dolt_diff_t('..feature');" \
  "invalid revision range" "$DB"

run_test_match "two_dot_missing_right_rejected" \
  "SELECT count(*) FROM dolt_diff_t('feature..');" \
  "invalid revision range" "$DB"

run_test_match "three_dot_missing_left_rejected" \
  "SELECT count(*) FROM dolt_diff_t('...feature');" \
  "invalid revision range" "$DB"

run_test_match "three_dot_missing_right_rejected" \
  "SELECT count(*) FROM dolt_diff_t('feature...');" \
  "invalid revision range" "$DB"

run_test_match "two_dot_unknown_ref_rejected" \
  "SELECT count(*) FROM dolt_diff_t('nosuchref..feature');" \
  "invalid revision range" "$DB"

run_test_match "three_dot_invalid_ancestor_rejected" \
  "SELECT count(*) FROM dolt_diff_t('HEAD~99...feature');" \
  "invalid revision range" "$DB"

run_test "three_dot_feature" \
  "SELECT coalesce(from_id,to_id) || ':' || diff_type
     FROM dolt_diff_t('main...feature');" "2:added" "$DB"

run_test "three_dot_main" \
  "SELECT coalesce(from_id,to_id) || ':' || diff_type
     FROM dolt_diff_t('feature...main');" "1:added" "$DB"

run_test "three_dot_uses_merge_base" \
  "SELECT from_commit || '|' || to_commit
     FROM dolt_diff_t('main...feature');" "$BASE|$FEATURE" "$DB"

run_test "hash_snapshot_diff" \
  "SELECT count(*) FROM dolt_diff_t('$MAIN','$FEATURE');" "2" "$DB"

run_test "log_branch_ref" \
  "SELECT message FROM dolt_log('main') LIMIT 1;" "main change" "$DB"

run_test "log_two_dot" \
  "SELECT group_concat(message, ',') FROM
     (SELECT message FROM dolt_log('main..feature') ORDER BY message);" \
  "feature change" "$DB"

run_test "log_three_dot" \
  "SELECT group_concat(message, ',') FROM
     (SELECT message FROM dolt_log('main...feature') ORDER BY message);" \
  "feature change,main change" "$DB"

run_test_match "log_two_dot_missing_left_rejected" \
  "SELECT count(*) FROM dolt_log('..feature');" \
  "invalid dolt_log revision" "$DB"

run_test_match "log_two_dot_missing_right_rejected" \
  "SELECT count(*) FROM dolt_log('feature..');" \
  "invalid dolt_log revision" "$DB"

run_test_match "log_three_dot_missing_left_rejected" \
  "SELECT count(*) FROM dolt_log('...feature');" \
  "invalid dolt_log revision" "$DB"

run_test_match "log_three_dot_missing_right_rejected" \
  "SELECT count(*) FROM dolt_log('feature...');" \
  "invalid dolt_log revision" "$DB"

HDB=/tmp/test_dt_history_$$.db; rm -f "$HDB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
UPDATE t SET v='a2' WHERE id=1;
SELECT dolt_commit('-A','-m','c3');
DELETE FROM t WHERE id=2;
SELECT dolt_commit('-A','-m','c4');
UPDATE t SET v='a3' WHERE id=1;
SELECT dolt_commit('-A','-m','c5');" \
  | $DOLTLITE "$HDB" > /dev/null 2>&1

C2=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c2';" | $DOLTLITE "$HDB")
C5=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c5';" | $DOLTLITE "$HDB")

run_test "endpoint_collapses_repeated_changes" \
  "SELECT count(*) FROM dolt_diff_t('$C2','$C5');" "2" "$HDB"

run_test "history_range_preserves_attribution" \
  "SELECT count(*) FROM dolt_diff_t d
     JOIN dolt_log('$C2..$C5') l ON l.commit_hash=d.to_commit;" "3" "$HDB"

run_test "history_range_preserves_repeated_changes" \
  "SELECT count(*) FROM dolt_diff_t d
     JOIN dolt_log('$C2..$C5') l ON l.commit_hash=d.to_commit
    WHERE d.diff_type='modified' AND d.to_id=1;" "2" "$HDB"

run_test "history_range_preserves_delete" \
  "SELECT count(*) FROM dolt_diff_t d
     JOIN dolt_log('$C2..$C5') l ON l.commit_hash=d.to_commit
    WHERE d.diff_type='removed' AND d.from_id=2;" "1" "$HDB"

echo "UPDATE t SET v='a4' WHERE id=1;" | $DOLTLITE "$HDB" > /dev/null 2>&1
run_test "endpoint_to_working" \
  "SELECT to_v || ':' || diff_type FROM dolt_diff_t('$C5','WORKING');" \
  "a4:modified" "$HDB"

rm -f "$DB" "$HDB"
dltest_finish
