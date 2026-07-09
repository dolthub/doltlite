#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

# dolt_diff_<table>(from_ref, to_ref) must attribute each changed row to the
# commit inside the range that actually made the change -- the same per-commit
# audit the unfiltered dolt_diff_<table> gives -- rather than collapsing the
# range into a single net two-dot diff labelled with the range endpoints.
# (The net two-dot diff lives in the dolt_diff() table function.)

echo "=== Doltlite dolt_diff_<table> ranged attribution tests ==="
echo ""

DB=/tmp/test_dt_range_$$.db; rm -f "$DB"

# c1 adds id=1; c2 adds id=2; c3 modifies id=1; c4 deletes id=2;
# c5 modifies id=1 again (a repeated change to the same row).
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
SELECT dolt_commit('-A','-m','c5');" | $DOLTLITE "$DB" > /dev/null 2>&1

C1=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c1';" | $DOLTLITE "$DB")
C2=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c2';" | $DOLTLITE "$DB")
C3=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c3';" | $DOLTLITE "$DB")
C4=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c4';" | $DOLTLITE "$DB")
C5=$(echo "SELECT commit_hash FROM dolt_log WHERE message='c5';" | $DOLTLITE "$DB")

# Range c2..c5 = commits c3 (modify id1), c4 (delete id2), c5 (modify id1).
# c2's own change (adding id=2) is the lower boundary and excluded.
run_test "range_count" \
  "SELECT count(*) FROM dolt_diff_t('$C2','$C5');" "3" "$DB"

# The core of the feature: three distinct attributing commits, not one.
run_test "range_distinct_commits" \
  "SELECT count(DISTINCT to_commit) FROM dolt_diff_t('$C2','$C5');" "3" "$DB"

# Each change is attributed to the commit that made it, not the endpoint.
run_test "range_attribution_c3" \
  "SELECT to_v FROM dolt_diff_t('$C2','$C5') WHERE to_commit='$C3';" "a2" "$DB"
run_test "range_attribution_c5" \
  "SELECT to_v FROM dolt_diff_t('$C2','$C5') WHERE to_commit='$C5';" "a3" "$DB"

# Deletions inside the range are shown, attributed to the deleting commit.
run_test "range_delete_shown" \
  "SELECT diff_type || ':' || from_id FROM dolt_diff_t('$C2','$C5') WHERE to_commit='$C4';" \
  "removed:2" "$DB"

# Repeated changes to one row appear once per commit that touched it.
run_test "range_repeated_change" \
  "SELECT count(*) FROM dolt_diff_t('$C2','$C5') WHERE diff_type='modified' AND to_id=1;" \
  "2" "$DB"

# Lower bound excludes from_ref's own change: adding id=2 happened at c2 and
# must not appear in c2..c5.
run_test "range_excludes_from_boundary" \
  "SELECT count(*) FROM dolt_diff_t('$C2','$C5') WHERE diff_type='added';" "0" "$DB"

# ...but the commit immediately after from_ref is included: c1..c3 covers c2's
# add of id=2 and c3's modify of id=1, while c1's add of id=1 is excluded.
run_test "range_lower_neighbor_count" \
  "SELECT count(*) FROM dolt_diff_t('$C1','$C3');" "2" "$DB"
run_test "range_lower_neighbor_add" \
  "SELECT to_commit FROM dolt_diff_t('$C1','$C3') WHERE diff_type='added' AND to_id=2;" \
  "$C2" "$DB"
run_test "range_lower_neighbor_excludes_c1" \
  "SELECT count(*) FROM dolt_diff_t('$C1','$C3') WHERE to_id=1 AND diff_type='added';" \
  "0" "$DB"

# A range is a subset of the full per-commit history and preserves the same
# attribution the unfiltered table gives for those commits.
run_test "range_matches_full_attribution" \
  "SELECT (SELECT to_v FROM dolt_diff_t('$C2','$C5') WHERE to_commit='$C3')
        = (SELECT to_v FROM dolt_diff_t WHERE to_commit='$C3' AND to_id=1);" \
  "1" "$DB"

# Reversed / empty range yields nothing.
run_test "range_reversed_empty" \
  "SELECT count(*) FROM dolt_diff_t('$C5','$C2');" "0" "$DB"
run_test "range_identical_empty" \
  "SELECT count(*) FROM dolt_diff_t('$C3','$C3');" "0" "$DB"

# to_ref = WORKING bounds the range at the tip and includes the working change,
# attributed to WORKING; c5's committed change to id=1 is also in c4..WORKING.
echo "UPDATE t SET v='a4' WHERE id=1;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "range_working_has_working_row" \
  "SELECT to_v FROM dolt_diff_t('$C4','WORKING') WHERE to_commit='WORKING';" "a4" "$DB"
run_test "range_working_includes_c5" \
  "SELECT to_v FROM dolt_diff_t('$C4','WORKING') WHERE to_commit='$C5';" "a3" "$DB"

rm -f "$DB"
dltest_finish
