#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Merge Status Tests ==="
echo ""

MS="SELECT is_merging || '|' || coalesce(source,'~') || '|' || coalesce(source_commit,'~') || '|' || coalesce(target,'~') || '|' || coalesce(unmerged_tables,'~') FROM dolt_merge_status;"

# Exactly one row, always, whether or not a merge is active.
DB=/tmp/test_ms_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'),(2,'b'); SELECT dolt_commit('-Am','base');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "row_count_clean" "SELECT count(*) FROM dolt_merge_status;" "1" "$DB"
run_test "clean_all_null" "$MS" "0|~|~|~|~" "$DB"
run_test "clean_is_merging_not_null" \
  "SELECT count(*) FROM dolt_merge_status WHERE is_merging IS NOT NULL;" "1" "$DB"

# A conflicted merge reports the source spec, the resolved source commit, the
# target ref and the unmerged table.
echo "SELECT dolt_branch('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE t SET v='feat' WHERE id=1; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB/feature" > /dev/null 2>&1
echo "UPDATE t SET v='main' WHERE id=1; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB" > /dev/null 2>&1
FEATURE_HASH=$(echo "SELECT dolt_hashof('feature');" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
run_test_lastline "conflict_in_session" \
  "BEGIN; SELECT dolt_merge('feature'); COMMIT; $MS" \
  "1|feature|$FEATURE_HASH|refs/heads/main|t" "$DB"
run_test "row_count_merging" "SELECT count(*) FROM dolt_merge_status;" "1" "$DB"

# Merge state lives in the working set, so a later connection -- which never saw
# the dolt_merge call -- must still report the merge.
run_test "conflict_persists_across_connections" "$MS" \
  "1|feature|$FEATURE_HASH|refs/heads/main|t" "$DB"

# Resolving and committing ends the merge.
echo "BEGIN; SELECT dolt_conflicts_resolve('--ours','t'); SELECT dolt_commit('-m','resolved');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "cleared_after_resolve_commit" "$MS" "0|~|~|~|~" "$DB"

# ROLLBACK of a conflicted merge leaves no trace, including no stale source spec.
DB2=/tmp/test_ms_rb_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB2" > /dev/null 2>&1
echo "UPDATE t SET v='t'; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB2/src" > /dev/null 2>&1
echo "UPDATE t SET v='o'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test_lastline "rollback_clears_state" \
  "BEGIN; SELECT dolt_merge('src'); ROLLBACK; $MS" "0|~|~|~|~" "$DB2"
run_test "rollback_clears_state_next_connection" "$MS" "0|~|~|~|~" "$DB2"

# A fast-forward merge completes, so no merge is left active.
DB3=/tmp/test_ms_ff_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('ff');" | $DOLTLITE "$DB3" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-Am','ahead');" | $DOLTLITE "$DB3/ff" > /dev/null 2>&1
run_test "fast_forward_not_merging" "SELECT dolt_merge('ff'); $MS" \
  "$(echo "SELECT dolt_hashof('ff');" | $DOLTLITE "$DB3" 2>/dev/null | tail -1)
0|~|~|~|~" "$DB3"

# A clean three-way merge also completes.
DB4=/tmp/test_ms_clean_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('b2');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "INSERT INTO t VALUES(2,'b'); SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB4/b2" > /dev/null 2>&1
echo "INSERT INTO t VALUES(3,'c'); SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB4" > /dev/null 2>&1
echo "SELECT dolt_merge('b2');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test "clean_merge_not_merging" "$MS" "0|~|~|~|~" "$DB4"

# unmerged_tables unions every unmerged table, deduplicated and name-ordered.
DB5=/tmp/test_ms_multi_$$.db; rm -f "$DB5"
echo "CREATE TABLE zeta(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE alpha(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO zeta VALUES(1,'b'); INSERT INTO alpha VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "UPDATE zeta SET v='t'; UPDATE alpha SET v='t'; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB5/src" > /dev/null 2>&1
echo "UPDATE zeta SET v='o'; UPDATE alpha SET v='o'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB5" > /dev/null 2>&1
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB5" > /dev/null 2>&1
run_test "unmerged_tables_sorted" \
  "SELECT unmerged_tables FROM dolt_merge_status;" "alpha, zeta" "$DB5"

# Constraint violations count as unmerged even when the conflict is elsewhere.
DB6=/tmp/test_ms_cv_$$.db; rm -f "$DB6"
echo "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id)); CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO parent VALUES(1); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "INSERT INTO child VALUES(11,1); UPDATE t SET v='theirs'; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB6/src" > /dev/null 2>&1
echo "DELETE FROM parent WHERE id=1; UPDATE t SET v='ours'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB6" > /dev/null 2>&1
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB6" > /dev/null 2>&1
run_test "unmerged_tables_includes_violations" \
  "SELECT unmerged_tables FROM dolt_merge_status;" "child, t" "$DB6"

# A schema conflict makes the table unmerged even with no row conflict.
DB7=/tmp/test_ms_schema_$$.db; rm -f "$DB7"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN c1 INT DEFAULT 1; SELECT dolt_commit('-Am','their col');" | $DOLTLITE "$DB7/src" > /dev/null 2>&1
echo "ALTER TABLE t ADD COLUMN c1 TEXT DEFAULT 'x'; SELECT dolt_commit('-Am','our col');" | $DOLTLITE "$DB7" > /dev/null 2>&1
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB7" > /dev/null 2>&1
run_test "unmerged_tables_includes_schema_conflict" \
  "SELECT unmerged_tables FROM dolt_merge_status;" "t" "$DB7"

# target follows the branch merged into, not the default branch.
DB8=/tmp/test_ms_target_$$.db; rm -f "$DB8"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('dev'); SELECT dolt_branch('src');" | $DOLTLITE "$DB8" > /dev/null 2>&1
echo "UPDATE t SET v='t'; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB8/src" > /dev/null 2>&1
echo "UPDATE t SET v='o'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB8/dev" > /dev/null 2>&1
run_test_lastline "target_is_merged_into_branch" \
  "BEGIN; SELECT dolt_merge('src'); COMMIT; SELECT target FROM dolt_merge_status;" \
  "refs/heads/dev" "$DB8/dev"

# Merging a raw commit hash reports that hash as the source, matching Dolt.
DB9=/tmp/test_ms_hash_$$.db; rm -f "$DB9"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB9" > /dev/null 2>&1
echo "UPDATE t SET v='t'; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB9/src" > /dev/null 2>&1
echo "UPDATE t SET v='o'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB9" > /dev/null 2>&1
SRC_HASH=$(echo "SELECT dolt_hashof('src');" | $DOLTLITE "$DB9" 2>/dev/null | tail -1)
run_test_lastline "source_is_hash_for_hash_spec" \
  "BEGIN; SELECT dolt_merge('$SRC_HASH'); COMMIT; SELECT source || '|' || source_commit FROM dolt_merge_status;" \
  "$SRC_HASH|$SRC_HASH" "$DB9"

# With no session spec to consult, a later connection recovers the branch name
# from the persisted merge commit.
run_test "source_derived_next_connection" \
  "SELECT source FROM dolt_merge_status;" "src" "$DB9"

# A merge commit no branch points at falls back to the hash rather than naming
# an unrelated branch.
DB10=/tmp/test_ms_mid_$$.db; rm -f "$DB10"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "UPDATE t SET v='t1'; SELECT dolt_commit('-Am','t1');" | $DOLTLITE "$DB10/src" > /dev/null 2>&1
MID_HASH=$(echo "SELECT dolt_hashof('src');" | $DOLTLITE "$DB10" 2>/dev/null | tail -1)
echo "INSERT INTO t VALUES(5,'t2'); SELECT dolt_commit('-Am','t2');" | $DOLTLITE "$DB10/src" > /dev/null 2>&1
echo "UPDATE t SET v='o'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB10" > /dev/null 2>&1
echo "BEGIN; SELECT dolt_merge('$MID_HASH'); COMMIT;" | $DOLTLITE "$DB10" > /dev/null 2>&1
run_test "source_falls_back_to_hash" \
  "SELECT source || '|' || source_commit FROM dolt_merge_status;" \
  "$MID_HASH|$MID_HASH" "$DB10"

# A merge stopped only by constraint violations -- no row conflict anywhere -- is
# still an unfinished merge, so it must report as one.
mk_cv_only_merge() {
  local db="$1"
  rm -f "$db"
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT UNIQUE); INSERT INTO t VALUES(1,10); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$db" > /dev/null 2>&1
  echo "INSERT INTO t VALUES(2,99); SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$db/src" > /dev/null 2>&1
  echo "INSERT INTO t VALUES(3,99); SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$db" > /dev/null 2>&1
}

DB12=/tmp/test_ms_cvonly_$$.db
mk_cv_only_merge "$DB12"
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB12" > /dev/null 2>&1
run_test "cv_only_merge_is_merging" "$MS" \
  "1|src|$(echo "SELECT dolt_hashof('src');" | $DOLTLITE "$DB12" 2>/dev/null | tail -1)|refs/heads/main|t" "$DB12"
run_test "cv_only_merge_no_conflicts" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB12"
run_test "cv_only_merge_has_violations" \
  "SELECT \"table\" || ':' || num_violations FROM dolt_constraint_violations;" "t:2" "$DB12"

# The three ways out of a merge all clear it, constraint violations included.
DB13=/tmp/test_ms_cvabort_$$.db
mk_cv_only_merge "$DB13"
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB13" > /dev/null 2>&1
echo "SELECT dolt_merge('--abort');" | $DOLTLITE "$DB13" > /dev/null 2>&1
run_test "cv_only_cleared_by_abort" "$MS" "0|~|~|~|~" "$DB13"

DB14=/tmp/test_ms_cvreset_$$.db
mk_cv_only_merge "$DB14"
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB14" > /dev/null 2>&1
echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB14" > /dev/null 2>&1
run_test "cv_only_cleared_by_reset" "$MS" "0|~|~|~|~" "$DB14"

DB15=/tmp/test_ms_cvresolve_$$.db
mk_cv_only_merge "$DB15"
echo "BEGIN; SELECT dolt_merge('src'); COMMIT;" | $DOLTLITE "$DB15" > /dev/null 2>&1
echo "BEGIN; DELETE FROM dolt_constraint_violations_t; SELECT dolt_commit('-Am','resolved');" | $DOLTLITE "$DB15" > /dev/null 2>&1
run_test "cv_only_cleared_by_resolve_commit" "$MS" "0|~|~|~|~" "$DB15"

# An autocommit merge rolls back whole, violations and merge state together.
DB16=/tmp/test_ms_cvauto_$$.db
mk_cv_only_merge "$DB16"
echo "SELECT dolt_merge('src');" | $DOLTLITE "$DB16" > /dev/null 2>&1
run_test "cv_only_autocommit_rolled_back" "$MS" "0|~|~|~|~" "$DB16"

# A merge still open after every conflict was resolved reports an empty
# unmerged_tables, not NULL and not an error.
DB11=/tmp/test_ms_resolved_$$.db; rm -f "$DB11"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'b'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('src');" | $DOLTLITE "$DB11" > /dev/null 2>&1
echo "UPDATE t SET v='t'; SELECT dolt_commit('-Am','theirs');" | $DOLTLITE "$DB11/src" > /dev/null 2>&1
echo "UPDATE t SET v='o'; SELECT dolt_commit('-Am','ours');" | $DOLTLITE "$DB11" > /dev/null 2>&1
run_test_lastline "resolved_but_uncommitted_empty_tables" \
  "BEGIN; SELECT dolt_merge('src'); SELECT dolt_conflicts_resolve('--ours','t');
   SELECT is_merging || '|' || unmerged_tables || '|' || length(unmerged_tables) FROM dolt_merge_status;" \
  "1||0" "$DB11"

# The table is read-only.
run_test_match "read_only" \
  "INSERT INTO dolt_merge_status VALUES(1,'a','b','c','d');" \
  "may not be modified" "$DB"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11" \
  "$DB12" "$DB13" "$DB14" "$DB15" "$DB16"
dltest_finish
