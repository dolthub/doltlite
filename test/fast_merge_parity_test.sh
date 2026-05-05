#!/bin/bash
#
# Fast-merge parity oracle.
#
# For each scenario, run the same merge twice:
#   1. With DOLTLITE_FORCE_ROW_MERGE=1 — always take the row-by-row path
#   2. Without that env var — fast path runs whenever predicate-eligible
#
# Assert that both runs produce:
#   - the same merged tree root hash (via dolt_hashof_table)
#   - the same row count
#   - the same row contents
#   - the same conflict set
#
# These three assertions together verify byte-level equivalence of the
# fast and slow paths for any case the predicate marks eligible.
#
# Usage: bash fast_merge_parity_test.sh [path/to/doltlite]

set -u
DOLTLITE="${1:-./doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
PASS=0; FAIL=0; ERRORS=""

# Run a merge scenario both ways, compare outputs.
# $1 = scenario name, $2 = setup SQL (must end before any dolt_merge),
# $3 = list of comma-separated query strings to run after merge to
#      verify state.
run_parity() {
  local label="$1"
  local setup="$2"
  local queries="$3"
  local db_fast="$TMPDIR/${label}_fast.db"
  local db_slow="$TMPDIR/${label}_slow.db"
  rm -f "$db_fast" "$db_slow"

  # Slow path (forced row-by-row).
  local out_slow
  out_slow=$(echo "$setup" | DOLTLITE_FORCE_ROW_MERGE=1 \
             "$DOLTLITE" "$db_slow" 2>&1)
  local rc_slow=$?

  # Fast path (predicate-driven).
  local out_fast
  out_fast=$(echo "$setup" | "$DOLTLITE" "$db_fast" 2>&1)
  local rc_fast=$?

  # Both runs must produce the same exit code AND the same final
  # output. A merge that surfaces a conflict (rc=1 + autocommit
  # rollback) is a valid result — parity just requires both paths
  # behave identically.
  if [ $rc_slow -ne $rc_fast ]; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $label (rc divergence fast=$rc_fast slow=$rc_slow)"
    ERRORS="$ERRORS\n  fast stderr: $out_fast"
    ERRORS="$ERRORS\n  slow stderr: $out_slow"
    return
  fi
  if [ "$out_fast" != "$out_slow" ]; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $label (output divergence)"
    ERRORS="$ERRORS\n  fast stderr: $out_fast"
    ERRORS="$ERRORS\n  slow stderr: $out_slow"
    return
  fi

  # Compare the merged-tree root via dolt_hashof_table on each user
  # table. (Built-in tables may differ in internal-only fields; we
  # care about user-data parity.) Iterate the user tables.
  local fast_hashes slow_hashes
  fast_hashes=$(echo "SELECT name, dolt_hashof_table(name) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'dolt_%' AND name NOT LIKE 'sqlite_%' ORDER BY name;" \
                | "$DOLTLITE" "$db_fast" 2>&1)
  slow_hashes=$(echo "SELECT name, dolt_hashof_table(name) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'dolt_%' AND name NOT LIKE 'sqlite_%' ORDER BY name;" \
                | "$DOLTLITE" "$db_slow" 2>&1)
  if [ "$fast_hashes" != "$slow_hashes" ]; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $label (table hash divergence)"
    ERRORS="$ERRORS\n  fast: $fast_hashes"
    ERRORS="$ERRORS\n  slow: $slow_hashes"
    return
  fi

  # Run each verification query against both DBs and compare.
  local IFS=$'\n'
  for q in $queries; do
    local r_fast r_slow
    r_fast=$(echo "$q" | "$DOLTLITE" "$db_fast" 2>&1)
    r_slow=$(echo "$q" | "$DOLTLITE" "$db_slow" 2>&1)
    if [ "$r_fast" != "$r_slow" ]; then
      FAIL=$((FAIL+1))
      ERRORS="$ERRORS\nFAIL: $label (query divergence)"
      ERRORS="$ERRORS\n  query: $q"
      ERRORS="$ERRORS\n  fast:  $r_fast"
      ERRORS="$ERRORS\n  slow:  $r_slow"
      return
    fi
  done

  PASS=$((PASS+1))
}

echo "=== Fast-Merge Parity Oracle ==="
echo ""

# 1. Small table, non-overlapping single inserts. Likely takes
#    the leaf-only fall-through path on both sides since the tree
#    fits in a single leaf — but parity must still hold.
SCENARIO_SMALL="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(4,'main4');
SELECT dolt_commit('-A','-m','main4');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(5,'feat5');
SELECT dolt_commit('-A','-m','feat5');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_parity "small_nonoverlap" \
  "$SCENARIO_SMALL" \
  "SELECT count(*) FROM t;
SELECT id||':'||v FROM t ORDER BY id;
SELECT count(*) FROM dolt_log;"

# 2. Larger table that builds an interior tree. Generate a few
#    thousand rows so chunking produces multiple leaves and at
#    least one interior level. Branches insert in disjoint key
#    ranges (the headline non-overlapping case fast merge wins on).
SCENARIO_LARGE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<2000)
INSERT INTO t SELECT n, 'init_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10)
UPDATE t SET v='main_'||id WHERE id IN (SELECT n FROM seq);
SELECT dolt_commit('-A','-m','main_updates');
SELECT dolt_checkout('feat');
WITH RECURSIVE seq(n) AS (SELECT 1991 UNION ALL SELECT n+1 FROM seq WHERE n<2000)
UPDATE t SET v='feat_'||id WHERE id IN (SELECT n FROM seq);
SELECT dolt_commit('-A','-m','feat_updates');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_parity "large_nonoverlap" \
  "$SCENARIO_LARGE" \
  "SELECT count(*) FROM t;
SELECT v FROM t WHERE id=1;
SELECT v FROM t WHERE id=10;
SELECT v FROM t WHERE id=1000;
SELECT v FROM t WHERE id=1991;
SELECT v FROM t WHERE id=2000;
SELECT count(*) FROM dolt_log;"

# 3. Identical changes on both sides — both branches add the same row.
#    The fast path's ours==theirs short-circuit handles this without
#    touching the chunker.
SCENARIO_IDENTICAL="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'same');
SELECT dolt_commit('-A','-m','main_same');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'same');
SELECT dolt_commit('-A','-m','feat_same');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_parity "identical_changes" \
  "$SCENARIO_IDENTICAL" \
  "SELECT count(*) FROM t;
SELECT v FROM t WHERE id=2;"

# 4. Modify-modify conflict on a small table. The fast path returns
#    not-handled (small tree → leaf-only); slow path resolves via
#    cell-merge or surfaces a conflict. Either way both runs must
#    agree.
SCENARIO_CONFLICT="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
UPDATE t SET v='main_changed' WHERE id=1;
SELECT dolt_commit('-A','-m','main_change');
SELECT dolt_checkout('feat');
UPDATE t SET v='feat_changed' WHERE id=1;
SELECT dolt_commit('-A','-m','feat_change');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_parity "modify_modify_conflict" \
  "$SCENARIO_CONFLICT" \
  "SELECT count(*) FROM t;
SELECT v FROM t WHERE id=1;
SELECT count(*) FROM dolt_conflicts_t;"

# 5. Delete + insert in disjoint regions on each side.
SCENARIO_DELETE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<200)
INSERT INTO t SELECT n, 'v_'||n FROM seq;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
DELETE FROM t WHERE id<=5;
SELECT dolt_commit('-A','-m','main_delete');
SELECT dolt_checkout('feat');
DELETE FROM t WHERE id>=196;
SELECT dolt_commit('-A','-m','feat_delete');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_parity "disjoint_deletes" \
  "$SCENARIO_DELETE" \
  "SELECT count(*) FROM t;
SELECT id FROM t WHERE id<10 ORDER BY id;
SELECT id FROM t WHERE id>190 ORDER BY id;"

echo ""
echo "================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================"
if [ $FAIL -gt 0 ]; then
  printf "%b\n" "$ERRORS"
  exit 1
fi
