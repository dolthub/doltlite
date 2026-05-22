#!/bin/bash
#
# Coverage for #990: sqlite_stat tables should not produce merge conflicts
# even when both branches ran ANALYZE. Merge should take ours for the
# stat rows and re-ANALYZE on the merged tree as a final step.

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0
fail=0
FAILED_NAMES=""

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected: $expected"
    echo "    actual:   $actual"
  fi
}

check_match() {
  local name="$1" pattern="$2" actual="$3"
  if echo "$actual" | grep -qE "$pattern"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    pattern: $pattern"
    echo "    got:     $actual"
  fi
}

# ── Both sides ran ANALYZE → no conflict, fresh stats post-merge.
DB="$TMPROOT/both.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t SELECT x, x%10 FROM (
  WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<200)
  SELECT x FROM r);
ANALYZE;
SELECT dolt_commit('-A','-m','init');

SELECT dolt_checkout('-b','feat');
INSERT INTO t SELECT x, x%5 FROM (
  WITH RECURSIVE r(x) AS (SELECT 201 UNION ALL SELECT x+1 FROM r WHERE x<400)
  SELECT x FROM r);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');

SELECT dolt_checkout('main');
DELETE FROM t WHERE id > 100;
ANALYZE;
SELECT dolt_commit('-A','-m','main shrink');

SELECT dolt_merge('feat');
EOF

# Expected post-merge row count: main keeps 100, feat adds 200 = 300
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check "both_analyzed_rows_merged" "300" "$out"

out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "both_analyzed_no_conflicts" "0" "$out"

# After the post-merge ANALYZE, sqlite_stat1's row count for iv must
# match the merged data (not either branch's stale snapshot).
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
# stat is "<rows> <avg_dups_per_key>" — the first number must be 300.
case "$out" in
  300\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES both_analyzed_stats_fresh"
     echo "  FAIL: both_analyzed_stats_fresh"; echo "    got: $out" ;;
esac

# ── Only feat ran ANALYZE → take feat's stats forward, then re-ANALYZE.
DB="$TMPROOT/onefeat.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,3);
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check "one_analyzed_rows_merged" "3" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "one_analyzed_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in
  3\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES one_analyzed_stats_fresh"
     echo "  FAIL: one_analyzed_stats_fresh"; echo "    got: $out" ;;
esac

# ── Neither side ran ANALYZE → sqlite_stat1 is not created by merge.
DB="$TMPROOT/none.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,3);
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM sqlite_master WHERE name='sqlite_stat1';")
check "no_analyze_no_stat_table_created" "0" "$out"

# ── Conflicts on real (non-stat) tables still surface normally. In
# explicit-transaction mode, the conflict is exposed via dolt_conflicts.
DB="$TMPROOT/realconf.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2 WHERE id=1;
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
ANALYZE;
SELECT dolt_commit('-A','-m','main');
EOF
err=$("$DOLTLITE" "$DB" "BEGIN; SELECT dolt_merge('feat'); SELECT \"table\" FROM dolt_conflicts; ROLLBACK;" 2>&1)
check_match "real_table_conflict_still_surfaces" "^t$" "$err"

# ── Both branches created sqlite_stat1 from a no-stats ancestor.
# Before the fix, this errored with "table added on both branches with
# different definitions". The schema is fixed; only data differs.
DB="$TMPROOT/freshboth.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
SELECT dolt_commit('-A','-m','init-no-analyze');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,4),(5,5);
ANALYZE;
SELECT dolt_commit('-A','-m','feat-first-analyze');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,6),(7,7),(8,8);
ANALYZE;
SELECT dolt_commit('-A','-m','main-first-analyze');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check "fresh_on_both_rows_merged" "8" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "fresh_on_both_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 8\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES fresh_on_both_stats_fresh"
     echo "  FAIL: fresh_on_both_stats_fresh"; echo "    got: $out" ;;
esac

# ── Both sides ran ANALYZE and produced IDENTICAL sqlite_stat1 rows
# (data on both branches is the same). Merge should be a clean dedupe.
DB="$TMPROOT/identical.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
ANALYZE;
SELECT dolt_commit('-A','-m','feat-analyze');
SELECT dolt_checkout('main');
ANALYZE;
SELECT dolt_commit('-A','-m','main-analyze');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "identical_stats_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 3\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES identical_stats_post_merge"
     echo "  FAIL: identical_stats_post_merge"; echo "    got: $out" ;;
esac

# ── Re-merge after ANALYZE on an already-merged tree: stats should still match.
DB="$TMPROOT/remerge.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1),(2,2);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(3,3);
ANALYZE;
SELECT dolt_commit('-A','-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,4);
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('f1');
EOF
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 4\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES remerge_stats_fresh"
     echo "  FAIL: remerge_stats_fresh"; echo "    got: $out" ;;
esac

# ── DROP INDEX on one side, ANALYZE on the other. The drop wins
# (mergeCatalogPass2 now honors it instead of erroring on theirs's
# automatically-maintained index changes), and the post-merge ANALYZE
# regenerates sqlite_stat1 without a row for the dropped index.
DB="$TMPROOT/dropidx.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX iv ON t(v); CREATE INDEX iw ON t(w);
INSERT INTO t VALUES(1,1,10),(2,2,20),(3,3,30);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,4,40),(5,5,50);
ANALYZE;
SELECT dolt_commit('-A','-m','feat add+analyze');
SELECT dolt_checkout('main');
DROP INDEX iw;
ANALYZE;
SELECT dolt_commit('-A','-m','main drop iw+analyze');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "drop_index_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check "drop_index_rows_merged" "5" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='iw';")
check "drop_index_iw_gone" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM sqlite_stat1 WHERE idx='iw';")
check "drop_index_no_iw_stat" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 5\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES drop_index_iv_fresh"
     echo "  FAIL: drop_index_iv_fresh"; echo "    got: $out" ;;
esac

# NOTE: creating a new index on only one side does NOT cleanly merge —
# theirs's new index lacks ours's rows because the merge brings the
# index in as-is. ANALYZE picks up the (incomplete) state. This is a
# pre-existing merge limitation; we don't run REINDEX on the user's
# behalf. The tests below keep the index landscape stable in the
# ancestor commit so the divergence is purely data + stats.

# ── Two indexes, both present in ancestor. Data diverges on both
# branches; both ANALYZE. Merge must not conflict, and post-merge
# stats must cover BOTH indexes against the merged data.
DB="$TMPROOT/twoidx.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX iv ON t(v); CREATE INDEX iw ON t(w);
INSERT INTO t VALUES(1,1,10),(2,2,20);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,3,30),(4,4,40);
ANALYZE;
SELECT dolt_commit('-A','-m','feat add+analyze');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,5,50);
ANALYZE;
SELECT dolt_commit('-A','-m','main add+analyze');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "two_idx_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM sqlite_stat1 WHERE idx IN ('iv','iw');")
check "two_idx_both_have_stats" "2" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 5\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES two_idx_iv_fresh"
     echo "  FAIL: two_idx_iv_fresh"; echo "    got: $out" ;;
esac
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iw';")
case "$out" in 5\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES two_idx_iw_fresh"
     echo "  FAIL: two_idx_iw_fresh"; echo "    got: $out" ;;
esac

# ── Composite index. ANALYZE produces a multi-column stat string.
DB="$TMPROOT/compidx.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX ivw ON t(v, w);
INSERT INTO t VALUES(1,1,10),(2,2,20);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,3,30);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,4,40);
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "composite_idx_no_conflicts" "0" "$out"
# Composite stat is "<rows> <dups_per_first_col> <dups_per_both_cols>".
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='ivw';")
case "$out" in 4\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES composite_idx_stats_fresh"
     echo "  FAIL: composite_idx_stats_fresh"; echo "    got: $out" ;;
esac

# ── Multiple tables + multiple indexes — stats regenerate for all.
DB="$TMPROOT/multi.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT); CREATE INDEX av ON a(v);
CREATE TABLE b(id INTEGER PRIMARY KEY, w INT); CREATE INDEX bw ON b(w);
INSERT INTO a VALUES(1,1),(2,2);
INSERT INTO b VALUES(1,10),(2,20);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(3,3);
INSERT INTO b VALUES(3,30);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO a VALUES(4,4);
INSERT INTO b VALUES(4,40);
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "multi_table_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='av';")
case "$out" in 4\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES multi_table_av_fresh"
     echo "  FAIL: multi_table_av_fresh"; echo "    got: $out" ;;
esac
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='bw';")
case "$out" in 4\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES multi_table_bw_fresh"
     echo "  FAIL: multi_table_bw_fresh"; echo "    got: $out" ;;
esac

# ── Stats are persisted into the merge commit (not just session state).
# Reopen the DB and check sqlite_stat1 from a fresh process; row count
# should still match the merged data.
DB="$TMPROOT/persist.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1),(2,2),(3,3);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,4),(5,5);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,6);
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
# Now reopen — fresh process reads sqlite_stat1 from the committed catalog.
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 6\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES stats_persisted_in_commit"
     echo "  FAIL: stats_persisted_in_commit"; echo "    got: $out" ;;
esac
# And the merge commit message should be the merge, not an extra ANALYZE commit.
out=$("$DOLTLITE" "$DB" "SELECT message FROM dolt_log LIMIT 1;")
check_match "no_extra_analyze_commit" "Merge branch.*feat" "$out"

# ── ANALYZE on an empty table. sqlite_stat1 typically has no rows for
# zero-row tables, but the merge path must still not break.
DB="$TMPROOT/empty.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
ANALYZE;
SELECT dolt_commit('-A','-m','init empty+analyze');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,1);
ANALYZE;
SELECT dolt_commit('-A','-m','feat add+analyze');
SELECT dolt_checkout('main');
ANALYZE;
SELECT dolt_commit('-A','-m','main re-analyze still empty');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "empty_then_filled_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check "empty_then_filled_row_count" "1" "$out"

# ── Merge --abort after a state that involves stats. The abort must
# leave sqlite_stat1 in its pre-merge shape.
DB="$TMPROOT/abort.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2);
-- Generate a real conflict by overlapping with main.
UPDATE t SET v=99 WHERE id=1;
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=88 WHERE id=1;
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
SELECT dolt_merge('--abort');
EOF
# After abort: t should have 1 row (main's, id=1 v=88), no merge in progress.
out=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=1;")
check "abort_restores_main_value" "88" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "abort_clears_conflicts" "0" "$out"

# ── UNIQUE index. ANALYZE should record dups-per-key of 1.
DB="$TMPROOT/uniq.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE UNIQUE INDEX iv ON t(v);
INSERT INTO t VALUES(1,10),(2,20);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,40);
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "unique_idx_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 4\ 1) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES unique_idx_dups_one"
     echo "  FAIL: unique_idx_dups_one (expected '4 1'); got: $out" ;;
esac

# ── TEXT-typed indexed column.
DB="$TMPROOT/textidx.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX iname ON t(name);
INSERT INTO t VALUES(1,'alice'),(2,'bob');
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'carol');
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'dave');
ANALYZE;
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "text_idx_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iname';")
case "$out" in 4\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES text_idx_fresh"
     echo "  FAIL: text_idx_fresh"; echo "    got: $out" ;;
esac

# ── Merge with --no-ff flag (no actual conflict, but forced merge commit).
DB="$TMPROOT/noff.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2),(3,3);
ANALYZE;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat','--no-ff','-m','forced merge');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "no_ff_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 3\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES no_ff_stats_fresh"
     echo "  FAIL: no_ff_stats_fresh"; echo "    got: $out" ;;
esac

# ── Sequence of two merges where second merge ALSO has stats.
DB="$TMPROOT/twomerges.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1),(2,2);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(3,3);
ANALYZE;
SELECT dolt_commit('-A','-m','f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,10);
ANALYZE;
SELECT dolt_commit('-A','-m','main1');
SELECT dolt_merge('f1');
SELECT dolt_checkout('-b','f2');
INSERT INTO t VALUES(20,20);
ANALYZE;
SELECT dolt_commit('-A','-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(30,30);
ANALYZE;
SELECT dolt_commit('-A','-m','main2');
SELECT dolt_merge('f2');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "two_merges_no_conflicts" "0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;")
check "two_merges_row_count" "6" "$out"
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 6\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES two_merges_stats_fresh"
     echo "  FAIL: two_merges_stats_fresh"; echo "    got: $out" ;;
esac

# ── A branch with stats, merging in another branch without stats.
DB="$TMPROOT/asymm.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,2),(3,3);
-- feat does NOT analyze
SELECT dolt_commit('-A','-m','feat-no-analyze');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,4);
ANALYZE;
SELECT dolt_commit('-A','-m','main-analyze');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "asymm_no_conflicts" "0" "$out"
# Post-merge: 4 rows. main had stats, so post-merge ANALYZE refreshes.
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
case "$out" in 4\ *) pass=$((pass+1)) ;;
  *) fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES asymm_stats_fresh"
     echo "  FAIL: asymm_stats_fresh"; echo "    got: $out" ;;
esac

# ── Conflict-resolved merge finalized via dolt_commit also refreshes
# sqlite_stat1. The post-merge ANALYZE only ran inside dolt_merge in
# the no-conflicts path; the resolve-then-commit path needs the same
# refresh on commit. We check that the stat changed away from the
# pre-merge baseline (1 row on main) rather than asserting an exact
# value — the index may still hold stale entries from the rejected
# theirs's row, which is a separate conflict-resolution bug.
DB="$TMPROOT/resolve.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2 WHERE id=1;
INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
INSERT INTO t VALUES(3,30);
SELECT dolt_commit('-A','-m','main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT stat FROM sqlite_stat1 WHERE idx='iv';")
# Pre-merge baseline on main was 1 row → stat '1 1'. Post-commit we
# expect something different (the merge added rows; ANALYZE saw them).
case "$out" in
  "1 1"|""|"NULL") fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES resolve_commit_reanalyzed"
     echo "  FAIL: resolve_commit_reanalyzed (expected refresh away from pre-merge '1 1'); got: $out" ;;
  *) pass=$((pass+1)) ;;
esac

# ── Identical-data scenario: ancestor had stats, both branches ANALYZED
# again WITHOUT data changes. ours==theirs==ancestor for sqlite_stat1.
DB="$TMPROOT/noop_reanalyze.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1),(2,2);
ANALYZE;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
ANALYZE;
SELECT dolt_commit('--allow-empty','-A','-m','feat re-analyze');
SELECT dolt_checkout('main');
ANALYZE;
SELECT dolt_commit('--allow-empty','-A','-m','main re-analyze');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_conflicts;")
check "noop_reanalyze_no_conflicts" "0" "$out"

echo
echo "doltlite_stats_merge: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
