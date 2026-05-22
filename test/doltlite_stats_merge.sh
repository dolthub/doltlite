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

# ── Conflicts on real (non-stat) tables still surface normally.
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
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT \"table\" FROM dolt_conflicts;")
check_match "real_table_conflict_still_surfaces" "^t$" "$out"

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

echo
echo "doltlite_stats_merge: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
