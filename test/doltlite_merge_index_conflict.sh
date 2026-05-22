#!/bin/bash
#
# Coverage for the merge-time secondary-index cleanup. Before the fix,
# a three-way merge that produced a row-level conflict could leave the
# secondary index with entries from BOTH sides (theirs's entry for the
# conflict row was never filtered out), even though the table itself
# kept ours's value. After conflict-resolve --ours and commit, scans
# through the index returned phantom rows.

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

# ── Classic conflict + --ours: index should have exactly ours's entry
# for the conflict row, plus the non-conflicting rows from both sides.
DB="$TMPROOT/ours.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
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
# Table: ours's row 1 plus both branches' new rows.
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t ORDER BY id;")
check "ours_table_state" "1|3
2|20
3|30" "$out"
# Covering index scan returns the same 3 rows, ordered by v. The pre-fix
# bug surfaced as a fourth phantom row from theirs's (v=2, rowid=1).
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "ours_iv_three_rows" "1|3
2|20
3|30" "$out"
# count(*) of an indexed range matches the table.
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t INDEXED BY iv WHERE v > 0;")
check "ours_iv_count_matches" "3" "$out"

# ── --theirs path: the table state ends up correct, but
# doltliteApplyRawRowMutation in conflict-resolve bypasses secondary-
# index maintenance, so the index entry for the resolved conflict
# row goes stale. Captured here as a known limitation; the fix lives
# at the conflict-resolve layer, not the merge layer.
DB="$TMPROOT/theirs.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
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
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t ORDER BY id;")
check "theirs_table_state" "1|2
2|20
3|30" "$out"

# ── Non-conflict merge: both sides add disjoint rows. Index should
# reflect all rows.
DB="$TMPROOT/clean.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,30);
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "clean_merge_iv_complete" "1|1
2|20
3|30" "$out"

# ── Two indexes on one table, conflict on a different column than the
# indexed ones. Both indexes should be consistent post-merge.
DB="$TMPROOT/twoidx.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX iv ON t(v);
CREATE INDEX iw ON t(w);
INSERT INTO t VALUES(1,1,10);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2, w=20 WHERE id=1;
INSERT INTO t VALUES(2,200,2000);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=3, w=30 WHERE id=1;
INSERT INTO t VALUES(3,300,3000);
SELECT dolt_commit('-A','-m','main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "two_idx_iv" "1|3
2|200
3|300" "$out"
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || w FROM t INDEXED BY iw WHERE w > 0 ORDER BY w;")
check "two_idx_iw" "1|30
2|2000
3|3000" "$out"

# ── DELETE-vs-MODIFY conflict: feat deletes row 1, main updates it.
# Resolve --ours keeps main's value; index should reflect that.
DB="$TMPROOT/delmod.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-A','-m','feat-delete');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_commit('-A','-m','main-update');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "delmod_iv_one_row" "1|3" "$out"

echo
echo "doltlite_merge_index_conflict: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
