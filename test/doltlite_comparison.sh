#!/bin/bash
# Cover deep-review E2-E7 (Tier 2 — comparison / equality inconsistencies).
#
# E2 — Fast 3-way merge used raw memcmp; slow used fieldwise. Records
#      re-encoded with a wider serial type were "convergent" for slow
#      and "conflict" for fast — non-deterministic merge.
# E3 — diffMergeWalk overloaded SQLITE_DONE as "values differ"; user
#      callbacks returning SQLITE_DONE could be confused with internal
#      sentinel.
# E4 — prollyValuesEqual declared not-equal when serial types differed
#      even if decoded values matched. INT 0 stored as serial-1 (one
#      byte 0x00) vs serial-8 (no body) → spurious diff/conflict.
# E5 — investigated; existing finalizeSeekOnLeaf sign-flip yields
#      SQLite-conformant *pRes (cursor.key - target.key). No change.
# E6 — prollyCursorPrev asserted on EOF. Stock SQLite supports
#      Next → EOF → Prev → repositioned at last row.
# E7 — diffNodesOneLevel mixed-level fallback walked from pOldRoot /
#      pNewRoot, re-emitting keys from the entire tree above the
#      mismatched subtree.

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Tier 2 comparison fixes (E2/E3/E4/E6/E7) ==="
echo ""

# ----------------------------------------------------------------
# E4 — same logical INT value, different serial encodings, must
#      compare equal in diff.
# ----------------------------------------------------------------
DB=/tmp/test_cmp_e4_$$.db; db_rm "$DB"
# Insert a row, commit, then update the row with the same value
# (this can cause SQLite to re-encode in the same type, but the
# diff path is exercised). Diff a..b should be empty.
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1, 0);
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v = 0 WHERE id = 1;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

# The two commits are logically identical; diff_stat should report 0 rows.
run_test "e4_identical_value_no_modified_rows" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD^','HEAD','t');" \
  "0" "$DB"

# Equally important: read back the value as integer.
run_test "e4_value_intact" \
  "SELECT v FROM t WHERE id=1;" "0" "$DB"
db_rm "$DB"

# ----------------------------------------------------------------
# E6 — Prev after EOF must reposition, not crash.
# ----------------------------------------------------------------
DB=/tmp/test_cmp_e6_$$.db; db_rm "$DB"
# Build a small table, scan forward to EOF, then scan backward.
# SQLite's behavior: ORDER BY DESC is what triggers Prev. A
# combination of forward and reverse iteration on the same cursor
# would exercise the EOF→Prev path; the cheapest user-visible
# witness is just running ORDER BY DESC after a forward scan.
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e6_forward_scan" \
  "SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id);" "a,b,c" "$DB"
run_test "e6_reverse_scan" \
  "SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id DESC);" "c,b,a" "$DB"
# A single sub-select that exercises the reverse path. The bare
# Prev-after-EOF crash would manifest as SIGABRT; we need the
# query to return successfully.
run_test "e6_reverse_with_filter" \
  "SELECT group_concat(v) FROM (SELECT v FROM t WHERE id > 0 ORDER BY id DESC);" \
  "c,b,a" "$DB"
db_rm "$DB"

# ----------------------------------------------------------------
# E2 — fast vs slow merge consistency. Branch A and B make
# independent UPDATE then UPDATE-back to identical values; merge
# should converge without conflict.
# ----------------------------------------------------------------
DB=/tmp/test_cmp_e2_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1, 5);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
UPDATE t SET v = 10 WHERE id = 1;
UPDATE t SET v = 5 WHERE id = 1;
SELECT dolt_commit('-A','-m','main_roundtrip');
SELECT dolt_checkout('feat');
UPDATE t SET v = 7 WHERE id = 1;
UPDATE t SET v = 5 WHERE id = 1;
SELECT dolt_commit('-A','-m','feat_roundtrip');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Both branches roundtripped to v=5; merge should succeed cleanly
# with v=5 in the final row.
run_test "e2_roundtrip_merge_no_conflict" \
  "SELECT v FROM t WHERE id = 1;" "5" "$DB"
run_test "e2_no_conflict_rows" \
  "SELECT count(*) FROM dolt_status WHERE status LIKE '%conflict%';" "0" "$DB"
db_rm "$DB"

# ----------------------------------------------------------------
# E7 — diff of trees with mismatched depths should not re-emit keys.
# Build a table large enough to make a multi-level prolly tree, then
# make a tiny change. The diff should report exactly the changed
# rows, not the entire tree.
# ----------------------------------------------------------------
DB=/tmp/test_cmp_e7_$$.db; db_rm "$DB"
{
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"
  printf "INSERT INTO t VALUES "
  for i in $(seq 1 2000); do
    if [ $i -gt 1 ]; then printf ", "; fi
    printf "(%d, 'row_%d_with_payload_to_force_multilevel_tree')" "$i" "$i"
  done
  echo ";"
  echo "SELECT dolt_commit('-A','-m','base');"
  echo "UPDATE t SET v = 'modified' WHERE id = 1000;"
  echo "SELECT dolt_commit('-A','-m','one_row_changed');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

# Diff should report exactly 1 modified row, not the entire 2000.
run_test "e7_single_row_diff" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
run_test "e7_no_phantom_rows" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD^','HEAD','t');" "0" "$DB"
db_rm "$DB"

# ----------------------------------------------------------------
# E3 — diff iteration semantics. The bug was that diffMergeWalk
# used SQLITE_DONE as a private sentinel. After this fix the
# private sentinel is gone; an out-param signals equal/differ.
# Verify a normal diff still produces the expected change list.
# ----------------------------------------------------------------
DB=/tmp/test_cmp_e3_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v='B' WHERE id=2;
DELETE FROM t WHERE id=3;
INSERT INTO t VALUES(4,'d');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e3_modified_rows" \
  "SELECT rows_modified FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
run_test "e3_added_rows" \
  "SELECT rows_added FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
run_test "e3_deleted_rows" \
  "SELECT rows_deleted FROM dolt_diff_stat('HEAD^','HEAD','t');" "1" "$DB"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
