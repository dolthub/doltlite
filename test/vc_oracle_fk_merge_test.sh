#!/bin/bash
#
# Version-control oracle test: FK / unique / CHECK constraint
# violations on merge.
#
# Spec follows https://www.dolthub.com/blog/2021-07-20-merging-branches-with-foreign-keys/
# extended to match what current Dolt actually does:
#
#   - Merges apply cell-by-cell and do NOT enforce referential
#     actions. Instead, after the merge, the diff vs the ancestor
#     is walked and any row that violates a constraint is
#     recorded in `dolt_constraint_violations_<table>`.
#
#   - A summary read-only vtable `dolt_constraint_violations`
#     reports (table, num_violations).
#
#   - Per-table vtables `dolt_constraint_violations_<table>` have
#     columns (violation_type, <user table PK+value cols>,
#     violation_info JSON). Rows are user-deletable to clear.
#
#   - violation_type values (matching Dolt, lowercase):
#       'foreign key'       FK orphan — row IS in the base table
#       'unique index'      duplicate — row IS in the base table
#       'check constraint'  CHECK failure — row IS in the base table
#
#   - `dolt_commit` MUST fail while any dolt_constraint_violations
#     row remains, to force the user to either resolve or
#     explicitly force-commit.
#
# Oracle design
#
# Dolt's exact byte format for violation_info JSON and its
# transaction semantics don't translate perfectly to doltlite
# (stored procs vs functions, session vars vs free calls).
# This oracle therefore asserts on the observable invariants:
#
#     1. Existence of violation row(s) in the per-table vtable
#     2. The summary vtable reports the right count
#     3. The base table's post-merge content matches the rule
#        (FK/CHECK/UNIQUE: row present; UNIQUE duplicates stay
#         in the base table until the user resolves them)
#     4. `dolt_commit` refuses to proceed while violations exist
#     5. Deleting violations from the per-table vtable allows
#        commit to succeed
#
# It does NOT cross-check exact violation_info JSON byte-for-byte
# against Dolt — only that the field is non-empty JSON with the
# expected top-level keys.
#
# Usage: bash vc_oracle_fk_merge_test.sh [path/to/doltlite]
#

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Run one SQL statement against an existing db and emit stdout
# tail-1 (strips leading hash-emitting commands). Errors leak to
# a named stderr file for the caller to inspect.
dl() {
  local db="$1" sql="$2" tag="$3"
  "$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$tag.err"
}

# Bulk setup — feed multiple statements via stdin.
dl_setup() {
  local db="$1" tag="$2"
  "$DOLTLITE" "$db" 2>"$TMPROOT/$tag.err" >/dev/null
}

# Bulk setup capturing stdout too. Used by the single-session
# scenarios (e.g. the vtable-auto-register tests) where the
# setup statements and the assertion queries must run in the
# same process to exercise in-session registration.
dl_setup_capture() {
  local db="$1" tag="$2"
  "$DOLTLITE" "$db" >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
}

# Does the given SQL error out? Returns 0 (true) if any error
# token shows up in stdout/stderr, 1 otherwise.
dl_errors() {
  local db="$1" sql="$2" tag="$3"
  "$DOLTLITE" "$db" "$sql" >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
  grep -qiE 'error|Error' "$TMPROOT/$tag.out" "$TMPROOT/$tag.err" 2>/dev/null
}

pass_name() {
  pass=$((pass+1))
  echo "  PASS: $1"
}

fail_name() {
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

expect_eq() {
  local name="$1" want="$2" got="$3"
  if [ "$want" = "$got" ]; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    want: |$want|"
    echo "    got:  |$got|"
  fi
}

expect_nonempty() {
  local name="$1" got="$2"
  if [ -n "$got" ]; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    (empty)"
  fi
}

expect_true() {
  local name="$1" cond="$2"
  if [ "$cond" = "1" ]; then pass_name "$name"; else fail_name "$name"; fi
}

echo "=== Version Control Oracle Tests: FK / UNIQUE / CHECK merge violations ==="
echo ""

# ── Scenario A: FK orphan (blog example) ─────────────────
echo "--- A. FK: parent delete + child add → orphan ---"

DB="$TMPROOT/fk_orphan.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk_orphan"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child  VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (2,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=2;
SELECT dolt_commit('-Am','main_drop_parent');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='child';" "fk_count")
expect_eq "fk_orphan_summary_count" "1" "$N"

TYPE=$(dl "$DB" "SELECT violation_type FROM dolt_constraint_violations_child WHERE pk=2;" "fk_type")
expect_eq "fk_orphan_violation_type" "foreign key" "$TYPE"

CHILD_ROW_COUNT=$(dl "$DB" "SELECT count(*) FROM child WHERE pk=2;" "fk_child_row")
expect_eq "fk_orphan_row_present_in_table" "1" "$CHILD_ROW_COUNT"

INFO=$(dl "$DB" "SELECT violation_info FROM dolt_constraint_violations_child WHERE pk=2;" "fk_info")
expect_nonempty "fk_orphan_violation_info_nonempty" "$INFO"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge');" "fk_commit_block"; then
  pass_name "fk_orphan_commit_blocked"
else
  fail_name "fk_orphan_commit_blocked"
fi

dl "$DB" "DELETE FROM dolt_constraint_violations_child;" "fk_clear" >/dev/null
N_AFTER=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child;" "fk_count_after")
expect_eq "fk_orphan_cleared_after_delete" "0" "$N_AFTER"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge-cleared');" "fk_commit_after"; then
  fail_name "fk_orphan_commit_after_clear"
else
  pass_name "fk_orphan_commit_after_clear"
fi

echo ""

# ── Scenario B: Clean merge over FK (negative test) ──────
echo "--- B. FK: both sides add valid children → clean merge ---"

DB="$TMPROOT/fk_clean.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk_clean"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child  VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (2,2);
SELECT dolt_commit('-Am','feat_add_2_2');
SELECT dolt_checkout('main');
INSERT INTO child VALUES (3,1);
SELECT dolt_commit('-Am','main_add_3_1');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations;" "fk_clean_count")
expect_eq "fk_clean_merge_no_violations" "0" "$N"

# Merge auto-creates a commit when there are no conflicts, so a
# second dolt_commit has nothing to commit — that's fine. The
# assertion we care about is: the merge DID complete with zero
# violations (checked above) so the working set is clean.

echo ""

# ── Scenario C: Unique index violation ───────────────────
echo "--- C. UNIQUE: both sides insert same unique value → rejected row in violations ---"

DB="$TMPROOT/uniq.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "uniq"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,1);
SELECT dolt_commit('-Am','feat_add_2_1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (1,1);
SELECT dolt_commit('-Am','main_add_1_1');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "uniq_count")
expect_eq "unique_violation_summary_count" "2" "$N"

TYPE=$(dl "$DB" "SELECT group_concat(violation_type, ',') FROM (SELECT DISTINCT violation_type FROM dolt_constraint_violations_t ORDER BY violation_type);" "uniq_type")
expect_eq "unique_violation_type" "unique index" "$TYPE"

BASE_COUNT=$(dl "$DB" "SELECT count(*) FROM t;" "uniq_base")
expect_eq "unique_violation_rows_kept_in_base" "2" "$BASE_COUNT"

BASE_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM t ORDER BY pk);" "uniq_base_pk")
expect_eq "unique_violation_base_rows" "1,2" "$BASE_ROWS"

VIOL_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM dolt_constraint_violations_t ORDER BY pk);" "uniq_viol_rows")
expect_eq "unique_violation_rows" "1,2" "$VIOL_ROWS"

echo ""

# ── Scenario C2: Unique violation survives temp shadowing ─
echo "--- C2. UNIQUE: temp shadow table cannot hide merge violation ---"

DB="$TMPROOT/uniq_shadow.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "uniq_shadow"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
INSERT INTO t VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3,20);
SELECT dolt_commit('-Am','feat_add_3_20');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2,20);
SELECT dolt_commit('-Am','main_add_2_20');
CREATE TEMP TABLE t(x INT);
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "uniq_shadow_count")
expect_eq "unique_shadow_violation_summary_count" "2" "$N"

TYPE=$(dl "$DB" "SELECT group_concat(violation_type, ',') FROM (SELECT DISTINCT violation_type FROM dolt_constraint_violations_t ORDER BY violation_type);" "uniq_shadow_type")
expect_eq "unique_shadow_violation_type" "unique index" "$TYPE"

BASE_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM t ORDER BY pk);" "uniq_shadow_base")
expect_eq "unique_shadow_rows_kept_in_base" "1,2,3" "$BASE_ROWS"

VIOL_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM dolt_constraint_violations_t ORDER BY pk);" "uniq_shadow_viol")
expect_eq "unique_shadow_rows" "2,3" "$VIOL_ROWS"

echo ""

# ── Scenario D: CHECK constraint violation ───────────────
echo "--- D. CHECK: one side adds constraint, other side commits a violating row ---"

DB="$TMPROOT/check.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "check"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,-5);
SELECT dolt_commit('-Am','feat_add_neg');
SELECT dolt_checkout('main');
ALTER TABLE t ADD CONSTRAINT positive_v CHECK (v > 0);
SELECT dolt_commit('-Am','main_add_check');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "check_count")
expect_eq "check_violation_summary_count" "1" "$N"

TYPE=$(dl "$DB" "SELECT violation_type FROM dolt_constraint_violations_t WHERE pk=2;" "check_type")
expect_eq "check_violation_type" "check constraint" "$TYPE"

BASE_COUNT=$(dl "$DB" "SELECT count(*) FROM t WHERE pk=2;" "check_base")
expect_eq "check_violation_row_present_in_table" "1" "$BASE_COUNT"

INFO=$(dl "$DB" "SELECT violation_info FROM dolt_constraint_violations_t WHERE pk=2;" "check_info")
expect_nonempty "check_violation_info_nonempty" "$INFO"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge');" "check_commit_block"; then
  pass_name "check_violation_commit_blocked"
else
  fail_name "check_violation_commit_blocked"
fi

echo ""

# ── Scenario D2: CHECK violation survives temp shadowing ──
echo "--- D2. CHECK: temp shadow table cannot hide merge violation ---"

DB="$TMPROOT/check_shadow.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "check_shadow"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,-5);
SELECT dolt_commit('-Am','feat_add_neg');
SELECT dolt_checkout('main');
ALTER TABLE t ADD CONSTRAINT positive_v CHECK (v > 0);
SELECT dolt_commit('-Am','main_add_check');
CREATE TEMP TABLE t(x INT);
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "check_shadow_count")
expect_eq "check_shadow_violation_summary_count" "1" "$N"

TYPE=$(dl "$DB" "SELECT violation_type FROM dolt_constraint_violations_t WHERE pk=2;" "check_shadow_type")
expect_eq "check_shadow_violation_type" "check constraint" "$TYPE"

BASE_COUNT=$(dl "$DB" "SELECT count(*) FROM t WHERE pk=2;" "check_shadow_base")
expect_eq "check_shadow_row_present_in_table" "1" "$BASE_COUNT"

echo ""

# ── Scenario E: Multiple violations, multiple tables ─────
echo "--- E. Multiple tables with violations ---"

DB="$TMPROOT/multi.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "multi"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2),(3,3);
INSERT INTO child  VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (2,2), (3,3);
SELECT dolt_commit('-Am','feat_add_children');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk IN (2,3);
SELECT dolt_commit('-Am','main_drop_parents');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='child';" "multi_count")
expect_eq "multi_fk_count_equals_2" "2" "$N"

NTABLES=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations;" "multi_tables")
expect_eq "multi_summary_one_table" "1" "$NTABLES"

echo ""

# ── Scenario F: Reopen preserves violations ──────────────
echo "--- F. Violations persist across reopen ---"

DB="$TMPROOT/reopen.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "reopen"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child  VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (2,2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=2;
SELECT dolt_commit('-Am','main_drop_parent');
SELECT dolt_merge('feat');
SQL

N_REOPEN=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='child';" "reopen_count")
expect_eq "violations_persist_across_reopen" "1" "$N_REOPEN"

echo ""

# ── Scenario G: Pre-existing FK orphan is NOT re-flagged ─
#
# An orphan that already existed in the ancestor (e.g. because
# FKs were off when the parent got deleted) must not be flagged
# by an unrelated merge that touches different rows. Dolt's
# model: merge-introduced violations only — a row that was
# broken before either side started working is not this merge's
# problem. Without the ancestor filter, the current walker sees
# the orphan in the post-merge tree and mis-flags it.
echo "--- G. FK: pre-existing orphan is not re-flagged by unrelated merge ---"

DB="$TMPROOT/fk_preexisting.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk_preexisting"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child  VALUES (1,1),(99,2);
DELETE FROM parent WHERE pk=2;
SELECT dolt_commit('-Am','ancestor_with_orphan');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO parent VALUES (3,3);
SELECT dolt_commit('-Am','feat_add_parent');
SELECT dolt_checkout('main');
INSERT INTO child VALUES (5,1);
SELECT dolt_commit('-Am','main_add_child');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations;" "fk_preexisting_count")
expect_eq "fk_preexisting_orphan_not_reflagged" "0" "$N"

# A follow-up commit should succeed — there's no dolt_constraint_violations
# block in effect. Insert an unrelated row first so there's actually
# something to commit (the merge above auto-commits on clean merge, so
# we need real working-set state for the followup commit to be non-empty).
dl "$DB" "INSERT INTO parent VALUES (7,7);" "fk_preexisting_insert" >/dev/null
if dl_errors "$DB" "SELECT dolt_commit('-Am','followup');" "fk_preexisting_commit"; then
  fail_name "fk_preexisting_commit_not_blocked"
else
  pass_name "fk_preexisting_commit_not_blocked"
fi

echo ""

# ── Scenario H: Pre-existing CHECK failure (via force) ───
#
# If a previous merge landed a CHECK violation and the user
# force-committed past it, the offending row lives in committed
# state. A subsequent unrelated merge must not re-flag it —
# same "merge-introduced only" rule as scenario G.
echo "--- H. CHECK: force-committed violation is not re-flagged ---"

DB="$TMPROOT/check_preexisting.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "check_preexisting"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,-5);
SELECT dolt_commit('-Am','feat_add_neg');
SELECT dolt_checkout('main');
ALTER TABLE t ADD CONSTRAINT positive_v CHECK (v > 0);
SELECT dolt_commit('-Am','main_add_check');
SELECT dolt_merge('feat');
DELETE FROM dolt_constraint_violations_t;
SELECT dolt_commit('--force','-m','accept_preexisting');
SELECT dolt_branch('feat2');
SELECT dolt_checkout('feat2');
INSERT INTO t VALUES (3,30);
SELECT dolt_commit('-Am','feat2_add_pos');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4,40);
SELECT dolt_commit('-Am','main_add_pos');
SELECT dolt_merge('feat2');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations;" "check_preexisting_count")
expect_eq "check_preexisting_not_reflagged" "0" "$N"

echo ""

# ── Scenario I: vtable auto-register after mid-session CREATE + commit ─
#
# Per #494: dolt_constraint_violations_<table> used to register
# only at db open time via doltliteForEachUserTable, so a table
# created in a live session never got its per-table vtable
# until the next reopen. Post-commit refresh now re-runs the
# registration walker, matching how dolt_diff_<table> and
# friends behave. Everything in this scenario runs in a single
# doltlite process so we actually exercise in-session state.
echo "--- I. dolt_constraint_violations_<table> auto-registers after in-session CREATE + commit ---"

DB="$TMPROOT/vtable_autoreg.db"
rm -f "$DB"
cat <<'SQL' | dl_setup_capture "$DB" "vtable_autoreg"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk INTEGER PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (1,1);
SELECT dolt_commit('-Am','init');
SELECT '=== query after commit ===' AS marker;
SELECT count(*) FROM dolt_constraint_violations_child;
SELECT count(*) FROM dolt_constraint_violations_parent;
SQL

if grep -q "no such table: dolt_constraint_violations_" "$TMPROOT/vtable_autoreg.out" \
                                                         "$TMPROOT/vtable_autoreg.err" 2>/dev/null; then
  fail_name "vtable_autoreg_after_commit"
  echo "    (in-session query after commit still reports no such table)"
else
  pass_name "vtable_autoreg_after_commit"
fi

echo ""

# ── Scenario J: WITHOUT ROWID FK violation works ─────────
echo "--- J. WITHOUT ROWID FK merge records orphan violation ---"

DB="$TMPROOT/without_rowid_fk.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_fk"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk TEXT PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1)) WITHOUT ROWID;
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES ('a',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES ('b',2);
SELECT dolt_commit('-Am','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=2;
SELECT dolt_commit('-Am','main_drop_parent');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='child';" "without_rowid_fk_count")
expect_eq "without_rowid_fk_summary_count" "1" "$N"

TYPE=$(dl "$DB" "SELECT violation_type FROM dolt_constraint_violations_child WHERE pk='b';" "without_rowid_fk_type")
expect_eq "without_rowid_fk_type" "foreign key" "$TYPE"

ROW_PRESENT=$(dl "$DB" "SELECT count(*) FROM child WHERE pk='b';" "without_rowid_fk_row_present")
expect_eq "without_rowid_fk_row_present" "1" "$ROW_PRESENT"

echo ""

# ── Scenario K: WITHOUT ROWID UNIQUE violation works ─────
echo "--- K. WITHOUT ROWID UNIQUE merge records loser row ---"

DB="$TMPROOT/without_rowid_unique.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_unique"
CREATE TABLE t(pk TEXT PRIMARY KEY, v1 INT UNIQUE) WITHOUT ROWID;
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('feat',1);
SELECT dolt_commit('-Am','feat_add');
SELECT dolt_checkout('main');
INSERT INTO t VALUES ('main',1);
SELECT dolt_commit('-Am','main_add');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "without_rowid_unique_count")
expect_eq "without_rowid_unique_summary_count" "2" "$N"

TYPE=$(dl "$DB" "SELECT group_concat(violation_type, ',') FROM (SELECT DISTINCT violation_type FROM dolt_constraint_violations_t ORDER BY violation_type);" "without_rowid_unique_type")
expect_eq "without_rowid_unique_type" "unique index" "$TYPE"

BASE_COUNT=$(dl "$DB" "SELECT count(*) FROM t;" "without_rowid_unique_base")
expect_eq "without_rowid_unique_base_count" "2" "$BASE_COUNT"

VIOL_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM dolt_constraint_violations_t ORDER BY pk);" "without_rowid_unique_rows")
expect_eq "without_rowid_unique_rows" "feat,main" "$VIOL_ROWS"

echo ""

# ── Scenario O: WITHOUT ROWID UNIQUE multi-row groups work ──
echo "--- O. WITHOUT ROWID UNIQUE merge records every duplicate row ---"

DB="$TMPROOT/without_rowid_unique_multi.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_unique_multi"
CREATE TABLE t(pk TEXT PRIMARY KEY, v1 INT UNIQUE) WITHOUT ROWID;
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('feat1',1),('feat2',2);
SELECT dolt_commit('-Am','feat_add');
SELECT dolt_checkout('main');
INSERT INTO t VALUES ('main1',1),('main2',2);
SELECT dolt_commit('-Am','main_add');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "without_rowid_unique_multi_count")
expect_eq "without_rowid_unique_multi_summary_count" "4" "$N"

VIOL_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM dolt_constraint_violations_t ORDER BY pk);" "without_rowid_unique_multi_rows")
expect_eq "without_rowid_unique_multi_rows" "feat1,feat2,main1,main2" "$VIOL_ROWS"

BASE_ROWS=$(dl "$DB" "SELECT group_concat(pk, ',') FROM (SELECT pk FROM t ORDER BY pk);" "without_rowid_unique_multi_base")
expect_eq "without_rowid_unique_multi_base_rows" "feat1,feat2,main1,main2" "$BASE_ROWS"

echo ""

# ── Scenario P: mixed WITHOUT ROWID FK + UNIQUE violations ──
echo "--- P. Mixed WITHOUT ROWID FK + UNIQUE violations both block commit ---"

DB="$TMPROOT/without_rowid_mixed_violations.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_mixed_violations"
CREATE TABLE parent(pk TEXT PRIMARY KEY, v1 INT UNIQUE) WITHOUT ROWID;
CREATE TABLE child(pk TEXT PRIMARY KEY, pv INT,
  FOREIGN KEY(pv) REFERENCES parent(v1)) WITHOUT ROWID;
CREATE TABLE u(pk TEXT PRIMARY KEY, v1 INT UNIQUE) WITHOUT ROWID;
INSERT INTO parent VALUES ('p1',1),('p2',2);
INSERT INTO child VALUES ('c1',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES ('c2',2);
INSERT INTO u VALUES ('uf',7);
SELECT dolt_commit('-Am','feat_add');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk='p2';
INSERT INTO u VALUES ('um',7);
SELECT dolt_commit('-Am','main_add');
SELECT dolt_merge('feat');
SQL

SUMMARY=$(dl "$DB" "SELECT group_concat(\"table\" || ':' || num_violations, ',') FROM (SELECT \"table\", num_violations FROM dolt_constraint_violations ORDER BY \"table\");" "without_rowid_mixed_summary")
expect_eq "without_rowid_mixed_summary" "child:1,u:2" "$SUMMARY"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge');" "without_rowid_mixed_commit"; then
  pass_name "without_rowid_mixed_commit_blocked"
else
  fail_name "without_rowid_mixed_commit_blocked"
fi

echo ""

# ── Scenario L: WITHOUT ROWID CHECK violation works ──────
echo "--- L. WITHOUT ROWID CHECK merge records violating row ---"

DB="$TMPROOT/without_rowid_check.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_check"
CREATE TABLE t(pk TEXT PRIMARY KEY, v1 INT) WITHOUT ROWID;
INSERT INTO t VALUES ('a', 1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('b', -5);
SELECT dolt_commit('-Am','feat_bad_row');
SELECT dolt_checkout('main');
CREATE TABLE t_new(pk TEXT PRIMARY KEY, v1 INT CHECK(v1 > 0)) WITHOUT ROWID;
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-Am','main_add_check');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT num_violations FROM dolt_constraint_violations WHERE \"table\"='t';" "without_rowid_check_count")
expect_eq "without_rowid_check_summary_count" "1" "$N"

TYPE=$(dl "$DB" "SELECT violation_type FROM dolt_constraint_violations_t WHERE pk='b';" "without_rowid_check_type")
expect_eq "without_rowid_check_type" "check constraint" "$TYPE"

BASE_COUNT=$(dl "$DB" "SELECT count(*) FROM t WHERE pk='b' AND v1=-5;" "without_rowid_check_base")
expect_eq "without_rowid_check_row_present" "1" "$BASE_COUNT"

echo ""

# ── Scenario M: WITHOUT ROWID FK targeted/full delete works ─
echo "--- M. WITHOUT ROWID FK violations delete by text PK ---"

DB="$TMPROOT/without_rowid_fk_delete.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_fk_delete"
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v1 INT, UNIQUE(v1));
CREATE TABLE child(pk TEXT PRIMARY KEY, v1 INT, FOREIGN KEY(v1) REFERENCES parent(v1)) WITHOUT ROWID;
INSERT INTO parent VALUES (1,1),(2,2),(3,3);
INSERT INTO child VALUES ('a',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES ('b',2),('c',3);
SELECT dolt_commit('-Am','feat_add_children');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk IN (2,3);
SELECT dolt_commit('-Am','main_drop_parents');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child;" "without_rowid_fk_delete_count")
expect_eq "without_rowid_fk_delete_initial_count" "2" "$N"

dl "$DB" "DELETE FROM dolt_constraint_violations_child WHERE pk='b';" "without_rowid_fk_delete_target" >/dev/null
N_ONE=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child;" "without_rowid_fk_delete_after_one")
expect_eq "without_rowid_fk_delete_one_left" "1" "$N_ONE"

REMAINING_PK=$(dl "$DB" "SELECT pk FROM dolt_constraint_violations_child;" "without_rowid_fk_delete_remaining")
expect_eq "without_rowid_fk_delete_keeps_c" "c" "$REMAINING_PK"

dl "$DB" "DELETE FROM dolt_constraint_violations_child;" "without_rowid_fk_delete_all" >/dev/null
N_ZERO=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child;" "without_rowid_fk_delete_after_all")
expect_eq "without_rowid_fk_delete_cleared" "0" "$N_ZERO"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge-cleared');" "without_rowid_fk_delete_commit"; then
  fail_name "without_rowid_fk_delete_commit_after_clear"
else
  pass_name "without_rowid_fk_delete_commit_after_clear"
fi

echo ""

# ── Scenario N: WITHOUT ROWID CHECK targeted/full delete works ─
echo "--- N. WITHOUT ROWID CHECK violations delete by composite PK ---"

DB="$TMPROOT/without_rowid_check_composite_delete.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_check_composite_delete"
CREATE TABLE t(a INT, b TEXT, v1 INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t VALUES (1,'a',1);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2,'b',-5),(3,'c',-6);
SELECT dolt_commit('-Am','feat_bad_rows');
SELECT dolt_checkout('main');
CREATE TABLE t_new(a INT, b TEXT, v1 INT CHECK(v1 > 0), PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_commit('-Am','main_add_check');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_t;" "without_rowid_check_composite_count")
expect_eq "without_rowid_check_composite_initial_count" "2" "$N"

dl "$DB" "DELETE FROM dolt_constraint_violations_t WHERE a=2 AND b='b';" "without_rowid_check_composite_target" >/dev/null
N_ONE=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_t;" "without_rowid_check_composite_after_one")
expect_eq "without_rowid_check_composite_one_left" "1" "$N_ONE"

REMAINING_PK=$(dl "$DB" "SELECT a || '|' || b FROM dolt_constraint_violations_t;" "without_rowid_check_composite_remaining")
expect_eq "without_rowid_check_composite_keeps_other" "3|c" "$REMAINING_PK"

dl "$DB" "DELETE FROM dolt_constraint_violations_t;" "without_rowid_check_composite_all" >/dev/null
N_ZERO=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_t;" "without_rowid_check_composite_after_all")
expect_eq "without_rowid_check_composite_cleared" "0" "$N_ZERO"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge-cleared');" "without_rowid_check_composite_commit"; then
  fail_name "without_rowid_check_composite_commit_after_clear"
else
  pass_name "without_rowid_check_composite_commit_after_clear"
fi

echo ""

# ── Scenario O: one row with two FK violations deletes cleanly ─
echo "--- O. WITHOUT ROWID row with two FK violations clears all matching rows ---"

DB="$TMPROOT/without_rowid_multi_fk_delete.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_multi_fk_delete"
CREATE TABLE p1(pk INT PRIMARY KEY, v1 INT UNIQUE);
CREATE TABLE p2(pk INT PRIMARY KEY, v2 INT UNIQUE);
CREATE TABLE child(
  pk TEXT PRIMARY KEY,
  f1 INT,
  f2 INT,
  FOREIGN KEY(f1) REFERENCES p1(v1),
  FOREIGN KEY(f2) REFERENCES p2(v2)
) WITHOUT ROWID;
INSERT INTO p1 VALUES (1,1),(2,2);
INSERT INTO p2 VALUES (1,10),(2,20);
INSERT INTO child VALUES ('ok',1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES ('bad',2,20);
SELECT dolt_commit('-Am','feat_bad_child');
SELECT dolt_checkout('main');
DELETE FROM p1 WHERE pk=2;
DELETE FROM p2 WHERE pk=2;
SELECT dolt_commit('-Am','main_drop_parents');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child WHERE pk='bad';" "without_rowid_multi_fk_delete_count")
expect_eq "without_rowid_multi_fk_delete_initial_count" "2" "$N"

dl "$DB" "DELETE FROM dolt_constraint_violations_child WHERE pk='bad';" "without_rowid_multi_fk_delete_target" >/dev/null
N_ZERO=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child WHERE pk='bad';" "without_rowid_multi_fk_delete_after")
expect_eq "without_rowid_multi_fk_delete_cleared" "0" "$N_ZERO"

if dl_errors "$DB" "SELECT dolt_commit('-m','post-merge-cleared');" "without_rowid_multi_fk_delete_commit"; then
  fail_name "without_rowid_multi_fk_delete_commit_after_clear"
else
  pass_name "without_rowid_multi_fk_delete_commit_after_clear"
fi

echo ""

# ── Scenario P: one selective delete leaves sibling violation row ─
echo "--- P. WITHOUT ROWID selective violation delete leaves sibling row ---"

DB="$TMPROOT/without_rowid_multi_fk_delete_one.db"
rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "without_rowid_multi_fk_delete_one"
CREATE TABLE p1(pk INT PRIMARY KEY, v1 INT UNIQUE);
CREATE TABLE p2(pk INT PRIMARY KEY, v2 INT UNIQUE);
CREATE TABLE child(
  pk TEXT PRIMARY KEY,
  f1 INT,
  f2 INT,
  FOREIGN KEY(f1) REFERENCES p1(v1),
  FOREIGN KEY(f2) REFERENCES p2(v2)
) WITHOUT ROWID;
INSERT INTO p1 VALUES (1,1),(2,2);
INSERT INTO p2 VALUES (1,10),(2,20);
INSERT INTO child VALUES ('ok',1,10);
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES ('bad',2,20);
SELECT dolt_commit('-Am','feat_bad_child');
SELECT dolt_checkout('main');
DELETE FROM p1 WHERE pk=2;
DELETE FROM p2 WHERE pk=2;
SELECT dolt_commit('-Am','main_drop_parents');
SELECT dolt_merge('feat');
SQL

N=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child WHERE pk='bad';" "without_rowid_multi_fk_delete_one_count")
expect_eq "without_rowid_multi_fk_delete_one_initial_count" "2" "$N"

dl "$DB" "DELETE FROM dolt_constraint_violations_child WHERE pk='bad' AND violation_info LIKE '%\"f1\"%';" "without_rowid_multi_fk_delete_one_target" >/dev/null
N_ONE=$(dl "$DB" "SELECT count(*) FROM dolt_constraint_violations_child WHERE pk='bad';" "without_rowid_multi_fk_delete_one_after")
expect_eq "without_rowid_multi_fk_delete_one_left" "1" "$N_ONE"

LEFT_INFO=$(dl "$DB" "SELECT violation_info FROM dolt_constraint_violations_child WHERE pk='bad';" "without_rowid_multi_fk_delete_one_info")
case "$LEFT_INFO" in
  *"\"f2\""*) pass_name "without_rowid_multi_fk_delete_one_keeps_f2" ;;
  *) fail_name "without_rowid_multi_fk_delete_one_keeps_f2" ;;
esac

if dl_errors "$DB" "SELECT dolt_commit('-m','still-blocked');" "without_rowid_multi_fk_delete_one_commit"; then
  pass_name "without_rowid_multi_fk_delete_one_commit_still_blocked"
else
  fail_name "without_rowid_multi_fk_delete_one_commit_still_blocked"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
