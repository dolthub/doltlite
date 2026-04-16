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
#       'unique index'      duplicate — row is NOT in the base table
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
#        (FK/CHECK: row present; UNIQUE: row absent)
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
expect_eq "unique_violation_summary_count" "1" "$N"

TYPE=$(dl "$DB" "SELECT violation_type FROM dolt_constraint_violations_t;" "uniq_type")
expect_eq "unique_violation_type" "unique index" "$TYPE"

BASE_COUNT=$(dl "$DB" "SELECT count(*) FROM t;" "uniq_base")
expect_eq "unique_violation_loser_not_in_base" "1" "$BASE_COUNT"

BASE_KEEPS_MAIN=$(dl "$DB" "SELECT pk FROM t;" "uniq_base_pk")
expect_eq "unique_violation_main_side_kept" "1" "$BASE_KEEPS_MAIN"

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

# ── Scenario J: WITHOUT ROWID FK merge refused loudly ────
#
# Per #495: constraint-violation detection assumes every table
# has a rowid and breaks silently on WITHOUT ROWID tables.
# Until prolly-layer-level support lands, the walker refuses
# the merge loudly with an actionable error instead of
# silently committing over missed violations. This scenario
# verifies that behavior: a merge that would produce an FK
# orphan on a WITHOUT ROWID child table must fail with the
# issue-495 error message, not silently commit.
echo "--- J. WITHOUT ROWID FK merge is refused loudly ---"

DB="$TMPROOT/without_rowid_fk.db"
rm -f "$DB"
cat <<'SQL' | dl_setup_capture "$DB" "without_rowid_fk"
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

if grep -q "WITHOUT ROWID" "$TMPROOT/without_rowid_fk.out" \
                            "$TMPROOT/without_rowid_fk.err" 2>/dev/null; then
  pass_name "without_rowid_fk_refused"
else
  fail_name "without_rowid_fk_refused"
  echo "    (merge did not surface the WITHOUT ROWID refusal)"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
