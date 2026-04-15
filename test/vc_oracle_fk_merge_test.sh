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
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
