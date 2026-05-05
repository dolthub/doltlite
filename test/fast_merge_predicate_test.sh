#!/bin/bash
#
# Fast-merge activation predicate test.
#
# The predicate (fastMergeIneligibleReason in src/doltlite_merge.c)
# decides whether a table-level merge can take the tree-walking
# fast path. This commit only observes the decision via a stderr
# trace gated on DOLTLITE_FAST_MERGE_DEBUG=1; later commits add the
# actual fast path that consults it.
#
# Each scenario runs a merge and asserts that the trace says either
# "eligible" or "ineligible: <reason>" for the data table. Schema
# tables and secondary-index catalog entries are skipped by the
# logger so they don't pollute the trace.
#
# Usage: bash fast_merge_predicate_test.sh [path/to/doltlite]

set -u
DOLTLITE="${1:-./doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
PASS=0; FAIL=0; ERRORS=""

# Run a merge scenario; capture stderr. $1=label, $2=expected trace
# substring, $3=setup SQL.
run_scenario() {
  local label="$1" expected="$2" sql="$3"
  local db="$TMPDIR/$label.db"
  rm -f "$db"
  local stderr
  stderr=$(echo "$sql" | DOLTLITE_FAST_MERGE_DEBUG=1 "$DOLTLITE" "$db" 2>&1 >/dev/null)
  if echo "$stderr" | grep -qF "$expected"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $label\n  expected trace: $expected\n  stderr: $stderr"
  fi
}

# Boilerplate: create a table, branch, diverge both sides, merge.
# $1 = extra DDL inserted into the CREATE TABLE, $2 = extra
# statements before INSERT.
make_setup() {
  local extra_ddl="$1" extra_stmt="$2"
  cat <<EOF
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT$extra_ddl);
$extra_stmt
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2, 'b');
SELECT dolt_commit('-A','-m','main2');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3, 'c');
SELECT dolt_commit('-A','-m','feat3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
EOF
}

echo "=== Fast-Merge Predicate Tests ==="
echo ""

# 1. Simple table, no constraints, no indexes → eligible.
run_scenario "simple_eligible" \
  "fast_merge: eligible 't'" \
  "$(make_setup '' '')"

# 2. Secondary index → ineligible (secondary_index).
run_scenario "with_secondary_index" \
  "fast_merge: ineligible 't': secondary_index" \
  "$(make_setup '' "CREATE INDEX i_v ON t(v);")"

# 3. CHECK constraint → ineligible (check_constraint).
run_scenario "with_check" \
  "fast_merge: ineligible 't': check_constraint" \
  "$(make_setup ", CHECK(v != 'bad')" '')"

# 4. FK CASCADE on the child → ineligible (fk_action_child).
#    Mutate child on both sides so the merge driver enters its
#    per-table merge arm.
FK_CHILD_SETUP="
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  v TEXT,
  FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE
);
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1, 1, 'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO child VALUES(2, 1, 'b');
SELECT dolt_commit('-A','-m','main2');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES(3, 2, 'c');
SELECT dolt_commit('-A','-m','feat3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_scenario "with_fk_cascade_child" \
  "fast_merge: ineligible 'child': fk_action_child" \
  "$FK_CHILD_SETUP"

# 5. The parent table referenced by a CASCADE child → ineligible
#    (fk_action_parent). Mutate parent on both sides so it enters
#    the per-table merge arm.
FK_PARENT_SETUP="
CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);
CREATE TABLE child(
  id INTEGER PRIMARY KEY,
  pid INTEGER,
  FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE
);
INSERT INTO parent VALUES(1, 'init');
INSERT INTO child VALUES(1, 1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
INSERT INTO parent VALUES(2, 'main_parent');
SELECT dolt_commit('-A','-m','main_parent_change');
SELECT dolt_checkout('feat');
INSERT INTO parent VALUES(3, 'feat_parent');
SELECT dolt_commit('-A','-m','feat_parent_change');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_scenario "with_fk_cascade_parent" \
  "fast_merge: ineligible 'parent': fk_action_parent" \
  "$FK_PARENT_SETUP"

# 6. DOLTLITE_FORCE_ROW_MERGE env var disables the fast path.
DB="$TMPDIR/force.db"; rm -f "$DB"
FORCE_STDERR=$(make_setup '' '' | DOLTLITE_FAST_MERGE_DEBUG=1 \
  DOLTLITE_FORCE_ROW_MERGE=1 "$DOLTLITE" "$DB" 2>&1 >/dev/null)
if echo "$FORCE_STDERR" | grep -qF "fast_merge: ineligible 't': force_row_merge_env"; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: force_row_merge_env\n  stderr: $FORCE_STDERR"
fi
rm -f "$DB"

# 7. Schema divergence between branches → ineligible (schema_divergence).
SCHEMA_DIV="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
INSERT INTO t VALUES(2, 'b', 'main_extra');
SELECT dolt_commit('-A','-m','main_alter');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3, 'c');
SELECT dolt_commit('-A','-m','feat3');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
"
run_scenario "schema_divergence" \
  "fast_merge: ineligible 't': schema_divergence" \
  "$SCHEMA_DIV"

# 8. Sanity: without DOLTLITE_FAST_MERGE_DEBUG the trace must be silent
#    (no observable behavior for production runs).
DB="$TMPDIR/silent.db"; rm -f "$DB"
SILENT_STDERR=$(make_setup '' '' | "$DOLTLITE" "$DB" 2>&1 >/dev/null)
if echo "$SILENT_STDERR" | grep -q "fast_merge:"; then
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: silent_without_env\n  unexpected stderr: $SILENT_STDERR"
else
  PASS=$((PASS+1))
fi
rm -f "$DB"

echo ""
echo "================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================"
if [ $FAIL -gt 0 ]; then
  printf "%b\n" "$ERRORS"
  exit 1
fi
