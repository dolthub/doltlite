#!/bin/bash
#
# Oracle test: savepoints, rollbacks, and transaction boundaries
# against stock SQLite.
#
# Savepoints are a dense corner of the prolly btree layer: each
# SAVEPOINT snapshots the in-memory catalog and the per-table
# pending mutmap state; ROLLBACK TO walks the stack to revert
# both. Any mismatch between snapshot and restore surfaces as a
# row that shouldn't exist or is missing after the rollback.
#
# Oracle target is stock sqlite3 built from the same tree so both
# engines parse identical SQLite SAVEPOINT syntax and the only
# variable is the storage layer's unwind logic.
#
# Coverage:
#   - BEGIN / COMMIT / ROLLBACK (full-transaction semantics)
#   - SAVEPOINT / RELEASE / ROLLBACK TO (partial revert)
#   - Nested savepoints (release-inner, rollback-inner, rollback-middle)
#   - Same-name shadowing (innermost wins)
#   - Savepoint over multi-table writes
#   - Savepoint over UPDATE and DELETE (not just INSERT)
#   - Savepoint over CREATE / DROP / ALTER TABLE
#   - Savepoint over triggered side effects (trigger body writes
#     to a second table; rollback has to revert both)
#   - In-place mutmap mutation transitions (INSERT→DELETE,
#     DELETE→INSERT, chained UPDATEs, UPSERT, REPLACE INTO, etc.)
#   - Bulk savepoint rollbacks (100/1000 rows)
#   - Savepoint-inside-BEGIN interaction
#   - Trigger RAISE(ROLLBACK) unwinding nested savepoints
#
# Known disabled scenarios (marked DISABLED inline with the
# repro and a pointer to the underlying bug):
#
#   - rollback_undoes_alter_rename,
#     rollback_undoes_alter_rename_column — pre-existing SIGSEGV
#     in the ALTER TABLE RENAME + ROLLBACK TO interaction
#     (reproduces on master). Crash lands in
#     sqlite3BtreeTripAllCursors before the savepoint restore
#     code. Filed as a separate follow-up.
#
# Usage: bash oracle_savepoints_test.sh [path/to/doltlite] [path/to/sqlite3]
#

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  # Normalize shell error prefixes: stock sqlite3 prints
  # "Runtime error near line N: msg (code)" while doltlite prints
  # "Error near line N: msg". Drop the leading "Runtime " token and
  # the trailing " (NN)" code so the two forms compare equal —
  # savepoint correctness is about row outcomes, not shell prose.
  tr -d '\r' \
    | sed -e 's/[[:space:]]\{1,\}/ /g' -e 's/^ //' -e 's/ $//' \
          -e 's/^Runtime error /Error /' \
          -e 's/^Error: in prepare, / /' \
          -e 's/ ([0-9]*)$//'
}

oracle() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/sq"

  local dl_out
  dl_out=$(printf '%s\n' "$sql" | "$DOLTLITE" "$dir/dl/db" 2>&1 | normalize)

  local sq_out
  sq_out=$(printf '%s\n' "$sql" | "$SQLITE3" "$dir/sq/db" 2>&1 | normalize)

  if [ "$dl_out" = "$sq_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    sqlite3:";  echo "$sq_out" | sed 's/^/      /'
  fi
}

echo "=== Oracle Tests: savepoints + rollbacks ==="
echo ""

# ─── Basic transactions ──────────────────────────────────────────────
echo "--- basic transactions ---"

oracle "txn_commit_visible" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_all" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
BEGIN;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_update" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
BEGIN;
UPDATE t SET v = v * 10;
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_delete" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
BEGIN;
DELETE FROM t WHERE id >= 2;
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

oracle "txn_rollback_reverts_multi_table" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, v INT);
INSERT INTO a VALUES(1, 10);
INSERT INTO b VALUES(1, 100);
BEGIN;
INSERT INTO a VALUES(2, 20);
INSERT INTO b VALUES(2, 200);
UPDATE a SET v = 999 WHERE id = 1;
DELETE FROM b WHERE id = 1;
ROLLBACK;
SELECT 'a', id, v FROM a ORDER BY id;
SELECT 'b', id, v FROM b ORDER BY id;
"

# ─── Single savepoint operations ─────────────────────────────────────
echo "--- single savepoint ---"

oracle "savepoint_release_keeps_changes" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_to_reverts" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# After ROLLBACK TO the savepoint is still active; a second batch
# of writes then RELEASE should leave only the second batch.
oracle "savepoint_rollback_then_reuse" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t VALUES(3, 30);
INSERT INTO t VALUES(4, 40);
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Savepoint over an UPDATE that overwrites existing rows — the
# rollback has to restore the original values, not just discard
# the staged edit.
oracle "savepoint_rollback_update_restores" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
SAVEPOINT s1;
UPDATE t SET v = v + 1000;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Savepoint over a DELETE — rollback has to restore deleted rows.
oracle "savepoint_rollback_delete_restores" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
SAVEPOINT s1;
DELETE FROM t WHERE id >= 2;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Mixed DML inside a savepoint.
oracle "savepoint_rollback_mixed_dml" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20),(3, 30);
SAVEPOINT s1;
INSERT INTO t VALUES(4, 40);
UPDATE t SET v = v + 100 WHERE id = 2;
DELETE FROM t WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# ─── Nested savepoints ───────────────────────────────────────────────
echo "--- nested savepoints ---"

# Two nested savepoints, release inner-only: outer state is the
# union of both batches.
oracle "nested_release_inner_only" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
RELEASE SAVEPOINT s2;
INSERT INTO t VALUES(3, 30);
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Rollback inner only: rows inside s2 gone, rows before s2 kept.
oracle "nested_rollback_inner_only" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Rollback outer: rows from BOTH savepoints gone, even though the
# inner was released before the outer rollback fired.
oracle "nested_rollback_outer_discards_inner" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
RELEASE SAVEPOINT s2;
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Three-deep nesting, rollback to middle. Everything inside s3 is
# reverted, but the s1→s2 writes are kept.
oracle "three_deep_rollback_to_middle" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 20);
SAVEPOINT s3;
INSERT INTO t VALUES(3, 30);
INSERT INTO t VALUES(4, 40);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Same-name savepoint shadowing: ROLLBACK TO goes to the innermost
# savepoint with that name (LIFO).
oracle "savepoint_same_name_shadowing" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, v FROM t ORDER BY id;
"

# ─── Multi-table savepoints ──────────────────────────────────────────
echo "--- multi-table savepoints ---"

oracle "savepoint_rollback_multi_table" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, v INT);
INSERT INTO a VALUES(1, 10);
INSERT INTO b VALUES(1, 100);
SAVEPOINT s1;
INSERT INTO a VALUES(2, 20);
INSERT INTO b VALUES(2, 200);
UPDATE a SET v = 999 WHERE id = 1;
DELETE FROM b WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT 'a', id, v FROM a ORDER BY id;
SELECT 'b', id, v FROM b ORDER BY id;
"

# Three tables, inner rollback over an UPDATE that targets a
# pending INSERT from the OUTER savepoint. Without the lazy
# undo-log fix in this PR, prollyMutMapInsert overwrites the
# existing mutmap entry in place and the savepoint count-based
# truncate can't undo it.
oracle "savepoint_three_tables_inner_rollback" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, v INT);
CREATE TABLE c(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO a VALUES(1, 1);
INSERT INTO b VALUES(1, 11);
SAVEPOINT s2;
INSERT INTO c VALUES(1, 111);
UPDATE a SET v = 999 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT 'a', id, v FROM a ORDER BY id;
SELECT 'b', id, v FROM b ORDER BY id;
SELECT 'c', id, v FROM c ORDER BY id;
"

# ─── In-place mutmap mutation transitions ──────────────────────────
#
# Each of these scenarios drives a specific transition through
# the in-place-mutating paths of prolly_mutmap.c and verifies the
# savepoint snapshot reverts it correctly. The fixed savepoint
# code stores a full mutmap clone, so any transition between
# (op, value) states should round-trip through ROLLBACK TO. If a
# new in-place mutation path is added later that doesn't go
# through prollyMutMapInsert / prollyMutMapDelete, one of these
# is the canary that will catch it.

echo "--- in-place mutation transitions ---"

# Transition: (INSERT, v0) → DELETE.
# Outer savepoint buffers an INSERT. Inner savepoint takes a
# snapshot. DELETE flips the mutmap entry's op from INSERT to
# DELETE and frees the value. ROLLBACK TO must restore the
# original (INSERT, v0).
oracle "rollback_undoes_insert_then_delete" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
DELETE FROM t WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

# Transition: (INSERT, v0) → DELETE → INSERT, v1.
# Two in-place mutations on top of the same savepointed entry,
# rolled back. Final state must be (INSERT, v0).
oracle "rollback_undoes_insert_delete_insert" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 999);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

# Transition: (DELETE) → INSERT, resurrecting a row that was
# committed to the tree before the savepoint. Inner savepoint
# snapshots a (DELETE) mutmap entry; the resurrect-INSERT mutates
# it back to (INSERT, v_new). Rollback must restore the (DELETE)
# state — i.e., the row from the tree must NOT reappear.
oracle "rollback_undoes_delete_then_resurrect" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
DELETE FROM t WHERE id = 1;
SAVEPOINT s2;
INSERT INTO t VALUES(1, 999);
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
SELECT 'after_rollback', count(*) FROM t;
RELEASE SAVEPOINT s1;
SELECT 'after_outer_release', id, v FROM t;
"

# Transition: (INSERT, v0) → UPDATE → UPDATE → UPDATE.
# Three in-place value mutations on the same entry. The savepoint
# snapshot captured (INSERT, v0). Rollback must skip past all
# three intermediate values.
oracle "rollback_undoes_chained_updates" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
UPDATE t SET v = 100 WHERE id = 1;
UPDATE t SET v = 200 WHERE id = 1;
UPDATE t SET v = 300 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

# UPSERT: INSERT INTO ... ON CONFLICT DO UPDATE goes through one
# statement that does insert-or-overwrite via the mutmap. Run it
# inside a savepoint after a row already exists, then roll back.
oracle "rollback_undoes_upsert_overwrite" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 999) ON CONFLICT(id) DO UPDATE SET v = 999;
INSERT INTO t VALUES(2, 20) ON CONFLICT(id) DO UPDATE SET v = excluded.v;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# UPSERT inside an inner savepoint, on a row that the OUTER
# savepoint just inserted (so the mutmap entry exists from outer
# scope). Rolls back past the upsert.
oracle "rollback_undoes_upsert_on_pending_insert" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s2;
INSERT INTO t VALUES(1, 999) ON CONFLICT(id) DO UPDATE SET v = 999;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

# REPLACE INTO is INSERT OR REPLACE — does delete-then-insert
# under the hood for PK conflicts. Inside a savepoint, on a row
# from the tree, then rollback.
oracle "rollback_undoes_replace_into" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
REPLACE INTO t VALUES(1, 999);
REPLACE INTO t VALUES(2, 22);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# UPDATE inside savepoint where the target row was inserted in
# the SAME savepoint — the mutmap entry was created by this
# savepoint and is still subject to its own rollback. The
# snapshot at the savepoint had no entry for this key; on
# rollback, the entry must vanish entirely (not just have its
# pre-update value restored).
oracle "rollback_drops_insert_then_update_in_same_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
UPDATE t SET v = 999 WHERE id = 2;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# Trigger that does UPDATE on a row in another table, where that
# other table has a pending INSERT from the same outer savepoint.
# The trigger-driven UPDATE mutates the mutmap in place. Rollback
# must revert both the trigger-side row and the rest.
oracle "rollback_undoes_trigger_update_on_pending" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE other(id INT PRIMARY KEY, v INT);
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN
  UPDATE other SET v = v + 1000 WHERE id = new.id;
END;
SAVEPOINT s1;
INSERT INTO t     VALUES(1, 10);
INSERT INTO other VALUES(1, 100);
SAVEPOINT s2;
UPDATE t SET v = 99 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT 't', id, v FROM t ORDER BY id;
SELECT 'o', id, v FROM other ORDER BY id;
"

# Nested savepoints with mutations at each level. Rollback to the
# OUTERMOST savepoint must walk all three levels of in-place
# mutations and end up at the pre-savepoint state.
oracle "deep_nesting_rollback_to_outer" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1);
SAVEPOINT s1;
UPDATE t SET v = 10 WHERE id = 1;
SAVEPOINT s2;
UPDATE t SET v = 100 WHERE id = 1;
SAVEPOINT s3;
UPDATE t SET v = 1000 WHERE id = 1;
SAVEPOINT s4;
UPDATE t SET v = 10000 WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t;
"

# Nested savepoints where each level inserts a different new row
# AND mutates a row from above. Rollback to s2 should keep s1's
# work and discard everything s3 and below.
oracle "nested_inserts_and_updates_rollback" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
SAVEPOINT s1;
INSERT INTO t VALUES(1, 1);
SAVEPOINT s2;
INSERT INTO t VALUES(2, 2);
UPDATE t SET v = 11 WHERE id = 1;
SAVEPOINT s3;
INSERT INTO t VALUES(3, 3);
UPDATE t SET v = 22 WHERE id = 2;
UPDATE t SET v = 111 WHERE id = 1;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s2;
RELEASE SAVEPOINT s1;
SELECT id, v FROM t ORDER BY id;
"

# ─── Schema state shallow-copy edge cases ──────────────────────────
#
# DISABLED: ALTER TABLE RENAME [COLUMN] inside a savepoint followed
# by ROLLBACK TO SAVEPOINT crashes doltlite with SIGSEGV. This
# reproduces on master (without the savepoint snapshot deep-copy
# fix in this PR), so it is a pre-existing crash in the rename +
# rollback interaction, NOT a regression and NOT addressed by the
# mutmap-snapshot fix here. The crash backtrace lands inside
# sqlite3BtreeTripAllCursors which runs BEFORE the savepoint
# restore code, suggesting the rename leaves a cursor-list /
# catalog state inconsistent that the rollback path then walks.
#
# Repro:
#
#   CREATE TABLE t(id INT PRIMARY KEY, v INT);
#   INSERT INTO t VALUES(1, 10);
#   SAVEPOINT s1;
#   ALTER TABLE t RENAME TO renamed_t;
#   ROLLBACK TO SAVEPOINT s1;            -- SIGSEGV here
#
# Filed as a follow-up issue. Re-enable when the rename interaction
# is fixed.
#
# oracle "rollback_undoes_alter_rename" "
# CREATE TABLE t(id INT PRIMARY KEY, v INT);
# INSERT INTO t VALUES(1, 10);
# SAVEPOINT s1;
# ALTER TABLE t RENAME TO renamed_t;
# ROLLBACK TO SAVEPOINT s1;
# RELEASE SAVEPOINT s1;
# SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
# SELECT id, v FROM t;
# "
#
# oracle "rollback_undoes_alter_rename_column" "
# CREATE TABLE t(id INT PRIMARY KEY, v INT);
# INSERT INTO t VALUES(1, 10);
# SAVEPOINT s1;
# ALTER TABLE t RENAME COLUMN v TO val;
# ROLLBACK TO SAVEPOINT s1;
# RELEASE SAVEPOINT s1;
# SELECT sql FROM sqlite_master WHERE type='table' AND name='t';
# SELECT id, v FROM t;
# "

# CREATE INDEX inside a savepoint. Indexes are sqlite_master rows
# with their own iTable / root. Rollback must remove both the
# schema row and the catalog table entry. Scoped to the user-created
# index name — doltlite and stock SQLite differ on how
# sqlite_autoindex_* entries are exposed in sqlite_master for PK
# columns, which is orthogonal to savepoint rollback correctness.
oracle "rollback_undoes_create_index" "
CREATE TABLE t(id INT PRIMARY KEY, v INT, tag TEXT);
INSERT INTO t VALUES(1, 10, 'a'),(2, 20, 'b');
SAVEPOINT s1;
CREATE INDEX idx_t_tag ON t(tag);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='index' AND name='idx_t_tag';
SELECT id, v, tag FROM t ORDER BY id;
"

# ─── Savepoint + schema changes ──────────────────────────────────────
echo "--- savepoint + schema ---"

oracle "savepoint_rollback_create_table" "
CREATE TABLE t(id INT PRIMARY KEY);
INSERT INTO t VALUES(1);
SAVEPOINT s1;
CREATE TABLE u(id INT PRIMARY KEY, v TEXT);
INSERT INTO u VALUES(1, 'x');
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
SELECT id FROM t ORDER BY id;
"

oracle "savepoint_rollback_drop_table" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 10),(2, 20);
INSERT INTO u VALUES(1, 'a'),(2, 'b');
SAVEPOINT s1;
DROP TABLE u;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
SELECT id, v FROM u ORDER BY id;
"

oracle "savepoint_rollback_add_column" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10),(2, 20);
SAVEPOINT s1;
ALTER TABLE t ADD COLUMN extra TEXT;
UPDATE t SET extra = 'filled' WHERE id = 1;
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT sql FROM sqlite_master WHERE type='table' AND name='t';
SELECT id, v FROM t ORDER BY id;
"

# ─── Savepoint + triggers ────────────────────────────────────────────
echo "--- savepoint + triggers ---"

# Trigger fires during an INSERT inside a savepoint; the trigger
# writes to a second table. Rolling back to the savepoint must
# revert BOTH tables.
oracle "savepoint_rollback_reverts_trigger_writes" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT, v INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(new.id, new.v);
END;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT 't', id, v FROM t ORDER BY id;
SELECT 'log', id, v FROM log ORDER BY id;
"

# Trigger RAISE(ROLLBACK) inside a nested savepoint — SQLite's
# semantics say RAISE(ROLLBACK) aborts the full transaction, NOT
# just the nearest savepoint. Both engines should revert the
# pre-savepoint writes too.
oracle "trigger_raise_rollback_through_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TRIGGER no_neg BEFORE INSERT ON t WHEN new.v < 0 BEGIN
  SELECT RAISE(ROLLBACK, 'no neg');
END;
BEGIN;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, -1);
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

# ─── Bulk savepoint rollbacks ────────────────────────────────────────
echo "--- bulk ---"

# 100 rows inside a savepoint, rolled back. Stresses the mutmap
# snapshot/restore rather than individual row paths.
make_bulk_insert() {
  local n="$1" tbl="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO $tbl VALUES($i, 'row_$i');"
  done
}

BULK_100="$(make_bulk_insert 100 t)"
BULK_1K="$(make_bulk_insert 1000 t)"

oracle "savepoint_rollback_100_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0, 'seed');
SAVEPOINT s1;
$BULK_100
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT id, v FROM t ORDER BY id;
"

oracle "savepoint_rollback_1000_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0, 'seed');
SAVEPOINT s1;
$BULK_1K
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT id FROM t;
"

# 1k rows inside a savepoint, RELEASED (not rolled back). All rows
# should land in the tree.
oracle "savepoint_release_1000_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
SAVEPOINT s1;
$BULK_1K
RELEASE SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT id FROM t WHERE id IN (1, 250, 500, 750, 1000) ORDER BY id;
"

# ─── Savepoint inside an explicit transaction ────────────────────────
echo "--- savepoint inside BEGIN ---"

# BEGIN → INSERT → SAVEPOINT → more inserts → RELEASE → COMMIT.
# Straightforward.
oracle "begin_savepoint_release_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
RELEASE SAVEPOINT s1;
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

# BEGIN → INSERT → SAVEPOINT → insert → ROLLBACK TO → COMMIT.
# Outer transaction keeps only the pre-savepoint write.
oracle "begin_savepoint_rollback_to_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
BEGIN;
INSERT INTO t VALUES(1, 10);
SAVEPOINT s1;
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
ROLLBACK TO SAVEPOINT s1;
RELEASE SAVEPOINT s1;
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

# BEGIN → everything → ROLLBACK. Outer rollback discards all
# savepoints and their effects, including released ones.
oracle "begin_rollback_discards_everything" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
BEGIN;
INSERT INTO t VALUES(2, 20);
SAVEPOINT s1;
INSERT INTO t VALUES(3, 30);
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO t VALUES(4, 40);
RELEASE SAVEPOINT s2;
ROLLBACK;
SELECT id, v FROM t ORDER BY id;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
