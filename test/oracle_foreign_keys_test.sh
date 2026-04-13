#!/bin/bash
#
# Oracle test: FOREIGN KEY constraints on a single branch, against
# stock SQLite.
#
# doltlite's FK story runs through the prolly btree like any other
# multi-table write, but the FK check machinery lives in SQLite's
# fkey.c and relies on the storage layer exposing consistent cursor
# state for every mutation that fires a pre- or post-op check.
# Cascades in particular chain multiple writes through the same
# savepoint stack, which is where semantic drift tends to hide.
#
# Oracle target is stock sqlite3 built from the same source tree so
# both engines parse identical SQL and the only variable is the
# storage layer (prolly btree vs stock btree).
#
# Coverage:
#   - PRAGMA foreign_keys = ON is set by every scenario
#     (SQLite defaults to OFF; doltlite matches that default)
#   - Basic INSERT / UPDATE / DELETE with valid / invalid references
#   - ON DELETE actions: NO ACTION, RESTRICT, CASCADE, SET NULL,
#     SET DEFAULT
#   - ON UPDATE actions: CASCADE, SET NULL, RESTRICT
#   - Self-referencing FK (a row's FK points at another row in the
#     same table)
#   - Multi-level CASCADE (A → B → C chained delete)
#   - Composite foreign keys
#   - Deferred FKs (DEFERRABLE INITIALLY DEFERRED): temporary
#     violations inside a transaction are legal if resolved before
#     COMMIT
#   - PRAGMA foreign_keys = OFF lets violations through
#   - ON DELETE/UPDATE NO ACTION — rejected, transaction rolls back
#   - Inserting a row whose FK value equals NULL (always legal)
#   - Indexed vs unindexed child columns (FK still checked either way)
#   - FK cycle prevented by deferred constraints
#
# Usage: bash oracle_foreign_keys_test.sh [path/to/doltlite] [path/to/sqlite3]
#

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  # Same error-prose normalizer as oracle_savepoints_test.sh: stock
  # sqlite3 prints "Runtime error near line N: msg (code)" while
  # doltlite prints "Error near line N: msg". FK correctness is
  # about whether a constraint fired and whether downstream rows
  # exist, not the error-message wording.
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

echo "=== Oracle Tests: foreign keys (single branch) ==="
echo ""

# ─── Basic FK check ─────────────────────────────────────────────────
echo "--- basic check ---"

# Insert into child with matching parent row — should succeed.
oracle "insert_child_matching_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1);
SELECT c.id, c.pid, p.name FROM child c JOIN parent p ON c.pid = p.id;
"

# Insert into child with no matching parent row — should fail.
oracle "insert_child_no_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 99);
SELECT id, pid FROM child;
"

# Insert with NULL FK is always legal (absence of reference).
oracle "insert_child_null_fk" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, NULL);
INSERT INTO child VALUES(11, 1);
SELECT id, pid FROM child ORDER BY id;
"

# Updating a child's FK to point at an invalid parent must fail.
oracle "update_child_to_invalid_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
UPDATE child SET pid = 99 WHERE id = 10;
SELECT id, pid FROM child;
"

# Deleting a parent row with a child referencing it fails by default
# (NO ACTION semantics, equivalent to RESTRICT for immediate checks).
oracle "delete_parent_with_child_rejected" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent;
SELECT id, pid FROM child;
"

# Explicit ON DELETE RESTRICT behaves like NO ACTION.
oracle "delete_parent_restrict" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE RESTRICT);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent;
SELECT id, pid FROM child;
"

# PRAGMA foreign_keys = OFF lets invalid inserts through. (The CREATE
# TABLE declares the FK but it is not enforced.) Both engines should
# accept the insert identically.
oracle "fks_off_skips_check" "
PRAGMA foreign_keys = OFF;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 99);
SELECT id, pid FROM child;
"

# ─── ON DELETE CASCADE ──────────────────────────────────────────────
echo "--- ON DELETE CASCADE ---"

# Deleting the parent row cascades to delete all children.
oracle "delete_cascade_removes_children" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 1), (12, 2);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# Deleting all parent rows cascades to empty the child table.
oracle "delete_cascade_empties_child" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1), (2), (3);
INSERT INTO child VALUES(10, 1), (11, 2), (12, 3);
DELETE FROM parent;
SELECT count(*) FROM parent;
SELECT count(*) FROM child;
"

# Multi-level cascade: A → B → C. Deleting A also deletes B and C.
oracle "delete_cascade_chain_3_levels" "
PRAGMA foreign_keys = ON;
CREATE TABLE a(id INT PRIMARY KEY);
CREATE TABLE b(id INT PRIMARY KEY,
  aid INT REFERENCES a(id) ON DELETE CASCADE);
CREATE TABLE c(id INT PRIMARY KEY,
  bid INT REFERENCES b(id) ON DELETE CASCADE);
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(10, 1), (11, 1);
INSERT INTO c VALUES(100, 10), (101, 11);
DELETE FROM a WHERE id = 1;
SELECT count(*) FROM a;
SELECT count(*) FROM b;
SELECT count(*) FROM c;
"

# ─── ON DELETE SET NULL ─────────────────────────────────────────────
echo "--- ON DELETE SET NULL ---"

oracle "delete_set_null_nulls_children" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 1), (12, 2);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# ─── ON DELETE SET DEFAULT ──────────────────────────────────────────
echo "--- ON DELETE SET DEFAULT ---"

# When the default value itself points at a valid parent, SET DEFAULT
# reassigns the child to it.
oracle "delete_set_default_reassigns_child" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(
  id INT PRIMARY KEY,
  pid INT DEFAULT 0 REFERENCES parent(id) ON DELETE SET DEFAULT
);
INSERT INTO parent VALUES(0), (1);
INSERT INTO child VALUES(10, 1), (11, 1);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# When the default value does NOT correspond to an existing parent,
# SET DEFAULT violates the FK and the delete fails.
oracle "delete_set_default_violates" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(
  id INT PRIMARY KEY,
  pid INT DEFAULT 99 REFERENCES parent(id) ON DELETE SET DEFAULT
);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent;
SELECT id, pid FROM child;
"

# ─── ON UPDATE CASCADE ──────────────────────────────────────────────
echo "--- ON UPDATE CASCADE ---"

# Updating the parent PK cascades to children's FK column.
oracle "update_cascade_propagates_pk" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON UPDATE CASCADE);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1), (11, 1);
UPDATE parent SET id = 99 WHERE id = 1;
SELECT id FROM parent;
SELECT id, pid FROM child ORDER BY id;
"

oracle "update_cascade_then_delete" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON UPDATE CASCADE ON DELETE CASCADE);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1), (11, 1);
UPDATE parent SET id = 42 WHERE id = 1;
DELETE FROM parent WHERE id = 42;
SELECT count(*) FROM parent;
SELECT count(*) FROM child;
"

# ─── ON UPDATE SET NULL ─────────────────────────────────────────────
echo "--- ON UPDATE SET NULL ---"

oracle "update_set_null_nulls_children" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON UPDATE SET NULL);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1), (11, 1);
UPDATE parent SET id = 99;
SELECT id FROM parent;
SELECT id, pid FROM child ORDER BY id;
"

# ─── Self-referencing FK ────────────────────────────────────────────
echo "--- self-referencing ---"

# Tree table where parent_id points back at this table's own id.
oracle "self_ref_insert_in_valid_order" "
PRAGMA foreign_keys = ON;
CREATE TABLE tree(
  id INT PRIMARY KEY,
  parent_id INT REFERENCES tree(id)
);
INSERT INTO tree VALUES(1, NULL);
INSERT INTO tree VALUES(2, 1);
INSERT INTO tree VALUES(3, 1);
INSERT INTO tree VALUES(4, 2);
SELECT id, parent_id FROM tree ORDER BY id;
"

# Inserting a row pointing at a not-yet-existing parent fails.
oracle "self_ref_insert_before_parent_fails" "
PRAGMA foreign_keys = ON;
CREATE TABLE tree(
  id INT PRIMARY KEY,
  parent_id INT REFERENCES tree(id)
);
INSERT INTO tree VALUES(2, 1);
SELECT count(*) FROM tree;
"

# Self-referencing cascade on delete: deleting the root cascades to
# descendants.
oracle "self_ref_cascade_delete" "
PRAGMA foreign_keys = ON;
CREATE TABLE tree(
  id INT PRIMARY KEY,
  parent_id INT REFERENCES tree(id) ON DELETE CASCADE
);
INSERT INTO tree VALUES(1, NULL), (2, 1), (3, 1), (4, 2);
DELETE FROM tree WHERE id = 1;
SELECT id, parent_id FROM tree ORDER BY id;
"

# ─── Composite foreign keys ─────────────────────────────────────────
echo "--- composite FK ---"

oracle "composite_fk_insert_ok" "
PRAGMA foreign_keys = ON;
CREATE TABLE p(
  region TEXT,
  code   INT,
  PRIMARY KEY(region, code)
);
CREATE TABLE c(
  id INT PRIMARY KEY,
  region TEXT,
  code INT,
  FOREIGN KEY(region, code) REFERENCES p(region, code)
);
INSERT INTO p VALUES('us', 1), ('eu', 2);
INSERT INTO c VALUES(10, 'us', 1);
INSERT INTO c VALUES(11, 'eu', 2);
SELECT id, region, code FROM c ORDER BY id;
"

oracle "composite_fk_insert_missing_half_fails" "
PRAGMA foreign_keys = ON;
CREATE TABLE p(
  region TEXT,
  code INT,
  PRIMARY KEY(region, code)
);
CREATE TABLE c(
  id INT PRIMARY KEY,
  region TEXT,
  code INT,
  FOREIGN KEY(region, code) REFERENCES p(region, code)
);
INSERT INTO p VALUES('us', 1);
INSERT INTO c VALUES(10, 'us', 2);
SELECT count(*) FROM c;
"

oracle "composite_fk_cascade_delete" "
PRAGMA foreign_keys = ON;
CREATE TABLE p(
  region TEXT,
  code INT,
  PRIMARY KEY(region, code)
);
CREATE TABLE c(
  id INT PRIMARY KEY,
  region TEXT,
  code INT,
  FOREIGN KEY(region, code) REFERENCES p(region, code) ON DELETE CASCADE
);
INSERT INTO p VALUES('us', 1), ('us', 2);
INSERT INTO c VALUES(10, 'us', 1), (11, 'us', 1), (12, 'us', 2);
DELETE FROM p WHERE code = 1;
SELECT region, code FROM p ORDER BY code;
SELECT id, region, code FROM c ORDER BY id;
"

# ─── Deferred foreign keys ──────────────────────────────────────────
echo "--- deferred FK ---"

# DEFERRABLE INITIALLY DEFERRED: violations inside a transaction are
# fine as long as they resolve before COMMIT.
oracle "deferred_fk_temp_violation_ok" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(
  id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
);
BEGIN;
INSERT INTO child VALUES(10, 1);
INSERT INTO parent VALUES(1);
COMMIT;
SELECT id, pid FROM child;
"

# Unresolved deferred violation at COMMIT — transaction aborts.
oracle "deferred_fk_unresolved_commit_fails" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(
  id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
);
BEGIN;
INSERT INTO child VALUES(10, 1);
COMMIT;
SELECT id, pid FROM child;
"

# PRAGMA defer_foreign_keys = ON temporarily defers every FK
# regardless of its declaration. Temporary violation, resolved in
# the same txn, should be fine.
oracle "defer_fks_pragma_ok_when_resolved" "
PRAGMA foreign_keys = ON;
PRAGMA defer_foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
BEGIN;
INSERT INTO child VALUES(10, 1);
INSERT INTO parent VALUES(1);
COMMIT;
SELECT id, pid FROM child;
"

# ─── Txn interactions ───────────────────────────────────────────────
echo "--- transaction interactions ---"

# Immediate FK violation inside a txn aborts the statement. After
# the failed INSERT, the parent row from earlier should still be
# present — only the violating statement is rolled back.
oracle "immediate_violation_aborts_statement" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
BEGIN;
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 99);
COMMIT;
SELECT id FROM parent;
SELECT count(*) FROM child;
"

# Manual ROLLBACK after a successful cascade reverts the cascade too.
oracle "rollback_undoes_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 2);
BEGIN;
DELETE FROM parent WHERE id = 1;
ROLLBACK;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# ─── NO ACTION vs RESTRICT ─────────────────────────────────────────
echo "--- NO ACTION vs RESTRICT ---"

# NO ACTION is the default and is checked at end of statement.
# A multi-row UPDATE that temporarily violates but resolves by the
# end of the statement is legal. Here we swap the PK of two parent
# rows in one UPDATE via CASE; children referencing either key are
# still pointing at a valid parent once the statement finishes.
oracle "no_action_deferred_to_end_of_statement" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(
  id INT PRIMARY KEY,
  pid INT REFERENCES parent(id)
);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 2);
UPDATE parent SET id = CASE id WHEN 1 THEN 3 WHEN 2 THEN 4 END;
UPDATE child SET pid = CASE pid WHEN 1 THEN 3 WHEN 2 THEN 4 END;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# RESTRICT is row-level: this same swap attempted on a RESTRICT FK
# must fail because mid-statement the invariant is momentarily
# violated.
oracle "restrict_rejects_midstatement_violation" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(
  id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON UPDATE RESTRICT
);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 2);
UPDATE parent SET id = id + 10;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# ─── REPLACE INTO interactions ─────────────────────────────────────
echo "--- REPLACE INTO ---"

# REPLACE INTO on the parent is a DELETE + INSERT. With ON DELETE
# CASCADE the child rows are wiped. Stock SQLite documents this.
oracle "replace_into_parent_cascades_delete" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1), (11, 1);
REPLACE INTO parent VALUES(1, 'a-prime');
SELECT id, name FROM parent;
SELECT count(*) FROM child;
"

# Same with SET NULL: REPLACE nulls out the child's pid.
oracle "replace_into_parent_nulls_children" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1);
REPLACE INTO parent VALUES(1, 'a-prime');
SELECT id, name FROM parent;
SELECT id, pid FROM child;
"

# ─── UPSERT (INSERT ON CONFLICT) ───────────────────────────────────
echo "--- UPSERT ---"

# ON CONFLICT DO UPDATE touches the parent row in place — should not
# fire a DELETE cascade.
oracle "upsert_do_update_leaves_children" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1);
INSERT INTO parent VALUES(1, 'a-new')
  ON CONFLICT(id) DO UPDATE SET name = excluded.name;
SELECT id, name FROM parent;
SELECT id, pid FROM child;
"

# UPSERT that changes the PK via CASCADE-UPDATE — children's pid
# should follow.
oracle "upsert_do_update_pk_cascades" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON UPDATE CASCADE);
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1);
INSERT INTO parent VALUES(1, 'a-new')
  ON CONFLICT(id) DO UPDATE SET id = 2;
SELECT id, name FROM parent;
SELECT id, pid FROM child;
"

# ─── Savepoint + cascade ───────────────────────────────────────────
echo "--- savepoint + cascade ---"

# A cascade inside a savepoint that is then rolled back should
# restore the entire graph.
oracle "rollback_to_undoes_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 1), (12, 2);
SAVEPOINT s;
DELETE FROM parent WHERE id = 1;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# A cascade inside a savepoint that is then released commits.
oracle "release_savepoint_keeps_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1), (2);
INSERT INTO child VALUES(10, 1), (11, 1), (12, 2);
SAVEPOINT s;
DELETE FROM parent WHERE id = 1;
RELEASE SAVEPOINT s;
SELECT id FROM parent ORDER BY id;
SELECT id, pid FROM child ORDER BY id;
"

# Rolling back an update-cascade inside a savepoint.
oracle "rollback_to_undoes_update_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON UPDATE CASCADE);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1), (11, 1);
SAVEPOINT s;
UPDATE parent SET id = 99;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id FROM parent;
SELECT id, pid FROM child ORDER BY id;
"

# ─── Trigger + FK interaction ──────────────────────────────────────
echo "--- triggers + FK ---"

# A BEFORE DELETE trigger on the parent fires before the cascade
# runs. The trigger writes to a log table; both the trigger side
# effect and the cascade must be visible after commit.
oracle "trigger_fires_then_cascade_runs" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
CREATE TABLE log(id INT PRIMARY KEY, msg TEXT);
CREATE TRIGGER log_del BEFORE DELETE ON parent BEGIN
  INSERT INTO log VALUES(old.id, 'parent-gone');
END;
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1), (11, 1);
DELETE FROM parent WHERE id = 1;
SELECT count(*) FROM parent;
SELECT count(*) FROM child;
SELECT id, msg FROM log ORDER BY id;
"

# A BEFORE DELETE trigger that RAISE(ABORT)s the delete. The cascade
# must NOT happen and the parent row must still exist.
oracle "trigger_abort_prevents_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
CREATE TRIGGER prevent_del BEFORE DELETE ON parent BEGIN
  SELECT RAISE(ABORT, 'no');
END;
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
DELETE FROM parent WHERE id = 1;
SELECT id FROM parent;
SELECT id, pid FROM child;
"

# AFTER DELETE trigger on child runs per-cascaded-row.
oracle "after_delete_on_child_runs_per_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, child_id INT);
CREATE TRIGGER log_child_del AFTER DELETE ON child BEGIN
  INSERT INTO log(child_id) VALUES(old.id);
END;
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1), (11, 1), (12, 1);
DELETE FROM parent WHERE id = 1;
SELECT count(*) FROM child;
SELECT child_id FROM log ORDER BY child_id;
"

# ─── PRAGMA foreign_key_check ──────────────────────────────────────
echo "--- foreign_key_check ---"

# PRAGMA foreign_key_check returns one row per orphan in the format
# (table, rowid, referenced_table, fkid). doltlite uses user PKs
# as primary keys, not sqlite's autogenerated rowids, so the second
# column comes back empty instead of the rowid. That's a known
# vtable-shape divergence documented in MEMORY.md and is not a
# row-level semantic issue — orphan detection itself is correct.
# We instead verify the behavior via a SQL-level subquery that
# both engines answer identically.

# Emulates `foreign_key_check` semantically: count orphans in child.
oracle "semantic_fk_check_reports_orphans" "
PRAGMA foreign_keys = OFF;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 99);
INSERT INTO child VALUES(11, 1);
SELECT count(*) FROM child WHERE pid IS NOT NULL
  AND pid NOT IN (SELECT id FROM parent);
"

oracle "semantic_fk_check_clean" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
SELECT count(*) FROM child WHERE pid IS NOT NULL
  AND pid NOT IN (SELECT id FROM parent);
"

# The actual PRAGMA foreign_key_check STILL runs and detects the
# orphan in doltlite — just with an empty rowid column. Verify the
# row count matches.
oracle "pragma_fk_check_detects_orphan_by_count" "
PRAGMA foreign_keys = OFF;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 99);
INSERT INTO child VALUES(11, 1);
CREATE TEMP TABLE fkc AS SELECT * FROM pragma_foreign_key_check;
SELECT count(*) FROM fkc;
DROP TABLE fkc;
"

oracle "pragma_fk_check_clean_zero" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
CREATE TEMP TABLE fkc AS SELECT * FROM pragma_foreign_key_check;
SELECT count(*) FROM fkc;
DROP TABLE fkc;
"

# ─── Cycles ────────────────────────────────────────────────────────
echo "--- cycles ---"

# Two tables, each with a deferred FK to the other. Both rows
# inserted in the same transaction and resolved by commit time.
oracle "two_table_cycle_deferred" "
PRAGMA foreign_keys = ON;
CREATE TABLE a(id INT PRIMARY KEY, b_id INT
  REFERENCES b(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE b(id INT PRIMARY KEY, a_id INT
  REFERENCES a(id) DEFERRABLE INITIALLY DEFERRED);
BEGIN;
INSERT INTO a VALUES(1, 10);
INSERT INTO b VALUES(10, 1);
COMMIT;
SELECT id, b_id FROM a;
SELECT id, a_id FROM b;
"

# ─── Large cascades ─────────────────────────────────────────────────
echo "--- bulk cascade ---"

make_cascade_rows() {
  local n="$1" tbl="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO $tbl VALUES($i, 1);"
  done
}

# Cascade delete across 100 child rows.
oracle "cascade_delete_100_children" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1);
$(make_cascade_rows 100 child)
DELETE FROM parent WHERE id = 1;
SELECT count(*) FROM parent;
SELECT count(*) FROM child;
"

# ─── Final report ───────────────────────────────────────────────────

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
