#!/bin/bash
#
# Oracle test: INSERT ... ON CONFLICT (UPSERT) and INSERT OR <action>
# against stock SQLite.
#
# UPSERT exercises the same prolly mutmap in-place mutation path that
# the savepoint rollback bug hid behind for years: when a fresh INSERT
# hits an already-pending row with the same key, the mutmap overwrites
# the entry in place. Any drift between stock sqlite's conflict
# resolution semantics and the prolly btree's lands here.
#
# Oracle target is stock sqlite3 built from the same source tree so
# both engines parse identical SQL and the only variable is the
# storage layer (prolly btree vs stock btree).
#
# Coverage:
#   - INSERT OR <action>: ABORT (default), IGNORE, REPLACE, FAIL,
#     ROLLBACK — single-row + multi-row
#   - INSERT ... ON CONFLICT(col) DO NOTHING
#   - INSERT ... ON CONFLICT(col) DO UPDATE SET col = val
#   - excluded pseudo-table: referencing the row that would have
#     been inserted
#   - DO UPDATE with a WHERE clause (conditional update)
#   - DO UPDATE that changes the PK (fires UPDATE path)
#   - Composite conflict target: ON CONFLICT(a, b)
#   - UNIQUE-index conflict target (not just PRIMARY KEY)
#   - Multi-conflict-target chains: ON CONFLICT(a) DO UPDATE
#     followed by ON CONFLICT(b) DO UPDATE on a second unique index
#   - Multi-row INSERT with some rows conflicting and others not
#   - UPSERT inside a transaction with COMMIT / ROLLBACK
#   - UPSERT across savepoints (ROLLBACK TO / RELEASE)
#   - UPSERT with BEFORE/AFTER INSERT triggers and BEFORE/AFTER
#     UPDATE triggers (which fire depends on resolution path)
#   - RETURNING with UPSERT (SQLite >= 3.35)
#   - INSERT OR REPLACE that deletes a row referenced by an FK with
#     ON DELETE CASCADE
#
# Usage: bash oracle_upsert_test.sh [path/to/doltlite] [path/to/sqlite3]
#

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
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

echo "=== Oracle Tests: UPSERT / INSERT OR <action> ==="
echo ""

# ─── INSERT OR ABORT (the default) ──────────────────────────────────
echo "--- INSERT OR ABORT ---"

# Default resolution: second INSERT hits a PK collision and the
# statement aborts. The prior row is preserved.
oracle "insert_or_abort_rejects_dup_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b');
SELECT id, v FROM t;
"

# Multi-row INSERT where the second row conflicts: ABORT rolls back
# the whole statement (the first row is not inserted either).
oracle "insert_or_abort_multirow_rollback" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(2, 'b'), (1, 'c');
SELECT id, v FROM t ORDER BY id;
"

# ─── INSERT OR IGNORE ──────────────────────────────────────────────
echo "--- INSERT OR IGNORE ---"

# The conflicting row is silently skipped; prior row preserved.
oracle "insert_or_ignore_skips_dup" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR IGNORE INTO t VALUES(1, 'b');
SELECT id, v FROM t;
"

# In a multi-row INSERT with one conflict, the non-conflicting rows
# land and the conflicting one is dropped.
oracle "insert_or_ignore_multirow_partial" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR IGNORE INTO t VALUES(2, 'b'), (1, 'dup'), (3, 'c');
SELECT id, v FROM t ORDER BY id;
"

# INSERT OR IGNORE on a UNIQUE column (not PK).
oracle "insert_or_ignore_unique" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1, 100);
INSERT OR IGNORE INTO t VALUES(2, 100);
SELECT id, u FROM t ORDER BY id;
"

# ─── INSERT OR REPLACE ─────────────────────────────────────────────
echo "--- INSERT OR REPLACE ---"

# The old row is deleted and the new row inserted in its place.
oracle "insert_or_replace_overwrites" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR REPLACE INTO t VALUES(1, 'b');
SELECT id, v FROM t;
"

# REPLACE on a UNIQUE column deletes the row holding that unique
# value and inserts the new row — even though the PKs differ.
oracle "insert_or_replace_unique_deletes_other_row" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(2, 200);
INSERT OR REPLACE INTO t VALUES(3, 100);
SELECT id, u FROM t ORDER BY id;
"

# REPLACE that deletes an FK parent with ON DELETE CASCADE fires
# the cascade on the children.
oracle "insert_or_replace_fires_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY,
  pid INT REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1), (11, 1);
INSERT OR REPLACE INTO parent VALUES(1, 'a-new');
SELECT id, name FROM parent;
SELECT count(*) FROM child;
"

# ─── INSERT OR FAIL ────────────────────────────────────────────────
echo "--- INSERT OR FAIL ---"

# FAIL differs from ABORT on multi-row insert: rows processed before
# the conflict STAY, the conflicting one and anything after is not
# inserted.
oracle "insert_or_fail_preserves_prior_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR FAIL INTO t VALUES(2, 'b'), (1, 'dup'), (3, 'c');
SELECT id, v FROM t ORDER BY id;
"

# ─── INSERT OR ROLLBACK ────────────────────────────────────────────
echo "--- INSERT OR ROLLBACK ---"

# ROLLBACK aborts the current transaction on conflict. Here no
# explicit txn is active, so it aborts the implicit one.
oracle "insert_or_rollback_aborts_txn" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1, 'a');
INSERT OR ROLLBACK INTO t VALUES(1, 'b');
COMMIT;
SELECT id, v FROM t;
"

# ─── ON CONFLICT DO NOTHING ────────────────────────────────────────
echo "--- ON CONFLICT DO NOTHING ---"

# Equivalent to INSERT OR IGNORE for a simple case.
oracle "do_nothing_skips_dup_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO NOTHING;
SELECT id, v FROM t;
"

# Without a conflict target: "any conflict" form.
oracle "do_nothing_no_target" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(1, 200) ON CONFLICT DO NOTHING;
INSERT INTO t VALUES(2, 100) ON CONFLICT DO NOTHING;
SELECT id, u FROM t ORDER BY id;
"

# Multi-row INSERT with DO NOTHING: non-conflicting rows land.
oracle "do_nothing_multirow_partial" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(2, 'b'), (1, 'dup'), (3, 'c')
  ON CONFLICT(id) DO NOTHING;
SELECT id, v FROM t ORDER BY id;
"

# ─── ON CONFLICT DO UPDATE (core upsert) ───────────────────────────
echo "--- ON CONFLICT DO UPDATE ---"

# Basic: update an existing row via an ON CONFLICT clause.
oracle "do_update_basic" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = 'updated';
SELECT id, v FROM t;
"

# excluded pseudo-table: refer to the would-be-inserted row.
oracle "do_update_excluded" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT id, v FROM t;
"

# DO UPDATE that concatenates old and new values.
oracle "do_update_concat_old_and_new" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id)
  DO UPDATE SET v = v || '+' || excluded.v;
SELECT id, v FROM t;
"

# WHERE filter on DO UPDATE: the update only fires when the filter
# matches. Here it does match.
oracle "do_update_where_fires" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(1, 5) ON CONFLICT(id)
  DO UPDATE SET v = excluded.v WHERE excluded.v > t.v;
INSERT INTO t VALUES(1, 20) ON CONFLICT(id)
  DO UPDATE SET v = excluded.v WHERE excluded.v > t.v;
SELECT id, v FROM t;
"

# WHERE filter that does NOT match — the UPDATE is skipped; the
# insert ALSO does not land (because the conflict was resolved, the
# row is not inserted, but the update was filtered out, net no-op).
oracle "do_update_where_skips" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(1, 99) ON CONFLICT(id)
  DO UPDATE SET v = excluded.v WHERE excluded.v < t.v;
SELECT id, v FROM t;
"

# Composite conflict target.
oracle "do_update_composite_target" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1, 1, 'orig');
INSERT INTO t VALUES(1, 1, 'new')
  ON CONFLICT(a, b) DO UPDATE SET v = excluded.v;
INSERT INTO t VALUES(1, 2, 'new-row')
  ON CONFLICT(a, b) DO UPDATE SET v = excluded.v;
SELECT a, b, v FROM t ORDER BY a, b;
"

# UNIQUE index conflict target (not PK).
oracle "do_update_unique_target" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1, 100, 'a');
INSERT INTO t VALUES(2, 100, 'b')
  ON CONFLICT(u) DO UPDATE SET v = excluded.v;
SELECT id, u, v FROM t ORDER BY id;
"

# DO UPDATE that touches the conflict column itself — legal only if
# the new value doesn't collide with another row's value.
oracle "do_update_modifies_target_column_ok" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET id = 99;
SELECT id, v FROM t ORDER BY id;
"

# DO UPDATE that changes the PK to one that conflicts with another
# existing row — second-order constraint violation.
oracle "do_update_modifies_target_column_collision" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(2, 'b');
INSERT INTO t VALUES(1, 'c') ON CONFLICT(id) DO UPDATE SET id = 2;
SELECT id, v FROM t ORDER BY id;
"

# ─── Multi-row UPSERT ──────────────────────────────────────────────
echo "--- multi-row UPSERT ---"

oracle "multirow_upsert_mix_insert_update" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
INSERT INTO t VALUES(1, 99), (3, 30), (2, 88)
  ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT id, v FROM t ORDER BY id;
"

# ─── Triggers + UPSERT ─────────────────────────────────────────────
echo "--- triggers + UPSERT ---"

# BEFORE INSERT fires only on the insert path. DO UPDATE branches
# to the UPDATE path and fires BEFORE UPDATE instead.
oracle "upsert_fires_before_insert_on_fresh" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO log(what) VALUES('bi'); END;
CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO log(what) VALUES('bu'); END;
INSERT INTO t VALUES(1, 'a') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT what FROM log ORDER BY id;
"

oracle "upsert_fires_before_update_on_conflict" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO log(what) VALUES('bi'); END;
CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO log(what) VALUES('bu'); END;
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT what FROM log ORDER BY id;
"

# AFTER INSERT vs AFTER UPDATE similarly.
oracle "upsert_fires_after_insert_on_fresh" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log(what) VALUES('ai'); END;
CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO log(what) VALUES('au'); END;
INSERT INTO t VALUES(1, 'a') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT what FROM log ORDER BY id;
"

# ─── UPSERT inside transactions / savepoints ───────────────────────
echo "--- UPSERT + txn/savepoints ---"

# UPSERT inside BEGIN / COMMIT.
oracle "upsert_inside_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'orig');
BEGIN;
INSERT INTO t VALUES(1, 'tx') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
COMMIT;
SELECT id, v FROM t;
"

# UPSERT inside BEGIN / ROLLBACK.
oracle "upsert_inside_rollback" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'orig');
BEGIN;
INSERT INTO t VALUES(1, 'tx') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
ROLLBACK;
SELECT id, v FROM t;
"

# UPSERT inside a savepoint that gets rolled back.
oracle "upsert_rollback_to_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'orig');
SAVEPOINT s;
INSERT INTO t VALUES(1, 'sp') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, v FROM t;
"

# Chained UPSERTs inside a savepoint then rolled back.
oracle "chained_upserts_rollback_to_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 0);
SAVEPOINT s;
INSERT INTO t VALUES(1, 1) ON CONFLICT(id) DO UPDATE SET v = v + 1;
INSERT INTO t VALUES(1, 1) ON CONFLICT(id) DO UPDATE SET v = v + 1;
INSERT INTO t VALUES(1, 1) ON CONFLICT(id) DO UPDATE SET v = v + 1;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, v FROM t;
"

# ─── Bulk UPSERT ───────────────────────────────────────────────────
echo "--- bulk UPSERT ---"

make_upserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES($i, $i) ON CONFLICT(id) DO UPDATE SET v = t.v + excluded.v;"
  done
}

# 100 upserts into an empty table — all fresh inserts.
oracle "bulk_upsert_100_fresh" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
$(make_upserts 100)
SELECT count(*), sum(v) FROM t;
"

# Same 100 upserts applied twice — second pass hits the DO UPDATE
# branch for every row.
oracle "bulk_upsert_100_then_100" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
$(make_upserts 100)
$(make_upserts 100)
SELECT count(*), sum(v) FROM t;
"

# ─── RETURNING ─────────────────────────────────────────────────────
echo "--- RETURNING ---"

# UPSERT with RETURNING on the fresh-insert path.
oracle "upsert_returning_insert" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a')
  ON CONFLICT(id) DO UPDATE SET v = excluded.v
  RETURNING id, v;
"

# UPSERT with RETURNING on the conflict/update path.
oracle "upsert_returning_update" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b')
  ON CONFLICT(id) DO UPDATE SET v = excluded.v
  RETURNING id, v;
"

# RETURNING from a DO NOTHING clause on conflict returns nothing.
oracle "upsert_returning_do_nothing" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b')
  ON CONFLICT(id) DO NOTHING
  RETURNING id, v;
SELECT id, v FROM t;
"

# ─── Final report ───────────────────────────────────────────────────

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
