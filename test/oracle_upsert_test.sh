#!/bin/bash

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

echo "--- INSERT OR ABORT ---"

oracle "insert_or_abort_rejects_dup_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b');
SELECT id, v FROM t;
"

oracle "insert_or_abort_multirow_rollback" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(2, 'b'), (1, 'c');
SELECT id, v FROM t ORDER BY id;
"

echo "--- INSERT OR IGNORE ---"

oracle "insert_or_ignore_skips_dup" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR IGNORE INTO t VALUES(1, 'b');
SELECT id, v FROM t;
"

oracle "insert_or_ignore_multirow_partial" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR IGNORE INTO t VALUES(2, 'b'), (1, 'dup'), (3, 'c');
SELECT id, v FROM t ORDER BY id;
"

oracle "insert_or_ignore_unique" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1, 100);
INSERT OR IGNORE INTO t VALUES(2, 100);
SELECT id, u FROM t ORDER BY id;
"

echo "--- INSERT OR REPLACE ---"

oracle "insert_or_replace_overwrites" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR REPLACE INTO t VALUES(1, 'b');
SELECT id, v FROM t;
"

oracle "insert_or_replace_unique_deletes_other_row" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(2, 200);
INSERT OR REPLACE INTO t VALUES(3, 100);
SELECT id, u FROM t ORDER BY id;
"

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

echo "--- INSERT OR FAIL ---"

oracle "insert_or_fail_preserves_prior_rows" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT OR FAIL INTO t VALUES(2, 'b'), (1, 'dup'), (3, 'c');
SELECT id, v FROM t ORDER BY id;
"

echo "--- INSERT OR ROLLBACK ---"

oracle "insert_or_rollback_aborts_txn" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES(1, 'a');
INSERT OR ROLLBACK INTO t VALUES(1, 'b');
COMMIT;
SELECT id, v FROM t;
"

echo "--- ON CONFLICT DO NOTHING ---"

oracle "do_nothing_skips_dup_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO NOTHING;
SELECT id, v FROM t;
"

oracle "do_nothing_no_target" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(1, 200) ON CONFLICT DO NOTHING;
INSERT INTO t VALUES(2, 100) ON CONFLICT DO NOTHING;
SELECT id, u FROM t ORDER BY id;
"

oracle "do_nothing_multirow_partial" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(2, 'b'), (1, 'dup'), (3, 'c')
  ON CONFLICT(id) DO NOTHING;
SELECT id, v FROM t ORDER BY id;
"

echo "--- ON CONFLICT DO UPDATE ---"

oracle "do_update_basic" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = 'updated';
SELECT id, v FROM t;
"

oracle "do_update_excluded" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT id, v FROM t;
"

oracle "do_update_concat_old_and_new" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id)
  DO UPDATE SET v = v || '+' || excluded.v;
SELECT id, v FROM t;
"

oracle "do_update_where_fires" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(1, 5) ON CONFLICT(id)
  DO UPDATE SET v = excluded.v WHERE excluded.v > t.v;
INSERT INTO t VALUES(1, 20) ON CONFLICT(id)
  DO UPDATE SET v = excluded.v WHERE excluded.v > t.v;
SELECT id, v FROM t;
"

oracle "do_update_where_skips" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(1, 99) ON CONFLICT(id)
  DO UPDATE SET v = excluded.v WHERE excluded.v < t.v;
SELECT id, v FROM t;
"

oracle "do_update_composite_target" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1, 1, 'orig');
INSERT INTO t VALUES(1, 1, 'new')
  ON CONFLICT(a, b) DO UPDATE SET v = excluded.v;
INSERT INTO t VALUES(1, 2, 'new-row')
  ON CONFLICT(a, b) DO UPDATE SET v = excluded.v;
SELECT a, b, v FROM t ORDER BY a, b;
"

oracle "do_update_unique_target" "
CREATE TABLE t(id INT PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1, 100, 'a');
INSERT INTO t VALUES(2, 100, 'b')
  ON CONFLICT(u) DO UPDATE SET v = excluded.v;
SELECT id, u, v FROM t ORDER BY id;
"

oracle "do_update_modifies_target_column_ok" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET id = 99;
SELECT id, v FROM t ORDER BY id;
"

oracle "do_update_modifies_target_column_collision" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(2, 'b');
INSERT INTO t VALUES(1, 'c') ON CONFLICT(id) DO UPDATE SET id = 2;
SELECT id, v FROM t ORDER BY id;
"

echo "--- multi-row UPSERT ---"

oracle "multirow_upsert_mix_insert_update" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
INSERT INTO t VALUES(1, 99), (3, 30), (2, 88)
  ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT id, v FROM t ORDER BY id;
"

echo "--- triggers + UPSERT ---"

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

oracle "upsert_fires_after_insert_on_fresh" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, what TEXT);
CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log(what) VALUES('ai'); END;
CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO log(what) VALUES('au'); END;
INSERT INTO t VALUES(1, 'a') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
INSERT INTO t VALUES(1, 'b') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT what FROM log ORDER BY id;
"

echo "--- UPSERT + txn/savepoints ---"

oracle "upsert_inside_commit" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'orig');
BEGIN;
INSERT INTO t VALUES(1, 'tx') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
COMMIT;
SELECT id, v FROM t;
"

oracle "upsert_inside_rollback" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'orig');
BEGIN;
INSERT INTO t VALUES(1, 'tx') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
ROLLBACK;
SELECT id, v FROM t;
"

oracle "upsert_rollback_to_savepoint" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'orig');
SAVEPOINT s;
INSERT INTO t VALUES(1, 'sp') ON CONFLICT(id) DO UPDATE SET v = excluded.v;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, v FROM t;
"

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

echo "--- bulk UPSERT ---"

make_upserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t VALUES($i, $i) ON CONFLICT(id) DO UPDATE SET v = t.v + excluded.v;"
  done
}

oracle "bulk_upsert_100_fresh" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
$(make_upserts 100)
SELECT count(*), sum(v) FROM t;
"

oracle "bulk_upsert_100_then_100" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
$(make_upserts 100)
$(make_upserts 100)
SELECT count(*), sum(v) FROM t;
"

echo "--- RETURNING ---"

oracle "upsert_returning_insert" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a')
  ON CONFLICT(id) DO UPDATE SET v = excluded.v
  RETURNING id, v;
"

oracle "upsert_returning_update" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b')
  ON CONFLICT(id) DO UPDATE SET v = excluded.v
  RETURNING id, v;
"

oracle "upsert_returning_do_nothing" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
INSERT INTO t VALUES(1, 'b')
  ON CONFLICT(id) DO NOTHING
  RETURNING id, v;
SELECT id, v FROM t;
"


echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
