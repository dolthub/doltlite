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

echo "=== Oracle Tests: foreign keys (single branch) ==="
echo ""

echo "--- basic check ---"

oracle "insert_child_matching_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 1);
SELECT c.id, c.pid, p.name FROM child c JOIN parent p ON c.pid = p.id;
"

oracle "insert_child_no_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY, name TEXT);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1, 'a');
INSERT INTO child VALUES(10, 99);
SELECT id, pid FROM child;
"

oracle "insert_child_null_fk" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, NULL);
INSERT INTO child VALUES(11, 1);
SELECT id, pid FROM child ORDER BY id;
"

oracle "update_child_to_invalid_parent" "
PRAGMA foreign_keys = ON;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 1);
UPDATE child SET pid = 99 WHERE id = 10;
SELECT id, pid FROM child;
"

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

oracle "fks_off_skips_check" "
PRAGMA foreign_keys = OFF;
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(10, 99);
SELECT id, pid FROM child;
"

echo "--- ON DELETE CASCADE ---"

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

echo "--- ON DELETE SET DEFAULT ---"

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

echo "--- ON UPDATE CASCADE ---"

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

echo "--- self-referencing ---"

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

oracle "self_ref_insert_before_parent_fails" "
PRAGMA foreign_keys = ON;
CREATE TABLE tree(
  id INT PRIMARY KEY,
  parent_id INT REFERENCES tree(id)
);
INSERT INTO tree VALUES(2, 1);
SELECT count(*) FROM tree;
"

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

echo "--- deferred FK ---"

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

echo "--- transaction interactions ---"

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

echo "--- NO ACTION vs RESTRICT ---"

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

echo "--- REPLACE INTO ---"

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

echo "--- UPSERT ---"

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

echo "--- savepoint + cascade ---"

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

echo "--- triggers + FK ---"

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

echo "--- foreign_key_check ---"


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

echo "--- cycles ---"

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

echo "--- bulk cascade ---"

make_cascade_rows() {
  local n="$1" tbl="$2"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO $tbl VALUES($i, 1);"
  done
}

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


echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
