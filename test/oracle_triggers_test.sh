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
    | sed -e 's/[[:space:]]\{1,\}/ /g' -e 's/^ //' -e 's/ $//'
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

echo "=== Oracle Tests: SQL triggers ==="
echo ""

echo "--- INSERT triggers ---"

oracle "after_insert_logs_new_row" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT, v INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(new.id, new.v);
END;
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT id, v FROM log ORDER BY id;
"

oracle "before_insert_when_filter" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT);
CREATE TRIGGER t_bi BEFORE INSERT ON t WHEN new.v > 15 BEGIN
  INSERT INTO log VALUES(new.id);
END;
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,5);
INSERT INTO t VALUES(4,100);
SELECT id FROM log ORDER BY id;
"

oracle "before_insert_fires_on_empty" "
CREATE TABLE t(id INT PRIMARY KEY);
CREATE TABLE log(note TEXT);
CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN
  INSERT INTO log VALUES('before');
END;
INSERT INTO t VALUES(1);
SELECT note FROM log;
"

oracle "multi_statement_body" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(k TEXT, val INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES('id', new.id);
  INSERT INTO log VALUES('v',  new.v);
  INSERT INTO log VALUES('sum', new.id + new.v);
END;
INSERT INTO t VALUES(1,10);
SELECT k, val FROM log ORDER BY k;
"

echo "--- UPDATE triggers ---"

oracle "after_update_old_new" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT, old_v INT, new_v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN
  INSERT INTO log VALUES(new.id, old.v, new.v);
END;
UPDATE t SET v = v + 100 WHERE id <= 2;
SELECT id, old_v, new_v FROM log ORDER BY id;
"

oracle "before_update_of_col_specific" "
CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT);
CREATE TABLE log(note TEXT);
INSERT INTO t VALUES(1,10,100);
CREATE TRIGGER t_bu BEFORE UPDATE OF a ON t BEGIN
  INSERT INTO log VALUES('a changed');
END;
UPDATE t SET b = 200 WHERE id = 1;
UPDATE t SET a = 11  WHERE id = 1;
UPDATE t SET b = 300 WHERE id = 1;
SELECT count(*) FROM log;
"

oracle "update_diff_body" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT, delta INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN
  INSERT INTO log VALUES(new.id, new.v - old.v);
END;
UPDATE t SET v = v * 2;
SELECT id, delta FROM log ORDER BY id;
"

echo "--- DELETE triggers ---"

oracle "after_delete_cascade_via_trigger" "
CREATE TABLE parent(id INT PRIMARY KEY);
CREATE TABLE child(id INT PRIMARY KEY, parent_id INT);
INSERT INTO parent VALUES(1),(2),(3);
INSERT INTO child VALUES(10,1),(11,1),(20,2),(30,3);
CREATE TRIGGER cascade_del AFTER DELETE ON parent BEGIN
  DELETE FROM child WHERE parent_id = old.id;
END;
DELETE FROM parent WHERE id IN (1,2);
SELECT id, parent_id FROM child ORDER BY id;
SELECT id FROM parent ORDER BY id;
"

oracle "before_delete_logs_old" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
CREATE TABLE log(id INT, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN
  INSERT INTO log VALUES(old.id, old.v);
END;
DELETE FROM t WHERE id > 1;
SELECT id, v FROM log ORDER BY id;
SELECT id FROM t ORDER BY id;
"

oracle "after_delete_counter" "
CREATE TABLE t(id INT PRIMARY KEY);
CREATE TABLE counter(n INT);
INSERT INTO counter VALUES(0);
INSERT INTO t VALUES(1),(2),(3),(4),(5);
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN
  UPDATE counter SET n = n + 1;
END;
DELETE FROM t WHERE id >= 3;
SELECT n FROM counter;
"

echo "--- RAISE actions ---"

oracle "raise_abort_reverts_statement" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
CREATE TRIGGER guard BEFORE INSERT ON t WHEN new.v < 0 BEGIN
  SELECT RAISE(ABORT, 'negative not allowed');
END;
INSERT INTO t VALUES(2, 20);
INSERT OR IGNORE INTO t SELECT 3, -1;
INSERT INTO t VALUES(4, 40);
SELECT id, v FROM t ORDER BY id;
"

oracle "raise_ignore_drops_row" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TRIGGER skip_neg BEFORE INSERT ON t WHEN new.v < 0 BEGIN
  SELECT RAISE(IGNORE);
END;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, -5);
INSERT INTO t VALUES(3, 30);
SELECT id, v FROM t ORDER BY id;
"

oracle "raise_fail_on_second_row" "
CREATE TABLE src(id INT, v INT);
INSERT INTO src VALUES(1,10),(2,-1),(3,30);
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TRIGGER guard BEFORE INSERT ON t WHEN new.v < 0 BEGIN
  SELECT RAISE(FAIL, 'no neg');
END;
INSERT OR FAIL INTO t SELECT id, v FROM src ORDER BY id;
SELECT id, v FROM t ORDER BY id;
"

oracle "raise_rollback_reverts_transaction" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TRIGGER no_neg BEFORE INSERT ON t WHEN new.v < 0 BEGIN
  SELECT RAISE(ROLLBACK, 'negatives forbidden');
END;
BEGIN;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, -1);
COMMIT;
SELECT count(*) FROM t;
"

echo "--- INSTEAD OF on views ---"

oracle "instead_of_insert_on_view" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE VIEW vt AS SELECT id, v FROM t;
CREATE TRIGGER v_ii INSTEAD OF INSERT ON vt BEGIN
  INSERT INTO t VALUES(new.id, new.v * 10);
END;
INSERT INTO vt VALUES(1,1),(2,2),(3,3);
SELECT id, v FROM t ORDER BY id;
"

oracle "instead_of_update_on_view" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE VIEW vt AS SELECT id, v FROM t;
CREATE TRIGGER v_iu INSTEAD OF UPDATE ON vt BEGIN
  UPDATE t SET v = new.v + 1000 WHERE id = old.id;
END;
UPDATE vt SET v = 99 WHERE id = 2;
SELECT id, v FROM t ORDER BY id;
"

oracle "instead_of_delete_on_view" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE VIEW vt AS SELECT id, v FROM t;
CREATE TRIGGER v_id INSTEAD OF DELETE ON vt BEGIN
  DELETE FROM t WHERE id = old.id;
END;
DELETE FROM vt WHERE id = 2;
SELECT id FROM t ORDER BY id;
"

echo "--- cascading triggers ---"

oracle "cascading_triggers_two_tables" "
CREATE TABLE a(id INT PRIMARY KEY, v INT);
CREATE TABLE b(id INT PRIMARY KEY, a_id INT, note TEXT);
CREATE TABLE log(src TEXT, id INT);
CREATE TRIGGER a_ai AFTER INSERT ON a BEGIN
  INSERT INTO b VALUES(new.id*10, new.id, 'from_a');
END;
CREATE TRIGGER b_ai AFTER INSERT ON b BEGIN
  INSERT INTO log VALUES('b', new.id);
END;
INSERT INTO a VALUES(1,10),(2,20);
SELECT src, id FROM log ORDER BY id;
SELECT id, a_id, note FROM b ORDER BY id;
"

oracle "cascading_triggers_three_levels" "
CREATE TABLE a(id INT PRIMARY KEY);
CREATE TABLE b(id INT PRIMARY KEY);
CREATE TABLE c(id INT PRIMARY KEY);
CREATE TRIGGER a_ai AFTER INSERT ON a BEGIN INSERT INTO b VALUES(new.id+100); END;
CREATE TRIGGER b_ai AFTER INSERT ON b BEGIN INSERT INTO c VALUES(new.id+1000); END;
INSERT INTO a VALUES(1),(2),(3);
SELECT id FROM a ORDER BY id;
SELECT id FROM b ORDER BY id;
SELECT id FROM c ORDER BY id;
"

echo "--- recursive triggers ---"

oracle "recursive_trigger_self_update" "
PRAGMA recursive_triggers = ON;
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(v INT);
INSERT INTO t VALUES(1,5);
CREATE TRIGGER t_au AFTER UPDATE ON t WHEN new.v > 0 BEGIN
  INSERT INTO log VALUES(new.v);
  UPDATE t SET v = new.v - 1 WHERE id = new.id;
END;
UPDATE t SET v = v - 1 WHERE id = 1;
SELECT v FROM log ORDER BY v;
SELECT v FROM t;
"

echo "--- lifecycle ---"

oracle "drop_trigger_stops_firing" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(id INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(new.id);
END;
INSERT INTO t VALUES(1,10);
DROP TRIGGER t_ai;
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT id FROM log ORDER BY id;
SELECT id FROM t ORDER BY id;
"

oracle "create_if_not_exists" "
CREATE TABLE t(id INT PRIMARY KEY);
CREATE TABLE log(note TEXT);
CREATE TRIGGER IF NOT EXISTS once AFTER INSERT ON t BEGIN INSERT INTO log VALUES('v1'); END;
CREATE TRIGGER IF NOT EXISTS once AFTER INSERT ON t BEGIN INSERT INTO log VALUES('v2'); END;
INSERT INTO t VALUES(1);
SELECT note FROM log;
"

echo "--- alternate PK shapes ---"

oracle "trigger_on_text_pk" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
CREATE TABLE log(k TEXT, v INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(new.k, new.v);
END;
INSERT INTO t VALUES('alpha',1),('beta',2),('gamma',3);
SELECT k, v FROM log ORDER BY k;
"

oracle "trigger_on_composite_pk" "
CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a,b));
CREATE TABLE log(a INT, b INT, v TEXT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(new.a, new.b, new.v);
END;
INSERT INTO t VALUES(1,2,'x'),(1,3,'y'),(2,2,'z');
SELECT a, b, v FROM log ORDER BY a, b;
"

oracle "trigger_on_without_rowid" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE TABLE log(k TEXT);
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN
  INSERT INTO log VALUES(old.k);
END;
INSERT INTO t VALUES('a',1),('b',2),('c',3);
DELETE FROM t WHERE v >= 2;
SELECT k FROM log ORDER BY k;
SELECT k FROM t ORDER BY k;
"

echo "--- self-referential ---"

oracle "trigger_reads_own_table" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(n INT, total INT);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES((SELECT count(*) FROM t), (SELECT sum(v) FROM t));
END;
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT n, total FROM log ORDER BY n;
"

oracle "trigger_reads_after_delete" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(remaining INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40);
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN
  INSERT INTO log VALUES((SELECT count(*) FROM t));
END;
DELETE FROM t WHERE id = 2;
DELETE FROM t WHERE id = 4;
SELECT remaining FROM log ORDER BY remaining;
SELECT id FROM t ORDER BY id;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
