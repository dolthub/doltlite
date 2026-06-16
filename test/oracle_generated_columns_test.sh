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

echo "=== Oracle Tests: generated columns ==="
echo ""

echo "--- basic round-trip ---"

oracle "stored_basic_arithmetic" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a  INT,
  b  INT,
  sum INT GENERATED ALWAYS AS (a + b) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 3, 4), (2, 10, 20);
SELECT id, a, b, sum FROM t ORDER BY id;
"

oracle "virtual_basic_arithmetic" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a  INT,
  b  INT,
  sum INT GENERATED ALWAYS AS (a + b) VIRTUAL
);
INSERT INTO t(id, a, b) VALUES(1, 3, 4), (2, 10, 20);
SELECT id, a, b, sum FROM t ORDER BY id;
"

oracle "stored_shorthand_syntax" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT AS (a * 2) STORED
);
INSERT INTO t(id, a) VALUES(1, 5), (2, 7);
SELECT id, a, doubled FROM t ORDER BY id;
"

oracle "stored_string_concat" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  first TEXT,
  last  TEXT,
  full  TEXT GENERATED ALWAYS AS (first || ' ' || last) STORED
);
INSERT INTO t(id, first, last) VALUES(1, 'ada', 'lovelace');
SELECT id, full FROM t;
"

oracle "virtual_function_call" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  name TEXT,
  name_len INT GENERATED ALWAYS AS (length(name)) VIRTUAL
);
INSERT INTO t(id, name) VALUES(1, 'abc'), (2, 'abcdef');
SELECT id, name, name_len FROM t ORDER BY id;
"

oracle "stored_case_expression" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  v INT,
  sign TEXT GENERATED ALWAYS AS (
    CASE WHEN v > 0 THEN 'pos'
         WHEN v < 0 THEN 'neg'
         ELSE 'zero' END
  ) STORED
);
INSERT INTO t(id, v) VALUES(1, 10), (2, -3), (3, 0);
SELECT id, v, sign FROM t ORDER BY id;
"

echo "--- NULL handling ---"

oracle "null_input_propagates" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  sum INT GENERATED ALWAYS AS (a + b) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 1, NULL);
INSERT INTO t(id, a, b) VALUES(2, NULL, 2);
INSERT INTO t(id, a, b) VALUES(3, NULL, NULL);
SELECT id, a, b, sum FROM t ORDER BY id;
"

oracle "coalesce_in_expression" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  safe_sum INT GENERATED ALWAYS AS (coalesce(a, 0) + coalesce(b, 0)) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 1, NULL), (2, NULL, 2), (3, NULL, NULL);
SELECT id, safe_sum FROM t ORDER BY id;
"

echo "--- UPDATE propagation ---"

oracle "update_base_cascades_stored" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  sum INT GENERATED ALWAYS AS (a + b) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 1, 2);
UPDATE t SET a = 100 WHERE id = 1;
SELECT id, a, b, sum FROM t;
UPDATE t SET b = 200 WHERE id = 1;
SELECT id, a, b, sum FROM t;
"

oracle "update_base_cascades_virtual" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  sum INT GENERATED ALWAYS AS (a + b) VIRTUAL
);
INSERT INTO t(id, a, b) VALUES(1, 1, 2);
UPDATE t SET a = 100 WHERE id = 1;
SELECT id, a, b, sum FROM t;
"

oracle "update_both_bases_single_stmt" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  prod INT GENERATED ALWAYS AS (a * b) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 2, 3);
UPDATE t SET a = 10, b = 20 WHERE id = 1;
SELECT id, a, b, prod FROM t;
"

echo "--- illegal specification ---"

oracle "insert_into_generated_fails" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT GENERATED ALWAYS AS (a * 2) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 5, 999);
SELECT id, a, b FROM t;
"

echo "--- constraints ---"

oracle "check_on_generated_violates" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED CHECK (doubled < 100)
);
INSERT INTO t(id, a) VALUES(1, 40);
INSERT INTO t(id, a) VALUES(2, 60);
SELECT id, a, doubled FROM t ORDER BY id;
"

oracle "check_on_generated_passes" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED CHECK (doubled < 100)
);
INSERT INTO t(id, a) VALUES(1, 10), (2, 20), (3, 49);
SELECT id, doubled FROM t ORDER BY id;
"

oracle "unique_on_stored_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  name TEXT,
  lower_name TEXT GENERATED ALWAYS AS (lower(name)) STORED UNIQUE
);
INSERT INTO t(id, name) VALUES(1, 'Alice');
INSERT INTO t(id, name) VALUES(2, 'alice');
SELECT id, name, lower_name FROM t ORDER BY id;
"

oracle "unique_index_on_virtual_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  name TEXT,
  lower_name TEXT GENERATED ALWAYS AS (lower(name)) VIRTUAL
);
CREATE UNIQUE INDEX idx_lower ON t(lower_name);
INSERT INTO t(id, name) VALUES(1, 'Alice');
INSERT INTO t(id, name) VALUES(2, 'alice');
SELECT id, name FROM t ORDER BY id;
"

echo "--- indexes ---"

oracle "index_on_stored_generated_seek" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED
);
CREATE INDEX idx_doubled ON t(doubled);
INSERT INTO t(id, a) VALUES(1, 1), (2, 5), (3, 10), (4, 20);
SELECT id, a, doubled FROM t WHERE doubled = 10;
SELECT id FROM t WHERE doubled BETWEEN 5 AND 30 ORDER BY doubled;
"

oracle "index_on_virtual_generated_seek" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL
);
CREATE INDEX idx_doubled ON t(doubled);
INSERT INTO t(id, a) VALUES(1, 1), (2, 5), (3, 10), (4, 20);
SELECT id, a, doubled FROM t WHERE doubled = 10;
"

oracle "order_by_virtual_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  name TEXT,
  name_len INT GENERATED ALWAYS AS (length(name)) VIRTUAL
);
INSERT INTO t(id, name) VALUES(1, 'abcdef'), (2, 'a'), (3, 'abc');
SELECT id, name FROM t ORDER BY name_len;
"

echo "--- chains ---"

oracle "chain_stored_references_stored" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED,
  plus_one INT GENERATED ALWAYS AS (doubled + 1) STORED
);
INSERT INTO t(id, a) VALUES(1, 5), (2, 10);
SELECT id, a, doubled, plus_one FROM t ORDER BY id;
"

oracle "chain_virtual_references_virtual" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL,
  plus_one INT GENERATED ALWAYS AS (doubled + 1) VIRTUAL
);
INSERT INTO t(id, a) VALUES(1, 5);
SELECT id, doubled, plus_one FROM t;
"

echo "--- ALTER TABLE ADD ---"

oracle "alter_add_stored_rejected" "
CREATE TABLE t(id INT PRIMARY KEY, a INT);
INSERT INTO t(id, a) VALUES(1, 5);
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) STORED;
SELECT id, a FROM t;
"

oracle "alter_add_virtual_accepted" "
CREATE TABLE t(id INT PRIMARY KEY, a INT);
INSERT INTO t(id, a) VALUES(1, 5), (2, 10);
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
SELECT id, a, doubled FROM t ORDER BY id;
"

echo "--- query-path references ---"

oracle "where_on_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  sum INT GENERATED ALWAYS AS (a + b) STORED
);
INSERT INTO t(id, a, b) VALUES(1,1,1),(2,2,2),(3,3,3),(4,4,4);
SELECT id, sum FROM t WHERE sum > 4 ORDER BY id;
"

oracle "group_by_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  label TEXT,
  upper_label TEXT GENERATED ALWAYS AS (upper(label)) VIRTUAL
);
INSERT INTO t(id, label) VALUES(1, 'a'), (2, 'A'), (3, 'b'), (4, 'B'), (5, 'a');
SELECT upper_label, count(*) FROM t GROUP BY upper_label ORDER BY upper_label;
"

oracle "aggregate_over_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT,
  prod INT GENERATED ALWAYS AS (a * b) STORED
);
INSERT INTO t(id, a, b) VALUES(1, 2, 3), (2, 4, 5), (3, 6, 7);
SELECT sum(prod), avg(prod), max(prod), min(prod) FROM t;
"

echo "--- collation ---"

oracle "collation_nocase_on_base_column" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  name TEXT COLLATE NOCASE,
  upper_name TEXT GENERATED ALWAYS AS (upper(name)) VIRTUAL
);
INSERT INTO t(id, name) VALUES(1, 'alice'), (2, 'Alice');
SELECT id, name, upper_name FROM t WHERE name = 'ALICE' ORDER BY id;
"

echo "--- REPLACE / UPSERT ---"

oracle "replace_recomputes_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED
);
INSERT INTO t(id, a) VALUES(1, 5);
REPLACE INTO t(id, a) VALUES(1, 25);
SELECT id, a, doubled FROM t;
"

oracle "upsert_do_update_recomputes_generated" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED
);
INSERT INTO t(id, a) VALUES(1, 5);
INSERT INTO t(id, a) VALUES(1, 100)
  ON CONFLICT(id) DO UPDATE SET a = excluded.a;
SELECT id, a, doubled FROM t;
"

echo "--- savepoint ---"

oracle "rollback_to_savepoint_undoes_generated_update" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT GENERATED ALWAYS AS (a + 1) STORED
);
INSERT INTO t(id, a) VALUES(1, 5);
SAVEPOINT s;
UPDATE t SET a = 100;
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
SELECT id, a, b FROM t;
"

echo "--- deep chains ---"

oracle "four_deep_stored_chain" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT GENERATED ALWAYS AS (a + 1) STORED,
  c INT GENERATED ALWAYS AS (b * 2) STORED,
  d INT GENERATED ALWAYS AS (c - 3) STORED
);
INSERT INTO t(id, a) VALUES(1, 10);
SELECT id, a, b, c, d FROM t;
UPDATE t SET a = 100 WHERE id = 1;
SELECT id, a, b, c, d FROM t;
"

oracle "four_deep_virtual_chain" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  b INT GENERATED ALWAYS AS (a + 1) VIRTUAL,
  c INT GENERATED ALWAYS AS (b * 2) VIRTUAL,
  d INT GENERATED ALWAYS AS (c - 3) VIRTUAL
);
INSERT INTO t(id, a) VALUES(1, 10);
SELECT id, a, b, c, d FROM t;
UPDATE t SET a = 100 WHERE id = 1;
SELECT id, a, b, c, d FROM t;
"

echo "--- bulk ---"

make_inserts() {
  local n="$1"
  local i
  for i in $(seq 1 "$n"); do
    echo "INSERT INTO t(id, a) VALUES($i, $i);"
  done
}

oracle "bulk_100_stored" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  sq INT GENERATED ALWAYS AS (a * a) STORED
);
$(make_inserts 100)
SELECT count(*), sum(sq) FROM t;
"

oracle "bulk_100_virtual" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  sq INT GENERATED ALWAYS AS (a * a) VIRTUAL
);
$(make_inserts 100)
SELECT count(*), sum(sq) FROM t;
"

echo "--- DELETE on generated-col index ---"

oracle "delete_via_generated_index" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT,
  sq INT GENERATED ALWAYS AS (a * a) STORED
);
CREATE INDEX idx_sq ON t(sq);
INSERT INTO t(id, a) VALUES(1, 2), (2, 3), (3, 4), (4, 5);
DELETE FROM t WHERE sq > 10;
SELECT id, a, sq FROM t ORDER BY id;
"

echo "--- illegal self-reference ---"

oracle "self_reference_rejected" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  a INT GENERATED ALWAYS AS (a + 1) STORED
);
INSERT INTO t(id) VALUES(1);
"

oracle "forward_reference_ok" "
CREATE TABLE t(
  id INT PRIMARY KEY,
  doubled INT GENERATED ALWAYS AS (a * 2) STORED,
  a INT
);
INSERT INTO t(id, a) VALUES(1, 7);
SELECT id, a, doubled FROM t;
"


echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
