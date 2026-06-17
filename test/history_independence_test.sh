#!/bin/bash

set -euo pipefail

DOLTLITE="${1:-./doltlite}"
LARGE_N="${HISTORY_INDEPENDENCE_LARGE_N:-5000}"
LARGE_TEMP_END=$((LARGE_N + 500))
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT

PASS=0
FAIL=0
FAILED=""

hash_text() {
  if command -v sha1sum >/dev/null 2>&1; then
    sha1sum | awk '{print $1}'
  else
    shasum -a 1 | awk '{print $1}'
  fi
}

run_setup() {
  local db="$1"
  local name="$2"
  local sql="$3"
  rm -f "$db"
  printf '%s\n' "$sql" | "$DOLTLITE" "$db" >"$TMPROOT/$name.out" 2>"$TMPROOT/$name.err"
}

run_setup_read() {
  local db="$1"
  local name="$2"
  local sql="$3"
  local script="$TMPROOT/$name.sql"
  rm -f "$db" "$script"
  printf '%s\n' "$sql" >"$script"
  printf '.read %s\n' "$script" | "$DOLTLITE" "$db" >"$TMPROOT/$name.out" 2>"$TMPROOT/$name.err"
}

run_query() {
  local db="$1"
  local name="$2"
  local sql="$3"
  "$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$name.query.err"
}

canonical_digest() {
  local db="$1"
  local name="$2"
  local sql="$3"
  run_query "$db" "$name" "$sql" | hash_text
}

db_hash() {
  local db="$1"
  local name="$2"
  run_query "$db" "$name" "SELECT dolt_hashof_db();" | tr -d '\n'
}

table_hash() {
  local db="$1"
  local name="$2"
  run_query "$db" "$name" "SELECT dolt_hashof_table('t');" | tr -d '\n'
}

catalog_hash() {
  local db="$1"
  local name="$2"
  run_query "$db" "$name" "SELECT dolt_hashof_catalog();" | tr -d '\n'
}

assert_equal() {
  local name="$1"
  local a="$2"
  local b="$3"
  if [ "$a" = "$b" ]; then
    PASS=$((PASS+1))
    echo "  PASS: $name"
  else
    FAIL=$((FAIL+1))
    FAILED="$FAILED $name"
    echo "  FAIL: $name"
    echo "    a=$a"
    echo "    b=$b"
  fi
}

assert_hash_shape() {
  local name="$1"
  local h="$2"
  if echo "$h" | grep -qE '^[0-9a-f]{40}$'; then
    PASS=$((PASS+1))
    echo "  PASS: $name"
  else
    FAIL=$((FAIL+1))
    FAILED="$FAILED $name"
    echo "  FAIL: $name"
    echo "    h=|$h|"
  fi
}

run_family_case() {
  local family="$1"
  local canonical_sql="$2"
  local history_a="$3"
  local history_b="$4"
  local history_c="$5"

  local db_a="$TMPROOT/${family}_a.db"
  local db_b="$TMPROOT/${family}_b.db"
  local db_c="$TMPROOT/${family}_c.db"

  echo "--- $family ---"

  run_setup "$db_a" "${family}_a" "$history_a"
  run_setup "$db_b" "${family}_b" "$history_b"
  run_setup "$db_c" "${family}_c" "$history_c"

  local digest_a digest_b digest_c
  digest_a=$(canonical_digest "$db_a" "${family}_a_canon" "$canonical_sql")
  digest_b=$(canonical_digest "$db_b" "${family}_b_canon" "$canonical_sql")
  digest_c=$(canonical_digest "$db_c" "${family}_c_canon" "$canonical_sql")

  assert_equal "${family}_visible_state_a_vs_b" "$digest_a" "$digest_b"
  assert_equal "${family}_visible_state_a_vs_c" "$digest_a" "$digest_c"

  local hash_a hash_b hash_c
  hash_a=$(db_hash "$db_a" "${family}_a_hash")
  hash_b=$(db_hash "$db_b" "${family}_b_hash")
  hash_c=$(db_hash "$db_c" "${family}_c_hash")

  assert_hash_shape "${family}_db_hash_shape_a" "$hash_a"
  assert_hash_shape "${family}_db_hash_shape_b" "$hash_b"
  assert_hash_shape "${family}_db_hash_shape_c" "$hash_c"
  assert_equal "${family}_db_hash_a_vs_b" "$hash_a" "$hash_b"
  assert_equal "${family}_db_hash_a_vs_c" "$hash_a" "$hash_c"

  local cat_a cat_b cat_c
  cat_a=$(catalog_hash "$db_a" "${family}_a_catalog")
  cat_b=$(catalog_hash "$db_b" "${family}_b_catalog")
  cat_c=$(catalog_hash "$db_c" "${family}_c_catalog")

  assert_hash_shape "${family}_catalog_hash_shape_a" "$cat_a"
  assert_hash_shape "${family}_catalog_hash_shape_b" "$cat_b"
  assert_hash_shape "${family}_catalog_hash_shape_c" "$cat_c"
  assert_equal "${family}_catalog_hash_a_vs_b" "$cat_a" "$cat_b"
  assert_equal "${family}_catalog_hash_a_vs_c" "$cat_a" "$cat_c"

  echo ""
}

run_branch_case() {
  local family="$1"
  local canonical_sql="$2"
  local history_a="$3"
  local history_b="$4"
  local history_c="$5"

  local db_a="$TMPROOT/${family}_branch_a.db"
  local db_b="$TMPROOT/${family}_branch_b.db"
  local db_c="$TMPROOT/${family}_branch_c.db"

  echo "--- $family branch histories ---"

  run_setup "$db_a" "${family}_branch_a" "$history_a"
  run_setup "$db_b" "${family}_branch_b" "$history_b"
  run_setup "$db_c" "${family}_branch_c" "$history_c"

  local digest_a digest_b digest_c
  digest_a=$(canonical_digest "$db_a" "${family}_branch_a_canon" "$canonical_sql")
  digest_b=$(canonical_digest "$db_b" "${family}_branch_b_canon" "$canonical_sql")
  digest_c=$(canonical_digest "$db_c" "${family}_branch_c_canon" "$canonical_sql")

  assert_equal "${family}_branch_visible_state_a_vs_b" "$digest_a" "$digest_b"
  assert_equal "${family}_branch_visible_state_a_vs_c" "$digest_a" "$digest_c"

  local hash_a hash_b hash_c
  hash_a=$(db_hash "$db_a" "${family}_branch_a_hash")
  hash_b=$(db_hash "$db_b" "${family}_branch_b_hash")
  hash_c=$(db_hash "$db_c" "${family}_branch_c_hash")

  assert_hash_shape "${family}_branch_db_hash_shape_a" "$hash_a"
  assert_hash_shape "${family}_branch_db_hash_shape_b" "$hash_b"
  assert_hash_shape "${family}_branch_db_hash_shape_c" "$hash_c"
  assert_equal "${family}_branch_db_hash_a_vs_b" "$hash_a" "$hash_b"
  assert_equal "${family}_branch_db_hash_a_vs_c" "$hash_a" "$hash_c"

  echo ""
}

run_read_case() {
  local family="$1"
  local canonical_sql="$2"
  local history_a="$3"
  local history_b="$4"
  local history_c="$5"

  local db_a="$TMPROOT/${family}_read_a.db"
  local db_b="$TMPROOT/${family}_read_b.db"
  local db_c="$TMPROOT/${family}_read_c.db"

  echo "--- $family .read histories ---"

  run_setup "$db_a" "${family}_read_a" "$history_a"
  run_setup_read "$db_b" "${family}_read_b" "$history_b"
  run_setup_read "$db_c" "${family}_read_c" "$history_c"

  local digest_a digest_b digest_c
  digest_a=$(canonical_digest "$db_a" "${family}_read_a_canon" "$canonical_sql")
  digest_b=$(canonical_digest "$db_b" "${family}_read_b_canon" "$canonical_sql")
  digest_c=$(canonical_digest "$db_c" "${family}_read_c_canon" "$canonical_sql")

  assert_equal "${family}_read_visible_state_a_vs_b" "$digest_a" "$digest_b"
  assert_equal "${family}_read_visible_state_a_vs_c" "$digest_a" "$digest_c"

  local hash_a hash_b hash_c
  hash_a=$(db_hash "$db_a" "${family}_read_a_hash")
  hash_b=$(db_hash "$db_b" "${family}_read_b_hash")
  hash_c=$(db_hash "$db_c" "${family}_read_c_hash")

  assert_hash_shape "${family}_read_db_hash_shape_a" "$hash_a"
  assert_hash_shape "${family}_read_db_hash_shape_b" "$hash_b"
  assert_hash_shape "${family}_read_db_hash_shape_c" "$hash_c"
  assert_equal "${family}_read_db_hash_a_vs_b" "$hash_a" "$hash_b"
  assert_equal "${family}_read_db_hash_a_vs_c" "$hash_a" "$hash_c"

  echo ""
}

run_index_case() {
  local family="$1"
  local canonical_sql="$2"
  local history_a="$3"
  local history_b="$4"
  local history_c="$5"
  local index_probe_sql="$6"

  local db_a="$TMPROOT/${family}_idx_a.db"
  local db_b="$TMPROOT/${family}_idx_b.db"
  local db_c="$TMPROOT/${family}_idx_c.db"

  echo "--- $family indexed histories ---"

  run_setup "$db_a" "${family}_idx_a" "$history_a"
  run_setup "$db_b" "${family}_idx_b" "$history_b"
  run_setup "$db_c" "${family}_idx_c" "$history_c"

  local digest_a digest_b digest_c
  digest_a=$(canonical_digest "$db_a" "${family}_idx_a_canon" "$canonical_sql")
  digest_b=$(canonical_digest "$db_b" "${family}_idx_b_canon" "$canonical_sql")
  digest_c=$(canonical_digest "$db_c" "${family}_idx_c_canon" "$canonical_sql")

  assert_equal "${family}_idx_visible_state_a_vs_b" "$digest_a" "$digest_b"
  assert_equal "${family}_idx_visible_state_a_vs_c" "$digest_a" "$digest_c"

  local probe_a probe_b probe_c
  probe_a=$(canonical_digest "$db_a" "${family}_idx_a_probe" "$index_probe_sql")
  probe_b=$(canonical_digest "$db_b" "${family}_idx_b_probe" "$index_probe_sql")
  probe_c=$(canonical_digest "$db_c" "${family}_idx_c_probe" "$index_probe_sql")

  assert_equal "${family}_idx_index_probe_a_vs_b" "$probe_a" "$probe_b"
  assert_equal "${family}_idx_index_probe_a_vs_c" "$probe_a" "$probe_c"

  local hash_a hash_b hash_c
  hash_a=$(db_hash "$db_a" "${family}_idx_a_hash")
  hash_b=$(db_hash "$db_b" "${family}_idx_b_hash")
  hash_c=$(db_hash "$db_c" "${family}_idx_c_hash")

  assert_hash_shape "${family}_idx_db_hash_shape_a" "$hash_a"
  assert_hash_shape "${family}_idx_db_hash_shape_b" "$hash_b"
  assert_hash_shape "${family}_idx_db_hash_shape_c" "$hash_c"
  assert_equal "${family}_idx_db_hash_a_vs_b" "$hash_a" "$hash_b"
  assert_equal "${family}_idx_db_hash_a_vs_c" "$hash_a" "$hash_c"

  echo ""
}

run_ddl_case() {
  local family="$1"
  local data_sql="$2"
  local schema_sql="$3"
  local history_a="$4"
  local history_b="$5"
  local history_c="$6"

  local db_a="$TMPROOT/${family}_ddl_a.db"
  local db_b="$TMPROOT/${family}_ddl_b.db"
  local db_c="$TMPROOT/${family}_ddl_c.db"

  echo "--- $family ddl histories ---"

  run_setup "$db_a" "${family}_ddl_a" "$history_a"
  run_setup "$db_b" "${family}_ddl_b" "$history_b"
  run_setup "$db_c" "${family}_ddl_c" "$history_c"

  local data_a data_b data_c
  data_a=$(canonical_digest "$db_a" "${family}_ddl_a_data" "$data_sql")
  data_b=$(canonical_digest "$db_b" "${family}_ddl_b_data" "$data_sql")
  data_c=$(canonical_digest "$db_c" "${family}_ddl_c_data" "$data_sql")

  assert_equal "${family}_ddl_data_a_vs_b" "$data_a" "$data_b"
  assert_equal "${family}_ddl_data_a_vs_c" "$data_a" "$data_c"

  local schema_a schema_b schema_c
  schema_a=$(canonical_digest "$db_a" "${family}_ddl_a_schema" "$schema_sql")
  schema_b=$(canonical_digest "$db_b" "${family}_ddl_b_schema" "$schema_sql")
  schema_c=$(canonical_digest "$db_c" "${family}_ddl_c_schema" "$schema_sql")

  assert_equal "${family}_ddl_schema_a_vs_b" "$schema_a" "$schema_b"
  assert_equal "${family}_ddl_schema_a_vs_c" "$schema_a" "$schema_c"

  local table_a table_b table_c
  table_a=$(table_hash "$db_a" "${family}_ddl_a_table_hash")
  table_b=$(table_hash "$db_b" "${family}_ddl_b_table_hash")
  table_c=$(table_hash "$db_c" "${family}_ddl_c_table_hash")

  assert_hash_shape "${family}_ddl_table_hash_shape_a" "$table_a"
  assert_hash_shape "${family}_ddl_table_hash_shape_b" "$table_b"
  assert_hash_shape "${family}_ddl_table_hash_shape_c" "$table_c"
  assert_equal "${family}_ddl_table_hash_a_vs_b" "$table_a" "$table_b"
  assert_equal "${family}_ddl_table_hash_a_vs_c" "$table_a" "$table_c"

  local hash_a hash_b hash_c
  hash_a=$(db_hash "$db_a" "${family}_ddl_a_hash")
  hash_b=$(db_hash "$db_b" "${family}_ddl_b_hash")
  hash_c=$(db_hash "$db_c" "${family}_ddl_c_hash")

  assert_hash_shape "${family}_ddl_db_hash_shape_a" "$hash_a"
  assert_hash_shape "${family}_ddl_db_hash_shape_b" "$hash_b"
  assert_hash_shape "${family}_ddl_db_hash_shape_c" "$hash_c"
  assert_equal "${family}_ddl_db_hash_a_vs_b" "$hash_a" "$hash_b"
  assert_equal "${family}_ddl_db_hash_a_vs_c" "$hash_a" "$hash_c"

  local cat_a cat_b cat_c
  cat_a=$(catalog_hash "$db_a" "${family}_ddl_a_catalog")
  cat_b=$(catalog_hash "$db_b" "${family}_ddl_b_catalog")
  cat_c=$(catalog_hash "$db_c" "${family}_ddl_c_catalog")

  assert_hash_shape "${family}_ddl_catalog_hash_shape_a" "$cat_a"
  assert_hash_shape "${family}_ddl_catalog_hash_shape_b" "$cat_b"
  assert_hash_shape "${family}_ddl_catalog_hash_shape_c" "$cat_c"
  assert_equal "${family}_ddl_catalog_hash_a_vs_b" "$cat_a" "$cat_b"
  assert_equal "${family}_ddl_catalog_hash_a_vs_c" "$cat_a" "$cat_c"

  echo ""
}

echo "=== History Independence Tests ==="
echo ""

run_family_case \
  "int_pk" \
  "SELECT printf('%d|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (3, 'charlie', 30);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (1, 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
INSERT INTO t VALUES (99, 'temp', 990);
INSERT INTO t VALUES (2, 'wrong', 0);
SAVEPOINT s1;
INSERT INTO t VALUES (101, 'rolled', 1010);
ROLLBACK TO s1;
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (3, 'charlie', 30);
DELETE FROM t WHERE id = 99;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET v = 'bravo', n = 20 WHERE id = 2;
UPDATE t SET v = 'charles' WHERE id = 3;
UPDATE t SET v = 'charlie' WHERE id = 3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_family_case \
  "text_pk" \
  "SELECT printf('%s|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
INSERT INTO t VALUES ('z-temp', 'temp', 999);
INSERT INTO t VALUES ('b-key', 'wrong', 0);
SAVEPOINT s1;
INSERT INTO t VALUES ('rolled-key', 'rolled', 1010);
ROLLBACK TO s1;
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
DELETE FROM t WHERE id = 'z-temp';
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET v = 'bravo', n = 20 WHERE id = 'b-key';
UPDATE t SET n = 31 WHERE id = 'c-key';
UPDATE t SET n = 30 WHERE id = 'c-key';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_family_case \
  "blob_pk" \
  "SELECT printf('%s|%s|%d', hex(id), v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'03', 'charlie', 30);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'01', 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
INSERT INTO t VALUES (x'99', 'temp', 999);
INSERT INTO t VALUES (x'02', 'wrong', 0);
SAVEPOINT s1;
INSERT INTO t VALUES (x'AA', 'rolled', 1010);
ROLLBACK TO s1;
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'03', 'charlie', 30);
DELETE FROM t WHERE id = x'99';
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET v = 'bravo', n = 20 WHERE id = x'02';
UPDATE t SET v = 'charles' WHERE id = x'03';
UPDATE t SET v = 'charlie' WHERE id = x'03';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_family_case \
  "composite_pk" \
  "SELECT printf('%s|%d|%s|%d', a, b, v, n) FROM t ORDER BY a, b;" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
INSERT INTO t VALUES ('z', 99, 'temp', 999);
INSERT INTO t VALUES ('b', 2, 'wrong', 0);
SAVEPOINT s1;
INSERT INTO t VALUES ('rolled', 101, 'rolled', 1010);
ROLLBACK TO s1;
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
DELETE FROM t WHERE a = 'z' AND b = 99;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET v = 'bravo', n = 20 WHERE a = 'b' AND b = 2;
UPDATE t SET n = 21 WHERE a = 'b' AND b = 2;
UPDATE t SET n = 20 WHERE a = 'b' AND b = 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_branch_case \
  "int_pk" \
  "SELECT printf('%d|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10), (2, 'bravo', 20), (3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (1, 'alpha', 10), (2, 'bravo', 20), (3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2, 'bravo', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main rows');
SELECT dolt_merge('feat');
"

run_branch_case \
  "text_pk" \
  "SELECT printf('%s|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10), ('b-key', 'bravo', 20), ('c-key', 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('a-key', 'alpha', 10), ('b-key', 'bravo', 20), ('c-key', 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('b-key', 'bravo', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES ('c-key', 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main rows');
SELECT dolt_merge('feat');
"

run_branch_case \
  "blob_pk" \
  "SELECT printf('%s|%s|%d', hex(id), v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10), (x'02', 'bravo', 20), (x'03', 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (x'01', 'alpha', 10), (x'02', 'bravo', 20), (x'03', 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (x'02', 'bravo', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (x'03', 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main rows');
SELECT dolt_merge('feat');
"

run_branch_case \
  "composite_pk" \
  "SELECT printf('%s|%d|%s|%d', a, b, v, n) FROM t ORDER BY a, b;" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10), ('b', 2, 'bravo', 20), ('c', 3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('a', 1, 'alpha', 10), ('b', 2, 'bravo', 20), ('c', 3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main rows');
SELECT dolt_merge('feat');
"

echo "--- large datasets (${LARGE_N} rows) ---"

run_family_case \
  "large_int_pk" \
  "SELECT printf('%d|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT x, printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT x, printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT x,
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT x, printf('temp%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE id <= ${LARGE_N} AND id % 10 = 0;
DELETE FROM t WHERE id > ${LARGE_N};
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_family_case \
  "large_text_pk" \
  "SELECT printf('%s|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x), printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x), printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x),
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT printf('temp%05d', x), printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE id <= printf('k%05d', ${LARGE_N}) AND CAST(substr(id, 2) AS INTEGER) % 10 = 0;
DELETE FROM t WHERE id LIKE 'temp%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_family_case \
  "large_blob_pk" \
  "SELECT printf('%s|%s|%d', hex(id), v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB), printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB), printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB),
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT CAST(printf('temp%05d', x) AS BLOB), printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t
SET n = n - 1
WHERE CAST(substr(CAST(id AS TEXT), 2) AS INTEGER) <= ${LARGE_N}
  AND CAST(substr(CAST(id AS TEXT), 2) AS INTEGER) % 10 = 0;
DELETE FROM t WHERE CAST(id AS TEXT) LIKE 'temp%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_family_case \
  "large_composite_pk" \
  "SELECT printf('%s|%d|%s|%d', a, b, v, n) FROM t ORDER BY a, b;" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000), x, printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000), x, printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000),
       x,
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT 'temp', x, printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE a != 'temp' AND b % 10 = 0;
DELETE FROM t WHERE a = 'temp';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

echo "--- large .read histories (${LARGE_N} rows) ---"

run_read_case \
  "read_large_int_pk" \
  "SELECT printf('%d|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT x, printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT x, printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT x,
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT x, printf('temp%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE id <= ${LARGE_N} AND id % 10 = 0;
DELETE FROM t WHERE id > ${LARGE_N};
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_read_case \
  "read_large_text_pk" \
  "SELECT printf('%s|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x), printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x), printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x),
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT printf('temp%05d', x), printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE id <= printf('k%05d', ${LARGE_N}) AND CAST(substr(id, 2) AS INTEGER) % 10 = 0;
DELETE FROM t WHERE id LIKE 'temp%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_read_case \
  "read_large_blob_pk" \
  "SELECT printf('%s|%s|%d', hex(id), v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB), printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB), printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB),
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT CAST(printf('temp%05d', x) AS BLOB), printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t
SET n = n - 1
WHERE CAST(substr(CAST(id AS TEXT), 2) AS INTEGER) <= ${LARGE_N}
  AND CAST(substr(CAST(id AS TEXT), 2) AS INTEGER) % 10 = 0;
DELETE FROM t WHERE CAST(id AS TEXT) LIKE 'temp%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_read_case \
  "read_large_composite_pk" \
  "SELECT printf('%s|%d|%s|%d', a, b, v, n) FROM t ORDER BY a, b;" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000), x, printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000), x, printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000),
       x,
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT 'temp', x, printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE a != 'temp' AND b % 10 = 0;
DELETE FROM t WHERE a = 'temp';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

echo "--- large indexed histories (${LARGE_N} rows) ---"

run_index_case \
  "idx_large_text_pk" \
  "SELECT printf('%s|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x), printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x), printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('k%05d', x),
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT printf('temp%05d', x), printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE CAST(substr(id, 2) AS INTEGER) <= ${LARGE_N} AND CAST(substr(id, 2) AS INTEGER) % 10 = 0;
DELETE FROM t WHERE id LIKE 'temp%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "SELECT printf('%s|%s|%d', id, v, n) FROM t INDEXED BY idx_v WHERE v IN ('v00001','v01000','v02500','v05000') ORDER BY v;"

run_index_case \
  "idx_large_blob_pk" \
  "SELECT printf('%s|%s|%d', hex(id), v, n) FROM t ORDER BY id;" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB), printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB), printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT CAST(printf('k%05d', x) AS BLOB),
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT CAST(printf('temp%05d', x) AS BLOB), printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t
SET n = n - 1
WHERE CAST(substr(CAST(id AS TEXT), 2) AS INTEGER) <= ${LARGE_N}
  AND CAST(substr(CAST(id AS TEXT), 2) AS INTEGER) % 10 = 0;
DELETE FROM t WHERE CAST(id AS TEXT) LIKE 'temp%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "SELECT printf('%s|%s|%d', hex(id), v, n) FROM t INDEXED BY idx_v WHERE v IN ('v00001','v01000','v02500','v05000') ORDER BY v;"

run_index_case \
  "idx_large_composite_pk" \
  "SELECT printf('%s|%d|%s|%d', a, b, v, n) FROM t ORDER BY a, b;" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000), x, printf('v%05d', x), x * 10 FROM seq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
SELECT dolt_commit('-A', '-m', 'schema');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000), x, printf('v%05d', x), x * 10 FROM seq ORDER BY x DESC;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat rows');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_n ON t(n);
BEGIN;
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < ${LARGE_N}
)
INSERT INTO t
SELECT printf('grp%02d', (x - 1) / 1000),
       x,
       printf('v%05d', x),
       x * 10 + CASE WHEN x % 10 = 0 THEN 1 ELSE 0 END
FROM seq;
WITH RECURSIVE tempseq(x) AS (
  SELECT ${LARGE_N} + 1
  UNION ALL
  SELECT x + 1 FROM tempseq WHERE x < ${LARGE_TEMP_END}
)
INSERT INTO t
SELECT 'temp', x, printf('tempv%05d', x), x * 10 FROM tempseq;
COMMIT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'intermediate');
UPDATE t SET n = n - 1 WHERE a != 'temp' AND b % 10 = 0;
DELETE FROM t WHERE a = 'temp';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "SELECT printf('%s|%d|%s|%d', a, b, v, n) FROM t INDEXED BY idx_v WHERE v IN ('v00001','v01000','v02500','v05000') ORDER BY v;"

echo "--- ddl histories ---"

run_ddl_case \
  "ddl_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, v, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed'
);
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  v TEXT,
  n INTEGER
);
INSERT INTO t(id, v, n) VALUES (1, 'alpha', 10);
INSERT INTO t(id, v, n) VALUES (2, 'bravo', 20);
INSERT INTO t(id, v, n) VALUES (3, 'charlie', 30);
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(
  id INTEGER PRIMARY KEY,
  v TEXT,
  n INTEGER
);
INSERT INTO t_old VALUES (1, 'alpha', 10);
INSERT INTO t_old VALUES (2, 'bravo', 20);
INSERT INTO t_old VALUES (3, 'charlie', 30);
CREATE TABLE t_new(
  id INTEGER PRIMARY KEY,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed'
);
INSERT INTO t_new(id, v, n, extra)
SELECT id, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, v, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(
  id TEXT PRIMARY KEY,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed'
);
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(
  id TEXT PRIMARY KEY,
  v TEXT,
  n INTEGER
);
INSERT INTO t(id, v, n) VALUES ('a-key', 'alpha', 10);
INSERT INTO t(id, v, n) VALUES ('b-key', 'bravo', 20);
INSERT INTO t(id, v, n) VALUES ('c-key', 'charlie', 30);
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(
  id TEXT PRIMARY KEY,
  v TEXT,
  n INTEGER
);
INSERT INTO t_old VALUES ('a-key', 'alpha', 10);
INSERT INTO t_old VALUES ('b-key', 'bravo', 20);
INSERT INTO t_old VALUES ('c-key', 'charlie', 30);
CREATE TABLE t_new(
  id TEXT PRIMARY KEY,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed'
);
INSERT INTO t_new(id, v, n, extra)
SELECT id, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), v, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(
  id BLOB PRIMARY KEY,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed'
);
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(
  id BLOB PRIMARY KEY,
  v TEXT,
  n INTEGER
);
INSERT INTO t(id, v, n) VALUES (x'01', 'alpha', 10);
INSERT INTO t(id, v, n) VALUES (x'02', 'bravo', 20);
INSERT INTO t(id, v, n) VALUES (x'03', 'charlie', 30);
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(
  id BLOB PRIMARY KEY,
  v TEXT,
  n INTEGER
);
INSERT INTO t_old VALUES (x'01', 'alpha', 10);
INSERT INTO t_old VALUES (x'02', 'bravo', 20);
INSERT INTO t_old VALUES (x'03', 'charlie', 30);
CREATE TABLE t_new(
  id BLOB PRIMARY KEY,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed'
);
INSERT INTO t_new(id, v, n, extra)
SELECT id, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, v, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(
  a TEXT,
  b INTEGER,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed',
  PRIMARY KEY(a, b)
);
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(
  a TEXT,
  b INTEGER,
  v TEXT,
  n INTEGER,
  PRIMARY KEY(a, b)
);
INSERT INTO t(a, b, v, n) VALUES ('a', 1, 'alpha', 10);
INSERT INTO t(a, b, v, n) VALUES ('b', 2, 'bravo', 20);
INSERT INTO t(a, b, v, n) VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(
  a TEXT,
  b INTEGER,
  v TEXT,
  n INTEGER,
  PRIMARY KEY(a, b)
);
INSERT INTO t_old VALUES ('a', 1, 'alpha', 10);
INSERT INTO t_old VALUES ('b', 2, 'bravo', 20);
INSERT INTO t_old VALUES ('c', 3, 'charlie', 30);
CREATE TABLE t_new(
  a TEXT,
  b INTEGER,
  v TEXT,
  n INTEGER,
  extra TEXT DEFAULT 'seed',
  PRIMARY KEY(a, b)
);
INSERT INTO t_new(a, b, v, n, extra)
SELECT a, b, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (1, 'alpha', 10);
INSERT INTO t_old VALUES (2, 'bravo', 20);
INSERT INTO t_old VALUES (3, 'charlie', 30);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra) SELECT id, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES ('a-key', 'alpha', 10);
INSERT INTO t_old VALUES ('b-key', 'bravo', 20);
INSERT INTO t_old VALUES ('c-key', 'charlie', 30);
CREATE TABLE t_new(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra) SELECT id, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (x'01', 'alpha', 10);
INSERT INTO t_old VALUES (x'02', 'bravo', 20);
INSERT INTO t_old VALUES (x'03', 'charlie', 30);
CREATE TABLE t_new(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra) SELECT id, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t_old VALUES ('a', 1, 'alpha', 10);
INSERT INTO t_old VALUES ('b', 2, 'bravo', 20);
INSERT INTO t_old VALUES ('c', 3, 'charlie', 30);
CREATE TABLE t_new(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
INSERT INTO t_new(a, b, payload, n, extra) SELECT a, b, v, n, 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_twocol_int_pk" \
  "SELECT printf('%d|%s|%d|%s|%s', id, v, n, tag, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES (1, 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (1, 'alpha', 10);
INSERT INTO t_old VALUES (2, 'bravo', 20);
INSERT INTO t_old VALUES (3, 'charlie', 30);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, v, n, tag, extra) SELECT id, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_twocol_text_pk" \
  "SELECT printf('%s|%s|%d|%s|%s', id, v, n, tag, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES ('a-key', 'alpha', 10);
INSERT INTO t_old VALUES ('b-key', 'bravo', 20);
INSERT INTO t_old VALUES ('c-key', 'charlie', 30);
CREATE TABLE t_new(id TEXT PRIMARY KEY, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, v, n, tag, extra) SELECT id, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_twocol_blob_pk" \
  "SELECT printf('%s|%s|%d|%s|%s', hex(id), v, n, tag, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (x'01', 'alpha', 10);
INSERT INTO t_old VALUES (x'02', 'bravo', 20);
INSERT INTO t_old VALUES (x'03', 'charlie', 30);
CREATE TABLE t_new(id BLOB PRIMARY KEY, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, v, n, tag, extra) SELECT id, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_twocol_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s|%s', a, b, v, n, tag, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t_old VALUES ('a', 1, 'alpha', 10);
INSERT INTO t_old VALUES ('b', 2, 'bravo', 20);
INSERT INTO t_old VALUES ('c', 3, 'charlie', 30);
CREATE TABLE t_new(a TEXT, b INTEGER, v TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
INSERT INTO t_new(a, b, v, n, tag, extra) SELECT a, b, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, tag) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|uidx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'tag0');
INSERT INTO t VALUES (2, 'bravo', 20, 'tag0');
INSERT INTO t VALUES (3, 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (1, 'alpha', 10);
INSERT INTO t_old VALUES (2, 'bravo', 20);
INSERT INTO t_old VALUES (3, 'charlie', 30);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
INSERT INTO t_new(id, payload, n, tag) SELECT id, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, tag) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|uidx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'tag0');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'tag0');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES ('a-key', 'alpha', 10);
INSERT INTO t_old VALUES ('b-key', 'bravo', 20);
INSERT INTO t_old VALUES ('c-key', 'charlie', 30);
CREATE TABLE t_new(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
INSERT INTO t_new(id, payload, n, tag) SELECT id, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, tag) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|uidx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'tag0');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'tag0');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (x'01', 'alpha', 10);
INSERT INTO t_old VALUES (x'02', 'bravo', 20);
INSERT INTO t_old VALUES (x'03', 'charlie', 30);
CREATE TABLE t_new(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
INSERT INTO t_new(id, payload, n, tag) SELECT id, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, tag) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|uidx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', PRIMARY KEY(a, b));
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'tag0');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'tag0');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t_old VALUES ('a', 1, 'alpha', 10);
INSERT INTO t_old VALUES ('b', 2, 'bravo', 20);
INSERT INTO t_old VALUES ('c', 3, 'charlie', 30);
CREATE TABLE t_new(a TEXT, b INTEGER, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', PRIMARY KEY(a, b));
INSERT INTO t_new(a, b, payload, n, tag) SELECT a, b, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE UNIQUE INDEX uidx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_index_order_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_temp_payload ON t(payload, n);
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
DROP INDEX idx_temp_payload;
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_index_order_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_temp_payload ON t(payload, n);
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
DROP INDEX idx_temp_payload;
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_index_order_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_temp_payload ON t(payload, n);
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
DROP INDEX idx_temp_payload;
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_index_order_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_temp_payload ON t(payload, n);
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
DROP INDEX idx_temp_payload;
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_chain_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE seed(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO seed VALUES (1, 'alpha', 10);
INSERT INTO seed VALUES (2, 'bravo', 20);
INSERT INTO seed VALUES (3, 'charlie', 30);
ALTER TABLE seed RENAME TO mid;
ALTER TABLE mid RENAME COLUMN v TO payload;
ALTER TABLE mid ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE mid RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO orig VALUES (1, 'alpha', 10);
INSERT INTO orig VALUES (2, 'bravo', 20);
INSERT INTO orig VALUES (3, 'charlie', 30);
ALTER TABLE orig RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_chain_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE seed(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO seed VALUES ('a-key', 'alpha', 10);
INSERT INTO seed VALUES ('b-key', 'bravo', 20);
INSERT INTO seed VALUES ('c-key', 'charlie', 30);
ALTER TABLE seed RENAME TO mid;
ALTER TABLE mid RENAME COLUMN v TO payload;
ALTER TABLE mid ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE mid RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO orig VALUES ('a-key', 'alpha', 10);
INSERT INTO orig VALUES ('b-key', 'bravo', 20);
INSERT INTO orig VALUES ('c-key', 'charlie', 30);
ALTER TABLE orig RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_chain_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE seed(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO seed VALUES (x'01', 'alpha', 10);
INSERT INTO seed VALUES (x'02', 'bravo', 20);
INSERT INTO seed VALUES (x'03', 'charlie', 30);
ALTER TABLE seed RENAME TO mid;
ALTER TABLE mid RENAME COLUMN v TO payload;
ALTER TABLE mid ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE mid RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO orig VALUES (x'01', 'alpha', 10);
INSERT INTO orig VALUES (x'02', 'bravo', 20);
INSERT INTO orig VALUES (x'03', 'charlie', 30);
ALTER TABLE orig RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_chain_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE seed(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO seed VALUES ('a', 1, 'alpha', 10);
INSERT INTO seed VALUES ('b', 2, 'bravo', 20);
INSERT INTO seed VALUES ('c', 3, 'charlie', 30);
ALTER TABLE seed RENAME TO mid;
ALTER TABLE mid RENAME COLUMN v TO payload;
ALTER TABLE mid ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE mid RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO orig VALUES ('a', 1, 'alpha', 10);
INSERT INTO orig VALUES ('b', 2, 'bravo', 20);
INSERT INTO orig VALUES ('c', 3, 'charlie', 30);
ALTER TABLE orig RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_after_column_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO orig VALUES (1, 'alpha', 10);
INSERT INTO orig VALUES (2, 'bravo', 20);
INSERT INTO orig VALUES (3, 'charlie', 30);
ALTER TABLE orig RENAME COLUMN v TO payload;
ALTER TABLE orig ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE orig RENAME TO t;
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_after_column_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO orig VALUES ('a-key', 'alpha', 10);
INSERT INTO orig VALUES ('b-key', 'bravo', 20);
INSERT INTO orig VALUES ('c-key', 'charlie', 30);
ALTER TABLE orig RENAME COLUMN v TO payload;
ALTER TABLE orig ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE orig RENAME TO t;
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_after_column_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO orig VALUES (x'01', 'alpha', 10);
INSERT INTO orig VALUES (x'02', 'bravo', 20);
INSERT INTO orig VALUES (x'03', 'charlie', 30);
ALTER TABLE orig RENAME COLUMN v TO payload;
ALTER TABLE orig ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE orig RENAME TO t;
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_after_column_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE orig(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO orig VALUES ('a', 1, 'alpha', 10);
INSERT INTO orig VALUES ('b', 2, 'bravo', 20);
INSERT INTO orig VALUES ('c', 3, 'charlie', 30);
ALTER TABLE orig RENAME COLUMN v TO payload;
ALTER TABLE orig ADD COLUMN extra TEXT DEFAULT 'seed';
ALTER TABLE orig RENAME TO t;
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_covering_index_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_extra|%d|%s', seqno, name) FROM pragma_index_info('idx_t_extra') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_extra ON t(extra);
CREATE INDEX idx_t_payload_n ON t(payload, n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO base VALUES (1, 'alpha', 10);
INSERT INTO base VALUES (2, 'bravo', 20);
INSERT INTO base VALUES (3, 'charlie', 30);
ALTER TABLE base RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_tmp_payload ON t(payload);
CREATE INDEX idx_t_payload_n ON t(payload, n);
DROP INDEX idx_tmp_payload;
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_covering_index_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_extra|%d|%s', seqno, name) FROM pragma_index_info('idx_t_extra') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_extra ON t(extra);
CREATE INDEX idx_t_payload_n ON t(payload, n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE base(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO base VALUES ('a-key', 'alpha', 10);
INSERT INTO base VALUES ('b-key', 'bravo', 20);
INSERT INTO base VALUES ('c-key', 'charlie', 30);
ALTER TABLE base RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_tmp_payload ON t(payload);
CREATE INDEX idx_t_payload_n ON t(payload, n);
DROP INDEX idx_tmp_payload;
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_covering_index_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_extra|%d|%s', seqno, name) FROM pragma_index_info('idx_t_extra') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_extra ON t(extra);
CREATE INDEX idx_t_payload_n ON t(payload, n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE base(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO base VALUES (x'01', 'alpha', 10);
INSERT INTO base VALUES (x'02', 'bravo', 20);
INSERT INTO base VALUES (x'03', 'charlie', 30);
ALTER TABLE base RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_tmp_payload ON t(payload);
CREATE INDEX idx_t_payload_n ON t(payload, n);
DROP INDEX idx_tmp_payload;
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_covering_index_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_extra|%d|%s', seqno, name) FROM pragma_index_info('idx_t_extra') ORDER BY seqno;
SELECT printf('idxcol|idx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_extra ON t(extra);
CREATE INDEX idx_t_payload_n ON t(payload, n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE base(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO base VALUES ('a', 1, 'alpha', 10);
INSERT INTO base VALUES ('b', 2, 'bravo', 20);
INSERT INTO base VALUES ('c', 3, 'charlie', 30);
ALTER TABLE base RENAME TO t;
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_tmp_payload ON t(payload);
CREATE INDEX idx_t_payload_n ON t(payload, n);
DROP INDEX idx_tmp_payload;
CREATE INDEX idx_t_extra ON t(extra);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_twocol_int_pk" \
  "SELECT printf('%d|%s|%d|%s|%s', id, payload, n, tag, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES (1, 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (1, 'alpha', 10);
INSERT INTO t_old VALUES (2, 'bravo', 20);
INSERT INTO t_old VALUES (3, 'charlie', 30);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, tag, extra) SELECT id, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_twocol_text_pk" \
  "SELECT printf('%s|%s|%d|%s|%s', id, payload, n, tag, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES ('a-key', 'alpha', 10);
INSERT INTO t_old VALUES ('b-key', 'bravo', 20);
INSERT INTO t_old VALUES ('c-key', 'charlie', 30);
CREATE TABLE t_new(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, tag, extra) SELECT id, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_twocol_blob_pk" \
  "SELECT printf('%s|%s|%d|%s|%s', hex(id), payload, n, tag, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (x'01', 'alpha', 10);
INSERT INTO t_old VALUES (x'02', 'bravo', 20);
INSERT INTO t_old VALUES (x'03', 'charlie', 30);
CREATE TABLE t_new(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, tag, extra) SELECT id, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_rename_twocol_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s|%s', a, b, payload, n, tag, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'tag0', 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'tag0', 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'tag0', 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0', extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t_old VALUES ('a', 1, 'alpha', 10);
INSERT INTO t_old VALUES ('b', 2, 'bravo', 20);
INSERT INTO t_old VALUES ('c', 3, 'charlie', 30);
CREATE TABLE t_new(a TEXT, b INTEGER, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
INSERT INTO t_new(a, b, payload, n, tag, extra) SELECT a, b, v, n, 'tag0', 'seed' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_covering_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, tag) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
SELECT printf('idxcol|uidx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES (1, 'alpha', 10, 'tag0');
INSERT INTO t VALUES (2, 'bravo', 20, 'tag0');
INSERT INTO t VALUES (3, 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (1, 'alpha', 10);
INSERT INTO t_old VALUES (2, 'bravo', 20);
INSERT INTO t_old VALUES (3, 'charlie', 30);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
INSERT INTO t_new(id, payload, n, tag) SELECT id, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_covering_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, tag) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
SELECT printf('idxcol|uidx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'tag0');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'tag0');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES ('a-key', 'alpha', 10);
INSERT INTO t_old VALUES ('b-key', 'bravo', 20);
INSERT INTO t_old VALUES ('c-key', 'charlie', 30);
CREATE TABLE t_new(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
INSERT INTO t_new(id, payload, n, tag) SELECT id, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_covering_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, tag) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
SELECT printf('idxcol|uidx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'tag0');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'tag0');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t_old VALUES (x'01', 'alpha', 10);
INSERT INTO t_old VALUES (x'02', 'bravo', 20);
INSERT INTO t_old VALUES (x'03', 'charlie', 30);
CREATE TABLE t_new(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0');
INSERT INTO t_new(id, payload, n, tag) SELECT id, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_unique_covering_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, tag) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_tag|%d|%s', seqno, name) FROM pragma_index_info('idx_t_tag') ORDER BY seqno;
SELECT printf('idxcol|uidx_t_payload_n|%d|%s', seqno, name) FROM pragma_index_info('uidx_t_payload_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', PRIMARY KEY(a, b));
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'tag0');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'tag0');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'tag0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'tag0';
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
CREATE INDEX idx_t_tag ON t(tag);
UPDATE t SET tag = 'tag0';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t_old(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t_old VALUES ('a', 1, 'alpha', 10);
INSERT INTO t_old VALUES ('b', 2, 'bravo', 20);
INSERT INTO t_old VALUES ('c', 3, 'charlie', 30);
CREATE TABLE t_new(a TEXT, b INTEGER, payload TEXT, n INTEGER, tag TEXT DEFAULT 'tag0', PRIMARY KEY(a, b));
INSERT INTO t_new(a, b, payload, n, tag) SELECT a, b, v, n, 'tag0' FROM t_old;
DROP TABLE t_old;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_tag ON t(tag);
CREATE UNIQUE INDEX uidx_t_payload_n ON t(payload, n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_dml_mix_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
UPDATE t SET payload = 'alpha2', n = 11 WHERE id = 1;
UPDATE t SET n = 22 WHERE id = 2;
DELETE FROM t WHERE id = 3;
INSERT INTO t VALUES (4, 'delta', 40, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (9, 'temp', 999);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
INSERT INTO t VALUES (3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
UPDATE t SET extra = 'seed';
UPDATE t SET payload = 'alpha2', n = 11 WHERE id = 1;
UPDATE t SET n = 22 WHERE id = 2;
DELETE FROM t WHERE id IN (3, 9);
INSERT INTO t VALUES (4, 'delta', 40, 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO src VALUES (1, 'alpha', 10);
INSERT INTO src VALUES (2, 'bravo', 20);
INSERT INTO src VALUES (3, 'charlie', 30);
UPDATE src SET v = 'alpha2', n = 11 WHERE id = 1;
UPDATE src SET n = 22 WHERE id = 2;
DELETE FROM src WHERE id = 3;
INSERT INTO src VALUES (4, 'delta', 40);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra)
SELECT id, v, n, 'seed' FROM src;
DROP TABLE src;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_dml_mix_text_pk" \
  "SELECT printf('%s|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a-key', 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b-key', 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c-key', 'charlie', 30, 'seed');
UPDATE t SET payload = 'alpha2', n = 11 WHERE id = 'a-key';
UPDATE t SET n = 22 WHERE id = 'b-key';
DELETE FROM t WHERE id = 'c-key';
INSERT INTO t VALUES ('d-key', 'delta', 40, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES ('temp-key', 'temp', 999);
INSERT INTO t VALUES ('a-key', 'alpha', 10);
INSERT INTO t VALUES ('b-key', 'bravo', 20);
INSERT INTO t VALUES ('c-key', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
UPDATE t SET extra = 'seed';
UPDATE t SET payload = 'alpha2', n = 11 WHERE id = 'a-key';
UPDATE t SET n = 22 WHERE id = 'b-key';
DELETE FROM t WHERE id IN ('c-key', 'temp-key');
INSERT INTO t VALUES ('d-key', 'delta', 40, 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE src(id TEXT PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO src VALUES ('a-key', 'alpha', 10);
INSERT INTO src VALUES ('b-key', 'bravo', 20);
INSERT INTO src VALUES ('c-key', 'charlie', 30);
UPDATE src SET v = 'alpha2', n = 11 WHERE id = 'a-key';
UPDATE src SET n = 22 WHERE id = 'b-key';
DELETE FROM src WHERE id = 'c-key';
INSERT INTO src VALUES ('d-key', 'delta', 40);
CREATE TABLE t_new(id TEXT PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra)
SELECT id, v, n, 'seed' FROM src;
DROP TABLE src;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_dml_mix_blob_pk" \
  "SELECT printf('%s|%s|%d|%s', hex(id), payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (x'01', 'alpha', 10, 'seed');
INSERT INTO t VALUES (x'02', 'bravo', 20, 'seed');
INSERT INTO t VALUES (x'03', 'charlie', 30, 'seed');
UPDATE t SET payload = 'alpha2', n = 11 WHERE id = x'01';
UPDATE t SET n = 22 WHERE id = x'02';
DELETE FROM t WHERE id = x'03';
INSERT INTO t VALUES (x'04', 'delta', 40, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (x'09', 'temp', 999);
INSERT INTO t VALUES (x'01', 'alpha', 10);
INSERT INTO t VALUES (x'02', 'bravo', 20);
INSERT INTO t VALUES (x'03', 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
UPDATE t SET extra = 'seed';
UPDATE t SET payload = 'alpha2', n = 11 WHERE id = x'01';
UPDATE t SET n = 22 WHERE id = x'02';
DELETE FROM t WHERE id IN (x'03', x'09');
INSERT INTO t VALUES (x'04', 'delta', 40, 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE src(id BLOB PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO src VALUES (x'01', 'alpha', 10);
INSERT INTO src VALUES (x'02', 'bravo', 20);
INSERT INTO src VALUES (x'03', 'charlie', 30);
UPDATE src SET v = 'alpha2', n = 11 WHERE id = x'01';
UPDATE src SET n = 22 WHERE id = x'02';
DELETE FROM src WHERE id = x'03';
INSERT INTO src VALUES (x'04', 'delta', 40);
CREATE TABLE t_new(id BLOB PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra)
SELECT id, v, n, 'seed' FROM src;
DROP TABLE src;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_dml_mix_composite_pk" \
  "SELECT printf('%s|%d|%s|%d|%s', a, b, payload, n, extra) FROM t ORDER BY a, b;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES ('a', 1, 'alpha', 10, 'seed');
INSERT INTO t VALUES ('b', 2, 'bravo', 20, 'seed');
INSERT INTO t VALUES ('c', 3, 'charlie', 30, 'seed');
UPDATE t SET payload = 'alpha2', n = 11 WHERE a = 'a' AND b = 1;
UPDATE t SET n = 22 WHERE a = 'b' AND b = 2;
DELETE FROM t WHERE a = 'c' AND b = 3;
INSERT INTO t VALUES ('d', 4, 'delta', 40, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('temp', 9, 'temp', 999);
INSERT INTO t VALUES ('a', 1, 'alpha', 10);
INSERT INTO t VALUES ('b', 2, 'bravo', 20);
INSERT INTO t VALUES ('c', 3, 'charlie', 30);
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
UPDATE t SET extra = 'seed';
UPDATE t SET payload = 'alpha2', n = 11 WHERE a = 'a' AND b = 1;
UPDATE t SET n = 22 WHERE a = 'b' AND b = 2;
DELETE FROM t WHERE (a = 'c' AND b = 3) OR (a = 'temp' AND b = 9);
INSERT INTO t VALUES ('d', 4, 'delta', 40, 'seed');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE src(a TEXT, b INTEGER, v TEXT, n INTEGER, PRIMARY KEY(a, b));
INSERT INTO src VALUES ('a', 1, 'alpha', 10);
INSERT INTO src VALUES ('b', 2, 'bravo', 20);
INSERT INTO src VALUES ('c', 3, 'charlie', 30);
UPDATE src SET v = 'alpha2', n = 11 WHERE a = 'a' AND b = 1;
UPDATE src SET n = 22 WHERE a = 'b' AND b = 2;
DELETE FROM src WHERE a = 'c' AND b = 3;
INSERT INTO src VALUES ('d', 4, 'delta', 40);
CREATE TABLE t_new(a TEXT, b INTEGER, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed', PRIMARY KEY(a, b));
INSERT INTO t_new(a, b, payload, n, extra)
SELECT a, b, v, n, 'seed' FROM src;
DROP TABLE src;
ALTER TABLE t_new RENAME TO t;
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_committed_evolution_int_pk" \
  "SELECT printf('%d|%s|%d|%s', id, payload, n, extra) FROM t ORDER BY id;" \
  "
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_payload|%d|%s', seqno, name) FROM pragma_index_info('idx_t_payload') ORDER BY seqno;
SELECT printf('idxcol|idx_t_n|%d|%s', seqno, name) FROM pragma_index_info('idx_t_n') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_payload ON t(payload);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'alpha', 10, 'seed');
INSERT INTO t VALUES (2, 'bravo', 20, 'seed');
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (1, 'alpha', 10);
INSERT INTO t VALUES (2, 'bravo', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base shape');
ALTER TABLE t RENAME COLUMN v TO payload;
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
UPDATE t SET extra = 'seed';
INSERT INTO t VALUES (3, 'charlie', 30, 'seed');
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'renamed with payload index');
CREATE INDEX idx_t_n ON t(n);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO src VALUES (1, 'alpha', 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'source seed');
INSERT INTO src VALUES (2, 'bravo', 20), (3, 'charlie', 30);
CREATE TABLE t_new(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t_new(id, payload, n, extra)
SELECT id, v, n, 'seed' FROM src;
DROP TABLE src;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'recreated final table');
CREATE INDEX idx_t_n ON t(n);
CREATE INDEX idx_t_payload ON t(payload);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"

run_ddl_case \
  "ddl_transient_schema_noise_int_pk" \
  "SELECT printf('%d|%s|%d', id, v, n) FROM t ORDER BY id;" \
  "
SELECT printf('obj|%s|%s', type, name) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name;
SELECT printf('col|%d|%s|%s|%d|%s|%d', cid, name, type, \"notnull\", COALESCE(dflt_value,'NULL'), pk) FROM pragma_table_info('t') ORDER BY cid;
SELECT printf('idx|%s|%d|%s|%d', name, \"unique\", origin, partial) FROM pragma_index_list('t') WHERE origin!='pk' ORDER BY name;
SELECT printf('idxcol|idx_t_v|%d|%s', seqno, name) FROM pragma_index_info('idx_t_v') ORDER BY seqno;
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES (1, 'alpha', 10), (2, 'bravo', 20), (3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES (1, 'alpha', 10), (2, 'bravo', 20), (3, 'charlie', 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base final');
CREATE TABLE scratch(id INTEGER PRIMARY KEY, note TEXT);
CREATE INDEX idx_scratch_note ON scratch(note);
CREATE VIEW scratch_v AS SELECT id, note FROM scratch;
CREATE TRIGGER scratch_ai AFTER INSERT ON scratch BEGIN SELECT 1; END;
INSERT INTO scratch VALUES (1, 'temporary');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'transient schema');
DROP TRIGGER scratch_ai;
DROP VIEW scratch_v;
DROP INDEX idx_scratch_note;
DROP TABLE scratch;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
" \
  "
CREATE TABLE scratch(id INTEGER PRIMARY KEY, note TEXT);
CREATE INDEX idx_scratch_note ON scratch(note);
INSERT INTO scratch VALUES (1, 'temporary');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'scratch first');
DROP INDEX idx_scratch_note;
DROP TABLE scratch;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);
INSERT INTO t VALUES (3, 'charlie', 30), (1, 'alpha', 10), (2, 'bravo', 20);
CREATE INDEX idx_t_v ON t(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'final');
"
echo "======================================="
echo "Results: $PASS passed, $FAIL failed"
echo "======================================="
if [ "$FAIL" -gt 0 ]; then
  echo "Failed:$FAILED"
  exit 1
fi
