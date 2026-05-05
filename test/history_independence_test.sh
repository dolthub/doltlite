#!/bin/bash
#
# History independence smoke suite.
#
# Goal:
#   For a fixed final logical database state, dolt_hashof_db() must be
#   identical regardless of the SQL history used to produce that state.
#
# Current scope:
#   - DML only
#   - single branch
#   - small datasets for PR-speed smoke coverage
#   - key families:
#       * INTEGER PRIMARY KEY
#       * TEXT PRIMARY KEY
#       * BLOB PRIMARY KEY
#       * composite PRIMARY KEY
#
# This suite deliberately proves two things for each case:
#   1. final user-visible state matches, by hashing canonical ordered rows
#   2. dolt_hashof_db() matches across distinct histories
#
# Planned follow-on phases:
#   - branch / merge histories
#   - large database variants
#   - .read / streamed import variants
#   - secondary-index-heavy variants
#   - DDL history independence
#   - clone / fetch / push / pull histories
#

set -euo pipefail

DOLTLITE="${1:-./doltlite}"
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

  echo ""
}

echo "=== History Independence Smoke Tests ==="
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

echo "======================================="
echo "Results: $PASS passed, $FAIL failed"
echo "======================================="
if [ "$FAIL" -gt 0 ]; then
  echo "Failed:$FAILED"
  exit 1
fi
