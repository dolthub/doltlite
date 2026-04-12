#!/bin/bash
#
# Version-control oracle test: dolt_conflicts_<table> (per-row schema)
#
# doltlite's dolt_conflicts_<table> vtable projects every user column
# under three prefixes (base_, our_, their_) and emits our_diff_type /
# their_diff_type classifications, matching Dolt's schema. This oracle
# compares the resulting rows byte-for-byte against Dolt for a variety
# of conflict scenarios and PK shapes.
#
# Columns NOT compared (because they diverge non-semantically):
#   - from_root_ish  (doltlite emits NULL, Dolt emits a commit hash)
#   - dolt_conflict_id (engines use different schemes)
#
# Compared: base_<col>, our_<col>, our_diff_type,
#           their_<col>, their_diff_type
#
# Scenarios cover single-row modify, multi-row modify, different PK
# shapes (INT, TEXT, composite), insert/insert conflicts, delete/modify
# conflicts (both directions), and wide schemas.
#
# Usage: bash vc_oracle_conflicts_table_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  tr -d '\r' | sort
}

# $1=name, $2=setup SQL (runs on both engines), $3=comparison query
# The comparison query must project rows prefixed with "R|" and use
# CONCAT (works in both SQLite and MySQL for string concatenation).
oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # -- doltlite --
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^R|' \
           | normalize)

  # -- Dolt --
  # Same scenario piped as a single `dolt sql` invocation so
  # @@autocommit=0 and @@dolt_allow_commit_conflicts=1 persist.
  local dolt_all
  dolt_all=$(printf '%s\n%s' "$setup" "$query" \
             | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_all"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | normalize)

  if [ -z "$dl_out" ] && [ -z "$dt_out" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (both queries empty — harness bug)"
    return
  fi

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_conflicts_<table> ==="
echo ""

echo "--- single-row modify/modify, INT PK ---"

oracle "int_pk_single_row" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=100 WHERE id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1000 WHERE id=1;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_v, '|', our_id, '|', our_v, '|', our_diff_type, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- multi-row modify/modify, INT PK ---"

oracle "int_pk_multi_row" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=100 WHERE id=1;
UPDATE t SET v=200 WHERE id=2;
UPDATE t SET v=300 WHERE id=3;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1000 WHERE id=1;
UPDATE t SET v=2000 WHERE id=2;
UPDATE t SET v=3000 WHERE id=3;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_v, '|', our_id, '|', our_v, '|', our_diff_type, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- INTEGER PRIMARY KEY (rowid-aliased) ---"

# INTEGER PK is the rowid-aliased shape where the PK is NOT stored in
# the record body (it's the intKey). The projected column must come
# from intKey, not from the record.
oracle "integer_pk_single_row" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=100 WHERE id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1000 WHERE id=1;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_v, '|', our_id, '|', our_v, '|', our_diff_type, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- VARCHAR PK ---"

oracle "varchar_pk" \
"CREATE TABLE t(id VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('alice',10),('bob',20);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=100 WHERE id='alice';
UPDATE t SET v=200 WHERE id='bob';
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1000 WHERE id='alice';
UPDATE t SET v=2000 WHERE id='bob';
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_v, '|', our_id, '|', our_v, '|', our_diff_type, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- composite INT PK ---"

oracle "composite_int_pk" \
"CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,1,11),(1,2,12),(2,1,21);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=110 WHERE a=1 AND b=1;
UPDATE t SET v=120 WHERE a=1 AND b=2;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1100 WHERE a=1 AND b=1;
UPDATE t SET v=1200 WHERE a=1 AND b=2;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_a, '|', base_b, '|', base_v, '|', our_a, '|', our_b, '|', our_v, '|', our_diff_type, '|', their_a, '|', their_b, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_a, base_b;"

echo "--- composite mixed VARCHAR + INT PK ---"

oracle "composite_mixed_pk" \
"CREATE TABLE t(region VARCHAR(8), id INT, v INT, PRIMARY KEY(region, id));
INSERT INTO t VALUES('us',1,11),('us',2,12),('eu',1,21);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=110 WHERE region='us' AND id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1100 WHERE region='us' AND id=1;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_region, '|', base_id, '|', base_v, '|', our_region, '|', our_id, '|', our_v, '|', our_diff_type, '|', their_region, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_region, base_id;"

echo "--- insert/insert same PK ---"

# Both sides add a row with the same PK. base should be NULL (no
# common ancestor for this row), diff types should be 'added' on
# both sides.
oracle "insert_insert_same_pk" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,200);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,2000);
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', IFNULL(base_id,'NULL'), '|', IFNULL(base_v,'NULL'), '|', our_id, '|', our_v, '|', our_diff_type, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY our_id;"

echo "--- delete/modify: main deletes, feature modifies ---"

# main deletes row 1, feature modifies it. our side is "removed",
# their side is "modified".
oracle "main_delete_feat_modify" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v=100 WHERE id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_v, '|', IFNULL(our_id,'NULL'), '|', IFNULL(our_v,'NULL'), '|', our_diff_type, '|', their_id, '|', their_v, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- modify/delete: main modifies, feature deletes ---"

oracle "main_modify_feat_delete" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=100 WHERE id=1;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_v, '|', our_id, '|', our_v, '|', our_diff_type, '|', IFNULL(their_id,'NULL'), '|', IFNULL(their_v,'NULL'), '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- wide schema (many mixed-type columns) ---"

oracle "wide_schema" \
"CREATE TABLE t(id INT PRIMARY KEY, a VARCHAR(32), b INT, c DOUBLE, d VARCHAR(32), e INT);
INSERT INTO t VALUES(1,'a1',11,1.5,'d1',111);
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET a='FEAT_A', b=999, d='feat_d' WHERE id=1;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET a='MAIN_A', b=888, c=9.99 WHERE id=1;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', base_a, '|', base_b, '|', base_c, '|', base_d, '|', base_e, '|', our_a, '|', our_b, '|', our_c, '|', our_d, '|', our_e, '|', our_diff_type, '|', their_a, '|', their_b, '|', their_c, '|', their_d, '|', their_e, '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo "--- NULL non-PK values on each side ---"

oracle "nulls_in_conflict" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT, note VARCHAR(32));
INSERT INTO t VALUES(1,10,NULL),(2,NULL,'hello');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET note='feat' WHERE id=1;
UPDATE t SET v=222 WHERE id=2;
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET note='main' WHERE id=1;
UPDATE t SET v=333 WHERE id=2;
SELECT dolt_commit('-A','-m','mainu');
SELECT dolt_merge('feat');
" \
"SELECT CONCAT('R|', base_id, '|', IFNULL(base_v,'NULL'), '|', IFNULL(base_note,'NULL'), '|', IFNULL(our_v,'NULL'), '|', IFNULL(our_note,'NULL'), '|', our_diff_type, '|', IFNULL(their_v,'NULL'), '|', IFNULL(their_note,'NULL'), '|', their_diff_type) FROM dolt_conflicts_t ORDER BY base_id;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
