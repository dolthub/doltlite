#!/bin/bash
#
# Version-control oracle test: dolt_conflicts_resolve
#
# Runs identical conflict-resolution scenarios against doltlite and Dolt
# and compares the resulting table data and conflict state. Covers:
#   - --ours resolution (keep our value)
#   - --theirs resolution (accept their value)
#   - multi-row conflicts resolved in one call
#   - partial resolution (multiple tables, resolve one)
#   - resolution followed by commit
#
# Usage: bash vc_oracle_conflicts_resolve_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Build a standard merge-conflict setup and add a resolution + query.
# Compare the final table data (SELECT id, v FROM t ORDER BY id) and
# the remaining conflict count.
#
# $1=name, $2=setup (up to and including the merge), $3=resolve+query SQL
oracle() {
  local name="$1" setup="$2" resolve_and_query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # -- doltlite --
  local dl_out
  dl_out=$(printf "%s\n%s\n" "$setup" "$resolve_and_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^Merge has' \
           | grep '^R|' \
           | tr -d '\r' | sort)

  # -- Dolt --
  # Everything must run in ONE `dolt sql` invocation so
  # @@autocommit=0 and @@dolt_allow_commit_conflicts=1 persist
  # across the setup, merge, resolution, and query.
  local dolt_all
  dolt_all=$(vc_oracle_translate_for_dolt "$(printf '%s\n%s' "$setup" "$resolve_and_query")")

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
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | tr -d '\r' | sort)

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

# Standard conflict setup: both sides modify the same row differently.
CONFLICT_SETUP="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=200 WHERE id=1;
UPDATE t SET v=201 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_checkout('main');
UPDATE t SET v=300 WHERE id=1;
UPDATE t SET v=301 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_update');
SELECT dolt_merge('feature');
"

echo "=== Version Control Oracle Tests: dolt_conflicts_resolve ==="
echo ""

echo "--- --ours resolution ---"

oracle "resolve_ours_single_table" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- --theirs resolution ---"

oracle "resolve_theirs_single_table" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- multi-table: resolve one, check other still conflicted ---"

oracle "resolve_one_of_two_tables" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, x INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO u VALUES(1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=20 WHERE id=1;
UPDATE u SET x=200 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v=30 WHERE id=1;
UPDATE u SET x=300 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_upd');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|t|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|u_conflicts|', num_conflicts) FROM dolt_conflicts WHERE \`table\`='u';"

echo "--- resolve then commit ---"

oracle "resolve_and_commit" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-A', '-m', 'resolved');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"

echo "--- TEXT primary key ---"

# TEXT PK exercises the conflict-apply path for non-integer keys.
# Row-based prolly-tree storage keys rows by the serialized PK; the
# conflict-resolve path must decode the text PK from the record body
# (since doltlite's tree key is an intKey, not the user PK bytes).
oracle "resolve_theirs_text_pk" \
"CREATE TABLE t(id VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('alice', 10);
INSERT INTO t VALUES('bob', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=200 WHERE id='alice';
UPDATE t SET v=201 WHERE id='bob';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_checkout('main');
UPDATE t SET v=300 WHERE id='alice';
UPDATE t SET v=301 WHERE id='bob';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_update');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

oracle "resolve_ours_text_pk" \
"CREATE TABLE t(id VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('alice', 10);
INSERT INTO t VALUES('bob', 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=200 WHERE id='alice';
UPDATE t SET v=201 WHERE id='bob';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_checkout('main');
UPDATE t SET v=300 WHERE id='alice';
UPDATE t SET v=301 WHERE id='bob';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_update');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- composite primary key (two INT columns) ---"

# Composite PK exercises the path where the tree key alone can't
# identify a row — the conflict apply must reconstruct the full key
# from the record body.
oracle "resolve_theirs_composite_int_pk" \
"CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1, 1, 11);
INSERT INTO t VALUES(1, 2, 12);
INSERT INTO t VALUES(2, 1, 21);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=110 WHERE a=1 AND b=1;
UPDATE t SET v=120 WHERE a=1 AND b=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1100 WHERE a=1 AND b=1;
UPDATE t SET v=1200 WHERE a=1 AND b=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', a, '|', b, '|', v) FROM t ORDER BY a, b;"

oracle "resolve_ours_composite_int_pk" \
"CREATE TABLE t(a INT, b INT, v INT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1, 1, 11);
INSERT INTO t VALUES(1, 2, 12);
INSERT INTO t VALUES(2, 1, 21);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=110 WHERE a=1 AND b=1;
UPDATE t SET v=120 WHERE a=1 AND b=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1100 WHERE a=1 AND b=1;
UPDATE t SET v=1200 WHERE a=1 AND b=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', a, '|', b, '|', v) FROM t ORDER BY a, b;"

echo "--- composite PK mixing INT and TEXT ---"

oracle "resolve_theirs_composite_mixed_pk" \
"CREATE TABLE t(region VARCHAR(8), id INT, v INT, PRIMARY KEY(region, id));
INSERT INTO t VALUES('us', 1, 11);
INSERT INTO t VALUES('us', 2, 12);
INSERT INTO t VALUES('eu', 1, 21);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=110 WHERE region='us' AND id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v=1100 WHERE region='us' AND id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', region, '|', id, '|', v) FROM t ORDER BY region, id;"

echo "--- multi-row conflicts in a single table ---"

# Many conflicting rows in the same table, resolved in one call.
# Exercises the loop over ConflictRow entries in applyTheirRecord.
oracle "resolve_theirs_many_rows" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=v+100;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET v=v+1000;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- delete/modify conflicts ---"

# Main deletes a row; feature modifies it. The two resolution choices
# have different meanings here — --ours keeps the delete, --theirs
# keeps the modification (row present with feature's value).
oracle "resolve_ours_delete_vs_modify" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=200 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_mod');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_del');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

oracle "resolve_theirs_delete_vs_modify" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v=200 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_mod');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_del');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

# Symmetric: main modifies, feature deletes.
oracle "resolve_theirs_modify_vs_delete" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_del');
SELECT dolt_checkout('main');
UPDATE t SET v=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_mod');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- insert/insert same PK ---"

# Both sides insert a new row with the same primary key but different
# values. No base row exists. Exercises the applyTheirRecord path for
# a row that has no 'base' side.
oracle "resolve_theirs_insert_insert_same_pk" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_ins');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_ins');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

oracle "resolve_ours_insert_insert_same_pk" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_ins');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_ins');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- wide schema (many non-PK columns) ---"

# Exercises the per-column decoding loop in buildInsertSql with a
# larger column count, mixing types.
oracle "resolve_theirs_wide_schema" \
"CREATE TABLE t(id INT PRIMARY KEY, a VARCHAR(32), b INT, c DOUBLE, d VARCHAR(32), e INT);
INSERT INTO t VALUES(1, 'a1', 11, 1.5, 'd1', 111);
INSERT INTO t VALUES(2, 'a2', 22, 2.5, 'd2', 222);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET a='FEAT', b=999, d='feat_d' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET a='MAIN', b=888, c=9.99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', a, '|', b, '|', c, '|', d, '|', e) FROM t ORDER BY id;"

echo "--- NULL values in conflict rows ---"

# Non-PK column containing NULL must survive the conflict roundtrip.
oracle "resolve_theirs_with_nulls" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT, note VARCHAR(32));
INSERT INTO t VALUES(1, 10, NULL);
INSERT INTO t VALUES(2, NULL, 'hello');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET note='feat' WHERE id=1;
UPDATE t SET v=222 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET note='main' WHERE id=1;
UPDATE t SET v=333 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mainu');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', IFNULL(v,'NULL'), '|', IFNULL(note,'NULL')) FROM t ORDER BY id;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
