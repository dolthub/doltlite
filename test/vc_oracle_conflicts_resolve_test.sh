#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

oracle() {
  local name="$1" setup="$2" resolve_and_query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_script
  local dl_out
  dl_script=$(printf "%s\n%s\n" "$setup" "$resolve_and_query" | perl -0pe "s/\nSELECT dolt_merge\\(/\nBEGIN;\\nSELECT dolt_merge\\(/")
  dl_out=$(printf "%s" "$dl_script" \
           | vc_oracle_run_doltlite --allow-error "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^Merge has' \
           | grep '^R|' \
           | tr -d '\r' | sort)

  local dolt_all
  dolt_all=$(vc_oracle_translate_for_dolt "$(printf '%s\n%s' "$setup" "$resolve_and_query")" \
             | sed -E 's/FOR EACH ROW BEGIN (.*); END;/FOR EACH ROW \1;/')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_all"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | tr -d '\r' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_setup
  local dl_rc
  dl_setup=$(printf "%s\n" "$setup" | perl -0pe "s/\nSELECT dolt_merge\\(/\nBEGIN;\\nSELECT dolt_merge\\(/")
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_setup" --expect-error
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "SET @@autocommit = 0;
SET @@dolt_allow_commit_conflicts = 1;
$dolt_setup"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

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

MULTI_CONFLICT_SETUP="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
CREATE TABLE clean(id INT PRIMARY KEY);
INSERT INTO t VALUES(1, 10);
INSERT INTO u VALUES(1, 100);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
UPDATE t SET v=20;
UPDATE u SET v=200;
SELECT dolt_commit('-am', 'feature');
SELECT dolt_checkout('main');
UPDATE t SET v=30;
UPDATE u SET v=300;
SELECT dolt_commit('-am', 'main');
SELECT dolt_merge('feature');
"

for mode in ours theirs; do
  i=0
  for tables in "'t','u'" "'u','t'" "'t','t','u'" "'clean','t','u'" "'t','u','clean'"; do
    i=$((i+1))
    oracle "resolve_${mode}_multiple_$i" "$MULTI_CONFLICT_SETUP" "
SELECT dolt_conflicts_resolve('--$mode', $tables);
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('R|t|', v) FROM t;
SELECT CONCAT('R|u|', v) FROM u;
"
  done
  i=0
  for tables in "'missing','t','u'" "'t','missing','u'" "'t','u','missing'"; do
    i=$((i+1))
    oracle "resolve_${mode}_missing_multiple_$i" "$MULTI_CONFLICT_SETUP" "
SELECT dolt_conflicts_resolve('--$mode', $tables);
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('R|t|', v) FROM t;
SELECT CONCAT('R|u|', v) FROM u;
"
  done
  oracle "resolve_${mode}_multiple_savepoint" "$MULTI_CONFLICT_SETUP" "
SAVEPOINT both_tables;
SELECT dolt_conflicts_resolve('--$mode', 't', 'u');
SELECT CONCAT('R|resolved|', count(*)) FROM dolt_conflicts;
ROLLBACK TO both_tables;
SELECT CONCAT('R|restored|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('R|t|', v) FROM t;
SELECT CONCAT('R|u|', v) FROM u;
"
done

echo "--- resolve then commit ---"

oracle "resolve_and_commit" \
  "$CONFLICT_SETUP" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-A', '-m', 'resolved');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"

echo "--- no-op on existing table with no conflicts ---"

oracle "resolve_ours_no_conflicts_noop" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"

oracle "resolve_theirs_no_conflicts_noop" \
"CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10), (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"

echo "--- error paths ---"

oracle_error "resolve_missing_table" \
  "$CONFLICT_SETUP
SELECT dolt_conflicts_resolve('--ours', 'nope');
"

oracle_error "resolve_missing_second_table" \
  "$CONFLICT_SETUP
SELECT dolt_conflicts_resolve('--ours', 't', 'extra');
"

echo "--- named savepoint rollback parity ---"

oracle "resolve_ours_savepoint_rollback" \
  "$CONFLICT_SETUP" \
  "SAVEPOINT sp1;
SELECT dolt_conflicts_resolve('--ours', 't');
ROLLBACK TO sp1;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

oracle "resolve_theirs_savepoint_rollback" \
  "$CONFLICT_SETUP" \
  "SAVEPOINT sp1;
SELECT dolt_conflicts_resolve('--theirs', 't');
ROLLBACK TO sp1;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

oracle "delete_conflict_row_savepoint_rollback" \
  "$CONFLICT_SETUP" \
  "SAVEPOINT sp1;
DELETE FROM dolt_conflicts_t WHERE our_id=1;
ROLLBACK TO sp1;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('R|', id, '|', v) FROM t ORDER BY id;"

echo "--- TEXT primary key ---"

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

echo "--- per-row DELETE on conflict tables (TEXT primary key) ---"

oracle "delete_conflict_row_text_pk" \
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
  "DELETE FROM dolt_conflicts_t WHERE our_id='alice';
SELECT CONCAT('R|row|', our_id, '|', our_v, '|', their_v) FROM dolt_conflicts_t ORDER BY our_id;
SELECT CONCAT('R|table|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|count|', count(*)) FROM dolt_conflicts;"

oracle "delete_all_conflict_rows_text_pk" \
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
  "DELETE FROM dolt_conflicts_t;
SELECT CONCAT('R|table|', id, '|', v) FROM t ORDER BY id;
SELECT CONCAT('R|count|', count(*)) FROM dolt_conflicts;"

echo "--- composite primary key (two INT columns) ---"

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

echo "--- per-row DELETE on conflict tables (composite primary key) ---"

oracle "delete_conflict_row_composite_pk" \
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
  "DELETE FROM dolt_conflicts_t WHERE our_a=1 AND our_b=1;
SELECT CONCAT('R|row|', our_a, '|', our_b, '|', our_v, '|', their_v) FROM dolt_conflicts_t ORDER BY our_a, our_b;
SELECT CONCAT('R|table|', a, '|', b, '|', v) FROM t ORDER BY a, b;
SELECT CONCAT('R|count|', count(*)) FROM dolt_conflicts;"

oracle "delete_all_conflict_rows_composite_pk" \
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
  "DELETE FROM dolt_conflicts_t;
SELECT CONCAT('R|table|', a, '|', b, '|', v) FROM t ORDER BY a, b;
SELECT CONCAT('R|count|', count(*)) FROM dolt_conflicts;"

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

echo "--- resolution onto occupied UNIQUE keys ---"

for key in 'INTEGER PRIMARY KEY' 'INT PRIMARY KEY'; do
  for kind in nullable notnull compound; do
    column='v INT'
    terms='v'
    if [ "$kind" != nullable ]; then column='v INT NOT NULL'; fi
    if [ "$kind" = compound ]; then terms='v,z'; fi
    oracle "resolve_theirs_unique_${key%% *}_$kind" "
CREATE TABLE t(id $key, $column, z INT NOT NULL);
CREATE UNIQUE INDEX u ON t($terms);
INSERT INTO t VALUES(1,10,1),(2,20,1);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side');
UPDATE t SET v=30 WHERE id=1;
SELECT dolt_commit('-am','theirs');
SELECT dolt_checkout('main');
UPDATE t SET v=40 WHERE id=1;
UPDATE t SET v=30 WHERE id=2;
SELECT dolt_commit('-am','ours');
SELECT dolt_merge('side');
" "
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT CONCAT('R|lookup|',id) FROM t WHERE v=30 AND z=1 ORDER BY id;
SELECT CONCAT('R|in|',id) FROM t WHERE v IN (30,40) ORDER BY id;
SELECT CONCAT('R|count|',count(*)) FROM t WHERE v=30;
SELECT CONCAT('R|distinct|',v,z) FROM (SELECT DISTINCT v,z FROM t) d;
SELECT dolt_commit('-Am','resolved');
SELECT CONCAT('R|committed|',id) FROM t WHERE v=30 AND z=1 ORDER BY id;
DELETE FROM t WHERE id=2;
SELECT CONCAT('R|retained|',id) FROM t WHERE v=30 AND z=1;
COMMIT;
"
  done
done

echo "--- view conflict resolved through dolt_schemas ---"

VIEW_CONFLICT_SETUP="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
SELECT dolt_branch('b');
CREATE VIEW vv AS SELECT id AS x FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_view');
SELECT dolt_checkout('b');
CREATE VIEW vv AS SELECT v AS x FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b_view');
SELECT dolt_checkout('main');
SELECT dolt_merge('b');"
for side in ours theirs; do
  oracle "resolve_${side}_view_add_add" "$VIEW_CONFLICT_SETUP" \
    "SELECT dolt_conflicts_resolve('--$side', 'dolt_schemas');
SELECT CONCAT('R|', x) FROM vv;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"
done

echo "--- --theirs refusals: foreign keys and diverged columns ---"

FK_CHILD_SETUP="
CREATE TABLE p(id INT PRIMARY KEY);
CREATE TABLE c(id INT PRIMARY KEY, v INT, pid INT, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1);
INSERT INTO p VALUES(2);
INSERT INTO c VALUES(1, 0, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE c SET v=10 WHERE id=1;
DELETE FROM p WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_side');
SELECT dolt_checkout('feature');
UPDATE c SET v=20, pid=2 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_side');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"
oracle "resolve_theirs_orphans_child_keeps_state" "$FK_CHILD_SETUP" \
  "SELECT dolt_conflicts_resolve('--theirs', 'c');
SELECT CONCAT('R|c|', id, '|', v, '|', pid) FROM c;
SELECT CONCAT('R|conflicts|', \`table\`, '|', num_conflicts) FROM dolt_conflicts;"

FK_PARENT_SETUP="
CREATE TABLE p(id INT PRIMARY KEY, v INT);
CREATE TABLE c(id INT PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1, 0);
INSERT INTO p VALUES(2, 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE p SET v=1 WHERE id=2;
INSERT INTO c VALUES(1, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_side');
SELECT dolt_checkout('feature');
DELETE FROM p WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_side');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"
oracle "resolve_theirs_deletes_referenced_parent_keeps_state" "$FK_PARENT_SETUP" \
  "SELECT dolt_conflicts_resolve('--theirs', 'p');
SELECT CONCAT('R|p|', id, '|', v) FROM p;
SELECT CONCAT('R|conflicts|', \`table\`, '|', num_conflicts) FROM dolt_conflicts;"

oracle "resolve_theirs_keeps_preexisting_orphan" \
"CREATE TABLE p(id INT PRIMARY KEY);
CREATE TABLE c(id INT PRIMARY KEY, v INT, pid INT, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1);
INSERT INTO p VALUES(2);
INSERT INTO c VALUES(1, 0, 1);
INSERT INTO c VALUES(2, 0, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE c SET v=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_side');
SELECT dolt_checkout('feature');
UPDATE c SET v=20 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_side');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
" \
  "SELECT dolt_conflicts_resolve('--theirs', 'c');
SELECT CONCAT('R|c|', id, '|', v, '|', pid) FROM c;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"

column_setup() {
  local main_ddl="$1" feature_ddl="$2"
  printf '%s\n' "
CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT);
INSERT INTO t VALUES(1, 1, 1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
$main_ddl
UPDATE t SET a=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_side');
SELECT dolt_checkout('feature');
$feature_ddl
UPDATE t SET a=20 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_side');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"
}
ADD_D="ALTER TABLE t ADD COLUMN d INT DEFAULT 5;"
DROP_C="ALTER TABLE t DROP COLUMN c;"

oracle "resolve_theirs_ours_added_column_keeps_ours" "$(column_setup "$ADD_D" "")" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', a, '|', d) FROM t;"
oracle "resolve_theirs_ours_dropped_column_keeps_ours" "$(column_setup "$DROP_C" "")" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', a, '|', b) FROM t;"
oracle "resolve_theirs_their_added_column" "$(column_setup "" "$ADD_D")" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', a, '|', b, '|', c, '|', d) FROM t;"
oracle "resolve_theirs_their_dropped_column" "$(column_setup "" "$DROP_C")" \
  "SELECT dolt_conflicts_resolve('--theirs', 't');
SELECT CONCAT('R|', id, '|', a, '|', b) FROM t;"
oracle "resolve_ours_their_added_column" "$(column_setup "" "$ADD_D")" \
  "SELECT dolt_conflicts_resolve('--ours', 't');
SELECT CONCAT('R|', id, '|', a, '|', b, '|', c, '|', d) FROM t;"

echo "--- trigger conflict resolved through dolt_schemas ---"

TRIGGER_CONFLICT_SETUP="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log(x INT);
INSERT INTO t VALUES(1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
SELECT dolt_branch('b');
CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES(1); END;
CREATE VIEW vv AS SELECT id AS x FROM t;
UPDATE t SET v = 11;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_side');
SELECT dolt_checkout('b');
CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES(2); END;
CREATE VIEW vv AS SELECT v AS x FROM t;
UPDATE t SET v = 12;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b_side');
SELECT dolt_checkout('main');
SELECT dolt_merge('b');"
oracle "schema_object_conflicts_listed" "$TRIGGER_CONFLICT_SETUP" \
  "SELECT CONCAT('R|', \`table\`, '|', num_conflicts) FROM dolt_conflicts;
SELECT CONCAT('R|status|', table_name, '|', status) FROM dolt_status WHERE table_name = 'dolt_schemas';
SELECT CONCAT('R|schema_conflicts|', count(*)) FROM dolt_schema_conflicts;"
for side in ours theirs; do
  oracle "resolve_${side}_trigger_and_view" "$TRIGGER_CONFLICT_SETUP" \
    "SELECT dolt_conflicts_resolve('--$side', 'dolt_schemas');
SELECT dolt_conflicts_resolve('--$side', 't');
INSERT INTO t VALUES(2, 20);
SELECT CONCAT('R|log|', x) FROM log;
SELECT CONCAT('R|view|', x) FROM vv WHERE x IS NOT NULL;
SELECT CONCAT('R|conflicts|', count(*)) FROM dolt_conflicts;"
done

vc_oracle_finish
