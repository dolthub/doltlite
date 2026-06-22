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

normalize() {
  tr -d '\r'
}

oracle() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT table_name || char(9) || staged || char(9) || status FROM dolt_status ORDER BY table_name, staged, status;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(table_name, char(9), staged, char(9), status) FROM dolt_status ORDER BY table_name, staged, status;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(vc_oracle_tail_csv_body "$dir/dt.raw" | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

status_filter_pushdown() {
  local name="$1" setup="$2" filter="$3" expected_idx="$4"
  local dir="$TMPROOT/${name}_pushdown"
  mkdir -p "$dir"

  local script
  script="$setup
.headers off
.mode list
.separator '	'
CREATE TEMP TABLE expected_status AS
  SELECT table_name, staged, status FROM dolt_status;
SELECT 'A|' || table_name || '|' || staged || '|' || status
  FROM expected_status
 WHERE $filter
 ORDER BY table_name, staged, status;
SELECT 'B|' || table_name || '|' || staged || '|' || status
  FROM dolt_status
 WHERE $filter
 ORDER BY table_name, staged, status;
EXPLAIN QUERY PLAN SELECT * FROM dolt_status WHERE $filter;"

  local out actual expected plan
  out=$(printf "%s\n" "$script" \
        | "$DOLTLITE" "$dir/db" 2>"$dir/err" \
        | tr -d '\r')
  expected=$(printf "%s\n" "$out" | grep '^A|' | sed 's/^A|//')
  actual=$(printf "%s\n" "$out" | grep '^B|' | sed 's/^B|//')
  plan=$(printf "%s\n" "$out" | grep "VIRTUAL TABLE INDEX $expected_idx")

  if [ "$actual" = "$expected" ] && [ -n "$plan" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected:"; echo "$expected" | sed 's/^/      /'
    echo "    actual:";   echo "$actual" | sed 's/^/      /'
    echo "    plan:";     printf "%s\n" "$out" | grep "dolt_status" | sed 's/^/      /'
    echo "    stderr:";   sed 's/^/      /' "$dir/err"
  fi
}

echo "=== Version Control Oracle Tests: dolt_status ==="
echo ""

echo "--- empty / baseline ---"

oracle "empty_fresh_db" "
-- no DDL, no commits; both sides should report empty status
SELECT 1;
" "EXPECT_EMPTY"

echo "--- new tables ---"

oracle "new_table_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
"

oracle "new_table_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
"

oracle "two_new_tables_one_staged" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a');
"

echo "--- modifications ---"

oracle "modified_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
"

oracle "modified_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
"

oracle "modified_mixed_staged_and_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
INSERT INTO t VALUES (3, 30);
"

oracle "schema_only_unstaged_is_modified" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a ADD COLUMN n INT;
"

oracle "schema_only_staged_is_modified" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a ADD COLUMN n INT;
SELECT dolt_add('a');
"

oracle "schema_and_data_unstaged_is_modified" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
UPDATE a SET s = 'x' WHERE id = 1;
ALTER TABLE a ADD COLUMN n INT;
"

oracle "drop_recreate_unstaged_is_modified" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
"

echo "--- deletions ---"

oracle "deleted_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
"

oracle "deleted_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
SELECT dolt_add('-A');
"

echo "--- renames ---"

oracle "renamed_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t RENAME TO t2;
"

oracle "renamed_and_modified_unstaged" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 20);
"

oracle "renamed_and_modified_staged" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 20);
SELECT dolt_add('b');
"

oracle "renamed_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('t2');
"

oracle "renamed_staged_then_modified_again" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('t2');
INSERT INTO t2 VALUES (2, 20);
"

oracle "rename_chain_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t RENAME TO t2;
ALTER TABLE t2 RENAME TO t3;
"

oracle "rename_chain_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE t RENAME TO t2;
ALTER TABLE t2 RENAME TO t3;
SELECT dolt_add('t3');
"

echo "--- filtered scans ---"

status_filter_pushdown "filtered_table_name_matches_unfiltered" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
INSERT INTO a VALUES (2, 20);
INSERT INTO b VALUES (2, 20);
SELECT dolt_add('a');
" "table_name='a'" "2"

status_filter_pushdown "filtered_table_name_and_staged_matches_unfiltered" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
INSERT INTO a VALUES (2, 20);
INSERT INTO b VALUES (2, 20);
SELECT dolt_add('a');
" "staged=1 AND table_name='a'" "3"

status_filter_pushdown "filtered_rename_new_name_stays_empty" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO b;
" "table_name='b'" "2"

status_filter_pushdown "filtered_rename_compound_name_matches" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO b;
" "table_name='a -> b'" "2"

echo "--- multi-table ---"

oracle "multi_table_mixed_states" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
INSERT INTO a VALUES (2, 20);
DROP TABLE b;
CREATE TABLE c(id INTEGER PRIMARY KEY);
INSERT INTO c VALUES (1);
"

oracle "drop_recreate_same_schema_not_renamed" "
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO b VALUES (1, 10);
SELECT dolt_add('b');
SELECT dolt_commit('-m', 'seed');
DROP TABLE b;
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT);
INSERT INTO c VALUES (2, 20);
"

oracle "multi_table_rename_drop_create_mix" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE c(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
INSERT INTO c VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO a2;
DROP TABLE b;
CREATE TABLE d(id INTEGER PRIMARY KEY);
INSERT INTO d VALUES (1);
INSERT INTO c VALUES (2, 20);
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
