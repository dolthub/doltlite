#!/bin/bash
#
# Version-control oracle test: dolt_schema_diff
#
# Runs identical schema-diff scenarios against doltlite and Dolt and
# compares the row output. Both engines now expose the same columns
# (from_table_name, to_table_name, from_create_statement,
# to_create_statement) and accept the same call forms
# (`dolt_schema_diff('from','to'[,'tbl'])` and `dolt_schema_diff('from..to')`),
# so the oracle can issue
# the same query string against both.
#
# Compared surface: (from_table_name, to_table_name, from_present,
# to_present) sorted, where {from,to}_present is Y/N for whether the
# create-statement column is non-empty. The create-statement TEXT is
# intentionally NOT compared row-for-row because Dolt and doltlite
# canonicalize CREATE TABLE differently (whitespace, type aliases,
# quoting, ENGINE=... suffix in Dolt).
#
# Known intentional divergences from Dolt (NOT oracle-tested here):
#
#   - CREATE INDEX: doltlite treats indexes as first-class schema
#     entries (sqlite_schema has a row of type='index' for each
#     one), so ALTER TABLE ... / CREATE INDEX adds a new row to
#     dolt_schema_diff with the index as a separate "added" entry.
#     Dolt rolls indexes into the table's CREATE statement and
#     reports the table as modified instead. doltlite's behavior is
#     the natural consequence of the sqlite_schema model and is
#     intentional — indexes ARE schemas in SQLite.
#
# Usage: bash vc_oracle_schema_diff_test.sh [path/to/doltlite] [path/to/dolt]
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

# $1=name, $2=setup SQL, $3=from_ref, $4=to_ref, $5=optional table filter.
oracle() {
  local name="$1" setup="$2" from_ref="$3" to_ref="$4" tbl="${5:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local args
  if [ -n "$tbl" ]; then
    args="'$from_ref','$to_ref','$tbl'"
  else
    args="'$from_ref','$to_ref'"
  fi

  # The "ROW|" sentinel lets us grep the answer rows out of the noise
  # that CALL dolt_*(...) emits in dolt's csv output. Use CONCAT not
  # || (MySQL parses || as logical OR). Both engines accept CONCAT.
  local q="SELECT CONCAT('ROW|', from_table_name, '|', to_table_name, '|', \
            CASE WHEN from_create_statement IS NULL OR from_create_statement='' THEN 'N' ELSE 'Y' END, '|', \
            CASE WHEN to_create_statement   IS NULL OR to_create_statement=''   THEN 'N' ELSE 'Y' END \
          ) FROM dolt_schema_diff($args) ORDER BY from_table_name, to_table_name;"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ROW|' \
           | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ROW|' | sort)

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

oracle_error() {
  local name="$1" setup="$2" q="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  local dl_sql
  dl_sql=$(printf "%s\n%s\n" "$setup" "$q")
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_sql"
  dl_rc=$?

  local dolt_setup
  local dt_rc
  local dt_sql
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_sql=$(printf "%s\n%s\n" "$dolt_setup" "$q")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dt_sql"
  dt_rc=$?

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_query() {
  local name="$1" setup="$2" q="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ROW|' \
           | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ROW|' | sort)

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

echo "=== Version Control Oracle Tests: dolt_schema_diff ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- added table ---"

oracle "added_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "HEAD~1" "HEAD"

echo "--- dropped table ---"

oracle "dropped_table" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t');
" "HEAD~1" "HEAD"

echo "--- modified table (add column) ---"

oracle "modified_add_col" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
" "HEAD~1" "HEAD"

oracle "modified_drop_col" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_col');
" "HEAD~1" "HEAD"

oracle "modified_rename_col" "
$SEED
ALTER TABLE t RENAME COLUMN v TO vv;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_col');
" "HEAD~1" "HEAD"

# ALTER TABLE RENAME TO: both engines emit a single row with
# from_table_name != to_table_name. doltlite's heuristic detects this
# by matching dropped+added pairs on iTable + tree root, which works
# for the pure-rename case (no data change in the same commit).
oracle "modified_rename_table" "
$SEED
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_table');
" "HEAD~1" "HEAD"

oracle "modified_rename_table_filter_old_name" "
$SEED
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_table');
" "HEAD~1" "HEAD" "t"

oracle "modified_add_not_null_default" "
$SEED
ALTER TABLE t ADD COLUMN extra VARCHAR(32) NOT NULL DEFAULT '';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_not_null');
" "HEAD~1" "HEAD"

oracle "modified_add_nullable_default" "
$SEED
ALTER TABLE t ADD COLUMN extra VARCHAR(32) DEFAULT 'hi';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_nullable');
" "HEAD~1" "HEAD"

oracle "modified_add_nullable_no_default" "
$SEED
ALTER TABLE t ADD COLUMN extra VARCHAR(32);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_bare');
" "HEAD~1" "HEAD"

# Multi-step: add col, populate it, drop it in follow-up commits.
# The commit range covering all three steps should still show the
# table as modified (net: add extra, populate, remove extra).
oracle "modified_net_addcol_dropcol_range" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
UPDATE t SET extra = 'x' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'populate');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_col_again');
" "HEAD~3" "HEAD"

# Multiple ALTER TABLE operations in a single commit should show up
# as a single modified-table row.
oracle "multiple_alters_single_commit" "
$SEED
ALTER TABLE t ADD COLUMN a TEXT;
ALTER TABLE t ADD COLUMN b INT;
ALTER TABLE t RENAME COLUMN v TO vv;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'many_alters');
" "HEAD~1" "HEAD"

# CREATE TABLE + ALTER TABLE in the same commit should only appear
# as an added-table row (the ALTER is rolled into the new table's
# initial schema), not as an added + modified pair.
oracle "create_then_alter_same_commit" "
$SEED
CREATE TABLE u(id INT PRIMARY KEY);
ALTER TABLE u ADD COLUMN v TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'create_and_alter');
" "HEAD~1" "HEAD"

echo "--- multiple changes in one diff ---"

oracle "multi_change" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi');
" "HEAD~1" "HEAD"

echo "--- no changes ---"

oracle "no_changes" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_only');
" "HEAD~1" "HEAD"

oracle "self_diff" "
$SEED
" "HEAD" "HEAD"

echo "--- branch refs ---"

oracle "branch_diff" "
$SEED
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add');
" "main" "feat"

echo "--- tag refs ---"

oracle "tag_diff" "
$SEED
SELECT dolt_tag('v1');
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'after_tag');
" "v1" "HEAD"

echo "--- table_name filter ---"

oracle "filter_added_table_only" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi');
" "HEAD~1" "HEAD" "u"

oracle "filter_modified_table_only" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi');
" "HEAD~1" "HEAD" "t"

oracle "filter_nonexistent_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "HEAD~1" "HEAD" "no_such_table"

oracle_query "single_arg_range" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "SELECT CONCAT('ROW|', from_table_name, '|', to_table_name, '|', \
      CASE WHEN from_create_statement IS NULL OR from_create_statement='' THEN 'N' ELSE 'Y' END, '|', \
      CASE WHEN to_create_statement   IS NULL OR to_create_statement=''   THEN 'N' ELSE 'Y' END \
    ) FROM dolt_schema_diff('HEAD~1..HEAD') ORDER BY from_table_name, to_table_name;"

echo "--- error paths ---"

oracle_error "bad_from_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff('nope','HEAD');"

oracle_error "bad_to_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff('HEAD','nope');"

oracle_error "bad_single_arg" "$SEED" \
  "SELECT * FROM dolt_schema_diff('nope');"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
