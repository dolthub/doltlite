#!/bin/bash
#
# Version-control oracle test: dolt_schema_diff
#
# Runs identical schema-diff scenarios against doltlite and Dolt and
# compares the resulting (table_name, diff_type) row sets. The two
# engines expose dolt_schema_diff via different SQL surfaces:
#
#   doltlite: SELECT ... FROM dolt_schema_diff
#               WHERE from_ref='X' AND to_ref='Y' [AND table_name='t']
#   dolt:     SELECT ... FROM dolt_schema_diff('X','Y' [, 't'])
#
# This is a known surface divergence that I'll file as a follow-up if
# the row output is otherwise consistent. For now the oracle issues
# each engine's native form and compares the output rows.
#
# Compared surface: (name, kind, from_present, to_present) sorted.
#   - name = the table the row is about
#   - kind = added | dropped | modified, derived from which side has the SQL
#   - from_present / to_present = Y/N, whether the create-stmt text is set
#
# doltlite and Dolt expose different column shapes:
#   doltlite: table_name, from_create_stmt,      to_create_stmt,      diff_type
#   dolt:     from_table_name, to_table_name, from_create_statement, to_create_statement
# The oracle normalizes each side to the common (name, kind, fY, tY) form.
# The create-stmt TEXT is intentionally not compared row-for-row because
# the two engines canonicalize CREATE TABLE differently (whitespace, type
# aliases, quoting, ENGINE=... suffix in Dolt).
#
# Surface divergence (tracked in #379): doltlite uses a vtable with
# hidden filter columns and different column names; Dolt uses a table
# function. Once #379 lands the per-engine query branches collapse.
#
# Usage: bash vc_oracle_schema_diff_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# $1=name, $2=setup SQL, $3=from_ref, $4=to_ref, $5=optional table filter.
oracle() {
  local name="$1" setup="$2" from_ref="$3" to_ref="$4" tbl="${5:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # doltlite uses a vtable with hidden filter columns and a literal
  # diff_type column. Normalize to (name, kind, fY, tY).
  local dl_filter=""
  if [ -n "$tbl" ]; then
    dl_filter=" AND table_name='$tbl'"
  fi
  local dl_q="SELECT 'ROW|' || table_name || '|' || diff_type || '|' || (CASE WHEN from_create_stmt IS NULL OR from_create_stmt='' THEN 'N' ELSE 'Y' END) || '|' || (CASE WHEN to_create_stmt IS NULL OR to_create_stmt='' THEN 'N' ELSE 'Y' END) FROM dolt_schema_diff WHERE from_ref='$from_ref' AND to_ref='$to_ref'$dl_filter ORDER BY table_name;"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ROW|' \
           | sort)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  # dolt uses a table function with from_table_name/to_table_name and
  # ..._statement instead of ..._stmt. Derive (name, kind) from which
  # side of the rename pair is empty. CONCAT, not ||.
  local dt_args
  if [ -n "$tbl" ]; then
    dt_args="'$from_ref','$to_ref','$tbl'"
  else
    dt_args="'$from_ref','$to_ref'"
  fi
  local dt_q="SELECT CONCAT('ROW|', \
                COALESCE(NULLIF(to_table_name,''), from_table_name), '|', \
                CASE \
                  WHEN from_table_name IS NULL OR from_table_name='' THEN 'added' \
                  WHEN to_table_name IS NULL OR to_table_name=''     THEN 'dropped' \
                  ELSE 'modified' \
                END, '|', \
                CASE WHEN from_create_statement IS NULL OR from_create_statement='' THEN 'N' ELSE 'Y' END, '|', \
                CASE WHEN to_create_statement   IS NULL OR to_create_statement=''   THEN 'N' ELSE 'Y' END \
              ) FROM dolt_schema_diff($dt_args) \
                ORDER BY COALESCE(NULLIF(to_table_name,''), from_table_name);"

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dt_q"
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
  local name="$1" setup="$2" dl_q="$3" dt_q="$4"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_err=0
  printf "%s\n%s\n" "$setup" "$dl_q" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then dl_err=1; fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_err=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    { echo "$dolt_setup"; echo "$dt_q"; } | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
  )
  if grep -qiE 'error|fail' "$dir/dt.out" "$dir/dt.err" 2>/dev/null; then dt_err=1; fi

  if [ "$dl_err" = 1 ] && [ "$dt_err" = 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite errored: $([ $dl_err = 1 ] && echo yes || echo NO)"
    echo "    dolt errored:     $([ $dt_err = 1 ] && echo yes || echo NO)"
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

echo "--- error paths ---"

oracle_error "bad_from_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff WHERE from_ref='nope' AND to_ref='HEAD';" \
  "SELECT * FROM dolt_schema_diff('nope','HEAD');"

oracle_error "bad_to_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff WHERE from_ref='HEAD' AND to_ref='nope';" \
  "SELECT * FROM dolt_schema_diff('HEAD','nope');"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
