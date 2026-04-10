#!/bin/bash
#
# Version-control oracle test: dolt_schema_diff
#
# Runs identical schema-diff scenarios against doltlite and Dolt and
# compares the row output. Both engines now expose the same columns
# (from_table_name, to_table_name, from_create_statement,
# to_create_statement) and accept the same call form
# (`dolt_schema_diff('from','to'[,'tbl'])`), so the oracle can issue
# the same query string against both.
#
# Compared surface: (from_table_name, to_table_name, from_present,
# to_present) sorted, where {from,to}_present is Y/N for whether the
# create-statement column is non-empty. The create-statement TEXT is
# intentionally NOT compared row-for-row because Dolt and doltlite
# canonicalize CREATE TABLE differently (whitespace, type aliases,
# quoting, ENGINE=... suffix in Dolt).
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
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

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

  local dl_err=0
  printf "%s\n%s\n" "$setup" "$q" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then dl_err=1; fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_err=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    { echo "$dolt_setup"; echo "$q"; } | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
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
  "SELECT * FROM dolt_schema_diff('nope','HEAD');"

oracle_error "bad_to_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff('HEAD','nope');"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
