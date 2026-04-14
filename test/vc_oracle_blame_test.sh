#!/bin/bash
#
# Version-control oracle test: dolt_blame_<table>
#
# Runs identical blame scenarios against doltlite and Dolt and
# compares the resulting (pk, commit_message) pairs. Commit hashes
# don't match across engines (prolly vs noms), so the oracle joins
# dolt_blame back to dolt_log by commit hash to get a stable
# comparison key.
#
# dolt_blame_<table> semantics (reverse-engineered from Dolt):
#
#   Schema: <pk_cols>, commit, commit_date, committer, email, message
#
#   Rows: one per LIVE row in the current table (deleted rows are
#   not reported). For each live row, "commit" is the most recent
#   commit that introduced the current value of that row, computed
#   by walking history first-parent from HEAD:
#     - At a linear commit, blame = that commit if the row's value
#       differs from the value in the commit's parent.
#     - At a merge commit (2+ parents), blame = merge commit if the
#       row's value differs from the merge base (LCA of parents);
#       otherwise continue walking first-parent.
#
#   Schema-only changes (ADD COLUMN with no row edits) do NOT update
#   blame. Revert-to-original and delete-then-reinsert-same-value DO
#   update blame (the latest commit that touched the row's storage
#   wins).
#
# Usage: bash vc_oracle_blame_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# $1=name, $2=setup SQL, $3=select list tagging rows like CONCAT('BL|', pk, '|', message).
# Each oracle compares doltlite and Dolt on the same scenario and
# extracts only tagged "BL|..." rows so the noise from CALL dolt_*()
# status/hash rows is filtered out.
oracle() {
  local name="$1" setup="$2" select_sql="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$select_sql" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^BL|' | sort)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$select_sql"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^BL|' | sort)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"
    echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"
    echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_blame_<table> ==="
echo ""

echo "--- linear history: single column int PK ---"

LINEAR_BASIC="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'SEED');
UPDATE t SET v = 200 WHERE id = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'BUMP2');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'ADD4');
DELETE FROM t WHERE id = 3;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'DROP3');
"

oracle "linear_basic" \
  "$LINEAR_BASIC" \
  "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- revert to original value attributes to the revert commit ---"

oracle "revert_to_original" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'SET10');
UPDATE t SET v = 20 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'SET20');
UPDATE t SET v = 10 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'BACK10');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t;"

echo "--- delete then reinsert same value attributes to the reinsert ---"

oracle "delete_reinsert_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INSERT');
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'DELETE');
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'REINSERT');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t;"

echo "--- schema-only change (ADD COLUMN) does not update blame ---"

oracle "add_column_no_blame_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10), (2, 20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INIT');
ALTER TABLE t ADD COLUMN w TEXT;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'ADDCOL');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- multi-column PK ---"

oracle "multi_col_pk" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one'), (1, 2, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C1');
UPDATE t SET v = 'TWO' WHERE a = 1 AND b = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C2');
INSERT INTO t VALUES (2, 1, 'x');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C3');
" "SELECT CONCAT('BL|', a, '-', b, '|', message) FROM dolt_blame_t ORDER BY a, b;"

echo "--- NULL values ---"

oracle "null_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, NULL), (2, 20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INIT');
UPDATE t SET v = 10 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'FILLNULL');
UPDATE t SET v = NULL WHERE id = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'NULLIFY2');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- text values ---"

oracle "text_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES (1, 'alice'), (2, 'bob');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INIT');
UPDATE t SET name = 'Bob' WHERE id = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'CAPB');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- fast-forward merge keeps original commit attribution ---"

oracle "ff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INIT');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 200);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'FEATADD');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- non-ff merge attributes new rows to the merge commit ---"

oracle "non_ff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INIT');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 200);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'FEATADD');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'MAINADD');
SELECT dolt_merge('feat', '--no-ff', '-m', 'MERGE');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- non-ff merge: rows unchanged from merge base keep their original blame ---"

oracle "non_ff_merge_preserves_base" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10), (3, 30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'INIT');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'PRE_BRANCH');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (5, 50);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'FEATADD');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'MAINADD');
SELECT dolt_merge('feat', '--no-ff', '-m', 'MERGE');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

echo "--- empty table returns no rows ---"

oracle "empty_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'CREATE');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
