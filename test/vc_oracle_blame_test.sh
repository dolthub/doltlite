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
  local name="$1" setup="$2" select_sql="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$select_sql" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^BL|' | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$select_sql"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^BL|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
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

echo "--- all-column PK (empty value record; row lives in the key) ---"

oracle "all_column_pk_int" "
CREATE TABLE t(a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1), (1, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C1');
INSERT INTO t VALUES (2, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C2');
INSERT INTO t VALUES (3, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C3');
" "SELECT CONCAT('BL|', a, '-', b, '|', message) FROM dolt_blame_t ORDER BY a, b;"

oracle "all_column_pk_text" "
CREATE TABLE t(a VARCHAR(16), b VARCHAR(16), PRIMARY KEY(a, b));
INSERT INTO t VALUES ('p', 'q'), ('p', 'r');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C1');
INSERT INTO t VALUES ('s', 't');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'C2');
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

echo "--- rename then recreate same-name family preserves blame by current table lineage ---"

oracle "rename_recreate_same_name_family" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO u;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'rename_u');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (7, 'z');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'new_t');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t ORDER BY id;"

oracle "rename_recreate_same_name_family_renamed_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO u;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'rename_u');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (7, 'z');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'new_t');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_u ORDER BY id;"

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

echo "--- schema replay after merge preserves blame visibility ---"

oracle "merge_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_u ORDER BY id;"

echo "--- schema replay after cherry-pick preserves blame visibility ---"

oracle "cherrypick_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_u ORDER BY id;"

echo "--- schema replay after rebase preserves blame visibility ---"

oracle "rebase_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (1, 'feat');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_u ORDER BY id;"

echo "--- FK replay after merge preserves blame visibility ---"

oracle "merge_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_c ORDER BY id;"

echo "--- FK replay after cherry-pick preserves blame visibility ---"

oracle "cherrypick_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_c ORDER BY id;"

echo "--- FK replay after rebase preserves blame visibility ---"

oracle "rebase_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_c ORDER BY id;"

echo "--- composite-PK replay after merge preserves blame visibility ---"

oracle "merge_replay_multi_pk_add_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO u VALUES (1, 1, 'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "SELECT CONCAT('BL|', a, '|', b, '|', message) FROM dolt_blame_u ORDER BY a, b;"

echo "--- composite-PK replay after cherry-pick preserves blame visibility ---"

oracle "cherrypick_replay_multi_pk_add_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO u VALUES (1, 1, 'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('BL|', a, '|', b, '|', message) FROM dolt_blame_u ORDER BY a, b;"

echo "--- composite-PK replay after rebase preserves blame visibility ---"

oracle "rebase_replay_multi_pk_add_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO u VALUES (1, 1, 'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "SELECT CONCAT('BL|', a, '|', b, '|', message) FROM dolt_blame_u ORDER BY a, b;"

echo "--- empty table returns no rows ---"

oracle "empty_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'CREATE');
" "SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t;" "EXPECT_EMPTY"

echo "--- temp shadow table does not spoof blame PK schema ---"

{
  dir="$TMPROOT/temp_shadow_pk"
  mkdir -p "$dir"
  out=$(printf "%s\n" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_commit('-Am', 'INIT');
UPDATE t SET v = 'b' WHERE id = 1;
SELECT dolt_commit('-Am', 'UPDATE');
CREATE TEMP TABLE t(x TEXT PRIMARY KEY, y INT);
SELECT CONCAT('BL|', id, '|', message) FROM dolt_blame_t;
" | "$DOLTLITE" "$dir/db" 2>"$dir/err" | tr -d '\r' | grep '^BL|')
  if [ "$out" = "BL|1|UPDATE" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES temp_shadow_pk"
    echo "  FAIL: temp_shadow_pk"
    echo "    doltlite:"
    echo "$out" | sed 's/^/      /'
  fi
}

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
