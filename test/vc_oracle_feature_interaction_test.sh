#!/bin/bash
#
# Feature interaction oracle tests (doltlite vs Dolt)
#
# Exercises combinations of SQL and version-control operations that
# interact in non-obvious ways. Each test runs the same SQL against
# both engines and compares the final table state + commit log.
#
# Usage: bash vc_oracle_feature_interaction_test.sh ./doltlite dolt
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

# Normalize: strip hashes/timestamps, keep table data + log messages.
normalize() {
  tr -d '\r' | grep -v '^$' | sort
}

# oracle NAME SETUP_SQL QUERY_SQL
#   SETUP_SQL: DDL + DML + VC operations
#   QUERY_SQL: SELECT that produces the comparable output
oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # --- doltlite ---
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode csv\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^$' \
           | grep -vi 'already up to date' \
           | grep -vi 'Fast-forward' \
           | tr -d '"' \
           | normalize)

  # --- dolt ---
  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dolt_query
  dolt_query=$(vc_oracle_translate_for_dolt "$query")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n" "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    printf "%s\n" "$dolt_query" | "$DOLT" sql -r csv 2>>"$dir/dt.err" \
      | tail -n +2 | tr -d '"'
  ) 2>/dev/null
  dt_out=$(echo "$dt_out" | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | head -20 | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | head -20 | sed 's/^/      /'
  fi
}

echo "=== Feature Interaction Oracle Tests ==="
echo ""

# ═══════════════════════════════════════════════════════════════════
# Section 1: Merge + ALTER TABLE ADD COLUMN
# ═══════════════════════════════════════════════════════════════════
echo "--- merge + alter table add column ---"

oracle "merge_add_col_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'x';
UPDATE t SET extra='hello' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add col on feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','insert on main');
SELECT dolt_merge('feat');
" "SELECT id, val, extra FROM t ORDER BY id;"

oracle "merge_add_col_both_sides_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN c1 INTEGER DEFAULT 0;
UPDATE t SET c1=10 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add c1 on feat');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN c1 INTEGER DEFAULT 0;
UPDATE t SET c1=20 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add c1 on main');
SELECT dolt_merge('feat');
" "SELECT id, val, c1 FROM t ORDER BY id;"

oracle "merge_add_col_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN feat_col TEXT DEFAULT 'f';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add feat_col');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN main_col TEXT DEFAULT 'm';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add main_col');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_add_col_with_data_on_both" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'alice'),(2,'bob');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 0;
UPDATE t SET score=100 WHERE id=1;
INSERT INTO t VALUES(3,'charlie',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET name='ALICE' WHERE id=1;
INSERT INTO t VALUES(4,'dave');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes');
SELECT dolt_merge('feat');
" "SELECT id, name FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 2: Merge + INSERT/UPDATE/DELETE combinations
# ═══════════════════════════════════════════════════════════════════
echo "--- merge + DML combinations ---"

oracle "merge_insert_both_different_keys" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat insert');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_update_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_update_different_cols_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='FEAT_A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update a');
SELECT dolt_checkout('main');
UPDATE t SET b='MAIN_B' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update b');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "merge_delete_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_delete_both_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main delete');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_insert_update_delete_mix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d');
UPDATE t SET val='B_FEAT' WHERE id=2;
DELETE FROM t WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat mix');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'e');
UPDATE t SET val='A_MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main mix');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 3: Merge + composite primary keys
# ═══════════════════════════════════════════════════════════════════
echo "--- merge + composite PK ---"

oracle "merge_composite_pk_insert" "
CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(10,1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,2,'feat');
INSERT INTO t VALUES(20,1,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat inserts');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT a, b, val FROM t ORDER BY a, b;"

oracle "merge_composite_pk_update" "
CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(10,1,'v1'),(10,2,'v2'),(20,1,'v3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat' WHERE a=10 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update');
SELECT dolt_checkout('main');
UPDATE t SET val='main' WHERE a=20 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT a, b, val FROM t ORDER BY a, b;"

oracle "merge_composite_pk_delete_and_insert" "
CREATE TABLE t(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,'v1'),(1,2,'v2'),(2,1,'v3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE a=1 AND b=2;
INSERT INTO t VALUES(3,1,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET val='updated' WHERE a=2 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT a, b, val FROM t ORDER BY a, b;"

# ═══════════════════════════════════════════════════════════════════
# Section 4: Merge + NULL values
# ═══════════════════════════════════════════════════════════════════
echo "--- merge + NULL values ---"

oracle "merge_null_to_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base with null');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='hello' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat sets value');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_value_to_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'hello');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat nulls');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_null_in_multiple_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,NULL,'b',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='filled_a' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills a');
SELECT dolt_checkout('main');
UPDATE t SET c='filled_c' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main fills c');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 5: Cherry-pick
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick ---"

oracle "cherry_pick_single_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to cherry pick');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='updated' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','update on feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','delete on feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_with_prior_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit 1');
INSERT INTO t VALUES(4,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit 2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main diverge');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 6: Revert
# ═══════════════════════════════════════════════════════════════════
echo "--- revert ---"

oracle "revert_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'to_revert');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add row');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','delete row');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='changed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','update');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 7: Reset (soft and hard)
# ═══════════════════════════════════════════════════════════════════
echo "--- reset ---"

oracle "reset_soft_keeps_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
SELECT dolt_reset('HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

oracle "reset_hard_discards_working" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','second');
SELECT dolt_reset('--hard', 'HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 8: Multi-table merges
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-table merges ---"

oracle "merge_two_tables_independent" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,'b');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds to both');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(3,'c');
INSERT INTO t2 VALUES(3,'z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds to both');
SELECT dolt_merge('feat');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

oracle "merge_new_table_on_branch" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t2 VALUES(1,'new_table');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat creates t2');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies t1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "merge_drop_table_on_branch" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP TABLE t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops t2');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies t1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 9: UPSERT + version control
# ═══════════════════════════════════════════════════════════════════
echo "--- upsert + version control ---"

oracle "upsert_replace_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
REPLACE INTO t VALUES(1,'replaced');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat replaces');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "update_as_upsert_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, count INTEGER DEFAULT 0);
INSERT INTO t VALUES(1,'a',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET count=count+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat increment');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val, count FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 10: Text/Blob data types in merge
# ═══════════════════════════════════════════════════════════════════
echo "--- text/blob in merge ---"

oracle "merge_long_text_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO t VALUES(1,'short');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET body='this is a much longer text value that spans many bytes and tests whether large text fields merge correctly across branches';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat long text');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'another row');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, length(body) FROM t ORDER BY id;"

oracle "merge_blob_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
INSERT INTO t VALUES(1, X'DEADBEEF');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET data=X'CAFEBABE' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat blob update');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, X'0102030405');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, hex(data) FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 11: Numeric types in merge
# ═══════════════════════════════════════════════════════════════════
echo "--- numeric types in merge ---"

oracle "merge_integer_types" "
CREATE TABLE t(id INTEGER PRIMARY KEY, small INTEGER, big INTEGER);
INSERT INTO t VALUES(1, 42, 999999);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET small=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update small');
SELECT dolt_checkout('main');
UPDATE t SET big=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update big');
SELECT dolt_merge('feat');
" "SELECT id, small, big FROM t ORDER BY id;"

oracle "merge_real_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
INSERT INTO t VALUES(1, 3.14159);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=2.71828 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat update');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 1.41421);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 12: Fast-forward vs three-way
# ═══════════════════════════════════════════════════════════════════
echo "--- fast-forward vs three-way ---"

oracle "ff_merge_no_divergence" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat only');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "noff_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat', '--no-ff', '-m', 'merge feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 13: Branch + checkout state preservation
# ═══════════════════════════════════════════════════════════════════
echo "--- branch + checkout ---"

oracle "checkout_preserves_committed_state" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'main_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat_val');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit');
SELECT dolt_checkout('main');
" "SELECT id, val FROM t ORDER BY id;"

oracle "checkout_then_modify_and_return" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 14: Multiple sequential merges
# ═══════════════════════════════════════════════════════════════════
echo "--- sequential merges ---"

oracle "two_merges_same_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat1');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_after_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to cherry');
INSERT INTO t VALUES(3,'not cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','extra');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 15: DEFAULT values in merge
# ═══════════════════════════════════════════════════════════════════
echo "--- default values in merge ---"

oracle "merge_with_default_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT DEFAULT 'default_val', num INTEGER DEFAULT 42);
INSERT INTO t(id) VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id) VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat insert with defaults');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'explicit',99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main insert explicit');
SELECT dolt_merge('feat');
" "SELECT id, val, num FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 16: Large row counts
# ═══════════════════════════════════════════════════════════════════
echo "--- large row counts ---"

oracle "merge_many_inserts_each_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'f10'),(11,'f11'),(12,'f12'),(13,'f13'),(14,'f14');
INSERT INTO t VALUES(15,'f15'),(16,'f16'),(17,'f17'),(18,'f18'),(19,'f19');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds 10');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(20,'m20'),(21,'m21'),(22,'m22'),(23,'m23'),(24,'m24');
INSERT INTO t VALUES(25,'m25'),(26,'m26'),(27,'m27'),(28,'m28'),(29,'m29');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds 10');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "merge_update_disjoint_ranges" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0),(8,0),(9,0),(10,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=val+1 WHERE id<=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates 1-5');
SELECT dolt_checkout('main');
UPDATE t SET val=val+10 WHERE id>5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates 6-10');
SELECT dolt_merge('feat');
" "SELECT sum(val) FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 17: Empty/boundary conditions
# ═══════════════════════════════════════════════════════════════════
echo "--- empty/boundary conditions ---"

oracle "merge_empty_table_both_add_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds to empty');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds to empty');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_one_side_empty_other_inserts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat inserts');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_empty_diff" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','noop commit', '--allow-empty');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 18: Tag interactions
# ═══════════════════════════════════════════════════════════════════
echo "--- tag interactions ---"

oracle "tag_survives_branch_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'tagged');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to tag');
SELECT dolt_tag('v1');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d', 'feat');
SELECT dolt_checkout('v1');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 19: Wide tables (many columns)
# ═══════════════════════════════════════════════════════════════════
echo "--- wide tables ---"

oracle "merge_wide_table_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='FEAT', c3='FEAT3' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates c1,c3');
SELECT dolt_checkout('main');
UPDATE t SET c5='MAIN', c7='MAIN7' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates c5,c7');
SELECT dolt_merge('feat');
" "SELECT id, c1, c2, c3, c4, c5, c6, c7, c8 FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 20: Savepoint + commit interaction
# ═══════════════════════════════════════════════════════════════════
echo "--- savepoint + commit ---"

oracle "commit_after_savepoint_release" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SAVEPOINT sp1;
INSERT INTO t VALUES(2,'in savepoint');
RELEASE sp1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after savepoint');
" "SELECT id, val FROM t ORDER BY id;"

oracle "commit_multiple_dml_before_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
UPDATE t SET val='A' WHERE id=1;
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','multiple dml');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 21: Merge + foreign keys
# ═══════════════════════════════════════════════════════════════════
echo "--- merge + foreign keys ---"

oracle "fk_insert_child_on_branch" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(2,2,'c2_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds child');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'c3_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_update_parent_and_child" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='P1_FEAT' WHERE id=1;
UPDATE child SET val='c1_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates');
SELECT dolt_checkout('main');
UPDATE parent SET name='P2_MAIN' WHERE id=2;
UPDATE child SET val='c2_main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates');
SELECT dolt_merge('feat');
" "SELECT p.id, p.name, c.id, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY p.id, c.id;"

oracle "fk_delete_parent_no_children" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2'),(3,'p3');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes parentless');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT id, name FROM parent ORDER BY id;"

oracle "fk_add_parent_and_child_on_branch" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
INSERT INTO child VALUES(1,2,'new child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds parent+child');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,1,'main child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT c.id, c.pid, c.val FROM child c ORDER BY c.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 22: Merge + secondary indexes
# ═══════════════════════════════════════════════════════════════════
echo "--- merge + secondary indexes ---"

oracle "merge_with_indexed_col_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, score INTEGER);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
CREATE INDEX idx_score ON t(score);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add index');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val, score FROM t ORDER BY id;"

oracle "merge_with_index_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, score INTEGER);
CREATE INDEX idx_score ON t(score);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'c',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET score=100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates score');
SELECT dolt_checkout('main');
UPDATE t SET score=200 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates score');
SELECT dolt_merge('feat');
" "SELECT id, val, score FROM t ORDER BY score;"

oracle "merge_with_unique_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE);
INSERT INTO t VALUES(1,'alice@test'),(2,'bob@test');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET email='alice_new@test' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat renames alice');
SELECT dolt_checkout('main');
UPDATE t SET email='bob_new@test' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main renames bob');
SELECT dolt_merge('feat');
" "SELECT id, email FROM t ORDER BY id;"

oracle "merge_with_composite_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, val TEXT);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1,10,20,'v1'),(2,10,30,'v2'),(3,20,10,'v3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=30 WHERE id=1;
INSERT INTO t VALUES(4,10,40,'v4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes');
SELECT dolt_checkout('main');
UPDATE t SET b=50 WHERE id=3;
INSERT INTO t VALUES(5,20,20,'v5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes');
SELECT dolt_merge('feat');
" "SELECT id, a, b, val FROM t ORDER BY a, b, id;"

# ═══════════════════════════════════════════════════════════════════
# Section 23: Cherry-pick edge cases
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick edge cases ---"

oracle "cherry_pick_into_diverged_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'cherry_this');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to_cherry');
SELECT dolt_checkout('main');
UPDATE t SET val='main_changed' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main diverges');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_delete_into_modified" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes 3');
SELECT dolt_checkout('main');
UPDATE t SET val='main_1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates 1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_multi_row_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='X' WHERE id IN (1,3);
DELETE FROM t WHERE id=4;
INSERT INTO t VALUES(5,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat multi change');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 24: Revert edge cases
# ═══════════════════════════════════════════════════════════════════
echo "--- revert edge cases ---"

oracle "revert_middle_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_revert('HEAD~1');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_then_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'will_revert');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add row');
SELECT dolt_revert('HEAD');
INSERT INTO t VALUES(3,'after_revert');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after revert');
" "SELECT id, val FROM t ORDER BY id;"

oracle "revert_update_restores_original" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, num INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='changed', num=99 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','update');
SELECT dolt_revert('HEAD');
" "SELECT id, val, num FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 25: Diamond merges
# ═══════════════════════════════════════════════════════════════════
echo "--- diamond merges ---"

oracle "diamond_merge_no_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
UPDATE t SET val='left' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
UPDATE t SET val='right' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, val FROM t ORDER BY id;"

oracle "diamond_merge_insert_both_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
INSERT INTO t VALUES(2,'left');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
INSERT INTO t VALUES(3,'right');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, val FROM t ORDER BY id;"

oracle "diamond_three_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
UPDATE t SET val='b1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
UPDATE t SET val='b2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
UPDATE t SET val='b3' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 26: Multiple tables with FK during merge
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-table FK merge ---"

oracle "fk_cascade_not_triggered_by_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='p1_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates parent');
SELECT dolt_checkout('main');
UPDATE child SET val='c2_main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates child');
SELECT dolt_merge('feat');
" "SELECT p.name, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY p.id;"

oracle "three_table_fk_chain_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), val TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id), val TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,1,'b1');
INSERT INTO c VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE a SET val='a1_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates a');
SELECT dolt_checkout('main');
UPDATE c SET val='c1_main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates c');
SELECT dolt_merge('feat');
" "SELECT a.val, b.val, c.val FROM a JOIN b ON b.aid=a.id JOIN c ON c.bid=b.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 27: Merge after multiple commits on each branch
# ═══════════════════════════════════════════════════════════════════
echo "--- deep branch history merge ---"

oracle "merge_after_3_commits_each" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'m1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
INSERT INTO t VALUES(6,'m2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main2');
INSERT INTO t VALUES(7,'m3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main3');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_with_updates_across_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'v0'),(2,'v0'),(3,'v0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='f1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
UPDATE t SET val='f2' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
UPDATE t SET val='m1' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main c1');
UPDATE t SET val='m2' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main c2');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 28: Type coercion in merge
# ═══════════════════════════════════════════════════════════════════
echo "--- type coercion ---"

oracle "merge_int_stored_as_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'100');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='200' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'300');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_mixed_null_types" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL);
INSERT INTO t VALUES(1, NULL, NULL, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base all null');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=42 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills int');
SELECT dolt_checkout('main');
UPDATE t SET c=3.14 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main fills real');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 29: Schema-only changes
# ═══════════════════════════════════════════════════════════════════
echo "--- schema-only changes ---"

oracle "add_col_no_data_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add col');
" "SELECT id, val FROM t ORDER BY id;"

oracle "add_col_then_populate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 0;
UPDATE t SET score=100 WHERE id=1;
INSERT INTO t VALUES(2,'b',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add and populate');
" "SELECT id, val, score FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 30: Merge + DROP TABLE on one side
# ═══════════════════════════════════════════════════════════════════
echo "--- drop table interactions ---"

oracle "drop_table_other_side_untouched" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DROP TABLE t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops t2');
SELECT dolt_checkout('main');
UPDATE t1 SET val='updated' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates t1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "create_new_table_on_branch_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t2 VALUES(1,'new');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat creates t2');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main modifies t1');
SELECT dolt_merge('feat');
" "SELECT 't1' AS tbl, id FROM t1 UNION ALL SELECT 't2', id FROM t2 ORDER BY 1, 2;"

# ═══════════════════════════════════════════════════════════════════
# Section 31: Merge with same row modified identically
# ═══════════════════════════════════════════════════════════════════
echo "--- convergent modifications ---"

oracle "both_sides_same_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'old');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='same_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='same_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "both_sides_insert_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "both_sides_delete_and_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,'reinserted');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main_changed' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 32: Reset interactions
# ═══════════════════════════════════════════════════════════════════
echo "--- reset edge cases ---"

oracle "soft_reset_then_recommit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('HEAD~1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 redo');
" "SELECT id, val FROM t ORDER BY id;"

oracle "hard_reset_then_rebuild" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard', 'HEAD~1');
INSERT INTO t VALUES(3,'after_reset');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 33: Multi-column cell merge
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-column cell merge ---"

oracle "cell_merge_4_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT);
INSERT INTO t VALUES(1,'a','b','c','d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='A', c='C' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes a,c');
SELECT dolt_checkout('main');
UPDATE t SET b='B', d='D' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes b,d');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c, d FROM t ORDER BY id;"

oracle "cell_merge_many_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, left_col TEXT, right_col TEXT);
INSERT INTO t VALUES(1,'L','R'),(2,'L','R'),(3,'L','R'),(4,'L','R'),(5,'L','R');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET left_col='FEAT' WHERE id IN (1,3,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET right_col='MAIN' WHERE id IN (2,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, left_col, right_col FROM t ORDER BY id;"

oracle "cell_merge_with_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='feat_a' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='main_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 34: Repeated merge of same branch
# ═══════════════════════════════════════════════════════════════════
echo "--- repeated merge ---"

oracle "merge_same_branch_twice" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'first_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'second_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_branch_back_and_forth" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'main_after');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main after merge');
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 35: Cherry-pick + merge interaction
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick + merge ---"

oracle "cherry_pick_then_merge_same_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cherry target');
INSERT INTO t VALUES(3,'extra');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','extra');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 36: Negative numbers and zero
# ═══════════════════════════════════════════════════════════════════
echo "--- negative numbers and zero ---"

oracle "merge_negative_ids" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(-1,'neg'),(0,'zero'),(1,'pos');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=-1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_zero_and_null_distinction" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
INSERT INTO t VALUES(1, 0, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=0 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 37: Empty string vs NULL
# ═══════════════════════════════════════════════════════════════════
echo "--- empty string vs NULL ---"

oracle "merge_empty_string_vs_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty string');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='filled' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main null');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 38: Merge with row-count-only verification
# ═══════════════════════════════════════════════════════════════════
echo "--- row count after merge ---"

oracle "merge_preserves_total_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=5;
INSERT INTO t VALUES(6,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 39: Multi-column PK merge
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-column PK merge ---"

oracle "three_col_pk_merge" "
CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER, val TEXT, PRIMARY KEY(a,b,c));
INSERT INTO t VALUES(1,1,1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,1,2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,2,1,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, c, val FROM t ORDER BY a, b, c;"

oracle "two_col_pk_cell_merge" "
CREATE TABLE t(a INTEGER, b INTEGER, x TEXT, y TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,'x0','y0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x='xF' WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET y='yM' WHERE a=1 AND b=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, x, y FROM t ORDER BY a, b;"

# ═══════════════════════════════════════════════════════════════════
# Section 40: Merge + add/commit workflow variations
# ═══════════════════════════════════════════════════════════════════
echo "--- add/commit workflow ---"

oracle "add_specific_table" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('t1');
SELECT dolt_commit('-m','only t1');
" "SELECT id, val FROM t1 ORDER BY id;"

oracle "add_all_then_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','both tables');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

oracle "commit_a_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A', '-m','auto add');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Results
# ═══════════════════════════════════════════════════════════════════
echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
