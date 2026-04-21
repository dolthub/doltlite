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
    printf "%s\n" "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>>"$dir/dt.err" \
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
# Section 41: Merge + many data types
# ═══════════════════════════════════════════════════════════════════
echo "--- many data types in merge ---"

oracle "merge_bool_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, active INTEGER);
INSERT INTO t VALUES(1,1),(2,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET active=0 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deactivates 1');
SELECT dolt_checkout('main');
UPDATE t SET active=1 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main activates 2');
SELECT dolt_merge('feat');
" "SELECT id, active FROM t ORDER BY id;"

oracle "merge_very_long_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO t VALUES(1,'short');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET body='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat long text');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, length(body) FROM t ORDER BY id;"

oracle "merge_large_integer_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, big INTEGER);
INSERT INTO t VALUES(1, 2147483647);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base large int');
SELECT dolt_checkout('-b','feat');
UPDATE t SET big=-2147483648 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat neg int');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, big FROM t ORDER BY id;"

oracle "merge_float_precision" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
INSERT INTO t VALUES(1, 0.1),(2, 0.2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=0.3 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val=0.4 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 42: Merge chain (A→B→C merges)
# ═══════════════════════════════════════════════════════════════════
echo "--- merge chains ---"

oracle "serial_branch_merge_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b2');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(4,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b3');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_chain_with_updates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
UPDATE t SET val='v1';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_checkout('-b','b2');
UPDATE t SET val='v2';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_merge('b2');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 43: Revert + merge interaction
# ═══════════════════════════════════════════════════════════════════
echo "--- revert + merge ---"

oracle "revert_then_merge_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='reverted';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','change');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_after_revert_on_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat_changed' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat change');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 44: Multiple table interactions
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-table complex ---"

oracle "merge_3_tables_mixed_ops" "
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, uid INTEGER, amount INTEGER);
CREATE TABLE items(id INTEGER PRIMARY KEY, oid INTEGER, name TEXT);
INSERT INTO users VALUES(1,'alice'),(2,'bob');
INSERT INTO orders VALUES(1,1,100),(2,2,200);
INSERT INTO items VALUES(1,1,'widget'),(2,2,'gadget');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO users VALUES(3,'charlie');
INSERT INTO orders VALUES(3,3,300);
UPDATE items SET name='WIDGET' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds user+order, updates item');
SELECT dolt_checkout('main');
UPDATE users SET name='ALICE' WHERE id=1;
DELETE FROM orders WHERE id=2;
INSERT INTO items VALUES(3,1,'accessory');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates user, deletes order, adds item');
SELECT dolt_merge('feat');
" "SELECT 'u' AS t, id, name FROM users UNION ALL SELECT 'i', id, name FROM items ORDER BY 1, 2;"

oracle "merge_with_unrelated_table_insert" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a'),(2,'b');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t1 SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates t1');
SELECT dolt_checkout('main');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main inserts t2');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t1 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 45: Idempotent operations
# ═══════════════════════════════════════════════════════════════════
echo "--- idempotent operations ---"

oracle "update_to_same_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='same' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','noop update');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_nonexistent_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DELETE FROM t WHERE id=999;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','noop delete');
" "SELECT id, val FROM t ORDER BY id;"

oracle "insert_delete_same_row_before_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'temp');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','insert then delete');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 46: Merge + column default interactions
# ═══════════════════════════════════════════════════════════════════
echo "--- column defaults in merge ---"

oracle "add_col_with_default_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
ALTER TABLE t ADD COLUMN flag INTEGER DEFAULT 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds col');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main inserts');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_rows_with_different_defaults" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, status TEXT DEFAULT 'active');
INSERT INTO t VALUES(1,'a','active');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id, val) VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat uses default');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c','inactive');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main explicit');
SELECT dolt_merge('feat');
" "SELECT id, val, status FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 47: Stress cell merge with many columns
# ═══════════════════════════════════════════════════════════════════
echo "--- stress cell merge ---"

oracle "cell_merge_8_cols_interleaved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='F1', c3='F3', c5='F5', c7='F7' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
UPDATE t SET c2='M2', c4='M4', c6='M6', c8='M8' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT id, c1, c2, c3, c4, c5, c6, c7, c8 FROM t ORDER BY id;"

oracle "cell_merge_10_rows_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','b'),(2,'a','b'),(3,'a','b'),(4,'a','b'),(5,'a','b');
INSERT INTO t VALUES(6,'a','b'),(7,'a','b'),(8,'a','b'),(9,'a','b'),(10,'a','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat a on odds');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main b on evens');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 48: Cherry-pick + FK
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick + FK ---"

oracle "cherry_pick_with_parent_child" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
INSERT INTO child VALUES(1,2,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds parent+child');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT p.id, p.name FROM parent p ORDER BY p.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 49: Revert + multi-table
# ═══════════════════════════════════════════════════════════════════
echo "--- revert + multi-table ---"

oracle "revert_multi_table_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t1 VALUES(2,'b');
INSERT INTO t2 VALUES(2,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add to both');
SELECT dolt_revert('HEAD');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

# ═══════════════════════════════════════════════════════════════════
# Section 50: Reset + branch interaction
# ═══════════════════════════════════════════════════════════════════
echo "--- reset + branch ---"

oracle "reset_hard_then_new_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3 after reset');
" "SELECT id, val FROM t ORDER BY id;"

oracle "branch_reset_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
INSERT INTO t VALUES(4,'main2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main2');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 51: Merge + NOT NULL constraints
# ═══════════════════════════════════════════════════════════════════
echo "--- NOT NULL in merge ---"

oracle "merge_not_null_col_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT NOT NULL);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_not_null_with_default" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT NOT NULL DEFAULT 'none');
INSERT INTO t(id) VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(id) VALUES(2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='updated' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 52: Interleaved branch operations
# ═══════════════════════════════════════════════════════════════════
echo "--- interleaved branch ops ---"

oracle "alternating_branch_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'m1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main1');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(4,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "branch_from_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','on feat1');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','on feat2 from feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_grandchild_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','child');
INSERT INTO t VALUES(2,'child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','child');
SELECT dolt_checkout('-b','grandchild');
INSERT INTO t VALUES(3,'grandchild');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','grandchild');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('grandchild');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 53: Large delete + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- large delete + merge ---"

oracle "delete_half_merge_other_half" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id <= 4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes first half');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id > 4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates second half');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_all_one_side_insert_other" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes all');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c'),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main inserts');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 54: Cherry-pick from deep history
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick from deep history ---"

oracle "cherry_pick_2nd_of_3_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
INSERT INTO t VALUES(3,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
INSERT INTO t VALUES(4,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 55: Merge + UPDATE same row multiple times
# ═══════════════════════════════════════════════════════════════════
echo "--- rapid updates before merge ---"

oracle "multiple_updates_before_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='v1';
UPDATE t SET val='v2';
UPDATE t SET val='v3';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat triple update');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "insert_update_delete_insert_same_id" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(1,'first');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,'second');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recreate row');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 56: Merge + CHECK constraints
# ═══════════════════════════════════════════════════════════════════
echo "--- CHECK constraints + merge ---"

oracle "merge_with_check_constraint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER CHECK(score >= 0));
INSERT INTO t VALUES(1, 50),(2, 75);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET score=90 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET score=80 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, score FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 57: Merge preserves row ordering
# ═══════════════════════════════════════════════════════════════════
echo "--- row ordering after merge ---"

oracle "merge_preserves_pk_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(10,'ten'),(20,'twenty'),(30,'thirty');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(15,'fifteen');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(25,'twentyfive');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 58: Merge after rename-like operations
# ═══════════════════════════════════════════════════════════════════
echo "--- rename-like operations ---"

oracle "delete_and_reinsert_different_val" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'old_name');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(1,'new_name');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat renames');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 59: Cross-branch FK integrity
# ═══════════════════════════════════════════════════════════════════
echo "--- cross-branch FK ---"

oracle "fk_both_branches_add_children" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2'),(3,'p3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(1,1,'fc1');
INSERT INTO child VALUES(2,2,'fc2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat children');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,2,'mc1');
INSERT INTO child VALUES(4,3,'mc2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main children');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_update_parent_merge_child" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='P1_UPDATED' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates parent');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child to p1');
SELECT dolt_merge('feat');
" "SELECT p.name, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY c.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 60: Merge + UNIQUE constraint
# ═══════════════════════════════════════════════════════════════════
echo "--- UNIQUE constraint + merge ---"

oracle "unique_col_merge_no_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, val TEXT);
INSERT INTO t VALUES(1,'A','first'),(2,'B','second');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code, val FROM t ORDER BY id;"

oracle "unique_col_insert_different_codes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, val TEXT);
INSERT INTO t VALUES(1,'A','base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'B','feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'C','main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 61: Merge + many rows same table different operations
# ═══════════════════════════════════════════════════════════════════
echo "--- complex row operations merge ---"

oracle "feat_inserts_main_deletes_no_overlap" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(6,'f'),(7,'g'),(8,'h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat inserts');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id IN (4,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main deletes');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "both_update_all_rows_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a1','b1'),(2,'a2','b2'),(3,'a3','b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='FA';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates all a');
SELECT dolt_checkout('main');
UPDATE t SET b='MB';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates all b');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "interleaved_insert_ids" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f'),(4,'f'),(6,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat even ids');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'m'),(5,'m'),(7,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main odd ids');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "update_same_rows_different_values_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, z INTEGER);
INSERT INTO t VALUES(1,0,0,0),(2,0,0,0),(3,0,0,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x=1 WHERE id=1;
UPDATE t SET x=2 WHERE id=2;
UPDATE t SET x=3 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat x');
SELECT dolt_checkout('main');
UPDATE t SET y=10 WHERE id=1;
UPDATE t SET y=20 WHERE id=2;
UPDATE t SET z=30 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main y,z');
SELECT dolt_merge('feat');
" "SELECT id, x, y, z FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 62: Multiple merges building on each other
# ═══════════════════════════════════════════════════════════════════
echo "--- accumulating merges ---"

oracle "merge_5_feature_branches" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_merge('f1');
SELECT dolt_checkout('-b','f2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('f2');
SELECT dolt_checkout('-b','f3');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('f3');
SELECT dolt_checkout('-b','f4');
INSERT INTO t VALUES(5,'f4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_merge('f4');
SELECT dolt_checkout('-b','f5');
INSERT INTO t VALUES(6,'f5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
SELECT dolt_checkout('main');
SELECT dolt_merge('f5');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_then_modify_then_merge_again" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
UPDATE t SET val='post_merge' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post-merge edit');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c2');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 63: Merge + various PK types
# ═══════════════════════════════════════════════════════════════════
echo "--- various PK types ---"

oracle "merge_negative_pk" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(-10,'neg10'),(-5,'neg5'),(0,'zero'),(5,'pos5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(-3,'feat');
UPDATE t SET val='FEAT' WHERE id=-10;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
UPDATE t SET val='MAIN' WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_sparse_pk_range" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(100,'a'),(200,'b'),(300,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(150,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat between');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(250,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main between');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_pk_at_boundaries" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'first'),(1000000,'last');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'near_start');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(999999,'near_end');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 64: Cherry-pick + multi-table
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick + multi-table ---"

oracle "cherry_pick_multi_table_commit" "
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
SELECT dolt_cherry_pick('feat');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"

# ═══════════════════════════════════════════════════════════════════
# Section 65: Merge + empty values
# ═══════════════════════════════════════════════════════════════════
echo "--- empty string handling ---"

oracle "merge_empty_string_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='notempty' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fills');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds empty');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_null_vs_empty_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,''),(2,'','hello');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='filled' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='world' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 66: Merge + multiple indexes
# ═══════════════════════════════════════════════════════════════════
echo "--- multiple indexes + merge ---"

oracle "merge_table_with_unique_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT UNIQUE, age INTEGER);
INSERT INTO t VALUES(1,'alice',30),(2,'bob',25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'charlie',35);
UPDATE t SET age=31 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'dave',28);
UPDATE t SET name='BOB' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, name, age FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 67: Revert + cherry-pick combined
# ═══════════════════════════════════════════════════════════════════
echo "--- revert + cherry-pick ---"

oracle "revert_then_cherry_pick_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'added');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add row');
SELECT dolt_revert('HEAD');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 68: Merge + auto-increment-like patterns
# ═══════════════════════════════════════════════════════════════════
echo "--- auto-increment patterns ---"

oracle "merge_with_max_id_pattern" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'feat4'),(5,'feat5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat auto ids');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,'main6'),(7,'main7');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main auto ids');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 69: Complex FK merge scenarios
# ═══════════════════════════════════════════════════════════════════
echo "--- complex FK merge ---"

oracle "fk_insert_parent_one_side_child_other" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1');
INSERT INTO child VALUES(1,1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO parent VALUES(2,'p2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds parent');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,1,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main adds child');
SELECT dolt_merge('feat');
" "SELECT p.id, p.name FROM parent p ORDER BY p.id;"

oracle "fk_merge_update_referenced_parent" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='P1_NEW' WHERE id=1;
UPDATE child SET val='c1_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT c.id, c.pid, c.val FROM child c ORDER BY c.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 70: Merge + multiple commits per branch (stress)
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-commit branch stress ---"

oracle "five_commits_per_branch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=val+1 WHERE id=1;
INSERT INTO t VALUES(2,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
UPDATE t SET val=val+1 WHERE id=1;
INSERT INTO t VALUES(3,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
UPDATE t SET val=val+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
INSERT INTO t VALUES(11,11);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m2');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 71: Verify merge doesn't duplicate rows
# ═══════════════════════════════════════════════════════════════════
echo "--- merge row dedup ---"

oracle "no_duplicates_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'shared'),(2,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "no_duplicates_convergent_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1,'same'),(2,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(1,'same'),(2,'same');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 72: Merge preserves data integrity
# ═══════════════════════════════════════════════════════════════════
echo "--- merge data integrity ---"

oracle "merge_sum_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount INTEGER);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base sum=600');
SELECT dolt_checkout('-b','feat');
UPDATE t SET amount=150 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes 1');
SELECT dolt_checkout('main');
UPDATE t SET amount=250 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes 2');
SELECT dolt_merge('feat');
" "SELECT sum(amount) FROM t;"

oracle "merge_min_max_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=5 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat lowers min');
SELECT dolt_checkout('main');
UPDATE t SET val=60 WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main raises max');
SELECT dolt_merge('feat');
" "SELECT min(val), max(val) FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 73: Cherry-pick preserves other data
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick data preservation ---"

oracle "cherry_pick_doesnt_affect_other_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'keep'),(2,'keep'),(3,'keep');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='changed' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat changes one');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cherry_pick_preserves_main_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changed');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 74: Revert preserves other data
# ═══════════════════════════════════════════════════════════════════
echo "--- revert data preservation ---"

oracle "revert_one_row_keeps_others" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
UPDATE t SET val='X' WHERE id=2;
INSERT INTO t VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','changes');
SELECT dolt_revert('HEAD');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 75: Merge + column ordering
# ═══════════════════════════════════════════════════════════════════
echo "--- column ordering ---"

oracle "select_cols_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first_col TEXT, second_col TEXT, third_col TEXT);
INSERT INTO t VALUES(1,'a','b','c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET first_col='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET third_col='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT first_col, second_col, third_col FROM t WHERE id=1;"

# ═══════════════════════════════════════════════════════════════════
# Section 76-80: Merge + various value patterns
# ═══════════════════════════════════════════════════════════════════
echo "--- value patterns in merge ---"

oracle "merge_special_chars_in_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'hello world'),(2,'line1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='has ''quotes''' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='has,commas' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_unicode_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'ascii');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='hello' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'world');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_with_spaces_in_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'  leading'),(2,'trailing  '),(3,' both ');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='no_spaces' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='also_no_spaces' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_zero_length_blob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
INSERT INTO t VALUES(1, X''),(2, X'FF');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET data=X'AABB' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET data=X'CCDD' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, hex(data) FROM t ORDER BY id;"

oracle "merge_numeric_text_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'100'),(2,'200');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='150' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='250' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 81-85: More FK interaction patterns
# ═══════════════════════════════════════════════════════════════════
echo "--- more FK patterns ---"

oracle "fk_both_add_children_to_same_parent" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'shared_parent');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(1,1,'feat_child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,1,'main_child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_delete_child_merge_parent_update" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM child WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes child');
SELECT dolt_checkout('main');
UPDATE parent SET name='P2_UPDATED' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main updates parent');
SELECT dolt_merge('feat');
" "SELECT c.id, c.val FROM child c ORDER BY c.id;"

oracle "fk_multiple_children_per_parent_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1a'),(2,1,'c1b'),(3,2,'c2a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(4,1,'c1c_feat');
UPDATE child SET val='c2a_feat' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(5,2,'c2b_main');
UPDATE child SET val='c1b_main' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM child ORDER BY id;"

oracle "fk_self_referencing_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id), val TEXT);
INSERT INTO t VALUES(1, NULL, 'root');
INSERT INTO t VALUES(2, 1, 'child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3, 1, 'feat_child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4, 2, 'main_grandchild');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM t ORDER BY id;"

oracle "fk_update_both_parent_and_child_same_branch" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,1,'c1'),(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE parent SET name='p1_new' WHERE id=1;
UPDATE child SET val='c1_new' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat both');
SELECT dolt_checkout('main');
UPDATE parent SET name='p2_new' WHERE id=2;
UPDATE child SET val='c2_new' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main both');
SELECT dolt_merge('feat');
" "SELECT p.name, c.val FROM parent p JOIN child c ON c.pid=p.id ORDER BY p.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 86-90: Merge topology stress
# ═══════════════════════════════════════════════════════════════════
echo "--- merge topology stress ---"

oracle "merge_into_already_merged_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
SELECT dolt_checkout('main');
" "SELECT id, val FROM t ORDER BY id;"

oracle "parallel_branches_same_base_merge_both" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(2,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(3,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(4,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_branch_that_merged_another" "
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
SELECT dolt_merge('feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "ff_then_three_way_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'ff');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat ff');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main diverge');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "long_linear_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(4,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(5,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(6,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 91-95: Merge + batch operations
# ═══════════════════════════════════════════════════════════════════
echo "--- batch operations + merge ---"

oracle "batch_insert_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'k'),(12,'l'),(13,'m'),(14,'n'),(15,'o');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main batch');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_delete_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch delete odds');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main batch delete evens');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_update_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','x'),(2,'a','x'),(3,'a','x'),(4,'a','x'),(5,'a','x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id IN (1,2,3);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id IN (3,4,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 96-100: Edge cases for dolt_status/dolt_add
# ═══════════════════════════════════════════════════════════════════
echo "--- add/status edge cases ---"

oracle "add_after_drop_and_recreate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'first');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'second');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','recreated');
" "SELECT id, val FROM t ORDER BY id;"

oracle "multiple_add_before_commit" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t1 VALUES(1,'a');
SELECT dolt_add('t1');
INSERT INTO t2 VALUES(1,'x');
SELECT dolt_add('t2');
SELECT dolt_commit('-m','staged both');
" "SELECT 't1' AS tbl, id, val FROM t1 UNION ALL SELECT 't2', id, val FROM t2 ORDER BY 1, 2;"


# ═══════════════════════════════════════════════════════════════════
# Section 101: Conflict boundaries
# ═══════════════════════════════════════════════════════════════════
echo "--- conflict boundaries ---"

oracle "convergent_same_field_same_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='converge' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='converge' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "convergent_both_set_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'notnull');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "cell_merge_null_to_value_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1, NULL, 'base_b');
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

oracle "cell_merge_value_to_null_one_side" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'val_a','val_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='new_b' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "conflict_same_field_safe_rows_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'conflict_target'),(2,'safe'),(3,'safe2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='feat_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='main_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t WHERE id>=2 ORDER BY id;"

oracle "conflict_null_vs_value_same_field" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, other TEXT);
INSERT INTO t VALUES(1,'orig','keep');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=NULL WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='different' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, other FROM t WHERE id=1;"

oracle "delete_modify_conflict_safe_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'target'),(2,'safe'),(3,'safe2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='modified' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t WHERE id>=2 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 102: Commit graph verification
# ═══════════════════════════════════════════════════════════════════
echo "--- commit graph counts ---"

oracle "linear_3_commits_count" "
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
" "SELECT count(*) FROM dolt_log;"

oracle "merge_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "ff_merge_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "cherry_pick_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cherry');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT count(*) FROM dolt_log;"

oracle "revert_commit_count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','to revert');
SELECT dolt_revert('HEAD');
" "SELECT count(*) FROM dolt_log;"

# ═══════════════════════════════════════════════════════════════════
# Section 103: PK edge cases
# ═══════════════════════════════════════════════════════════════════
echo "--- PK edge cases in merge ---"

oracle "pk_zero_in_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='FEAT_ZERO' WHERE id=0;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN_ONE' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "pk_negative_in_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(-5,'neg'),(0,'zero'),(5,'pos');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(-10,'very_neg');
UPDATE t SET val='F' WHERE id=-5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'very_pos');
UPDATE t SET val='M' WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "pk_gaps_fill_from_both_sides" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(10,'a'),(20,'b'),(30,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(15,'f15'),(25,'f25');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5,'m5'),(35,'m35');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 104: INT PRIMARY KEY (WITHOUT ROWID)
# ═══════════════════════════════════════════════════════════════════
echo "--- INT PK (WITHOUT ROWID) ---"

oracle "int_pk_insert_merge" "
CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "int_pk_cell_merge" "
CREATE TABLE t(id INT PRIMARY KEY, x TEXT, y TEXT);
INSERT INTO t VALUES(1,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x='F' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET y='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, x, y FROM t ORDER BY id;"

oracle "int_pk_delete_merge" "
CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val='M' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "int_pk_cherry_pick" "
CREATE TABLE t(id INT PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','cherry');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 105: Undo patterns
# ═══════════════════════════════════════════════════════════════════
echo "--- undo patterns ---"

echo "--- merge topology ---"

oracle "merge_5_branches_serial" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','f1');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_merge('f1');
SELECT dolt_checkout('-b','f2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('f2');
SELECT dolt_checkout('-b','f3');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
SELECT dolt_checkout('main');
SELECT dolt_merge('f3');
SELECT dolt_checkout('-b','f4');
INSERT INTO t VALUES(5,'f4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_merge('f4');
SELECT dolt_checkout('-b','f5');
INSERT INTO t VALUES(6,'f5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f5');
SELECT dolt_checkout('main');
SELECT dolt_merge('f5');
" "SELECT id, val FROM t ORDER BY id;"

oracle "branch_from_branch_merge_grandchild" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','child');
INSERT INTO t VALUES(2,'child');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','child');
SELECT dolt_checkout('-b','grandchild');
INSERT INTO t VALUES(3,'grandchild');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','grandchild');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('grandchild');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_branch_that_already_merged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat1');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_merge('feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "ff_then_three_way" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'ff');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_checkout('-b','feat2');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat2');
" "SELECT id, val FROM t ORDER BY id;"

oracle "merge_back_and_forth" "
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
INSERT INTO t VALUES(3,'main_post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post merge');
SELECT dolt_checkout('feat');
SELECT dolt_merge('main');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 107: Stress cell merge
# ═══════════════════════════════════════════════════════════════════
echo "--- stress cell merge ---"

oracle "cell_merge_8_cols_interleaved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='F1', c3='F3', c5='F5', c7='F7' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
UPDATE t SET c2='M2', c4='M4', c6='M6', c8='M8' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT c1,c2,c3,c4,c5,c6,c7,c8 FROM t WHERE id=1;"

oracle "cell_merge_10_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT, c9 TEXT, c10 TEXT);
INSERT INTO t VALUES(1,'a','b','c','d','e','f','g','h','i','j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET c1='FEAT' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat c1');
SELECT dolt_checkout('main');
UPDATE t SET c10='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main c10');
SELECT dolt_merge('feat');
" "SELECT c1, c10 FROM t WHERE id=1;"

oracle "cell_merge_many_rows_alternating" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','b'),(2,'a','b'),(3,'a','b'),(4,'a','b'),(5,'a','b');
INSERT INTO t VALUES(6,'a','b'),(7,'a','b'),(8,'a','b'),(9,'a','b'),(10,'a','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "cell_merge_20_rows_all_different_cols" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, y TEXT);
INSERT INTO t VALUES(1,'x','y'),(2,'x','y'),(3,'x','y'),(4,'x','y'),(5,'x','y');
INSERT INTO t VALUES(6,'x','y'),(7,'x','y'),(8,'x','y'),(9,'x','y'),(10,'x','y');
INSERT INTO t VALUES(11,'x','y'),(12,'x','y'),(13,'x','y'),(14,'x','y'),(15,'x','y');
INSERT INTO t VALUES(16,'x','y'),(17,'x','y'),(18,'x','y'),(19,'x','y'),(20,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET x='F';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat all x');
SELECT dolt_checkout('main');
UPDATE t SET y='M';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main all y');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, count(CASE WHEN x='F' THEN 1 END) AS xf, count(CASE WHEN y='M' THEN 1 END) AS ym FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 108: FK stress
# ═══════════════════════════════════════════════════════════════════
echo "--- FK stress ---"

oracle "fk_self_ref_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id), val TEXT);
INSERT INTO t VALUES(1, NULL, 'root');
INSERT INTO t VALUES(2, 1, 'child1');
INSERT INTO t VALUES(3, 1, 'child2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4, 2, 'grandchild_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(5, 3, 'grandchild_main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, pid, val FROM t ORDER BY id;"

oracle "fk_multiple_tables_merge" "
CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE emp(id INTEGER PRIMARY KEY, did INTEGER REFERENCES dept(id), name TEXT);
INSERT INTO dept VALUES(1,'eng'),(2,'sales');
INSERT INTO emp VALUES(1,1,'alice'),(2,2,'bob');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dept VALUES(3,'ops');
INSERT INTO emp VALUES(3,3,'charlie');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO emp VALUES(4,1,'dave');
UPDATE dept SET name='engineering' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT e.id, e.name, d.name AS dept FROM emp e JOIN dept d ON e.did=d.id ORDER BY e.id;"

oracle "fk_both_add_children_same_parent" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), val TEXT);
INSERT INTO parent VALUES(1,'shared');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO child VALUES(1,1,'feat_child1');
INSERT INTO child VALUES(2,1,'feat_child2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(3,1,'main_child1');
INSERT INTO child VALUES(4,1,'main_child2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM child ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 109: Batch operations
# ═══════════════════════════════════════════════════════════════════
echo "--- batch operations ---"

oracle "batch_insert_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f'),(3,'f'),(4,'f'),(5,'f'),(6,'f'),(7,'f'),(8,'f'),(9,'f'),(10,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat 9 rows');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(11,'m'),(12,'m'),(13,'m'),(14,'m'),(15,'m');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main 5 rows');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_delete_disjoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id IN (1,3,5,7,9);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat odds');
SELECT dolt_checkout('main');
DELETE FROM t WHERE id IN (2,4,6,8,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main evens');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "batch_update_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a','x'),(2,'a','x'),(3,'a','x'),(4,'a','x'),(5,'a','x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='F' WHERE id<=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='M' WHERE id>=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 110: Aggregate verification
# ═══════════════════════════════════════════════════════════════════
echo "--- aggregate verification ---"

oracle "merge_preserves_sum" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount INTEGER);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET amount=150 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET amount=250 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT sum(amount) FROM t;"

oracle "merge_preserves_count_with_mixed_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base 5');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=5;
INSERT INTO t VALUES(6,'f');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat -1+1');
SELECT dolt_checkout('main');
UPDATE t SET val='A' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main update');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM t;"

oracle "merge_group_by_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'X',10),(2,'X',20),(3,'Y',30),(4,'Y',40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=15 WHERE id=1;
INSERT INTO t VALUES(5,'X',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET val=35 WHERE id=3;
INSERT INTO t VALUES(6,'Y',60);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;"



# ═══════════════════════════════════════════════════════════════════
# Section 112: Net-no-op commits (empty commit rejected, merge no-op)
# ═══════════════════════════════════════════════════════════════════
echo "--- net-no-op commit + merge ---"

oracle "roundtrip_update_no_net_change" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val='temp';
UPDATE t SET val='original';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat no-op roundtrip');
SELECT dolt_checkout('main');
UPDATE t SET val='main_change' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "delete_reinsert_same_value_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'keep'),(2,'target');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES(2,'target');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat recreate');
SELECT dolt_checkout('main');
UPDATE t SET val='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

oracle "insert_delete_net_zero_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(99,'temp');
DELETE FROM t WHERE id=99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat no net change');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 92: UPDATE with CASE + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- UPDATE CASE + merge ---"

oracle "update_case_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'c',30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET val=CASE WHEN n>15 THEN 'big' ELSE 'small' END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat case');
SELECT dolt_checkout('main');
UPDATE t SET n=n+100 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, val, n FROM t ORDER BY id;"

oracle "case_in_select_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,5),(2,15),(3,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,35);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, CASE WHEN n<10 THEN 's' WHEN n<20 THEN 'm' ELSE 'l' END AS sz FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 93: Subquery in WHERE + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- subquery WHERE + merge ---"

oracle "update_where_subquery_then_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, threshold INTEGER);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t1 SET v=0 WHERE v < (SELECT threshold FROM t2 WHERE id=1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t1 ORDER BY id;"

oracle "delete_where_subquery_then_merge" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE t2(id INTEGER PRIMARY KEY, cutoff INTEGER);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,25);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t1 WHERE v > (SELECT cutoff FROM t2 WHERE id=1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat del');
SELECT dolt_checkout('main');
INSERT INTO t1 VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t1 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 94: INSERT SELECT + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- INSERT SELECT + merge ---"

oracle "insert_select_from_other_table_merge" "
CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE dst(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO src VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO dst SELECT id, v FROM src WHERE id <= 2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat copy');
SELECT dolt_checkout('main');
INSERT INTO src VALUES(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main src++');
SELECT dolt_merge('feat');
" "SELECT id, v FROM dst ORDER BY id;"

oracle "insert_select_same_table_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, grp INTEGER);
INSERT INTO t VALUES(1,'a',1),(2,'b',1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t SELECT id+10, v, grp+1 FROM t WHERE id<=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat self copy');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v, grp FROM t WHERE id>=10 ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 95: LIKE / IN / BETWEEN + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- LIKE/IN/BETWEEN + merge ---"

oracle "update_where_like_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES(1,'apple'),(2,'apricot'),(3,'banana'),(4,'cherry');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET name='FRUIT_A' WHERE name LIKE 'ap%';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET name='MAIN_B' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, name FROM t ORDER BY id;"

oracle "update_where_in_list_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='X' WHERE id IN (1,3,5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='M' WHERE id IN (2,4);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "delete_where_between_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id BETWEEN 2 AND 4;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='M' WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 96: Aggregates after merge
# ═══════════════════════════════════════════════════════════════════
echo "--- aggregates after merge ---"

oracle "sum_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n+1 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT sum(n) AS s FROM t;"

oracle "group_by_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'a',100),(5,'b',50);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, sum(n) AS total FROM t GROUP BY grp ORDER BY grp;"

oracle "avg_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT sum(n)/count(*) AS a FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 97: HEAD~N refs + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- HEAD~N refs ---"

oracle "reset_to_head_tilde_2" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_reset('--hard','HEAD~2');
" "SELECT id, v FROM t ORDER BY id;"

oracle "merge_branch_after_reset_head_tilde" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_checkout('-b','side','HEAD~1');
INSERT INTO t VALUES(99,'side');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','side commit');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 98: allow-empty + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- allow-empty commit + merge ---"

oracle "allow_empty_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','empty marker','--allow-empty');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat data');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "allow_empty_only_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
SELECT dolt_commit('-m','just empty','--allow-empty');
SELECT dolt_commit('-m','another empty','--allow-empty');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 99: Multi-branch diamond patterns
# ═══════════════════════════════════════════════════════════════════
echo "--- diamond via branches ---"

oracle "diamond_with_cell_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'a0','b0');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','left');
UPDATE t SET a='L' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
UPDATE t SET b='R' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT id, a, b FROM t;"

oracle "diamond_independent_tables" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty');
SELECT dolt_checkout('-b','left');
INSERT INTO t1 VALUES(1,'l1'),(2,'l2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','left');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','right');
INSERT INTO t2 VALUES(1,'r1'),(2,'r2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('left');
SELECT dolt_merge('right');
" "SELECT 't1' AS tbl, count(*) AS n FROM t1 UNION ALL SELECT 't2', count(*) FROM t2 ORDER BY 1;"

# ═══════════════════════════════════════════════════════════════════
# Section 100: Multi-level FK chain + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- multi-level FK + merge ---"

oracle "four_level_fk_chain_merge" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id), v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id), v TEXT);
CREATE TABLE d(id INTEGER PRIMARY KEY, cid INTEGER REFERENCES c(id), v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,1,'b1');
INSERT INTO c VALUES(1,1,'c1');
INSERT INTO d VALUES(1,1,'d1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,2,'b2');
INSERT INTO c VALUES(2,2,'c2');
INSERT INTO d VALUES(2,2,'d2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat chain');
SELECT dolt_checkout('main');
UPDATE d SET v='MAIN' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a.v, b.v, c.v, d.v FROM d JOIN c ON d.cid=c.id JOIN b ON c.bid=b.id JOIN a ON b.aid=a.id ORDER BY d.id;"

# ═══════════════════════════════════════════════════════════════════
# Section 101: Checkout commit hash + data visibility
# ═══════════════════════════════════════════════════════════════════
echo "--- branch from historical commit ---"

oracle "branch_from_past_commit_new_work" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_checkout('-b','oldbranch','HEAD~2');
INSERT INTO t VALUES(99,'oldside');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','oldside');
SELECT dolt_checkout('main');
SELECT dolt_merge('oldbranch');
" "SELECT id, v FROM t ORDER BY id;"

oracle "two_branches_from_past" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
SELECT dolt_checkout('-b','past_a','HEAD~1');
INSERT INTO t VALUES(10,'past_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','past_a');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','past_b','HEAD~1');
INSERT INTO t VALUES(20,'past_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','past_b');
SELECT dolt_checkout('main');
SELECT dolt_merge('past_a');
SELECT dolt_merge('past_b');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 102: Conditional UPDATE + cell merge
# ═══════════════════════════════════════════════════════════════════
echo "--- conditional UPDATE + cell merge ---"

oracle "update_coalesce_then_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,NULL,'b0'),(2,'a0',NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a=COALESCE(a,'fallback') WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b=COALESCE(b,'mfallback') WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t ORDER BY id;"

oracle "update_different_cols_disjoint_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);
INSERT INTO t VALUES(1,'x','y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET a='x_feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET b='y_main' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 103: LIMIT/OFFSET after merge
# ═══════════════════════════════════════════════════════════════════
echo "--- LIMIT/OFFSET after merge ---"

oracle "select_limit_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id LIMIT 3;"

oracle "select_limit_offset_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'d'),(5,'e');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id LIMIT 2 OFFSET 2;"

# ═══════════════════════════════════════════════════════════════════
# Section 104: DISTINCT/UNION after merge
# ═══════════════════════════════════════════════════════════════════
echo "--- DISTINCT/UNION after merge ---"

oracle "distinct_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, cat TEXT);
INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'z'),(5,'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT DISTINCT cat FROM t ORDER BY cat;"

oracle "union_all_from_merged" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a1');
INSERT INTO t2 VALUES(1,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t1 VALUES(2,'a2');
INSERT INTO t2 VALUES(2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT v FROM t1 UNION ALL SELECT v FROM t2 ORDER BY v;"

# ═══════════════════════════════════════════════════════════════════
# Section 105: Multi-statement transactions + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- multiple inserts then merge ---"

oracle "many_inserts_same_batch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,10);
INSERT INTO t VALUES(11,11);
INSERT INTO t VALUES(12,12);
INSERT INTO t VALUES(13,13);
INSERT INTO t VALUES(14,14);
INSERT INTO t VALUES(15,15);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat batch');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS n, sum(v) AS s FROM t;"

oracle "many_updates_same_batch_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=10 WHERE id=1;
UPDATE t SET v=20 WHERE id=2;
UPDATE t SET v=30 WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=99 WHERE id=5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 106: Cherry-pick followed by many ops
# ═══════════════════════════════════════════════════════════════════
echo "--- cherry-pick chain ---"

oracle "cherry_pick_two_commits_sequentially" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat1');
INSERT INTO t VALUES(3,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat2');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "cherry_pick_then_reset_then_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
SELECT dolt_reset('--hard','HEAD~1');
SELECT dolt_cherry_pick('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 107: Deep history + cherry-pick
# ═══════════════════════════════════════════════════════════════════
echo "--- deep history + cherry-pick ---"

oracle "cherry_pick_from_deep_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'f1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
INSERT INTO t VALUES(3,'f2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f2');
INSERT INTO t VALUES(4,'f3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f3');
INSERT INTO t VALUES(5,'f4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f4');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat~2');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 108: UNIQUE + merge complex
# ═══════════════════════════════════════════════════════════════════
echo "--- UNIQUE + merge complex ---"

oracle "unique_col_delete_then_reinsert_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE);
INSERT INTO t VALUES(1,'X'),(2,'Y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
INSERT INTO t VALUES(3,'X');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat swap');
SELECT dolt_checkout('main');
UPDATE t SET code='Z' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code FROM t ORDER BY id;"

oracle "multi_unique_cols_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code1 TEXT UNIQUE, code2 TEXT UNIQUE);
INSERT INTO t VALUES(1,'A','X'),(2,'B','Y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'C','Z');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET code2='YY' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, code1, code2 FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 109: NULL ordering in merge
# ═══════════════════════════════════════════════════════════════════
echo "--- NULL handling edge cases ---"

oracle "null_to_value_both_sides_different_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,NULL),(2,NULL),(3,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat_val' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=3;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

oracle "is_null_filter_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,NULL),(4,'d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) AS null_count FROM t WHERE v IS NULL;"

# ═══════════════════════════════════════════════════════════════════
# Section 110: Branch lifecycle + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- branch lifecycle + merge ---"

oracle "create_merge_delete_branch_data_intact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_branch('-d','feat');
INSERT INTO t VALUES(3,'post_delete');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','after delete');
" "SELECT id, v FROM t ORDER BY id;"

oracle "rebranch_after_delete_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
SELECT dolt_branch('-d','feat');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(10,'new_feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','new feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 111: Chained updates on same rows + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- chained updates same row + merge ---"

oracle "many_updates_same_row_feat_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
UPDATE t SET v=v+1 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat +5');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main new row');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 112: HAVING clause + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- HAVING after merge ---"

oracle "having_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, n INTEGER);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(4,'c',100),(5,'a',5);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT grp, sum(n) AS total FROM t GROUP BY grp HAVING sum(n) > 10 ORDER BY grp;"

# ═══════════════════════════════════════════════════════════════════
# Section 113: Mixed column ordering in INSERT + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- explicit column lists + merge ---"

oracle "insert_named_cols_different_order_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t(id,a,b,c) VALUES(1,'a1','b1','c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t(c,a,id,b) VALUES('c2','a2',2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(id,a,b,c) VALUES(3,'a3','b3','c3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, a, b, c FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 114: Revert chain
# ═══════════════════════════════════════════════════════════════════
echo "--- revert chain ---"

oracle "revert_then_revert_the_revert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
UPDATE t SET v='modified' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2 modified');
SELECT dolt_revert('HEAD');
SELECT dolt_revert('HEAD');
" "SELECT id, v FROM t;"

oracle "revert_two_sequential_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c3');
SELECT dolt_revert('HEAD');
SELECT dolt_revert('HEAD');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 115: dolt_log filtering after merge
# ═══════════════════════════════════════════════════════════════════
echo "--- dolt_log after various ops ---"

oracle "log_message_presence_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base_xyz');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_xyz');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_xyz');
SELECT dolt_merge('feat','--no-ff','-m','merge_xyz');
" "SELECT count(*) FROM dolt_log WHERE message LIKE '%_xyz';"

oracle "log_messages_after_cherry_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','m1');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT message FROM dolt_log ORDER BY message;"

# ═══════════════════════════════════════════════════════════════════
# Section 116: CHECK constraint interactions
# ═══════════════════════════════════════════════════════════════════
echo "--- CHECK constraint interactions ---"

oracle "check_constraint_multi_row_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK(n > 0));
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,30),(4,40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET n=n*2 WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, n FROM t ORDER BY id;"

oracle "check_with_not_null_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL CHECK(length(v)>0));
INSERT INTO t VALUES(1,'abc');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'def');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='xyz' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 117: Wide PK patterns
# ═══════════════════════════════════════════════════════════════════
echo "--- wide PK patterns + merge ---"

oracle "varchar_pk_merge" "
CREATE TABLE t(k VARCHAR(16) PRIMARY KEY, v TEXT);
INSERT INTO t VALUES('alpha','a'),('beta','b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES('gamma','g');
UPDATE t SET v='BETA' WHERE k='beta';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('delta','d');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT k, v FROM t ORDER BY k;"

oracle "composite_varchar_pk_merge" "
CREATE TABLE t(a VARCHAR(8), b VARCHAR(8), v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES('x','1','v1'),('x','2','v2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES('y','1','yv1');
UPDATE t SET v='MOD' WHERE a='x' AND b='1';
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('z','1','zv1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat');
" "SELECT a, b, v FROM t ORDER BY a, b;"

# ═══════════════════════════════════════════════════════════════════
# Section 118: Merge yields predictable working state
# ═══════════════════════════════════════════════════════════════════
echo "--- post-merge working state ---"

oracle "post_merge_immediate_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'post_merge');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t ORDER BY id;"

oracle "post_merge_immediate_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
UPDATE t SET v='post' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','post');
" "SELECT id, v FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 119: Many-branch parallel work
# ═══════════════════════════════════════════════════════════════════
echo "--- parallel branches ---"

oracle "four_parallel_branches_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(100,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(1,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(3,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b4');
INSERT INTO t VALUES(4,'b4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b4');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
" "SELECT id, v FROM t ORDER BY id;"

oracle "six_parallel_unique_inserts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base empty','--allow-empty');
SELECT dolt_checkout('-b','b1');
INSERT INTO t VALUES(1,'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b1');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b2');
INSERT INTO t VALUES(2,'b2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b2');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b3');
INSERT INTO t VALUES(3,'b3');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b3');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b4');
INSERT INTO t VALUES(4,'b4');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b4');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b5');
INSERT INTO t VALUES(5,'b5');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b5');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','b6');
INSERT INTO t VALUES(6,'b6');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','b6');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
SELECT dolt_merge('b3');
SELECT dolt_merge('b4');
SELECT dolt_merge('b5');
SELECT dolt_merge('b6');
" "SELECT count(*) AS n, sum(id) AS s FROM t;"

# ═══════════════════════════════════════════════════════════════════
# Section 120: Merge then immediate revert
# ═══════════════════════════════════════════════════════════════════
echo "--- merge then revert ---"

oracle "revert_merge_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main');
SELECT dolt_merge('feat','--no-ff','-m','merge feat');
SELECT dolt_revert('HEAD','-m','1');
" "SELECT id, v FROM t ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 121: FK + delete restriction + merge
# ═══════════════════════════════════════════════════════════════════
echo "--- FK delete restriction + merge ---"

oracle "fk_delete_parent_with_children_merge" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, n TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), v TEXT);
INSERT INTO parent VALUES(1,'p1'),(2,'p2');
INSERT INTO child VALUES(1,2,'c1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM parent WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat delete unreferenced');
SELECT dolt_checkout('main');
INSERT INTO child VALUES(2,2,'c2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main more children');
SELECT dolt_merge('feat');
" "SELECT id, v, pid FROM child ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Results
# ═══════════════════════════════════════════════════════════════════
echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
