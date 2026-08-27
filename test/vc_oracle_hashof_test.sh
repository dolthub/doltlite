#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

setup_pair() {
  local name="$1" setup="$2"
  local dl_db="$TMPROOT/$name.db" dt_repo="$TMPROOT/$name.dolt" dt_setup
  local dl_rc dt_rc
  rm -f "$dl_db"
  mkdir -p "$dt_repo"
  printf "%s\n" "$setup" \
    | "$DOLTLITE" "$dl_db" >/dev/null 2>"$TMPROOT/$name.dl.setup.err"
  dl_rc=$?
  (
    cd "$dt_repo" || exit 1
    vc_oracle_init_repo
    dt_setup=$(vc_oracle_translate_for_dolt "$setup")
    printf "%s\n" "$dt_setup" \
      | "$DOLT" sql -c >/dev/null 2>"$TMPROOT/$name.dt.setup.err"
  )
  dt_rc=$?
  [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ] \
    && ! grep -qiE '(^|[^a-z])(error|failed)([ :]|$)' \
      "$TMPROOT/$name.dl.setup.err" "$TMPROOT/$name.dt.setup.err" 2>/dev/null
}

query_pair() {
  local name="$1" query="$2"
  local dl dt
  dl=$("$DOLTLITE" "$TMPROOT/$name.db" "$query" \
    2>>"$TMPROOT/$name.dl.query.err" | tr -d '\r"')
  dt=$(cd "$TMPROOT/$name.dolt" \
    && "$DOLT" sql -r csv -q "$query" \
      2>>"$TMPROOT/$name.dt.query.err" | tail -n +2 | tr -d '\r"')
  printf '%s|%s\n' "$dl" "$dt"
}

query_pair_separate() {
  local name="$1" dl_query="$2" dt_query="$3"
  local dl dt
  dl=$("$DOLTLITE" "$TMPROOT/$name.db" "$dl_query" \
    2>>"$TMPROOT/$name.dl.query.err" | tr -d '\r"')
  dt=$(cd "$TMPROOT/$name.dolt" \
    && "$DOLT" sql -r csv -q "$dt_query" \
      2>>"$TMPROOT/$name.dt.query.err" | tail -n +2 | tr -d '\r"')
  printf '%s|%s\n' "$dl" "$dt"
}

run_hash() {
  local name="$1" setup="$2" query="$3"
  if ! setup_pair "$name" "$setup"; then
    printf '|\n'
    return
  fi
  query_pair "$name" "$query"
}

run_hash_on() {
  query_pair "$1" "$2"
}

exec_pair() {
  local name="$1" sql="$2" dt_sql
  printf '%s\n' "$sql" | "$DOLTLITE" "$TMPROOT/$name.db" \
    >/dev/null 2>>"$TMPROOT/$name.dl.exec.err"
  dt_sql=$(vc_oracle_translate_for_dolt "$sql")
  (cd "$TMPROOT/$name.dolt" && printf '%s\n' "$dt_sql" | "$DOLT" sql -c) \
    >/dev/null 2>>"$TMPROOT/$name.dt.exec.err"
}

pair_dl() { printf '%s\n' "${1%%|*}"; }
pair_dt() { printf '%s\n' "${1#*|}"; }

fail_pair() {
  local name="$1" detail="$2" a="$3" b="${4:-}"
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES $name"
  echo "  FAIL: $name ($detail)"
  echo "    doltlite: $(pair_dl "$a") ${b:+vs $(pair_dl "$b")}"
  echo "    dolt:     $(pair_dt "$a") ${b:+vs $(pair_dt "$b")}"
}

same() {
  local name="$1" a="$2" b="$3" ad bd at bt
  ad=$(pair_dl "$a"); bd=$(pair_dl "$b")
  at=$(pair_dt "$a"); bt=$(pair_dt "$b")
  if [ -z "$ad" ] || [ -z "$bd" ] || [ -z "$at" ] || [ -z "$bt" ]; then
    fail_pair "$name" "empty hash" "$a" "$b"
    return
  fi
  if [ "$ad" = "$bd" ] && [ "$at" = "$bt" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail_pair "$name" "expected equality within each engine" "$a" "$b"
  fi
}

different() {
  local name="$1" a="$2" b="$3" ad bd at bt
  ad=$(pair_dl "$a"); bd=$(pair_dl "$b")
  at=$(pair_dt "$a"); bt=$(pair_dt "$b")
  if [ -z "$ad" ] || [ -z "$bd" ] || [ -z "$at" ] || [ -z "$bt" ]; then
    fail_pair "$name" "empty hash" "$a" "$b"
    return
  fi
  if [ "$ad" != "$bd" ] && [ "$at" != "$bt" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail_pair "$name" "expected inequality within each engine" "$a" "$b"
  fi
}

both_error() {
  local name="$1" db="$2" query="$3"
  local dl_rc dt_rc
  "$DOLTLITE" "$TMPROOT/$db.db" "$query" \
    >/dev/null 2>"$TMPROOT/$name.dl.err"
  dl_rc=$?
  (cd "$TMPROOT/$db.dolt" && "$DOLT" sql -q "$query") \
    >/dev/null 2>"$TMPROOT/$name.dt.err"
  dt_rc=$?
  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both engines to reject the query)"
    echo "    doltlite exit: $dl_rc"
    echo "    dolt exit:     $dt_rc"
  fi
}

shape() {
  local name="$1" h="$2" dl dt
  dl=$(pair_dl "$h"); dt=$(pair_dt "$h")
  if echo "$dl" | grep -qE '^[0-9a-f]{40}$' \
     && echo "$dt" | grep -qE '^[0-9a-v]{32}$'; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail_pair "$name" "unexpected engine-specific hash shape" "$h"
  fi
}

echo "=== Version Control Oracle Tests: dolt_hashof suite ==="
echo ""

echo "--- 1. Determinism: same input → same hash ---"

SEED_A="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
"

H1=$(run_hash "det1" "$SEED_A" "SELECT dolt_hashof_table('t');")
H2=$(run_hash "det2" "$SEED_A" "SELECT dolt_hashof_table('t');")
same "determinism_table_same_inputs" "$H1" "$H2"

H1=$(run_hash "det_db1" "$SEED_A" "SELECT dolt_hashof_db();")
H2=$(run_hash "det_db2" "$SEED_A" "SELECT dolt_hashof_db();")
same "determinism_db_same_inputs" "$H1" "$H2"

echo ""

echo "--- 2. Insert order invariance ---"

ORDER_ABC="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
INSERT INTO t VALUES (2, 'b');
INSERT INTO t VALUES (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'abc');
"

ORDER_CBA="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (3, 'c');
INSERT INTO t VALUES (2, 'b');
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'cba');
"

ORDER_BATCH="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (2, 'b'), (1, 'a'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'batch');
"

H_ABC=$(run_hash "order_abc" "$ORDER_ABC" "SELECT dolt_hashof_table('t');")
H_CBA=$(run_hash "order_cba" "$ORDER_CBA" "SELECT dolt_hashof_table('t');")
H_BAT=$(run_hash "order_batch" "$ORDER_BATCH" "SELECT dolt_hashof_table('t');")
same "order_abc_vs_cba"    "$H_ABC" "$H_CBA"
same "order_abc_vs_batch"  "$H_ABC" "$H_BAT"

echo ""

echo "--- 3. Intermediate state invariance (delete+reinsert is a no-op) ---"

NET="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'net');
"

THROUGH_DELETE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (99, 'temp');
DELETE FROM t WHERE id = 99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'through_delete');
"

DELETE_REINSERT="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'delete_reinsert');
"

H_NET=$(run_hash "net" "$NET" "SELECT dolt_hashof_table('t');")
H_TD=$(run_hash "through_delete" "$THROUGH_DELETE" "SELECT dolt_hashof_table('t');")
H_DR=$(run_hash "delete_reinsert" "$DELETE_REINSERT" "SELECT dolt_hashof_table('t');")
same "net_vs_through_delete"  "$H_NET" "$H_TD"
same "net_vs_delete_reinsert" "$H_NET" "$H_DR"

echo ""

echo "--- 3b. Oversized singleton leaf vs one-shot insert ---"

SMALL_TWO="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b');
"

SMALL_ONE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1), (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'both');
"

H_SMALL_TWO=$(run_hash "small_two" "$SMALL_TWO" "SELECT dolt_hashof_table('t');")
H_SMALL_ONE=$(run_hash "small_one" "$SMALL_ONE" "SELECT dolt_hashof_table('t');")
same "small_int_two_commit_vs_one" "$H_SMALL_ONE" "$H_SMALL_TWO"

FAT_TWO="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t
WITH RECURSIVE s(n, x) AS (
  SELECT 1, 'x'
  UNION ALL
  SELECT n+1, x||x FROM s WHERE n<14
)
SELECT 1, x FROM s WHERE n=14;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'fat');
INSERT INTO t VALUES (2, 'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'small');
"

FAT_ONE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t
WITH RECURSIVE s(n, x) AS (
  SELECT 1, 'x'
  UNION ALL
  SELECT n+1, x||x FROM s WHERE n<14
)
SELECT 1, x FROM s WHERE n=14;
INSERT INTO t VALUES (2, 'y');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'both');
"

H_FAT_TWO=$(run_hash "fat_two" "$FAT_TWO" "SELECT dolt_hashof_table('t');")
H_FAT_ONE=$(run_hash "fat_one" "$FAT_ONE" "SELECT dolt_hashof_table('t');")
same "fat_text_8192_two_commit_vs_one" "$H_FAT_ONE" "$H_FAT_TWO"

echo ""

echo "--- 4. Cross-branch state invariance ---"

BRANCH_CONVERGE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');

SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c2');

SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c2');
"

DB="cross_branch"
setup_pair "$DB" "$BRANCH_CONVERGE"

H_MAIN_T=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
exec_pair "$DB" "SELECT dolt_checkout('feat');"
H_FEAT_T=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
same "cross_branch_table_hash_equal" "$H_MAIN_T" "$H_FEAT_T"

H_MAIN_COMMIT=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")
H_FEAT_COMMIT=$(run_hash_on "$DB" "SELECT dolt_hashof('feat');")
different "cross_branch_commit_hash_differs" "$H_MAIN_COMMIT" "$H_FEAT_COMMIT"

echo ""

echo "--- 5. Per-table isolation in dolt_hashof_table ---"

TWO_TABLES="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t VALUES (1, 'a');
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'two_tables');
"

DB="two_tables"
setup_pair "$DB" "$TWO_TABLES"

HT_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
HU_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_table('u');")
HDB_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")

exec_pair "$DB" "INSERT INTO u VALUES (2, 'y'); SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'bump_u');"

HT_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
HU_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_table('u');")
HDB_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")

same      "t_hash_stable_on_u_mutation"   "$HT_BEFORE" "$HT_AFTER"
different "u_hash_changes_on_u_mutation"  "$HU_BEFORE" "$HU_AFTER"
different "db_hash_changes_on_u_mutation" "$HDB_BEFORE" "$HDB_AFTER"

echo ""

echo "--- 6. Reopen stability ---"

REOPEN_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'reopen');
"

DB="reopen"
setup_pair "$DB" "$REOPEN_SEED"

H_OPEN1=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_OPEN2=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_OPEN3_DB=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_OPEN4_MAIN=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")
H_OPEN5_TAB=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_OPEN6_DB=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_OPEN7_MAIN=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")

same "reopen_table_stable" "$H_OPEN1" "$H_OPEN5_TAB"
same "reopen_db_stable"    "$H_OPEN3_DB" "$H_OPEN6_DB"
same "reopen_commit_stable" "$H_OPEN4_MAIN" "$H_OPEN7_MAIN"

echo ""

echo "--- 7. Mutations must invalidate ---"

BASE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
"

ADD_ROW="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_row');
"

UPDATE_CELL="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
UPDATE t SET v = 'A' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'update');
"

ALTER_SCHEMA="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
INSERT INTO t VALUES (1, 'a', NULL), (2, 'b', NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'alter');
"

H_BASE=$(run_hash "neg_base" "$BASE" "SELECT dolt_hashof_table('t');")
H_ADD=$(run_hash "neg_add" "$ADD_ROW" "SELECT dolt_hashof_table('t');")
H_UPD=$(run_hash "neg_upd" "$UPDATE_CELL" "SELECT dolt_hashof_table('t');")
H_ALT=$(run_hash "neg_alt" "$ALTER_SCHEMA" "SELECT dolt_hashof_table('t');")

different "add_row_changes_table_hash"   "$H_BASE" "$H_ADD"
different "update_cell_changes_table_hash" "$H_BASE" "$H_UPD"
different "alter_schema_changes_table_hash" "$H_BASE" "$H_ALT"

echo ""

echo "--- 7b. Equivalent DDL histories converge ---"

DDL_DIRECT="
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), n INTEGER, extra TEXT DEFAULT 'seed');
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
INSERT INTO t VALUES (1, 'one', 10, 'seed'), (2, 'two', 20, 'seed');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'direct');
"

DDL_ALTER="
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), n INTEGER);
INSERT INTO t VALUES (1, 'one', 10), (2, 'two', 20);
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'seed';
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
UPDATE t SET extra = 'seed';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'alter');
"

DDL_RECREATE="
CREATE TABLE t_old(id INTEGER PRIMARY KEY, v VARCHAR(64), n INTEGER);
INSERT INTO t_old VALUES (1, 'one', 10), (2, 'two', 20);
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), n INTEGER, extra TEXT DEFAULT 'seed');
INSERT INTO t SELECT id, v, n, 'seed' FROM t_old;
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_n ON t(n);
DROP TABLE t_old;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'recreate');
"

H_DDL_DIRECT=$(run_hash "ddl_direct" "$DDL_DIRECT" "SELECT dolt_hashof_table('t');")
H_DDL_ALTER=$(run_hash "ddl_alter" "$DDL_ALTER" "SELECT dolt_hashof_table('t');")
H_DDL_RECREATE=$(run_hash "ddl_recreate" "$DDL_RECREATE" "SELECT dolt_hashof_table('t');")
same "ddl_direct_vs_alter_table_hash" "$H_DDL_DIRECT" "$H_DDL_ALTER"
same "ddl_direct_vs_recreate_table_hash" "$H_DDL_DIRECT" "$H_DDL_RECREATE"

echo ""

echo "--- 8. Working, staged, and committed roots ---"

WORKING_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
"

DB="working_roots"
setup_pair "$DB" "$WORKING_SEED"
H_T_COMMITTED=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_DB_COMMITTED=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_HEAD_COMMITTED=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD');")

exec_pair "$DB" "INSERT INTO t VALUES (2, 'b');"
H_T_WORKING=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_DB_WORKING=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_DB_NAMED_WORKING=$(run_hash_on "$DB" "SELECT dolt_hashof_db('WORKING');")
H_DB_EMPTY=$(run_hash_on "$DB" "SELECT dolt_hashof_db('');")
H_DB_STAGED_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_db('STAGED');")
H_DB_HEAD_NAMED=$(run_hash_on "$DB" "SELECT dolt_hashof_db('HEAD');")
H_HEAD_WORKING=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD');")
different "working_change_updates_table_hash" "$H_T_COMMITTED" "$H_T_WORKING"
different "working_change_updates_db_hash" "$H_DB_COMMITTED" "$H_DB_WORKING"
same "working_change_does_not_move_HEAD" "$H_HEAD_COMMITTED" "$H_HEAD_WORKING"
same "named_working_equals_noarg_db" "$H_DB_WORKING" "$H_DB_NAMED_WORKING"
same "empty_ref_equals_working_db" "$H_DB_EMPTY" "$H_DB_NAMED_WORKING"
same "staged_equals_head_before_add" "$H_DB_STAGED_BEFORE" "$H_DB_HEAD_NAMED"
different "named_working_differs_from_head_when_dirty" "$H_DB_NAMED_WORKING" "$H_DB_HEAD_NAMED"
both_error "hashof_working_is_commit_only" "$DB" "SELECT dolt_hashof('WORKING');"
both_error "hashof_staged_is_commit_only" "$DB" "SELECT dolt_hashof('STAGED');"

exec_pair "$DB" "SELECT dolt_add('-A');"
H_T_STAGED=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_DB_STAGED=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_DB_STAGED_NAMED=$(run_hash_on "$DB" "SELECT dolt_hashof_db('STAGED');")
H_DB_WORKING_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_db('WORKING');")
H_DB_HEAD_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_db('HEAD');")
H_HEAD_STAGED=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD');")
same "staging_keeps_working_table_hash" "$H_T_WORKING" "$H_T_STAGED"
same "staging_keeps_working_db_hash" "$H_DB_WORKING" "$H_DB_STAGED"
same "staging_does_not_move_HEAD" "$H_HEAD_COMMITTED" "$H_HEAD_STAGED"
same "named_staged_equals_working_after_add" "$H_DB_STAGED_NAMED" "$H_DB_WORKING_AFTER"
different "named_staged_differs_from_head_after_add" "$H_DB_STAGED_NAMED" "$H_DB_HEAD_AFTER"

echo ""

echo "--- 9. Ref resolution ---"

REF_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

DB="refs"
setup_pair "$DB" "$REF_SEED"

H_MAIN=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")
H_HEAD=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD');")
same "main_equals_HEAD" "$H_MAIN" "$H_HEAD"
shape "main_hash_shape" "$H_MAIN"

H_HEAD_PARENT=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD~1');")
different "HEAD_differs_from_HEAD_parent" "$H_HEAD" "$H_HEAD_PARENT"

both_error "oversized_parent_number_is_rejected" "$DB" \
  "SELECT dolt_hashof('HEAD^4294967297');"

H_ID_OUT=$(query_pair_separate "$DB" \
  "SELECT dolt_hashof('$(pair_dl "$H_HEAD")');" \
  "SELECT dolt_hashof('$(pair_dt "$H_HEAD")');")
same "commit_hash_is_identity_on_hashof" "$H_HEAD" "$H_ID_OUT"

COLLIDE_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'later');
SELECT dolt_checkout('main');
SELECT dolt_tag('feat');
"

DB="tag_branch_collide"
setup_pair "$DB" "$COLLIDE_SEED"
H_TAG=$(run_hash_on "$DB" "SELECT tag_hash FROM dolt_tags WHERE tag_name='feat';")
H_BRANCH=$(run_hash_on "$DB" "SELECT hash FROM dolt_branches WHERE name='feat';")
H_NAME=$(run_hash_on "$DB" "SELECT dolt_hashof('feat');")
same "hashof_colliding_name_equals_branch" "$H_BRANCH" "$H_NAME"
different "hashof_colliding_name_differs_from_tag" "$H_TAG" "$H_NAME"
both_error "checkout_colliding_name_is_tag" "$DB" "SELECT dolt_checkout('feat');"

echo ""

echo "--- 10. Engine-specific shape conformance ---"

SHAPE_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'shape');
"

H_T=$(run_hash "shape_t" "$SHAPE_SEED" "SELECT dolt_hashof_table('t');")
H_D=$(run_hash "shape_d" "$SHAPE_SEED" "SELECT dolt_hashof_db();")
H_R=$(run_hash "shape_r" "$SHAPE_SEED" "SELECT dolt_hashof('main');")
shape "hashof_table_shape" "$H_T"
shape "hashof_db_shape"    "$H_D"
shape "hashof_ref_shape"   "$H_R"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
