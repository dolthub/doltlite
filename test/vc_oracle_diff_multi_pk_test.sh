#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# doltlite's dolt_diff_<tbl>(from, to) is a ranged, per-commit-attributed diff;
# Dolt's equivalent is the dolt_diff_<tbl> system table filtered to the commits
# in the from..to range. dolt_diff_stat/summary stay net-range functions and are
# shielded from the row-diff rewrite.
translate_for_dolt() {
  sed -E "
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_(stat|summary)([^a-zA-Z0-9_])/@@DOLT_DIFF_\1@@\2/g
    s/dolt_diff_([a-zA-Z0-9_]+)\('([^']*)', *'WORKING'\)/dolt_diff_\1 WHERE to_commit = 'WORKING' OR to_commit IN (SELECT commit_hash FROM dolt_log('\2..HEAD'))/g
    s/dolt_diff_([a-zA-Z0-9_]+)\('([^']*)', *'([^']*)'\)/dolt_diff_\1 WHERE to_commit IN (SELECT commit_hash FROM dolt_log('\2..\3'))/g
    s/@@DOLT_DIFF_(stat|summary)@@/dolt_diff_\1/g
  "
}

oracle() {
  local name="$1" setup="$2" query="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

echo "=== Version Control Oracle Tests: multi-col PK diff ==="
echo ""

SETUP_A="
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one'), (1, 2, 'two'), (2, 1, 'three');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'TWO' WHERE a = 1 AND b = 2;
INSERT INTO t VALUES (3, 3, 'four');
DELETE FROM t WHERE a = 1 AND b = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

echo "--- Group A: two-col INT PK ---"

oracle "a_history" "$SETUP_A" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t;"

oracle "a_slice_one" "$SETUP_A" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "a_slice_full" "$SETUP_A" \
  "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~2', 'HEAD');"

oracle "a_summary" "$SETUP_A" \
  "SELECT CONCAT('R|', dd.table_name, '|', coalesce(dl.message, dd.commit_hash), '|', dd.data_change, '|', dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash WHERE dd.table_name = 't';"

echo "--- Group B: PK-column UPDATE ---"

oracle "b_pk_update" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET b = 99 WHERE a = 1 AND b = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'move_pk');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "b_pk_update_history" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET a = 99 WHERE a = 1 AND b = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'move_pk');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t;"

echo "--- Group C: PK cols not at the front of the declaration ---"

oracle "c_pk_after_nonpk" "
CREATE TABLE t(v TEXT, a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('one', 1, 1), ('two', 1, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES ('three', 2, 1);
UPDATE t SET v = 'TWO' WHERE a = 1 AND b = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "c_pk_reversed" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(b, a));
INSERT INTO t VALUES (1, 10, 'one'), (2, 20, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'TWO' WHERE a = 2 AND b = 20;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

echo "--- Group D: three-col PK ---"

oracle "d_three_col_pk" "
CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER, v TEXT, PRIMARY KEY(a, b, c));
INSERT INTO t VALUES (1, 1, 10, 'alpha'), (1, 1, 20, 'beta'), (1, 2, 10, 'gamma');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'BETA' WHERE a = 1 AND b = 1 AND c = 20;
INSERT INTO t VALUES (2, 2, 20, 'delta');
DELETE FROM t WHERE a = 1 AND b = 2 AND c = 10;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_c,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_c,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "d_three_col_history" "
CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER, v TEXT, PRIMARY KEY(a, b, c));
INSERT INTO t VALUES (1, 1, 10, 'alpha'), (1, 1, 20, 'beta');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'ALPHA' WHERE a = 1 AND b = 1 AND c = 10;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_c,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_c,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t;"

echo "--- Group E: mixed-type PKs ---"

oracle "e_text_int_pk" "
CREATE TABLE t(name VARCHAR(20), id INTEGER, v TEXT, PRIMARY KEY(name, id));
INSERT INTO t VALUES ('alice', 1, 'a'), ('alice', 2, 'b'), ('bob', 1, 'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'B' WHERE name = 'alice' AND id = 2;
INSERT INTO t VALUES ('carol', 1, 'd');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_name,''), '|', IFNULL(to_id,''), '|', IFNULL(to_v,''), '|', IFNULL(from_name,''), '|', IFNULL(from_id,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

oracle "e_int_text_pk" "
CREATE TABLE t(id INTEGER, tag VARCHAR(20), v TEXT, PRIMARY KEY(id, tag));
INSERT INTO t VALUES (1, 'x', 'foo'), (1, 'y', 'bar');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'BAR' WHERE id = 1 AND tag = 'y';
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_id,''), '|', IFNULL(to_tag,''), '|', IFNULL(to_v,''), '|', IFNULL(from_id,''), '|', IFNULL(from_tag,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

echo "--- Group F: dolt_diff_stat on multi-col PK ---"

oracle "f_stat_counts" "
CREATE TABLE t(a INTEGER, b INTEGER, v1 TEXT, v2 TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one', 'I'), (1, 2, 'two', 'II');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v1 = 'TWO', v2 = 'II-prime' WHERE a = 1 AND b = 2;
INSERT INTO t VALUES (2, 1, 'three', 'III');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', table_name, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified) FROM dolt_diff_stat('HEAD~1', 'HEAD', 't');"

oracle "f_stat_pk_update" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one'), (1, 2, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET b = 99 WHERE a = 1 AND b = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'pk_update');
" "SELECT CONCAT('R|', table_name, '|', rows_added, '|', rows_deleted, '|', rows_modified) FROM dolt_diff_stat('HEAD~1', 'HEAD', 't');"

echo "--- Group G: dolt_blame on multi-col PK ---"

oracle "g_blame_multi_pk" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES (1, 1, 'one'), (1, 2, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = 'TWO' WHERE a = 1 AND b = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (2, 1, 'three');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c3');
" "SELECT CONCAT('R|', a, '|', b, '|', message) FROM dolt_blame_t ORDER BY a, b;"

echo "--- Group H: ALTER on non-leading-PK table (schema-only filter) ---"

oracle "h_drop_middle_nonpk_nonleading_pk" "
CREATE TABLE t(v INT, c INT, a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t(v, a, b) VALUES (10, 1, 2), (20, 1, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
ALTER TABLE t DROP COLUMN c;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'drop_c');
" "SELECT CONCAT('R|', IFNULL(to_v,''), '|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(from_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');" \
  "EXPECT_EMPTY"

oracle "h_add_col_nonleading_pk_no_data" "
CREATE TABLE t(v INT, a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t(v, a, b) VALUES (10, 1, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'add_extra');
" "SELECT CONCAT('R|', IFNULL(to_v,''), '|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_extra,''), '|', IFNULL(from_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');" \
  "EXPECT_EMPTY"

echo "--- Group I: replay on multi-col PK table additions ---"

oracle "i_merge_replay_multi_pk_add_table" "
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
" "SELECT CONCAT('R|', to_a, '|', to_b, '|', to_v, '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_u('HEAD^1', 'HEAD') ORDER BY to_a, to_b;"

oracle "i_cherrypick_replay_multi_pk_add_table" "
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
" "SELECT CONCAT('R|', to_a, '|', to_b, '|', to_v, '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_u('HEAD~1', 'HEAD') ORDER BY to_a, to_b;"

oracle "i_rebase_replay_multi_pk_add_table" "
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
" "SELECT CONCAT('R|', to_a, '|', to_b, '|', to_v, '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_u('main', 'feat') ORDER BY to_a, to_b;"

echo "--- Group J: replay on multi-col PK stat/summary ---"

oracle "j_merge_replay_multi_pk_stat" "
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
" "SELECT CONCAT('R|', table_name, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified) FROM dolt_diff_stat('HEAD^1', 'HEAD', 'u');"

oracle "j_merge_replay_multi_pk_summary" "
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
" "SELECT CONCAT('R|', IFNULL(from_table_name,''), '|', IFNULL(to_table_name,''), '|', diff_type, '|', CASE WHEN data_change THEN 1 ELSE 0 END, '|', CASE WHEN schema_change THEN 1 ELSE 0 END) FROM dolt_diff_summary('HEAD^1', 'HEAD', 'u');"

oracle "j_cherrypick_replay_multi_pk_stat" "
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
" "SELECT CONCAT('R|', table_name, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified) FROM dolt_diff_stat('HEAD~1', 'HEAD', 'u');"

oracle "j_cherrypick_replay_multi_pk_summary" "
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
" "SELECT CONCAT('R|', IFNULL(from_table_name,''), '|', IFNULL(to_table_name,''), '|', diff_type, '|', CASE WHEN data_change THEN 1 ELSE 0 END, '|', CASE WHEN schema_change THEN 1 ELSE 0 END) FROM dolt_diff_summary('HEAD~1', 'HEAD', 'u');"

oracle "j_rebase_replay_multi_pk_stat" "
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
" "SELECT CONCAT('R|', table_name, '|', rows_added, '|', rows_deleted, '|', rows_modified, '|', cells_added, '|', cells_deleted, '|', cells_modified) FROM dolt_diff_stat('main', 'feat', 'u');"

oracle "j_rebase_replay_multi_pk_summary" "
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
" "SELECT CONCAT('R|', IFNULL(from_table_name,''), '|', IFNULL(to_table_name,''), '|', diff_type, '|', CASE WHEN data_change THEN 1 ELSE 0 END, '|', CASE WHEN schema_change THEN 1 ELSE 0 END) FROM dolt_diff_summary('main', 'feat', 'u');"

echo "--- Group K: replay history on multi-col PK table additions ---"

oracle "k_merge_replay_multi_pk_history" "
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
" "SELECT CONCAT('R|', a, '|', b, '|', v, '|', message) FROM dolt_history_u ORDER BY a, b, message;" "EXPECT_EMPTY"

oracle "k_cherrypick_replay_multi_pk_history" "
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
" "SELECT CONCAT('R|', a, '|', b, '|', v, '|', message) FROM dolt_history_u ORDER BY a, b, message;" "EXPECT_EMPTY"

oracle "k_rebase_replay_multi_pk_history" "
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
" "SELECT CONCAT('R|', a, '|', b, '|', v, '|', message) FROM dolt_history_u ORDER BY a, b, message;" "EXPECT_EMPTY"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
