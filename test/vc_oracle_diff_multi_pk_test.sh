#!/bin/bash
#
# Version-control oracle tests: diff surfaces on multi-column PKs
#
# Depth suite covering every dolt_diff* surface against tables with
# compound primary keys, mixed-type keys, keys that are not the
# leading declared columns, and keys that are reordered relative to
# declaration. These are the shapes that previously surfaced a
# rowid-alias detection bug; this harness makes sure no surface
# regresses silently.
#
# Oracle target: real Dolt 1.83.5. For each scenario the harness
# runs identical setup SQL on both engines and compares tagged
# output lines. Commit metadata is excluded from comparisons (hashes
# and timestamps differ by engine); PK values, non-PK values,
# diff_type, and row/cell counts are the comparison keys.
#
# Usage: bash vc_oracle_diff_multi_pk_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# doltlite uses `SELECT dolt_*()` for procedure-style calls and
# `dolt_diff_<t>(...)` for the per-table slice TVF. Dolt uses `CALL
# dolt_*()` and `dolt_diff(..., '<t>')`. Translate only the per-table
# slice form, not dolt_diff_stat / dolt_diff_summary which share the
# dolt_diff_ prefix but are commit-range TVFs with identical signatures
# on both engines.
translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_(stat|summary)([^a-zA-Z0-9_])/@@DOLT_DIFF_\1@@\2/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
    s/@@DOLT_DIFF_(stat|summary)@@/dolt_diff_\1/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3"
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
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

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

echo "=== Version Control Oracle Tests: multi-col PK diff ==="
echo ""

# ---------------------------------------------------------------
# Group A: Two-column INT PK — insert / update-nonpk / delete.
# Hits dolt_diff_<t> (full history), dolt_diff_<t>(from,to) slice
# TVF, and dolt_diff summary.
# ---------------------------------------------------------------

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

# Summary: one row per (commit, table). Compare table_name + change flags joined on message.
oracle "a_summary" "$SETUP_A" \
  "SELECT CONCAT('R|', dd.table_name, '|', coalesce(dl.message, dd.commit_hash), '|', dd.data_change, '|', dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash WHERE dd.table_name = 't';"

# ---------------------------------------------------------------
# Group B: Two-column PK with PK-column UPDATE (= delete + add).
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# Group C: PK columns not in the first declared positions.
# PRAGMA table_info order differs from the PK positions, which
# has historically broken both projection and sortkey encoding.
# ---------------------------------------------------------------

echo "--- Group C: PK cols not at the front of the declaration ---"

oracle "c_pk_after_nonpk" "
CREATE TABLE t(v TEXT, a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t VALUES ('one', 1, 1), ('two', 1, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES ('three', 2, 1);
UPDATE t SET v = 'TWO' WHERE a = 1 AND b = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

# PK declared in REVERSE order vs physical column position.
oracle "c_pk_reversed" "
CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(b, a));
INSERT INTO t VALUES (1, 10, 'one'), (2, 20, 'two');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
UPDATE t SET v = 'TWO' WHERE a = 2 AND b = 20;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
" "SELECT CONCAT('R|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', IFNULL(from_v,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

# ---------------------------------------------------------------
# Group D: Three-column compound PK.
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# Group E: Mixed-type PKs. VARCHAR in PK is accepted by both
# engines (Dolt needs an explicit length; SQLite is flexible).
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# Group F: dolt_diff_stat row/cell counts on multi-col PK tables.
# If PK column projection is broken, stat counts can still be
# right (stat counts rows, not columns), but cell math depends on
# the correct column total for the table.
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# Group G: dolt_blame_<t> on a compound-PK table. Blame projects
# the PK columns separately from the record payload, so it
# exercises the same rowid-alias detection path.
# ---------------------------------------------------------------

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

# ---------------------------------------------------------------
# Group H: ALTER TABLE on a table with PK cols not leading the
# declaration. Exercises the schema-only filter in
# changeIsSchemaOnly(): MODIFY rows emitted by the prolly diff
# when two commits have different schemas must be compared by
# the actual record-field layout (PK-first in WITHOUT ROWID),
# not by the declared-column index. Dropping a middle non-PK
# column whose only value was NULL should be schema-only on
# both engines, meaning dolt_diff_t('HEAD~1','HEAD') emits 0
# rows for the surviving row.
# ---------------------------------------------------------------

echo "--- Group H: ALTER on non-leading-PK table (schema-only filter) ---"

oracle "h_drop_middle_nonpk_nonleading_pk" "
CREATE TABLE t(v INT, c INT, a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t(v, a, b) VALUES (10, 1, 2), (20, 1, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
ALTER TABLE t DROP COLUMN c;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'drop_c');
" "SELECT CONCAT('R|', IFNULL(to_v,''), '|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(from_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

# ADD COLUMN on a non-leading-PK table. Schema-only change; no
# rows should appear in the per-table diff slice. Same shape as
# the PK-leading test in vc_oracle_diff_test.sh, but with v in
# front of the PK cols so the record layout no longer matches
# the declared layout.
oracle "h_add_col_nonleading_pk_no_data" "
CREATE TABLE t(v INT, a INTEGER, b INTEGER, PRIMARY KEY(a, b));
INSERT INTO t(v, a, b) VALUES (10, 1, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
ALTER TABLE t ADD COLUMN extra INT;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'add_extra');
" "SELECT CONCAT('R|', IFNULL(to_v,''), '|', IFNULL(to_a,''), '|', IFNULL(to_b,''), '|', IFNULL(to_extra,''), '|', IFNULL(from_v,''), '|', IFNULL(from_a,''), '|', IFNULL(from_b,''), '|', diff_type) FROM dolt_diff_t('HEAD~1', 'HEAD');"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
