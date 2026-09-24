#!/bin/bash
# Two-sided ADD COLUMN of different names plus an index on one new column
# used to load as SQLITE_CORRUPT. Refuse with cannot merge instead.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite merge dual ADD COLUMN + index ==="
echo ""

DB=/tmp/test_merge_dual_add_idx_$$.db
rm -f "$DB"

SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);
INSERT INTO t VALUES(1, 'p');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
ALTER TABLE t ADD COLUMN xcol_a TEXT DEFAULT 'da';
CREATE INDEX ix ON t(xcol_a);
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN xcol_b TEXT;
SELECT dolt_commit('-Am', 'main');
"

if dltest_require "dual_add_setup" "$DB" "$SETUP"; then
  run_test_match "dual_add_index_refuses" \
    "SELECT dolt_merge('feat');" \
    "cannot merge: index 'ix' covers column 'xcol_a'" "$DB"
  run_test "dual_add_index_integrity" \
    "PRAGMA integrity_check;" \
    "ok" "$DB"
  run_test "dual_add_index_table_unmerged" \
    "SELECT sql FROM sqlite_schema WHERE name='t';" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, xcol_b TEXT)" "$DB"
fi

# Two-sided ADD without an index still merges both columns.
DB2=/tmp/test_merge_dual_add_noidx_$$.db
rm -f "$DB2"
SETUP2="
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);
INSERT INTO t VALUES(1, 'p');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
ALTER TABLE t ADD COLUMN xcol_a TEXT DEFAULT 'da';
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN xcol_b TEXT;
SELECT dolt_commit('-Am', 'main');
"
if dltest_require "dual_add_noidx_setup" "$DB2" "$SETUP2"; then
  run_test_match "dual_add_noidx_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB2"
  run_test "dual_add_noidx_sql" \
    "SELECT sql FROM sqlite_schema WHERE name='t';" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, xcol_b TEXT, xcol_a TEXT DEFAULT 'da')" "$DB2"
fi

# One side adds an index, the other adds a different index and renames an
# unrelated column. Schema init used to reject the assembled catalog as
# malformed. The merge now stops as a schema conflict and leaves main intact.
DB3=/tmp/test_merge_rename_plus_idx_$$.db
rm -f "$DB3"
SETUP3="
CREATE TABLE t_flex(
  id INTEGER PRIMARY KEY, a INTEGER, r REAL, num NUMERIC, u, trail TEXT
);
CREATE INDEX t_flex_trail ON t_flex(trail);
CREATE INDEX t_flex_r ON t_flex(r);
CREATE UNIQUE INDEX t_flex_expr ON t_flex(length(coalesce(trail, '')));
CREATE UNIQUE INDEX t_flex_partial ON t_flex(a) WHERE a IS NOT NULL;
INSERT INTO t_flex VALUES(0, 1, 1.5, 1, 1, 'base');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'side');
CREATE UNIQUE INDEX flex_pu_side ON t_flex(a) WHERE a IS NOT NULL;
ALTER TABLE t_flex RENAME COLUMN r TO flex_100;
SELECT dolt_commit('-Am', 'side');
SELECT dolt_checkout('main');
CREATE UNIQUE INDEX flex_pu_main ON t_flex(trail) WHERE trail IS NOT NULL;
SELECT dolt_commit('-Am', 'main');
"
if dltest_require "rename_plus_idx_setup" "$DB3" "$SETUP3"; then
  run_test_match "rename_plus_idx_conflict" \
    "SELECT dolt_merge('--squash','side');" \
    "cannot merge: conflicts detected" "$DB3"
  run_test "rename_plus_idx_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB3"
  run_test "rename_plus_idx_unmerged" \
    "SELECT sql FROM sqlite_schema WHERE name='t_flex_r';" \
    "CREATE INDEX t_flex_r ON t_flex(r)" "$DB3"
fi

# Branches created from the empty init commit both build sqlite_master.
# A root mismatch used to return a bare "merge failed". Each branch is
# opened on its own so the empty catalog is what that branch committed.
DB4=/tmp/test_merge_empty_anc_same_$$.db
rm -f "$DB4"
if dltest_require "empty_anc_same_branches" "$DB4" \
    "SELECT dolt_branch('left'); SELECT dolt_branch('right');" \
 && dltest_require "empty_anc_same_left" "$DB4/left" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
     INSERT INTO t VALUES(1, 'L');
     SELECT dolt_commit('-Am', 'left');" \
 && dltest_require "empty_anc_same_right" "$DB4/right" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
     INSERT INTO t VALUES(2, 'R');
     SELECT dolt_commit('-Am', 'right');"; then
  run_test_match "empty_anc_same_merge" \
    "SELECT dolt_merge('right');" "^[0-9a-f]{40}$" "$DB4/left"
  run_test "empty_anc_same_rows" \
    "SELECT group_concat(id || '|' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);" \
    "1|L,2|R" "$DB4/left"
  run_test "empty_anc_same_integrity" "PRAGMA integrity_check;" "ok" "$DB4/left"
  tip=$(dltest_run_sql "SELECT dolt_hashof('HEAD');" "$DB4/left" | tr -d '[:space:]')
  run_test "empty_anc_same_reopen_rows" \
    "SELECT group_concat(id || '|' || v, ',') FROM (SELECT id, v FROM t ORDER BY id);" \
    "1|L,2|R" "$DB4/$tip"
  run_test "empty_anc_same_reopen_schema" \
    "SELECT sql FROM sqlite_schema WHERE name='t';" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)" "$DB4/$tip"
  run_test "empty_anc_same_reopen_integrity" \
    "PRAGMA integrity_check;" "ok" "$DB4/$tip"
fi

DB5=/tmp/test_merge_empty_anc_diff_$$.db
rm -f "$DB5"
if dltest_require "empty_anc_diff_branches" "$DB5" \
    "SELECT dolt_branch('left'); SELECT dolt_branch('right');" \
 && dltest_require "empty_anc_diff_left" "$DB5/left" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER);
     INSERT INTO t VALUES(1, 1);
     SELECT dolt_commit('-Am', 'left');" \
 && dltest_require "empty_anc_diff_right" "$DB5/right" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, b INTEGER);
     INSERT INTO t VALUES(1, 2);
     SELECT dolt_commit('-Am', 'right');"; then
  run_test_match "empty_anc_diff_conflict" \
    "SELECT dolt_merge('right');" \
    "cannot merge: conflicts detected" "$DB5/left"
  run_test "empty_anc_diff_integrity" "PRAGMA integrity_check;" "ok" "$DB5/left"
  run_test "empty_anc_diff_unmerged" \
    "SELECT sql FROM sqlite_schema WHERE name='t';" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)" "$DB5/left"
fi

# One side renames columns. Both sides add the same indexes, and the other
# side adds one more on a renamed column. The catalog used to load with the
# pre-rename index text and report a malformed schema.
DB6=/tmp/test_merge_rename_shared_idx_$$.db
rm -f "$DB6"
SETUP6="
CREATE TABLE t_flex(
  id INTEGER PRIMARY KEY, a INTEGER, r REAL, num NUMERIC, u, trail TEXT
);
CREATE UNIQUE INDEX t_flex_partial ON t_flex(a) WHERE a IS NOT NULL;
INSERT INTO t_flex VALUES(0, 1, 1.5, 1, 1, 'base');
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'side');
CREATE UNIQUE INDEX flex_pu_14 ON t_flex(r) WHERE r IS NOT NULL;
CREATE UNIQUE INDEX flex_xu_18 ON t_flex(length(coalesce(r, '')));
CREATE UNIQUE INDEX flex_xu_341 ON t_flex(length(coalesce(a, '')));
SELECT dolt_commit('-Am', 'side');
SELECT dolt_checkout('main');
ALTER TABLE t_flex RENAME COLUMN a TO flex_251;
ALTER TABLE t_flex RENAME COLUMN num TO flex_143;
CREATE UNIQUE INDEX flex_pu_14 ON t_flex(r) WHERE r IS NOT NULL;
CREATE UNIQUE INDEX flex_xu_18 ON t_flex(length(coalesce(r, '')));
SELECT dolt_commit('-Am', 'main');
"
if dltest_require "rename_shared_idx_setup" "$DB6" "$SETUP6"; then
  run_test_match "rename_shared_idx_merge" \
    "SELECT dolt_merge('--squash', 'side');" "^[0-9a-f]{40}$" "$DB6"
  run_test "rename_shared_idx_partial" \
    "SELECT sql FROM sqlite_schema WHERE name='t_flex_partial';" \
    "CREATE UNIQUE INDEX t_flex_partial ON t_flex(flex_251) WHERE flex_251 IS NOT NULL" "$DB6"
  run_test "rename_shared_idx_expr" \
    "SELECT sql FROM sqlite_schema WHERE name='flex_xu_341';" \
    "CREATE UNIQUE INDEX flex_xu_341 ON t_flex(length(coalesce(flex_251, '')))" "$DB6"
  run_test "rename_shared_idx_row" \
    "SELECT flex_251 || '|' || r || '|' || flex_143 || '|' || trail FROM t_flex;" \
    "1|1.5|1|base" "$DB6"
  run_test "rename_shared_idx_integrity" "PRAGMA integrity_check;" "ok" "$DB6"
fi

# One side renames columns, then both sides add an index under one name on
# that column's local spelling. The catalog used to load the pre-rename
# index text and report a malformed schema.
DB7=/tmp/test_merge_rename_post_idx_$$.db
rm -f "$DB7"
SETUP7="
CREATE TABLE t(id INTEGER PRIMARY KEY, r INTEGER, a REAL, trail TEXT);
CREATE UNIQUE INDEX ix ON t(r) WHERE r IS NOT NULL;
INSERT INTO t VALUES(1, 1, 1.5, 'base');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'side');
ALTER TABLE t RENAME COLUMN r TO swap;
ALTER TABLE t RENAME COLUMN a TO r;
ALTER TABLE t RENAME COLUMN swap TO a;
ALTER TABLE t RENAME COLUMN trail TO flex_86;
CREATE UNIQUE INDEX ix_both ON t(a) WHERE a IS NOT NULL;
CREATE UNIQUE INDEX ix_new ON t(flex_86);
SELECT dolt_commit('-Am', 'side');
SELECT dolt_checkout('main');
CREATE UNIQUE INDEX ix_both ON t(r) WHERE r IS NOT NULL;
CREATE UNIQUE INDEX ix_extra ON t(r);
SELECT dolt_commit('-Am', 'main');
"
if dltest_require "rename_post_idx_setup" "$DB7" "$SETUP7"; then
  run_test_match "rename_post_idx_merge" \
    "SELECT dolt_merge('side');" \
    "cannot merge: conflicts detected" "$DB7"
  run_test "rename_post_idx_integrity" "PRAGMA integrity_check;" "ok" "$DB7"
  run_test "rename_post_idx_unmerged" \
    "SELECT sql FROM sqlite_schema WHERE name='t';" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, r INTEGER, a REAL, trail TEXT)" "$DB7"
fi

# A swap plus several other renames used to stop ordering early and return
# a bare merge failure. Values stay in their slots.
DB8=/tmp/test_merge_rename_order_$$.db
rm -f "$DB8"
SETUP8="
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, r REAL, c1 NUMERIC, c2, c3 TEXT);
INSERT INTO t VALUES(1, 10, 1.5, 7, 'u', 'trail');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'side');
ALTER TABLE t RENAME COLUMN a TO swap;
ALTER TABLE t RENAME COLUMN r TO a;
ALTER TABLE t RENAME COLUMN swap TO r;
ALTER TABLE t RENAME COLUMN c3 TO c3b;
SELECT dolt_commit('-Am', 'side');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME COLUMN c1 TO d1;
ALTER TABLE t RENAME COLUMN c2 TO d2;
SELECT dolt_commit('-Am', 'main');
"
if dltest_require "rename_order_setup" "$DB8" "$SETUP8"; then
  run_test_match "rename_order_merge" \
    "SELECT dolt_merge('side');" "^[0-9a-f]{40}$" "$DB8"
  run_test "rename_order_sql" \
    "SELECT sql FROM sqlite_schema WHERE name='t';" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, r INTEGER, a REAL, d1 NUMERIC, d2, c3b TEXT)" "$DB8"
  run_test "rename_order_row" \
    "SELECT r || '|' || a || '|' || d1 || '|' || d2 || '|' || c3b FROM t;" \
    "10|1.5|7|u|trail" "$DB8"
  run_test "rename_order_integrity" "PRAGMA integrity_check;" "ok" "$DB8"
fi

dltest_finish
