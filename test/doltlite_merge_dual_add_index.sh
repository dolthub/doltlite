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

dltest_finish
