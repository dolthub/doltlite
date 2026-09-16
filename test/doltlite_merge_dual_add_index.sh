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

dltest_finish
