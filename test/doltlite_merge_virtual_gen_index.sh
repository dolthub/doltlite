#!/bin/bash
# Merge a their-side insert into a VIRTUAL generated index after ADD COLUMN.
# Table column numbers after the VIRTUAL column are not record field numbers.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite merge VIRTUAL generated index ==="
echo ""

DB=/tmp/test_merge_virtual_gen_idx_$$.db
rm -f "$DB"

SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, n INT, g INT AS (n * 2) VIRTUAL);
CREATE INDEX ig ON t(g);
INSERT INTO t(id, n) VALUES(0, 0);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
ALTER TABLE t ADD COLUMN x TEXT;
SELECT dolt_commit('-Am', 'left add col');
SELECT dolt_checkout('feature');
INSERT INTO t(id, n) VALUES(30050, 240);
SELECT dolt_commit('-Am', 'right insert');
SELECT dolt_checkout('main');
"

if dltest_require "virtual_gen_setup" "$DB" "$SETUP"; then
  run_test_match "virtual_gen_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB"
  run_test "virtual_gen_rows" \
    "SELECT group_concat(id || ':' || n || ':' || g, ',') FROM (SELECT id, n, g FROM t ORDER BY id);" \
    "0:0:0,30050:240:480" "$DB"
  run_test "virtual_gen_index_seek" \
    "SELECT id FROM t INDEXED BY ig WHERE g = 480;" \
    "30050" "$DB"
  run_test "virtual_gen_index_count" \
    "SELECT count(*) FROM t INDEXED BY ig;" \
    "2" "$DB"
  run_test_lastline "virtual_gen_integrity" "PRAGMA integrity_check;" "ok" "$DB"
fi

dltest_finish
