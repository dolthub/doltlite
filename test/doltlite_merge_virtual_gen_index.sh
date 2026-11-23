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

DB2=/tmp/test_merge_virtual_gen_notnull_$$.db
rm -f "$DB2"
SETUP2="
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  a INT,
  g INT GENERATED ALWAYS AS (a * 2) VIRTUAL NOT NULL,
  b TEXT
);
INSERT INTO t(id, a, b) VALUES (1, 10, 'hello');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'from_main';
INSERT INTO t(id, a, b, c) VALUES (3, 30, 'main_row', 'explicit');
SELECT dolt_commit('-Am', 'left add c');
SELECT dolt_checkout('feature');
ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'from_feat';
INSERT INTO t(id, a, b, d) VALUES (2, 20, 'world', 'feat_d');
SELECT dolt_commit('-Am', 'right add d');
SELECT dolt_checkout('main');
"
if dltest_require "virtual_notnull_setup" "$DB2" "$SETUP2"; then
  run_test_match "virtual_notnull_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB2"
  run_test "virtual_notnull_rows" \
    "SELECT group_concat(id || ':' || a || ':' || g || ':' || b || ':' || c || ':' || d, ',') FROM (SELECT id, a, g, b, c, d FROM t ORDER BY id);" \
    "1:10:20:hello:from_main:from_feat,2:20:40:world:from_main:feat_d,3:30:60:main_row:explicit:from_feat" "$DB2"
  run_test_lastline "virtual_notnull_integrity" "PRAGMA integrity_check;" "ok" "$DB2"
fi
rm -f "$DB2"

DB3=/tmp/test_merge_virtual_gen_check_$$.db
rm -f "$DB3"
SETUP3="
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  a INT,
  g INT NOT NULL AS (a * 2) CHECK(g > 0),
  b TEXT CHECK(CAST(b AS TEXT) != '')
);
INSERT INTO t(id, a, b) VALUES (1, 10, 'hello');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'x';
SELECT dolt_commit('-Am', 'left add c');
SELECT dolt_checkout('feature');
INSERT INTO t(id, a, b) VALUES (2, 20, 'world');
SELECT dolt_commit('-Am', 'right insert');
SELECT dolt_checkout('main');
"
if dltest_require "virtual_check_setup" "$DB3" "$SETUP3"; then
  run_test_match "virtual_check_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB3"
  run_test "virtual_check_rows" \
    "SELECT group_concat(id || ':' || a || ':' || g || ':' || b || ':' || c, ',') FROM (SELECT id, a, g, b, c FROM t ORDER BY id);" \
    "1:10:20:hello:x,2:20:40:world:x" "$DB3"
  run_test_lastline "virtual_check_integrity" "PRAGMA integrity_check;" "ok" "$DB3"
fi
rm -f "$DB3"

DB4=/tmp/test_merge_stored_gen_notnull_$$.db
rm -f "$DB4"
SETUP4="
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  a INT,
  g INT GENERATED ALWAYS AS (a * 2) STORED NOT NULL,
  b TEXT
);
INSERT INTO t(id, a, b) VALUES (1, 10, 'hello');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'x';
SELECT dolt_commit('-Am', 'left add c');
SELECT dolt_checkout('feature');
INSERT INTO t(id, a, b) VALUES (2, 20, 'world');
SELECT dolt_commit('-Am', 'right insert');
SELECT dolt_checkout('main');
"
if dltest_require "stored_notnull_setup" "$DB4" "$SETUP4"; then
  run_test_match "stored_notnull_merge" "SELECT dolt_merge('feature');" "^[0-9a-f]{40}$" "$DB4"
  run_test "stored_notnull_rows" \
    "SELECT group_concat(id || ':' || a || ':' || g || ':' || b || ':' || c, ',') FROM (SELECT id, a, g, b, c FROM t ORDER BY id);" \
    "1:10:20:hello:x,2:20:40:world:x" "$DB4"
  run_test_lastline "stored_notnull_integrity" "PRAGMA integrity_check;" "ok" "$DB4"
fi
rm -f "$DB4"

DB5=/tmp/test_merge_virtual_gen_equiv_$$.db
rm -f "$DB5"
SETUP5="
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('same_constrained');
SELECT dolt_branch('diff_constrained');
ALTER TABLE t ADD COLUMN g INT GENERATED ALWAYS AS (a * 2) VIRTUAL NOT NULL;
SELECT dolt_commit('-Am', 'main add g');
SELECT dolt_checkout('same_constrained');
ALTER TABLE t ADD COLUMN g INT AS (a * 2) NOT NULL;
SELECT dolt_commit('-Am', 'same add g');
SELECT dolt_checkout('diff_constrained');
ALTER TABLE t ADD COLUMN g INT AS (a * 2) CHECK(g > 0);
SELECT dolt_commit('-Am', 'diff add g');
SELECT dolt_checkout('main');
"
if dltest_require "virtual_equiv_setup" "$DB5" "$SETUP5"; then
  run_test_match "virtual_equiv_merge" "SELECT dolt_merge('same_constrained');" "^[0-9a-f]{40}$" "$DB5"
  run_test "virtual_equiv_rows" "SELECT id || ':' || a || ':' || g FROM t;" "1:10:20" "$DB5"
  run_test_error_match "virtual_diff_conflict" "SELECT dolt_merge('diff_constrained');" "conflict" "$DB5"
  run_test_lastline "virtual_equiv_integrity" "PRAGMA integrity_check;" "ok" "$DB5"
fi
rm -f "$DB5"

dltest_finish
