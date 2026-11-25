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

DB6=/tmp/test_merge_virtual_without_rowid_unique_$$.db
rm -f "$DB6"
SETUP6="
CREATE TABLE t2(
  id INT PRIMARY KEY,
  x INT,
  g INT GENERATED ALWAYS AS (CASE WHEN x > 0 THEN x ELSE 0 END) VIRTUAL,
  UNIQUE(g)
) WITHOUT ROWID;
CREATE TABLE t3(
  x INT,
  g1 TEXT GENERATED ALWAYS AS (CAST(x AS TEXT)) VIRTUAL,
  g2 TEXT GENERATED ALWAYS AS (
    CASE WHEN x IS NULL THEN NULL
         WHEN x IN (1, 2) AND x > 0 THEN g1 || '_ok'
         ELSE 'other' END
  ) VIRTUAL,
  id INT PRIMARY KEY,
  UNIQUE(g2)
) WITHOUT ROWID;
INSERT INTO t2(id, x) VALUES (1, 10);
INSERT INTO t3(id, x) VALUES (1, 1), (2, 2), (3, NULL), (4, NULL), (5, 99);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('clean_feat');
SELECT dolt_branch('dup_feat');
INSERT INTO t2(id, x) VALUES (2, -5);
SELECT dolt_commit('-Am', 'main');
SELECT dolt_checkout('clean_feat');
INSERT INTO t2(id, x) VALUES (4, 20);
SELECT dolt_commit('-Am', 'clean_feat');
SELECT dolt_checkout('dup_feat');
INSERT INTO t2(id, x) VALUES (3, -9);
SELECT dolt_commit('-Am', 'dup_feat');
SELECT dolt_checkout('main');
"
if dltest_require "virtual_worowid_unique_setup" "$DB6" "$SETUP6"; then
  run_test "virtual_worowid_verify_clean" \
    "SELECT dolt_verify_constraints('--all');" "0" "$DB6"
  TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('dup_feat');
SELECT 'TX|' || (SELECT \"table\" || ':' || num_violations FROM dolt_constraint_violations) || '|' ||
       (SELECT group_concat(id || '=' || x, ',') FROM (SELECT id, x FROM dolt_constraint_violations_t2 ORDER BY id)) || '|' ||
       (SELECT dolt_verify_constraints('--all'));
ROLLBACK;" | $DOLTLITE "$DB6" 2>&1 | grep '^TX|')
  if [ "$TX_OUT" = "TX|t2:2|2=-5,3=-9|1" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: virtual_worowid_merge_dup_violations\n  expected: TX|t2:2|2=-5,3=-9|1\n  got:      $TX_OUT"
  fi
  run_test_match "virtual_worowid_merge_clean" \
    "SELECT dolt_merge('clean_feat');" "^[0-9a-f]{40}$" "$DB6"
  run_test "virtual_worowid_verify_after_merge" \
    "SELECT dolt_verify_constraints('--all');" "0" "$DB6"
  run_test_lastline "virtual_worowid_integrity" "PRAGMA integrity_check;" "ok" "$DB6"
fi
rm -f "$DB6"

DB7=/tmp/test_merge_unique_expr_sort_$$.db
rm -f "$DB7"
SETUP7="
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE UNIQUE INDEX idx_lower ON t(lower(name) COLLATE NOCASE DESC);
INSERT INTO t VALUES (1, 'Alpha');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feat');
INSERT INTO t VALUES (2, 'Beta');
SELECT dolt_commit('-Am', 'main');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 'BETA');
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
"
if dltest_require "unique_expr_sort_setup" "$DB7" "$SETUP7"; then
  run_test "unique_expr_sort_verify_clean" \
    "SELECT dolt_verify_constraints('--all');" "0" "$DB7"
  TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('feat');
SELECT 'TX|' || (SELECT \"table\" || ':' || num_violations FROM dolt_constraint_violations) || '|' ||
       (SELECT dolt_verify_constraints('--all'));
ROLLBACK;" | $DOLTLITE "$DB7" 2>&1 | grep '^TX|')
  if [ "$TX_OUT" = "TX|t:2|1" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: unique_expr_sort_merge_dup\n  expected: TX|t:2|1\n  got:      $TX_OUT"
  fi
fi
rm -f "$DB7"

DB8=/tmp/test_merge_strict_virtual_gen_$$.db
rm -f "$DB8"
SETUP8="
CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY, g INT GENERATED ALWAYS AS (a) VIRTUAL);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('bad_feat');
SELECT dolt_branch('ok_feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY, g INT GENERATED ALWAYS AS (a) VIRTUAL) STRICT;
SELECT dolt_commit('-am', 'make strict');
SELECT dolt_checkout('bad_feat');
INSERT INTO t(id, a) VALUES (1, 'not_an_int'), (2, 42);
SELECT dolt_commit('-am', 'insert bad and good');
SELECT dolt_checkout('ok_feat');
INSERT INTO t(id, a) VALUES (2, 42);
SELECT dolt_commit('-am', 'insert good only');
SELECT dolt_checkout('main');
"
if dltest_require "strict_virtual_gen_setup" "$DB8" "$SETUP8"; then
  TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('bad_feat');
SELECT 'TX|' || (SELECT \"table\" || ':' || num_violations FROM dolt_constraint_violations) || '|' ||
       (SELECT group_concat(id || '=' || a || ':' || violation_type || ':' || violation_info, ',')
          FROM (SELECT id, a, violation_type, violation_info FROM dolt_constraint_violations_t ORDER BY id)) || '|' ||
       (SELECT dolt_verify_constraints('--all'));
ROLLBACK;" | $DOLTLITE "$DB8" 2>&1 | grep '^TX|')
  if [ "$TX_OUT" = 'TX|t:1|1=not_an_int:strict type:{"Columns": ["g"]}|1' ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: strict_virtual_gen_violation\n  expected: TX|t:1|1=not_an_int:strict type:{\"Columns\": [\"g\"]}|1\n  got:      $TX_OUT"
  fi
  run_test_match "strict_virtual_gen_merge_ok" \
    "SELECT dolt_merge('ok_feat');" "^[0-9a-f]{40}$" "$DB8"
  run_test "strict_virtual_gen_verify_ok" \
    "SELECT dolt_verify_constraints('--all');" "0" "$DB8"
  run_test "strict_virtual_gen_rows_ok" \
    "SELECT id || ':' || a || ':' || g FROM t;" "2:42:42" "$DB8"
  run_test_lastline "strict_virtual_gen_integrity" "PRAGMA integrity_check;" "ok" "$DB8"
fi
rm -f "$DB8"

DB9=/tmp/test_merge_strict_stored_gen_$$.db
rm -f "$DB9"
SETUP9="
CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY, g INT GENERATED ALWAYS AS (a) STORED);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('bad_feat');
SELECT dolt_branch('ok_feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, a ANY, g INT GENERATED ALWAYS AS (a) STORED) STRICT;
SELECT dolt_commit('-am', 'make strict');
SELECT dolt_checkout('bad_feat');
INSERT INTO t(id, a) VALUES (1, 'not_an_int'), (2, 42);
SELECT dolt_commit('-am', 'insert bad and good');
SELECT dolt_checkout('ok_feat');
INSERT INTO t(id, a) VALUES (2, 42);
SELECT dolt_commit('-am', 'insert good only');
SELECT dolt_checkout('main');
"
if dltest_require "strict_stored_gen_setup" "$DB9" "$SETUP9"; then
  TX_OUT=$(echo "BEGIN;
SELECT dolt_merge('bad_feat');
SELECT 'TX|' || (SELECT \"table\" || ':' || num_violations FROM dolt_constraint_violations) || '|' ||
       (SELECT group_concat(id || '=' || a || ':' || violation_type || ':' || violation_info, ',')
          FROM (SELECT id, a, violation_type, violation_info FROM dolt_constraint_violations_t ORDER BY id)) || '|' ||
       (SELECT dolt_verify_constraints('--all'));
ROLLBACK;" | $DOLTLITE "$DB9" 2>&1 | grep '^TX|')
  if [ "$TX_OUT" = 'TX|t:1|1=not_an_int:strict type:{"Columns": ["g"]}|1' ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: strict_stored_gen_violation\n  expected: TX|t:1|1=not_an_int:strict type:{\"Columns\": [\"g\"]}|1\n  got:      $TX_OUT"
  fi
  run_test_match "strict_stored_gen_merge_ok" \
    "SELECT dolt_merge('ok_feat');" "^[0-9a-f]{40}$" "$DB9"
  run_test "strict_stored_gen_verify_ok" \
    "SELECT dolt_verify_constraints('--all');" "0" "$DB9"
  run_test "strict_stored_gen_rows_ok" \
    "SELECT id || ':' || a || ':' || g FROM t;" "2:42:42" "$DB9"
  run_test_lastline "strict_stored_gen_integrity" "PRAGMA integrity_check;" "ok" "$DB9"
fi
rm -f "$DB9"

dltest_finish
