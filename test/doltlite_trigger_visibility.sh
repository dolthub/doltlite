#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite trigger visibility tests ==="
echo ""

DB=/tmp/test_doltlite_trigger_visibility_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

run_test "before_delete_trigger_updates_prior_insert" "
CREATE TABLE tbl(a PRIMARY KEY, b, c);
CREATE TABLE log(a, b, c);
INSERT INTO tbl VALUES(1, 2, 3);
INSERT INTO log VALUES(1, 2, 3);
INSERT INTO log VALUES(10, 20, 30);
CREATE TRIGGER the_trigger BEFORE DELETE ON tbl BEGIN
  INSERT INTO tbl VALUES(500, '' * 10, 700);
  UPDATE tbl SET c = old.c;
  DELETE FROM log;
END;
DELETE FROM tbl WHERE a = 1;
SELECT * FROM tbl;
SELECT * FROM log;
" "500|0|3" "$DB"

run_test_lastline "before_delete_trigger_integrity" "
PRAGMA integrity_check;
" "ok" "$DB"

rm -f "$DB"

run_test "after_insert_trigger_updates_prior_insert" "
CREATE TABLE tbl(a PRIMARY KEY, b, c);
CREATE TABLE log(a, b, c);
INSERT INTO log VALUES(1, 2, 3);
INSERT INTO log VALUES(10, 20, 30);
CREATE TRIGGER the_trigger AFTER INSERT ON tbl BEGIN
  INSERT INTO tbl VALUES(500, new.b * 10, 700);
  UPDATE tbl SET c = '';
  DELETE FROM log;
END;
INSERT INTO tbl VALUES(1, 2, 3);
SELECT * FROM tbl;
SELECT * FROM log;
" "1|2|
500|20|" "$DB"

run_test_lastline "after_insert_trigger_integrity" "
PRAGMA integrity_check;
" "ok" "$DB"

dltest_finish
