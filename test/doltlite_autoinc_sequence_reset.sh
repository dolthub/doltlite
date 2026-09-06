#!/bin/bash
# Direct writes to sqlite_sequence must reset the shared AUTOINCREMENT counter the way they do in SQLite.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== sqlite_sequence writes reset the AUTOINCREMENT counter ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_seq_reset_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
DB="$ROOT/seq.db"

run_test "seq_delete_row_restarts_at_1" "
CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v);
INSERT INTO a(v) VALUES('x'),('y'),('z');
DELETE FROM a;
DELETE FROM sqlite_sequence WHERE name='a';
INSERT INTO a(v) VALUES('after delete');
SELECT id, v FROM a;
SELECT name, seq FROM sqlite_sequence;
" "1|after delete
a|1" "$DB"

run_test "seq_update_to_zero_restarts_at_1" "
DELETE FROM a;
UPDATE sqlite_sequence SET seq=0 WHERE name='a';
INSERT INTO a(v) VALUES('after update');
SELECT id FROM a;
" "1" "$DB"

run_test "seq_update_below_max_rowid_uses_max_rowid" "
UPDATE sqlite_sequence SET seq=0 WHERE name='a';
INSERT INTO a(v) VALUES('second');
SELECT group_concat(id) FROM a;
" "1,2" "$DB"

run_test "seq_update_raises" "
UPDATE sqlite_sequence SET seq=100 WHERE name='a';
INSERT INTO a(v) VALUES('hundred one');
SELECT max(id) FROM a;
" "101" "$DB"

run_test "seq_truncate_restarts_at_1" "
DELETE FROM a;
DELETE FROM sqlite_sequence;
INSERT INTO a(v) VALUES('after truncate');
SELECT id FROM a;
SELECT count(*) FROM sqlite_sequence;
" "1
1" "$DB"

run_test "seq_reset_persists_across_reopen" "
INSERT INTO a(v) VALUES('two');
SELECT group_concat(id) FROM a;
" "1,2" "$DB"

run_test "seq_preseed_row" "
CREATE TABLE b(id INTEGER PRIMARY KEY AUTOINCREMENT, v);
INSERT INTO sqlite_sequence VALUES('b', 40);
INSERT INTO b(v) VALUES('x');
SELECT id FROM b;
" "41" "$DB"

run_test "seq_rename_row_moves_counter" "
DELETE FROM b;
UPDATE sqlite_sequence SET name='c', seq=7 WHERE name='b';
CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, v);
INSERT INTO c(v) VALUES('x');
INSERT INTO b(v) VALUES('y');
SELECT id FROM c;
SELECT id FROM b;
" "8
1" "$DB"

run_test "seq_reset_rolls_back" "
DELETE FROM a;
INSERT INTO a(v) VALUES('one'),('two'),('three');
BEGIN;
DELETE FROM a;
DELETE FROM sqlite_sequence WHERE name='a';
ROLLBACK;
INSERT INTO a(v) VALUES('four');
SELECT max(id) FROM a;
" "6" "$DB"

run_test "seq_reset_commit_then_reopen" "
DELETE FROM a;
UPDATE sqlite_sequence SET seq=0 WHERE name='a';
SELECT length(dolt_commit('-Am','reset'));
" "40" "$DB"

run_test "seq_reset_visible_after_reopen" "
INSERT INTO a(v) VALUES('fresh');
SELECT id FROM a;
" "1" "$DB"

run_test "seq_branch_keeps_shared_max_after_other_branch_reset" "
INSERT INTO a(v) VALUES('two'),('three');
SELECT length(dolt_commit('-am','main 1..3'));
SELECT dolt_checkout('-b','f');
INSERT INTO a(v) VALUES('f4');
SELECT length(dolt_commit('-am','f 4'));
SELECT dolt_checkout('main');
INSERT INTO a(v) VALUES('m5');
SELECT max(id) FROM a;
DELETE FROM a;
DELETE FROM sqlite_sequence WHERE name='a';
INSERT INTO a(v) VALUES('m1 again');
SELECT max(id) FROM a;
SELECT dolt_checkout('f');
INSERT INTO a(v) VALUES('f after main reset');
SELECT max(id) FROM a;
" "40
0
40
0
5
1
0
5" "$DB"

run_test "seq_temp_table_unaffected" "
CREATE TEMP TABLE tt(id INTEGER PRIMARY KEY AUTOINCREMENT, v);
INSERT INTO tt(v) VALUES(1),(2);
DELETE FROM tt;
DELETE FROM temp.sqlite_sequence WHERE name='tt';
INSERT INTO tt(v) VALUES(3);
SELECT id FROM tt;
" "1" "$DB"

dltest_finish
