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

run_test "seq_clone_copies_without_error" "
.mode batch
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO t VALUES(1),(2);
UPDATE sqlite_sequence SET seq=100 WHERE name='t';
.clone $ROOT/clone.db
.open $ROOT/clone.db
SELECT name, seq FROM sqlite_sequence ORDER BY name;
SELECT count(*) FROM sqlite_sequence;
INSERT INTO t DEFAULT VALUES;
SELECT max(id) FROM t;
SELECT name, seq FROM sqlite_sequence ORDER BY name;
SELECT count(*) FROM sqlite_sequence;
" "t... done
done
t|100
1
101
t|101
1" ":memory:"

run_test "seq_clone_two_autoinc_tables_one_row_each" "
.mode batch
CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT);
CREATE TABLE b(id INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT INTO a DEFAULT VALUES;
INSERT INTO b DEFAULT VALUES;
UPDATE sqlite_sequence SET seq=40 WHERE name='a';
UPDATE sqlite_sequence SET seq=80 WHERE name='b';
.clone $ROOT/clone2.db
.open $ROOT/clone2.db
SELECT name, seq FROM sqlite_sequence ORDER BY name;
SELECT count(*) FROM sqlite_sequence;
INSERT INTO a DEFAULT VALUES;
INSERT INTO b DEFAULT VALUES;
SELECT max(id) FROM a;
SELECT max(id) FROM b;
SELECT name, seq FROM sqlite_sequence ORDER BY name;
SELECT count(*) FROM sqlite_sequence;
" "a... done
b... done
done
a|40
b|80
2
41
81
a|41
b|81
2" ":memory:"

# sqlite_sequence is the reset surface for AUTOINCREMENT tables only. Every
# prolly rowid table reads the same shared counter, so a row naming a plain
# table must leave that table's ids where they were: after deleting the
# largest row the next id still follows that maximum.
SEED_DB="$ROOT/seed.db"
run_test "seq_seed_row_does_not_move_a_plain_table" "
CREATE TABLE seqdummy(id INTEGER PRIMARY KEY AUTOINCREMENT);
CREATE TABLE plain(id INTEGER PRIMARY KEY);
INSERT INTO plain VALUES(8);
DELETE FROM plain;
INSERT INTO sqlite_sequence(name, seq) VALUES('plain', 100);
INSERT INTO plain DEFAULT VALUES;
SELECT id FROM plain;
" "9" "$SEED_DB"

run_test "seq_seed_row_for_a_plain_table_is_still_stored" "
SELECT name, seq FROM sqlite_sequence WHERE name='plain';
" "plain|100" "$SEED_DB"

run_test "seq_delete_of_a_plain_seed_row_leaves_the_counter" "
DELETE FROM sqlite_sequence WHERE name='plain';
INSERT INTO plain DEFAULT VALUES;
SELECT id FROM plain ORDER BY id;
" "9
10" "$SEED_DB"

run_test "seq_seed_row_for_an_unknown_table_is_inert" "
INSERT INTO sqlite_sequence(name, seq) VALUES('ghost', 100);
CREATE TABLE ghost(id INTEGER PRIMARY KEY);
INSERT INTO ghost DEFAULT VALUES;
SELECT id FROM ghost;
" "1" "$SEED_DB"

# A second sqlite_sequence row for the same name does not move the counter.
# The next AUTOINCREMENT read uses the first row and leaves the duplicate.
DUP_DB="$ROOT/dup.db"
run_test "seq_duplicate_row_keeps_the_first" "
CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, v);
INSERT INTO c(v) VALUES('x');
INSERT INTO sqlite_sequence VALUES('c', 200);
INSERT INTO c(v) VALUES('y');
SELECT group_concat(id, ',') FROM (SELECT id FROM c ORDER BY id);
SELECT group_concat(name || ':' || seq, ',') FROM (SELECT name, seq FROM sqlite_sequence ORDER BY seq);
" "1,2
c:2,c:200" "$DUP_DB"

run_test "seq_second_duplicate_row_still_keeps_the_first" "
INSERT INTO sqlite_sequence VALUES('c', 500);
INSERT INTO c(v) VALUES('z');
SELECT group_concat(id, ',') FROM (SELECT id FROM c ORDER BY id);
SELECT group_concat(seq, ',') FROM (SELECT seq FROM sqlite_sequence ORDER BY seq);
" "1,2,3
3,200,500" "$DUP_DB"

run_test "seq_case_mismatched_name_does_not_move_counter" "
INSERT INTO sqlite_sequence VALUES('C', 900);
INSERT INTO c(v) VALUES('w');
SELECT group_concat(id, ',') FROM (SELECT id FROM c ORDER BY id);
" "1,2,3,4" "$DUP_DB"

# The path that must stay unguarded: DROP TABLE owns every rowid table's
# counter, AUTOINCREMENT or not.
run_test "seq_drop_table_still_clears_a_plain_counter" "
CREATE TABLE dropme(id INTEGER PRIMARY KEY);
INSERT INTO dropme VALUES(8);
DROP TABLE dropme;
CREATE TABLE dropme(id INTEGER PRIMARY KEY);
INSERT INTO dropme DEFAULT VALUES;
SELECT id FROM dropme;
" "1" "$SEED_DB"

dltest_finish
