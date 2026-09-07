#!/bin/bash
# Implicit rowids on rowid tables come from the branch-shared counter, so inserts on different branches merge.
DOLTLITE="${1:-${DOLTLITE:-./doltlite}}"
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== implicit rowid allocation is shared across branches ==="
echo ""

ROOT=$(mktemp -d /tmp/dl_rowid_alloc_XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
DB="$ROOT/a.db"

run_test "ipk_two_branches_merge_clean" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(v) VALUES('base');
SELECT length(dolt_commit('-Am','init'));
SELECT dolt_checkout('-b','f');
INSERT INTO t(v) VALUES('from f');
SELECT length(dolt_commit('-am','f'));
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('from main');
SELECT length(dolt_commit('-am','m'));
SELECT length(dolt_merge('f'));
SELECT id, v FROM t ORDER BY id;
" "40
0
40
0
40
40
1|base
2|from f
3|from main" "$DB"

run_test "keyless_two_branches_merge_clean" "
CREATE TABLE k(v TEXT);
INSERT INTO k VALUES('base');
SELECT length(dolt_commit('-Am','k'));
SELECT dolt_checkout('-b','kf');
INSERT INTO k VALUES('from kf');
SELECT length(dolt_commit('-am','kf'));
SELECT dolt_checkout('main');
INSERT INTO k VALUES('from main');
SELECT length(dolt_commit('-am','km'));
SELECT length(dolt_merge('kf'));
SELECT rowid, v FROM k ORDER BY rowid;
" "40
0
40
0
40
40
1|base
2|from kf
3|from main" "$DB"

run_test "explicit_id_bumps_shared_counter" "
SELECT dolt_checkout('-b','g');
INSERT INTO t(id, v) VALUES(50, 'explicit on g');
SELECT length(dolt_commit('-am','g'));
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('after 50');
SELECT max(id) FROM t;
" "0
40
0
51" "$DB"

run_test "ipk_no_reuse_after_delete_max" "
DELETE FROM t WHERE id = (SELECT max(id) FROM t);
INSERT INTO t(v) VALUES('next');
SELECT max(id) FROM t;
" "52" "$DB"

run_test "rollback_restores_counter" "
BEGIN;
INSERT INTO t(v) VALUES('rolled back');
SELECT max(id) FROM t;
ROLLBACK;
INSERT INTO t(v) VALUES('after rollback');
SELECT max(id) FROM t;
" "53
53" "$DB"

run_test "insert_select_bumps_counter" "
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t2 SELECT id, v FROM t;
INSERT INTO t2(v) VALUES('after copy');
SELECT max(id) FROM t2;
SELECT length(dolt_commit('-Am','t2'));
SELECT dolt_checkout('-b','h');
INSERT INTO t2(v) VALUES('on h');
SELECT max(id) FROM t2;
SELECT dolt_checkout('main');
" "54
40
0
55
0" "$DB"

run_test "drop_create_restarts" "
DROP TABLE t2;
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t2(v) VALUES('fresh');
SELECT id FROM t2;
" "1" "$DB"

run_test "rename_carries_counter" "
INSERT INTO t2(v) VALUES('two');
DELETE FROM t2;
ALTER TABLE t2 RENAME TO t3;
INSERT INTO t3(v) VALUES('after rename');
SELECT id FROM t3;
" "3" "$DB"

run_test "counter_persists_across_reopen" "
INSERT INTO t3(v) VALUES('reopened');
SELECT id FROM t3 ORDER BY id;
" "3
4" "$DB"

run_test "temp_table_reuses_like_sqlite" "
CREATE TEMP TABLE tt(id INTEGER PRIMARY KEY, v);
INSERT INTO tt(v) VALUES(1),(2);
DELETE FROM tt WHERE id=2;
INSERT INTO tt(v) VALUES(3);
SELECT id FROM tt ORDER BY id;
" "1
2" "$DB"

run_test "autoincrement_table_unchanged" "
CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, v);
INSERT INTO ai(v) VALUES(1),(2);
DELETE FROM ai WHERE id=2;
INSERT INTO ai(v) VALUES(3);
SELECT id FROM ai ORDER BY id;
SELECT name, seq FROM sqlite_sequence;
" "1
3
ai|3" "$DB"

dltest_finish
