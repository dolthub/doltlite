#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite dolt_status Pushdown Tests ==="

DB1=/tmp/test_status_pd_rename_$$.db
rm -f "$DB1"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a');
INSERT INTO b VALUES(1,'b');
INSERT INTO c VALUES(1,'c');
SELECT dolt_commit('-Am','base');
ALTER TABLE a RENAME TO z;" | $DOLTLITE "$DB1" >/dev/null

run_test "rename_order_shift_unfiltered" \
  "SELECT table_name, staged, status FROM dolt_status ORDER BY table_name;" \
  "a -> z|0|renamed" "$DB1"

run_test "rename_order_shift_eq_compound" \
  "SELECT table_name, staged, status FROM dolt_status WHERE table_name = 'a -> z';" \
  "a -> z|0|renamed" "$DB1"

run_test "rename_order_shift_eq_compound_matches_unpushed" \
  "SELECT (SELECT group_concat(table_name || '|' || staged || '|' || status, ',') FROM dolt_status WHERE table_name = 'a -> z')
        IS (SELECT group_concat(table_name || '|' || staged || '|' || status, ',') FROM dolt_status WHERE table_name || '' = 'a -> z');" \
  "1" "$DB1"

run_test "rename_order_shift_eq_new_name_empty" \
  "SELECT count(*) FROM dolt_status WHERE table_name = 'z';" \
  "0" "$DB1"

run_test "rename_order_shift_eq_old_name_empty" \
  "SELECT count(*) FROM dolt_status WHERE table_name = 'a';" \
  "0" "$DB1"

run_test "rename_order_shift_staged" \
  "SELECT dolt_add('-A');
   SELECT table_name, staged, status FROM dolt_status WHERE table_name = 'a -> z';
   SELECT count(*) FROM dolt_status WHERE table_name = 'z';
   SELECT count(*) FROM dolt_status WHERE table_name = 'a';" \
  "0
a -> z|1|renamed
0
0" "$DB1"

DB2=/tmp/test_status_pd_ignore_$$.db
rm -f "$DB2"
echo "INSERT INTO dolt_ignore VALUES('ign_seq', 1), ('ign_fts', 1);
SELECT dolt_commit('-Am','ignore');
CREATE TABLE ign_seq(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO ign_seq(v) VALUES('x');
CREATE VIRTUAL TABLE ign_fts USING fts5(content);
INSERT INTO ign_fts VALUES('hello');" | $DOLTLITE "$DB2" >/dev/null

run_test "ignore_unfiltered_empty" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB2"

run_test "ignore_eq_sqlite_sequence_empty" \
  "SELECT count(*) FROM dolt_status WHERE table_name = 'sqlite_sequence';" \
  "0" "$DB2"

run_test "ignore_eq_sqlite_sequence_matches_unpushed" \
  "SELECT (SELECT count(*) FROM dolt_status WHERE table_name = 'sqlite_sequence')
        = (SELECT count(*) FROM dolt_status WHERE table_name || '' = 'sqlite_sequence');" \
  "1" "$DB2"

run_test "ignore_eq_fts5_shadow_empty" \
  "SELECT count(*) FROM dolt_status WHERE table_name = 'ign_fts_data';" \
  "0" "$DB2"

run_test "ignore_eq_fts5_shadow_matches_unpushed" \
  "SELECT (SELECT count(*) FROM dolt_status WHERE table_name = 'ign_fts_data')
        = (SELECT count(*) FROM dolt_status WHERE table_name || '' = 'ign_fts_data');" \
  "1" "$DB2"

DB3=/tmp/test_status_pd_staged_cv_$$.db
rm -f "$DB3"
echo "CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT REFERENCES p(id));
CREATE TABLE other(id INTEGER PRIMARY KEY);
INSERT INTO p VALUES(1);
SELECT dolt_commit('-Am','base');
PRAGMA foreign_keys = OFF;
INSERT INTO c VALUES(1, 999);
INSERT INTO other VALUES(1);
SELECT dolt_add('c', 'other');
SELECT dolt_verify_constraints();" | $DOLTLITE "$DB3" >/dev/null

run_test "staged_cv_unfiltered" \
  "SELECT table_name, staged, status FROM dolt_status ORDER BY table_name, staged;" \
  "c|0|constraint violation
other|1|modified" "$DB3"

run_test "staged_cv_staged_eq_1_matches_unpushed" \
  "SELECT table_name, staged, status FROM dolt_status WHERE staged = 1 ORDER BY table_name;" \
  "other|1|modified" "$DB3"

run_test "staged_cv_staged_eq_1_and_table_eq_c_empty" \
  "SELECT count(*) FROM dolt_status WHERE staged = 1 AND table_name = 'c';" \
  "0" "$DB3"

run_test "staged_cv_matches_staged_plus_0" \
  "SELECT (SELECT group_concat(table_name || '|' || staged || '|' || status, ',') FROM dolt_status WHERE staged = 1)
        IS (SELECT group_concat(table_name || '|' || staged || '|' || status, ',') FROM dolt_status WHERE staged + 0 = 1);" \
  "1" "$DB3"

rm -f "$DB1" "$DB2" "$DB3"
dltest_finish
