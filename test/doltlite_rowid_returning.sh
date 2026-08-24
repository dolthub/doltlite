#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite clustered-PK RETURNING and NEW.rowid ==="
echo ""

DB=/tmp/test_doltlite_rowid_returning_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

run_test "insert_returning_matches_select" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a',1) RETURNING rowid = last_insert_rowid();
SELECT rowid = last_insert_rowid() FROM t;
" "1
1" "$DB"

rm -f "$DB"
run_test "insert_returning_typeof_integer" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a',1) RETURNING typeof(rowid), k;
" "integer|a" "$DB"

rm -f "$DB"
run_test "delete_returning_typeof_integer" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a',1);
DELETE FROM t RETURNING typeof(rowid), k;
SELECT count(*) FROM t;
" "integer|a
0" "$DB"

rm -f "$DB"
printf '%s\n' "CREATE TABLE t(k TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('a',1);" \
  | "$DOLTLITE" "$DB" >/dev/null
OLD=$(dltest_run_sql "SELECT rowid FROM t;" "$DB")
run_test "delete_returning_matches_prior_select" "
DELETE FROM t RETURNING rowid, k;
" "$OLD|a" "$DB"

rm -f "$DB"
run_test "update_returning_unchanged_pk" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
INSERT INTO t VALUES('a',1);
UPDATE t SET v=2 RETURNING rowid = last_insert_rowid(), k, v;
SELECT rowid = last_insert_rowid(), v FROM t;
" "1|a|2
1|2" "$DB"

rm -f "$DB"
printf '%s\n' "CREATE TABLE t(k TEXT PRIMARY KEY, v INT); INSERT INTO t VALUES('a',1);" \
  | "$DOLTLITE" "$DB" >/dev/null
RET=$(dltest_run_sql "UPDATE t SET k='b' RETURNING rowid;" "$DB")
SEL=$(dltest_run_sql "SELECT rowid FROM t;" "$DB")
if [ -n "$RET" ] && [ "$RET" = "$SEL" ]; then
  dltest_pass
else
  dltest_fail "update_pk_returning_matches_new_select" \
    "  returning: $RET
  select:     $SEL"
fi

rm -f "$DB"
run_test "after_insert_new_rowid" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
CREATE TABLE log(kind TEXT, r);
CREATE TRIGGER tr AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES(typeof(new.rowid), new.rowid);
END;
INSERT INTO t VALUES('a',1);
SELECT kind FROM log;
SELECT r = (SELECT rowid FROM t) FROM log;
" "integer
1" "$DB"

rm -f "$DB"
printf '%s\n' "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
CREATE TABLE log(kind TEXT, r);
INSERT INTO t VALUES('a',1);
CREATE TRIGGER tr AFTER DELETE ON t BEGIN
  INSERT INTO log VALUES(typeof(old.rowid), old.rowid);
END;
" | "$DOLTLITE" "$DB" >/dev/null
OLD=$(dltest_run_sql "SELECT rowid FROM t;" "$DB")
run_test "after_delete_old_rowid" "
DELETE FROM t;
SELECT kind, r FROM log;
" "integer|$OLD" "$DB"

rm -f "$DB"
run_test "after_update_pk_new_differs_from_old" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
CREATE TABLE log(same INT, new_ok INT);
INSERT INTO t VALUES('a',1);
CREATE TRIGGER tr AFTER UPDATE ON t BEGIN
  INSERT INTO log VALUES(old.rowid = new.rowid,
                         new.rowid = (SELECT rowid FROM t));
END;
UPDATE t SET k='b';
SELECT * FROM log;
" "0|1" "$DB"

rm -f "$DB"
run_test "after_update_old_and_new_rowid" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
CREATE TABLE log(okind TEXT, nkind TEXT, same INT);
INSERT INTO t VALUES('a',1);
CREATE TRIGGER tr AFTER UPDATE ON t BEGIN
  INSERT INTO log VALUES(typeof(old.rowid), typeof(new.rowid),
                         old.rowid = new.rowid);
END;
UPDATE t SET v=2;
SELECT * FROM log;
" "integer|integer|1" "$DB"

rm -f "$DB"
run_test "int_pk_returning_is_the_pk" "
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(10,'a') RETURNING rowid, id;
UPDATE t SET v='b' RETURNING rowid;
DELETE FROM t RETURNING rowid, id;
" "10|10
10
10|10" "$DB"

rm -f "$DB"
run_test "integer_pk_returning_unchanged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t(v) VALUES('a') RETURNING rowid, id;
" "1|1" "$DB"

rm -f "$DB"
run_test_match "explicit_without_rowid_returning_rejected" "
CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES('a','one') RETURNING rowid;
" "no such column: rowid" "$DB"

rm -f "$DB"
run_test "oid_and_rowid_alias" "
CREATE TABLE t(k TEXT PRIMARY KEY);
INSERT INTO t VALUES('a') RETURNING oid = rowid, _rowid_ = rowid;
" "1|1" "$DB"

rm -f "$DB"
run_test "composite_pk_returning_matches_select" "
CREATE TABLE t(a INT, b TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,'x') RETURNING rowid = last_insert_rowid();
SELECT rowid = last_insert_rowid() FROM t;
" "1
1" "$DB"

dltest_finish
