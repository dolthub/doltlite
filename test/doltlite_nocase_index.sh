#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite NOCASE index tests ==="
echo ""

DB=/tmp/test_doltlite_nocase_index_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

setup_sql="
PRAGMA case_sensitive_like=off;
CREATE TABLE t11(
  a INTEGER PRIMARY KEY,
  b TEXT COLLATE nocase,
  c TEXT COLLATE binary
);
INSERT INTO t11 VALUES(1, 'a','a');
INSERT INTO t11 VALUES(2, 'ab','ab');
INSERT INTO t11 VALUES(3, 'abc','abc');
INSERT INTO t11 VALUES(4, 'abcd','abcd');
INSERT INTO t11 VALUES(5, 'A','A');
INSERT INTO t11 VALUES(6, 'AB','AB');
INSERT INTO t11 VALUES(7, 'ABC','ABC');
INSERT INTO t11 VALUES(8, 'ABCD','ABCD');
CREATE INDEX t11b ON t11(b);
"

dltest_run_sql "$setup_sql" "$DB" >/dev/null

run_test "nocase_covering_index_preserves_text" "
SELECT a,b FROM t11 WHERE b LIKE 'abc%' ORDER BY +a;
" "3|abc
4|abcd
7|ABC
8|ABCD" "$DB"

run_test_lastline "nocase_index_integrity_check" "
PRAGMA integrity_check;
" "ok" "$DB"

run_test "nocase_like_z_boundary_preserves_text" "
CREATE TABLE t2(x TEXT COLLATE NOCASE);
CREATE INDEX i2 ON t2(x COLLATE NOCASE);
INSERT INTO t2 VALUES('ZZ-upper-upper');
INSERT INTO t2 VALUES('zZ-lower-upper');
INSERT INTO t2 VALUES('Zz-upper-lower');
INSERT INTO t2 VALUES('zz-lower-lower');
SELECT x FROM t2 WHERE x LIKE 'zz%';
" "zz-lower-lower
zZ-lower-upper
Zz-upper-lower
ZZ-upper-upper" "$DB"

run_test_lastline "nocase_z_boundary_integrity_check" "
PRAGMA integrity_check;
" "ok" "$DB"

# A NOCASE key holding a NUL byte sorts differently from the index order the
# planner would otherwise trust, so the index stops being usable for ORDER BY.
# The plan is the visible form of that answer; it has to stay right whether the
# NUL is committed or still pending.
NULDB=/tmp/test_doltlite_nocase_nul_$$.db
rm -f "$NULDB"
trap 'rm -f "$DB" "$NULDB"' EXIT

run_test "nocase_nul_absent_keeps_index_order" "
CREATE TABLE n(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);
INSERT INTO n VALUES(1,'aa'),(2,'bb'),(3,'cc');
CREATE INDEX i_n ON n(s);
EXPLAIN QUERY PLAN SELECT s FROM n ORDER BY s;
" "QUERY PLAN
\`--SCAN n USING COVERING INDEX i_n" "$NULDB"

run_test "nocase_nul_pending_edit_without_nul_keeps_index_order" "
BEGIN;
UPDATE n SET s='zz' WHERE id=2;
EXPLAIN QUERY PLAN SELECT s FROM n ORDER BY s;
EXPLAIN QUERY PLAN SELECT s FROM n ORDER BY s;
COMMIT;
" "QUERY PLAN
\`--SCAN n USING COVERING INDEX i_n
QUERY PLAN
\`--SCAN n USING COVERING INDEX i_n" "$NULDB"

run_test "nocase_nul_pending_edit_with_nul_drops_index_order" "
BEGIN;
UPDATE n SET s='b'||char(0)||'b' WHERE id=3;
EXPLAIN QUERY PLAN SELECT s FROM n ORDER BY s;
ROLLBACK;
" "QUERY PLAN
|--SCAN n
\`--USE TEMP B-TREE FOR ORDER BY" "$NULDB"

run_test "nocase_nul_committed_drops_index_order" "
INSERT INTO n VALUES(4,'d'||char(0)||'d');
EXPLAIN QUERY PLAN SELECT s FROM n ORDER BY s;
" "QUERY PLAN
|--SCAN n
\`--USE TEMP B-TREE FOR ORDER BY" "$NULDB"

run_test_lastline "nocase_nul_integrity_check" "
PRAGMA integrity_check;
" "ok" "$NULDB"

# A commit moves the index root, and the answer is carried across the flush by
# re-reading only the flushed inserts. Committing between probes must not
# change what the planner concludes, in either direction.
run_test "nocase_nul_absent_survives_commits" "
CREATE TABLE n2(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);
INSERT INTO n2 VALUES(1,'aa'),(2,'bb');
CREATE INDEX i_n2 ON n2(s);
EXPLAIN QUERY PLAN SELECT s FROM n2 ORDER BY s;
INSERT INTO n2 VALUES(3,'cc');
INSERT INTO n2 VALUES(4,'dd');
EXPLAIN QUERY PLAN SELECT s FROM n2 ORDER BY s;
" "QUERY PLAN
\`--SCAN n2 USING COVERING INDEX i_n2
QUERY PLAN
\`--SCAN n2 USING COVERING INDEX i_n2" "$NULDB"

run_test "nocase_nul_committed_after_clean_probe_is_seen" "
INSERT INTO n2 VALUES(5,'e'||char(0)||'e');
EXPLAIN QUERY PLAN SELECT s FROM n2 ORDER BY s;
" "QUERY PLAN
|--SCAN n2
\`--USE TEMP B-TREE FOR ORDER BY" "$NULDB"

run_test "nocase_nul_multicolumn_index_commits" "
CREATE TABLE n3(a TEXT COLLATE NOCASE, b TEXT COLLATE NOCASE, PRIMARY KEY(a,b));
INSERT INTO n3 VALUES('p','q'),('r','s');
CREATE INDEX i_n3 ON n3(b,a);
EXPLAIN QUERY PLAN SELECT b,a FROM n3 ORDER BY b,a;
INSERT INTO n3 VALUES('x','y'||char(0)||'y');
EXPLAIN QUERY PLAN SELECT b,a FROM n3 ORDER BY b,a;
" "QUERY PLAN
\`--SCAN n3 USING COVERING INDEX i_n3
QUERY PLAN
|--SCAN n3 USING COVERING INDEX i_n3
\`--USE TEMP B-TREE FOR ORDER BY" "$NULDB"

# The key that made the index unordered can be deleted, and the index is then
# ordered again. Latching the first positive answer left the plan stuck.
DELDB=/tmp/test_doltlite_nocase_del_$$.db
rm -f "$DELDB"
trap 'rm -f "$DB" "$NULDB" "$DELDB"' EXIT

run_test "nocase_nul_delete_restores_index_order" "
CREATE TABLE d(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);
INSERT INTO d VALUES(1,'aa'),(2,'b'||char(0)||'b');
CREATE INDEX i_d ON d(s);
EXPLAIN QUERY PLAN SELECT s FROM d ORDER BY s;
DELETE FROM d WHERE id=2;
EXPLAIN QUERY PLAN SELECT s FROM d ORDER BY s;
SELECT count(*) FROM d;
" "QUERY PLAN
|--SCAN d
\`--USE TEMP B-TREE FOR ORDER BY
QUERY PLAN
\`--SCAN d USING COVERING INDEX i_d
1" "$DELDB"

# Re-probing must never undo the unordered verdict ANALYZE recorded.
run_test "nocase_analyze_unordered_is_not_undone" "
CREATE TABLE au(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);
INSERT INTO au VALUES(1,'aa'),(2,'bb'),(3,'cc');
CREATE INDEX i_au ON au(s);
ANALYZE;
DELETE FROM sqlite_stat1;
INSERT INTO sqlite_stat1 VALUES('au','i_au','3 1 unordered');
ANALYZE sqlite_master;
EXPLAIN QUERY PLAN SELECT s FROM au ORDER BY s;
" "QUERY PLAN
|--SCAN au
\`--USE TEMP B-TREE FOR ORDER BY" "$DELDB"

dltest_finish
