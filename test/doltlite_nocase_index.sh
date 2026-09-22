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

run_test "nocase_nul_equality_keeps_search" "
EXPLAIN QUERY PLAN SELECT id FROM n WHERE s='aa';
SELECT id FROM n WHERE s='aa';
EXPLAIN QUERY PLAN SELECT id FROM n WHERE s='AA';
SELECT id FROM n WHERE s='AA';
" "QUERY PLAN
\`--SEARCH n USING COVERING INDEX i_n (s=?)
1
QUERY PLAN
\`--SEARCH n USING COVERING INDEX i_n (s=?)
1" "$NULDB"

run_test "nocase_nul_probe_scans_for_every_equal" "
CREATE TABLE eq(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);
CREATE INDEX eq_s ON eq(s);
INSERT INTO eq(s) VALUES
  ('a'||char(0)||'b'),('a'||char(0)||'c'),('A'||char(0)||'B'),('k3');
SELECT group_concat(id) FROM (SELECT id FROM eq WHERE s='a'||char(0)||'c' ORDER BY id);
SELECT id FROM eq WHERE s='k3';
EXPLAIN QUERY PLAN SELECT id FROM eq WHERE s='a'||char(0)||'c';
EXPLAIN QUERY PLAN SELECT id FROM eq WHERE s='k3';
" "1,2,3
4
QUERY PLAN
\`--SCAN eq
QUERY PLAN
\`--SEARCH eq USING COVERING INDEX eq_s (s=?)" "$NULDB"

run_test "nocase_nul_in_list_keeps_search" "
EXPLAIN QUERY PLAN SELECT id FROM eq WHERE s IN ('k3','AA');
SELECT group_concat(id) FROM (SELECT id FROM eq WHERE s IN ('k3','aa') ORDER BY id);
EXPLAIN QUERY PLAN SELECT id FROM eq WHERE s IS 'k3';
SELECT id FROM eq WHERE s IS 'K3';
" "QUERY PLAN
\`--SEARCH eq USING COVERING INDEX eq_s (s=?)
4
QUERY PLAN
\`--SEARCH eq USING COVERING INDEX eq_s (s=?)
4" "$NULDB"

run_test "nocase_nul_in_list_with_nul_scans" "
SELECT group_concat(id) FROM (
  SELECT id FROM eq WHERE s IN ('k3','a'||char(0)||'c') ORDER BY id);
EXPLAIN QUERY PLAN SELECT id FROM eq WHERE s IN ('k3','a'||char(0)||'c');
SELECT group_concat(id) FROM (
  SELECT id FROM eq WHERE s='k3' OR s='a'||char(0)||'c' ORDER BY id);
SELECT group_concat(id) FROM (
  SELECT id FROM eq WHERE s LIKE 'k%' ORDER BY id);
EXPLAIN QUERY PLAN SELECT id FROM eq WHERE s=?;
" "1,2,3,4
QUERY PLAN
\`--SCAN eq
1,2,3,4
4
QUERY PLAN
\`--SCAN eq" "$NULDB"

run_test "nocase_nul_leading_binary_keeps_search" "
CREATE TABLE mix(id INTEGER PRIMARY KEY, b TEXT COLLATE BINARY, s TEXT COLLATE NOCASE);
CREATE INDEX mix_bs ON mix(b, s);
INSERT INTO mix VALUES(1,'k','a'||char(0)||'b'),(2,'k','aa'),(3,'z','aa');
EXPLAIN QUERY PLAN SELECT id FROM mix WHERE b='k';
SELECT group_concat(id) FROM (SELECT id FROM mix WHERE b='k' ORDER BY id);
EXPLAIN QUERY PLAN SELECT id FROM mix WHERE b='k' AND s='aa';
SELECT id FROM mix WHERE b='k' AND s='AA';
EXPLAIN QUERY PLAN SELECT id FROM mix WHERE b='k' AND s='a'||char(0)||'c';
SELECT group_concat(id) FROM (
  SELECT id FROM mix WHERE b='k' AND s='a'||char(0)||'c' ORDER BY id);
" "QUERY PLAN
\`--SEARCH mix USING COVERING INDEX mix_bs (b=?)
1,2
QUERY PLAN
\`--SEARCH mix USING COVERING INDEX mix_bs (b=? AND s=?)
2
QUERY PLAN
\`--SEARCH mix USING COVERING INDEX mix_bs (b=?)
1" "$NULDB"

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

# A NOCASE primary key compares equal when the bytes after a NUL differ.
# The second key is a constraint failure, and the stored key answers the
# other spelling.
PKDB=/tmp/test_doltlite_nocase_pk_$$.db
WOROWDB=/tmp/test_doltlite_nocase_pk_worow_$$.db
rm -f "$PKDB" "$WOROWDB"
trap 'rm -f "$DB" "$NULDB" "$DELDB" "$PKDB" "$WOROWDB"' EXIT

run_test "nocase_pk_nul_rejects_equal_key" "
CREATE TABLE t(a TEXT PRIMARY KEY COLLATE NOCASE, b);
INSERT INTO t VALUES ('a', 1);
INSERT INTO t VALUES ('b', 3);
INSERT INTO t VALUES ('a'||char(0)||'b', 4);
INSERT INTO t VALUES ('a'||char(0)||'c', 6);
SELECT count(*) FROM t;
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a='a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a IN ('a'||char(0)||'c') ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a<'a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a>'a'||char(0)||'a' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a<='a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a>='a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a='A' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t ORDER BY a, b);
" "Error near line 6: UNIQUE constraint failed: t.a
3
4
4
1
3
1,4
3,4
1
1,4,3" "$PKDB"

run_test "nocase_pk_nul_without_rowid_rejects_equal_key" "
CREATE TABLE t(a TEXT PRIMARY KEY COLLATE NOCASE, b) WITHOUT ROWID;
INSERT INTO t VALUES ('a', 1);
INSERT INTO t VALUES ('b', 3);
INSERT INTO t VALUES ('a'||char(0)||'b', 4);
INSERT INTO t VALUES ('a'||char(0)||'c', 6);
SELECT count(*) FROM t;
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a='a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a IN ('a'||char(0)||'c') ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a<'a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a>'a'||char(0)||'a' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a<='a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a>='a'||char(0)||'c' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t WHERE a='A' ORDER BY b);
SELECT ifnull(group_concat(b,','),'') FROM (SELECT b FROM t ORDER BY a, b);
" "Error near line 6: UNIQUE constraint failed: t.a
3
4
4
1
3
1,4
3,4
1
1,4,3" "$WOROWDB"

dltest_finish
