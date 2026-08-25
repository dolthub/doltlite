#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite writable_schema catalog poke tests ==="
echo ""

# CLI honors writable_schema without `.dbconfig defensive off`.
DB=/tmp/test_dl_writable_schema_pragma_$$.db; rm -f "$DB"
run_test "writable_schema_default_off" \
  "PRAGMA writable_schema;" \
  "0" "$DB"
run_test "writable_schema_on_roundtrip" \
  "PRAGMA writable_schema=ON; PRAGMA writable_schema;" \
  "1" "$DB"
run_test "writable_schema_off_roundtrip" \
  "PRAGMA writable_schema=ON; PRAGMA writable_schema=OFF; PRAGMA writable_schema;" \
  "0" "$DB"
rm -f "$DB"

DB=/tmp/test_dl_writable_schema_null_row_$$.db; rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null
CREATE TABLE t(a);
PRAGMA writable_schema=ON;
INSERT INTO sqlite_master VALUES(NULL, NULL, NULL, NULL, NULL);
SQL
run_test_match "all_null_schema_row_malformed_on_reopen" \
  "SELECT name FROM sqlite_master;" \
  "malformed database schema" "$DB"
rm -f "$DB"

DB=/tmp/test_dl_writable_schema_root_$$.db; rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null
CREATE TABLE t1(oid INTEGER PRIMARY KEY, a INT);
CREATE INDEX t1i1 ON t1(a);
WITH RECURSIVE c(x) AS (
  SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<98
)
INSERT INTO t1 SELECT x, x FROM c;
.dbconfig defensive off
PRAGMA writable_schema=1;
UPDATE sqlite_master
SET rootpage = CASE name
  WHEN 't1' THEN (SELECT rootpage FROM sqlite_master WHERE name='t1i1')
  WHEN 't1i1' THEN (SELECT rootpage FROM sqlite_master WHERE name='t1')
  ELSE rootpage END
WHERE name IN ('t1','t1i1');
PRAGMA writable_schema=0;
PRAGMA schema_version=123;
SQL

run_test_match "rootpage_swap_fails_loudly" \
  "SELECT count(*) FROM t1 WHERE oid=10;" \
  "malformed database schema" "$DB"
rm -f "$DB"

DB=/tmp/test_dl_writable_schema_xfer_$$.db; rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null
CREATE TABLE t1(a, b, c, d INTEGER PRIMARY KEY);
CREATE TABLE t2(a, b, c, d INTEGER PRIMARY KEY);
INSERT INTO t1 VALUES (1,2,3,100),(4,5,6,101);
INSERT INTO t2 VALUES (1,100,3,1000),(4,101,6,1001);
CREATE INDEX t1a ON t1(a);
CREATE INDEX t2a ON t2(a, b, c);
.dbconfig defensive off
PRAGMA writable_schema = 1;
UPDATE sqlite_master SET sql = 'CREATE INDEX t2a ON t2(a)' WHERE name='t2a';
PRAGMA writable_schema = 0;
PRAGMA schema_version=321;
SQL

run_test "xfer_from_poked_index_keeps_dest_index_consistent" \
  "INSERT INTO t1 SELECT * FROM t2;
   SELECT count(*) FROM pragma_integrity_check WHERE integrity_check LIKE '%t1a%';" \
  "0" "$DB"

run_test_match "poked_source_index_still_reported" \
  "SELECT group_concat(integrity_check, '|') FROM pragma_integrity_check;" \
  "missing from index t2a" "$DB"
rm -f "$DB"

dltest_finish
