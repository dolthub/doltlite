#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite indexed OR DELETE tests ==="
echo ""

DB=/tmp/test_doltlite_delete_or_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

setup_sql="
CREATE TABLE tab1(
  pk INTEGER PRIMARY KEY,
  col0 INTEGER,
  col1 FLOAT,
  col2 TEXT,
  col3 INTEGER,
  col4 FLOAT,
  col5 TEXT
);
CREATE INDEX idx_tab1_0 on tab1 (col0);
CREATE INDEX idx_tab1_1 on tab1 (col1);
CREATE INDEX idx_tab1_3 on tab1 (col3);
CREATE INDEX idx_tab1_4 on tab1 (col4);
INSERT INTO tab1 VALUES(0,23,80.49,'a',0,0.0,'a');
INSERT INTO tab1 VALUES(1,35,93.54,'b',0,0.0,'b');
INSERT INTO tab1 VALUES(2,56,89.33,'c',0,0.0,'c');
INSERT INTO tab1 VALUES(3,90,94.9,'d',0,0.0,'d');
INSERT INTO tab1 VALUES(4,76,32.25,'e',0,0.0,'e');
INSERT INTO tab1 VALUES(5,48,43.98,'f',0,0.0,'f');
INSERT INTO tab1 VALUES(6,98,95.92,'g',0,0.0,'g');
INSERT INTO tab1 VALUES(7,37,86.74,'h',0,0.0,'h');
INSERT INTO tab1 VALUES(8,62,76.58,'i',0,0.0,'i');
INSERT INTO tab1 VALUES(9,12,81.67,'j',0,0.0,'j');
"

dltest_run_sql "$setup_sql" "$DB" >/dev/null

plan=$(dltest_run_sql \
  "EXPLAIN DELETE FROM tab1 WHERE col0 < 79 OR col1 > 38.84;" "$DB")
if echo "$plan" | grep -q 'RowSetAdd' && echo "$plan" | grep -q 'RowSetRead'; then
  dltest_pass
else
  dltest_fail "or_delete_uses_rowset" \
    "  expected RowSetAdd and RowSetRead in EXPLAIN output\n  got:\n$plan"
fi

run_test_lastline "or_delete_removes_all_matches" "
DELETE FROM tab1 WHERE col0 < 79 OR col1 > 38.84;
SELECT count(*) FROM tab1;
" "0" "$DB"

dltest_finish
