#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== dbstat on DoltLite-format databases ==="
echo ""

DB=/tmp/test_dbstat_$$.db; rm -f "$DB"
echo "CREATE TABLE big(i INTEGER PRIMARY KEY, v TEXT);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<200)
INSERT INTO big SELECT x, printf('v%d',x) FROM c;
SELECT dolt_commit('-Am','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "dbstat_scan_rejected" \
  "SELECT count(*) FROM dbstat;" \
  "doltlite: dbstat is not supported" \
  "$DB"

run_test_match "dbstat_not_empty_ok" \
  "SELECT count(*) FROM dbstat;" \
  "Error" \
  "$DB"

run_test_match "dbstat_star_rejected" \
  "SELECT * FROM dbstat;" \
  "content-addressed chunk store has no page layout" \
  "$DB"

run_test "dbstat_table_info_exists" \
  "SELECT count(*) FROM pragma_table_info('dbstat') WHERE name='name';" \
  "1" "$DB"

run_test_match "page_count_still_nonzero" \
  "PRAGMA page_count;" \
  "^[1-9]" \
  "$DB"

STOCK=/tmp/test_dbstat_stock_$$.db; rm -f "$STOCK"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b');" \
  | $DOLTLITE "file:$STOCK?doltlite_engine=sqlite" > /dev/null 2>&1

run_test_match "dbstat_attached_stock_has_rows" \
  "ATTACH 'file:$STOCK?doltlite_engine=sqlite' AS stock;
SELECT count(*)>0 FROM dbstat('stock');" \
  "1" \
  "$DB"

run_test_match "dbstat_temp_has_rows" \
  "CREATE TEMP TABLE tt(x);
INSERT INTO tt VALUES(1),(2);
SELECT count(*)>0 FROM dbstat('temp');" \
  "1" \
  "$DB"

run_test_match "dbstat_memory_rejected" \
  "SELECT count(*) FROM dbstat;" \
  "doltlite: dbstat is not supported" \
  ":memory:"

rm -f "$DB" "$STOCK"
dltest_finish
