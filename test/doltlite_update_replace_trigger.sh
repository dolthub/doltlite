#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite UPDATE REPLACE trigger tests ==="
echo ""

DB=/tmp/test_doltlite_update_replace_trigger_$$.db
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

setup_sql="
CREATE TABLE t1(a UNIQUE ON CONFLICT REPLACE, b);
INSERT INTO t1(a,b) VALUES(4,12),(9,13);
CREATE INDEX i0 ON t1(b);
CREATE TRIGGER tr0 DELETE ON t1 BEGIN
  UPDATE t1 SET b = a;
END;
"

dltest_run_sql "$setup_sql" "$DB" >/dev/null

run_test_lastline "update_replace_trigger_initial_integrity" "
PRAGMA integrity_check;
" "ok" "$DB"

out=$(dltest_run_sql "
PRAGMA recursive_triggers = true;
UPDATE t1 SET a=0;
" "$DB" 2>&1)
rc=$?
if [ "$rc" -ne 0 ] && echo "$out" | grep -q "constraint failed"; then
  dltest_pass
else
  dltest_fail "update_replace_trigger_constraint" \
    "  expected constraint failed\n  rc=$rc\n  output:\n$out"
fi

run_test_lastline "update_replace_trigger_final_integrity" "
PRAGMA integrity_check;
" "ok" "$DB"

run_test "update_replace_trigger_rows_unchanged" "
SELECT rowid,a,b FROM t1 ORDER BY rowid;
" "1|4|12
2|9|13" "$DB"

dltest_finish
