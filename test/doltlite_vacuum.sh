#!/bin/bash
DOLTLITE=${DOLTLITE:-./doltlite}
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); echo "  PASS: $n"; else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; echo "  FAIL: $n"; echo "    expected: $e"; echo "    got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); echo "  PASS: $n"; else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; echo "  FAIL: $n"; echo "    pattern: $p"; echo "    got:     $r"; fi; }

db_rm() { rm -f "$1" "${1}-wal" "${1}-journal"; }

echo "=== Doltlite VACUUM Tests ==="
echo ""

echo "--- VACUUM removes unreachable chunks (alias for dolt_gc) ---"

DB=/tmp/test_vac_clean_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, 'val_'||x FROM (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<1000) SELECT x FROM r);
SELECT dolt_commit('-A','-m','init');
DELETE FROM t WHERE id > 100;
SELECT dolt_commit('-A','-m','shrink');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "vacuum_no_output" "VACUUM;" "" "$DB"
run_test_match "vacuum_cleaned_already" \
  "SELECT dolt_gc();" \
  "^0 chunks removed" "$DB"
run_test "vacuum_data_intact" \
  "SELECT count(*) FROM t;" \
  "100" "$DB"
db_rm "$DB"

echo ""
echo "--- VACUUM is idempotent ---"

DB=/tmp/test_vac_idem_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','init');
DELETE FROM t WHERE id>1;
SELECT dolt_commit('-A','-m','del');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "VACUUM;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "vacuum_second_run" "VACUUM;" "" "$DB"
run_test_match "vacuum_second_run_gc_noop" \
  "SELECT dolt_gc();" \
  "^0 chunks removed" "$DB"
db_rm "$DB"

echo ""
echo "--- VACUUM on empty database ---"

DB=/tmp/test_vac_empty_$$.db; db_rm "$DB"
run_test "vacuum_empty" "VACUUM;" "" "$DB"
db_rm "$DB"

DB=/tmp/test_vac_no_data_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "vacuum_after_empty_commit" "VACUUM;" "" "$DB"
db_rm "$DB"

echo ""
echo "--- VACUUM INTO is refused ---"

DB=/tmp/test_vac_into_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1);" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "vacuum_into_rejected" \
  "VACUUM INTO '/tmp/test_vac_into_target_$$.db';" \
  "VACUUM INTO is not supported" "$DB"
rm -f "/tmp/test_vac_into_target_$$.db"
db_rm "$DB"

echo ""
echo "--- VACUUM inside explicit transaction is a GC no-op ---"

DB=/tmp/test_vac_txn_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1);" | $DOLTLITE "$DB" > /dev/null 2>&1
# Prolly VACUUM bridges to GC; open transactions skip compaction (SQLITE_OK).
run_test "vacuum_in_txn_noop" \
  "BEGIN; VACUUM; SELECT count(*) FROM t; COMMIT;" \
  "1" "$DB"
db_rm "$DB"

echo ""
echo "--- VACUUM equivalence with dolt_gc ---"

DB=/tmp/test_vac_eq_a_$$.db; db_rm "$DB"
DB_B=/tmp/test_vac_eq_b_$$.db; db_rm "$DB_B"

for D in "$DB" "$DB_B"; do
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, 'v_'||x FROM (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<500) SELECT x FROM r);
SELECT dolt_commit('-A','-m','c1');
UPDATE t SET v=v||'_changed' WHERE id<=100;
DELETE FROM t WHERE id>400;
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$D" > /dev/null 2>&1
done

echo "VACUUM;" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_gc();" | $DOLTLITE "$DB_B" > /dev/null 2>&1

VAC_GC_RESULT=$(echo "SELECT dolt_gc();" | $DOLTLITE "$DB")
DGC_GC_RESULT=$(echo "SELECT dolt_gc();" | $DOLTLITE "$DB_B")

if [ "$VAC_GC_RESULT" = "$DGC_GC_RESULT" ]; then
  PASS=$((PASS+1)); echo "  PASS: vacuum_and_dolt_gc_leave_same_state"
else
  FAIL=$((FAIL+1)); echo "  FAIL: vacuum_and_dolt_gc_leave_same_state"
  echo "    vacuum followed by gc: $VAC_GC_RESULT"
  echo "    dolt_gc followed by gc: $DGC_GC_RESULT"
  ERRORS="$ERRORS\nFAIL: vacuum_and_dolt_gc_leave_same_state\n  vacuum: $VAC_GC_RESULT\n  dolt_gc: $DGC_GC_RESULT"
fi

db_rm "$DB"; db_rm "$DB_B"

echo ""
echo "=== Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests ==="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
