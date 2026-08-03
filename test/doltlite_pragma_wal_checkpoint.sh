#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
PASS=0; FAIL=0; ERRORS=""

run_test_match() {
  local n="$1" got="$2" pat="$3"
  if echo "$got" | grep -qE "$pat"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $pat\n  got:     $got"
  fi
}

run_test_eq() {
  local n="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $want\n  got:      $got"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== PRAGMA wal_checkpoint (doltlite-format) ==="
echo ""

DB=/tmp/test_wc_dl_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

run_test_eq "wc_dl_default" \
  "$($DOLTLITE "$DB" "PRAGMA wal_checkpoint;" 2>&1)" "0|0|0"

run_test_eq "wc_dl_passive_explicit" \
  "$($DOLTLITE "$DB" "PRAGMA wal_checkpoint(PASSIVE);" 2>&1)" "0|0|0"

for mode in FULL RESTART TRUNCATE; do
  out=$($DOLTLITE "$DB" "PRAGMA wal_checkpoint($mode);" 2>&1)
  run_test_match "wc_dl_${mode}_rejected" "$out" \
    "wal_checkpoint mode is not configurable on doltlite-format"
done

db_rm "$DB"

echo ""
echo "--- checkpoint compacts the named database only ---"

# Checkpoint bridges to GC, so aiming it at the wrong store rewrites a file the
# caller never named. Both databases carry reclaimable chunks so either one
# shrinking is visible.
MAIN=/tmp/test_wc_main_$$.db; db_rm "$MAIN"
AUX=/tmp/test_wc_aux_$$.db;  db_rm "$AUX"
for D in "$MAIN" "$AUX"; do
  $DOLTLITE "$D" "CREATE TABLE big(a INTEGER PRIMARY KEY, b TEXT);
WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<4000)
  INSERT INTO big SELECT i, 'padding-data-'||i FROM c;
DELETE FROM big WHERE a%2=0;" > /dev/null 2>&1
done

main_before=$(wc -c < "$MAIN"); aux_before=$(wc -c < "$AUX")
$DOLTLITE "$MAIN" "ATTACH '$AUX' AS aux; PRAGMA aux.wal_checkpoint;" > /dev/null 2>&1
main_after=$(wc -c < "$MAIN"); aux_after=$(wc -c < "$AUX")

run_test_eq "wc_aux_leaves_main_untouched" "$main_after" "$main_before"
if [ "$aux_after" -lt "$aux_before" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: wc_aux_compacts_aux\n  aux size $aux_before -> $aux_after (expected shrink)"
fi
run_test_eq "wc_aux_rows_intact" \
  "$($DOLTLITE "$AUX" "SELECT count(*) FROM big;" 2>&1)" "2000"

db_rm "$MAIN"; db_rm "$AUX"

# The main-named form must still compact main.
DB=/tmp/test_wc_selfmain_$$.db; db_rm "$DB"
$DOLTLITE "$DB" "CREATE TABLE big(a INTEGER PRIMARY KEY, b TEXT);
WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<4000)
  INSERT INTO big SELECT i, 'padding-data-'||i FROM c;
DELETE FROM big WHERE a%2=0;" > /dev/null 2>&1
before=$(wc -c < "$DB")
$DOLTLITE "$DB" "PRAGMA main.wal_checkpoint;" > /dev/null 2>&1
after=$(wc -c < "$DB")
if [ "$after" -lt "$before" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: wc_main_still_compacts\n  main size $before -> $after (expected shrink)"
fi
db_rm "$DB"

if [ -x "$SQLITE3" ]; then
  echo ""
  echo "--- stock-SQLite file via orig route ---"

  DB=/tmp/test_wc_stock_$$.db; db_rm "$DB"
  $SQLITE3 "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY);" > /dev/null 2>&1

  out=$($DOLTLITE "$DB" "PRAGMA wal_checkpoint(FULL);" 2>&1)
  if echo "$out" | grep -qi "doltlite-format"; then
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: wc_stock_no_doltlite_msg_leaked\n  got: $out"
  else
    PASS=$((PASS+1))
  fi

  db_rm "$DB"
fi

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
