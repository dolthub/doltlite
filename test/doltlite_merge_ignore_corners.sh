#!/bin/bash

DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""

run_test_eq() {
  local n="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $want\n  got:      $got"
  fi
}

run_test_match() {
  local n="$1" got="$2" pat="$3"
  if echo "$got" | grep -qE "$pat"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $pat\n  got:     $got"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== Merge / push corner cases (F3 / F7) ==="
echo ""

DB=/tmp/test_t4_nn_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT NOT NULL, v INTEGER);
INSERT INTO t VALUES(1, 'a', 10);
INSERT INTO t VALUES(2, 'b', 20);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');
UPDATE t SET v=11 WHERE id=1;
SELECT dolt_commit('-A','-m','main_change');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('feat');
UPDATE t SET v=22 WHERE id=2;
SELECT dolt_commit('-A','-m','feat_change');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "f3_merge_with_notnull_succeeds" \
  "$($DOLTLITE "$DB" "SELECT dolt_merge('feat');" 2>&1)" "^[0-9a-f]{40}$"
run_test_eq "f3_main_change_visible" \
  "$($DOLTLITE "$DB" "SELECT v FROM t WHERE id=1;" 2>&1)" "11"
run_test_eq "f3_feat_change_visible" \
  "$($DOLTLITE "$DB" "SELECT v FROM t WHERE id=2;" 2>&1)" "22"
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
