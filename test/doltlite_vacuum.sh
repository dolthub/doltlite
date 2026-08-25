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
echo "--- VACUUM INTO writes a working copy (basic) ---"

DB=/tmp/test_vac_into_basic_$$.db; db_rm "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1);" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "vacuum_into_accepted" \
  "VACUUM INTO '/tmp/test_vac_into_target_$$.db';" \
  "" "$DB"
run_test "vacuum_into_target_readable" \
  "SELECT x FROM t;" "1" "/tmp/test_vac_into_target_$$.db"
rm -f "/tmp/test_vac_into_target_$$.db"
run_test_match "vacuum_into_memory_refused" \
  "VACUUM INTO ':memory:';" \
  "cannot VACUUM a doltlite database INTO an in-memory target" "$DB"
run_test_match "vacuum_into_memory_expr_refused" \
  "CREATE TABLE t2(name TEXT); INSERT INTO t2 VALUES(':memory:');
VACUUM main INTO (SELECT name FROM t2);" \
  "cannot VACUUM a doltlite database INTO an in-memory target" "$DB"
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
echo "--- VACUUM inside a transaction errors like stock; the txn survives ---"

DB=/tmp/test_vac_txn_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "vacuum_in_txn_errors" \
  "BEGIN;
INSERT INTO t VALUES(2,2);
VACUUM;
COMMIT;
SELECT group_concat(id) FROM t;" \
  "cannot VACUUM from within a transaction" "$DB"
run_test_match "vacuum_in_txn_commit_survives_error" \
  "BEGIN;
INSERT INTO t VALUES(3,3);
VACUUM;
COMMIT;" \
  "cannot VACUUM from within a transaction" "$DB"
run_test "vacuum_in_txn_commit_survives_rows" \
  "SELECT count(*) FROM t;" "3" "$DB"
run_test_match "vacuum_into_in_txn_errors" \
  "BEGIN;
VACUUM INTO '/tmp/test_vac_txn_into_$$.db';" \
  "cannot VACUUM from within a transaction" "$DB"
db_rm "$DB"; rm -f "/tmp/test_vac_txn_into_$$.db"

echo ""
echo "--- VACUUM INTO writes an independent compacted copy ---"

DB=/tmp/test_vac_into_$$.db; COPY=/tmp/test_vac_into_copy_$$.db
db_rm "$DB"; db_rm "$COPY"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'kept');
SELECT dolt_commit('-A','-m','first');
INSERT INTO t VALUES(2,'second');
SELECT dolt_commit('-A','-m','second');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "vacuum_into_runs" "VACUUM INTO '$COPY';" "" "$DB"
run_test "vacuum_into_copy_rows" \
  "SELECT group_concat(id||':'||v) FROM t;" "1:kept,2:second" "$COPY"
run_test "vacuum_into_copy_history" \
  "SELECT count(*) FROM dolt_log;" "3" "$COPY"
run_test "vacuum_into_copy_integrity" "PRAGMA integrity_check;" "ok" "$COPY"
run_test "vacuum_into_copy_independent" \
  "INSERT INTO t VALUES(9,'copy only'); SELECT count(*) FROM t;" "3" "$COPY"
run_test "vacuum_into_source_untouched" \
  "SELECT count(*) FROM t;" "2" "$DB"
run_test_match "vacuum_into_existing_target_errors" \
  "VACUUM INTO '$COPY';" "output file already exists" "$DB"
db_rm "$COPY"

echo ""
echo "--- VACUUM INTO onto an empty file is allowed, like stock ---"
: > "$COPY"
run_test "vacuum_into_empty_target_runs" "VACUUM INTO '$COPY';" "" "$DB"
run_test "vacuum_into_empty_target_readable" \
  "SELECT v FROM t WHERE id=1;" "kept" "$COPY"
db_rm "$DB"; db_rm "$COPY"

echo ""
echo "--- VACUUM INTO drops chunks history no longer reaches ---"

DB=/tmp/test_vac_into_gc_$$.db; COPY=/tmp/test_vac_into_gc_copy_$$.db
db_rm "$DB"; db_rm "$COPY"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t SELECT x, 'val_'||x FROM (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<2000) SELECT x FROM r);
SELECT dolt_commit('-A','-m','big');
DELETE FROM t WHERE id > 10;
SELECT dolt_commit('-A','-m','small');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "VACUUM INTO '$COPY';" | $DOLTLITE "$DB" > /dev/null 2>&1
SRC_SIZE=$(wc -c < "$DB" | tr -d ' ')
COPY_SIZE=$(wc -c < "$COPY" | tr -d ' ')
if [ -s "$COPY" ] && [ "$COPY_SIZE" -le "$SRC_SIZE" ]; then
  PASS=$((PASS+1)); echo "  PASS: vacuum_into_copy_not_larger ($COPY_SIZE <= $SRC_SIZE)"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: vacuum_into_copy_not_larger"
  echo "  FAIL: vacuum_into_copy_not_larger ($COPY_SIZE vs $SRC_SIZE)"
fi
run_test "vacuum_into_gc_copy_rows" "SELECT count(*) FROM t;" "10" "$COPY"
db_rm "$DB"; db_rm "$COPY"

echo ""
echo "--- VACUUM replays catalog SQL like stock ---"

# Malformed CREATE text left by writable_schema must fail VACUUM with the
# parser's error, not survive the GC copy.
DB=/tmp/test_vac_writable_schema_$$.db; db_rm "$DB"
OUT=$(echo "CREATE TABLE t7(x);
INSERT INTO t7 VALUES(1);
PRAGMA writable_schema=ON;
UPDATE sqlite_master SET sql='CREATE TABLE [M%s%s%s%s%s%s%s%s%s%s%s%s%s' WHERE name='t7';
VACUUM;" | $DOLTLITE "$DB" 2>&1)
if echo "$OUT" | grep -q 'unrecognized token'; then
  PASS=$((PASS+1)); echo "  PASS: vacuum_rejects_poked_schema"
else
  FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: vacuum_rejects_poked_schema"
  echo "  FAIL: vacuum_rejects_poked_schema (got: $OUT)"
fi
db_rm "$DB"

# A healthy schema still vacuums, and the scratch replay leaves no debris.
DB=/tmp/test_vac_replay_clean_$$.db; db_rm "$DB"
echo "CREATE TABLE a(x INTEGER PRIMARY KEY, y TEXT);
CREATE INDEX ay ON a(y);
INSERT INTO a VALUES(1,'z');
VACUUM;" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "vacuum_replay_clean_data" "SELECT y FROM a WHERE x=1;" "z" "$DB"
run_test "vacuum_replay_integrity" "PRAGMA integrity_check;" "ok" "$DB"
run_test "vacuum_replay_no_scratch_db" "SELECT count(*) FROM pragma_database_list WHERE name LIKE 'vacuum_%';" "0" "$DB"
db_rm "$DB"

echo ""
echo "=== Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests ==="
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
