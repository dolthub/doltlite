#!/bin/bash





DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Per-Row Conflict Resolution Tests ==="
echo ""

setup_row_conflict_repo() {
  local DB="$1"
  rm -f "$DB"
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'orig');
INSERT INTO t VALUES(2,'keep');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf_val' WHERE id=1;
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1
}

setup_delete_conflict_repo() {
  local DB="$1"
  rm -f "$DB"
  echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE trig_log(note TEXT);
INSERT INTO t VALUES(1,'orig');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-A','-m','hf delete');
SELECT dolt_checkout('main');
UPDATE t SET v='main_val' WHERE id=1;
SELECT dolt_commit('-A','-m','main update');" | $DOLTLITE "$DB" > /dev/null 2>&1
}

setup_text_pk_conflict_repo() {
  local DB="$1"
  rm -f "$DB"
  echo "CREATE TABLE t(id TEXT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES('a','orig_a'),('b','orig_b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf_a' WHERE id='a';
UPDATE t SET v='hf_b' WHERE id='b';
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main_a' WHERE id='a';
UPDATE t SET v='main_b' WHERE id='b';
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1
}

setup_composite_pk_conflict_repo() {
  local DB="$1"
  rm -f "$DB"
  echo "CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,1,'orig_a'),(2,2,'orig_b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('hf');
SELECT dolt_checkout('hf');
UPDATE t SET v='hf_a' WHERE a=1 AND b=1;
UPDATE t SET v='hf_b' WHERE a=2 AND b=2;
SELECT dolt_commit('-A','-m','hf');
SELECT dolt_checkout('main');
UPDATE t SET v='main_a' WHERE a=1 AND b=1;
UPDATE t SET v='main_b' WHERE a=2 AND b=2;
SELECT dolt_commit('-A','-m','main');" | $DOLTLITE "$DB" > /dev/null 2>&1
}


DB=/tmp/test_cfrow_view_$$.db
setup_row_conflict_repo "$DB"
run_test_match "view_count" "BEGIN; SELECT dolt_merge('hf'); SELECT 'VC|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" "^VC\\|1$" "$DB"
run_test_match "view_base_id" "BEGIN; SELECT dolt_merge('hf'); SELECT 'VB|' || base_id FROM dolt_conflicts_t; ROLLBACK;" "^VB\\|1$" "$DB"
run_test_match "view_our_id" "BEGIN; SELECT dolt_merge('hf'); SELECT 'VO|' || our_id FROM dolt_conflicts_t; ROLLBACK;" "^VO\\|1$" "$DB"
run_test_match "view_their_id" "BEGIN; SELECT dolt_merge('hf'); SELECT 'VT|' || their_id FROM dolt_conflicts_t; ROLLBACK;" "^VT\\|1$" "$DB"
run_test_match "view_base_val" "BEGIN; SELECT dolt_merge('hf'); SELECT 'VBT|' || typeof(base_v) FROM dolt_conflicts_t; ROLLBACK;" "^VBT\\|(text|null)$" "$DB"
run_test_match "view_their_val" "BEGIN; SELECT dolt_merge('hf'); SELECT 'VTT|' || typeof(their_v) FROM dolt_conflicts_t; ROLLBACK;" "^VTT\\|text$" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_del_$$.db
setup_row_conflict_repo "$DB"
run_test_match "del_summary_cleared" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id=1; SELECT 'DC|' || count(*) FROM dolt_conflicts; ROLLBACK;" "^DC\\|0$" "$DB"
run_test_match "del_ours_kept" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id=1; SELECT 'DV|' || v FROM t WHERE id=1; ROLLBACK;" "^DV\\|main_val$" "$DB"
run_test_match "del_other_ok" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id=1; SELECT 'DK|' || v FROM t WHERE id=2; ROLLBACK;" "^DK\\|keep$" "$DB"
run_test_match "del_clean" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id=1; SELECT 'DS|' || count(*) FROM dolt_status; ROLLBACK;" "^DS\\|0$" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_ours_$$.db
setup_row_conflict_repo "$DB"
run_test_match "ours_cleared" "BEGIN; SELECT dolt_merge('hf'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT 'OC|' || count(*) FROM dolt_conflicts; ROLLBACK;" "^OC\\|0$" "$DB"
run_test_match "ours_val" "BEGIN; SELECT dolt_merge('hf'); SELECT dolt_conflicts_resolve('--ours','t'); SELECT 'OV|' || v FROM t WHERE id=1; ROLLBACK;" "^OV\\|main_val$" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_noconf_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "noconf_table" "SELECT count(*) FROM dolt_conflicts_t;" "0" "$DB"
run_test "noconf_summary" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_persist_$$.db
setup_row_conflict_repo "$DB"
echo "SELECT dolt_merge('hf');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "persist_summary" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "persist_rows" "SELECT count(*) FROM dolt_conflicts_t;" "0" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_noop_$$.db
setup_row_conflict_repo "$DB"
run_test_match "noop_still_there" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id=999; SELECT 'NC|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" "^NC\\|1$" "$DB"
run_test_match "noop_rowid" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id=999; SELECT 'NR|' || base_id FROM dolt_conflicts_t; ROLLBACK;" "^NR\\|1$" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_delete_$$.db
setup_delete_conflict_repo "$DB"
run_test_match "theirs_delete_clears" "BEGIN; SELECT dolt_merge('hf'); SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TC|' || count(*) FROM dolt_conflicts; ROLLBACK;" "^TC\\|0$" "$DB"
run_test_match "theirs_delete_removes_row" "BEGIN; SELECT dolt_merge('hf'); CREATE TRIGGER audit_delete BEFORE DELETE ON t BEGIN INSERT INTO trig_log VALUES('fired'); END; SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TD|' || count(*) FROM t WHERE id=1; ROLLBACK;" "^TD\\|0$" "$DB"
run_test_match "theirs_delete_trigger_skipped" "BEGIN; SELECT dolt_merge('hf'); CREATE TRIGGER audit_delete BEFORE DELETE ON t BEGIN INSERT INTO trig_log VALUES('fired'); END; SELECT dolt_conflicts_resolve('--theirs','t'); SELECT 'TT|' || count(*) FROM trig_log; ROLLBACK;" "^TT\\|0$" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_textpk_$$.db
setup_text_pk_conflict_repo "$DB"
run_test_match "textpk_conflict_count" "BEGIN; SELECT dolt_merge('hf'); SELECT 'TC|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" "^TC\\|2$" "$DB"
run_test_match "textpk_target_delete_leaves_one" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id='a'; SELECT 'TL|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" "^TL\\|1$" "$DB"
run_test_match "textpk_target_delete_keeps_b" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_id='a'; SELECT 'TK|' || base_id FROM dolt_conflicts_t; ROLLBACK;" "^TK\\|b$" "$DB"
run_test_match "textpk_full_delete_clears" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t; SELECT 'TF|' || count(*) FROM dolt_conflicts; ROLLBACK;" "^TF\\|0$" "$DB"
rm -f "$DB"


DB=/tmp/test_cfrow_compositepk_$$.db
setup_composite_pk_conflict_repo "$DB"
run_test_match "compositepk_conflict_count" "BEGIN; SELECT dolt_merge('hf'); SELECT 'CC|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" "^CC\\|2$" "$DB"
run_test_match "compositepk_target_delete_leaves_one" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_a=1 AND base_b=1; SELECT 'CL|' || count(*) FROM dolt_conflicts_t; ROLLBACK;" "^CL\\|1$" "$DB"
run_test_match "compositepk_target_delete_keeps_other" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t WHERE base_a=1 AND base_b=1; SELECT 'CK|' || base_a || ',' || base_b FROM dolt_conflicts_t; ROLLBACK;" "^CK\\|2,2$" "$DB"
run_test_match "compositepk_full_delete_clears" "BEGIN; SELECT dolt_merge('hf'); DELETE FROM dolt_conflicts_t; SELECT 'CF|' || count(*) FROM dolt_conflicts; ROLLBACK;" "^CF\\|0$" "$DB"
rm -f "$DB"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
