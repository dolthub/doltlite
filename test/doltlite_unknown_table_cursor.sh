#!/bin/bash

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(printf '%s\n' "$s" | $DOLTLITE "$d" 2>&1)
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

db_rm() { rm -f "$1" "${1}-wal"; }

echo "=== P5 — unknown-table cursor synthesis ==="
echo ""

DB=/tmp/test_p5_create_$$.db; db_rm "$DB"
run_test "p5_create_insert_select" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT count(*) FROM t;" "3" "$DB"
db_rm "$DB"

DB=/tmp/test_p5_mixed_$$.db; db_rm "$DB"
echo "
CREATE TABLE rt(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE bt(k BLOB PRIMARY KEY, v TEXT) WITHOUT ROWID;
CREATE TABLE ct(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE INDEX idx_rt_v ON rt(v);
CREATE INDEX idx_ct_v ON ct(v);
INSERT INTO rt VALUES(1,'r1'),(2,'r2');
INSERT INTO bt VALUES(x'01','b1');
INSERT INTO ct VALUES(1,1,'c1'),(1,2,'c2');
" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "p5_rowid_table_count" "SELECT count(*) FROM rt;" "2" "$DB"
run_test "p5_without_rowid_blob_pk" "SELECT v FROM bt WHERE k=x'01';" "b1" "$DB"
run_test "p5_composite_pk" "SELECT v FROM ct WHERE a=1 AND b=2;" "c2" "$DB"
run_test "p5_rowid_index_lookup" "SELECT id FROM rt WHERE v='r2';" "2" "$DB"
db_rm "$DB"

DB=/tmp/test_p5_merge_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_add_fk_tables');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_check');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "p5_merge_introduces_new_tables" \
  "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
run_test "p5_merge_main_table_intact" "SELECT count(*) FROM t;" "1" "$DB"
run_test "p5_merge_feat_table_p" "SELECT count(*) FROM p;" "1" "$DB"
run_test "p5_merge_feat_table_c" "SELECT count(*) FROM c;" "1" "$DB"
db_rm "$DB"

DB=/tmp/test_p5_cp_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE newtab(id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO newtab VALUES(1,'hello');
SELECT dolt_commit('-A','-m','add_newtab');" | $DOLTLITE "$DB" > /dev/null 2>&1

CP_HASH=$(echo "SELECT commit_hash FROM dolt_log WHERE message='add_newtab';" \
  | $DOLTLITE "$DB/feat" 2>/dev/null | head -1)

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

if [ -n "$CP_HASH" ]; then
  run_test_match "p5_cherry_pick_new_table" \
    "SELECT dolt_cherry_pick('$CP_HASH');" \
    "^[0-9a-f]{40}$" "$DB"
  run_test "p5_cp_new_table_visible" "SELECT label FROM newtab WHERE id=1;" "hello" "$DB"
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: p5_cherry_pick_setup (couldn't find commit hash)"
fi
db_rm "$DB"

echo ""
if [ $FAIL -gt 0 ]; then
  printf "$ERRORS\n"
  echo "RESULTS: $PASS passed, $FAIL failed"
  exit 1
fi
echo "RESULTS: $PASS passed, $FAIL failed"
