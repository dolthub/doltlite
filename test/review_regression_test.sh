#!/bin/bash
DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""
run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi
}
run_test_match() {
  local n="$1" s="$2" p="$3" d="$4"
  local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi
}

echo "=== Review Regression Guards ==="
echo ""


echo "--- Guard 1: Durability (data survives reopen) ---"

DB=/tmp/test_rg_durable_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'durable');
SELECT dolt_commit('-A','-m','persist test');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "durable_data" "SELECT v FROM t WHERE id=1;" "durable" "$DB"
run_test "durable_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

echo "INSERT INTO t VALUES(2,'also durable');
SELECT dolt_commit('-A','-m','second persist');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "durable_second" "SELECT v FROM t WHERE id=2;" "also durable" "$DB"
run_test "durable_log2" "SELECT count(*) FROM dolt_log;" "3" "$DB"
rm -f "$DB"


echo "--- Guard 2: Error propagation (refs integrity) ---"

DB=/tmp/test_rg_refs_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INT);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "refs_exist" \
  "SELECT count(*) FROM dolt_branches;" "^[1-9]" "$DB"

run_test_match "refs_valid_hash" \
  "SELECT length(hash) FROM dolt_branches LIMIT 1;" "^40$" "$DB"

rm -f "$DB"


echo "--- Guard 3: Concurrent commit detection ---"


DB=/tmp/test_rg_conflict_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INT, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','first');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "head_matches_branch" \
  "SELECT (SELECT commit_hash FROM dolt_log LIMIT 1) = (SELECT hash FROM dolt_branches WHERE name='main');" \
  "1" "$DB"

echo "INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','second');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "head_matches_branch_2" \
  "SELECT (SELECT commit_hash FROM dolt_log LIMIT 1) = (SELECT hash FROM dolt_branches WHERE name='main');" \
  "1" "$DB"

rm -f "$DB"


echo "--- Guard 4: Merge log shows both parents ---"

DB=/tmp/test_rg_merge_log_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main1');
SELECT dolt_commit('-A','-m','main init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'feat1');
SELECT dolt_commit('-A','-m','feature work');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main2');
SELECT dolt_commit('-A','-m','main work');
SELECT dolt_merge('feature');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "merge_log_all_parents" \
  "SELECT count(*) FROM dolt_log;" "5" "$DB"

run_test_match "merge_log_has_feature" \
  "SELECT group_concat(message, '|') FROM dolt_log;" "feature work" "$DB"

run_test_match "merge_log_has_main" \
  "SELECT group_concat(message, '|') FROM dolt_log;" "main work" "$DB"

rm -f "$DB"


echo "--- Guard 5: Branch reopen with diverged manifest ---"

DB=/tmp/test_rg_diverge_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'from_main');
SELECT dolt_commit('-A','-m','main commit');
SELECT dolt_branch('dev');
SELECT dolt_checkout('dev');
INSERT INTO t VALUES(2,'from_dev');
SELECT dolt_commit('-A','-m','dev commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main_again');
SELECT dolt_commit('-A','-m','main second');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "diverged_dev_count" "SELECT count(*) FROM t;" "2" "$DB/dev"
run_test "diverged_dev_val" "SELECT v FROM t WHERE id=2;" "from_dev" "$DB/dev"

rm -f "$DB"


echo "--- Guard 6: Virtual table schema correctness ---"

DB=/tmp/test_rg_vtab_$$.db; rm -f "$DB"
echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INT);
INSERT INTO users VALUES(1,'alice',30);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "diff_has_real_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_diff_users');" \
  "from_name" "$DB"

run_test_match "history_has_real_cols" \
  "SELECT group_concat(name) FROM pragma_table_info('dolt_history_users');" \
  "\bname\b" "$DB"

rm -f "$DB"


echo "--- Guard 7: Commit chain integrity ---"

DB=/tmp/test_rg_chain_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_branch('br');
SELECT dolt_checkout('br');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(4,'d');
SELECT dolt_commit('-A','-m','c4');
SELECT dolt_merge('br');
SELECT dolt_commit('-A','-m','c5 merge');
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "chain_log_count" \
  "SELECT count(*) FROM dolt_log;" "^[5-6]$" "$DB"

run_test_match "chain_branches_valid" \
  "SELECT length(hash) FROM dolt_branches WHERE name='main';" "^40$" "$DB"

run_test "chain_data_count" "SELECT count(*) FROM t;" "4" "$DB"

run_test "chain_reopen_count" "SELECT count(*) FROM t;" "4" "$DB"
run_test_match "chain_reopen_log" \
  "SELECT count(*) FROM dolt_log;" "^[5-6]$" "$DB"

rm -f "$DB"


echo "--- Guard 10: .read mixed DML on composite PK ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/mixed_dml.db"
SQL="$TMPROOT/mixed_dml.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 5000); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,NULL);"
  done
  for i in $(seq 1001 4000); do
    echo "UPDATE t SET d=-$i, e='u$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 4 4 5000); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 5001 6500); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','mixed dml');" > /dev/null 2>&1

run_test "mixed_dml_count" \
  "SELECT COUNT(*) FROM t;" "5250" "$DB"
run_test "mixed_dml_min" \
  "SELECT MIN(a) FROM t;" "1" "$DB"
run_test "mixed_dml_max" \
  "SELECT MAX(a) FROM t;" "6500" "$DB"
run_test "mixed_dml_updated_row" \
  "SELECT printf('%d|%s', d, e) FROM t WHERE a=1025 AND b=1025;" "-1025|u1025" "$DB"
run_test "mixed_dml_deleted_row" \
  "SELECT COUNT(*) FROM t WHERE a=2000 AND b=2000;" "0" "$DB"
run_test "mixed_dml_tail_row" \
  "SELECT e FROM t WHERE a=6400 AND b=6400;" "tail" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 11: .read interleaved composite-PK tables ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/interleaved_dml.db"
SQL="$TMPROOT/interleaved_dml.sql"

echo "CREATE TABLE a(
  k1 INTEGER NOT NULL,
  k2 INTEGER NOT NULL,
  v TEXT,
  PRIMARY KEY(k1,k2)
);
CREATE TABLE b(
  k1 INTEGER NOT NULL,
  k2 INTEGER NOT NULL,
  v TEXT,
  PRIMARY KEY(k1,k2)
);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 3000); do
    echo "INSERT INTO a VALUES($i,$i,'a$i');"
    echo "INSERT INTO b VALUES($i,$i,'b$i');"
  done
  for i in $(seq 501 2500); do
    echo "UPDATE a SET v='au$i' WHERE k1=$i AND k2=$i;"
    echo "UPDATE b SET v='bu$i' WHERE k1=$i AND k2=$i;"
  done
  for i in $(seq 3 3 3000); do
    echo "DELETE FROM a WHERE k1=$i AND k2=$i;"
  done
  for i in $(seq 5 5 3000); do
    echo "DELETE FROM b WHERE k1=$i AND k2=$i;"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','interleaved dml');" > /dev/null 2>&1

run_test "interleaved_a_count" \
  "SELECT COUNT(*) FROM a;" "2000" "$DB"
run_test "interleaved_b_count" \
  "SELECT COUNT(*) FROM b;" "2400" "$DB"
run_test "interleaved_a_updated" \
  "SELECT v FROM a WHERE k1=1001 AND k2=1001;" "au1001" "$DB"
run_test "interleaved_b_updated" \
  "SELECT v FROM b WHERE k1=1001 AND k2=1001;" "bu1001" "$DB"
run_test "interleaved_a_deleted" \
  "SELECT COUNT(*) FROM a WHERE k1=1500 AND k2=1500;" "0" "$DB"
run_test "interleaved_b_deleted" \
  "SELECT COUNT(*) FROM b WHERE k1=1500 AND k2=1500;" "0" "$DB"
run_test "interleaved_a_kept" \
  "SELECT v FROM a WHERE k1=1499 AND k2=1499;" "au1499" "$DB"
run_test "interleaved_b_kept" \
  "SELECT v FROM b WHERE k1=1499 AND k2=1499;" "bu1499" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 12: .read mixed DML on WITHOUT ROWID composite PK ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/mixed_dml_wor.db"
SQL="$TMPROOT/mixed_dml_wor.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
) WITHOUT ROWID;" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 3600); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,NULL);"
  done
  for i in $(seq 801 2800); do
    echo "UPDATE t SET d=-$i, e='wu$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 6 6 3600); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 3601 4800); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','mixed dml wor');" > /dev/null 2>&1

run_test "mixed_dml_wor_count" \
  "SELECT COUNT(*) FROM t;" "4200" "$DB"
run_test "mixed_dml_wor_min" \
  "SELECT MIN(a) FROM t;" "1" "$DB"
run_test "mixed_dml_wor_max" \
  "SELECT MAX(a) FROM t;" "4800" "$DB"
run_test "mixed_dml_wor_updated" \
  "SELECT printf('%d|%s', d, e) FROM t WHERE a=1001 AND b=1001;" "-1001|wu1001" "$DB"
run_test "mixed_dml_wor_deleted" \
  "SELECT COUNT(*) FROM t WHERE a=1800 AND b=1800;" "0" "$DB"
run_test "mixed_dml_wor_tail" \
  "SELECT e FROM t WHERE a=4700 AND b=4700;" "tail" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 13: .read interleaved WITHOUT ROWID tables ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/interleaved_wor.db"
SQL="$TMPROOT/interleaved_wor.sql"

echo "CREATE TABLE a(
  k1 INTEGER NOT NULL,
  k2 INTEGER NOT NULL,
  v TEXT,
  PRIMARY KEY(k1,k2)
) WITHOUT ROWID;
CREATE TABLE b(
  k1 INTEGER NOT NULL,
  k2 INTEGER NOT NULL,
  v TEXT,
  PRIMARY KEY(k1,k2)
) WITHOUT ROWID;" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 2400); do
    echo "INSERT INTO a VALUES($i,$i,'a$i');"
    echo "INSERT INTO b VALUES($i,$i,'b$i');"
  done
  for i in $(seq 401 2000); do
    echo "UPDATE a SET v='awu$i' WHERE k1=$i AND k2=$i;"
    echo "UPDATE b SET v='bwu$i' WHERE k1=$i AND k2=$i;"
  done
  for i in $(seq 7 7 2400); do
    echo "DELETE FROM a WHERE k1=$i AND k2=$i;"
  done
  for i in $(seq 8 8 2400); do
    echo "DELETE FROM b WHERE k1=$i AND k2=$i;"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','interleaved wor');" > /dev/null 2>&1

run_test "interleaved_wor_a_count" \
  "SELECT COUNT(*) FROM a;" "2058" "$DB"
run_test "interleaved_wor_b_count" \
  "SELECT COUNT(*) FROM b;" "2100" "$DB"
run_test "interleaved_wor_a_updated" \
  "SELECT v FROM a WHERE k1=999 AND k2=999;" "awu999" "$DB"
run_test "interleaved_wor_b_updated" \
  "SELECT v FROM b WHERE k1=999 AND k2=999;" "bwu999" "$DB"
run_test "interleaved_wor_a_deleted" \
  "SELECT COUNT(*) FROM a WHERE k1=1400 AND k2=1400;" "0" "$DB"
run_test "interleaved_wor_b_deleted" \
  "SELECT COUNT(*) FROM b WHERE k1=1600 AND k2=1600;" "0" "$DB"
run_test "interleaved_wor_a_kept" \
  "SELECT v FROM a WHERE k1=1000 AND k2=1000;" "awu1000" "$DB"
run_test "interleaved_wor_b_kept" \
  "SELECT v FROM b WHERE k1=1001 AND k2=1001;" "bwu1001" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 14: .read savepoint-heavy composite PK ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/savepoint_blobkey.db"
SQL="$TMPROOT/savepoint_blobkey.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  v TEXT,
  PRIMARY KEY(a,b)
);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 1200); do
    echo "INSERT INTO t VALUES($i,$i,'base$i');"
  done
  echo "SAVEPOINT sp1;"
  for i in $(seq 1201 2400); do
    echo "INSERT INTO t VALUES($i,$i,'keep$i');"
  done
  echo "RELEASE sp1;"
  echo "SAVEPOINT sp2;"
  for i in $(seq 2401 3200); do
    echo "INSERT INTO t VALUES($i,$i,'drop$i');"
  done
  for i in $(seq 401 1800); do
    echo "UPDATE t SET v='u$i' WHERE a=$i AND b=$i;"
  done
  echo "ROLLBACK TO sp2;"
  echo "RELEASE sp2;"
  echo "SAVEPOINT sp3;"
  for i in $(seq 6 6 2400); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  echo "RELEASE sp3;"
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','savepoint blobkey');" > /dev/null 2>&1

run_test "savepoint_blobkey_count" \
  "SELECT COUNT(*) FROM t;" "2000" "$DB"
run_test "savepoint_blobkey_kept" \
  "SELECT v FROM t WHERE a=1201 AND b=1201;" "keep1201" "$DB"
run_test "savepoint_blobkey_rolled_back_insert" \
  "SELECT COUNT(*) FROM t WHERE a=2500 AND b=2500;" "0" "$DB"
run_test "savepoint_blobkey_rolled_back_update" \
  "SELECT v FROM t WHERE a=1000 AND b=1000;" "base1000" "$DB"
run_test "savepoint_blobkey_released_insert_survives_rollback" \
  "SELECT v FROM t WHERE a=1501 AND b=1501;" "keep1501" "$DB"
run_test "savepoint_blobkey_delete" \
  "SELECT COUNT(*) FROM t WHERE a=1200 AND b=1200;" "0" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 15: .read savepoint-heavy WITHOUT ROWID composite PK ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/savepoint_wor.db"
SQL="$TMPROOT/savepoint_wor.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  v TEXT,
  PRIMARY KEY(a,b)
) WITHOUT ROWID;" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 1000); do
    echo "INSERT INTO t VALUES($i,$i,'base$i');"
  done
  echo "SAVEPOINT sp1;"
  for i in $(seq 1001 2200); do
    echo "INSERT INTO t VALUES($i,$i,'keep$i');"
  done
  echo "RELEASE sp1;"
  echo "SAVEPOINT sp2;"
  for i in $(seq 2201 3000); do
    echo "INSERT INTO t VALUES($i,$i,'drop$i');"
  done
  for i in $(seq 301 1600); do
    echo "UPDATE t SET v='wu$i' WHERE a=$i AND b=$i;"
  done
  echo "ROLLBACK TO sp2;"
  echo "RELEASE sp2;"
  echo "SAVEPOINT sp3;"
  for i in $(seq 5 5 2200); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  echo "RELEASE sp3;"
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','savepoint wor');" > /dev/null 2>&1

run_test "savepoint_wor_count" \
  "SELECT COUNT(*) FROM t;" "1760" "$DB"
run_test "savepoint_wor_kept" \
  "SELECT v FROM t WHERE a=1001 AND b=1001;" "keep1001" "$DB"
run_test "savepoint_wor_rolled_back_insert" \
  "SELECT COUNT(*) FROM t WHERE a=2500 AND b=2500;" "0" "$DB"
run_test "savepoint_wor_rolled_back_update" \
  "SELECT v FROM t WHERE a=901 AND b=901;" "base901" "$DB"
run_test "savepoint_wor_released_insert_survives_rollback" \
  "SELECT v FROM t WHERE a=1501 AND b=1501;" "keep1501" "$DB"
run_test "savepoint_wor_delete" \
  "SELECT COUNT(*) FROM t WHERE a=2200 AND b=2200;" "0" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 16: .read mixed DML on composite PK with indexes ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/mixed_dml_idx.db"
SQL="$TMPROOT/mixed_dml_idx.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
);
CREATE INDEX idx_t_e ON t(e);
CREATE INDEX idx_t_cd ON t(c,d);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 4200); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'seed');"
  done
  for i in $(seq 1201 3100); do
    echo "UPDATE t SET d=-$i, e='hot$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 9 9 4200); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 4201 5200); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','mixed dml idx');" > /dev/null 2>&1

run_test "mixed_dml_idx_count" \
  "SELECT COUNT(*) FROM t;" "4734" "$DB"
run_test "mixed_dml_idx_forced_hot" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='hot1201';" "-1201|hot1201" "$DB"
run_test "mixed_dml_idx_forced_tail_count" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tail';" "1000" "$DB"
run_test "mixed_dml_idx_forced_cd_lookup" \
  "SELECT e FROM t INDEXED BY idx_t_cd WHERE c=2401 AND d=-2401;" "hot2401" "$DB"
run_test "mixed_dml_idx_deleted_missing" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_cd WHERE c=1800 AND d=-1800;" "0" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 17: .read mixed DML on WITHOUT ROWID composite PK with indexes ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/mixed_dml_wor_idx.db"
SQL="$TMPROOT/mixed_dml_wor_idx.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
) WITHOUT ROWID;
CREATE INDEX idx_t_e ON t(e);
CREATE INDEX idx_t_cd ON t(c,d);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 3600); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'seed');"
  done
  for i in $(seq 901 2600); do
    echo "UPDATE t SET d=-$i, e='warm$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 8 8 3600); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 3601 4300); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','mixed dml wor idx');" > /dev/null 2>&1

run_test "mixed_dml_wor_idx_count" \
  "SELECT COUNT(*) FROM t;" "3850" "$DB"
run_test "mixed_dml_wor_idx_forced_hot" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='warm901';" "-901|warm901" "$DB"
run_test "mixed_dml_wor_idx_forced_tail_count" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tail';" "700" "$DB"
run_test "mixed_dml_wor_idx_forced_cd_lookup" \
  "SELECT e FROM t INDEXED BY idx_t_cd WHERE c=1501 AND d=-1501;" "warm1501" "$DB"
run_test "mixed_dml_wor_idx_deleted_missing" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_cd WHERE c=1200 AND d=-1200;" "0" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 18: .read indexed composite PK through add/commit/reopen ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/bulk_vc_idx.db"
SQL="$TMPROOT/bulk_vc_idx.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
);
CREATE INDEX idx_t_e ON t(e);
CREATE INDEX idx_t_cd ON t(c,d);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 3200); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'seed');"
  done
  for i in $(seq 801 2200); do
    echo "UPDATE t SET d=-$i, e='hot$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 10 10 3200); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 3201 3800); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT COUNT(*) FROM dolt_status;" \
  "SELECT dolt_add('-A');" \
  "SELECT COUNT(*) FROM dolt_status;" \
  "SELECT dolt_commit('-A','-m','bulk vc idx');" > /dev/null 2>&1

run_test "bulk_vc_idx_count" \
  "SELECT COUNT(*) FROM t;" "3480" "$DB"
run_test "bulk_vc_idx_forced_hot" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='hot901';" "-901|hot901" "$DB"
run_test "bulk_vc_idx_forced_tail_count" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tail';" "600" "$DB"
run_test "bulk_vc_idx_forced_cd_lookup" \
  "SELECT e FROM t INDEXED BY idx_t_cd WHERE c=1501 AND d=-1501;" "hot1501" "$DB"
run_test "bulk_vc_idx_log" \
  "SELECT COUNT(*) FROM dolt_log;" "2" "$DB"
run_test "bulk_vc_idx_status_clean" \
  "SELECT COUNT(*) FROM dolt_status;" "0" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 19: .read indexed WITHOUT ROWID composite PK through add/commit/reopen ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/bulk_vc_wor_idx.db"
SQL="$TMPROOT/bulk_vc_wor_idx.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
) WITHOUT ROWID;
CREATE INDEX idx_t_e ON t(e);
CREATE INDEX idx_t_cd ON t(c,d);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 2800); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'seed');"
  done
  for i in $(seq 701 1900); do
    echo "UPDATE t SET d=-$i, e='warm$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 12 12 2800); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 2801 3400); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT COUNT(*) FROM dolt_status;" \
  "SELECT dolt_add('-A');" \
  "SELECT COUNT(*) FROM dolt_status;" \
  "SELECT dolt_commit('-A','-m','bulk vc wor idx');" > /dev/null 2>&1

run_test "bulk_vc_wor_idx_count" \
  "SELECT COUNT(*) FROM t;" "3167" "$DB"
run_test "bulk_vc_wor_idx_forced_hot" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='warm777';" "-777|warm777" "$DB"
run_test "bulk_vc_wor_idx_forced_tail_count" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tail';" "600" "$DB"
run_test "bulk_vc_wor_idx_forced_cd_lookup" \
  "SELECT e FROM t INDEXED BY idx_t_cd WHERE c=1501 AND d=-1501;" "warm1501" "$DB"
run_test "bulk_vc_wor_idx_log" \
  "SELECT COUNT(*) FROM dolt_log;" "2" "$DB"
run_test "bulk_vc_wor_idx_status_clean" \
  "SELECT COUNT(*) FROM dolt_status;" "0" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 20: .read indexed composite PK through branch divergence ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/bulk_branch_idx.db"
SQL="$TMPROOT/bulk_branch_idx.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
);
CREATE INDEX idx_t_e ON t(e);
CREATE INDEX idx_t_cd ON t(c,d);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 2400); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'seed');"
  done
  for i in $(seq 601 1600); do
    echo "UPDATE t SET d=-$i, e='base$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 10 10 2400); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 2401 2800); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail0');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','bulk branch base');" > /dev/null 2>&1

{
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  for i in $(seq 1701 2200); do
    echo "UPDATE t SET d=-$i, e='feat$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 13 13 2800); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 2801 3200); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tailf');"
  done
  echo "SELECT dolt_commit('-A','-m','feat bulk branch');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bulk_branch_idx_main_count" \
  "SELECT COUNT(*) FROM t;" "2560" "$DB"
run_test "bulk_branch_idx_main_forced_base" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='base777';" "-777|base777" "$DB"
run_test "bulk_branch_idx_main_tailf_absent" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tailf';" "0" "$DB"
run_test "bulk_branch_idx_main_log" \
  "SELECT COUNT(*) FROM dolt_log;" "2" "$DB"
run_test "bulk_branch_idx_feat_count" \
  "SELECT COUNT(*) FROM t;" "2763" "$DB/feat"
run_test "bulk_branch_idx_forced_feat" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='feat1702';" "-1702|feat1702" "$DB/feat"
run_test "bulk_branch_idx_tailf_count" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tailf';" "400" "$DB/feat"
run_test "bulk_branch_idx_deleted_missing" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_cd WHERE c=1807 AND d=-1807;" "0" "$DB/feat"
run_test "bulk_branch_idx_feat_log" \
  "SELECT COUNT(*) FROM dolt_log;" "3" "$DB/feat"

rm -rf "$TMPROOT"


echo "--- Guard 21: .read indexed WITHOUT ROWID composite PK through branch divergence ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/bulk_branch_wor_idx.db"
SQL="$TMPROOT/bulk_branch_wor_idx.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
) WITHOUT ROWID;
CREATE INDEX idx_t_e ON t(e);
CREATE INDEX idx_t_cd ON t(c,d);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 2100); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'seed');"
  done
  for i in $(seq 501 1400); do
    echo "UPDATE t SET d=-$i, e='base$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 12 12 2100); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 2101 2400); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tail0');"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','bulk branch wor base');" > /dev/null 2>&1

{
  echo "SELECT dolt_branch('feat');"
  echo "SELECT dolt_checkout('feat');"
  for i in $(seq 1501 2000); do
    echo "UPDATE t SET d=-$i, e='feat$i' WHERE a=$i AND b=$i;"
  done
  for i in $(seq 14 14 2400); do
    echo "DELETE FROM t WHERE a=$i AND b=$i;"
  done
  for i in $(seq 2401 2700); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,'tailf');"
  done
  echo "SELECT dolt_commit('-A','-m','feat bulk wor branch');"
} | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "bulk_branch_wor_idx_main_count" \
  "SELECT COUNT(*) FROM t;" "2225" "$DB"
run_test "bulk_branch_wor_idx_main_forced_base" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='base777';" "-777|base777" "$DB"
run_test "bulk_branch_wor_idx_main_tailf_absent" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tailf';" "0" "$DB"
run_test "bulk_branch_wor_idx_main_log" \
  "SELECT COUNT(*) FROM dolt_log;" "2" "$DB"
run_test "bulk_branch_wor_idx_feat_count" \
  "SELECT COUNT(*) FROM t;" "2379" "$DB/feat"
run_test "bulk_branch_wor_idx_forced_feat" \
  "SELECT printf('%d|%s', d, e) FROM t INDEXED BY idx_t_e WHERE e='feat1703';" "-1703|feat1703" "$DB/feat"
run_test "bulk_branch_wor_idx_tailf_count" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_e WHERE e='tailf';" "300" "$DB/feat"
run_test "bulk_branch_wor_idx_deleted_missing" \
  "SELECT COUNT(*) FROM t INDEXED BY idx_t_cd WHERE c=1764 AND d=-1764;" "0" "$DB/feat"
run_test "bulk_branch_wor_idx_feat_log" \
  "SELECT COUNT(*) FROM dolt_log;" "3" "$DB/feat"

rm -rf "$TMPROOT"


echo "--- Guard 22: sparse >2GiB database opens ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/large_open.db"

GUARD22_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD22_VERSION=$(grep '^#define CHUNK_STORE_VERSION ' "$GUARD22_SCRIPT_DIR/../src/chunk_store.h" | awk '{print $3}')

perl -e '
  use strict;
  use warnings;
  my ($path, $version) = @ARGV;
  my $MAGIC = 0x444C5443;
  my $VERSION = $version;
  my $MANIFEST_SIZE = 168;
  my $WAL_OFF = $MANIFEST_SIZE;
  my $CHUNK_LEN = 2147483400; # just under INT_MAX, pushes file > 2^31
  my $ROOT_OFF = $WAL_OFF + 25 + $CHUNK_LEN;

  sub put_u32 {
    my ($bufref, $off, $v) = @_;
    substr($$bufref, $off, 4) = pack("V", $v);
  }
  sub put_u64 {
    my ($bufref, $off, $v) = @_;
    substr($$bufref, $off, 8) = pack("Q<", $v);
  }

  open my $fh, "+>", $path or die $!;
  binmode $fh;

  my $manifest = "\0" x $MANIFEST_SIZE;
  put_u32(\$manifest, 0, $MAGIC);
  put_u32(\$manifest, 4, $VERSION);
  put_u32(\$manifest, 28, 1);
  put_u64(\$manifest, 32, 0);
  put_u32(\$manifest, 40, 0);
  put_u64(\$manifest, 84, $WAL_OFF);
  print {$fh} $manifest or die $!;

  seek($fh, $WAL_OFF, 0) or die $!;
  print {$fh} chr(1), ("\x11" x 20), pack("V", $CHUNK_LEN) or die $!;

  seek($fh, $ROOT_OFF, 0) or die $!;
  print {$fh} chr(2), $manifest or die $!;
  close $fh or die $!;
' "$DB" "$GUARD22_VERSION"

LARGE_SIZE=$(stat -c%s "$DB" 2>/dev/null || stat -f%z "$DB")
if [ "$LARGE_SIZE" -gt 2147483648 ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: large_db_sparse_size\n  expected: >2147483648\n  got:      $LARGE_SIZE"
fi

LARGE_OPEN_RESULT=$(echo "SELECT 1;" | $DOLTLITE "$DB" 2>&1)
if [ "$LARGE_OPEN_RESULT" = "1" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: large_db_sparse_open\n  expected: 1\n  got:      $LARGE_OPEN_RESULT"
fi

rm -rf "$TMPROOT"


echo "--- Guard 8: Serialization round-trip ---"

DB=/tmp/test_rg_serial_$$.db; rm -f "$DB"

echo "CREATE TABLE mixed(
  id INTEGER PRIMARY KEY,
  name TEXT,
  score REAL,
  data BLOB,
  flag INTEGER
);
INSERT INTO mixed VALUES(1,'hello',3.14,X'DEADBEEF',1);
INSERT INTO mixed VALUES(2,'world',2.71828,X'00FF00FF00',0);
INSERT INTO mixed VALUES(999999,'big id',0.0,NULL,-1);
SELECT dolt_commit('-A','-m','mixed types');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "serial_text" "SELECT name FROM mixed WHERE id=1;" "hello" "$DB"
run_test "serial_real" "SELECT printf('%.2f',score) FROM mixed WHERE id=1;" "3.14" "$DB"
run_test "serial_blob" "SELECT hex(data) FROM mixed WHERE id=1;" "DEADBEEF" "$DB"
run_test "serial_null" "SELECT data IS NULL FROM mixed WHERE id=999999;" "1" "$DB"
run_test "serial_negative" "SELECT flag FROM mixed WHERE id=999999;" "-1" "$DB"
run_test "serial_large_id" "SELECT name FROM mixed WHERE id=999999;" "big id" "$DB"

rm -f "$DB"


echo "--- Guard 9: GC preserves data ---"

DB=/tmp/test_rg_gc_$$.db; rm -f "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'before gc');
SELECT dolt_commit('-A','-m','pre-gc');
INSERT INTO t VALUES(2,'more data');
SELECT dolt_commit('-A','-m','pre-gc 2');
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_data_intact" "SELECT count(*) FROM t;" "2" "$DB"
run_test "gc_log_intact" "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test "gc_val_intact" "SELECT v FROM t WHERE id=1;" "before gc" "$DB"

run_test "gc_reopen_data" "SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"


echo "--- Guard 10: .read bulk INSERT VALUES ---"

TMPROOT=$(mktemp -d)
DB="$TMPROOT/bulk_read.db"
SQL="$TMPROOT/bulk_read.sql"

echo "CREATE TABLE t(
  a INTEGER NOT NULL,
  b INTEGER NOT NULL,
  c INTEGER,
  d INTEGER,
  e TEXT,
  PRIMARY KEY(a,b)
);" | $DOLTLITE "$DB" > /dev/null 2>&1

{
  echo "BEGIN;"
  for i in $(seq 1 5000); do
    echo "INSERT INTO t(a,b,c,d,e) VALUES($i,$i,$i,$i,NULL);"
  done
  echo "COMMIT;"
} > "$SQL"

$DOLTLITE -bail "$DB" -cmd ".read $SQL" \
  "SELECT dolt_commit('-A','-m','bulk read');" > /dev/null 2>&1

run_test "bulk_read_row_count" \
  "SELECT COUNT(*) FROM t;" "5000" "$DB"
run_test "bulk_read_min_pk" \
  "SELECT MIN(a) FROM t;" "1" "$DB"
run_test "bulk_read_max_pk" \
  "SELECT MAX(a) FROM t;" "5000" "$DB"

rm -rf "$TMPROOT"


echo "--- Guard 23: Large integer index keys ---"

DB=/tmp/test_rg_large_int_idx_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x INTEGER UNIQUE, y TEXT);
INSERT INTO t VALUES(9007199254740992,'a');
INSERT INTO t VALUES(9007199254740993,'b');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "large_int_index_order" \
  "SELECT group_concat(x || ':' || y, '|') FROM (SELECT x,y FROM t ORDER BY x);" \
  "9007199254740992:a|9007199254740993:b" "$DB"
run_test "large_int_index_lookup" \
  "SELECT y FROM t WHERE x=9007199254740993;" "b" "$DB"
run_test "large_int_forced_index_lookup" \
  "SELECT y FROM t INDEXED BY sqlite_autoindex_t_1 WHERE x=9007199254740993;" \
  "b" "$DB"

rm -f "$DB"

DB=/tmp/test_rg_negative_large_int_idx_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x INTEGER UNIQUE, y TEXT);
INSERT INTO t VALUES(-9007199254740992,'a');
INSERT INTO t VALUES(-9007199254740993,'b');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "negative_large_int_index_order" \
  "SELECT group_concat(x || ':' || y, '|') FROM (SELECT x,y FROM t ORDER BY x);" \
  "-9007199254740993:b|-9007199254740992:a" "$DB"
run_test "negative_large_int_index_lookup" \
  "SELECT y FROM t WHERE x=-9007199254740993;" "b" "$DB"

rm -f "$DB"

# An integer above 2^53 that no double represents exactly encodes as 9 bytes of
# IEEE base plus the exact value, so the 9-byte key of the base is a byte-prefix
# of it. Probing for the base must not match the larger row: the cases above
# only ever probe the extended value, whose whole key matches.

DB=/tmp/test_rg_large_int_prefix_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x NUMERIC PRIMARY KEY, y TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(9007199254740993,'keep');
INSERT INTO t VALUES(1,'other');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "large_int_base_probe_finds_nothing" \
  "SELECT count(*) FROM t WHERE x=9007199254740992;" "0" "$DB"
run_test "large_int_base_probe_real_finds_nothing" \
  "SELECT count(*) FROM t WHERE x=9007199254740992.0;" "0" "$DB"
run_test "large_int_extended_probe_still_hits" \
  "SELECT y FROM t WHERE x=9007199254740993;" "keep" "$DB"
run_test "large_int_base_update_spares_extended_row" \
  "UPDATE t SET y='clobbered' WHERE x=9007199254740992.0;
   SELECT group_concat(x || ':' || y, '|') FROM (SELECT x,y FROM t ORDER BY x);" \
  "1:other|9007199254740993:keep" "$DB"
run_test "large_int_base_delete_spares_extended_row" \
  "DELETE FROM t WHERE x=9007199254740992;
   SELECT count(*) FROM t;" "2" "$DB"

rm -f "$DB"

DB=/tmp/test_rg_large_int_prefix_uniq_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x NUMERIC UNIQUE, y TEXT);
INSERT INTO t VALUES(9007199254740993,'keep');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "large_int_base_not_a_unique_conflict" \
  "INSERT INTO t VALUES(9007199254740992.0,'base');
   SELECT group_concat(x || ':' || y, '|') FROM (SELECT x,y FROM t ORDER BY x);" \
  "9007199254740992:base|9007199254740993:keep" "$DB"
run_test "large_int_prefix_integrity_check" \
  "PRAGMA integrity_check;" "ok" "$DB"

rm -f "$DB"

DB=/tmp/test_rg_large_int_prefix_range_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x NUMERIC PRIMARY KEY) WITHOUT ROWID;
INSERT INTO t VALUES(9007199254740993);
INSERT INTO t VALUES(9007199254740994);" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "large_int_range_above_base_sees_both" \
  "SELECT count(*) FROM t WHERE x>9007199254740992.0 AND x<9007199254740995;" \
  "2" "$DB"
run_test "large_int_range_order" \
  "SELECT group_concat(x, '|') FROM (SELECT x FROM t ORDER BY x);" \
  "9007199254740993|9007199254740994" "$DB"

rm -f "$DB"

# A descending field is stored with every byte inverted, so the 9-byte numeric
# form would stay a byte prefix of the 18-byte one and memcmp puts a prefix
# first in either direction -- while descending order needs the longer form
# first. The values below pair a base with the integer it stands for, which is
# the only way to put both forms under one base.

DB=/tmp/test_rg_desc_index_order_$$.db; rm -f "$DB"
echo "CREATE TABLE t(a NUMERIC);
CREATE INDEX ad ON t(a DESC);
INSERT INTO t VALUES(9007199254740992.0),(9007199254740993),(9007199254740994),
                    (18014398509481983),(18014398509481984);" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "desc_index_order_large_ints" \
  "SELECT group_concat(a) FROM (SELECT a FROM t ORDER BY a DESC);" \
  "18014398509481984,18014398509481983,9007199254740994,9007199254740993,9007199254740992" \
  "$DB"
# A wrong stored order shows up ascending too: the planner answers ORDER BY a by
# walking the descending index backwards.
run_test "desc_index_order_large_ints_ascending" \
  "SELECT group_concat(a) FROM (SELECT a FROM t ORDER BY a);" \
  "9007199254740992,9007199254740993,9007199254740994,18014398509481983,18014398509481984" \
  "$DB"
run_test "desc_index_range_large_ints" \
  "SELECT count(*) FROM t WHERE a > 9007199254740992.0 AND a < 9007199254740995;" \
  "2" "$DB"
run_test "desc_index_integrity_large_ints" \
  "PRAGMA integrity_check;" "ok" "$DB"

rm -f "$DB"

DB=/tmp/test_rg_desc_pk_order_$$.db; rm -f "$DB"
echo "CREATE TABLE d(a NUMERIC PRIMARY KEY DESC, b) WITHOUT ROWID;
INSERT INTO d VALUES(9007199254740992.0,'x'),(9007199254740993,'y'),
                    (18014398509481983,'z');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "desc_pk_order_large_ints" \
  "SELECT group_concat(a) FROM (SELECT a FROM d ORDER BY a DESC);" \
  "18014398509481983,9007199254740993,9007199254740992" "$DB"
# A descending primary key stores the rows themselves, so a wrong order is
# self-reported rather than just mis-answered.
run_test "desc_pk_integrity_large_ints" \
  "PRAGMA integrity_check;" "ok" "$DB"

rm -f "$DB"

# Reconstructing rows from a DESC numeric PK (ORDER BY a ASC walks the key
# backwards) must return each inserted row once. The short DESC form is
# 10 bytes (base + terminator); decoding must strip the terminator rather
# than treat it as numeric payload.
DB=/tmp/test_rg_desc_pk_asc_scan_$$.db; rm -f "$DB"
echo "CREATE TABLE d(a NUMERIC PRIMARY KEY DESC, b) WITHOUT ROWID;
INSERT INTO d VALUES(9007199254740992.0,'p0'),(9007199254740993,'p1'),
                    (9007199254740994,'p2'),(-9007199254740993,'n1'),
                    (-9007199254740992,'n2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "desc_pk_asc_scan_count" \
  "SELECT count(*) FROM (SELECT a,b FROM d ORDER BY a ASC);" "5" "$DB"
run_test "desc_pk_asc_scan_order" \
  "SELECT group_concat(a||'|'||b) FROM (SELECT a,b FROM d ORDER BY a ASC);" \
  "-9007199254740993|n1,-9007199254740992|n2,9007199254740992|p0,9007199254740993|p1,9007199254740994|p2" \
  "$DB"
run_test "desc_pk_natural_scan_count" \
  "SELECT count(*) FROM d;" "5" "$DB"
run_test "desc_pk_asc_scan_integrity" \
  "PRAGMA integrity_check;" "ok" "$DB"

rm -f "$DB"

DB=/tmp/test_rg_desc_negative_$$.db; rm -f "$DB"
echo "CREATE TABLE t(a NUMERIC, b TEXT);
CREATE INDEX ad ON t(a DESC, b);
INSERT INTO t VALUES(-9007199254740993,'p'),(-9007199254740992,'q'),(5,'r'),
                    (-9223372036854775808,'s');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "desc_index_order_negative_large_ints" \
  "SELECT group_concat(a||'/'||b) FROM (SELECT a,b FROM t ORDER BY a DESC, b);" \
  "5/r,-9007199254740992/q,-9007199254740993/p,-9223372036854775808/s" "$DB"
run_test "desc_index_negative_integrity" \
  "PRAGMA integrity_check;" "ok" "$DB"

rm -f "$DB"


echo "--- Guard 24: ALTER TABLE ADD COLUMN DEFAULT is invisible to the user ---"

# Storing the new column rewrites every row, which stock never does. The rewrite
# must not reach anything the user can observe: no trigger fires for it, the
# change counters do not move, and a trigger that cannot resolve does not fail
# the ALTER.
DB=/tmp/test_rg_altercol_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k INTEGER PRIMARY KEY, a);
CREATE TABLE log(what TEXT);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE TRIGGER tu AFTER UPDATE ON t BEGIN INSERT INTO log VALUES('u'||OLD.k); END;
CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES('i'||NEW.k); END;
CREATE TRIGGER td AFTER DELETE ON t BEGIN INSERT INTO log VALUES('d'||OLD.k); END;
ALTER TABLE t ADD COLUMN c DEFAULT 7;" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "altercol_no_trigger_fired" \
  "SELECT coalesce(group_concat(what,','),'none') FROM log;" "none" "$DB"
run_test "altercol_default_materialized" \
  "SELECT group_concat(k||'/'||quote(c),',') FROM t;" "1/7,2/7,3/7" "$DB"
run_test "altercol_changes_not_bumped" \
  "UPDATE t SET a=a+1 WHERE k=1;
   ALTER TABLE t ADD COLUMN d DEFAULT 'x';
   SELECT changes();" "1" "$DB"
run_test "altercol_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# The trigger body references a table that does not exist. Stock resolves
# trigger bodies when they fire, so the ALTER succeeds; an internal UPDATE that
# compiled the trigger would fail it instead.
DB=/tmp/test_rg_altercol_dangling_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k INTEGER PRIMARY KEY, a);
INSERT INTO t VALUES(1,10);
CREATE TRIGGER tr AFTER UPDATE ON t BEGIN INSERT INTO gone(n) VALUES(NEW.a); END;
ALTER TABLE t ADD COLUMN c DEFAULT 7;" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "altercol_dangling_trigger_column_added" \
  "SELECT count(*) FROM pragma_table_info('t');" "3" "$DB"
run_test "altercol_dangling_trigger_value" \
  "SELECT quote(c) FROM t WHERE k=1;" "7" "$DB"

# The trigger still fires for a real update, so suppression is scoped to the
# ALTER and not left switched on.
DB=/tmp/test_rg_altercol_scope_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k INTEGER PRIMARY KEY, a);
CREATE TABLE log(what TEXT);
INSERT INTO t VALUES(1,10);
CREATE TRIGGER tu AFTER UPDATE ON t BEGIN INSERT INTO log VALUES('u'||OLD.k); END;
ALTER TABLE t ADD COLUMN c DEFAULT 7;
UPDATE t SET a=99 WHERE k=1;" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "altercol_trigger_still_fires_after" \
  "SELECT group_concat(what,',') FROM log;" "u1" "$DB"
rm -f "$DB" /tmp/test_rg_altercol_dangling_$$.db


echo "--- Guard 24: SAVEPOINT rollback after INSERT RETURNING + ANALYZE (#2103) ---"
# prollyBtreeBeginStmt used to push every journal level as a statement
# savepoint, so a named SAVEPOINT never captured table roots. ANALYZE after
# INSERT RETURNING flushed the index; ROLLBACK TO then left the index entry
# for the rolled-back row behind while the table scan was correct.
DB=/tmp/test_rg_sp_returning_analyze_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k INTEGER, a, b TEXT DEFAULT 'dflt', g AS (length(coalesce(quote(b),'')) + coalesce(a, 0)) VIRTUAL);
CREATE INDEX i_a ON t(a);
INSERT OR IGNORE INTO t(k, a, b) VALUES(34, -18, x'00');
INSERT OR REPLACE INTO t(k, a, b) VALUES(0, 'AB ', 'z');
INSERT OR REPLACE INTO t(k, a, b) VALUES(-27, 'A', 'a' || char(0) || 'b');
INSERT OR IGNORE INTO t(k, a, b) VALUES (34, 'a', 'ab'), (18, 20.369, 'z');
INSERT OR ROLLBACK INTO t(k, a, b) VALUES(9007199254740993, -46.284, 'A');
INSERT OR IGNORE INTO t(k, a, b) VALUES(38, -27, 'a');
INSERT OR ROLLBACK INTO t(k, a, b) VALUES(-22, -45.950, 'A');
ANALYZE t;
BEGIN;
DELETE FROM t WHERE b IN (SELECT b FROM t WHERE k = -25);
SAVEPOINT sp2;
INSERT OR REPLACE INTO t(k, a, b) VALUES(9007199254740992, '', 'a' || char(0) || 'b') RETURNING coalesce(quote(k),'N'), coalesce(quote(a),'N'), coalesce(quote(b),'N');
ANALYZE t;
ROLLBACK TO sp2;" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "sp_returning_analyze_count" "SELECT count(*) FROM t;" "8" "$DB"
run_test "sp_returning_analyze_not_indexed" \
  "SELECT count(*) FROM t NOT INDEXED;" "8" "$DB"
run_test "sp_returning_analyze_indexed" \
  "SELECT count(*) FROM t INDEXED BY i_a;" "8" "$DB"
run_test "sp_returning_analyze_order" \
  "SELECT group_concat(coalesce(quote(a), 'N'), '|') FROM (SELECT a FROM t ORDER BY a ASC, coalesce(quote(k), 'N'), coalesce(quote(b), 'N'));" \
  "-46.284|-45.95|-27|-18|20.369|'A'|'AB '|'a'" "$DB"
run_test "sp_returning_analyze_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"


echo "--- Guard: BOTH-landing keeps the pending value (in-txn UPDATE vs descending bounded scans) ---"

# The IndexMoveto past-end fallback lands on the merged last row; caching the
# tree payload there on a BOTH landing served the shadowed committed value
# instead of the one this transaction wrote. Expected values verified against
# stock SQLite 3.54.
DB=/tmp/test_rg_both_landing_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k TEXT PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO t VALUES('a','old');
INSERT INTO t VALUES('m','mid');
CREATE TABLE t2(k TEXT PRIMARY KEY DESC, v) WITHOUT ROWID;
INSERT INTO t2 VALUES('a','old');
INSERT INTO t2 VALUES('m','mid');
CREATE TABLE t3(k TEXT PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO t3 VALUES('a','old');
INSERT INTO t3 VALUES('m','mid');
INSERT INTO t3 VALUES('z','top');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "both_landing_desc_lt" \
  "BEGIN; UPDATE t SET v='new' WHERE k='m'; SELECT group_concat(v,'|') FROM (SELECT v FROM t WHERE k < 'q' ORDER BY k DESC); ROLLBACK;" \
  "new|old" "$DB"
run_test "both_landing_desc_le" \
  "BEGIN; UPDATE t SET v='new' WHERE k='m'; SELECT group_concat(v,'|') FROM (SELECT v FROM t WHERE k <= 'm' ORDER BY k DESC); ROLLBACK;" \
  "new|old" "$DB"
run_test "both_landing_scalar_last" \
  "BEGIN; UPDATE t SET v='new' WHERE k='m'; SELECT (SELECT v FROM t WHERE k < 'z' ORDER BY k DESC LIMIT 1); ROLLBACK;" \
  "new" "$DB"
run_test "both_landing_desc_pk_asc_scan" \
  "BEGIN; UPDATE t2 SET v='new' WHERE k='m'; SELECT group_concat(v,'|') FROM (SELECT v FROM t2 WHERE k > '' ORDER BY k); ROLLBACK;" \
  "old|new" "$DB"
run_test "both_landing_tombstone_reroute" \
  "BEGIN; DELETE FROM t3 WHERE k='z'; UPDATE t3 SET v='new' WHERE k='m'; SELECT group_concat(v,'|') FROM (SELECT v FROM t3 WHERE k < 'y' ORDER BY k DESC); ROLLBACK;" \
  "new|old" "$DB"
rm -f "$DB"

echo "--- Guard 25: clustered primary-key autoindex catalog row (#2375) ---"

DB=/tmp/test_rg_pk_autoindex_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k TEXT PRIMARY KEY, u TEXT UNIQUE, v INT);
INSERT INTO t VALUES('a','one',1);
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "pk_autoindex_schema_rows" \
  "SELECT group_concat(name,'|') FROM (SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name);" \
  "sqlite_autoindex_t_1|sqlite_autoindex_t_2" "$DB"
run_test "pk_autoindex_schema_sql" \
  "SELECT group_concat(name||':'||coalesce(sql,'NULL'),'|') FROM (SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name);" \
  "sqlite_autoindex_t_1:NULL|sqlite_autoindex_t_2:NULL" "$DB"
run_test "pk_autoindex_pragma_unchanged" \
  "SELECT group_concat(name||':'||origin,'|') FROM (SELECT name,origin FROM pragma_index_list('t') ORDER BY seq);" \
  "sqlite_autoindex_t_2:u|sqlite_autoindex_t_1:pk" "$DB"
run_test "pk_autoindex_shares_table_root" \
  "SELECT i.rootpage=t.rootpage FROM sqlite_master AS i JOIN sqlite_master AS t ON t.name=i.tbl_name WHERE i.name='sqlite_autoindex_t_1';" \
  "1" "$DB"
run_test "pk_autoindex_forced_scan" \
  "SELECT v FROM t INDEXED BY sqlite_autoindex_t_1 WHERE k='a';" \
  "1" "$DB"
run_test "pk_autoindex_analyze_name" \
  "ANALYZE t; SELECT group_concat(idx,'|') FROM (SELECT idx FROM sqlite_stat1 WHERE tbl='t' ORDER BY idx);" \
  "sqlite_autoindex_t_1|sqlite_autoindex_t_2" "$DB"
run_test "pk_autoindex_no_table_pragma" \
  "PRAGMA index_info(t);" \
  "" "$DB"
run_test "pk_autoindex_named_pragma" \
  "PRAGMA index_info('sqlite_autoindex_t_1');" \
  "0|0|k" "$DB"
run_test_match "pk_autoindex_query_plan" \
  "EXPLAIN QUERY PLAN SELECT * FROM t WHERE k='a';" \
  "USING INDEX sqlite_autoindex_t_1" "$DB"
run_test_match "pk_autoindex_covering_query_plan" \
  "EXPLAIN QUERY PLAN SELECT k FROM t WHERE k='a';" \
  "USING COVERING INDEX sqlite_autoindex_t_1" "$DB"
run_test "pk_autoindex_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

DB=/tmp/test_rg_without_rowid_pk_name_$$.db; rm -f "$DB"
echo "CREATE TABLE wr(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO wr VALUES('a',1);
ANALYZE wr;" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "without_rowid_analyze_table_name" \
  "SELECT idx FROM sqlite_stat1 WHERE tbl='wr';" \
  "wr" "$DB"
run_test "without_rowid_table_pragma" \
  "PRAGMA index_info(wr);" \
  "0|0|k" "$DB"
run_test_match "without_rowid_query_plan" \
  "EXPLAIN QUERY PLAN SELECT * FROM wr WHERE k='a';" \
  "USING PRIMARY KEY" "$DB"
rm -f "$DB"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
