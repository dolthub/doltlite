#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Clustered PK history / at / blame seek ==="
echo ""

DB=/tmp/test_clustered_pk_pushdown_$$.db; rm -f "$DB"
echo "CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES('a','1'),('b','2'),('c','3');
SELECT dolt_commit('-Am','c1');
UPDATE t SET v='1b' WHERE k='a';
SELECT dolt_commit('-am','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "history_text_pk_eq_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_t WHERE k='a';" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"
run_test "history_text_pk_eq_count" \
  "SELECT count(*) FROM dolt_history_t WHERE k='a';" \
  "2" "$DB"
run_test "history_text_pk_eq_values" \
  "SELECT v FROM dolt_history_t WHERE k='a' ORDER BY commit_date, v;" \
  "1
1b" "$DB"
run_test_match "at_text_pk_eq_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_at_t('HEAD') WHERE k='a';" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"
run_test "at_text_pk_eq_row" \
  "SELECT k || '|' || v FROM dolt_at_t('HEAD') WHERE k='a';" \
  "a|1b" "$DB"
run_test_match "blame_text_pk_eq_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_blame_t WHERE k='a';" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"
run_test "blame_text_pk_eq_count" \
  "SELECT count(*) FROM dolt_blame_t WHERE k='a';" \
  "1" "$DB"
run_test "blame_text_pk_eq_key" \
  "SELECT k FROM dolt_blame_t WHERE k='a';" \
  "a" "$DB"

rm -f "$DB"
echo "CREATE TABLE t(a INT, b TEXT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,'x','old'),(1,'y','keep'),(2,'x','other');
SELECT dolt_commit('-Am','c1');
UPDATE t SET v='new' WHERE a=1 AND b='x';
SELECT dolt_commit('-am','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "history_composite_pk_eq_uses_index" \
  "EXPLAIN QUERY PLAN SELECT * FROM dolt_history_t WHERE a=1 AND b='x';" \
  "VIRTUAL TABLE INDEX [1-9]" "$DB"
run_test "history_composite_pk_eq_count" \
  "SELECT count(*) FROM dolt_history_t WHERE a=1 AND b='x';" \
  "2" "$DB"
run_test "at_composite_pk_eq_row" \
  "SELECT v FROM dolt_at_t('HEAD') WHERE a=1 AND b='x';" \
  "new" "$DB"
run_test "blame_composite_pk_eq_count" \
  "SELECT count(*) FROM dolt_blame_t WHERE a=1 AND b='x';" \
  "1" "$DB"

rm -f "$DB"
echo "CREATE TABLE nc(k TEXT PRIMARY KEY COLLATE NOCASE, v TEXT);
INSERT INTO nc VALUES('Alpha','1');
SELECT dolt_commit('-Am','c1');
CREATE TABLE rt(k TEXT PRIMARY KEY COLLATE RTRIM, v TEXT);
INSERT INTO rt VALUES('beta   ','1');
SELECT dolt_commit('-Am','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "blame_nocase_eq" \
  "SELECT k FROM dolt_blame_nc WHERE k='ALPHA';" \
  "Alpha" "$DB"
run_test "blame_rtrim_eq" \
  "SELECT k FROM dolt_blame_rt WHERE k='beta';" \
  "beta   " "$DB"
run_test "history_nocase_eq" \
  "SELECT count(*) FROM dolt_history_nc WHERE k='ALPHA';" \
  "2" "$DB"
run_test "at_rtrim_eq" \
  "SELECT count(*) FROM dolt_at_rt('HEAD') WHERE k='beta';" \
  "1" "$DB"

rm -f "$DB"
dltest_finish
