#!/bin/bash
DOLTLITE=./doltlite
PASS=0
FAIL=0
ERRORS=""

run_sql() {
  if [ "${DLTEST_STRIP_CR:-0}" = "1" ]; then
    echo "$1" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$2" 2>&1 | tr -d '\r'
  else
    echo "$1" | perl -e 'alarm(10); exec @ARGV' $DOLTLITE "$2" 2>&1
  fi
}

run_test() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local db="${4:-:memory:}"
  local result
  result=$(run_sql "$sql" "$db")
  local exit_code=$?
  if [ $exit_code -eq 137 ] || [ $exit_code -eq 139 ]; then
    result="CRASH (exit $exit_code)"
  fi
  if [ "$result" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $result"
  fi
}

run_test_match() {
  local name="$1"
  local sql="$2"
  local pattern="$3"
  local db="${4:-:memory:}"
  local result
  result=$(run_sql "$sql" "$db")
  if echo "$result" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $result"
  fi
}

echo "=== Doltlite Commit & Log Tests ==="
echo ""

DB=/tmp/test_dolt_commit_$$.db
rm -f "$DB"

run_test_match "commit_returns_hash" \
  "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A', '-m', 'init');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "commit_requires_message" \
  "SELECT dolt_commit();" \
  "Error near line 1: dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')" "$DB"

run_test_match "log_shows_commit" \
  "SELECT message FROM dolt_log;" \
  "init" "$DB"

run_test_match "log_has_committer" \
  "SELECT committer FROM dolt_log;" \
  "doltlite" "$DB"

run_test_match "log_has_hash" \
  "SELECT commit_hash FROM dolt_log;" \
  "^[0-9a-f]{40}$" "$DB"

run_test_match "second_commit" \
  "INSERT INTO t VALUES(2); SELECT dolt_commit('-A', '-m', 'add row 2');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "log_count_two" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB"

run_test_match "log_order" \
  "SELECT message FROM dolt_log;" \
  "add row 2" "$DB"

run_test_match "commit_with_author" \
  "INSERT INTO t VALUES(3); SELECT dolt_commit('-A', '-m', 'add 3', '--author', 'Alice <alice@test.com>');" \
  "^[0-9a-f]{40}$" "$DB"

run_test "author_name" \
  "SELECT committer FROM dolt_log LIMIT 1;" \
  "Alice" "$DB"

run_test "author_email" \
  "SELECT email FROM dolt_log LIMIT 1;" \
  "alice@test.com" "$DB"

run_test "log_count_three" \
  "SELECT count(*) FROM dolt_log;" \
  "4" "$DB"

DB2=/tmp/test_dolt_persist_$$.db
rm -f "$DB2"

echo "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO items VALUES(1,'hat'),(2,'coat'); SELECT dolt_commit('-A', '-m', 'create items');" | $DOLTLITE "$DB2" > /dev/null 2>&1

run_test "persist_data" \
  "SELECT name FROM items ORDER BY id;" \
  "hat
coat" "$DB2"

run_test "persist_log" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "create items" "$DB2"

run_test_match "commit_after_alter" \
  "ALTER TABLE items ADD COLUMN price REAL DEFAULT 0; SELECT dolt_commit('-A', '-m', 'add price column');" \
  "^[0-9a-f]{40}$" "$DB2"

run_test "log_after_alter" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB2"

DB7=/tmp/test_dolt_commit_rename_reopen_$$.db
rm -f "$DB7"

echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES(1,'base');
SELECT dolt_commit('-A','-m','init');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES(2,'x');
SELECT dolt_add('b');
SELECT dolt_commit('-m','rename and edit');" | $DOLTLITE "$DB7" > /dev/null 2>&1

run_test "reopen_rename_commit_status_clean" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB7"

run_test "reopen_rename_commit_schema" \
  "SELECT group_concat(name || ':' || lower(type), '|') FROM pragma_table_info('b');" \
  "id:integer|s:text" "$DB7"

run_test "reopen_rename_commit_rows" \
  "SELECT s FROM b ORDER BY id;" \
  "base
x" "$DB7"

DB8=/tmp/test_dolt_commit_recreate_reopen_$$.db
rm -f "$DB8"

echo "CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES(1,'base');
SELECT dolt_commit('-A','-m','init');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES(7,70);
SELECT dolt_add('a');
SELECT dolt_commit('-m','recreate a');" | $DOLTLITE "$DB8" > /dev/null 2>&1

run_test "reopen_recreate_commit_status_clean" \
  "SELECT count(*) FROM dolt_status;" \
  "0" "$DB8"

run_test "reopen_recreate_commit_schema" \
  "SELECT group_concat(name || ':' || lower(type), '|') FROM pragma_table_info('a');" \
  "k:integer|n:integer" "$DB8"

run_test "reopen_recreate_commit_rows" \
  "SELECT k || '|' || n FROM a;" \
  "7|70" "$DB8"

DB9=/tmp/test_dolt_commit_schema_only_$$.db
rm -f "$DB9"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','init');
ALTER TABLE t ADD COLUMN n INTEGER;
SELECT dolt_add('t');
INSERT INTO t VALUES(2,'x',2);
SELECT dolt_commit('-m','schema only staged');" | $DOLTLITE "$DB9" > /dev/null 2>&1

run_test "reopen_schema_only_commit_status_preserves_unstaged" \
  "SELECT table_name || '|' || staged || '|' || status FROM dolt_status ORDER BY table_name, staged, status;" \
  "t|0|modified" "$DB9"

run_test "reopen_schema_only_commit_schema" \
  "SELECT group_concat(name || ':' || lower(type), '|') FROM pragma_table_info('t');" \
  "id:integer|s:text|n:integer" "$DB9"

run_test "reopen_schema_only_commit_rows" \
  "SELECT id || '|' || s || '|' || coalesce(n,'NULL') FROM t ORDER BY id;" \
  "1|base|NULL
2|x|2" "$DB9"

run_test "empty_log" \
  "SELECT count(*) FROM dolt_log;" \
  "1" ":memory:"

DB3=/tmp/test_dolt_nochange_$$.db
rm -f "$DB3"

echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A', '-m', 'first');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test "commit_no_changes" \
  "SELECT dolt_commit('-m', 'no changes');" \
  "Error near line 1: nothing to commit, working tree clean (use dolt_add to stage changes)" "$DB3"

run_test "one_commit_after_no_change" \
  "SELECT count(*) FROM dolt_log;" \
  "2" "$DB3"

DB4=/tmp/test_dolt_multi_$$.db
rm -f "$DB4"

run_test_match "multi_table_commit" \
  "CREATE TABLE a(x); CREATE TABLE b(y); INSERT INTO a VALUES(1); INSERT INTO b VALUES(2); SELECT dolt_commit('-A', '-m', 'two tables');" \
  "^[0-9a-f]{40}$" "$DB4"

run_test "multi_table_data_a" \
  "SELECT * FROM a;" \
  "1" "$DB4"

run_test "multi_table_data_b" \
  "SELECT * FROM b;" \
  "2" "$DB4"

run_test "log_column_count" \
  "SELECT count(*) FROM pragma_table_info('dolt_log');" \
  "5" ":memory:"

run_test_match "log_select_columns" \
  "SELECT commit_hash, committer, email, date, message FROM dolt_log LIMIT 1;" \
  "." "$DB"

DB5=/tmp/test_dolt_compound_$$.db; rm -f "$DB5"

echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','seed');" | $DOLTLITE "$DB5" > /dev/null 2>&1

run_test_match "compound_am" \
  "INSERT INTO t VALUES(2,'a');
SELECT dolt_commit('-am','compound flag commit');" \
  "^[0-9a-f]{40}$" "$DB5"

run_test "compound_am_message" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "compound flag commit" "$DB5"

run_test "compound_am_data" \
  "SELECT v FROM t WHERE id=2;" \
  "a" "$DB5"

DB6=/tmp/test_dolt_compound2_$$.db; rm -f "$DB6"

run_test_match "compound_Am" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
SELECT dolt_commit('-Am','uppercase A compound');" \
  "^[0-9a-f]{40}$" "$DB6"

run_test "compound_Am_message" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "uppercase A compound" "$DB6"

DB7=/tmp/test_dolt_compound3_$$.db; rm -f "$DB7"

run_test_match "compound_ma_with_add" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_add('-A');
SELECT dolt_commit('-ma');" \
  "^[0-9a-f]{40}$" "$DB7"

run_test "compound_ma_message_is_a" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "a" "$DB7"

DB8=/tmp/test_dolt_compound4_$$.db; rm -f "$DB8"

run_test_match "compound_am_multi_1" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','first');" \
  "^[0-9a-f]{40}$" "$DB8"

run_test_match "compound_am_multi_2" \
  "INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-am','second');" \
  "^[0-9a-f]{40}$" "$DB8"

run_test "compound_am_multi_count" \
  "SELECT count(*) FROM dolt_log;" \
  "3" "$DB8"

run_test "compound_am_multi_data" \
  "SELECT count(*) FROM t;" \
  "2" "$DB8"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
