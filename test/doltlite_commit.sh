#!/bin/bash
DOLTLITE="${1:-./doltlite}"
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

# Named dolt_add stages the table's indexes; -am must keep index roots paired across catalog domains.
DB9=/tmp/test_dolt_stageidx_$$.db; rm -f "$DB9"

run_test_match "staged_index_named_add_setup"   "CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO a VALUES(1,10);
CREATE INDEX av ON a(v);
SELECT dolt_commit('-Am','c1');"   "^[0-9a-f]{40}$" "$DB9"

run_test_match "staged_index_named_add_commit"   "CREATE TABLE b(id INTEGER PRIMARY KEY, w INTEGER);
INSERT INTO b VALUES(1,5);
CREATE INDEX bw ON b(w);
SELECT dolt_add('b');
UPDATE a SET v=11;
SELECT dolt_commit('-am','mix');"   "^[0-9a-f]{40}$" "$DB9"

run_test "staged_index_survives_reset"   "SELECT dolt_reset('--hard');
SELECT group_concat(v) FROM a INDEXED BY av;
SELECT group_concat(w) FROM b INDEXED BY bw;"   "0
11
5" "$DB9"

run_test "staged_index_integrity"   "PRAGMA integrity_check;"   "ok" "$DB9"

DB10=/tmp/test_dolt_stagedrop_$$.db; rm -f "$DB10"

run_test_match "staged_drop_index_setup"   "CREATE TABLE a(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO a VALUES(1,10);
CREATE INDEX av ON a(v);
SELECT dolt_commit('-Am','c1');"   "^[0-9a-f]{40}$" "$DB10"

run_test_match "staged_drop_index_commit"   "DROP TABLE a;
SELECT dolt_add('a');
SELECT dolt_commit('-m','dropped');"   "^[0-9a-f]{40}$" "$DB10"

run_test "staged_drop_no_orphan_entries"   "SELECT dolt_reset('--hard');
SELECT count(*) FROM sqlite_master;
PRAGMA integrity_check;"   "0
0
ok" "$DB10"

# Named add must not adopt unstaged views/triggers; -A and -am may.
DB11=/tmp/test_dolt_viewstage_$$.db; rm -f "$DB11"

run_test_match "view_stage_setup"   "CREATE TABLE t(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','base');"   "^[0-9a-f]{40}$" "$DB11"

run_test_match "view_stage_named_commit"   "ALTER TABLE t ADD COLUMN c INTEGER;
CREATE VIEW vv AS SELECT a FROM t;
CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END;
SELECT dolt_add('t');
SELECT dolt_commit('-m','only t');"   "^[0-9a-f]{40}$" "$DB11"

run_test "view_stays_out_of_named_commit"   "SELECT count(*) FROM sqlite_master WHERE type IN ('view','trigger');
SELECT dolt_reset('--hard');
SELECT count(*) FROM sqlite_master WHERE type IN ('view','trigger');
SELECT count(*) FROM pragma_table_info('t');"   "2
0
0
2" "$DB11"

run_test_match "view_rides_with_am"   "CREATE VIEW v2 AS SELECT a FROM t; INSERT INTO t(a) VALUES(1);
SELECT dolt_commit('-am','am');"   "^[0-9a-f]{40}$" "$DB11"

run_test "view_in_am_commit"   "SELECT dolt_reset('--hard');
SELECT name FROM sqlite_master WHERE type='view';"   "0
v2" "$DB11"

# Named add must not adopt other tables' unstaged schema; staged index roots must survive.
DB12=/tmp/test_dolt_namedscope_$$.db; rm -f "$DB12"

run_test_match "named_scope_setup"   "CREATE TABLE p(id INTEGER PRIMARY KEY, v INT);
INSERT INTO p VALUES(1,1);
SELECT dolt_commit('-Am','base');"   "^[0-9a-f]{40}$" "$DB12"

run_test_match "named_scope_commit"   "ALTER TABLE p ADD COLUMN c INT;
CREATE TABLE q(id INTEGER PRIMARY KEY);
SELECT dolt_add('q');
SELECT dolt_commit('-m','only q');"   "^[0-9a-f]{40}$" "$DB12"

run_test "unstaged_alter_stays_out"   "SELECT dolt_reset('--hard');
SELECT count(*) FROM pragma_table_info('p');
SELECT count(*) FROM q;
PRAGMA integrity_check;"   "0
2
0
ok" "$DB12"

DB13=/tmp/test_dolt_stagedidx_$$.db; rm -f "$DB13"

run_test_match "staged_index_collision_setup"   "CREATE TABLE p(id INTEGER PRIMARY KEY, v INT);
INSERT INTO p VALUES(1,1);
CREATE INDEX ip ON p(v);
SELECT dolt_commit('-Am','base');"   "^[0-9a-f]{40}$" "$DB13"

run_test_match "staged_index_collision_commit"   "UPDATE p SET v=2;
SELECT dolt_add('-A');
DROP INDEX ip;
CREATE TABLE q(id INTEGER PRIMARY KEY);
SELECT dolt_add('q');
SELECT dolt_commit('-m','mix');"   "^[0-9a-f]{40}$" "$DB13"

run_test "staged_index_survives_unstaged_drop"   "SELECT dolt_reset('--hard');
SELECT v FROM p INDEXED BY ip;
SELECT count(*) FROM q;
PRAGMA integrity_check;"   "0
2
0
ok" "$DB13"

DB14=/tmp/test_dolt_staged_untracked_$$.db; rm -f "$DB14"

run_test_match "staged_untracked_setup"   "CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO kv VALUES(1,'base');
SELECT dolt_commit('-Am','base');"   "^[0-9a-f]{40}$" "$DB14"

run_test_match "staged_untracked_commit"   "CREATE TABLE aux(id INTEGER PRIMARY KEY);
INSERT INTO kv VALUES(2,'next');
SELECT dolt_add('kv');
SELECT dolt_commit('-m','staged');"   "[0-9a-f]{40}$" "$DB14"

run_test "staged_untracked_head_reopens"   "SELECT dolt_reset('--hard');
SELECT group_concat(id) FROM kv;
SELECT group_concat(name,'|') FROM (
  SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name
);
PRAGMA integrity_check;"   "0
1,2
aux|kv
ok" "$DB14"

DB15=/tmp/test_dolt_author_$$.db; rm -f "$DB15"

run_test_match "author_validation_setup"   "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','base');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-Am','bad','--author','not-an-author');" \
  "Author not formatted correctly" "$DB15"

run_test "malformed_author_does_not_commit" \
  "SELECT count(*) FROM dolt_log WHERE message='bad';" "0" "$DB15"

run_test_match "empty_author_rejected" \
  "SELECT dolt_commit('-m','bad','--author','');" \
  "Option 'author' requires a value" "$DB15"

run_test_match "empty_author_email_rejected" \
  "SELECT dolt_commit('-Am','empty email','--author','Edge Case <>');" \
  "Aborting commit due to empty author email" "$DB15"

run_test "empty_author_email_does_not_commit" \
  "SELECT count(*) FROM dolt_log WHERE message='empty email';" "0" "$DB15"

run_test_match "author_without_closing_bracket" \
  "SELECT dolt_commit('-Am','missing close','--author','John Doe <john@example.com');" \
  "^[0-9a-f]{40}$" "$DB15"

run_test "author_without_closing_bracket_fields" \
  "SELECT committer || '|' || email FROM dolt_log WHERE message='missing close';" \
  "John Doe|john@example.com" "$DB15"

run_test_match "author_removes_closing_brackets" \
  "INSERT INTO t VALUES(3);
SELECT dolt_commit('-Am','brackets','--author','Angle <a>b>');" \
  "^[0-9a-f]{40}$" "$DB15"

run_test "author_removes_closing_brackets_fields" \
  "SELECT committer || '|' || email FROM dolt_log WHERE message='brackets';" \
  "Angle|ab" "$DB15"

DB16=/tmp/test_dolt_nul_args_$$.db; rm -f "$DB16"

run_test_match "nul_argument_setup" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','base');
INSERT INTO t VALUES(2);" \
  "^[0-9a-f]{40}$" "$DB16"

run_test_match "nul_positional_rejected" \
  "SELECT dolt_branch('visible' || char(0) || 'hidden');" \
  "command arguments may not contain NUL bytes" "$DB16"

run_test "nul_positional_does_not_create_prefix_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='visible';" \
  "0" "$DB16"

run_test_match "nul_table_argument_rejected" \
  "SELECT dolt_add('t' || char(0) || 'hidden');" \
  "command arguments may not contain NUL bytes" "$DB16"

run_test "nul_table_argument_does_not_stage_prefix" \
  "SELECT staged FROM dolt_status WHERE table_name='t';" \
  "0" "$DB16"

run_test_match "nul_detached_option_value_rejected" \
  "SELECT dolt_commit('-A','-m','visible' || char(0) || 'hidden');" \
  "command arguments may not contain NUL bytes" "$DB16"

run_test_match "nul_attached_option_value_rejected" \
  "SELECT dolt_commit('-A','-mvisible' || char(0) || 'hidden');" \
  "command arguments may not contain NUL bytes" "$DB16"

run_test "nul_option_values_do_not_commit_or_stage" \
  "SELECT count(*) FROM dolt_log WHERE message='visible';
SELECT staged FROM dolt_status WHERE table_name='t';" \
  "0
0" "$DB16"

rm -f "$DB" "$DB2" "$DB3" "$DB4" "$DB5" "$DB6" "$DB7" "$DB8" "$DB9" "$DB10" "$DB11" "$DB12" "$DB13" "$DB14" "$DB15" "$DB16"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
