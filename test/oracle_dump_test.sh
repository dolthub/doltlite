#!/usr/bin/env bash
set -euo pipefail
DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3-stock}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "$SCRIPT_DIR/assert_stock_reference.sh" "$SQLITE3" "$DOLTLITE"
TMP=$(mktemp -d)
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) TMP=$(cygpath -m "$TMP") ;; esac
trap 'rm -rf "$TMP"' EXIT
pass=0
fail=0
compare() {
  local name="$1" actual="$2" expected="$3"
  if [[ "$actual" == "$expected" ]]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo "FAIL: $name"
    printf 'expected: %s\nactual: %s\n' "$expected" "$actual"
  fi
}
SCHEMA="
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v BLOB,
 g TEXT GENERATED ALWAYS AS (hex(v)) STORED);
CREATE TABLE audit(id INTEGER PRIMARY KEY,v TEXT);
CREATE INDEX tv ON t(v);
CREATE VIEW visible AS SELECT id,typeof(v) AS kind,hex(v) AS value,g FROM t;
CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id,'trigger'); END;
INSERT INTO t(id,v) VALUES(1,NULL),(2,'a''b'),(3,X'00FF'),
 (4,CAST(X'610062' AS TEXT)),(5,char(13,10)),(6,'Unicode: 🐬'),(7,1.2345678901234567);
INSERT INTO t(id,v) VALUES(100,'deleted'); DELETE FROM t WHERE id=100;
PRAGMA user_version=17; PRAGMA application_id=1234;
"
"$DOLTLITE" "$TMP/source" "$SCHEMA SELECT dolt_commit('-Am','base'); SELECT dolt_tag('base'); SELECT dolt_branch('other'); UPDATE t SET v='staged' WHERE id=1; PRAGMA user_version=18; SELECT dolt_add('.'); UPDATE t SET v='working' WHERE id=1; PRAGMA user_version=19;" > /dev/null
source_before=$("$DOLTLITE" "$TMP/source" "SELECT dolt_hashof('HEAD'),active_branch(); SELECT table_name,staged,status FROM dolt_status ORDER BY table_name,staged;")
QUERY="SELECT * FROM visible ORDER BY id; SELECT * FROM audit ORDER BY id; PRAGMA user_version; PRAGMA application_id; PRAGMA integrity_check; SELECT count(*) FROM sqlite_schema WHERE name IN ('tv','tr','visible');"
for ref in WORKING STAGED HEAD base other; do
  expected="$TMP/expected-$ref.sqlite"
  "$SQLITE3" "$expected" "$SCHEMA"
  case "$ref" in
    WORKING) "$SQLITE3" "$expected" "UPDATE t SET v='working' WHERE id=1; PRAGMA user_version=19;" ;;
    STAGED) "$SQLITE3" "$expected" "UPDATE t SET v='staged' WHERE id=1; PRAGMA user_version=18;" ;;
  esac
  expected_rows=$("$SQLITE3" "$expected" "$QUERY")
  for format in sql sqlite; do
    path="$TMP/$ref.$format"
    "$DOLTLITE" "$TMP/source" "SELECT dolt_dump('--file-name','$path','--result-format','$format','--ref','$ref');" > /dev/null
    if [[ "$format" == sql ]]; then
      output="$TMP/$ref-from-sql.sqlite"
      "$SQLITE3" "$output" < "$path"
    else
      output="$path"
    fi
    actual=$("$SQLITE3" "$output" "$QUERY")
    compare "$ref/$format schema, values, triggers and metadata" "$actual" "$expected_rows"
    compare "$ref/$format no history" "$("$SQLITE3" "$output" "SELECT count(*) FROM sqlite_schema WHERE name GLOB 'dolt_*'")" 0
    compare "$ref/$format AUTOINCREMENT" "$("$SQLITE3" "$output" "INSERT INTO t(v) VALUES('next'); SELECT max(id) FROM t;")" 101
    compare "$ref/$format source unchanged" "$("$DOLTLITE" "$TMP/source" "SELECT dolt_hashof('HEAD'),active_branch(); SELECT table_name,staged,status FROM dolt_status ORDER BY table_name,staged;")" "$source_before"
  done
done

"$DOLTLITE" "$TMP/fts" "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('export hello'),('second row');" > /dev/null
for format in sql sqlite; do
  path="$TMP/fts.$format"
  "$DOLTLITE" "$TMP/fts" "SELECT dolt_dump('--file-name','$path','-r','$format')" > /dev/null
  output="$path"
  if [[ "$format" == sql ]]; then
    output="$TMP/fts-replayed.sqlite"
    "$SQLITE3" "$output" < "$path"
  fi
  compare "FTS5 $format stock query" "$("$SQLITE3" "$output" "SELECT rowid,body FROM ft WHERE ft MATCH 'hello';")" '1|export hello'
  "$SQLITE3" "$output" "INSERT INTO ft(ft) VALUES('integrity-check');"
done

"$DOLTLITE" "$TMP/tok" "CREATE VIRTUAL TABLE tk USING fts3tokenize(simple);
CREATE VIRTUAL TABLE tksep USING fts3tokenize(simple, '', 'xyz ');" > /dev/null
for format in sql sqlite; do
  path="$TMP/tok.$format"
  "$DOLTLITE" "$TMP/tok" "SELECT dolt_dump('--file-name','$path','-r','$format')" > /dev/null
  output="$path"
  if [[ "$format" == sql ]]; then
    output="$TMP/tok-replayed-$format.sqlite"
    "$SQLITE3" "$output" < "$path"
  fi
  compare "fts3tokenize $format simple" \
    "$("$SQLITE3" "$output" "SELECT group_concat(token, ' ') FROM (SELECT token FROM tk WHERE input='hello world' ORDER BY position);")" \
    "hello world"
  compare "fts3tokenize $format separators" \
    "$("$SQLITE3" "$output" "SELECT group_concat(token, ' ') FROM (SELECT token FROM tksep WHERE input='1x2x3x' ORDER BY position);")" \
    "1 2 3"
done

"$DOLTLITE" "$TMP/rebase" > /dev/null <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(0);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(1);
SELECT dolt_commit('-am','f1');
INSERT INTO t VALUES(2);
SELECT dolt_commit('-am','f2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10);
SELECT dolt_commit('-am','main');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i','main');
SQL
plan_query='SELECT rebase_order,action,commit_hash,commit_message FROM dolt_rebase ORDER BY rebase_order;'
plan=$("$DOLTLITE" "$TMP/rebase/dolt_rebase_feat" "$plan_query")
for mode in all plan rowids; do
  case "$mode" in
    all) command='.dump' ;;
    plan) command='.dump dolt_rebase' ;;
    rowids) command='.dump --preserve-rowids dolt_rebase' ;;
  esac
  "$DOLTLITE" "$TMP/rebase/dolt_rebase_feat" "$command" > "$TMP/rebase-$mode.sql"
  "$SQLITE3" "$TMP/rebase-$mode.sqlite" < "$TMP/rebase-$mode.sql"
  compare "mid-rebase $mode plan" "$("$SQLITE3" "$TMP/rebase-$mode.sqlite" "$plan_query")" "$plan"
  compare "mid-rebase $mode integrity" "$("$SQLITE3" "$TMP/rebase-$mode.sqlite" 'PRAGMA integrity_check')" ok
  rejected=0
  if ! "$SQLITE3" "$TMP/rebase-$mode.sqlite" "UPDATE dolt_rebase SET action='bogus'" > /dev/null 2>&1; then
    rejected=1
  fi
  compare "mid-rebase $mode action constraint" "$rejected" 1
done
compare "mid-rebase source unchanged" "$("$DOLTLITE" "$TMP/rebase/dolt_rebase_feat" "$plan_query")" "$plan"

echo "Results: $pass passed, $fail failed"
echo __SUITE_COMPLETE__
[[ "$fail" == 0 ]]
