#!/bin/bash
#
# Tests for ATTACH standard SQLite databases (#181)
#
DOLTLITE=./doltlite
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
PASS=0; FAIL=0; ERRORS=""

run_test() {
  local n="$1" s="$2" e="$3" d="$4"
  local r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1)
  if [ "$r" = "$e" ]; then PASS=$((PASS+1))
  else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi
}

echo "=== ATTACH SQLite Database Tests ==="
echo ""

if [ ! -x "$SQLITE3" ]; then
  echo "SKIP: sqlite3 not found"
  exit 0
fi

# ============================================================
# Setup: create standard SQLite databases
# ============================================================

SQLDB1=/tmp/test_attach1_$$.db
SQLDB2=/tmp/test_attach2_$$.db
DLDB=/tmp/test_attach_dl_$$.db
rm -f "$SQLDB1" "$SQLDB2" "$DLDB"

$SQLITE3 "$SQLDB1" "
CREATE TABLE events(id INTEGER PRIMARY KEY, type TEXT, data TEXT);
INSERT INTO events VALUES(1,'click','btn1');
INSERT INTO events VALUES(2,'view','page1');
INSERT INTO events VALUES(3,'click','btn2');
CREATE TABLE users(uid INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES(10,'Alice');
INSERT INTO users VALUES(20,'Bob');
"

$SQLITE3 "$SQLDB2" "
CREATE TABLE logs(ts INTEGER PRIMARY KEY, msg TEXT);
INSERT INTO logs VALUES(1000,'start');
INSERT INTO logs VALUES(2000,'running');
INSERT INTO logs VALUES(3000,'done');
"

# ============================================================
# Basic ATTACH and SELECT
# ============================================================

run_test "attach_select_all" \
  "ATTACH DATABASE '$SQLDB1' AS ops;
SELECT * FROM ops.events ORDER BY id;" \
  "1|click|btn1
2|view|page1
3|click|btn2" ":memory:"

run_test "attach_select_where" \
  "ATTACH DATABASE '$SQLDB1' AS ops;
SELECT type, data FROM ops.events WHERE type='click' ORDER BY id;" \
  "click|btn1
click|btn2" ":memory:"

run_test "attach_count" \
  "ATTACH DATABASE '$SQLDB1' AS ops;
SELECT count(*) FROM ops.events;" \
  "3" ":memory:"

run_test "attach_max" \
  "ATTACH DATABASE '$SQLDB1' AS ops;
SELECT MAX(id) FROM ops.events;" \
  "3" ":memory:"

# ============================================================
# Multiple tables in attached db
# ============================================================

run_test "attach_multiple_tables" \
  "ATTACH DATABASE '$SQLDB1' AS ops;
SELECT count(*) FROM ops.events;
SELECT count(*) FROM ops.users;" \
  "3
2" ":memory:"

# ============================================================
# Cross-database JOIN
# ============================================================

run_test "cross_db_join" \
  "CREATE TABLE threads(id INTEGER PRIMARY KEY, title TEXT);
INSERT INTO threads VALUES(1,'Thread A');
INSERT INTO threads VALUES(2,'Thread B');
ATTACH DATABASE '$SQLDB1' AS ops;
SELECT t.title, e.type FROM threads t JOIN ops.events e ON t.id=e.id ORDER BY t.id;" \
  "Thread A|click
Thread B|view" ":memory:"

# ============================================================
# Multiple ATTACH
# ============================================================

run_test "attach_two_sqlite_dbs" \
  "ATTACH DATABASE '$SQLDB1' AS db1;
ATTACH DATABASE '$SQLDB2' AS db2;
SELECT count(*) FROM db1.events;
SELECT count(*) FROM db2.logs;" \
  "3
3" ":memory:"

# ============================================================
# Main db queries still work after ATTACH
# ============================================================

run_test "main_db_after_attach" \
  "CREATE TABLE t(x INTEGER);
INSERT INTO t VALUES(42);
ATTACH DATABASE '$SQLDB1' AS ops;
SELECT x FROM t;
SELECT count(*) FROM ops.events;" \
  "42
3" ":memory:"

# ============================================================
# ATTACH with doltlite versioning on main db
# ============================================================

run_test "dolt_commit_with_attach" \
  "CREATE TABLE t(x INTEGER);
INSERT INTO t VALUES(99);
SELECT dolt_commit('-am','init') IS NOT NULL;
ATTACH DATABASE '$SQLDB1' AS ops;
SELECT x FROM t;
SELECT count(*) FROM ops.events;" \
  "1
99
3" "$DLDB"

# ============================================================
# Write to attached SQLite database
# ============================================================

SQLDB_W=/tmp/test_attach_write_$$.db
$SQLITE3 "$SQLDB_W" "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);"

run_test "attach_write" \
  "ATTACH DATABASE '$SQLDB_W' AS w;
INSERT INTO w.t VALUES(1,'hello');
INSERT INTO w.t VALUES(2,'world');
SELECT * FROM w.t ORDER BY id;" \
  "1|hello
2|world" ":memory:"

# Verify writes persisted by reading with sqlite3
WRITE_CHECK=$($SQLITE3 "$SQLDB_W" "SELECT count(*) FROM t;")
if [ "$WRITE_CHECK" = "2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: attach_write_persisted\n  expected: 2\n  got:      $WRITE_CHECK"
fi

# ============================================================
# Cleanup
# ============================================================

rm -f "$SQLDB1" "$SQLDB2" "$DLDB" "$SQLDB_W"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
echo "All tests passed!"
