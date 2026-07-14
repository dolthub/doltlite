#!/bin/bash
#
# Catalog numbering is allocated at CREATE and never changes: sqlite_master
# rowids, rootpages, and SQL text must be identical before dolt_commit,
# after it (same session), and after reopen. Before verbatim persistence the
# serializer renumbered at commit, so the live view flipped to canonical
# order at dolt_commit and reloads scrambled session state (fts shadow
# tables were the visible casualty -- e_fts3/fts3malloc aborts).

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0
fail=0
FAILED_NAMES=""

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected: $expected"
    echo "    actual:   $actual"
  fi
}

DB="$TMPROOT/db"
MASTER_Q="SELECT rowid||'|'||name||'|'||rootpage FROM sqlite_master;"

echo "--- master view survives dolt_commit and reopen ---"

PRE=$("$DOLTLITE" "$DB" 2>/dev/null <<EOF
CREATE TABLE zebra(z INTEGER PRIMARY KEY, v TEXT);
CREATE VIRTUAL TABLE ft USING fts3(x);
CREATE TABLE apple(a INTEGER PRIMARY KEY);
INSERT INTO ft VALUES ('hello world');
.mode list
$MASTER_Q
EOF
)
POST=$("$DOLTLITE" "$DB" 2>/dev/null <<EOF
.mode list
SELECT CASE WHEN dolt_commit('-A','-m','c1') IS NOT NULL THEN '' END;
$MASTER_Q
EOF
)
REOPEN=$("$DOLTLITE" "$DB" ".mode list" "$MASTER_Q" 2>/dev/null)

check "pre_nonempty" "6" "$(printf '%s\n' "$PRE" | grep -c '|')"
check "post_matches_pre" "$PRE" "$(printf '%s\n' "$POST" | grep '|')"
check "reopen_matches_pre" "$PRE" "$REOPEN"

echo "--- same-session commit keeps view (single connection) ---"

ONECONN=$("$DOLTLITE" "$TMPROOT/one" 2>/dev/null <<'EOF'
CREATE TABLE zebra(z INTEGER PRIMARY KEY);
CREATE VIRTUAL TABLE ft USING fts3(x);
.mode list
SELECT 'A:'||rowid||'|'||name FROM sqlite_master;
SELECT CASE WHEN dolt_commit('-A','-m','c1') IS NOT NULL THEN '' END;
SELECT 'B:'||rowid||'|'||name FROM sqlite_master;
EOF
)
A=$(printf '%s\n' "$ONECONN" | grep '^A:' | sed 's/^A://')
B=$(printf '%s\n' "$ONECONN" | grep '^B:' | sed 's/^B://')
check "same_session_stable" "$A" "$B"

echo "--- SQL text is preserved verbatim across reopen ---"

"$DOLTLITE" "$TMPROOT/sql" "CREATE TABLE t(a INTEGER PRIMARY KEY,   b   TEXT  )" \
  "SELECT CASE WHEN dolt_commit('-A','-m','c') IS NOT NULL THEN '' END" >/dev/null 2>&1
SQLTXT=$("$DOLTLITE" "$TMPROOT/sql" "SELECT sql FROM sqlite_master WHERE name='t'" 2>/dev/null)
check "sql_verbatim" "CREATE TABLE t(a INTEGER PRIMARY KEY,   b   TEXT  )" "$SQLTXT"

echo "--- rolled-back DDL leaves view and cleanliness intact ---"

RB=$("$DOLTLITE" "$DB" 2>/dev/null <<EOF
.mode list
BEGIN;
CREATE TABLE scratch(x);
ROLLBACK;
$MASTER_Q
SELECT 'STATUS:'||count(*) FROM dolt_status;
EOF
)
check "rollback_view" "$PRE" "$(printf '%s\n' "$RB" | grep -v '^STATUS:')"
check "rollback_clean" "STATUS:0" "$(printf '%s\n' "$RB" | grep '^STATUS:')"

echo "--- ALTER through commit and reopen stays clean ---"

ALT=$("$DOLTLITE" "$TMPROOT/alt" 2>/dev/null <<'EOF'
.mode list
CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
INSERT INTO t VALUES (1,'x');
SELECT 'C1:'||(dolt_commit('-A','-m','c1') IS NOT NULL);
ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;
SELECT 'C2:'||(dolt_commit('-A','-m','c2') IS NOT NULL);
SELECT 'S:'||count(*) FROM dolt_status;
EOF
)
check "alter_commits" "C1:1 C2:1" "$(printf '%s\n' "$ALT" | grep '^C' | tr '\n' ' ' | sed 's/ $//')"
check "alter_clean" "S:0" "$(printf '%s\n' "$ALT" | grep '^S:')"
ALT2=$("$DOLTLITE" "$TMPROOT/alt" 2>/dev/null <<'EOF'
.mode list
SELECT 'S:'||count(*) FROM dolt_status;
INSERT INTO t VALUES (2,'y',8);
SELECT 'C3:'||(dolt_commit('-A','-m','c3') IS NOT NULL);
SELECT 'S:'||count(*) FROM dolt_status;
EOF
)
check "alter_reopen_clean" "S:0 C3:1 S:0" "$(printf '%s\n' "$ALT2" | tr '\n' ' ' | sed 's/ $//')"

echo ""
echo "Results: $pass passed, $fail failed out of $((pass+fail)) tests"
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
