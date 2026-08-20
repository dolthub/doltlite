#!/bin/bash
#
# The live session and the persisted catalog must never diverge: after any
# schema-changing commit the connection adopts the canonical catalog form
# (sorted rows, positional numbering), so sqlite_master reads identically
# in-session, after dolt_commit, and after reopen -- and the canonical form
# makes the catalog hash independent of DDL construction order. Before this
# held, reloads scrambled rowid bindings mid-connection (fts shadow tables
# were the visible casualty) and OOM-retry harnesses saw phantom DDL.

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

echo "--- master view identical in-session, after commit, and after reopen ---"

POST=$("$DOLTLITE" "$DB" 2>/dev/null <<EOF
CREATE TABLE zebra(z INTEGER PRIMARY KEY, v TEXT);
CREATE VIRTUAL TABLE ft USING fts3(x);
CREATE TABLE apple(a INTEGER PRIMARY KEY);
INSERT INTO ft VALUES ('hello world');
.mode list
$MASTER_Q
EOF
)
COMMITTED=$("$DOLTLITE" "$DB" 2>/dev/null <<EOF
.mode list
SELECT CASE WHEN dolt_commit('-A','-m','c1') IS NOT NULL THEN '' END;
$MASTER_Q
EOF
)
REOPEN=$("$DOLTLITE" "$DB" ".mode list" "$MASTER_Q" 2>/dev/null)

check "post_nonempty" "6" "$(printf '%s\n' "$POST" | grep -c '|')"
check "committed_matches_post" "$POST" "$(printf '%s\n' "$COMMITTED" | grep '|')"
check "reopen_matches_post" "$POST" "$REOPEN"

echo "--- same-session dolt_commit keeps the adopted view (single connection) ---"

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

echo "--- SQL text identical in-session and across reopen ---"

INSESSION_SQL=$("$DOLTLITE" "$TMPROOT/sql" 2>/dev/null <<'EOF'
CREATE TABLE t(a INTEGER PRIMARY KEY,   b   TEXT  );
SELECT CASE WHEN dolt_commit('-A','-m','c') IS NOT NULL THEN '' END;
SELECT sql FROM sqlite_master WHERE name='t';
EOF
)
REOPEN_SQL=$("$DOLTLITE" "$TMPROOT/sql" "SELECT sql FROM sqlite_master WHERE name='t'" 2>/dev/null)
check "sql_stable_across_reopen" "$(printf '%s\n' "$INSESSION_SQL" | grep CREATE)" "$REOPEN_SQL"

echo "--- comment markers inside quoted identifiers remain schema text ---"

QUOTED=$("$DOLTLITE" "$TMPROOT/quoted" 2>/dev/null <<'EOF'
CREATE TABLE q(
  `back--tick` TEXT,
  [bracket/*name] INTEGER,
  "double""--quote" TEXT
);
CREATE INDEX `index--name` ON q(`back--tick`);
CREATE VIEW `view--name` AS
 SELECT `back--tick`, [bracket/*name] FROM q;
CREATE TRIGGER `trigger--name` AFTER INSERT ON q BEGIN
  UPDATE q SET `back--tick`=NEW.`back--tick`
   WHERE [bracket/*name]=NEW.[bracket/*name];
END;
INSERT INTO q VALUES('value',1,'quoted');
SELECT 'C:'||(dolt_commit('-A','-m','quoted') IS NOT NULL);
SELECT 'Q:'||count(*) FROM pragma_table_xinfo('q')
 WHERE name IN ('back--tick','bracket/*name','double"--quote');
SELECT 'O:'||count(*) FROM sqlite_master
 WHERE name IN ('index--name','view--name','trigger--name');
SELECT 'V:'||count(*) FROM `view--name` WHERE `back--tick`='value';
SELECT 'K:'||integrity_check FROM pragma_integrity_check;
EOF
)
check "quoted_comment_markers_in_session" "C:1 Q:3 O:3 V:1 K:ok" \
  "$(printf '%s\n' "$QUOTED" | tr '\n' ' ' | sed 's/ $//')"
QUOTED_REOPEN=$("$DOLTLITE" "$TMPROOT/quoted" 2>/dev/null <<'EOF'
SELECT 'Q:'||count(*) FROM pragma_table_xinfo('q')
 WHERE name IN ('back--tick','bracket/*name','double"--quote');
SELECT 'O:'||count(*) FROM sqlite_master
 WHERE name IN ('index--name','view--name','trigger--name');
SELECT 'V:'||count(*) FROM `view--name` WHERE `back--tick`='value';
SELECT 'K:'||integrity_check FROM pragma_integrity_check;
EOF
)
check "quoted_comment_markers_after_reopen" "Q:3 O:3 V:1 K:ok" \
  "$(printf '%s\n' "$QUOTED_REOPEN" | tr '\n' ' ' | sed 's/ $//')"

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
check "rollback_view" "$POST" "$(printf '%s\n' "$RB" | grep -v '^STATUS:')"
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

echo "--- catalog hash independent of DDL construction order ---"

"$DOLTLITE" "$TMPROOT/orderA" "CREATE TABLE aaa(x INTEGER PRIMARY KEY); CREATE INDEX i_a ON aaa(x); CREATE TABLE bbb(y INTEGER PRIMARY KEY); SELECT CASE WHEN dolt_commit('-A','-m','c') IS NOT NULL THEN '' END;" >/dev/null 2>&1
"$DOLTLITE" "$TMPROOT/orderB" "CREATE TABLE bbb(y INTEGER PRIMARY KEY); CREATE TABLE aaa(x INTEGER PRIMARY KEY); CREATE INDEX i_a ON aaa(x); SELECT CASE WHEN dolt_commit('-A','-m','c') IS NOT NULL THEN '' END;" >/dev/null 2>&1
HA=$("$DOLTLITE" "$TMPROOT/orderA" "SELECT dolt_hashof_catalog()" 2>/dev/null)
HB=$("$DOLTLITE" "$TMPROOT/orderB" "SELECT dolt_hashof_catalog()" 2>/dev/null)
check "hash_shape" "40" "${#HA}"
check "order_independent_catalog_hash" "$HA" "$HB"

echo ""
echo "Results: $pass passed, $fail failed out of $((pass+fail)) tests"
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
