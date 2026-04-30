#!/bin/bash
#
# Local regressions for schema-edge non-interactive rebase behavior.
#
set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
PASS=0; FAIL=0; ERRORS=""

run_db_match() {
  local n="$1" s="$2" p="$3"
  local db="/tmp/${n}_$$.db"
  rm -f "$db"
  local r
  r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$db" 2>&1)
  rm -f "$db"
  if echo "$r" | grep -qE "$p"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"
  fi
}

run_db_eq() {
  local n="$1" s="$2" e="$3"
  local db="/tmp/${n}_$$.db"
  rm -f "$db"
  local r
  r=$(echo "$s" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$db" 2>&1 | tail -n 1)
  rm -f "$db"
  if [ "$r" = "$e" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"
  fi
}

echo "=== Doltlite Rebase Schema Tests ==="
echo ""

TABLE_CHECK_SETUP="
CREATE TABLE base(id INTEGER PRIMARY KEY, v INT);
INSERT INTO base VALUES(1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
CREATE TABLE base_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO base_new SELECT * FROM base;
DROP TABLE base;
ALTER TABLE base_new RENAME TO base;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main check');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(k INTEGER PRIMARY KEY, w TEXT);
INSERT INTO feat_tbl VALUES(1,'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat table');
"

run_db_match "rebase_schema_table_check_hash" "
$TABLE_CHECK_SETUP
SELECT dolt_rebase('main');
" "Successfully rebased|^[0-9a-f]{40}$"

run_db_eq "rebase_schema_table_check_feat_tbl" "
$TABLE_CHECK_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM feat_tbl;
" "1"

run_db_eq "rebase_schema_table_check_base" "
$TABLE_CHECK_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM base;
" "1"

INDEX_SETUP="
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES(1,10);
INSERT INTO b VALUES(1,20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main idx');
SELECT dolt_checkout('feat');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat idx');
"

run_db_match "rebase_schema_disjoint_idx_hash" "
$INDEX_SETUP
SELECT dolt_rebase('main');
" "Successfully rebased|^[0-9a-f]{40}$"

run_db_eq "rebase_schema_disjoint_idx_main" "
$INDEX_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM pragma_index_list('a') WHERE name='idx_a_v';
" "1"

run_db_eq "rebase_schema_disjoint_idx_feat_same_session_current_dolt_behavior" "
$INDEX_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM pragma_index_list('b') WHERE name='idx_b_v';
" "1"

run_db_eq "rebase_schema_disjoint_idx_feat_reopen_current_dolt_behavior" "
$INDEX_SETUP
SELECT dolt_rebase('main');
" "Successfully rebased and updated refs/heads/feat"

{
  db="/tmp/rebase_schema_disjoint_idx_feat_reopen_current_dolt_behavior_$$.db"
  rm -f "$db"
  echo "$INDEX_SETUP
SELECT dolt_rebase('main');" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" "$db" >/dev/null 2>&1
  r=$(printf ".headers off\n.mode list\nSELECT count(*) FROM pragma_index_list('b') WHERE name='idx_b_v';\n" | "$DOLTLITE" "$db" 2>&1 | tail -n 1)
  rm -f "$db"
  if [ "$r" = "0" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: rebase_schema_disjoint_idx_feat_reopen_current_dolt_behavior\n  expected: 0\n  got:      $r"
  fi
}

FK_TABLES_SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main check');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat fk tables');
"

run_db_match "rebase_schema_fk_tables_hash" "
$FK_TABLES_SETUP
SELECT dolt_rebase('main');
" "Successfully rebased|^[0-9a-f]{40}$"

run_db_eq "rebase_schema_fk_tables_parent" "
$FK_TABLES_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM p;
" "1"

run_db_eq "rebase_schema_fk_tables_child" "
$FK_TABLES_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM c;
" "1"

run_db_eq "rebase_schema_fk_tables_fk" "
$FK_TABLES_SETUP
SELECT dolt_rebase('main');
SELECT count(*) FROM pragma_foreign_key_list('c');
" "1"

SELF_REF_SETUP="
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main root');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat descendant');
"

run_db_match "rebase_schema_self_ref_fk_hash" "
$SELF_REF_SETUP
SELECT dolt_rebase('main');
" "Successfully rebased|^[0-9a-f]{40}$"

run_db_eq "rebase_schema_self_ref_fk_cascade" "
$SELF_REF_SETUP
SELECT dolt_rebase('main');
DELETE FROM t WHERE id = 1;
SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id);
" "10:-1"

CHAIN_SETUP="
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feat');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main root');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat child');
"

run_db_match "rebase_schema_fk_chain_hash" "
$CHAIN_SETUP
SELECT dolt_rebase('main');
" "Successfully rebased|^[0-9a-f]{40}$"

run_db_eq "rebase_schema_fk_chain_cascade" "
$CHAIN_SETUP
SELECT dolt_rebase('main');
DELETE FROM gp WHERE id = 1;
SELECT (SELECT count(*) FROM gp) || '|' || (SELECT count(*) FROM p) || '|' || (SELECT count(*) FROM c);
" "1|0|0"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
