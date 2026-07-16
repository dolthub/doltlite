#!/bin/bash
# Index x version-control matrix: every VC surface against every index
# flavor, verifying after each step that (a) integrity_check passes and
# (b) index-planned queries agree with table scans. Index entries are
# unnamed in catalogs and by-name catalog overlays are structurally blind
# to them; this suite is the intent-level oracle for that bug class.
DOLTLITE="${1:-./doltlite}"
PASS=0
FAIL=0
ERRORS=""

run_sql() {
  echo "$1" | perl -e 'alarm(20); exec @ARGV' $DOLTLITE "$2" 2>&1
}

note_fail() {
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: $1\n  $2"
  echo "  FAIL: $1"
}

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    PASS=$((PASS+1))
  else
    note_fail "$name" "expected |$expected| got |$actual|"
  fi
}

# The fixture covers every index flavor: secondary, UNIQUE, partial,
# expression, PK/UNIQUE autoindexes, WITHOUT ROWID, and NOCASE.
FIXTURE="CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, s TEXT, e INTEGER);
CREATE INDEX iv ON t(v);
CREATE UNIQUE INDEX us ON t(s);
CREATE INDEX pv ON t(v) WHERE v > 100;
CREATE INDEX ev ON t(abs(e));
CREATE TABLE u(a TEXT PRIMARY KEY, b INTEGER UNIQUE);
CREATE TABLE w(k TEXT PRIMARY KEY, x INTEGER) WITHOUT ROWID;
CREATE TABLE nc(z TEXT COLLATE NOCASE, y INTEGER);
CREATE INDEX ncz ON nc(z);
INSERT INTO t VALUES(1,50,'a',-1),(2,150,'b',2),(3,250,'c',-3),(4,60,'d',4);
INSERT INTO u VALUES('k1',10),('k2',20);
INSERT INTO w VALUES('w1',7),('w2',8);
INSERT INTO nc VALUES('Ab',1),('aB',2),('cd',3);"

# Every index-touching mutation class: key updates that move index
# membership (incl. partial-index boundary), unique-key rewrites,
# expression inputs, inserts and deletes on every table.
MUTATE="UPDATE t SET v=v+100 WHERE id IN (1,2);
UPDATE t SET s=s||'x' WHERE id IN (2,3);
UPDATE t SET e=-e WHERE id IN (1,4);
DELETE FROM t WHERE id=4;
INSERT INTO t VALUES(5,75,'e',5);
UPDATE u SET b=b+1 WHERE a='k1';
INSERT INTO u VALUES('k3',30);
UPDATE w SET x=x*10 WHERE k='w1';
DELETE FROM nc WHERE z='cd';
INSERT INTO nc VALUES('CD',4);"

# One line per index: "<name>|<index-planned>|<table-scan>"; the two
# projections must agree, and integrity_check must be clean.
VERIFY_SQL="SELECT 'ic|'||(SELECT count(*) FROM pragma_integrity_check WHERE integrity_check='ok')||'|'||(SELECT count(*) FROM pragma_integrity_check);
SELECT 'iv|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t INDEXED BY iv)||'|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t NOT INDEXED);
SELECT 'us|'||(SELECT count(*)||','||coalesce(min(s),'-')||','||coalesce(max(s),'-') FROM t INDEXED BY us)||'|'||(SELECT count(*)||','||coalesce(min(s),'-')||','||coalesce(max(s),'-') FROM t NOT INDEXED);
SELECT 'pv|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t INDEXED BY pv WHERE v>100)||'|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t NOT INDEXED WHERE v>100);
SELECT 'ev|'||(SELECT count(*) FROM t INDEXED BY ev WHERE abs(e)>=1)||'|'||(SELECT count(*) FROM t NOT INDEXED WHERE abs(e)>=1);
SELECT 'ua|'||(SELECT count(*)||','||coalesce(min(a),'-') FROM u INDEXED BY sqlite_autoindex_u_1)||'|'||(SELECT count(*)||','||coalesce(min(a),'-') FROM u NOT INDEXED);
SELECT 'ub|'||(SELECT count(*)||','||coalesce(sum(b),0) FROM u INDEXED BY sqlite_autoindex_u_2)||'|'||(SELECT count(*)||','||coalesce(sum(b),0) FROM u NOT INDEXED);
SELECT 'w|'||(SELECT count(*)||','||coalesce(sum(x),0) FROM w)||'|'||(SELECT coalesce(count(k),0)||','||coalesce(sum(x),0) FROM w NOT INDEXED);
SELECT 'ncz|'||(SELECT count(*) FROM nc INDEXED BY ncz WHERE z='ab')||'|'||(SELECT count(*) FROM nc NOT INDEXED WHERE z='ab');"

# verify <label> <db>: same-session and fresh-session agreement
verify() {
  local label="$1" db="$2" line lhs rhs name bad=""
  local out
  out=$(run_sql "$VERIFY_SQL" "$db")
  while IFS='|' read -r name lhs rhs; do
    [ -z "$name" ] && continue
    if [ "$lhs" != "$rhs" ]; then bad="$bad [$name: $lhs != $rhs]"; fi
  done <<EOF
$out
EOF
  if echo "$out" | grep -qE "Parse error|Runtime error|no such"; then
    bad="$bad [error: $(echo "$out" | grep -aE 'error|no such' | head -1)]"
  fi
  if [ -z "$bad" ]; then
    PASS=$((PASS+1))
  else
    note_fail "$label" "$bad"
  fi
}

# verify_commit <label> <db>: materialize HEAD on a copy and verify it,
# so committed catalogs are checked independently of live session state.
verify_commit() {
  local label="$1" db="$2"
  cp "$db" "$db.headcopy"
  run_sql "SELECT dolt_reset('--hard');" "$db.headcopy" > /dev/null
  verify "$label" "$db.headcopy"
  rm -f "$db.headcopy"
}

TDIR=$(mktemp -d /tmp/idxvc.XXXXXX)
trap 'rm -rf "$TDIR"' EXIT
N=0
newdb() { N=$((N+1)); DB="$TDIR/m$N.db"; }

scenario() { echo "--- $1 ---"; }

# ── staging surfaces ──────────────────────────────────────────────
scenario "add -A + commit"
newdb
run_sql "$FIXTURE SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_add('-A'); SELECT dolt_commit('-m','mut');" "$DB" > /dev/null
verify "addA_live" "$DB"; verify_commit "addA_commit" "$DB"

scenario "named add per table + commit"
newdb
run_sql "$FIXTURE SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_add('t'); SELECT dolt_add('u'); SELECT dolt_add('w'); SELECT dolt_add('nc'); SELECT dolt_commit('-m','mut');" "$DB" > /dev/null
verify "namedadd_live" "$DB"; verify_commit "namedadd_commit" "$DB"

scenario "commit -am"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
verify "am_live" "$DB"; verify_commit "am_commit" "$DB"

scenario "partial staging then -am"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "UPDATE t SET v=v+100 WHERE id=1; SELECT dolt_add('t');
$MUTATE SELECT dolt_commit('-am','mix');" "$DB" > /dev/null
verify "partial_am_live" "$DB"; verify_commit "partial_am_commit" "$DB"

scenario "unstage all then recommit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_add('-A'); SELECT dolt_reset(); SELECT dolt_add('-A'); SELECT dolt_commit('-m','mut');" "$DB" > /dev/null
verify "unstage_all_live" "$DB"; verify_commit "unstage_all_commit" "$DB"

scenario "named unstage then recommit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_add('-A'); SELECT dolt_reset('t'); SELECT dolt_add('t'); SELECT dolt_commit('-m','mut');" "$DB" > /dev/null
verify "unstage_named_live" "$DB"; verify_commit "unstage_named_commit" "$DB"

# ── reset surfaces ────────────────────────────────────────────────
scenario "reset --hard discards index-touching mutations"
newdb
run_sql "$FIXTURE SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_reset('--hard');" "$DB" > /dev/null
verify "reset_hard_live" "$DB"
result=$(run_sql "SELECT count(*) FROM t; SELECT count(*) FROM dolt_status;" "$DB")
check "reset_hard_restores_baseline" "4
0" "$result"

scenario "reset --soft one commit back, recommit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
run_sql "SELECT dolt_reset('--soft','HEAD~1'); SELECT dolt_commit('-am','recommit');" "$DB" > /dev/null
verify "reset_soft_live" "$DB"; verify_commit "reset_soft_commit" "$DB"

# ── branch / checkout / merge ─────────────────────────────────────
scenario "branch checkout roundtrip"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side','HEAD~1'); SELECT dolt_checkout('main');" "$DB" > /dev/null
verify "checkout_roundtrip_live" "$DB"
BRANCH_VERIFY="SELECT dolt_checkout('side');
$VERIFY_SQL"
out=$(run_sql "$BRANCH_VERIFY" "$DB")
if echo "$out" | grep -qE "Parse error|Runtime error|no such"; then
  note_fail "checkout_old_branch" "$(echo "$out" | grep -aE 'error|no such' | head -1)"
else
  PASS=$((PASS+1))
fi

scenario "divergent merge, disjoint index-touching edits"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');" "$DB" > /dev/null
run_sql "UPDATE t SET v=v+100 WHERE id=1; INSERT INTO t VALUES(6,300,'f',6); SELECT dolt_commit('-am','main-side');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('side'); UPDATE t SET s=s||'y' WHERE id=3; INSERT INTO u VALUES('k9',90); SELECT dolt_commit('-am','side-side'); SELECT dolt_checkout('main');" "$DB" > /dev/null
run_sql "SELECT dolt_merge('side');" "$DB" > /dev/null
verify "merge_live" "$DB"; verify_commit "merge_commit" "$DB"

scenario "merge carrying index schema changes"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('side'); CREATE INDEX ie ON t(e); DROP INDEX iv; SELECT dolt_add('-A'); SELECT dolt_commit('-m','idx-schema'); SELECT dolt_checkout('main');" "$DB" > /dev/null
run_sql "INSERT INTO t VALUES(7,80,'g',7); SELECT dolt_commit('-am','row'); SELECT dolt_merge('side');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM t INDEXED BY ie WHERE e>=0; PRAGMA integrity_check;" "$DB")
check "merge_index_schema" "3
ok" "$result"

# ── cherry-pick / revert ──────────────────────────────────────────
scenario "cherry-pick an index-touching commit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('side'); $MUTATE SELECT dolt_commit('-am','muts'); SELECT dolt_checkout('main');" "$DB" > /dev/null
CP=$(run_sql "SELECT commit_hash FROM dolt_log('side') LIMIT 1;" "$DB" | tail -1)
run_sql "SELECT dolt_cherry_pick('$CP');" "$DB" > /dev/null
verify "cherry_pick_live" "$DB"; verify_commit "cherry_pick_commit" "$DB"

scenario "revert an index-touching commit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
run_sql "SELECT dolt_revert('HEAD');" "$DB" > /dev/null
verify "revert_live" "$DB"; verify_commit "revert_commit" "$DB"
result=$(run_sql "SELECT count(*) FROM t;" "$DB")
check "revert_restores_rows" "4" "$result"

# ── index DDL through VC ──────────────────────────────────────────
scenario "CREATE INDEX / DROP INDEX through commit and checkout"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "CREATE INDEX i2 ON t(e); DROP INDEX iv; SELECT dolt_commit('-Am','ddl');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_reset('--hard'); SELECT count(*) FROM t INDEXED BY i2 WHERE e>=-99; SELECT name FROM sqlite_master WHERE name='iv'; PRAGMA integrity_check;" "$DB")
check "index_ddl_commit" "0
4
ok" "$result"
result=$(run_sql "SELECT dolt_checkout('-b','old','HEAD~1'); SELECT count(*) FROM t INDEXED BY iv; SELECT name FROM sqlite_master WHERE name='i2'; PRAGMA integrity_check;" "$DB")
check "index_ddl_old_branch" "0
4
ok" "$result"

scenario "ALTER TABLE RENAME carries indexes"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "ALTER TABLE t RENAME TO t2; SELECT dolt_commit('-Am','rename');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_reset('--hard'); SELECT count(*) FROM t2 INDEXED BY iv; PRAGMA integrity_check;" "$DB")
check "rename_carries_indexes" "0
4
ok" "$result"

# ── remotes and gc ────────────────────────────────────────────────
scenario "clone/push/pull roundtrip preserves indexes"
newdb
REMOTE="file://$TDIR/rem$N.db"
run_sql "$FIXTURE SELECT dolt_commit('-Am','base'); SELECT dolt_remote('add','origin','$REMOTE'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
CLONE="$TDIR/clone$N.db"
run_sql "SELECT dolt_clone('$REMOTE');" "$CLONE" > /dev/null
verify "clone_indexes" "$CLONE"
run_sql "$MUTATE SELECT dolt_commit('-am','mut'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
run_sql "SELECT dolt_pull('origin','main');" "$CLONE" > /dev/null
verify "pull_indexes" "$CLONE"

scenario "gc preserves index chunks"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut'); SELECT dolt_gc();" "$DB" > /dev/null
verify "gc_live" "$DB"; verify_commit "gc_commit" "$DB"

# ── virtual tables: shadow tables travel with their vtab ─────────
scenario "vtab shadows through staging surfaces"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body);
INSERT INTO ft VALUES('alpha doc'),('beta doc');
CREATE VIRTUAL TABLE rt USING rtree(id, x1, x2);
INSERT INTO rt VALUES(1, 1.0, 2.0);
SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "INSERT INTO ft VALUES('gamma doc'); INSERT INTO rt VALUES(2, 5.0, 6.0);
SELECT dolt_add('ft'); SELECT dolt_add('rt'); SELECT dolt_commit('-m','named');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_reset('--hard');
SELECT count(*) FROM ft WHERE ft MATCH 'doc';
SELECT count(*) FROM rt WHERE x1>=4.0;
PRAGMA integrity_check;" "$DB")
check "vtab_named_add_commit" "0
3
1
ok" "$result"
run_sql "INSERT INTO ft VALUES('delta doc'); SELECT dolt_commit('-am','am');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_reset('--hard'); SELECT count(*) FROM ft WHERE ft MATCH 'doc';" "$DB")
check "vtab_am_commit" "0
4" "$result"
run_sql "DROP TABLE ft; SELECT dolt_add('ft'); SELECT dolt_commit('-m','dropft');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_reset('--hard'); SELECT count(*) FROM sqlite_master WHERE name LIKE 'ft%'; PRAGMA integrity_check;" "$DB")
check "vtab_staged_drop" "0
0
ok" "$result"

scenario "vtab through branch checkout and clone"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('one doc');
SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "INSERT INTO ft VALUES('two doc'); SELECT dolt_commit('-am','more');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('-b','old','HEAD~1'); SELECT count(*) FROM ft WHERE ft MATCH 'doc';" "$DB")
check "vtab_old_branch" "0
1" "$result"
REMOTE="file://$TDIR/vrem$N.db"
run_sql "SELECT dolt_checkout('main'); SELECT dolt_remote('add','origin','$REMOTE'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
CLONE="$TDIR/vclone$N.db"
result=$(run_sql "SELECT dolt_clone('$REMOTE'); SELECT count(*) FROM ft WHERE ft MATCH 'doc'; PRAGMA integrity_check;" "$CLONE")
check "vtab_clone" "0
2
ok" "$result"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
