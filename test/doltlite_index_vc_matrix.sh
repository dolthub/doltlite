#!/bin/bash
# Index × VC: integrity_check plus index-planned vs table-scan. Index entries
# are unnamed in catalogs; by-name overlays are blind to them.
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

VERIFY_SQL="SELECT 'ic|'||(SELECT count(*) FROM pragma_integrity_check WHERE integrity_check='ok')||'|'||(SELECT count(*) FROM pragma_integrity_check);
SELECT 'iv|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t INDEXED BY iv)||'|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t NOT INDEXED);
SELECT 'us|'||(SELECT count(*)||','||coalesce(min(s),'-')||','||coalesce(max(s),'-') FROM t INDEXED BY us)||'|'||(SELECT count(*)||','||coalesce(min(s),'-')||','||coalesce(max(s),'-') FROM t NOT INDEXED);
SELECT 'pv|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t INDEXED BY pv WHERE v>100)||'|'||(SELECT count(*)||','||coalesce(sum(v),0) FROM t NOT INDEXED WHERE v>100);
SELECT 'ev|'||(SELECT count(*) FROM t INDEXED BY ev WHERE abs(e)>=1)||'|'||(SELECT count(*) FROM t NOT INDEXED WHERE abs(e)>=1);
SELECT 'ua|'||(SELECT count(*)||','||coalesce(min(a),'-') FROM u INDEXED BY sqlite_autoindex_u_1)||'|'||(SELECT count(*)||','||coalesce(min(a),'-') FROM u NOT INDEXED);
SELECT 'ub|'||(SELECT count(*)||','||coalesce(sum(b),0) FROM u INDEXED BY sqlite_autoindex_u_2)||'|'||(SELECT count(*)||','||coalesce(sum(b),0) FROM u NOT INDEXED);
SELECT 'w|'||(SELECT count(*)||','||coalesce(sum(x),0) FROM w)||'|'||(SELECT coalesce(count(k),0)||','||coalesce(sum(x),0) FROM w NOT INDEXED);
SELECT 'ncz|'||(SELECT count(*) FROM nc INDEXED BY ncz WHERE z='ab')||'|'||(SELECT count(*) FROM nc NOT INDEXED WHERE z='ab');"

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

# Materialize HEAD on a copy so committed catalogs are independent of live state.
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

# Three-way row merge must honor NOCASE/DESC sort keys or probes miss theirs rows.
scenario "divergent merge maintains NOCASE and DESC secondary indexes"
newdb
run_sql "CREATE TABLE m(id INTEGER PRIMARY KEY, tx TEXT COLLATE NOCASE, n INTEGER);
CREATE INDEX mtx ON m(tx);
CREATE INDEX mn ON m(n DESC);
INSERT INTO m VALUES(1,'aaa',10),(2,'bbb',20),(3,'ccc',30),(4,'ddd',40);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');" "$DB" > /dev/null
run_sql "UPDATE m SET tx='MAIN1',n=15 WHERE id=1; UPDATE m SET tx='MAIN2',n=5 WHERE id=2; SELECT dolt_commit('-am','main-side');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('side'); UPDATE m SET tx='SIDE3',n=35 WHERE id=3; UPDATE m SET tx='SIDE4',n=45 WHERE id=4; SELECT dolt_commit('-am','side-side'); SELECT dolt_checkout('main');" "$DB" > /dev/null
run_sql "SELECT dolt_merge('side');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM m INDEXED BY mtx WHERE tx='side3';
SELECT count(*) FROM m NOT INDEXED WHERE tx='side3';
SELECT count(*) FROM m INDEXED BY mtx WHERE tx='main1';
SELECT group_concat(n) FROM (SELECT n FROM m INDEXED BY mn ORDER BY n DESC);
SELECT group_concat(n) FROM (SELECT n FROM m NOT INDEXED ORDER BY n DESC);
PRAGMA integrity_check;" "$DB")
check "merge_nocase_desc_live" "1
1
1
45,35,15,5
45,35,15,5
ok" "$result"
result=$(run_sql "SELECT dolt_reset('--hard');
SELECT count(*) FROM m INDEXED BY mtx WHERE tx='side3';
SELECT group_concat(n) FROM (SELECT n FROM m INDEXED BY mn ORDER BY n DESC);
PRAGMA integrity_check;" "$DB")
check "merge_nocase_desc_head" "0
1
45,35,15,5
ok" "$result"

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

# Vtabs have no catalog entry; merged sqlite_master must keep their schema rows.
scenario "vtab survives clean three-way merge"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('base doc');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side'); INSERT INTO t VALUES(2,'s'); SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main'); INSERT INTO t VALUES(3,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM sqlite_master WHERE name='ft';
SELECT count(*) FROM ft WHERE ft MATCH 'doc';
SELECT count(*) FROM t; PRAGMA integrity_check;" "$DB")
check "vtab_clean_merge" "1
1
3
ok" "$result"

scenario "vtab survives conflicted merge and resolve"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('base doc');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side'); UPDATE t SET v='s' WHERE id=1; SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main'); UPDATE t SET v='m' WHERE id=1; SELECT dolt_commit('-am','main');" "$DB" > /dev/null
result=$(run_sql "BEGIN;
SELECT dolt_merge('side');
SELECT count(*) FROM sqlite_master WHERE name='ft';
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT length(dolt_commit('-Am','resolved'))=40;
SELECT count(*) FROM sqlite_master WHERE name='ft';" "$DB" | grep -v "^Error")
check "vtab_conflicted_merge" "1
0
1
1" "$result"
result=$(run_sql "SELECT count(*) FROM sqlite_master WHERE name='ft';
SELECT count(*) FROM ft WHERE ft MATCH 'doc'; PRAGMA integrity_check;" "$DB")
check "vtab_conflicted_merge_reopen" "1
1
ok" "$result"

scenario "vtab added on a branch is adopted by merge"
newdb
run_sql "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side');
CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('side doc');
SELECT dolt_commit('-Am','side adds ft');
SELECT dolt_checkout('main'); INSERT INTO t VALUES(2,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM sqlite_master WHERE name='ft';
SELECT count(*) FROM ft WHERE ft MATCH 'doc'; PRAGMA integrity_check;" "$DB")
check "vtab_merge_adopt" "1
1
ok" "$result"

scenario "vtab dropped on a branch stays dropped after merge"
newdb
run_sql "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('doomed doc');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side'); DROP TABLE ft; SELECT dolt_commit('-Am','side drops ft');
SELECT dolt_checkout('main'); INSERT INTO t VALUES(2,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM sqlite_master WHERE name LIKE 'ft%';
PRAGMA integrity_check;" "$DB")
check "vtab_merge_drop_wins" "0
ok" "$result"

# Name-based checkout must take the vtab via its schema row and swap shadows.
scenario "table checkout discards working vtab changes"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('one doc');
SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
result=$(run_sql "INSERT INTO ft VALUES('two doc'); SELECT dolt_checkout('ft');
SELECT count(*) FROM ft WHERE ft MATCH 'doc';
INSERT INTO ft(ft) VALUES('integrity-check'); SELECT 'ftok'; PRAGMA integrity_check;" "$DB")
check "vtab_table_checkout_discard" "0
1
ftok
ok" "$result"

scenario "table checkout of a vtab from another branch"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('one doc');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side');
INSERT INTO ft VALUES('two doc'); INSERT INTO t VALUES(2,'s');
SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main');
SELECT dolt_checkout('side','ft');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM ft WHERE ft MATCH 'doc';
SELECT count(*) FROM t;
INSERT INTO ft(ft) VALUES('integrity-check'); SELECT 'ftok'; PRAGMA integrity_check;" "$DB")
check "vtab_table_checkout_from_branch" "2
1
ftok
ok" "$result"

scenario "named indexes on shadow tables reconcile through vtab checkout"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('one doc');
CREATE INDEX live_only ON ft_content(c0);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','source');
DROP INDEX live_only; CREATE INDEX source_only ON ft_content(c0);
INSERT INTO ft VALUES('two doc');
SELECT dolt_commit('-Am','source reindexed');
SELECT dolt_checkout('main');
SELECT dolt_checkout('source','ft');" "$DB" > /dev/null
result=$(run_sql "SELECT group_concat(name) FROM (SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='ft_content' ORDER BY name);
SELECT count(*) FROM ft WHERE ft MATCH 'doc';
PRAGMA integrity_check;" "$DB")
check "vtab_shadow_index_reconcile" "source_only
2
ok" "$result"

scenario "table checkout restores a dropped vtab from history"
newdb
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body); INSERT INTO ft VALUES('one doc');
CREATE VIRTUAL TABLE rt USING rtree(id, x1, x2); INSERT INTO rt VALUES(1, 0.0, 1.0);
SELECT dolt_commit('-Am','base');
DROP TABLE ft; DROP TABLE rt;
SELECT dolt_commit('-Am','drop both');
SELECT dolt_checkout('HEAD~1','ft','rt');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM ft WHERE ft MATCH 'doc';
SELECT rtreecheck('main','rt');
INSERT INTO ft(ft) VALUES('integrity-check'); SELECT 'ftok'; PRAGMA integrity_check;" "$DB")
check "vtab_table_checkout_restore_dropped" "1
ok
ftok
ok" "$result"

scenario "new indexed tables staged individually"
for count in 4 12 24; do
  canonical_hash=""
  for mode in all individual reverse; do
    newdb
    setup=""
    stage=""
    probes=""
    expected=""
    for ((i=0; i<count; i++)); do
      setup="$setup CREATE TABLE items_$i(id TEXT PRIMARY KEY, label TEXT UNIQUE);
INSERT INTO items_$i VALUES('base_$i','label_$i');"
      if [ "$mode" = reverse ]; then
        stage="SELECT dolt_add('items_$i'); $stage"
      else
        stage="$stage SELECT dolt_add('items_$i');"
      fi
      probes="$probes SELECT id FROM items_$i INDEXED BY sqlite_autoindex_items_${i}_2 WHERE label='label_$i';"
      expected="${expected}base_$i
"
    done
    if [ "$mode" = all ]; then stage="SELECT dolt_add('-A');"; fi
    result=$(run_sql ".bail on
$setup
$stage
SELECT dolt_commit('-m','initial');
$probes
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
$probes
PRAGMA integrity_check;" "$DB")
    check "${mode}_${count}_exit" "0" "$?"
    check "${mode}_${count}_checkout" "${expected}ok" "$(echo "$result" | tail -n $((count+1)))"
    result=$(run_sql ".bail on
$probes
PRAGMA integrity_check;" "$DB@feature")
    check "${mode}_${count}_reopen" "${expected}ok" "$result"
    hash=$(run_sql "SELECT dolt_hashof_db('HEAD');" "$DB@feature")
    if [ "$mode" = all ]; then canonical_hash="$hash"; fi
    check "${mode}_${count}_canonical" "$canonical_hash" "$hash"
    result=$(run_sql "INSERT INTO items_0 VALUES('duplicate','label_0');" "$DB@feature")
    check "${mode}_${count}_unique_exit" "1" "$?"
    case "$result" in
      *"UNIQUE constraint failed"*) PASS=$((PASS+1));;
      *) note_fail "${mode}_${count}_unique_error" "$result";;
    esac
    staged_hash=$(run_sql "SELECT dolt_hashof_db('STAGED');" "$DB@feature")
    result=$(run_sql "SELECT dolt_add('items_0','missing_table');" "$DB@feature")
    check "${mode}_${count}_missing_add_exit" "1" "$?"
    check "${mode}_${count}_missing_add_atomic" "$staged_hash" \
      "$(run_sql "SELECT dolt_hashof_db('STAGED');" "$DB@feature")"
    REMOTE="file://$TDIR/incremental$N.db"
    result=$(run_sql ".bail on
SELECT dolt_remote('add','origin','$REMOTE');
SELECT dolt_push('origin','feature');" "$DB@feature")
    check "${mode}_${count}_push_exit" "0" "$?"
    CLONE="$TDIR/incremental_clone$N.db"
    result=$(run_sql ".bail on
SELECT dolt_clone('$REMOTE');
$probes
PRAGMA integrity_check;" "$CLONE")
    check "${mode}_${count}_clone_exit" "0" "$?"
    check "${mode}_${count}_clone_indexes" "${expected}ok" \
      "$(echo "$result" | tail -n $((count+1)))"
  done
done

scenario "commit -am preserves staged-only indexes across numbering changes"
setup=""
for ((i=0; i<12; i++)); do
  setup="$setup CREATE TABLE items_$i(id TEXT PRIMARY KEY, label TEXT UNIQUE);
INSERT INTO items_$i VALUES('base_$i','label_$i');"
done
for staged in 0 10; do
  newdb
  result=$(run_sql ".bail on
CREATE TABLE z(id TEXT PRIMARY KEY, label TEXT UNIQUE);
INSERT INTO z VALUES('z','z');
SELECT dolt_commit('-Am','base');
$setup
SELECT dolt_add('items_$staged');
UPDATE items_$staged SET label='unstaged';
UPDATE z SET label='zz';
SELECT dolt_commit('-am','mixed');
SELECT dolt_checkout('-b','snapshot');
SELECT dolt_reset('--hard');
SELECT id FROM items_$staged INDEXED BY sqlite_autoindex_items_${staged}_2 WHERE label='label_$staged';
SELECT id FROM z INDEXED BY sqlite_autoindex_z_2 WHERE label='zz';
SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'items_%';
PRAGMA integrity_check;" "$DB")
  check "am_staged_${staged}_exit" "0" "$?"
  check "am_staged_${staged}_indexes" "base_$staged
z
1
ok" "$(echo "$result" | tail -n 4)"
done

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
