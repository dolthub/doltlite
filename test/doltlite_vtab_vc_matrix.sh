#!/bin/bash
# Vtabs have no catalog entry (shadows + storage-free schema row), so by-entry
# catalog rebuilds are blind to them. Check MATCH/bbox, module self-checks,
# and fresh-session agreement after schema replay.
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

FIXTURE="CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO docs VALUES(1,'alpha bravo'),(2,'bravo charlie'),(3,'charlie delta');
CREATE VIRTUAL TABLE f5 USING fts5(body);
INSERT INTO f5(rowid, body) SELECT id, body FROM docs;
CREATE VIRTUAL TABLE f5x USING fts5(body, content='docs', content_rowid='id');
INSERT INTO f5x(rowid, body) SELECT id, body FROM docs;
CREATE VIRTUAL TABLE f4 USING fts4(body);
INSERT INTO f4(rowid, body) SELECT id, body FROM docs;
CREATE VIRTUAL TABLE f3 USING fts3(body);
INSERT INTO f3(rowid, body) SELECT id, body FROM docs;
CREATE VIRTUAL TABLE rt USING rtree(id, x1, x2, y1, y2);
INSERT INTO rt VALUES(1, 0,1, 0,1),(2, 2,3, 2,3),(3, 4,5, 4,5);
CREATE TABLE plain(k INTEGER PRIMARY KEY, v TEXT);
INSERT INTO plain VALUES(1,'p');"

MUTATE="INSERT INTO docs VALUES(4,'delta echo');
INSERT INTO f5(rowid, body) VALUES(4,'delta echo');
INSERT INTO f5x(rowid, body) VALUES(4,'delta echo');
INSERT INTO f4(rowid, body) VALUES(4,'delta echo');
INSERT INTO f3(rowid, body) VALUES(4,'delta echo');
UPDATE f5 SET body='alpha zulu' WHERE rowid=1;
UPDATE f4 SET body='alpha zulu' WHERE rowid=1;
UPDATE f3 SET body='alpha zulu' WHERE rowid=1;
DELETE FROM f5 WHERE rowid=2;
DELETE FROM f4 WHERE rowid=2;
DELETE FROM f3 WHERE rowid=2;
INSERT INTO f5(f5) VALUES('optimize');
INSERT INTO rt VALUES(4, 6,7, 6,7);
UPDATE rt SET x1=10, x2=11 WHERE id=1;
DELETE FROM rt WHERE id=2;
UPDATE plain SET v='q' WHERE k=1;"

VERIFY_SQL="SELECT 'ic|'||(SELECT count(*) FROM pragma_integrity_check WHERE integrity_check='ok')||'/'||(SELECT count(*) FROM pragma_integrity_check);
SELECT 'rtck|'||rtreecheck('main','rt');
INSERT INTO f5(f5) VALUES('integrity-check');
INSERT INTO f4(f4) VALUES('integrity-check');
SELECT 'f5|'||coalesce((SELECT group_concat(rowid) FROM (SELECT rowid FROM f5 WHERE f5 MATCH 'delta' ORDER BY rowid)),'-');
SELECT 'f5x|'||coalesce((SELECT group_concat(rowid) FROM (SELECT rowid FROM f5x WHERE f5x MATCH 'delta' ORDER BY rowid)),'-');
SELECT 'f4|'||coalesce((SELECT group_concat(rowid) FROM (SELECT rowid FROM f4 WHERE f4 MATCH 'delta' ORDER BY rowid)),'-');
SELECT 'f3|'||coalesce((SELECT group_concat(rowid) FROM (SELECT rowid FROM f3 WHERE f3 MATCH 'delta' ORDER BY rowid)),'-');
SELECT 'rt|'||coalesce((SELECT group_concat(id) FROM (SELECT id FROM rt WHERE x1>=0 AND x2<=20 ORDER BY id)),'-');"

BASE_STATE="ic|1/1
rtck|ok
f5|3
f5x|3
f4|3
f3|3
rt|1,2,3"

MUT_STATE="ic|1/1
rtck|ok
f5|3,4
f5x|3,4
f4|3,4
f3|3,4
rt|1,3,4"

verify() {
  local label="$1" db="$2" expected="$3"
  local out
  out=$(run_sql "$VERIFY_SQL" "$db")
  if [ "$out" = "$expected" ]; then
    PASS=$((PASS+1))
  else
    note_fail "$label" "expected |$expected| got |$out|"
  fi
}

# Materialize HEAD on a copy so committed catalogs are independent of live state.
verify_commit() {
  local label="$1" db="$2" expected="$3"
  cp "$db" "$db.headcopy"
  run_sql "SELECT dolt_reset('--hard');" "$db.headcopy" > /dev/null
  verify "$label" "$db.headcopy" "$expected"
  rm -f "$db.headcopy"
}

TDIR=$(mktemp -d /tmp/vtabvc.XXXXXX)
trap 'rm -rf "$TDIR"' EXIT
N=0
newdb() { N=$((N+1)); DB="$TDIR/v$N.db"; }

scenario() { echo "--- $1 ---"; }

scenario "add -A + commit"
newdb
run_sql "$FIXTURE SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');" "$DB" > /dev/null
verify "addA_base_live" "$DB" "$BASE_STATE"
run_sql "$MUTATE SELECT dolt_add('-A'); SELECT dolt_commit('-m','mut');" "$DB" > /dev/null
verify "addA_mut_live" "$DB" "$MUT_STATE"
verify_commit "addA_mut_commit" "$DB" "$MUT_STATE"

scenario "named add carries each vtab's shadows"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_add('docs'); SELECT dolt_add('f5'); SELECT dolt_add('f5x');
SELECT dolt_add('f4'); SELECT dolt_add('f3'); SELECT dolt_add('rt');
SELECT dolt_commit('-m','vtabs only');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_reset('--hard'); SELECT v FROM plain WHERE k=1;" "$DB" | tail -1)
check "namedadd_plain_left_behind" "p" "$result"
result=$(run_sql "SELECT group_concat(rowid) FROM (SELECT rowid FROM f5 WHERE f5 MATCH 'delta' ORDER BY rowid);" "$DB")
check "namedadd_vtab_committed" "3,4" "$result"

scenario "commit -am"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
verify_commit "am_commit" "$DB" "$MUT_STATE"

scenario "reset --hard discards vtab mutations"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_reset('--hard');" "$DB" > /dev/null
verify "reset_hard" "$DB" "$BASE_STATE"

scenario "reset --soft one back, recommit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
run_sql "SELECT dolt_reset('--soft','HEAD~1'); SELECT dolt_commit('-am','recommit');" "$DB" > /dev/null
verify_commit "reset_soft_recommit" "$DB" "$MUT_STATE"

scenario "branch isolation and roundtrip"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); $MUTATE SELECT dolt_commit('-am','side mut');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('main');" "$DB" > /dev/null
verify "branch_main_unchanged" "$DB" "$BASE_STATE"
verify "branch_side_mutated" "$DB/side" "$MUT_STATE"

scenario "table-level checkout of a vtab from another branch"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side');
INSERT INTO f5(rowid, body) VALUES(9,'delta niner');
SELECT dolt_commit('-am','side f5');
SELECT dolt_checkout('main');
SELECT dolt_checkout('side','f5');" "$DB" > /dev/null
result=$(run_sql "SELECT group_concat(rowid) FROM (SELECT rowid FROM f5 WHERE f5 MATCH 'delta' ORDER BY rowid);
INSERT INTO f5(f5) VALUES('integrity-check');
SELECT count(*) FROM f4 WHERE f4 MATCH 'delta';" "$DB")
check "table_checkout_vtab" "3,9
1" "$result"

scenario "table-level checkout discards working vtab changes"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "INSERT INTO f5(rowid, body) VALUES(9,'delta niner'); SELECT dolt_checkout('f5');" "$DB" > /dev/null
verify "table_checkout_discard" "$DB" "$BASE_STATE"

scenario "tag and hash checkout"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base'); SELECT dolt_tag('v1');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
run_sql "SELECT dolt_branch('at_tag','v1'); SELECT dolt_branch('at_hash', dolt_hashof('HEAD~1'));" "$DB" > /dev/null
verify "tag_branch_checkout" "$DB/at_tag" "$BASE_STATE"
verify "hash_branch_checkout" "$DB/at_hash" "$BASE_STATE"

scenario "drop vtab, commit, restore from history"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "DROP TABLE f5; DROP TABLE rt; SELECT dolt_commit('-Am','drop two');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM sqlite_master WHERE name='f5' OR name='rt'
  OR name LIKE 'f5\_%' ESCAPE '\' OR name LIKE 'rt\_%' ESCAPE '\'; PRAGMA integrity_check;" "$DB")
check "drop_commit_gone" "0
ok" "$result"
run_sql "SELECT dolt_checkout('HEAD~1','f5','rt');" "$DB" > /dev/null
result=$(run_sql "SELECT group_concat(rowid) FROM (SELECT rowid FROM f5 WHERE f5 MATCH 'delta' ORDER BY rowid);
SELECT rtreecheck('main','rt');
INSERT INTO f5(f5) VALUES('integrity-check');
PRAGMA integrity_check;" "$DB")
check "drop_restore_from_history" "3
ok
ok" "$result"

scenario "rename vtab, commit, checkout across the rename"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "ALTER TABLE f5 RENAME TO f5r; SELECT dolt_commit('-Am','rename');" "$DB" > /dev/null
result=$(run_sql "SELECT group_concat(rowid) FROM (SELECT rowid FROM f5r WHERE f5r MATCH 'delta' ORDER BY rowid);
INSERT INTO f5r(f5r) VALUES('integrity-check');
SELECT count(*) FROM sqlite_master WHERE name='f5';
PRAGMA integrity_check;" "$DB")
check "rename_committed" "3
0
ok" "$result"
run_sql "SELECT dolt_branch('pre_rename','HEAD~1');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM f5 WHERE f5 MATCH 'delta'; SELECT count(*) FROM sqlite_master WHERE name='f5r'; PRAGMA integrity_check;" "$DB/pre_rename")
check "rename_old_branch_has_old_name" "1
0
ok" "$result"

scenario "clean merge with untouched vtabs"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); INSERT INTO plain VALUES(2,'s'); SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main'); INSERT INTO plain VALUES(3,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
verify "merge_clean_vtabs_survive" "$DB" "$BASE_STATE"
verify_commit "merge_clean_commit" "$DB" "$BASE_STATE"

scenario "merge adopts one-sided vtab content"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); $MUTATE SELECT dolt_commit('-am','side mut');
SELECT dolt_checkout('main'); INSERT INTO plain VALUES(3,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
verify "merge_one_sided_vtab_content" "$DB" "$MUT_STATE"

scenario "fast-forward merge with vtab content"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); $MUTATE SELECT dolt_commit('-am','side mut');
SELECT dolt_checkout('main'); SELECT dolt_merge('side');" "$DB" > /dev/null
verify "merge_ff_vtab_content" "$DB" "$MUT_STATE"

# Colliding fts5 segment shadows: neither side is right; merge rebuilds from content.
scenario "colliding vtab writes: the merge rebuilds the index itself"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side');
INSERT INTO f5(rowid, body) VALUES(10,'delta side');
SELECT dolt_commit('-am','side doc');
SELECT dolt_checkout('main');
INSERT INTO f5(rowid, body) VALUES(20,'delta main');
SELECT dolt_commit('-am','main doc');" "$DB" > /dev/null
result=$(run_sql "BEGIN;
SELECT dolt_merge('side');
SELECT 'TX|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM sqlite_master WHERE name='f5');
ROLLBACK;" "$DB" | grep '^TX|')
check "merge_colliding_vtab_no_conflicts" "TX|0|1" "$result"
check "merge_colliding_vtab_keeps_both_rows" "5" \
  "$(run_sql "SELECT count(*) FROM f5_content;" "$DB")"
check "merge_colliding_vtab_finds_ours" "20" \
  "$(run_sql "SELECT group_concat(rowid) FROM f5 WHERE f5 MATCH 'main';" "$DB")"
check "merge_colliding_vtab_finds_theirs" "10" \
  "$(run_sql "SELECT group_concat(rowid) FROM f5 WHERE f5 MATCH 'side';" "$DB")"
result=$(run_sql "INSERT INTO f5(f5) VALUES('integrity-check'); SELECT 'ok'; PRAGMA integrity_check;" "$DB")
check "merge_conflict_rebuilt_index_valid" "ok
ok" "$result"

scenario "clean fts5 shadow merge rebuilds from content"
newdb
run_sql "CREATE VIRTUAL TABLE docs USING fts5(body);
INSERT INTO docs(rowid,body) VALUES(270203,'row b27 00868 doc');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side');
INSERT INTO docs(rowid,body) VALUES(610229,'row b61 01070 doc');
INSERT INTO docs(docs) VALUES('rebuild');
SELECT dolt_commit('-Am','side');
SELECT dolt_checkout('main');
INSERT INTO docs(rowid,body) VALUES(900212,'row b90 01626 doc');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
check "merge_clean_shadow_content" "3" \
  "$(run_sql "SELECT count(*) FROM docs_content;" "$DB")"
check "merge_clean_shadow_search" "610229,900212" \
  "$(run_sql "SELECT group_concat(rowid) FROM (SELECT rowid FROM docs WHERE docs MATCH 'b61 OR b90' ORDER BY rowid);" "$DB")"
check "merge_clean_shadow_integrity" "" \
  "$(run_sql "INSERT INTO docs(docs) VALUES('integrity-check');" "$DB")"

scenario "merge adopts a branch-added vtab of every flavor"
newdb
run_sql "CREATE TABLE plain(k INTEGER PRIMARY KEY, v TEXT); INSERT INTO plain VALUES(1,'p');
SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); $FIXTURE SELECT dolt_commit('-Am','side adds vtabs');
SELECT dolt_checkout('main'); INSERT INTO plain VALUES(2,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null 2>&1
verify "merge_adopt_all_flavors" "$DB" "$BASE_STATE"

scenario "vtab dropped on a branch stays dropped after merge"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); DROP TABLE f4; SELECT dolt_commit('-Am','side drops f4');
SELECT dolt_checkout('main'); INSERT INTO plain VALUES(2,'m'); SELECT dolt_commit('-am','main');
SELECT dolt_merge('side');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM sqlite_master WHERE name LIKE 'f4%';
SELECT count(*) FROM f5 WHERE f5 MATCH 'delta'; PRAGMA integrity_check;" "$DB")
check "merge_drop_wins" "0
1
ok" "$result"

scenario "cherry-pick a vtab-content commit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); $MUTATE SELECT dolt_commit('-am','side mut');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick(dolt_hashof('side'));" "$DB" > /dev/null
verify "cherry_pick_vtab_content" "$DB" "$MUT_STATE"
verify_commit "cherry_pick_commit" "$DB" "$MUT_STATE"

scenario "revert a vtab-content commit"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');" "$DB" > /dev/null
run_sql "SELECT dolt_revert('HEAD');" "$DB" > /dev/null
verify "revert_vtab_content" "$DB" "$BASE_STATE"
verify_commit "revert_commit" "$DB" "$BASE_STATE"

scenario "rebase vtab commits onto an advanced main"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "SELECT dolt_checkout('-b','side'); $MUTATE SELECT dolt_commit('-am','side mut');
SELECT dolt_checkout('main'); INSERT INTO plain VALUES(2,'m'); SELECT dolt_commit('-am','main move');
SELECT dolt_checkout('side'); SELECT dolt_rebase('main');" "$DB" > /dev/null
verify "rebase_vtab_content" "$DB/side" "$MUT_STATE"
result=$(run_sql "SELECT v FROM plain WHERE k=2;" "$DB")
check "rebase_picked_up_main" "m" "$result"

scenario "clone with all vtab flavors"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
REMOTE="file://$TDIR/rem$N.db"
run_sql "SELECT dolt_remote('add','origin','$REMOTE'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
CLONE="$TDIR/clone$N.db"
run_sql "SELECT dolt_clone('$REMOTE');" "$CLONE" > /dev/null
verify "clone_all_flavors" "$CLONE" "$BASE_STATE"

scenario "pull vtab changes into a clone"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
REMOTE="file://$TDIR/rem$N.db"
run_sql "SELECT dolt_remote('add','origin','$REMOTE'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
CLONE="$TDIR/clone$N.db"
run_sql "SELECT dolt_clone('$REMOTE');" "$CLONE" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
run_sql "SELECT dolt_pull('origin','main');" "$CLONE" > /dev/null
verify "pull_vtab_changes" "$CLONE" "$MUT_STATE"

scenario "gc after vtab churn"
newdb
run_sql "$FIXTURE SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
run_sql "$MUTATE SELECT dolt_commit('-am','mut');
INSERT INTO f5(f5) VALUES('rebuild'); SELECT dolt_commit('-am','rebuild');
DROP TABLE f3; SELECT dolt_commit('-Am','drop f3');
SELECT dolt_gc();" "$DB" > /dev/null
result=$(run_sql "SELECT group_concat(rowid) FROM (SELECT rowid FROM f5 WHERE f5 MATCH 'delta' ORDER BY rowid);
INSERT INTO f5(f5) VALUES('integrity-check');
SELECT rtreecheck('main','rt');
PRAGMA integrity_check;" "$DB")
check "gc_after_churn" "3,4
ok
ok" "$result"
run_sql "SELECT dolt_branch('old','HEAD~3');" "$DB" > /dev/null
verify "gc_history_reachable" "$DB/old" "$BASE_STATE"

# Concurrent vtab writes rewrite shadows wholesale; merge rebuilds or refuses.
DBM=/tmp/test_vtab_merge_shadow_$$.db; rm -f "$DBM"
run_sql "CREATE VIRTUAL TABLE ft USING fts5(body);
INSERT INTO ft(rowid,body) VALUES(1,'alpha common'),(2,'beta common');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');" "$DBM" > /dev/null
run_sql "INSERT INTO ft(rowid,body) VALUES(101,'zebra one');
SELECT dolt_commit('-A','-m','b1');" "$DBM/b1" > /dev/null
run_sql "INSERT INTO ft(rowid,body) VALUES(201,'banana two');
SELECT dolt_commit('-A','-m','b2');" "$DBM/b2" > /dev/null
run_sql "SELECT dolt_merge('b1');" "$DBM" > /dev/null
result=$(run_sql "SELECT dolt_merge('b2');" "$DBM")
case "$result" in
  [0-9a-f]*) PASS=$((PASS+1));;
  *) note_fail "fts5_merge_rebuilds" "merge did not succeed: $result";;
esac
check "fts5_merge_finds_ours" "101" \
  "$(run_sql "SELECT group_concat(rowid) FROM ft WHERE ft MATCH 'zebra';" "$DBM")"
check "fts5_merge_finds_theirs" "201" \
  "$(run_sql "SELECT group_concat(rowid) FROM ft WHERE ft MATCH 'banana';" "$DBM")"
check "fts5_merge_content_complete" "4" \
  "$(run_sql "SELECT count(*) FROM ft_content;" "$DBM")"
check "fts5_merge_integrity" "" \
  "$(run_sql "INSERT INTO ft(ft) VALUES('integrity-check');" "$DBM")"
rm -f "$DBM"

# Contentless fts5 has no copy of the text; fts5 refuses 'rebuild'.
DBC=/tmp/test_vtab_merge_contentless_$$.db; rm -f "$DBC"
run_sql "CREATE VIRTUAL TABLE ftc USING fts5(body, content='');
INSERT INTO ftc(rowid,body) VALUES(1,'seed');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');" "$DBC" > /dev/null
run_sql "INSERT INTO ftc(rowid,body) VALUES(101,'apple pie');
SELECT dolt_commit('-A','-m','b1');" "$DBC/b1" > /dev/null
run_sql "INSERT INTO ftc(rowid,body) VALUES(201,'banana split');
SELECT dolt_commit('-A','-m','b2');" "$DBC/b2" > /dev/null
run_sql "SELECT dolt_merge('b1');" "$DBC" > /dev/null
result=$(run_sql "SELECT dolt_merge('b2');" "$DBC")
case "$result" in
  *"cannot be rebuilt"*) PASS=$((PASS+1));;
  *) note_fail "contentless_fts5_merge_refused" "expected a refusal, got: $result";;
esac
rm -f "$DBC"

# r-tree coords live in node blobs with no rebuild; colliding nodes cannot be regenerated.
DBR=/tmp/test_vtab_merge_rtree_$$.db; rm -f "$DBR"
run_sql "CREATE VIRTUAL TABLE rt2 USING rtree(id,x0,x1,y0,y1);
WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<300)
  INSERT INTO rt2 SELECT i, i%50, i%50+1, i/50, i/50+1 FROM c;
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');" "$DBR" > /dev/null
run_sql "INSERT INTO rt2 VALUES(1001,10.0,10.1,1.0,1.1);
SELECT dolt_commit('-A','-m','b1');" "$DBR/b1" > /dev/null
run_sql "INSERT INTO rt2 VALUES(2001,10.2,10.3,1.2,1.3);
SELECT dolt_commit('-A','-m','b2');" "$DBR/b2" > /dev/null
run_sql "SELECT dolt_merge('b1');" "$DBR" > /dev/null
result=$(run_sql "SELECT dolt_merge('b2');" "$DBR")
case "$result" in
  *"cannot be rebuilt"*) PASS=$((PASS+1));;
  *) note_fail "rtree_merge_refused" "expected a refusal, got: $result";;
esac
check "rtree_refusal_leaves_tree_intact" "ok" \
  "$(run_sql "SELECT rtreecheck('rt2');" "$DBR")"
# Refusal must not undo the merge that already landed.
check "rtree_refusal_keeps_first_merge" "1001" \
  "$(run_sql "SELECT group_concat(id) FROM rt2 WHERE id>1000;" "$DBR")"
rm -f "$DBR"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
