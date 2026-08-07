#!/bin/bash
# Virtual-table x version-control matrix: every VC surface against every
# built-in vtab flavor. Virtual tables have no catalog entry of their own —
# their storage is the shadow tables and their schema row is storage-free —
# so by-entry catalog rebuilds (merge, cherry-pick, revert, rebase, clone)
# are structurally blind to them. This suite is the intent-level oracle for
# that bug class, plus shadow-table content fidelity across every surface:
# each step re-checks module self-checks (fts5/fts4 integrity-check,
# rtreecheck), MATCH/bbox results, integrity_check, and fresh-session
# agreement after schema replay.
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

# Every built-in vtab flavor with distinct shadow-table shapes: fts5
# (blob segments + WITHOUT ROWID idx), fts5 external-content (shadows
# depend on a real table), fts4 (segdir composite keys + stat), fts3
# (no docsize/stat), rtree (fixed-width node blobs).
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

# Index-shape-churning mutations for every flavor: new docs, an updated
# doc (delete+insert through the index), a delete, an fts5 optimize
# (rewrites segment shadows wholesale), and an rtree move.
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

# One line per flavor: "<name>|<actual>"; expectations are supplied by the
# caller because fixture and mutated states differ. Module self-checks are
# folded in: fts5/fts4 'integrity-check' raises on a malformed index and
# rtreecheck returns 'ok' only for a consistent tree.
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

# verify <label> <db> <expected-state>: run VERIFY_SQL in a fresh session
# (schema replay included) and compare the whole projection.
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

# verify_commit <label> <db> <expected-state>: materialize HEAD on a copy,
# so committed catalogs are checked independently of live session state.
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

# ── staging surfaces ──────────────────────────────────────────────
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

# ── reset surfaces ────────────────────────────────────────────────
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

# ── branch / checkout ─────────────────────────────────────────────
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

# ── merge surfaces ────────────────────────────────────────────────
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

scenario "conflicted merge: resolve then rebuild the index"
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
SELECT 'TX|' || (SELECT count(*)>0 FROM dolt_conflicts) || '|' || (SELECT count(*) FROM sqlite_master WHERE name='f5');
ROLLBACK;" "$DB" | grep '^TX|')
check "merge_conflict_pending_state" "TX|1|1" "$result"
# The documented recovery: take one side's shadows wholesale, then rebuild
# the index from its content table so the segments match the row set.
result=$(run_sql "BEGIN;
SELECT dolt_merge('side');
SELECT dolt_conflicts_resolve('--theirs', (SELECT \"table\" FROM dolt_conflicts LIMIT 1));
SELECT CASE WHEN (SELECT count(*) FROM dolt_conflicts)>0
  THEN dolt_conflicts_resolve('--theirs', (SELECT \"table\" FROM dolt_conflicts LIMIT 1)) END;
SELECT CASE WHEN (SELECT count(*) FROM dolt_conflicts)>0
  THEN dolt_conflicts_resolve('--theirs', (SELECT \"table\" FROM dolt_conflicts LIMIT 1)) END;
INSERT INTO f5(f5) VALUES('rebuild');
SELECT length(dolt_commit('-Am','resolved + rebuilt'))=40;" "$DB" | tail -1)
check "merge_conflict_resolve_rebuild_commit" "1" "$result"
result=$(run_sql "INSERT INTO f5(f5) VALUES('integrity-check'); SELECT 'ok'; PRAGMA integrity_check;" "$DB")
check "merge_conflict_rebuilt_index_valid" "ok
ok" "$result"

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

# ── cherry-pick / revert / rebase ─────────────────────────────────
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

# ── remotes ───────────────────────────────────────────────────────
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

# ── gc ────────────────────────────────────────────────────────────
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

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
