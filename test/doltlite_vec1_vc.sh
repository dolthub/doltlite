#!/bin/bash
# vec1 x version-control: the built-in vector extension against the VC
# surfaces, plus the properties doltlite's versioning story depends on.
# vec1's %_base keeps one row per rowid until 'rebuild' migrates vectors
# into %_idx segment blobs, so: raw bases row-merge across branches; built
# indexes conflict on their derived segments and are recovered by
# rebuilding from an authoritative source; and rebuilds are deterministic,
# which content-addressed storage turns into free no-op recompactions.
DOLTLITE="${1:-./doltlite}"
PASS=0
FAIL=0
ERRORS=""

run_sql() {
  echo "$1" | perl -e 'alarm(30); exec @ARGV' $DOLTLITE "$2" 2>&1
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

# Four 4-dim float32 vectors as little-endian blobs:
#   V1=[1,0,0,0]  V2=[0,1,0,0]  V3=[0,0,1,0]  V4=[0,0,0,1]
V1="x'0000803f000000000000000000000000'"
V2="x'000000000000803f0000000000000000'"
V3="x'00000000000000000000803f00000000'"
V4="x'0000000000000000000000000000803f'"

TDIR=$(mktemp -d /tmp/vec1vc.XXXXXX)
trap 'rm -rf "$TDIR"' EXIT
N=0
newdb() { N=$((N+1)); DB="$TDIR/v$N.db"; }

scenario() { echo "--- $1 ---"; }

scenario "built in: no .load required"
newdb
result=$(run_sql "SELECT vec1_info() LIKE 'version%';
CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2), (3, $V3);
SELECT rowid FROM t($V1, '{k: 1}');
SELECT count(*) FROM sqlite_master WHERE name LIKE 't\_%' ESCAPE '\';
PRAGMA integrity_check;" "$DB")
check "vec1_builtin_smoke" "1
1
6
ok" "$result"

scenario "flat index: commit, branch isolation, historical KNN"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','two vectors');
SELECT dolt_checkout('-b','side');
INSERT INTO t(rowid, vector) VALUES (3, $V3);
SELECT dolt_commit('-am','side adds 3');" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V3, '{k: 1}'); PRAGMA integrity_check;" "$DB/side")
check "vec1_branch_knn" "3
ok" "$result"
result=$(run_sql "SELECT rowid FROM t($V3, '{k: 1}'); SELECT count(*) FROM t_base;" "$DB")
check "vec1_main_isolated" "1
2" "$result"

scenario "trained model: bucketed KNN through reset --hard"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2), (3, $V3), (4, $V4);
CREATE TABLE m(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO m SELECT 1, vec1_train(vector, '{nbucket: 2, distance: \"l2\"}') FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));
SELECT dolt_commit('-Am','trained');" "$DB" > /dev/null
run_sql "INSERT INTO t(rowid, vector) VALUES (9, $V4); SELECT dolt_reset('--hard');" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V4, '{k: 1, nprobe: 2}');
SELECT count(*) FROM t_base; PRAGMA integrity_check;" "$DB")
check "vec1_trained_reset_hard" "4
4
ok" "$result"

scenario "raw bases row-merge across branches"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','left'); INSERT INTO t(rowid, vector) VALUES (10, $V2);
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector) VALUES (20, $V3);
SELECT dolt_commit('-am','right');
SELECT dolt_checkout('left');
SELECT dolt_merge('right');" "$DB" > /dev/null
result=$(run_sql "SELECT count(*) FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT rowid FROM t($V3, '{k: 1}'); PRAGMA integrity_check;" "$DB/left")
check "vec1_raw_base_merges" "3
20
ok" "$result"

scenario "built indexes conflict on derived segments; source-table rebuild recovers"
newdb
run_sql "CREATE TABLE docs(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO docs VALUES (1, $V1), (2, $V2);
CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) SELECT id, v FROM docs;
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','built');
SELECT dolt_checkout('-b','left');
INSERT INTO docs VALUES (10, $V3); INSERT INTO t(rowid, vector) VALUES (10, $V3);
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO docs VALUES (20, $V4); INSERT INTO t(rowid, vector) VALUES (20, $V4);
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left');
BEGIN;
SELECT dolt_merge('right');
SELECT count(*) > 0 FROM dolt_conflicts;
ROLLBACK;" "$DB" | grep -cE "^1$|conflict")
check "vec1_built_index_conflicts" "2" "$result"
run_sql "SELECT dolt_checkout('left');
BEGIN;
SELECT dolt_merge('right');
SELECT CASE WHEN (SELECT count(*) FROM dolt_conflicts)>0
  THEN dolt_conflicts_resolve('--ours', (SELECT \"table\" FROM dolt_conflicts LIMIT 1)) END;
SELECT CASE WHEN (SELECT count(*) FROM dolt_conflicts)>0
  THEN dolt_conflicts_resolve('--ours', (SELECT \"table\" FROM dolt_conflicts LIMIT 1)) END;
DROP TABLE t;
CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) SELECT id, v FROM docs;
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','merged and rebuilt from docs');" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V3, '{k: 1}');
SELECT rowid FROM t($V4, '{k: 1}');
SELECT count(*) FROM docs; PRAGMA integrity_check;" "$DB/left")
check "vec1_source_rebuild_recovers_both" "10
20
4
ok" "$result"

scenario "PQ config: base stays authoritative, merges are lossless"
newdb
python3 -c "
import struct, random
random.seed(3)
with open('$TDIR/pq600.sql','w') as f:
    for i in range(1, 601):
        v = [random.uniform(-1,1) for _ in range(8)]
        blob = ''.join(f'{b:02x}' for b in struct.pack('<8f', *v))
        f.write(f\"INSERT INTO t(rowid, vector) VALUES ({i}, x'{blob}');\n\")
"
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
.read $TDIR/pq600.sql
CREATE TABLE m(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO m SELECT 1, vec1_train(vector, '{nbucket: 8, codesize: 4, distance: \"l2\"}') FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));
SELECT dolt_commit('-Am','built PQ index');
SELECT dolt_checkout('-b','left');
INSERT INTO t(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector) VALUES (8001, x'0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f');
SELECT dolt_commit('-am','right, same bucket');" "$DB" > /dev/null
# With codesize>0 the raw vectors never migrate out of %_base, so it
# row-merges with full fidelity and the only conflicts are on derived
# %_idx segments. The merge absorbs those and rebuilds from the merged
# base and stored model itself: no conflicts surface, and both branches'
# vectors come out searchable.
result=$(run_sql "SELECT dolt_checkout('left');
SELECT length(dolt_merge('right'))=40;
SELECT count(*) FROM dolt_conflicts;
SELECT (SELECT count(*) FROM t_base WHERE id IN (7001,8001))
    || '|' || (SELECT count(*) FROM t_base WHERE length(vector)=32);" "$DB")
check "vec1_pq_auto_merge" "0
1
0
2|602" "$result"
# Approximate PQ k=1 can return the other same-bucket row; both must still
# appear in a wider probe so the merge did not drop either vector.
result=$(run_sql "SELECT count(*) FROM t(x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f', '{k: 16, nprobe: 8}') WHERE rowid=7001;
SELECT count(*) FROM t(x'0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f', '{k: 16, nprobe: 8}') WHERE rowid=8001;
SELECT message FROM dolt_log LIMIT 1;
PRAGMA integrity_check;" "$DB/left")
check "vec1_pq_lossless_merge" "1
1
Merge branch 'right' into left
ok" "$result"

scenario "uncompressed configs still conflict loudly"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','flat built');
SELECT dolt_checkout('-b','left'); INSERT INTO t(rowid, vector) VALUES (10, $V2);
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector) VALUES (20, $V3);
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
# Migrated bases hold bucket numbers, not vectors; auto-resolving here
# would silently lose the discarded side's data, so the merge must not.
result=$(run_sql "SELECT dolt_checkout('left'); SELECT dolt_merge('right');" "$DB" | grep -c "conflict")
check "vec1_flat_guard_stays_loud" "1" "$result"

scenario "a missing stored model disables the automation"
newdb
python3 -c "
import struct, random
random.seed(3)
with open('$TDIR/gate600.sql','w') as f:
    for i in range(1, 601):
        v = [random.uniform(-1,1) for _ in range(8)]
        blob = ''.join(f'{b:02x}' for b in struct.pack('<8f', *v))
        f.write(f\"INSERT INTO t(rowid, vector) VALUES ({i}, x'{blob}');\n\")
"
# vec1 treats a NULL rebuild argument as keep-the-current-model, so an
# owner whose stored model row is gone must stay in the manual-conflict
# path rather than let the merge commit a stale index.
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
.read $TDIR/gate600.sql
CREATE TABLE m(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO m SELECT 1, vec1_train(vector, '{nbucket: 8, codesize: 4, distance: \"l2\"}') FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));
SELECT dolt_commit('-Am','built');
SELECT dolt_checkout('-b','left');
INSERT INTO t(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
DELETE FROM t_model WHERE id=1;
SELECT dolt_commit('-am','left, model removed');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector) VALUES (8001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left'); SELECT dolt_merge('right');" "$DB" | grep -c "conflict")
check "vec1_missing_model_stays_loud" "1" "$result"

scenario "a user-table conflict keeps every conflict manual"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES (1,'base');
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','left');
INSERT INTO t(rowid, vector) VALUES (10, $V3); UPDATE u SET v='left' WHERE id=1;
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector) VALUES (20, $V4); UPDATE u SET v='right' WHERE id=1;
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left');
BEGIN;
SELECT dolt_merge('right');
SELECT count(*) FROM dolt_conflicts WHERE \"table\"='u';
ROLLBACK;" "$DB" | grep -cE "^1$|conflict")
check "vec1_mixed_conflicts_stay_manual" "2" "$result"

scenario "rebuild is deterministic across insert orders"
newdb
DB2="$TDIR/det2.db"
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2), (3, $V3), (4, $V4);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');" "$DB" > /dev/null
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (4, $V4), (3, $V3), (2, $V2), (1, $V1);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');" "$DB2" > /dev/null
A=$(run_sql "SELECT id, bucket, first, last, hex(val) FROM t_idx ORDER BY id;" "$DB")
B=$(run_sql "SELECT id, bucket, first, last, hex(val) FROM t_idx ORDER BY id;" "$DB2")
if [ -n "$A" ] && [ "$A" = "$B" ]; then
  PASS=$((PASS+1))
else
  note_fail "vec1_rebuild_deterministic" "idx dumps differ or empty"
fi

scenario "table-level checkout of a vec1 table"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','side');
INSERT INTO t(rowid, vector) VALUES (2, $V2);
SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main');
SELECT dolt_checkout('side','t');" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V2, '{k: 1}'); PRAGMA integrity_check;" "$DB")
check "vec1_table_checkout" "2
ok" "$result"

scenario "clone and pull carry the vector index"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','base');" "$DB" > /dev/null
REMOTE="file://$TDIR/rem$N.db"
run_sql "SELECT dolt_remote('add','origin','$REMOTE'); SELECT dolt_push('origin','main');" "$DB" > /dev/null
CLONE="$TDIR/clone$N.db"
run_sql "SELECT dolt_clone('$REMOTE');" "$CLONE" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V2, '{k: 1}'); PRAGMA integrity_check;" "$CLONE")
check "vec1_clone" "2
ok" "$result"
run_sql "INSERT INTO t(rowid, vector) VALUES (3, $V3); SELECT dolt_commit('-am','more');
SELECT dolt_push('origin','main');" "$DB" > /dev/null
run_sql "SELECT dolt_pull('origin','main');" "$CLONE" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V3, '{k: 1}');" "$CLONE")
check "vec1_pull" "3" "$result"

scenario "gc keeps live segments and history reachable"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1), (2, $V2);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','base');
INSERT INTO t(rowid, vector) VALUES (3, $V3);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-am','rebuilt');
SELECT dolt_branch('old','HEAD~1');
SELECT dolt_gc();" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V3, '{k: 1}'); PRAGMA integrity_check;" "$DB")
check "vec1_gc_live" "3
ok" "$result"
result=$(run_sql "SELECT count(*) FROM t_base; SELECT rowid FROM t($V1, '{k: 1}');" "$DB/old")
check "vec1_gc_history" "2
1" "$result"

scenario "drop and restore from history"
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','base');
DROP TABLE t;
SELECT dolt_commit('-Am','dropped');
SELECT dolt_checkout('HEAD~1','t');" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t($V1, '{k: 1}'); PRAGMA integrity_check;" "$DB")
check "vec1_drop_restore" "1
ok" "$result"

# ── second wave: divergence shapes and remaining VC surfaces ──────
# Shared 600-vector fixtures for the PQ scenarios below (training needs
# at least 512 vectors). gen600 <file> [table] [meta] writes inserts.
gen600() {
  python3 -c "
import struct, random
random.seed(3)
tbl = '$2' or 't'
meta = '$3' == 'meta'
with open('$1','w') as f:
    for i in range(1, 601):
        v = [random.uniform(-1,1) for _ in range(8)]
        blob = ''.join(f'{b:02x}' for b in struct.pack('<8f', *v))
        if meta:
            f.write(f\"INSERT INTO {tbl}(rowid, vector, c0) VALUES ({i}, x'{blob}', {i%7});\n\")
        else:
            f.write(f\"INSERT INTO {tbl}(rowid, vector) VALUES ({i}, x'{blob}');\n\")
"
}
PQ_BUILD="CREATE TABLE m(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO m SELECT 1, vec1_train(vector, '{nbucket: 8, codesize: 4, distance: \"l2\"}') FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));"

scenario "deletes merge against inserts and survive the rebuild"
newdb
gen600 "$TDIR/w600.sql"
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
.read $TDIR/w600.sql
$PQ_BUILD
SELECT dolt_commit('-Am','built');
SELECT dolt_checkout('-b','left');
DELETE FROM t WHERE rowid = (SELECT rowid FROM t(x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f', '{k:1, nprobe:8}'));
SELECT dolt_commit('-am','left deletes the target bucket neighbor');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','right inserts into that bucket');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left');
SELECT length(dolt_merge('right'))=40;
SELECT count(*) FROM t_base;
SELECT rowid FROM t(x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f', '{k:1, nprobe:8}');
PRAGMA integrity_check;" "$DB")
check "vec1_delete_vs_insert_merge" "0
1
600
7001
ok" "$result"

scenario "both branches retraining converges on ours' model"
newdb
gen600 "$TDIR/w600.sql"
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
.read $TDIR/w600.sql
$PQ_BUILD
SELECT dolt_commit('-Am','built');
SELECT dolt_checkout('-b','left');
INSERT INTO m SELECT 2, vec1_train(vector, '{nbucket: 4, codesize: 4, distance: \"l2\"}') FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', (SELECT v FROM m WHERE id=2));
SELECT dolt_commit('-am','left retrains nbucket=4');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO m SELECT 3, vec1_train(vector, '{nbucket: 16, codesize: 4, distance: \"l2\"}') FROM t_base;
INSERT INTO t(cmd, arg) VALUES ('rebuild', (SELECT v FROM m WHERE id=3));
SELECT dolt_commit('-am','right retrains nbucket=16');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left');
SELECT length(dolt_merge('right'))=40;
SELECT count(DISTINCT bucket) FROM t_idx;
SELECT rowid FROM t((SELECT vector FROM t_base WHERE id=42), '{k:1, nprobe:16}');
PRAGMA integrity_check;" "$DB")
check "vec1_dual_retrain_ours_model" "0
1
4
42
ok" "$result"

scenario "two vector tables rebuild in one merge"
newdb
gen600 "$TDIR/wa600.sql" a
gen600 "$TDIR/wb600.sql" b
run_sql "CREATE VIRTUAL TABLE a USING vec1(vector);
CREATE VIRTUAL TABLE b USING vec1(vector);
.read $TDIR/wa600.sql
.read $TDIR/wb600.sql
CREATE TABLE m(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO m SELECT 1, vec1_train(vector, '{nbucket: 8, codesize: 4, distance: \"l2\"}') FROM a_base;
INSERT INTO a(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));
INSERT INTO b(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));
SELECT dolt_commit('-Am','two built tables');
SELECT dolt_checkout('-b','left');
INSERT INTO a(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
INSERT INTO b(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO a(rowid, vector) VALUES (8001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
INSERT INTO b(rowid, vector) VALUES (8001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left');
CREATE TEMP TABLE vec1_premerge(a TEXT, b TEXT);
INSERT INTO vec1_premerge VALUES(dolt_hashof_table('a_idx'), dolt_hashof_table('b_idx'));
SELECT length(dolt_merge('right'))=40;
SELECT (dolt_hashof_table('a_idx') != (SELECT a FROM vec1_premerge))
    || '|' || (dolt_hashof_table('b_idx') != (SELECT b FROM vec1_premerge));
SELECT (SELECT count(*) FROM a_base WHERE rowid IN (7001,8001))
    || '|' || (SELECT count(*) FROM b_base WHERE rowid IN (7001,8001));
PRAGMA integrity_check;" "$DB")
check "vec1_multi_table_merge" "0
1
1|1
2|2
ok" "$result"

scenario "one ineligible table keeps the whole merge manual"
newdb
gen600 "$TDIR/wa600.sql" a
run_sql "CREATE VIRTUAL TABLE a USING vec1(vector);
CREATE VIRTUAL TABLE f USING vec1(vector);
.read $TDIR/wa600.sql
CREATE TABLE m(id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO m SELECT 1, vec1_train(vector, '{nbucket: 8, codesize: 4, distance: \"l2\"}') FROM a_base;
INSERT INTO a(cmd, arg) VALUES ('rebuild', (SELECT v FROM m));
INSERT INTO f(rowid, vector) VALUES (1, x'0000803f000000000000000000000000');
INSERT INTO f(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','one PQ one flat');
SELECT dolt_checkout('-b','left');
INSERT INTO a(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
INSERT INTO f(rowid, vector) VALUES (10, x'000000000000803f0000000000000000');
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO a(rowid, vector) VALUES (8001, x'0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f');
INSERT INTO f(rowid, vector) VALUES (20, x'00000000000000000000803f00000000');
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left'); SELECT dolt_merge('right');" "$DB" | grep -c "conflict")
check "vec1_mixed_eligibility_stays_loud" "1" "$result"

scenario "cherry-pick and revert carry vector content"
newdb
gen600 "$TDIR/w600.sql"
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
.read $TDIR/w600.sql
$PQ_BUILD
SELECT dolt_commit('-Am','built');
SELECT dolt_checkout('-b','side');
INSERT INTO t(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','side adds 7001');
SELECT dolt_checkout('main');" "$DB" > /dev/null
result=$(run_sql "SELECT length(dolt_cherry_pick(dolt_hashof('side')))>0;
SELECT rowid FROM t(x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f', '{k:1, nprobe:8}');
SELECT length(dolt_revert('HEAD'))>0;
SELECT count(*) FROM t_base;
PRAGMA integrity_check;" "$DB")
check "vec1_cherry_pick_revert" "1
7001
1
600
ok" "$result"

scenario "a conflicting cherry-pick stays manual"
# Flat (uncompressed) builds leave bucket numbers in %_base, so derived-shadow
# auto-absorb is ineligible — same guard as vec1_flat_guard_stays_loud. Cherry-
# pick also never requests the rebuild list (unlike merge). Use flat + dual
# inserts rather than PQ "same bucket" inserts: which PQ segment two vectors
# share is platform-sensitive under train, and was flaking the loudness check
# on macOS CI when the inserts did not actually conflict.
newdb
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
INSERT INTO t(rowid, vector) VALUES (1, $V1);
INSERT INTO t(cmd, arg) VALUES ('rebuild', '{index:\"flat\", distance:\"l2\"}');
SELECT dolt_commit('-Am','flat built');
SELECT dolt_checkout('-b','side');
INSERT INTO t(rowid, vector) VALUES (10, $V2);
SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main');
INSERT INTO t(rowid, vector) VALUES (20, $V3);
SELECT dolt_commit('-am','main');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_cherry_pick(dolt_hashof('side'));" "$DB" | grep -c "conflict")
check "vec1_cherry_pick_conflict_stays_loud" "1" "$result"

scenario "rebase replays vector commits"
newdb
gen600 "$TDIR/w600.sql"
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector);
.read $TDIR/w600.sql
$PQ_BUILD
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','built');
SELECT dolt_checkout('-b','side');
INSERT INTO t(rowid, vector) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f');
SELECT dolt_commit('-am','side vector');
SELECT dolt_checkout('main');
INSERT INTO u VALUES (1, 'main moved');
SELECT dolt_commit('-am','main move');
SELECT dolt_checkout('side');
SELECT dolt_rebase('main');" "$DB" > /dev/null
result=$(run_sql "SELECT rowid FROM t(x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f', '{k:1, nprobe:8}');
SELECT v FROM u WHERE id=1;
PRAGMA integrity_check;" "$DB/side")
check "vec1_rebase" "7001
main moved
ok" "$result"

scenario "metadata columns rebuild through the automatic merge"
newdb
gen600 "$TDIR/wm600.sql" t meta
run_sql "CREATE VIRTUAL TABLE t USING vec1(vector, c0);
.read $TDIR/wm600.sql
$PQ_BUILD
SELECT dolt_commit('-Am','built with meta');
SELECT dolt_checkout('-b','left');
INSERT INTO t(rowid, vector, c0) VALUES (7001, x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f', 3);
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('main'); SELECT dolt_checkout('-b','right');
INSERT INTO t(rowid, vector, c0) VALUES (8001, x'0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f', 5);
SELECT dolt_commit('-am','right');" "$DB" > /dev/null
result=$(run_sql "SELECT dolt_checkout('left');
SELECT length(dolt_merge('right'))=40;
SELECT rowid FROM t(x'0000803f0000803f0000803f0000803f0000803f0000803f0000803f0000803f','{k:5, nprobe:8}') WHERE c0=3 LIMIT 1;
SELECT rowid FROM t(x'0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f0ad7a33f','{k:5, nprobe:8}') WHERE c0=5 LIMIT 1;
PRAGMA integrity_check;" "$DB")
check "vec1_meta_columns_auto_merge" "0
1
7001
8001
ok" "$result"

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
