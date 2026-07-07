#!/bin/bash
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"
db_size() { local s=0; for f in "$1" "${1}-wal"; do [ -f "$f" ] && s=$((s + $(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null))); done; echo $s; }
db_rm() { rm -f "$1" "${1}-wal" "${1}-lock"; }

echo "=== Doltlite Garbage Collection Tests ==="
echo ""

DB=/tmp/test_gc_clean_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_clean" "SELECT dolt_gc();" "0 chunks removed" "$DB"
run_test "gc_clean_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_clean_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

nFiles=$(ls "$DB"* 2>/dev/null | grep -v -- '-lock$' | wc -l | tr -d ' ')
if [ "$nFiles" = "1" ]; then PASS=$((PASS+1))
else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_single_file\n  expected: 1 file\n  got:      $nFiles files"; fi

db_rm "$DB"

DB=/tmp/test_gc_multi_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_commit('-A','-m','c3');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_BEFORE=$(db_size "$DB")

run_test_match "gc_multi_result" "SELECT dolt_gc();" "chunks removed.*chunks kept" "$DB"

SIZE_AFTER=$(db_size "$DB")

if [ "$SIZE_AFTER" -le "$SIZE_BEFORE" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_multi_smaller\n  before: $SIZE_BEFORE\n  after:  $SIZE_AFTER"; fi

run_test "gc_multi_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "gc_multi_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"
run_test "gc_multi_val" "SELECT v FROM t WHERE id=3;" "c" "$DB"

run_test "gc_multi_reopen_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test "gc_multi_reopen_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_102_byte_commit_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init','--author','beads <beads@local>');
UPDATE t SET v='b' WHERE id=1;
SELECT dolt_commit('-A','-m','gc update bead td-wisp-gmg4agp','--author','beads <beads@local>');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_102_byte_commit" "SELECT dolt_gc();" "chunks" "$DB"
run_test "gc_102_byte_commit_integrity" "PRAGMA integrity_check;" "ok" "$DB"
run_test "gc_102_byte_commit_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_branch_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_connect_branch('feat');
INSERT INTO t VALUES(100,'feat_only');
SELECT dolt_commit('-A','-m','feat commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

SIZE_WITH_BRANCH=$(db_size "$DB")

echo "SELECT dolt_branch('-d','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_branch_result" "SELECT dolt_gc();" "chunks removed" "$DB"

SIZE_AFTER_GC=$(db_size "$DB")

if [ "$SIZE_AFTER_GC" -lt "$SIZE_WITH_BRANCH" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_branch_smaller\n  with_branch: $SIZE_WITH_BRANCH\n  after_gc:    $SIZE_AFTER_GC"; fi

run_test "gc_branch_data" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_branch_val" "SELECT v FROM t WHERE id=1;" "a" "$DB"
run_test "gc_branch_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"

run_test "gc_branch_no_feat" "SELECT count(*) FROM t WHERE id=100;" "0" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_preserve_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main_data');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_connect_branch('dev');
INSERT INTO t VALUES(2,'dev_data');
SELECT dolt_commit('-A','-m','dev commit');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_tag('v1.0');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_preserve_branches" "SELECT count(*) FROM dolt_branches;" "2" "$DB"

run_test "gc_preserve_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"

echo "SELECT dolt_checkout('dev');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "gc_preserve_dev" "SELECT dolt_connect_branch('dev'); SELECT count(*) FROM t;" "0
2" "$DB"

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "gc_preserve_reopen_main" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_preserve_reopen_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_updates_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v0');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

for i in $(seq 1 20); do
  echo "UPDATE t SET v='v$i' WHERE id=1;
SELECT dolt_commit('-A','-m','update $i');" | $DOLTLITE "$DB" > /dev/null 2>&1
done

SIZE_BEFORE=$(db_size "$DB")

run_test_match "gc_updates_result" "SELECT dolt_gc();" "chunks removed" "$DB"

SIZE_AFTER=$(db_size "$DB")

if [ "$SIZE_AFTER" -le "$SIZE_BEFORE" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: gc_updates_smaller\n  before: $SIZE_BEFORE\n  after:  $SIZE_AFTER"; fi

run_test "gc_updates_val" "SELECT v FROM t WHERE id=1;" "v20" "$DB"
run_test "gc_updates_log" "SELECT count(*) FROM dolt_log;" "22" "$DB"

run_test_match "gc_updates_first_msg" \
  "SELECT message FROM dolt_log LIMIT 1;" "update 20" "$DB"

db_rm "$DB"

run_test_match "gc_memory" \
  "SELECT dolt_gc();" "in-memory" ":memory:"

DB=/tmp/test_gc_diff_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_diff_works" \
  "SELECT coalesce(sum(rows_added + rows_deleted + rows_modified), 0) FROM dolt_diff_stat((SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1), (SELECT commit_hash FROM dolt_log LIMIT 1), 't');" \
  "^[1-9]" "$DB"

echo "INSERT INTO t VALUES(3,'c');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "gc_diff_working" "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';" "1" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1
db_rm "$DB"

DB=/tmp/test_gc_merge_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_branch('-d','feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_merge_result" "SELECT dolt_gc();" "chunks" "$DB"

run_test "gc_merge_count" "SELECT count(*) FROM t;" "3" "$DB"
run_test_match "gc_merge_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"

run_test "gc_merge_reopen" "SELECT count(*) FROM t;" "3" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_cprv_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'init');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat_row');
SELECT dolt_commit('-A','-m','feat add');
SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_cherry_pick('feat');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_cprv_result" "SELECT dolt_gc();" "chunks" "$DB"

run_test "gc_cprv_count" "SELECT count(*) FROM t;" "1" "$DB"
run_test "gc_cprv_log" "SELECT count(*) FROM dolt_log;" "4" "$DB"

run_test "gc_cprv_reopen" "SELECT count(*) FROM t;" "1" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_tables_$$.db; db_rm "$DB"
echo "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES(1,'a1');
INSERT INTO b VALUES(1,'b1');
INSERT INTO c VALUES(1,'c1');
SELECT dolt_commit('-A','-m','init');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "INSERT INTO a VALUES(2,'a2');
INSERT INTO b VALUES(2,'b2');
INSERT INTO c VALUES(2,'c2');
SELECT dolt_commit('-A','-m','add more');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test_match "gc_tables_result" "SELECT dolt_gc();" "chunks" "$DB"

run_test "gc_tables_a" "SELECT count(*) FROM a;" "2" "$DB"
run_test "gc_tables_b" "SELECT count(*) FROM b;" "2" "$DB"
run_test "gc_tables_c" "SELECT count(*) FROM c;" "2" "$DB"

run_test "gc_tables_reopen_a" "SELECT count(*) FROM a;" "2" "$DB"
run_test "gc_tables_reopen_b" "SELECT count(*) FROM b;" "2" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_schema_rootpages_$$.db; db_rm "$DB"
echo "PRAGMA foreign_keys=ON;
CREATE TABLE a(
  id TEXT PRIMARY KEY,
  x TEXT NOT NULL
);
CREATE TABLE b(
  id TEXT PRIMARY KEY,
  a_id TEXT NOT NULL REFERENCES a(id),
  y TEXT NOT NULL
);
CREATE TABLE c(
  a_id TEXT NOT NULL REFERENCES a(id),
  b_id TEXT NOT NULL REFERENCES b(id),
  y TEXT NOT NULL,
  z INTEGER NOT NULL CHECK(z >= 0),
  PRIMARY KEY(a_id,b_id)
) WITHOUT ROWID;
CREATE INDEX idx_b_a ON b(a_id);
CREATE INDEX idx_c_y ON c(y);
SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_schema_rootpage_reopen" \
  "SELECT count(*) FROM sqlite_master WHERE name='idx_c_y';" "1" "$DB"
run_test_match "gc_schema_rootpage_second_gc" \
  "SELECT dolt_gc();" "chunks" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_idem_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','c1');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-A','-m','c2');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "gc_idem_second" "SELECT dolt_gc();" "0 chunks removed" "$DB"
run_test "gc_idem_data" "SELECT count(*) FROM t;" "2" "$DB"

db_rm "$DB"

DB=/tmp/test_gc_taghist_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'v1');
SELECT dolt_commit('-A','-m','release v1');
SELECT dolt_tag('v1.0');
INSERT INTO t VALUES(2,'v2');
SELECT dolt_commit('-A','-m','release v2');
SELECT dolt_tag('v2.0');
INSERT INTO t VALUES(3,'v3');
SELECT dolt_commit('-A','-m','release v3');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_gc();" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "gc_taghist_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test "gc_taghist_data" "SELECT count(*) FROM t;" "3" "$DB"

run_test_match "gc_taghist_diff" \
  "SELECT coalesce(sum(rows_added + rows_deleted + rows_modified), 0) FROM dolt_diff_stat((SELECT tag_hash FROM dolt_tags WHERE tag_name='v1.0'), (SELECT tag_hash FROM dolt_tags WHERE tag_name='v2.0'), 't');" \
  "^[1-9]" "$DB"

db_rm "$DB"

# A mark failure must name the unresolvable chunk (hash, source, rc), not
# just "gc mark phase failed" — that hash is what makes a field report
# actionable. Flipping the first WAL record's tag byte makes the chunks
# behind it unreachable while the store still opens.
DB=/tmp/test_gc_mark_diag_$$.db; db_rm "$DB"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','seed');" | $DOLTLITE "$DB" > /dev/null 2>&1
printf '\xff' | dd of="$DB" bs=1 seek=168 conv=notrunc 2>/dev/null

run_test_match "gc_mark_failure_names_missing_chunk" \
  "SELECT dolt_gc();" \
  "missing chunk [0-9a-f]{40}.*source=.*rc=" \
  "$DB"

db_rm "$DB"

dltest_finish
