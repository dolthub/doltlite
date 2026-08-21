#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

db_rm() {
  rm -rf "$1" "${1}-wal" "${1}-lock"
}

echo "=== GC/integrity over commits that collide with the V2 working-set size ==="
echo ""

# COMMIT_V2 and WS_V2 both start with byte 2; a 102-byte one-parent commit used to classify as a working set.

DB=/tmp/test_gc_commit_classify_$$.db; db_rm "$DB"

# author 'beads <beads@local>' (5+11) + 30-char message = 46 string bytes.
run_test_match "gc_ok_with_102_byte_commit" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (1,'x');
   SELECT dolt_commit('-A', '-m', 'seed') IS NOT NULL;
   UPDATE t SET v='y' WHERE k=1;
   SELECT dolt_commit('-A', '-m', 'gc update bead td-wisp-gmg4agp', '--author', 'beads <beads@local>') IS NOT NULL;
   SELECT dolt_gc();" \
  "chunks removed" \
  "$DB"

run_test_lastline "integrity_ok_with_102_byte_commit" \
  "PRAGMA integrity_check;" \
  "ok" \
  "$DB"

db_rm "$DB"

run_test_match "gc_ok_at_neighboring_commit_sizes" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (1,'x');
   SELECT dolt_commit('-A', '-m', 'seed') IS NOT NULL;
   UPDATE t SET v='a' WHERE k=1;
   SELECT dolt_commit('-A', '-m', 'gc update bead td-wisp-gmg4ag', '--author', 'beads <beads@local>') IS NOT NULL;
   UPDATE t SET v='b' WHERE k=1;
   SELECT dolt_commit('-A', '-m', 'gc update bead td-wisp-gmg4agpx', '--author', 'beads <beads@local>') IS NOT NULL;
   SELECT dolt_gc();" \
  "chunks removed" \
  "$DB"

run_test_lastline "integrity_ok_at_neighboring_commit_sizes" \
  "PRAGMA integrity_check;" \
  "ok" \
  "$DB"

db_rm "$DB"

# Two-parent at colliding size: 26 string bytes (author 8 + 18-char message).
run_test_match "gc_ok_with_102_byte_merge_commit" \
  "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES (1,'x');
   SELECT dolt_commit('-A', '-m', 'seed') IS NOT NULL;
   SELECT dolt_branch('feat');
   UPDATE t SET v='main' WHERE k=1;
   SELECT dolt_commit('-A', '-m', 'main change') IS NOT NULL;
   SELECT dolt_checkout('feat');
   INSERT INTO t VALUES (2,'feat');
   SELECT dolt_commit('-A', '-m', 'feat change') IS NOT NULL;
   SELECT dolt_checkout('main');
   SELECT dolt_merge('feat', '--no-ff', '-m', 'merge msg 18 chars') IS NOT NULL;
   SELECT dolt_gc();" \
  "chunks removed" \
  "$DB"

run_test_lastline "integrity_ok_with_102_byte_merge_commit" \
  "PRAGMA integrity_check;" \
  "ok" \
  "$DB"

db_rm "$DB"

# DLC/DCV lead with 'D' like CATALOG_V3. Conflicts never persist; exercise via the still-committable DCV blob.
run_test_match "gc_ok_with_unresolved_violation" \
  "CREATE TABLE parent(id INTEGER PRIMARY KEY);
   CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id));
   INSERT INTO parent VALUES (1);
   SELECT dolt_commit('-A','-m','seed') IS NOT NULL;
   SELECT dolt_branch('feat');
   SELECT dolt_checkout('feat');
   INSERT INTO child VALUES (11,1);
   SELECT dolt_commit('-A','-m','child') IS NOT NULL;
   SELECT dolt_checkout('main');
   DELETE FROM parent WHERE id=1;
   SELECT dolt_commit('-A','-m','drop parent') IS NOT NULL;
   BEGIN;
   SELECT dolt_merge('feat');
   COMMIT;
   SELECT dolt_gc();" \
  "chunks removed" \
  "$DB"

run_test_lastline "integrity_ok_with_unresolved_violation" \
  "PRAGMA integrity_check;" \
  "ok" \
  "$DB"

run_test_lastline "violation_survives_gc" \
  "SELECT count(*) FROM dolt_constraint_violations;" \
  "1" \
  "$DB"

db_rm "$DB"

dltest_finish
