#!/bin/bash
#
# Oracle test: run identical SQL against doltlite and stock SQLite,
# compare output. Catches any divergence in index-related mutations.
#
# Usage: bash index_oracle_test.sh [path/to/doltlite] [path/to/sqlite3]
#

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
pass=0; fail=0

oracle() {
  local name="$1" sql="$2"
  local dl="$TMPDIR/dl_${name}.db" sq="$TMPDIR/sq_${name}.db"
  rm -f "$dl" "$sq"
  out_dl=$(echo "$sql" | "$DOLTLITE" "$dl" 2>/dev/null)
  out_sq=$(echo "$sql" | "$SQLITE3" "$sq" 2>/dev/null)
  if [ "$out_dl" = "$out_sq" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo "  FAIL: $name"
    echo "    doltlite: $(echo "$out_dl" | head -3)"
    echo "    sqlite3:  $(echo "$out_sq" | head -3)"
  fi
}

# ════════════════════════════════════════════════════════════════════
echo "=== Index Oracle Tests ==="
echo ""

# ════════════════════════════════════════════════════════════════════
# Category 1: UPDATE with single-column index
# ════════════════════════════════════════════════════════════════════
echo "--- Category 1: UPDATE with single-column index ---"

for N in 100 1000; do

  # 1a. UPDATE indexed column (val), verify via table scan
  oracle "cat1_update_indexed_col_tablescan_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val * 2;
SELECT count(*), sum(val), min(val), max(val) FROM t ORDER BY 1;
"

  # 1b. UPDATE indexed column (val), verify via index scan
  oracle "cat1_update_indexed_col_idxscan_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val * 2;
SELECT val FROM t WHERE val >= 0 ORDER BY val LIMIT 5;
SELECT val FROM t WHERE val >= 0 ORDER BY val DESC LIMIT 5;
"

  # 1c. UPDATE non-indexed column, verify index is unchanged
  oracle "cat1_update_nonindexed_col_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, other TEXT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x, 'orig' FROM c;
UPDATE t SET other = 'changed';
SELECT count(*), sum(val), min(val), max(val) FROM t;
SELECT val FROM t WHERE val <= 5 ORDER BY val;
"

  # 1d. UPDATE WHERE on indexed column (uses index for seek)
  oracle "cat1_update_where_indexed_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val + 10000 WHERE val <= 50;
SELECT count(*) FROM t WHERE val > 10000;
SELECT count(*) FROM t WHERE val <= 50;
SELECT count(*) FROM t;
"

  # 1e. UPDATE SET val=val+1 (self-referential, regression for double-apply)
  oracle "cat1_update_self_ref_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val + 1;
SELECT count(*), sum(val), min(val), max(val) FROM t;
SELECT val FROM t ORDER BY val LIMIT 5;
"

  # 1f. Bulk UPDATE all rows
  oracle "cat1_bulk_update_all_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = 999;
SELECT count(*) FROM t;
SELECT count(DISTINCT val) FROM t;
SELECT val FROM t LIMIT 1;
"

  # 1g. Partial UPDATE (WHERE id%10=0)
  oracle "cat1_partial_update_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = -1 WHERE id % 10 = 0;
SELECT count(*) FROM t WHERE val = -1;
SELECT count(*) FROM t WHERE val >= 0;
SELECT count(*) FROM t;
"

  # 1h. Single row UPDATE via PK
  oracle "cat1_single_row_update_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = 99999 WHERE id = 50;
SELECT val FROM t WHERE id = 50;
SELECT count(*) FROM t WHERE val = 99999;
SELECT count(*) FROM t;
"

  # 1i. UPDATE indexed col to same value for all rows (duplicates in non-unique index)
  oracle "cat1_update_all_same_val_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = 42;
SELECT count(*) FROM t WHERE val = 42;
SELECT count(*) FROM t;
SELECT min(id), max(id) FROM t;
"

done

# ════════════════════════════════════════════════════════════════════
# Category 2: Multi-column composite index
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 2: Multi-column composite index ---"

# 2a. UPDATE only col a, verify
oracle "cat2_update_col_a" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
UPDATE t SET a = 'new_' || a WHERE id <= 100;
SELECT count(*) FROM t WHERE a LIKE 'new_%';
SELECT count(*) FROM t WHERE a NOT LIKE 'new_%';
SELECT count(*) FROM t;
"

# 2b. UPDATE only col b, verify
oracle "cat2_update_col_b" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
UPDATE t SET b = b + 10000 WHERE id <= 250;
SELECT count(*) FROM t WHERE b > 10000;
SELECT count(*) FROM t WHERE b <= 500;
SELECT count(*) FROM t;
"

# 2c. UPDATE both a and b, verify
oracle "cat2_update_both_ab" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
UPDATE t SET a = 'X', b = -1 WHERE id % 5 = 0;
SELECT count(*) FROM t WHERE a = 'X' AND b = -1;
SELECT count(*) FROM t WHERE a != 'X';
SELECT count(*) FROM t;
"

# 2d. DELETE WHERE a='val' AND b>X, verify
oracle "cat2_delete_composite_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
DELETE FROM t WHERE a = 'grp0' AND b > 200;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE a = 'grp0';
"

# 2e. SELECT using prefix (WHERE a='val'), verify matches
oracle "cat2_select_prefix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
SELECT count(*) FROM t WHERE a = 'grp0';
SELECT count(*) FROM t WHERE a = 'grp5';
SELECT min(b), max(b) FROM t WHERE a = 'grp0';
"

# 2f. SELECT using full key (WHERE a='val' AND b=X), verify
oracle "cat2_select_full_key" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
SELECT id, a, b FROM t WHERE a = 'grp0' AND b = 10 ORDER BY id;
SELECT id, a, b FROM t WHERE a = 'grp0' AND b = 100 ORDER BY id;
SELECT count(*) FROM t WHERE a = 'grp0' AND b = 10;
"

# 2g. INSERT OR REPLACE with UNIQUE(a,b) -- separate table
oracle "cat2_insert_or_replace_unique_ab" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL, UNIQUE(a, b));
INSERT INTO t VALUES(1, 'hello', 10, 1.0);
INSERT INTO t VALUES(2, 'hello', 20, 2.0);
INSERT INTO t VALUES(3, 'world', 10, 3.0);
INSERT OR REPLACE INTO t VALUES(4, 'hello', 10, 99.0);
SELECT count(*) FROM t;
SELECT id, a, b, c FROM t ORDER BY id;
"

# 2h. UPDATE after DELETE on composite index
oracle "cat2_update_after_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
DELETE FROM t WHERE id <= 100;
UPDATE t SET a = 'updated' WHERE id <= 200;
SELECT count(*) FROM t WHERE a = 'updated';
SELECT count(*) FROM t;
"

# 2i. Composite index: verify ordering
oracle "cat2_composite_ordering" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1, 'a', 3, 1.0);
INSERT INTO t VALUES(2, 'a', 1, 2.0);
INSERT INTO t VALUES(3, 'b', 2, 3.0);
INSERT INTO t VALUES(4, 'a', 2, 4.0);
INSERT INTO t VALUES(5, 'b', 1, 5.0);
SELECT id, a, b FROM t WHERE a >= 'a' ORDER BY a, b;
"

# 2j. Multiple UPDATEs on composite index
oracle "cat2_multiple_updates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
UPDATE t SET a = 'first' WHERE id <= 100;
UPDATE t SET a = 'second' WHERE id > 100 AND id <= 200;
UPDATE t SET b = 0 WHERE a = 'first';
SELECT count(*) FROM t WHERE a = 'first' AND b = 0;
SELECT count(*) FROM t WHERE a = 'second';
SELECT count(*) FROM t;
"

# 2k. DELETE all with composite index then re-insert
oracle "cat2_delete_all_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, 'grp' || (x % 5), x, x * 1.5 FROM c;
DELETE FROM t;
SELECT count(*) FROM t;
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<50)
INSERT INTO t SELECT x, 'new' || (x % 3), x * 10, x * 2.5 FROM c;
SELECT count(*) FROM t;
SELECT a, count(*) FROM t GROUP BY a ORDER BY a;
"

# 2l. Composite index prefix scan after mutations
oracle "cat2_prefix_scan_after_mutations" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
DELETE FROM t WHERE a = 'grp0';
UPDATE t SET b = -b WHERE a = 'grp1';
SELECT count(*) FROM t WHERE a = 'grp0';
SELECT count(*) FROM t WHERE a = 'grp1' AND b < 0;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 3: BLOB and mixed-type indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 3: BLOB and mixed-type indexes ---"

# 3a. INSERT blobs, UPDATE blob values, verify
oracle "cat3_blob_insert_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, x'DEADBEEF');
INSERT INTO t VALUES(2, x'CAFEBABE');
INSERT INTO t VALUES(3, x'00112233');
UPDATE t SET data = x'FFFFFFFF' WHERE id = 1;
SELECT id, hex(data) FROM t ORDER BY id;
"

# 3b. Empty blob (zeroblob(0)) in index
oracle "cat3_empty_blob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, zeroblob(0));
INSERT INTO t VALUES(2, x'AA');
INSERT INTO t VALUES(3, zeroblob(0));
SELECT id, hex(data), length(data) FROM t ORDER BY id;
SELECT count(*) FROM t WHERE data = zeroblob(0);
"

# 3c. NULL in indexed column, UPDATE from NULL to non-NULL
oracle "cat3_null_to_nonnull" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, NULL);
INSERT INTO t VALUES(2, NULL);
INSERT INTO t VALUES(3, x'AABB');
SELECT id, data IS NULL FROM t ORDER BY id;
UPDATE t SET data = x'1234' WHERE id = 1;
SELECT id, hex(data) FROM t WHERE data IS NOT NULL ORDER BY id;
SELECT count(*) FROM t WHERE data IS NULL;
"

# 3d. Mixed types in same column: integer, text, blob, real, null
oracle "cat3_mixed_types" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42);
INSERT INTO t VALUES(2, 'hello');
INSERT INTO t VALUES(3, x'AABB');
INSERT INTO t VALUES(4, 3.14);
INSERT INTO t VALUES(5, NULL);
INSERT INTO t VALUES(6, 0);
INSERT INTO t VALUES(7, '');
INSERT INTO t VALUES(8, x'');
SELECT id, typeof(val), val FROM t ORDER BY id;
SELECT count(*) FROM t WHERE val IS NULL;
SELECT count(*) FROM t WHERE typeof(val) = 'integer';
SELECT count(*) FROM t WHERE typeof(val) = 'text';
"

# 3e. Large blob (randomblob) in index - use deterministic content
oracle "cat3_large_blob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, zeroblob(1000));
INSERT INTO t VALUES(2, zeroblob(500));
UPDATE t SET data = zeroblob(2000) WHERE id = 1;
SELECT id, length(data) FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 3f. BLOB with embedded zeros
oracle "cat3_blob_embedded_zeros" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, x'00FF0000FF');
INSERT INTO t VALUES(2, x'00FF0001FF');
INSERT INTO t VALUES(3, x'00000000');
UPDATE t SET data = x'FF00FF00' WHERE id = 3;
SELECT id, hex(data) FROM t ORDER BY id;
SELECT count(*) FROM t WHERE data >= x'00FF0000FF';
"

# 3g. Blob UPDATE with index - multiple sizes
oracle "cat3_blob_sizes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, x'AA');
INSERT INTO t VALUES(2, x'AABB');
INSERT INTO t VALUES(3, x'AABBCC');
INSERT INTO t VALUES(4, x'AABBCCDD');
UPDATE t SET data = x'FF' WHERE id = 1;
UPDATE t SET data = x'FFEE' WHERE id = 2;
SELECT id, hex(data) FROM t ORDER BY id;
SELECT id, hex(data) FROM t ORDER BY data;
"

# 3h. NULL blob operations
oracle "cat3_null_blob_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, NULL);
INSERT INTO t VALUES(2, x'AA');
INSERT INTO t VALUES(3, NULL);
UPDATE t SET data = NULL WHERE id = 2;
SELECT count(*) FROM t WHERE data IS NULL;
UPDATE t SET data = x'BB' WHERE data IS NULL;
SELECT count(*) FROM t WHERE data IS NULL;
SELECT id, hex(data) FROM t WHERE data IS NOT NULL ORDER BY id;
"

# 3i. Mixed type updates
oracle "cat3_mixed_type_updates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42);
INSERT INTO t VALUES(2, 'hello');
INSERT INTO t VALUES(3, x'AABB');
UPDATE t SET val = 'now_text' WHERE id = 1;
UPDATE t SET val = 100 WHERE id = 2;
UPDATE t SET val = NULL WHERE id = 3;
SELECT id, typeof(val), val FROM t ORDER BY id;
"

# 3j. Blob index with DELETE and re-INSERT
oracle "cat3_blob_delete_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, x'AABBCC');
INSERT INTO t VALUES(2, x'DDEEFF');
INSERT INTO t VALUES(3, x'112233');
DELETE FROM t WHERE id = 2;
INSERT INTO t VALUES(4, x'DDEEFF');
SELECT id, hex(data) FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 4: Transaction interactions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 4: Transaction interactions ---"

# 4a. BEGIN; UPDATE indexed col; COMMIT; verify
oracle "cat4_begin_update_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
BEGIN;
UPDATE t SET val = val + 100;
COMMIT;
SELECT id, val FROM t ORDER BY id;
"

# 4b. BEGIN; UPDATE indexed col; ROLLBACK; verify unchanged
oracle "cat4_begin_update_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
BEGIN;
UPDATE t SET val = val + 100;
ROLLBACK;
SELECT id, val FROM t ORDER BY id;
"

# 4c. SAVEPOINT sp; UPDATE indexed col; RELEASE sp; verify
oracle "cat4_savepoint_release" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
SAVEPOINT sp;
UPDATE t SET val = val + 100;
RELEASE sp;
SELECT id, val FROM t ORDER BY id;
"

# 4d. SAVEPOINT sp; UPDATE indexed col; ROLLBACK TO sp; verify unchanged
oracle "cat4_savepoint_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
SAVEPOINT sp;
UPDATE t SET val = val + 100;
ROLLBACK TO sp;
SELECT id, val FROM t ORDER BY id;
"

# 4e. Multiple UPDATEs to same row in one transaction
oracle "cat4_multi_update_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
BEGIN;
UPDATE t SET val = 100 WHERE id = 1;
UPDATE t SET val = 200 WHERE id = 1;
UPDATE t SET val = 300 WHERE id = 1;
COMMIT;
SELECT id, val FROM t ORDER BY id;
"

# 4f. UPDATE + DELETE same row in one transaction
oracle "cat4_update_delete_same_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
BEGIN;
UPDATE t SET val = 999 WHERE id = 2;
DELETE FROM t WHERE id = 2;
COMMIT;
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 4g. DELETE + re-INSERT same key in one transaction
oracle "cat4_delete_reinsert_same_key" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
BEGIN;
DELETE FROM t WHERE id = 2;
INSERT INTO t VALUES(2, 999);
COMMIT;
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 4h. Nested savepoints with index mutations
oracle "cat4_nested_savepoints" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
SAVEPOINT sp1;
UPDATE t SET val = 100 WHERE id = 1;
SAVEPOINT sp2;
UPDATE t SET val = 200 WHERE id = 2;
ROLLBACK TO sp2;
RELEASE sp1;
SELECT id, val FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 5: INSERT OR REPLACE / UPSERT
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 5: INSERT OR REPLACE / UPSERT ---"

# 5a. INSERT OR REPLACE with PK conflict, secondary index present
oracle "cat5_replace_pk_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, name TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT INTO t VALUES(3, 30, 'charlie');
INSERT OR REPLACE INTO t VALUES(2, 200, 'bob_replaced');
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5b. INSERT OR REPLACE with UNIQUE index conflict
oracle "cat5_replace_unique_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, name TEXT);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT INTO t VALUES(3, 30, 'charlie');
INSERT OR REPLACE INTO t VALUES(4, 20, 'new_bob');
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5c. INSERT OR IGNORE with UNIQUE index conflict
oracle "cat5_ignore_unique_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, name TEXT);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT OR IGNORE INTO t VALUES(3, 10, 'should_be_ignored');
INSERT OR IGNORE INTO t VALUES(4, 40, 'dave');
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5d. ON CONFLICT DO UPDATE (upsert) with secondary index
oracle "cat5_upsert_do_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, name TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT INTO t VALUES(1, 100, 'alice_updated')
  ON CONFLICT(id) DO UPDATE SET val=excluded.val, name=excluded.name;
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5e. ON CONFLICT DO NOTHING
oracle "cat5_upsert_do_nothing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, name TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT INTO t VALUES(1, 100, 'should_not_appear')
  ON CONFLICT(id) DO NOTHING;
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5f. REPLACE INTO with multiple indexes
oracle "cat5_replace_multi_idx" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1, 10, 'x');
INSERT INTO t VALUES(2, 20, 'y');
INSERT INTO t VALUES(3, 30, 'z');
REPLACE INTO t VALUES(2, 200, 'y_replaced');
SELECT id, a, b FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5g. Multiple REPLACE operations
oracle "cat5_multi_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, name TEXT);
INSERT INTO t VALUES(1, 10, 'a');
INSERT INTO t VALUES(2, 20, 'b');
INSERT INTO t VALUES(3, 30, 'c');
REPLACE INTO t VALUES(1, 10, 'a_replaced');
REPLACE INTO t VALUES(2, 20, 'b_replaced');
REPLACE INTO t VALUES(4, 30, 'c_moved');
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 5h. Upsert with composite unique index
oracle "cat5_upsert_composite_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, val TEXT, UNIQUE(a, b));
INSERT INTO t VALUES(1, 'x', 1, 'first');
INSERT INTO t VALUES(2, 'x', 2, 'second');
INSERT INTO t VALUES(3, 'y', 1, 'third');
INSERT INTO t VALUES(4, 'x', 1, 'updated_first')
  ON CONFLICT(a, b) DO UPDATE SET val=excluded.val;
SELECT id, a, b, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 6: Edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 6: Edge cases ---"

# 6a. DELETE all rows from indexed table, INSERT new rows, verify
oracle "cat6_delete_all_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x FROM c;
DELETE FROM t;
SELECT count(*) FROM t;
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<50)
INSERT INTO t SELECT x + 1000, x * 10 FROM c;
SELECT count(*) FROM t;
SELECT min(id), max(id), min(val), max(val) FROM t;
"

# 6b. CREATE INDEX on existing table with data, then UPDATE
oracle "cat6_create_index_on_existing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<200)
INSERT INTO t SELECT x, x FROM c;
CREATE INDEX idx ON t(val);
UPDATE t SET val = val + 1000 WHERE id <= 50;
SELECT count(*) FROM t WHERE val > 1000;
SELECT count(*) FROM t;
SELECT min(val), max(val) FROM t;
"

# 6c. Two indexes on same table, UPDATE column in only one index
oracle "cat6_two_indexes_update_one" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<200)
INSERT INTO t SELECT x, x, x*10 FROM c;
UPDATE t SET a = a + 5000 WHERE id <= 100;
SELECT count(*) FROM t WHERE a > 5000;
SELECT min(b), max(b) FROM t;
SELECT count(*) FROM t;
"

# 6d. UPDATE that reverses sort order
oracle "cat6_reverse_sort_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 1);
INSERT INTO t VALUES(2, 2);
INSERT INTO t VALUES(3, 3);
INSERT INTO t VALUES(4, 4);
INSERT INTO t VALUES(5, 5);
UPDATE t SET val = 6 - val;
SELECT id, val FROM t ORDER BY val;
SELECT id, val FROM t ORDER BY id;
"

# 6e. INSERT 5000 rows with index (multi-level tree), UPDATE subset, verify
oracle "cat6_large_scale" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<5000)
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val + 100000 WHERE id % 100 = 0;
SELECT count(*) FROM t WHERE val > 100000;
SELECT count(*) FROM t;
SELECT min(val), max(val) FROM t;
"

# 6f. Interleaved operations: INSERT, UPDATE, DELETE, INSERT in sequence
oracle "cat6_interleaved_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(2, 200);
INSERT INTO t VALUES(3, 300);
UPDATE t SET val = 999 WHERE id = 2;
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(4, 400);
UPDATE t SET val = val + 1 WHERE id > 2;
DELETE FROM t WHERE val = 1000;
INSERT INTO t VALUES(5, 500);
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 6g. WITHOUT ROWID table with index, UPDATE
oracle "cat6_without_rowid" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES('x', 1, 100);
INSERT INTO t VALUES('x', 2, 200);
INSERT INTO t VALUES('y', 1, 300);
INSERT INTO t VALUES('y', 2, 400);
UPDATE t SET c = c + 1000 WHERE a = 'x';
SELECT a, b, c FROM t ORDER BY a, b;
SELECT count(*) FROM t WHERE c > 1000;
"

# 6h. Partial index: CREATE INDEX idx ON t(a) WHERE a IS NOT NULL
oracle "cat6_partial_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
CREATE INDEX idx ON t(a) WHERE a IS NOT NULL;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, NULL);
INSERT INTO t VALUES(3, 30);
INSERT INTO t VALUES(4, NULL);
INSERT INTO t VALUES(5, 50);
UPDATE t SET a = 99 WHERE id = 2;
SELECT id, a FROM t ORDER BY id;
SELECT count(*) FROM t WHERE a IS NOT NULL;
SELECT count(*) FROM t WHERE a IS NULL;
"

# 6i. UPDATE with expression index emulation (indexed computed column)
oracle "cat6_multiple_deletes_inserts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x FROM c;
DELETE FROM t WHERE id <= 30;
INSERT INTO t SELECT id + 100, val + 100 FROM t WHERE id <= 50;
DELETE FROM t WHERE val > 150;
SELECT count(*) FROM t;
SELECT min(id), max(id) FROM t;
SELECT min(val), max(val) FROM t;
"

# 6j. Rapid insert-delete cycle on indexed table
oracle "cat6_rapid_insert_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 20);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 30);
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 7: Integrity checks
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 7: Integrity checks ---"

# 7a. After bulk UPDATE with index
oracle "cat7_integrity_bulk_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val * 2;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 7b. After DELETE + re-INSERT cycle
oracle "cat7_integrity_delete_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<200)
INSERT INTO t SELECT x, x FROM c;
DELETE FROM t WHERE id % 2 = 0;
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x + 1000, x + 1000 FROM c;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 7c. After REPLACE with unique index
oracle "cat7_integrity_replace_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, name TEXT);
INSERT INTO t VALUES(1, 10, 'a');
INSERT INTO t VALUES(2, 20, 'b');
INSERT INTO t VALUES(3, 30, 'c');
REPLACE INTO t VALUES(2, 20, 'b_new');
REPLACE INTO t VALUES(4, 10, 'a_moved');
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 7d. After savepoint rollback with index changes
oracle "cat7_integrity_savepoint_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
INSERT INTO t VALUES(3, 30);
SAVEPOINT sp;
UPDATE t SET val = val * 100;
DELETE FROM t WHERE id = 2;
ROLLBACK TO sp;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 7e. After CREATE INDEX on existing data + UPDATE
oracle "cat7_integrity_create_index_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<300)
INSERT INTO t SELECT x, x FROM c;
CREATE INDEX idx ON t(val);
UPDATE t SET val = val + 5000 WHERE id % 3 = 0;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 7f. After mixed INSERT/UPDATE/DELETE batch
oracle "cat7_integrity_mixed_batch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<200)
INSERT INTO t SELECT x, x FROM c;
DELETE FROM t WHERE id <= 50;
UPDATE t SET val = val + 10000 WHERE id <= 100;
INSERT INTO t VALUES(9001, 9001);
INSERT INTO t VALUES(9002, 9002);
DELETE FROM t WHERE id = 100;
UPDATE t SET val = -1 WHERE id = 150;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 8: WITHOUT ROWID tables
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 8: WITHOUT ROWID tables ---"

# 8a. Basic CRUD on WITHOUT ROWID
oracle "cat8_wr_basic_crud" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200), ('y', 1, 300);
UPDATE t SET c = 999 WHERE a = 'x' AND b = 1;
DELETE FROM t WHERE a = 'y';
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
"

# 8b. WITHOUT ROWID single-column PK UPDATE non-PK column
oracle "cat8_wr_single_pk_update" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('a', 1), ('b', 2), ('c', 3);
UPDATE t SET v = v * 10;
SELECT * FROM t ORDER BY k;
"

# 8c. WITHOUT ROWID multi-row UPDATE with secondary index
oracle "cat8_wr_multirow_update_secidx" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200), ('x', 3, 300);
INSERT INTO t VALUES('y', 1, 400), ('y', 2, 500);
UPDATE t SET c = c + 1000 WHERE a = 'x';
SELECT a, b, c FROM t ORDER BY a, b;
SELECT count(*) FROM t WHERE c > 1000;
SELECT c FROM t WHERE c >= 1100 ORDER BY c;
"

# 8d. WITHOUT ROWID REPLACE INTO
oracle "cat8_wr_replace" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100);
REPLACE INTO t VALUES('x', 1, 999);
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
"

# 8e. WITHOUT ROWID UPDATE PK column
oracle "cat8_wr_update_pk" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200);
UPDATE t SET a = 'z' WHERE b = 1;
SELECT * FROM t ORDER BY a, b;
"

# 8f. WITHOUT ROWID DELETE with secondary index
oracle "cat8_wr_delete_secidx" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES('a', 1, 10), ('a', 2, 20), ('b', 1, 30), ('b', 2, 40);
DELETE FROM t WHERE c > 20;
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t WHERE c <= 20;
"

# 8g. WITHOUT ROWID UPSERT
oracle "cat8_wr_upsert" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100);
INSERT INTO t VALUES('x', 1, 999) ON CONFLICT(a, b) DO UPDATE SET c = excluded.c;
SELECT * FROM t;
SELECT count(*) FROM t;
"

# 8h. WITHOUT ROWID bulk UPDATE all rows
oracle "cat8_wr_bulk_update_all" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT, n INT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50);
UPDATE t SET n = n * 100;
SELECT * FROM t ORDER BY k;
"

# 8i. WITHOUT ROWID with unique secondary index and REPLACE
oracle "cat8_wr_unique_secidx_replace" "
CREATE TABLE t(a INT, b INT, c TEXT UNIQUE, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, 1, 'hello');
INSERT INTO t VALUES(2, 2, 'world');
REPLACE INTO t VALUES(3, 3, 'hello');
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
"

# 8j. WITHOUT ROWID integrity check after mutations
oracle "cat8_wr_integrity" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200), ('y', 1, 300), ('y', 2, 400);
UPDATE t SET c = c + 1000 WHERE a = 'x';
DELETE FROM t WHERE a = 'y' AND b = 2;
INSERT INTO t VALUES('z', 1, 500);
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 8k. WITHOUT ROWID large-ish dataset
oracle "cat8_wr_large" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<200)
INSERT INTO t SELECT x / 50, x, x * 10 FROM r;
UPDATE t SET c = c + 5000 WHERE b % 3 = 0;
DELETE FROM t WHERE b % 7 = 0;
SELECT count(*) FROM t;
SELECT sum(c) FROM t;
SELECT count(*) FROM t WHERE c > 5000;
"

# ════════════════════════════════════════════════════════════════════
# Category 9: Reverse scans and ORDER BY DESC
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 9: Reverse scans and ORDER BY DESC ---"

# 9a. ORDER BY DESC on indexed column
oracle "cat9_desc_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 30), (2, 10), (3, 50), (4, 20), (5, 40);
SELECT val FROM t ORDER BY val DESC;
"

# 9b. ORDER BY DESC with LIMIT
oracle "cat9_desc_limit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x * 7 % 100 FROM c;
SELECT val FROM t ORDER BY val DESC LIMIT 5;
"

# 9c. ORDER BY DESC on composite index prefix
oracle "cat9_desc_composite_prefix" "
CREATE TABLE t(a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES('x', 3, 100), ('x', 1, 200), ('x', 2, 300);
INSERT INTO t VALUES('y', 2, 400), ('y', 1, 500);
SELECT a, b FROM t WHERE a = 'x' ORDER BY b DESC;
"

# 9d. ORDER BY DESC on composite index full key
oracle "cat9_desc_composite_full" "
CREATE TABLE t(a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES('a', 1, 10), ('a', 2, 20), ('b', 1, 30), ('b', 2, 40);
SELECT a, b FROM t ORDER BY a DESC, b DESC;
"

# 9e. Reverse scan with range condition
oracle "cat9_desc_range" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
SELECT val FROM t WHERE val BETWEEN 20 AND 40 ORDER BY val DESC;
"

# 9f. MAX() uses reverse index scan
oracle "cat9_max_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 50), (3, 30), (4, 20), (5, 40);
SELECT max(val) FROM t;
SELECT min(val) FROM t;
"

# 9g. ORDER BY DESC after UPDATE
oracle "cat9_desc_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
UPDATE t SET val = val + 100 WHERE id <= 3;
SELECT val FROM t ORDER BY val DESC;
"

# 9h. ORDER BY DESC on WITHOUT ROWID
oracle "cat9_desc_without_rowid" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 10), ('x', 2, 20), ('x', 3, 30);
INSERT INTO t VALUES('y', 1, 40), ('y', 2, 50);
SELECT a, b FROM t WHERE a = 'x' ORDER BY b DESC;
SELECT a, b FROM t ORDER BY a DESC, b DESC;
"

# 9i. LAST_VALUE / window with DESC ordering
oracle "cat9_desc_window" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1, 'a', 10), (2, 'a', 30), (3, 'a', 20);
INSERT INTO t VALUES(4, 'b', 40), (5, 'b', 50);
SELECT grp, val, rank() OVER (PARTITION BY grp ORDER BY val DESC) as rnk
FROM t ORDER BY grp, rnk;
"

# ════════════════════════════════════════════════════════════════════
# Category 10: Range queries and boundary conditions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 10: Range queries and boundary conditions ---"

# 10a. Seek past all entries
oracle "cat10_seek_past_end" "
CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1), (3), (5), (7), (9);
SELECT * FROM t WHERE x >= 10;
SELECT * FROM t WHERE x > 9;
SELECT count(*) FROM t WHERE x > 100;
"

# 10b. Seek before all entries
oracle "cat10_seek_before_start" "
CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(10), (20), (30);
SELECT * FROM t WHERE x < 5;
SELECT * FROM t WHERE x <= 0;
SELECT count(*) FROM t WHERE x < 10;
"

# 10c. BETWEEN on indexed column
oracle "cat10_between" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 5), (2, 10), (3, 15), (4, 20), (5, 25), (6, 30);
SELECT val FROM t WHERE val BETWEEN 10 AND 25 ORDER BY val;
SELECT val FROM t WHERE val BETWEEN 100 AND 200 ORDER BY val;
SELECT val FROM t WHERE val BETWEEN 5 AND 5 ORDER BY val;
"

# 10d. Empty table queries
oracle "cat10_empty_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
SELECT * FROM t;
SELECT count(*) FROM t;
SELECT max(val) FROM t;
SELECT min(val) FROM t;
SELECT * FROM t WHERE val = 5;
SELECT * FROM t WHERE val BETWEEN 1 AND 100;
"

# 10e. Single-row table
oracle "cat10_single_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42);
SELECT * FROM t WHERE val = 42;
SELECT * FROM t WHERE val > 42;
SELECT * FROM t WHERE val < 42;
SELECT * FROM t WHERE val >= 42;
SELECT * FROM t WHERE val <= 42;
SELECT * FROM t WHERE val != 42;
"

# 10f. Boundary: exact match at first and last entry
oracle "cat10_boundary_first_last" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
SELECT val FROM t WHERE val >= 10 ORDER BY val LIMIT 1;
SELECT val FROM t WHERE val <= 50 ORDER BY val DESC LIMIT 1;
SELECT val FROM t WHERE val > 10 ORDER BY val LIMIT 1;
SELECT val FROM t WHERE val < 50 ORDER BY val DESC LIMIT 1;
"

# 10g. Gaps in index values
oracle "cat10_gaps" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 1000), (3, 1000000);
SELECT val FROM t WHERE val > 10 AND val < 1000000 ORDER BY val;
SELECT val FROM t WHERE val >= 500 ORDER BY val LIMIT 1;
SELECT count(*) FROM t WHERE val BETWEEN 11 AND 999;
"

# 10h. Duplicate index values with range
oracle "cat10_dup_range" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 10), (3, 10), (4, 20), (5, 20), (6, 30);
SELECT count(*) FROM t WHERE val = 10;
SELECT count(*) FROM t WHERE val >= 10 AND val < 20;
SELECT count(*) FROM t WHERE val > 10 AND val <= 20;
SELECT id FROM t WHERE val = 10 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 11: NULL handling in indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 11: NULL handling in indexes ---"

# 11a. NULLs in indexed column ordering
oracle "cat11_null_ordering" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL), (2, 10), (3, NULL), (4, 5), (5, 20);
SELECT id, val FROM t ORDER BY val;
SELECT id, val FROM t ORDER BY val DESC;
"

# 11b. NULL in composite index
oracle "cat11_null_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1, 'x', NULL), (2, 'x', 10), (3, NULL, 5), (4, NULL, NULL);
SELECT id, a, b FROM t ORDER BY a, b;
SELECT count(*) FROM t WHERE a IS NULL;
SELECT count(*) FROM t WHERE a = 'x' AND b IS NULL;
"

# 11c. UPDATE NULL to value and value to NULL
oracle "cat11_null_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL), (2, 10), (3, 20);
UPDATE t SET val = 99 WHERE val IS NULL;
UPDATE t SET val = NULL WHERE val = 10;
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t WHERE val IS NULL;
SELECT count(*) FROM t WHERE val IS NOT NULL;
"

# 11d. NULL with UNIQUE index
oracle "cat11_null_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
INSERT INTO t VALUES(1, NULL);
INSERT INTO t VALUES(2, NULL);
INSERT INTO t VALUES(3, 10);
SELECT count(*) FROM t;
SELECT id, val FROM t ORDER BY id;
"

# 11e. IS NULL / IS NOT NULL index scan
oracle "cat11_isnull_scan" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL), (2, 'a'), (3, NULL), (4, 'b'), (5, NULL);
SELECT id FROM t WHERE val IS NULL ORDER BY id;
SELECT id FROM t WHERE val IS NOT NULL ORDER BY id;
SELECT count(*) FROM t WHERE val IS NULL;
"

# 11f. NULL in WITHOUT ROWID PK (not allowed, but secondary index)
oracle "cat11_null_wr_secondary" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x', 1, NULL), ('x', 2, 10), ('y', 1, NULL);
SELECT a, b, c FROM t WHERE c IS NULL ORDER BY a, b;
SELECT count(*) FROM t WHERE c IS NOT NULL;
UPDATE t SET c = 42 WHERE c IS NULL;
SELECT * FROM t ORDER BY a, b;
"

# ════════════════════════════════════════════════════════════════════
# Category 12: Type affinity and mixed types
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 12: Type affinity and mixed types ---"

# 12a. Integer stored as text vs integer comparison
oracle "cat12_int_text_affinity" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42), (2, '42'), (3, 42.0);
SELECT id, typeof(val), val FROM t ORDER BY id;
SELECT id FROM t WHERE val = 42 ORDER BY id;
SELECT id FROM t WHERE val = '42' ORDER BY id;
"

# 12b. Mixed types in indexed column ordering
oracle "cat12_mixed_type_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL);
INSERT INTO t VALUES(2, 1);
INSERT INTO t VALUES(3, 2.5);
INSERT INTO t VALUES(4, 'hello');
INSERT INTO t VALUES(5, X'BEEF');
INSERT INTO t VALUES(6, 0);
INSERT INTO t VALUES(7, '');
SELECT id, typeof(val), val FROM t ORDER BY val;
"

# 12c. Integer boundary values (within double precision range)
oracle "cat12_integer_boundaries" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 2147483647);
INSERT INTO t VALUES(2, -2147483648);
INSERT INTO t VALUES(3, 0);
INSERT INTO t VALUES(4, 2147483646);
INSERT INTO t VALUES(5, -1);
SELECT val FROM t ORDER BY val;
SELECT val FROM t WHERE val > 2147483646;
SELECT val FROM t WHERE val = 2147483647;
"

# 12d. Float values in index
oracle "cat12_float_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 3.14);
INSERT INTO t VALUES(2, -2.71);
INSERT INTO t VALUES(3, 0.0);
INSERT INTO t VALUES(4, 1e10);
INSERT INTO t VALUES(5, -1e10);
SELECT id, val FROM t ORDER BY val;
SELECT id FROM t WHERE val > 0 ORDER BY val;
SELECT id FROM t WHERE val BETWEEN -3 AND 4 ORDER BY val;
"

# 12e. Text collation default (BINARY)
oracle "cat12_text_binary_collation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1, 'abc'), (2, 'ABC'), (3, 'Abc'), (4, 'aBC');
SELECT name FROM t ORDER BY name;
SELECT name FROM t WHERE name > 'Abc' ORDER BY name;
SELECT name FROM t WHERE name = 'abc';
"

# 12f. Text ordering with default BINARY collation
oracle "cat12_text_ordering" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1, 'banana'), (2, 'apple'), (3, 'cherry'), (4, 'date');
SELECT name FROM t ORDER BY name;
SELECT name FROM t WHERE name >= 'cherry' ORDER BY name;
SELECT name FROM t WHERE name BETWEEN 'b' AND 'd' ORDER BY name;
"

# 12g. Empty string and space handling
oracle "cat12_empty_string" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, ''), (2, ' '), (3, 'a'), (4, '  ');
SELECT id, length(val) FROM t ORDER BY val;
SELECT id FROM t WHERE val = '' ORDER BY id;
SELECT id FROM t WHERE val > '' ORDER BY val;
"

# 12h. Blob comparison in index
oracle "cat12_blob_comparison" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, X'00'), (2, X'0000'), (3, X'0001'), (4, X'01'), (5, X'FF');
SELECT id FROM t ORDER BY data;
SELECT id FROM t WHERE data > X'00' ORDER BY data;
SELECT id FROM t WHERE data = X'0000';
"

# ════════════════════════════════════════════════════════════════════
# Category 13: Savepoint and transaction edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 13: Savepoint and transaction edge cases ---"

# 13a. Nested savepoints with INSERT/DELETE/UPDATE
oracle "cat13_nested_sp_mixed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
SAVEPOINT sp1;
INSERT INTO t VALUES(4, 40);
UPDATE t SET val = 99 WHERE id = 1;
SAVEPOINT sp2;
DELETE FROM t WHERE id = 2;
INSERT INTO t VALUES(5, 50);
ROLLBACK TO sp2;
SELECT * FROM t ORDER BY id;
RELEASE sp1;
SELECT * FROM t ORDER BY id;
"

# 13b. Rollback after multiple UPDATE on same row
oracle "cat13_rollback_multi_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
SAVEPOINT sp;
UPDATE t SET val = 20 WHERE id = 1;
UPDATE t SET val = 30 WHERE id = 1;
UPDATE t SET val = 40 WHERE id = 1;
ROLLBACK TO sp;
SELECT * FROM t;
"

# 13c. Savepoint rollback with secondary index consistency
oracle "cat13_sp_secidx_consistency" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z');
SAVEPOINT sp;
UPDATE t SET a = 99, b = 'changed' WHERE id = 2;
DELETE FROM t WHERE id = 3;
INSERT INTO t VALUES(4, 40, 'w');
ROLLBACK TO sp;
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE a = 20;
SELECT count(*) FROM t WHERE b = 'y';
PRAGMA integrity_check;
"

# 13d. Deeply nested savepoints
oracle "cat13_deep_nested_sp" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 100);
SAVEPOINT s1;
UPDATE t SET val = 200 WHERE id = 1;
SAVEPOINT s2;
UPDATE t SET val = 300 WHERE id = 1;
SAVEPOINT s3;
UPDATE t SET val = 400 WHERE id = 1;
ROLLBACK TO s3;
SELECT val FROM t WHERE id = 1;
ROLLBACK TO s2;
SELECT val FROM t WHERE id = 1;
RELEASE s1;
SELECT val FROM t WHERE id = 1;
"

# 13e. Transaction with DELETE all then re-INSERT
oracle "cat13_delete_all_reinsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
BEGIN;
DELETE FROM t;
SELECT count(*) FROM t;
INSERT INTO t VALUES(4, 40), (5, 50);
SELECT * FROM t ORDER BY id;
COMMIT;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# 13f. Savepoint with WITHOUT ROWID
oracle "cat13_sp_without_rowid" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200);
SAVEPOINT sp;
UPDATE t SET c = c + 1000;
DELETE FROM t WHERE b = 1;
INSERT INTO t VALUES('z', 1, 500);
ROLLBACK TO sp;
SELECT * FROM t ORDER BY a, b;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 14: REPLACE and UPSERT edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 14: REPLACE and UPSERT edge cases ---"

# 14a. REPLACE with cascading unique conflicts
oracle "cat14_replace_cascade_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b INT UNIQUE);
INSERT INTO t VALUES(1, 10, 100);
INSERT INTO t VALUES(2, 20, 200);
INSERT INTO t VALUES(3, 30, 300);
REPLACE INTO t VALUES(4, 10, 200);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 14b. UPSERT with expression in DO UPDATE
oracle "cat14_upsert_expression" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, count INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 1);
INSERT INTO t VALUES(1, 10, 1) ON CONFLICT(id) DO UPDATE SET count = count + 1, val = val + excluded.val;
INSERT INTO t VALUES(1, 10, 1) ON CONFLICT(id) DO UPDATE SET count = count + 1, val = val + excluded.val;
SELECT * FROM t;
"

# 14c. REPLACE on WITHOUT ROWID with secondary index
oracle "cat14_replace_wr_secidx" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200), ('y', 1, 300);
REPLACE INTO t VALUES('x', 1, 999);
REPLACE INTO t VALUES('y', 1, 888);
SELECT * FROM t ORDER BY a, b;
SELECT c FROM t ORDER BY c;
PRAGMA integrity_check;
"

# 14d. INSERT OR IGNORE with multiple conflicts
oracle "cat14_ignore_multi_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b TEXT);
INSERT INTO t VALUES(1, 10, 'first');
INSERT INTO t VALUES(2, 20, 'second');
INSERT OR IGNORE INTO t VALUES(1, 30, 'pk_conflict');
INSERT OR IGNORE INTO t VALUES(3, 10, 'unique_conflict');
INSERT OR IGNORE INTO t VALUES(4, 40, 'success');
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 14e. UPSERT on composite UNIQUE index
oracle "cat14_upsert_composite_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, val INT, UNIQUE(a, b));
INSERT INTO t VALUES(1, 'x', 1, 100);
INSERT INTO t VALUES(2, 'x', 2, 200);
INSERT INTO t VALUES(3, 'x', 1, 999)
  ON CONFLICT(a, b) DO UPDATE SET val = excluded.val;
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 14f. Multiple REPLACE in sequence
oracle "cat14_multi_replace_sequence" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
INSERT INTO t VALUES(1, 10);
REPLACE INTO t VALUES(2, 10);
REPLACE INTO t VALUES(3, 10);
REPLACE INTO t VALUES(4, 10);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 14g. UPSERT DO UPDATE with WHERE clause
oracle "cat14_upsert_do_update_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, status TEXT);
INSERT INTO t VALUES(1, 10, 'active');
INSERT INTO t VALUES(1, 20, 'new')
  ON CONFLICT(id) DO UPDATE SET val = excluded.val WHERE status = 'active';
SELECT * FROM t;
INSERT INTO t VALUES(1, 30, 'newer')
  ON CONFLICT(id) DO UPDATE SET val = excluded.val WHERE status = 'inactive';
SELECT * FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 15: Complex UPDATE patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 15: Complex UPDATE patterns ---"

# 15a. UPDATE with subquery
oracle "cat15_update_subquery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref_id INT, bonus INT);
CREATE INDEX idx ON t1(val);
INSERT INTO t1 VALUES(1, 100), (2, 200), (3, 300);
INSERT INTO t2 VALUES(1, 1, 10), (2, 2, 20);
UPDATE t1 SET val = val + (SELECT COALESCE(bonus, 0) FROM t2 WHERE t2.ref_id = t1.id)
  WHERE id IN (SELECT ref_id FROM t2);
SELECT * FROM t1 ORDER BY id;
"

# 15b. UPDATE with JOIN (FROM clause)
oracle "cat15_update_from" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE lookup(id INT, factor INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
INSERT INTO lookup VALUES(1, 100), (3, 300);
UPDATE t SET val = t.val * lookup.factor FROM lookup WHERE t.id = lookup.id;
SELECT * FROM t ORDER BY id;
"

# 15c. UPDATE with correlated subquery in SET
oracle "cat15_update_correlated_set" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, grp TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b');
UPDATE t SET val = (SELECT sum(val) FROM t AS t2 WHERE t2.grp = t.grp);
SELECT * FROM t ORDER BY id;
"

# 15d. UPDATE setting column to expression of itself
oracle "cat15_self_ref_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1, 10, 100), (2, 20, 200), (3, 30, 300);
UPDATE t SET a = b, b = a;
SELECT * FROM t ORDER BY id;
"

# 15e. Cascading UPDATE via trigger
oracle "cat15_trigger_cascade" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INT, derived INT);
CREATE INDEX idx_p ON parent(val);
CREATE INDEX idx_c ON child(parent_id);
INSERT INTO parent VALUES(1, 10), (2, 20);
INSERT INTO child VALUES(1, 1, 0), (2, 1, 0), (3, 2, 0);
CREATE TRIGGER trg AFTER UPDATE ON parent
BEGIN
  UPDATE child SET derived = NEW.val * 10 WHERE parent_id = NEW.id;
END;
UPDATE parent SET val = val + 5;
SELECT * FROM parent ORDER BY id;
SELECT * FROM child ORDER BY id;
"

# 15f. UPDATE with CASE expression
oracle "cat15_update_case" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category TEXT, val INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
UPDATE t SET val = CASE category WHEN 'a' THEN val * 2 WHEN 'b' THEN val * 3 ELSE val END;
SELECT * FROM t ORDER BY id;
SELECT sum(val) FROM t WHERE category = 'a';
"

# ════════════════════════════════════════════════════════════════════
# Category 16: Complex DELETE patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 16: Complex DELETE patterns ---"

# 16a. DELETE with subquery
oracle "cat16_delete_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE blacklist(bad_val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
INSERT INTO blacklist VALUES(20), (40);
DELETE FROM t WHERE val IN (SELECT bad_val FROM blacklist);
SELECT * FROM t ORDER BY id;
"

# 16b. DELETE with IN subquery
oracle "cat16_delete_in_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x * 10 FROM c;
DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY val DESC LIMIT 5);
SELECT count(*) FROM t;
SELECT max(val) FROM t;
"

# 16c. DELETE all rows with multiple indexes
oracle "cat16_delete_all_multi_idx" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT, c REAL);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES(1, 10, 'x', 1.1), (2, 20, 'y', 2.2), (3, 30, 'z', 3.3);
DELETE FROM t;
SELECT count(*) FROM t;
INSERT INTO t VALUES(4, 40, 'w', 4.4);
SELECT * FROM t;
PRAGMA integrity_check;
"

# 16d. DELETE with complex WHERE on composite index
oracle "cat16_delete_complex_where" "
CREATE TABLE t(a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES('x', 1, 10), ('x', 2, 20), ('x', 3, 30);
INSERT INTO t VALUES('y', 1, 40), ('y', 2, 50);
DELETE FROM t WHERE a = 'x' AND b >= 2;
SELECT * FROM t ORDER BY a, b;
"

# 16e. DELETE on WITHOUT ROWID with multi-row match
oracle "cat16_delete_wr_multirow" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x', 1, 10), ('x', 2, 20), ('x', 3, 30);
INSERT INTO t VALUES('y', 1, 40), ('y', 2, 50);
DELETE FROM t WHERE a = 'x';
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 17: Partial indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 17: Partial indexes ---"

# 17a. Partial index basic operations
oracle "cat17_partial_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, status TEXT, val INT);
CREATE INDEX idx ON t(val) WHERE status = 'active';
INSERT INTO t VALUES(1, 'active', 10), (2, 'inactive', 20), (3, 'active', 30);
SELECT val FROM t WHERE status = 'active' ORDER BY val;
UPDATE t SET status = 'active' WHERE id = 2;
SELECT val FROM t WHERE status = 'active' ORDER BY val;
"

# 17b. Partial index with UPDATE moving rows in/out
oracle "cat17_partial_update_inout" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flag INT, val INT);
CREATE INDEX idx ON t(val) WHERE flag = 1;
INSERT INTO t VALUES(1, 1, 100), (2, 0, 200), (3, 1, 300), (4, 0, 400);
UPDATE t SET flag = 0 WHERE id = 1;
UPDATE t SET flag = 1 WHERE id = 2;
SELECT id, val FROM t WHERE flag = 1 ORDER BY val;
PRAGMA integrity_check;
"

# 17c. Partial index with DELETE
oracle "cat17_partial_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, type TEXT, val INT);
CREATE INDEX idx ON t(val) WHERE type != 'hidden';
INSERT INTO t VALUES(1, 'visible', 10), (2, 'hidden', 20), (3, 'visible', 30);
DELETE FROM t WHERE type = 'visible';
SELECT * FROM t ORDER BY id;
INSERT INTO t VALUES(4, 'visible', 40);
SELECT id, val FROM t WHERE type != 'hidden' ORDER BY val;
PRAGMA integrity_check;
"

# 17d. Partial index with NULL condition
oracle "cat17_partial_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(b) WHERE a IS NOT NULL;
INSERT INTO t VALUES(1, 10, 100), (2, NULL, 200), (3, 30, 300), (4, NULL, 400);
SELECT id, b FROM t WHERE a IS NOT NULL ORDER BY b;
UPDATE t SET a = 99 WHERE id = 2;
SELECT id, b FROM t WHERE a IS NOT NULL ORDER BY b;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 18: Multi-table and complex queries
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 18: Multi-table and complex queries ---"

# 18a. Correlated subquery with index
oracle "cat18_correlated_subquery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx1 ON t1(grp);
CREATE INDEX idx2 ON t2(grp);
INSERT INTO t1 VALUES(1, 'a', 10), (2, 'a', 20), (3, 'b', 30);
INSERT INTO t2 VALUES(1, 'a', 100), (2, 'b', 200);
SELECT t1.id, t1.val, (SELECT sum(t2.val) FROM t2 WHERE t2.grp = t1.grp)
FROM t1 ORDER BY t1.id;
"

# 18b. DELETE with EXISTS subquery
oracle "cat18_delete_exists" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INT);
CREATE INDEX idx ON child(parent_id);
INSERT INTO parent VALUES(1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO child VALUES(1, 1), (2, 1), (3, 3);
DELETE FROM parent WHERE NOT EXISTS (SELECT 1 FROM child WHERE child.parent_id = parent.id);
SELECT * FROM parent ORDER BY id;
"

# 18c. INSERT INTO ... SELECT with index
oracle "cat18_insert_select" "
CREATE TABLE src(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE dst(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx_src ON src(val);
CREATE INDEX idx_dst ON dst(val);
INSERT INTO src VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
INSERT INTO dst SELECT id + 100, val * 2 FROM src WHERE val > 20;
SELECT * FROM dst ORDER BY id;
SELECT count(*) FROM dst WHERE val > 50;
"

# 18d. UPDATE with multi-table WHERE
oracle "cat18_update_multi_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category INT, val INT);
CREATE TABLE cats(id INTEGER PRIMARY KEY, multiplier INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40);
INSERT INTO cats VALUES(1, 10), (2, 100);
UPDATE t SET val = val * (SELECT multiplier FROM cats WHERE cats.id = t.category);
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 19: Concurrent cursor / iteration edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 19: Concurrent cursor / iteration edge cases ---"

# 19a. Trigger modifies same table during UPDATE
oracle "cat19_trigger_same_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, log INT DEFAULT 0);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg AFTER UPDATE OF val ON t
BEGIN
  UPDATE t SET log = log + 1 WHERE id = NEW.id;
END;
INSERT INTO t VALUES(1, 10, 0), (2, 20, 0), (3, 30, 0);
UPDATE t SET val = val + 5 WHERE id <= 2;
SELECT * FROM t ORDER BY id;
"

# 19b. Trigger INSERT into same table
oracle "cat19_trigger_insert_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg AFTER INSERT ON t WHEN NEW.val < 100
BEGIN
  INSERT OR IGNORE INTO t VALUES(NEW.id + 1000, NEW.val + 100);
END;
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
SELECT * FROM t ORDER BY id;
"

# 19c. FK CASCADE DELETE with index
oracle "cat19_fk_cascade" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE CASCADE, val INT);
CREATE INDEX idx ON child(parent_id);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1, 'a'), (2, 'b');
INSERT INTO child VALUES(1, 1, 10), (2, 1, 20), (3, 2, 30);
DELETE FROM parent WHERE id = 1;
SELECT * FROM child ORDER BY id;
SELECT count(*) FROM child;
"

# 19d. CREATE INDEX on table with pending changes
oracle "cat19_create_index_pending" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 20), (5, 10);
CREATE INDEX idx ON t(val);
SELECT val FROM t WHERE val = 20 ORDER BY id;
UPDATE t SET val = val + 1;
SELECT val FROM t ORDER BY val;
PRAGMA integrity_check;
"

# 19e. DROP and recreate index
oracle "cat19_drop_recreate_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
UPDATE t SET val = val * 2;
DROP INDEX idx;
CREATE INDEX idx ON t(val);
SELECT val FROM t ORDER BY val;
PRAGMA integrity_check;
"

# 19f. REINDEX after mutations
oracle "cat19_reindex" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 30), (2, 10), (3, 20);
UPDATE t SET val = val + 100;
REINDEX;
SELECT val FROM t ORDER BY val;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 20: Stress and scale edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 20: Stress and scale edge cases ---"

# 20a. Many columns in index
oracle "cat20_many_columns" "
CREATE TABLE t(a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT,
               i INT, j INT, k INT, l INT, m INT, n INT, o INT, p INT,
               PRIMARY KEY(a));
CREATE INDEX idx ON t(b, c, d, e, f, g, h);
INSERT INTO t VALUES(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
INSERT INTO t VALUES(2, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17);
UPDATE t SET h = h + 100;
SELECT a, h FROM t ORDER BY a;
PRAGMA integrity_check;
"

# 20b. Many indexes on one table
oracle "cat20_many_indexes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT, e INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
CREATE INDEX idx_d ON t(d);
CREATE INDEX idx_e ON t(e);
CREATE INDEX idx_ab ON t(a, b);
CREATE INDEX idx_cd ON t(c, d);
INSERT INTO t VALUES(1, 10, 20, 30, 40, 50);
INSERT INTO t VALUES(2, 11, 21, 31, 41, 51);
INSERT INTO t VALUES(3, 12, 22, 32, 42, 52);
UPDATE t SET a=a+100, b=b+100, c=c+100, d=d+100, e=e+100;
DELETE FROM t WHERE id = 2;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# 20c. Rapid sequential mutations
oracle "cat20_rapid_mutations" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 1);
UPDATE t SET val = 2 WHERE id = 1;
UPDATE t SET val = 3 WHERE id = 1;
UPDATE t SET val = 4 WHERE id = 1;
UPDATE t SET val = 5 WHERE id = 1;
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 100);
UPDATE t SET val = 200 WHERE id = 1;
SELECT * FROM t;
"

# 20d. Large batch UPDATE
oracle "cat20_large_batch_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1000)
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val + 10000 WHERE id % 2 = 0;
SELECT count(*) FROM t WHERE val > 10000;
SELECT count(*) FROM t WHERE val < 10000;
SELECT min(val) FROM t;
SELECT max(val) FROM t;
"

# 20e. Interleaved INSERT and DELETE
oracle "cat20_interleaved_ins_del" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(3, 30);
DELETE FROM t WHERE id = 2;
INSERT INTO t VALUES(4, 40);
INSERT INTO t VALUES(5, 50);
DELETE FROM t WHERE id = 4;
SELECT * FROM t ORDER BY id;
SELECT val FROM t ORDER BY val;
"

# 20f. WITHOUT ROWID: many rows, update all, verify count and sum
oracle "cat20_wr_large_update" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<500)
INSERT INTO t SELECT x / 10, x, x * 3 FROM r;
UPDATE t SET c = c + 10000 WHERE b % 5 = 0;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE c > 10000;
SELECT min(c) FROM t WHERE c > 10000;
"

# ════════════════════════════════════════════════════════════════════
# Category 21: Aggregate queries with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 21: Aggregate queries with indexes ---"

# 21a. GROUP BY on indexed column
oracle "cat21_group_by_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40),(5,'c',50);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;
"

# 21b. GROUP BY with HAVING
oracle "cat21_group_by_having" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100);
SELECT grp, sum(val) as s FROM t GROUP BY grp HAVING sum(val) > 20 ORDER BY grp;
"

# 21c. DISTINCT on indexed column
oracle "cat21_distinct_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,10),(4,30),(5,20),(6,10);
SELECT DISTINCT val FROM t ORDER BY val;
"

# 21d. COUNT with WHERE on indexed column
oracle "cat21_count_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, status TEXT, val INT);
CREATE INDEX idx ON t(status);
INSERT INTO t VALUES(1,'active',10),(2,'inactive',20),(3,'active',30),(4,'active',40);
SELECT status, count(*) FROM t GROUP BY status ORDER BY status;
SELECT count(*) FROM t WHERE status = 'active';
"

# 21e. Aggregate after UPDATE
oracle "cat21_agg_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50);
UPDATE t SET grp = 1 WHERE id = 5;
SELECT grp, count(*), sum(val), min(val), max(val) FROM t GROUP BY grp ORDER BY grp;
"

# 21f. DISTINCT with composite index
oracle "cat21_distinct_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,'x',1),(2,'x',1),(3,'x',2),(4,'y',1),(5,'y',1);
SELECT DISTINCT a, b FROM t ORDER BY a, b;
"

# 21g. GROUP BY on expression
oracle "cat21_group_by_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,15),(2,25),(3,35),(4,12),(5,28),(6,31);
SELECT (val / 10) * 10 as bucket, count(*) FROM t GROUP BY bucket ORDER BY bucket;
"

# 21h. Aggregate with NULL groups
oracle "cat21_agg_null_groups" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,NULL,10),(2,'a',20),(3,NULL,30),(4,'a',40),(5,'b',50);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;
"

# ════════════════════════════════════════════════════════════════════
# Category 22: Window functions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 22: Window functions ---"

# 22a. ROW_NUMBER with PARTITION BY
oracle "cat22_row_number" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',30),(2,'a',10),(3,'a',20),(4,'b',50),(5,'b',40);
SELECT id, grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) as rn
FROM t ORDER BY grp, rn;
"

# 22b. RANK and DENSE_RANK
oracle "cat22_rank" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,20),(4,30),(5,30),(6,30);
SELECT id, val, rank() OVER (ORDER BY val) as rnk,
       dense_rank() OVER (ORDER BY val) as drnk
FROM t ORDER BY id;
"

# 22c. SUM window with frame
oracle "cat22_sum_frame" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id, val,
  sum(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as s
FROM t ORDER BY id;
"

# 22d. LAG and LEAD
oracle "cat22_lag_lead" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id, val, lag(val,1) OVER (ORDER BY id) as prev,
       lead(val,1) OVER (ORDER BY id) as next
FROM t ORDER BY id;
"

# 22e. Window with PARTITION BY and aggregate
oracle "cat22_partition_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, dept TEXT, salary INT);
CREATE INDEX idx ON t(dept);
INSERT INTO t VALUES(1,'eng',100),(2,'eng',120),(3,'eng',110);
INSERT INTO t VALUES(4,'sales',80),(5,'sales',90);
SELECT id, dept, salary,
  avg(salary) OVER (PARTITION BY dept) as dept_avg,
  salary - avg(salary) OVER (PARTITION BY dept) as diff
FROM t ORDER BY id;
"

# 22f. NTILE window function
oracle "cat22_ntile" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60);
SELECT id, val, ntile(3) OVER (ORDER BY val) as tile FROM t ORDER BY id;
"

# 22g. Window after UPDATE
oracle "cat22_window_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
UPDATE t SET val = val + 100 WHERE grp = 'a';
SELECT grp, val, sum(val) OVER (PARTITION BY grp ORDER BY val) as running
FROM t ORDER BY grp, val;
"

# ════════════════════════════════════════════════════════════════════
# Category 23: CTEs (WITH clause)
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 23: CTEs (WITH clause) ---"

# 23a. Non-recursive CTE with indexed table
oracle "cat23_cte_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
WITH top3 AS (SELECT * FROM t WHERE val > 20 ORDER BY val)
SELECT id, val FROM top3 ORDER BY val DESC;
"

# 23b. Recursive CTE generating data then inserting
oracle "cat23_cte_recursive_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<50)
INSERT INTO t SELECT x, x * x FROM r;
SELECT count(*) FROM t;
SELECT val FROM t WHERE val > 2400 ORDER BY val;
"

# 23c. CTE used in UPDATE
oracle "cat23_cte_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, bonus INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0),(4,40,0),(5,50,0);
WITH high AS (SELECT id FROM t WHERE val > 25)
UPDATE t SET bonus = 100 WHERE id IN (SELECT id FROM high);
SELECT * FROM t ORDER BY id;
"

# 23d. CTE used in DELETE
oracle "cat23_cte_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<20)
INSERT INTO t SELECT x, x * 10 FROM r;
WITH low AS (SELECT id FROM t WHERE val < 100)
DELETE FROM t WHERE id IN (SELECT id FROM low);
SELECT count(*) FROM t;
SELECT min(val) FROM t;
"

# 23e. Multiple CTEs
oracle "cat23_multi_cte" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);
WITH
  grp_sums AS (SELECT grp, sum(val) as total FROM t GROUP BY grp),
  big_grps AS (SELECT grp FROM grp_sums WHERE total > 25)
SELECT t.* FROM t WHERE grp IN (SELECT grp FROM big_grps) ORDER BY id;
"

# 23f. Recursive CTE tree traversal
oracle "cat23_cte_tree" "
CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INT, name TEXT);
CREATE INDEX idx ON tree(parent_id);
INSERT INTO tree VALUES(1,NULL,'root'),(2,1,'child1'),(3,1,'child2');
INSERT INTO tree VALUES(4,2,'grandchild1'),(5,2,'grandchild2'),(6,3,'grandchild3');
WITH RECURSIVE ancestors(id, name, depth) AS (
  SELECT id, name, 0 FROM tree WHERE parent_id IS NULL
  UNION ALL
  SELECT t.id, t.name, a.depth+1 FROM tree t JOIN ancestors a ON t.parent_id = a.id
)
SELECT id, name, depth FROM ancestors ORDER BY depth, id;
"

# ════════════════════════════════════════════════════════════════════
# Category 24: JOINs with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 24: JOINs with indexes ---"

# 24a. INNER JOIN on indexed columns
oracle "cat24_inner_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, label TEXT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,'a'),(2,1,'b'),(3,2,'c'),(4,9,'orphan');
SELECT t1.id, t1.val, t2.label FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id, t2.id;
"

# 24b. LEFT JOIN preserving NULLs
oracle "cat24_left_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, val INT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,1,100),(2,1,200),(3,3,300);
SELECT t1.name, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id, t2.id;
"

# 24c. Self-join with index
oracle "cat24_self_join" "
CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, mgr_id INT);
CREATE INDEX idx ON emp(mgr_id);
INSERT INTO emp VALUES(1,'Alice',NULL),(2,'Bob',1),(3,'Carol',1),(4,'Dave',2);
SELECT e.name, m.name as manager
FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id ORDER BY e.id;
"

# 24d. Multi-table JOIN
oracle "cat24_multi_join" "
CREATE TABLE orders(id INTEGER PRIMARY KEY, cust_id INT, total INT);
CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE items(id INTEGER PRIMARY KEY, order_id INT, product TEXT);
CREATE INDEX idx_o ON orders(cust_id);
CREATE INDEX idx_i ON items(order_id);
INSERT INTO customers VALUES(1,'Alice'),(2,'Bob');
INSERT INTO orders VALUES(1,1,100),(2,1,200),(3,2,300);
INSERT INTO items VALUES(1,1,'widget'),(2,1,'gadget'),(3,2,'widget'),(4,3,'gizmo');
SELECT c.name, o.total, i.product
FROM customers c JOIN orders o ON c.id = o.cust_id JOIN items i ON o.id = i.order_id
ORDER BY c.name, o.id, i.id;
"

# 24e. JOIN after mutations
oracle "cat24_join_after_mutations" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT, data TEXT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,'a'),(2,2,'b'),(3,3,'c');
UPDATE t1 SET val = val + 100 WHERE id = 2;
DELETE FROM t2 WHERE ref = 1;
INSERT INTO t2 VALUES(4,2,'d');
SELECT t1.id, t1.val, t2.data FROM t1 JOIN t2 ON t1.id = t2.ref ORDER BY t1.id, t2.id;
"

# 24f. JOIN with composite index
oracle "cat24_join_composite" "
CREATE TABLE t1(a TEXT, b INT, val INT, PRIMARY KEY(a, b));
CREATE TABLE t2(id INTEGER PRIMARY KEY, a TEXT, b INT, extra TEXT);
CREATE INDEX idx ON t2(a, b);
INSERT INTO t1 VALUES('x',1,10),('x',2,20),('y',1,30);
INSERT INTO t2 VALUES(1,'x',1,'p'),(2,'x',2,'q'),(3,'y',1,'r'),(4,'z',1,'s');
SELECT t1.a, t1.b, t1.val, t2.extra
FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b ORDER BY t1.a, t1.b;
"

# ════════════════════════════════════════════════════════════════════
# Category 25: UNION / INTERSECT / EXCEPT
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 25: UNION / INTERSECT / EXCEPT ---"

# 25a. UNION removes duplicates
oracle "cat25_union" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,30),(3,40);
SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val;
"

# 25b. UNION ALL keeps duplicates
oracle "cat25_union_all" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t1 VALUES(1,10),(2,20);
INSERT INTO t2 VALUES(1,20),(2,30);
SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val;
"

# 25c. INTERSECT
oracle "cat25_intersect" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,30),(3,40);
SELECT val FROM t1 INTERSECT SELECT val FROM t2 ORDER BY val;
"

# 25d. EXCEPT
oracle "cat25_except" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,40);
SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val;
"

# 25e. UNION with NULLs
oracle "cat25_union_nulls" "
CREATE TABLE t1(val INT);
CREATE TABLE t2(val INT);
INSERT INTO t1 VALUES(NULL),(1),(2);
INSERT INTO t2 VALUES(NULL),(2),(3);
SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val;
"

# 25f. Compound after mutations
oracle "cat25_compound_after_mutation" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,10),(2,40),(3,50);
DELETE FROM t1 WHERE val = 20;
UPDATE t2 SET val = 30 WHERE id = 1;
SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val;
SELECT val FROM t1 INTERSECT SELECT val FROM t2 ORDER BY val;
SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val;
"

# ════════════════════════════════════════════════════════════════════
# Category 26: IN operator with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 26: IN operator with indexes ---"

# 26a. IN with literal list on indexed column
oracle "cat26_in_list" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val IN (10, 30, 50) ORDER BY id;
"

# 26b. IN with subquery
oracle "cat26_in_subquery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, target INT);
CREATE INDEX idx ON t1(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
INSERT INTO t2 VALUES(1,20),(2,40);
SELECT id, val FROM t1 WHERE val IN (SELECT target FROM t2) ORDER BY id;
"

# 26c. NOT IN
oracle "cat26_not_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val NOT IN (20, 40) ORDER BY id;
"

# 26d. IN with NULLs
oracle "cat26_in_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,NULL),(2,10),(3,20),(4,NULL);
SELECT id FROM t WHERE val IN (10, NULL) ORDER BY id;
SELECT id FROM t WHERE val NOT IN (10) ORDER BY id;
"

# 26e. IN on composite index prefix
oracle "cat26_in_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,'x',1,10),(2,'x',2,20),(3,'y',1,30),(4,'y',2,40),(5,'z',1,50);
SELECT id, a, b FROM t WHERE a IN ('x', 'z') ORDER BY a, b;
"

# 26f. Multi-column IN
oracle "cat26_multi_col_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,1,10),(2,1,20),(3,2,10),(4,2,20),(5,3,30);
SELECT id FROM t WHERE (a, b) IN ((1,10),(2,20),(3,30)) ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 27: LIKE and pattern matching with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 27: LIKE and pattern matching with indexes ---"

# 27a. LIKE prefix optimization
oracle "cat27_like_prefix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'alice'),(2,'bob'),(3,'alice2'),(4,'carol'),(5,'ali');
SELECT name FROM t WHERE name LIKE 'ali%' ORDER BY name;
"

# 27b. LIKE with no wildcard
oracle "cat27_like_exact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'hello'),(2,'world'),(3,'HELLO');
SELECT id FROM t WHERE name LIKE 'hello' ORDER BY id;
"

# 27c. GLOB pattern
oracle "cat27_glob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'abc'),(2,'abd'),(3,'bcd'),(4,'abc123');
SELECT name FROM t WHERE name GLOB 'ab*' ORDER BY name;
"

# 27d. LIKE after UPDATE
oracle "cat27_like_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT);
CREATE INDEX idx ON t(tag);
INSERT INTO t VALUES(1,'test_a'),(2,'test_b'),(3,'prod_a'),(4,'prod_b');
UPDATE t SET tag = 'staging_' || substr(tag, 6) WHERE tag LIKE 'test_%';
SELECT tag FROM t ORDER BY tag;
SELECT tag FROM t WHERE tag LIKE 'staging_%' ORDER BY tag;
"

# 27e. LIKE with escape character
oracle "cat27_like_escape" "
CREATE TABLE t(id INTEGER PRIMARY KEY, path TEXT);
CREATE INDEX idx ON t(path);
INSERT INTO t VALUES(1,'a%b'),(2,'a_b'),(3,'axb'),(4,'a%bc');
SELECT path FROM t WHERE path LIKE 'a!%b%' ESCAPE '!' ORDER BY path;
"

# ════════════════════════════════════════════════════════════════════
# Category 28: ALTER TABLE with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 28: ALTER TABLE with indexes ---"

# 28a. ADD COLUMN then query with index
oracle "cat28_add_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'none';
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE val > 15 ORDER BY id;
INSERT INTO t VALUES(4, 40, 'has_extra');
SELECT * FROM t ORDER BY id;
"

# 28b. RENAME TABLE preserves indexes
oracle "cat28_rename_table" "
CREATE TABLE old_name(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON old_name(val);
INSERT INTO old_name VALUES(1,10),(2,20),(3,30);
ALTER TABLE old_name RENAME TO new_name;
SELECT val FROM new_name WHERE val > 15 ORDER BY val;
INSERT INTO new_name VALUES(4,25);
SELECT val FROM new_name ORDER BY val;
"

# 28c. RENAME COLUMN with index
oracle "cat28_rename_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, old_col INT);
CREATE INDEX idx ON t(old_col);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
ALTER TABLE t RENAME COLUMN old_col TO new_col;
SELECT new_col FROM t WHERE new_col > 15 ORDER BY new_col;
UPDATE t SET new_col = new_col + 100;
SELECT new_col FROM t ORDER BY new_col;
"

# 28d. ADD COLUMN to WITHOUT ROWID
oracle "cat28_add_column_wr" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,100),('y',2,200);
ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'new';
SELECT * FROM t ORDER BY a, b;
INSERT INTO t VALUES('z',3,300,'custom');
SELECT * FROM t ORDER BY a;
"

# ════════════════════════════════════════════════════════════════════
# Category 29: VACUUM and maintenance
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 29: VACUUM and maintenance ---"

# 29a. VACUUM after heavy mutation
oracle "cat29_vacuum_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<200)
INSERT INTO t SELECT x, x FROM c;
DELETE FROM t WHERE id % 2 = 0;
UPDATE t SET val = val + 1000 WHERE id % 3 = 0;
VACUUM;
SELECT count(*) FROM t;
SELECT min(val) FROM t;
SELECT max(val) FROM t;
PRAGMA integrity_check;
"

# 29b. VACUUM empty table
oracle "cat29_vacuum_empty" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20);
DELETE FROM t;
VACUUM;
SELECT count(*) FROM t;
INSERT INTO t VALUES(1,100);
SELECT * FROM t;
PRAGMA integrity_check;
"

# 29c. ANALYZE updates statistics
oracle "cat29_analyze" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x % 10 FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE val = 5;
"

# ════════════════════════════════════════════════════════════════════
# Category 30: Autoincrement
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 30: Autoincrement ---"

# 30a. Basic AUTOINCREMENT
oracle "cat30_autoincrement_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT);
INSERT INTO t(val) VALUES('a'),('b'),('c');
SELECT * FROM t ORDER BY id;
DELETE FROM t WHERE id = 2;
INSERT INTO t(val) VALUES('d');
SELECT * FROM t ORDER BY id;
"

# 30b. AUTOINCREMENT doesn't reuse deleted IDs
oracle "cat30_autoincrement_no_reuse" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT);
INSERT INTO t(val) VALUES('first'),('second'),('third');
DELETE FROM t;
INSERT INTO t(val) VALUES('after_delete');
SELECT * FROM t;
"

# 30c. AUTOINCREMENT with explicit ID
oracle "cat30_autoincrement_explicit" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT);
INSERT INTO t(val) VALUES('a');
INSERT INTO t VALUES(100, 'b');
INSERT INTO t(val) VALUES('c');
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 31: Expression indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 31: Expression indexes ---"

# 31a. Index on UPPER()
oracle "cat31_expr_upper" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(UPPER(name));
INSERT INTO t VALUES(1,'Alice'),(2,'BOB'),(3,'carol');
SELECT id, name FROM t WHERE UPPER(name) = 'ALICE';
SELECT id, name FROM t WHERE UPPER(name) > 'B' ORDER BY UPPER(name);
"

# 31b. Index on arithmetic expression
oracle "cat31_expr_arithmetic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a + b);
INSERT INTO t VALUES(1,10,5),(2,3,12),(3,8,8),(4,1,1);
SELECT id, a, b FROM t WHERE a + b = 15 ORDER BY id;
SELECT id, a + b as total FROM t ORDER BY total;
"

# 31c. Index on substr
oracle "cat31_expr_substr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX idx ON t(substr(code, 1, 3));
INSERT INTO t VALUES(1,'ABC-001'),(2,'ABC-002'),(3,'DEF-001'),(4,'ABC-003');
SELECT id, code FROM t WHERE substr(code, 1, 3) = 'ABC' ORDER BY id;
"

# 31d. Expression index after UPDATE
oracle "cat31_expr_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val * 2);
INSERT INTO t VALUES(1,5),(2,10),(3,15);
UPDATE t SET val = val + 10;
SELECT id, val FROM t WHERE val * 2 = 30 ORDER BY id;
SELECT id, val FROM t ORDER BY val * 2;
"

# ════════════════════════════════════════════════════════════════════
# Category 32: Covering index scans
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 32: Covering index scans ---"

# 32a. Index covers all needed columns
oracle "cat32_covering_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,10,100,'x'),(2,20,200,'y'),(3,10,300,'z');
SELECT a, b FROM t WHERE a = 10 ORDER BY b;
"

# 32b. Covering index with aggregate
oracle "cat32_covering_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
SELECT grp, sum(val) FROM t GROUP BY grp ORDER BY grp;
SELECT grp, min(val), max(val) FROM t GROUP BY grp ORDER BY grp;
"

# 32c. Covering index after mutations
oracle "cat32_covering_after_mutations" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
UPDATE t SET b = b + 1000 WHERE a = 20;
DELETE FROM t WHERE a = 10;
INSERT INTO t VALUES(4,40,400);
SELECT a, b FROM t ORDER BY a;
"

# ════════════════════════════════════════════════════════════════════
# Category 33: Foreign key cascades
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 33: Foreign key cascades ---"

# 33a. ON DELETE CASCADE
oracle "cat33_fk_cascade_delete" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE, val INT);
CREATE INDEX idx ON child(pid);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO child VALUES(1,1,10),(2,1,20),(3,2,30),(4,3,40);
DELETE FROM parent WHERE id = 1;
SELECT * FROM child ORDER BY id;
"

# 33b. ON UPDATE CASCADE
oracle "cat33_fk_cascade_update" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE CASCADE, val INT);
CREATE INDEX idx ON child(pid);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1,'a'),(2,'b');
INSERT INTO child VALUES(1,1,10),(2,1,20),(3,2,30);
UPDATE parent SET id = 10 WHERE id = 1;
SELECT * FROM child ORDER BY id;
"

# 33c. ON DELETE SET NULL
oracle "cat33_fk_set_null" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL);
CREATE INDEX idx ON child(pid);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,1),(2,1),(3,2);
DELETE FROM parent WHERE id = 1;
SELECT * FROM child ORDER BY id;
"

# 33d. Multi-level cascade
oracle "cat33_fk_multi_level" "
CREATE TABLE t1(id INTEGER PRIMARY KEY);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT REFERENCES t1(id) ON DELETE CASCADE);
CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INT REFERENCES t2(id) ON DELETE CASCADE);
CREATE INDEX idx2 ON t2(t1_id);
CREATE INDEX idx3 ON t3(t2_id);
PRAGMA foreign_keys = ON;
INSERT INTO t1 VALUES(1),(2);
INSERT INTO t2 VALUES(1,1),(2,1),(3,2);
INSERT INTO t3 VALUES(1,1),(2,2),(3,3);
DELETE FROM t1 WHERE id = 1;
SELECT * FROM t2 ORDER BY id;
SELECT * FROM t3 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 34: RETURNING clause
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 34: RETURNING clause ---"

# 34a. INSERT RETURNING
oracle "cat34_insert_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20) RETURNING id, val;
"

# 34b. UPDATE RETURNING
oracle "cat34_update_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val + 100 WHERE id >= 2 RETURNING id, val;
"

# 34c. DELETE RETURNING
oracle "cat34_delete_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
DELETE FROM t WHERE val > 15 RETURNING id, val;
"

# 34d. INSERT OR REPLACE RETURNING
oracle "cat34_replace_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10);
INSERT OR REPLACE INTO t VALUES(1,99) RETURNING id, val;
SELECT * FROM t;
"

# 34e. RETURNING with WITHOUT ROWID
oracle "cat34_returning_wr" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,100),('y',2,200) RETURNING *;
UPDATE t SET c = c + 1000 RETURNING a, b, c;
"

# ════════════════════════════════════════════════════════════════════
# Category 35: Complex subqueries
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 35: Complex subqueries ---"

# 35a. Scalar subquery in SELECT
oracle "cat35_scalar_subquery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, amount INT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,100),(2,1,200),(3,2,300);
SELECT id, val, (SELECT sum(amount) FROM t2 WHERE t2.t1_id = t1.id) as total
FROM t1 ORDER BY id;
"

# 35b. EXISTS subquery
oracle "cat35_exists" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1),(2,3);
SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.id) ORDER BY id;
SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.id) ORDER BY id;
"

# 35c. Subquery in FROM (derived table)
oracle "cat35_derived_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
SELECT d.grp, d.total FROM (SELECT grp, sum(val) as total FROM t GROUP BY grp) d
WHERE d.total > 25 ORDER BY d.grp;
"

# 35d. Nested subqueries
oracle "cat35_nested" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT * FROM t WHERE val > (SELECT avg(val) FROM t WHERE val < (SELECT max(val) FROM t))
ORDER BY id;
"

# 35e. Correlated subquery in WHERE
oracle "cat35_correlated_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',30),(3,'b',20),(4,'b',40),(5,'a',20);
SELECT * FROM t t1 WHERE val = (SELECT max(val) FROM t t2 WHERE t2.grp = t1.grp)
ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 36: Temp tables and attached databases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 36: Temp tables and attached databases ---"

# 36a. TEMP table SELECT with index
oracle "cat36_temp_select" "
CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,30),(2,10),(3,20);
SELECT * FROM t ORDER BY val;
SELECT count(*) FROM t WHERE val > 15;
"

# 36b. TEMP and main table interaction (read-only on temp)
oracle "cat36_temp_main_interaction" "
CREATE TABLE main_t(id INTEGER PRIMARY KEY, val INT);
CREATE TEMP TABLE temp_t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO main_t VALUES(1,10),(2,20);
INSERT INTO temp_t VALUES(1,100),(2,200);
SELECT m.val, t.val FROM main_t m JOIN temp_t t ON m.id = t.id ORDER BY m.id;
"

# 36c. Attached database (read-only operations)
oracle "cat36_attach_read" "
ATTACH ':memory:' AS db2;
CREATE TABLE db2.t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO db2.t VALUES(1,10),(2,20),(3,30);
SELECT * FROM db2.t ORDER BY val;
SELECT count(*) FROM db2.t WHERE val > 15;
DETACH db2;
"

# 36d. Cross-database JOIN
oracle "cat36_cross_db" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t1 VALUES(1,10),(2,20);
ATTACH ':memory:' AS db2;
CREATE TABLE db2.t2(id INTEGER PRIMARY KEY, ref INT, label TEXT);
INSERT INTO db2.t2 VALUES(1,1,'a'),(2,2,'b');
SELECT t1.val, t2.label FROM t1 JOIN db2.t2 t2 ON t1.id = t2.ref ORDER BY t1.id;
DETACH db2;
"

# ════════════════════════════════════════════════════════════════════
# Category 37: Generated columns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 37: Generated columns ---"

# 37a. STORED generated column with index
oracle "cat37_generated_stored" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED);
CREATE INDEX idx ON t(c);
INSERT INTO t(id, a, b) VALUES(1, 10, 5),(2, 20, 3),(3, 5, 15);
SELECT * FROM t ORDER BY c;
SELECT * FROM t WHERE c > 15 ORDER BY id;
"

# 37b. Generated column UPDATE
oracle "cat37_generated_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, doubled INT GENERATED ALWAYS AS (val * 2) STORED);
CREATE INDEX idx ON t(doubled);
INSERT INTO t(id, val) VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val + 5;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE doubled > 40 ORDER BY id;
"

# 37c. VIRTUAL generated column
oracle "cat37_generated_virtual" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first TEXT, last TEXT, full_name TEXT GENERATED ALWAYS AS (first || ' ' || last) VIRTUAL);
INSERT INTO t(id, first, last) VALUES(1,'John','Doe'),(2,'Jane','Smith');
SELECT full_name FROM t ORDER BY id;
UPDATE t SET first = 'Bob' WHERE id = 1;
SELECT full_name FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 38: Complex trigger chains
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 38: Complex trigger chains ---"

# 38a. BEFORE and AFTER trigger on same table
oracle "cat38_before_after" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, modified INT DEFAULT 0);
CREATE TABLE audit(action TEXT, old_val INT, new_val INT);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg_before BEFORE UPDATE ON t BEGIN
  INSERT INTO audit VALUES('before', OLD.val, NEW.val);
END;
CREATE TRIGGER trg_after AFTER UPDATE ON t BEGIN
  INSERT INTO audit VALUES('after', OLD.val, NEW.val);
END;
INSERT INTO t(id, val) VALUES(1,10),(2,20);
UPDATE t SET val = 99 WHERE id = 1;
SELECT * FROM audit ORDER BY rowid;
SELECT * FROM t ORDER BY id;
"

# 38b. Trigger chain across tables
oracle "cat38_trigger_chain" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t3(id INTEGER PRIMARY KEY, val INT);
CREATE TRIGGER trg1 AFTER INSERT ON t1 BEGIN
  INSERT INTO t2 VALUES(NEW.id, NEW.val * 2);
END;
CREATE TRIGGER trg2 AFTER INSERT ON t2 BEGIN
  INSERT INTO t3 VALUES(NEW.id, NEW.val * 2);
END;
INSERT INTO t1 VALUES(1,10),(2,20);
SELECT * FROM t1 ORDER BY id;
SELECT * FROM t2 ORDER BY id;
SELECT * FROM t3 ORDER BY id;
"

# 38c. INSTEAD OF trigger on view
oracle "cat38_instead_of_view" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
CREATE VIEW v AS SELECT * FROM t WHERE val > 10;
CREATE TRIGGER trg INSTEAD OF DELETE ON v BEGIN
  UPDATE t SET val = -1 WHERE id = OLD.id;
END;
DELETE FROM v WHERE id = 2;
SELECT * FROM t ORDER BY id;
"

# 38d. Recursive trigger prevention
oracle "cat38_trigger_prevent_infinite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, depth INT DEFAULT 0);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg AFTER UPDATE OF val ON t WHEN NEW.depth < 3
BEGIN
  UPDATE t SET val = NEW.val + 1, depth = NEW.depth + 1 WHERE id = NEW.id;
END;
PRAGMA recursive_triggers = ON;
INSERT INTO t VALUES(1, 0, 0);
UPDATE t SET val = 1 WHERE id = 1;
SELECT * FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 39: OR optimization and multi-index
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 39: OR optimization and multi-index ---"

# 39a. OR with separate indexes
oracle "cat39_or_separate_idx" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,100),(4,10,300),(5,50,500);
SELECT id FROM t WHERE a = 10 OR b = 100 ORDER BY id;
"

# 39b. OR within same indexed column
oracle "cat39_or_same_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val = 10 OR val = 30 OR val = 50 ORDER BY id;
"

# 39c. Complex OR with AND
oracle "cat39_or_and_mix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1,1,10,100),(2,1,20,200),(3,2,10,300),(4,2,20,400);
SELECT id FROM t WHERE (a = 1 AND b = 10) OR (a = 2 AND b = 20) ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 40: Misc edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 40: Misc edge cases ---"

# 40a. INSERT OR REPLACE on WITHOUT ROWID with no secondary index (prolly-specific)
oracle "cat40_replace_wr_no_secidx" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('a', 1);
REPLACE INTO t VALUES('a', 2);
REPLACE INTO t VALUES('b', 3);
SELECT * FROM t ORDER BY k;
SELECT count(*) FROM t;
"

# 40b. COALESCE in WHERE with index
oracle "cat40_coalesce_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,NULL),(2,10),(3,NULL),(4,20);
SELECT id FROM t WHERE COALESCE(val, 0) > 5 ORDER BY id;
"

# 40c. CASE in ORDER BY with index
oracle "cat40_case_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, status TEXT, val INT);
CREATE INDEX idx ON t(status);
INSERT INTO t VALUES(1,'high',10),(2,'low',20),(3,'medium',30),(4,'high',40);
SELECT * FROM t ORDER BY CASE status WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, val;
"

# 40d. VALUES clause
oracle "cat40_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT * FROM t WHERE val IN (VALUES(10),(30)) ORDER BY id;
"

# 40e. Multiple operations in single transaction affecting same index
oracle "cat40_multi_ops_same_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
BEGIN;
INSERT INTO t VALUES(1, 100);
INSERT INTO t VALUES(2, 200);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(3, 100);
UPDATE t SET val = 300 WHERE id = 2;
INSERT INTO t VALUES(4, 200);
COMMIT;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# 40f. Overflow in indexed aggregate
oracle "cat40_large_sum" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 1000000000),(2, 1000000000),(3, 1000000000);
SELECT sum(val) FROM t;
SELECT sum(val) FROM t WHERE val > 0;
"

# 40g. Empty string vs NULL in index
oracle "cat40_empty_vs_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL),(2, ''),(3, 'a'),(4, NULL),(5, '');
SELECT id, typeof(val), val FROM t ORDER BY val, id;
SELECT count(*) FROM t WHERE val IS NULL;
SELECT count(*) FROM t WHERE val = '';
SELECT count(*) FROM t WHERE val IS NOT NULL;
"

# 40h. WITHOUT ROWID: REPLACE then SELECT across statements
oracle "cat40_wr_replace_select" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES(1,1,100);
REPLACE INTO t VALUES(1,1,200);
SELECT * FROM t;
SELECT count(*) FROM t;
REPLACE INTO t VALUES(1,1,300);
SELECT * FROM t;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 40i. Index on boolean expression
oracle "cat40_bool_expr_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a > b);
INSERT INTO t VALUES(1,10,5),(2,3,7),(3,5,5),(4,8,2);
SELECT id, a, b, a > b FROM t ORDER BY (a > b), id;
"

# 40j. Zeroblob in indexed column
oracle "cat40_zeroblob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, zeroblob(0)),(2, zeroblob(4)),(3, X'00000000');
SELECT id, length(data), hex(data) FROM t ORDER BY data, id;
"

# ════════════════════════════════════════════════════════════════════
echo ""
echo "================================"
echo "Results: $pass passed, $fail failed"
echo "================================"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
exit 0
