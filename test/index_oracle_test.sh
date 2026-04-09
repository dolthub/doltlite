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

# 29a. VACUUM after heavy mutation (no-op for doltlite, must not crash)
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

# 36a. TEMP table CRUD with index
oracle "cat36_temp_crud" "
CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX temp.idx ON t(val);
INSERT INTO t VALUES(1,30),(2,10),(3,20);
SELECT * FROM t ORDER BY val;
UPDATE t SET val = val + 100;
SELECT * FROM t ORDER BY val;
DELETE FROM t WHERE val > 120;
SELECT count(*) FROM t;
SELECT * FROM t ORDER BY id;
"

# 36b. TEMP and main table interaction (read-only on temp)
oracle "cat36_temp_main_interaction" "
CREATE TABLE main_t(id INTEGER PRIMARY KEY, val INT);
CREATE TEMP TABLE temp_t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO main_t VALUES(1,10),(2,20);
INSERT INTO temp_t VALUES(1,100),(2,200);
SELECT m.val, t.val FROM main_t m JOIN temp_t t ON m.id = t.id ORDER BY m.id;
"

# 36c. Attached database CRUD
oracle "cat36_attach_crud" "
ATTACH ':memory:' AS db2;
CREATE TABLE db2.t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX db2.idx ON db2.t(val);
INSERT INTO db2.t VALUES(1,10),(2,20),(3,30);
UPDATE db2.t SET val = val * 2;
SELECT * FROM db2.t ORDER BY val;
DELETE FROM db2.t WHERE val > 40;
SELECT count(*) FROM db2.t;
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
# Category 41: Views with indexed base tables
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 41: Views with indexed base tables ---"

# 41a. Basic view on indexed table
oracle "cat41_view_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, status TEXT);
CREATE INDEX idx_val ON t(val);
CREATE INDEX idx_status ON t(status);
INSERT INTO t VALUES(1,25,'low'),(2,75,'high'),(3,100,'high'),(4,30,'low');
CREATE VIEW v AS SELECT id, val FROM t WHERE val > 50;
SELECT * FROM v ORDER BY val;
UPDATE t SET val = val + 50 WHERE status = 'low';
SELECT * FROM v ORDER BY val;
"

# 41b. View with JOIN
oracle "cat41_view_join" "
CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, cust_id INT, amount INT);
CREATE INDEX idx ON orders(cust_id);
INSERT INTO customers VALUES(1,'Alice'),(2,'Bob');
INSERT INTO orders VALUES(1,1,150),(2,2,50),(3,1,200);
CREATE VIEW v AS SELECT o.id as oid, c.name, o.amount FROM orders o JOIN customers c ON o.cust_id = c.id;
SELECT * FROM v WHERE amount > 100 ORDER BY amount DESC;
"

# 41c. View with aggregate
oracle "cat41_view_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
CREATE VIEW v AS SELECT grp, sum(val) as total, count(*) as cnt FROM t GROUP BY grp;
SELECT * FROM v ORDER BY grp;
INSERT INTO t VALUES(5,'a',50);
SELECT * FROM v ORDER BY grp;
"

# 41d. View used in subquery
oracle "cat41_view_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
CREATE VIEW high AS SELECT * FROM t WHERE val > 25;
SELECT * FROM t WHERE id IN (SELECT id FROM high) ORDER BY id;
"

# 41e. Nested views
oracle "cat41_nested_views" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300),(4,40,400);
CREATE VIEW v1 AS SELECT id, a FROM t WHERE a > 10;
CREATE VIEW v2 AS SELECT * FROM v1 WHERE a < 40;
SELECT * FROM v2 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 42: JSON functions with indexed tables
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 42: JSON functions with indexed tables ---"

# 42a. json_extract in SELECT with index filter
oracle "cat42_json_extract" "
CREATE TABLE data(id INTEGER PRIMARY KEY, doc TEXT, status INT);
CREATE INDEX idx ON data(status);
INSERT INTO data VALUES(1,'{\"name\":\"Alice\",\"age\":30}',1);
INSERT INTO data VALUES(2,'{\"name\":\"Bob\",\"age\":25}',1);
INSERT INTO data VALUES(3,'{\"name\":\"Carol\",\"age\":35}',0);
SELECT id, json_extract(doc,'$.name') FROM data WHERE status=1 ORDER BY id;
"

# 42b. json_each with indexed table
oracle "cat42_json_each" "
CREATE TABLE items(id INTEGER PRIMARY KEY, tags TEXT, type INT);
CREATE INDEX idx ON items(type);
INSERT INTO items VALUES(1,'[\"a\",\"b\",\"c\"]',1);
INSERT INTO items VALUES(2,'[\"x\",\"y\"]',1);
INSERT INTO items VALUES(3,'[\"p\",\"q\"]',2);
SELECT items.id, j.value FROM items, json_each(items.tags) j WHERE items.type=1 ORDER BY items.id, j.key;
"

# 42c. json_extract in WHERE
oracle "cat42_json_where" "
CREATE TABLE data(id INTEGER PRIMARY KEY, doc TEXT);
INSERT INTO data VALUES(1,'{\"score\":85}');
INSERT INTO data VALUES(2,'{\"score\":42}');
INSERT INTO data VALUES(3,'{\"score\":97}');
SELECT id FROM data WHERE json_extract(doc,'$.score') > 80 ORDER BY id;
"

# 42d. JSON after UPDATE
oracle "cat42_json_after_update" "
CREATE TABLE config(id INTEGER PRIMARY KEY, settings TEXT, active INT);
CREATE INDEX idx ON config(active);
INSERT INTO config VALUES(1,'{\"theme\":\"dark\"}',1);
INSERT INTO config VALUES(2,'{\"theme\":\"light\"}',0);
UPDATE config SET settings = json_set(settings,'$.theme','blue') WHERE active=1;
SELECT id, json_extract(settings,'$.theme') FROM config ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 43: Date/time functions with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 43: Date/time functions with indexes ---"

# 43a. date() in WHERE with index
oracle "cat43_date_where" "
CREATE TABLE events(id INTEGER PRIMARY KEY, event_date TEXT, importance INT);
CREATE INDEX idx ON events(importance);
INSERT INTO events VALUES(1,'2026-01-15',5),(2,'2026-02-10',3),(3,'2026-03-20',5),(4,'2026-04-05',2);
SELECT id FROM events WHERE date(event_date) > date('2026-02-01') AND importance > 2 ORDER BY id;
"

# 43b. date() in ORDER BY
oracle "cat43_date_order" "
CREATE TABLE tasks(id INTEGER PRIMARY KEY, due TEXT, priority INT);
CREATE INDEX idx ON tasks(priority);
INSERT INTO tasks VALUES(1,'2026-03-15',1),(2,'2026-01-10',2),(3,'2026-02-20',1);
SELECT id, due FROM tasks WHERE priority=1 ORDER BY date(due);
"

# 43c. date arithmetic
oracle "cat43_date_arithmetic" "
CREATE TABLE deadlines(id INTEGER PRIMARY KEY, base_date TEXT);
INSERT INTO deadlines VALUES(1,'2026-01-01'),(2,'2026-06-15'),(3,'2026-12-31');
SELECT id, date(base_date,'+30 days') as extended FROM deadlines ORDER BY id;
SELECT id FROM deadlines WHERE date(base_date,'+30 days') > '2026-07-01' ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 44: Math functions with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 44: Math functions with indexes ---"

# 44a. abs() in queries
oracle "cat44_abs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,-10.5),(2,20.3),(3,-5.0),(4,15.7);
SELECT id, abs(val) as a FROM t ORDER BY a;
SELECT id FROM t WHERE abs(val) > 10 ORDER BY id;
"

# 44b. min/max on indexed column
oracle "cat44_min_max" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30),(2,10),(3,50),(4,20),(5,40);
SELECT min(val), max(val) FROM t;
UPDATE t SET val = val + 100 WHERE val < 20;
SELECT min(val), max(val) FROM t;
"

# 44c. total() vs sum()
oracle "cat44_total_sum" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,NULL),(3,20),(4,NULL),(5,30);
SELECT sum(val), total(val), count(val), count(*) FROM t;
"

# 44d. Round and arithmetic in index context
oracle "cat44_round" "
CREATE TABLE t(id INTEGER PRIMARY KEY, price REAL, qty INT);
CREATE INDEX idx ON t(qty);
INSERT INTO t VALUES(1,9.99,3),(2,14.50,2),(3,7.25,5);
SELECT id, round(price * qty, 2) as total FROM t ORDER BY total;
SELECT id FROM t WHERE round(price * qty, 2) > 25 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 45: CAST expressions with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 45: CAST expressions with indexes ---"

# 45a. CAST text to integer in WHERE
oracle "cat45_cast_text_int" "
CREATE TABLE t(id INTEGER PRIMARY KEY, num_str TEXT, flag INT);
CREATE INDEX idx ON t(flag);
INSERT INTO t VALUES(1,'100',1),(2,'50',1),(3,'200',0),(4,'75',1);
SELECT id FROM t WHERE CAST(num_str AS INTEGER) > 60 AND flag=1 ORDER BY id;
"

# 45b. CAST with NULL
oracle "cat45_cast_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'42'),(2,NULL),(3,'99'),(4,'abc');
SELECT id, CAST(val AS INTEGER) FROM t ORDER BY id;
"

# 45c. CAST in aggregate
oracle "cat45_cast_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount TEXT, grp TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'10','a'),(2,'20','a'),(3,'30','b'),(4,'40','b');
SELECT grp, sum(CAST(amount AS INTEGER)) as total FROM t GROUP BY grp ORDER BY grp;
"

# ════════════════════════════════════════════════════════════════════
# Category 46: Multi-column UPDATE with multiple indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 46: Multi-column UPDATE with multiple indexes ---"

# 46a. Update all indexed columns simultaneously
oracle "cat46_multi_idx_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES(1,10,20,30),(2,15,25,35),(3,12,22,32);
UPDATE t SET a = a+100, b = b+200, c = c+300;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE a > 100 ORDER BY id;
SELECT id FROM t WHERE b > 200 ORDER BY id;
SELECT id FROM t WHERE c > 300 ORDER BY id;
PRAGMA integrity_check;
"

# 46b. Update with composite index
oracle "cat46_composite_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT, z INT);
CREATE INDEX idx_xy ON t(x, y);
INSERT INTO t VALUES(1,1,10,100),(2,2,20,200),(3,1,10,300);
UPDATE t SET x = x*2, y = y*2 WHERE z > 150;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE x = 2 ORDER BY id;
PRAGMA integrity_check;
"

# 46c. Update indexed and non-indexed together
oracle "cat46_mixed_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, indexed_col INT, non_indexed TEXT, also_indexed INT);
CREATE INDEX idx1 ON t(indexed_col);
CREATE INDEX idx2 ON t(also_indexed);
INSERT INTO t VALUES(1,10,'hello',100),(2,20,'world',200),(3,30,'foo',300);
UPDATE t SET indexed_col = indexed_col*10, non_indexed = 'changed', also_indexed = also_indexed+1;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE indexed_col > 100 ORDER BY id;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 47: INSERT INTO SELECT patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 47: INSERT INTO SELECT patterns ---"

# 47a. INSERT INTO SELECT with transformation
oracle "cat47_insert_select_transform" "
CREATE TABLE src(id INTEGER PRIMARY KEY, val INT, mult INT);
CREATE TABLE dst(id INTEGER PRIMARY KEY, computed INT);
CREATE INDEX idx_src ON src(mult);
CREATE INDEX idx_dst ON dst(computed);
INSERT INTO src VALUES(1,10,2),(2,20,3),(3,30,1),(4,40,2);
INSERT INTO dst SELECT NULL, val * mult FROM src WHERE mult > 1;
SELECT * FROM dst ORDER BY computed;
"

# 47b. INSERT INTO SELECT from same table
oracle "cat47_insert_self" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, gen INT DEFAULT 1);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,1),(2,20,1),(3,30,1);
INSERT INTO t SELECT NULL, val + 100, 2 FROM t WHERE gen = 1;
SELECT val, gen FROM t ORDER BY val;
"

# 47c. INSERT INTO SELECT with GROUP BY
oracle "cat47_insert_grouped" "
CREATE TABLE raw(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE TABLE summary(grp TEXT PRIMARY KEY, total INT, cnt INT);
CREATE INDEX idx ON raw(grp);
INSERT INTO raw VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'a',50);
INSERT INTO summary SELECT grp, sum(val), count(*) FROM raw GROUP BY grp;
SELECT * FROM summary ORDER BY grp;
"

# ════════════════════════════════════════════════════════════════════
# Category 48: CASE WHEN with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 48: CASE WHEN with indexes ---"

# 48a. CASE in SELECT with index filter
oracle "cat48_case_select" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INT, active INT);
CREATE INDEX idx ON t(active);
INSERT INTO t VALUES(1,95,1),(2,45,1),(3,75,0),(4,30,1),(5,85,1);
SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END as grade
FROM t WHERE active=1 ORDER BY score DESC;
"

# 48b. CASE in UPDATE SET
oracle "cat48_case_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, label TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,''),(2,20,''),(3,30,''),(4,40,'');
UPDATE t SET label = CASE WHEN val > 25 THEN 'high' WHEN val > 15 THEN 'mid' ELSE 'low' END;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE label = 'high' ORDER BY id;
"

# 48c. CASE in WHERE
oracle "cat48_case_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, type TEXT, val INT);
CREATE INDEX idx ON t(type);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40);
SELECT id, val FROM t WHERE CASE type WHEN 'a' THEN val > 20 WHEN 'b' THEN val > 30 END ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 49: String aggregates
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 49: String aggregates ---"

# 49a. GROUP_CONCAT basic
oracle "cat49_group_concat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INT, tag TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,1,'red'),(2,1,'blue'),(3,2,'green'),(4,1,'yellow'),(5,2,'purple');
SELECT grp, GROUP_CONCAT(tag,', ') FROM t GROUP BY grp ORDER BY grp;
"

# 49b. GROUP_CONCAT with DISTINCT
oracle "cat49_group_concat_distinct" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INT, val TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,1,'a'),(2,1,'b'),(3,1,'a'),(4,2,'c'),(5,2,'c');
SELECT grp, GROUP_CONCAT(DISTINCT val) FROM t GROUP BY grp ORDER BY grp;
"

# 49c. GROUP_CONCAT after mutation
oracle "cat49_group_concat_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, item TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'x','a'),(2,'x','b'),(3,'y','c');
UPDATE t SET grp = 'x' WHERE id = 3;
SELECT grp, GROUP_CONCAT(item,'+') FROM t GROUP BY grp ORDER BY grp;
"

# ════════════════════════════════════════════════════════════════════
# Category 50: Multi-statement transactions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 50: Multi-statement transactions ---"

# 50a. Multiple INSERTs in one transaction
oracle "cat50_multi_insert_txn" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
BEGIN;
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT count(*) FROM t;
COMMIT;
SELECT * FROM t ORDER BY id;
"

# 50b. INSERT + UPDATE + DELETE in one transaction
oracle "cat50_mixed_txn" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
BEGIN;
INSERT INTO t VALUES(4,40);
UPDATE t SET val = 99 WHERE id = 2;
DELETE FROM t WHERE id = 1;
SELECT * FROM t ORDER BY id;
COMMIT;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# 50c. Transaction rollback preserves state
oracle "cat50_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
BEGIN;
DELETE FROM t;
INSERT INTO t VALUES(10,100);
SELECT count(*) FROM t;
ROLLBACK;
SELECT * FROM t ORDER BY id;
"

# 50d. Multiple UPDATEs to different rows in one txn
oracle "cat50_multi_update_txn" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
BEGIN;
UPDATE t SET val = val + 1000 WHERE id = 1;
UPDATE t SET val = val + 2000 WHERE id = 3;
UPDATE t SET val = val + 3000 WHERE id = 5;
COMMIT;
SELECT * FROM t ORDER BY id;
SELECT val FROM t WHERE val > 100 ORDER BY val;
"

# 50e. Cross-table operations (separate transactions)
oracle "cat50_cross_table" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT, data TEXT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(ref);
INSERT INTO t1 VALUES(1,100),(2,200);
INSERT INTO t2 VALUES(1,1,'a'),(2,2,'b');
UPDATE t1 SET val = val + 50;
DELETE FROM t2 WHERE ref = 1;
SELECT * FROM t1 ORDER BY id;
SELECT * FROM t2 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 51: Unicode and special characters
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 51: Unicode and special characters ---"

# 51a. Unicode text in indexed column
oracle "cat51_unicode_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'hello'),(2,'café'),(3,'naïve'),(4,'über');
SELECT name FROM t ORDER BY name;
SELECT id FROM t WHERE name = 'café';
"

# 51b. Unicode in composite index
oracle "cat51_unicode_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, city TEXT, code INT);
CREATE INDEX idx ON t(city, code);
INSERT INTO t VALUES(1,'Zürich',1),(2,'München',2),(3,'Zürich',3);
SELECT city, code FROM t WHERE city = 'Zürich' ORDER BY code;
"

# 51c. Special characters in text
oracle "cat51_special_chars" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'line1' || char(10) || 'line2');
INSERT INTO t VALUES(2,'tab' || char(9) || 'separated');
INSERT INTO t VALUES(3,'normal text');
SELECT id, length(val) FROM t ORDER BY id;
"

# 51d. Very long text in index
oracle "cat51_long_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, substr(hex(randomblob(500)),1,1000));
INSERT INTO t VALUES(2, 'short');
INSERT INTO t VALUES(3, substr(hex(randomblob(500)),1,1000));
SELECT id, length(val) FROM t ORDER BY length(val);
SELECT count(*) FROM t WHERE length(val) > 100;
"

# ════════════════════════════════════════════════════════════════════
# Category 52: ROWID edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 52: ROWID edge cases ---"

# 52a. Explicit rowid access
oracle "cat52_rowid_explicit" "
CREATE TABLE t(a INT, b TEXT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(10,'x'),(20,'y'),(30,'z');
SELECT rowid, a, b FROM t ORDER BY rowid;
SELECT rowid, a FROM t WHERE a > 15 ORDER BY rowid;
"

# 52b. INTEGER PRIMARY KEY is rowid alias
oracle "cat52_ipk_alias" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(100,10),(200,20),(300,30);
SELECT rowid, id, val FROM t ORDER BY id;
SELECT rowid = id FROM t ORDER BY id;
"

# 52c. Negative rowids
oracle "cat52_negative_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(-5,'a'),(-1,'b'),(0,'c'),(1,'d'),(5,'e');
SELECT id, val FROM t ORDER BY id;
SELECT id FROM t WHERE id < 0 ORDER BY id;
"

# 52d. Large rowids
oracle "cat52_large_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1000000,1),(2000000,2),(3000000,3);
UPDATE t SET val = val + 100;
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 53: Complex WHERE clauses
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 53: Complex WHERE clauses ---"

# 53a. Deeply nested AND/OR
oracle "cat53_nested_and_or" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,1,10,100),(2,2,20,200),(3,1,20,300),(4,2,10,400);
SELECT id FROM t WHERE (a = 1 AND b > 15) OR (a = 2 AND c > 300) ORDER BY id;
"

# 53b. NOT with index
oracle "cat53_not" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE NOT (val > 20 AND val < 50) ORDER BY id;
"

# 53c. IS / IS NOT
oracle "cat53_is_isnot" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,NULL),(2,0),(3,1),(4,NULL),(5,0);
SELECT id FROM t WHERE val IS NULL ORDER BY id;
SELECT id FROM t WHERE val IS NOT NULL ORDER BY id;
SELECT id FROM t WHERE val IS 0 ORDER BY id;
"

# 53d. Complex expression in WHERE
oracle "cat53_expr_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,5),(2,20,15),(3,30,25),(4,40,35);
SELECT id FROM t WHERE a - b > 5 AND a * b > 200 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 54: COALESCE / IFNULL / NULLIF
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 54: COALESCE / IFNULL / NULLIF ---"

# 54a. COALESCE in SELECT
oracle "cat54_coalesce" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,NULL,20,30),(2,10,NULL,30),(3,NULL,NULL,30),(4,10,20,NULL);
SELECT id, COALESCE(a, b, c, 0) as first_val FROM t ORDER BY id;
"

# 54b. IFNULL in WHERE with index
oracle "cat54_ifnull_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, flag INT);
CREATE INDEX idx ON t(flag);
INSERT INTO t VALUES(1,NULL,1),(2,10,1),(3,NULL,0),(4,20,1);
SELECT id FROM t WHERE IFNULL(val, 0) > 5 AND flag = 1 ORDER BY id;
"

# 54c. NULLIF
oracle "cat54_nullif" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES(1,10,10),(2,20,30),(3,30,30),(4,40,50);
SELECT id, NULLIF(a, b) FROM t ORDER BY id;
SELECT count(*) FROM t WHERE NULLIF(a, b) IS NULL;
"

# ════════════════════════════════════════════════════════════════════
# Category 55: String functions with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 55: String functions with indexes ---"

# 55a. length() in queries
oracle "cat55_length" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'a'),(2,'bb'),(3,'ccc'),(4,'dddd'),(5,'eeeee');
SELECT id FROM t WHERE length(name) > 3 ORDER BY id;
SELECT id, length(name) FROM t ORDER BY length(name) DESC;
"

# 55b. substr() in queries
oracle "cat55_substr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX idx ON t(code);
INSERT INTO t VALUES(1,'ABC-001'),(2,'DEF-002'),(3,'ABC-003'),(4,'GHI-004');
SELECT id FROM t WHERE substr(code,1,3) = 'ABC' ORDER BY id;
"

# 55c. replace() in UPDATE
oracle "cat55_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, path TEXT);
CREATE INDEX idx ON t(path);
INSERT INTO t VALUES(1,'/old/file1.txt'),(2,'/old/file2.txt'),(3,'/new/file3.txt');
UPDATE t SET path = replace(path, '/old/', '/new/');
SELECT * FROM t ORDER BY id;
SELECT path FROM t WHERE path LIKE '/new/%' ORDER BY path;
"

# 55d. trim() and instr()
oracle "cat55_trim_instr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'  hello  '),(2,'world'),(3,' spaces ');
SELECT id, trim(val), length(trim(val)) FROM t ORDER BY id;
SELECT id FROM t WHERE instr(val, 'o') > 0 ORDER BY id;
"

# 55e. || concatenation with index
oracle "cat55_concat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first TEXT, last TEXT);
CREATE INDEX idx ON t(last);
INSERT INTO t VALUES(1,'John','Doe'),(2,'Jane','Smith'),(3,'Bob','Doe');
SELECT first || ' ' || last as full_name FROM t WHERE last = 'Doe' ORDER BY first;
"

# ════════════════════════════════════════════════════════════════════
# Category 56: Subquery patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 56: Subquery patterns ---"

# 56a. ALL / ANY comparisons
oracle "cat56_all_any" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val > ALL(SELECT val FROM t WHERE id <= 2) ORDER BY id;
"

# 56b. Subquery in HAVING
oracle "cat56_subquery_having" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100);
SELECT grp, sum(val) as s FROM t GROUP BY grp
HAVING sum(val) > (SELECT avg(val) FROM t) ORDER BY grp;
"

# 56c. Subquery in FROM with JOIN
oracle "cat56_subquery_from_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, label TEXT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,'x'),(2,2,'y'),(3,1,'z');
SELECT s.id, s.val, t2.label
FROM (SELECT * FROM t1 WHERE val > 10) s
JOIN t2 ON s.id = t2.t1_id ORDER BY s.id, t2.id;
"

# 56d. Correlated EXISTS with mutation
oracle "cat56_correlated_exists_mutation" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1),(2,3);
DELETE FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.id);
SELECT * FROM t1 ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 57: Index interactions with schema changes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 57: Index interactions with schema changes ---"

# 57a. Create index after data, query, mutate, query
oracle "cat57_late_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
CREATE INDEX idx ON t(a);
SELECT id FROM t WHERE a > 15 ORDER BY id;
UPDATE t SET a = a + 100;
SELECT id FROM t WHERE a > 115 ORDER BY id;
PRAGMA integrity_check;
"

# 57b. Drop index, re-create
oracle "cat57_drop_recreate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
DROP INDEX idx;
UPDATE t SET val = val * 2;
CREATE INDEX idx ON t(val);
SELECT val FROM t ORDER BY val;
PRAGMA integrity_check;
"

# 57c. Multiple indexes created at different times
oracle "cat57_staggered_indexes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
INSERT INTO t VALUES(1,10,100,1000),(2,20,200,2000);
CREATE INDEX idx_a ON t(a);
INSERT INTO t VALUES(3,30,300,3000);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(4,40,400,4000);
CREATE INDEX idx_c ON t(c);
UPDATE t SET a = a+1, b = b+1, c = c+1;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 58: Edge cases with empty/single results
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 58: Edge cases with empty/single results ---"

# 58a. Queries that return nothing
oracle "cat58_empty_results" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT * FROM t WHERE val > 100;
SELECT count(*) FROM t WHERE val > 100;
SELECT max(val) FROM t WHERE val > 100;
SELECT sum(val) FROM t WHERE val > 100;
"

# 58b. DELETE all then INSERT
oracle "cat58_delete_all_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20);
DELETE FROM t;
SELECT count(*) FROM t;
INSERT INTO t VALUES(3,30);
SELECT * FROM t;
"

# 58c. UPDATE that matches no rows
oracle "cat58_update_no_match" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = 999 WHERE val > 100;
SELECT * FROM t ORDER BY id;
"

# 58d. Single row operations
oracle "cat58_single_row" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42);
SELECT * FROM t WHERE val = 42;
UPDATE t SET val = 99;
SELECT * FROM t;
DELETE FROM t;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 59: Implicit type coercion
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 59: Implicit type coercion ---"

# 59a. Integer vs text comparison
oracle "cat59_int_text_cmp" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10),(2, '10'),(3, 10.0);
SELECT id, typeof(val) FROM t ORDER BY id;
SELECT id FROM t WHERE val = 10 ORDER BY id;
SELECT id FROM t WHERE val > 5 ORDER BY id;
"

# 59b. Numeric string ordering
oracle "cat59_numeric_string_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'9'),(2,'10'),(3,'100'),(4,'2'),(5,'20');
SELECT val FROM t ORDER BY val;
SELECT val FROM t ORDER BY CAST(val AS INTEGER);
"

# 59c. Boolean-like comparisons
oracle "cat59_bool" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flag INT, val INT);
CREATE INDEX idx ON t(flag);
INSERT INTO t VALUES(1,0,10),(2,1,20),(3,0,30),(4,1,40),(5,NULL,50);
SELECT id FROM t WHERE flag ORDER BY id;
SELECT id FROM t WHERE NOT flag ORDER BY id;
SELECT id FROM t WHERE flag IS NOT NULL AND NOT flag ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 60: WITHOUT ROWID stress patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 60: WITHOUT ROWID stress patterns ---"

# 60a. WITHOUT ROWID with many non-PK UPDATEs
oracle "cat60_wr_many_updates" "
CREATE TABLE t(k INT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE INDEX idx ON t(v);
INSERT INTO t VALUES(1,100),(2,200),(3,300),(4,400),(5,500);
UPDATE t SET v = v + 1;
UPDATE t SET v = v + 1;
UPDATE t SET v = v + 1;
SELECT * FROM t ORDER BY k;
SELECT v FROM t ORDER BY v;
PRAGMA integrity_check;
"

# 60b. WITHOUT ROWID interleaved INSERT/UPDATE/DELETE
oracle "cat60_wr_interleaved" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x',1,10),('x',2,20),('y',1,30);
UPDATE t SET c = 99 WHERE a = 'x' AND b = 1;
DELETE FROM t WHERE a = 'y';
INSERT INTO t VALUES('z',1,50);
UPDATE t SET c = c + 100 WHERE b = 1;
SELECT * FROM t ORDER BY a, b;
PRAGMA integrity_check;
"

# 60c. WITHOUT ROWID single-col PK UPDATE all
oracle "cat60_wr_single_pk_update_all" "
CREATE TABLE t(k TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITHOUT ROWID;
CREATE INDEX idx1 ON t(v1);
INSERT INTO t VALUES('a',10,'x'),('b',20,'y'),('c',30,'z');
UPDATE t SET v1 = v1 * 10, v2 = v2 || '!';
SELECT * FROM t ORDER BY k;
SELECT k FROM t WHERE v1 > 100 ORDER BY k;
PRAGMA integrity_check;
"

# 60d. WITHOUT ROWID: DELETE with secondary index, re-INSERT
oracle "cat60_wr_delete_reinsert" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES(1,1,100),(1,2,200),(2,1,300);
DELETE FROM t WHERE a = 1;
INSERT INTO t VALUES(1,1,999),(1,3,888);
SELECT * FROM t ORDER BY a, b;
SELECT c FROM t ORDER BY c;
PRAGMA integrity_check;
"

# 60e. WITHOUT ROWID: REPLACE multiple times
oracle "cat60_wr_multi_replace" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('a', 1);
REPLACE INTO t VALUES('a', 2);
REPLACE INTO t VALUES('a', 3);
REPLACE INTO t VALUES('b', 10);
REPLACE INTO t VALUES('b', 20);
SELECT * FROM t ORDER BY k;
SELECT count(*) FROM t;
"

# 60f. WITHOUT ROWID: range scan after mutations
oracle "cat60_wr_range_after_mutation" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1,1,10),(1,2,20),(1,3,30),(2,1,40),(2,2,50);
UPDATE t SET c = c + 1000 WHERE a = 1 AND b >= 2;
DELETE FROM t WHERE a = 2 AND b = 1;
SELECT * FROM t WHERE a = 1 ORDER BY b;
SELECT * FROM t WHERE a >= 1 ORDER BY a, b;
"

# ════════════════════════════════════════════════════════════════════
# Category 61: LIMIT and OFFSET with indexes
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 61: LIMIT and OFFSET with indexes ---"

# 61a. LIMIT on indexed scan
oracle "cat61_limit_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x*10 FROM c;
SELECT val FROM t ORDER BY val LIMIT 5;
SELECT val FROM t ORDER BY val DESC LIMIT 5;
"

# 61b. LIMIT with OFFSET
oracle "cat61_limit_offset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x*10 FROM c;
SELECT val FROM t ORDER BY val LIMIT 5 OFFSET 5;
SELECT val FROM t ORDER BY val LIMIT 3 OFFSET 17;
"

# 61c. LIMIT with WHERE
oracle "cat61_limit_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',40),(5,'b',50),(6,'a',60);
SELECT val FROM t WHERE grp = 'a' ORDER BY val LIMIT 2;
SELECT val FROM t WHERE grp = 'a' ORDER BY val DESC LIMIT 2;
"

# 61d. LIMIT 1 (optimization path)
oracle "cat61_limit_one" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30),(2,10),(3,50),(4,20),(5,40);
SELECT val FROM t ORDER BY val LIMIT 1;
SELECT val FROM t ORDER BY val DESC LIMIT 1;
SELECT val FROM t WHERE val > 25 ORDER BY val LIMIT 1;
"

# 61e. LIMIT after mutation
oracle "cat61_limit_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
DELETE FROM t WHERE val = 30;
UPDATE t SET val = val + 100 WHERE val >= 40;
SELECT val FROM t ORDER BY val LIMIT 3;
"

# ════════════════════════════════════════════════════════════════════
# Category 62: Multi-way JOINs
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 62: Multi-way JOINs ---"

# 62a. Three-table JOIN
oracle "cat62_three_table_join" "
CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE employees(id INTEGER PRIMARY KEY, dept_id INT, name TEXT);
CREATE TABLE salaries(id INTEGER PRIMARY KEY, emp_id INT, amount INT);
CREATE INDEX idx_e ON employees(dept_id);
CREATE INDEX idx_s ON salaries(emp_id);
INSERT INTO departments VALUES(1,'eng'),(2,'sales');
INSERT INTO employees VALUES(1,1,'Alice'),(2,1,'Bob'),(3,2,'Carol');
INSERT INTO salaries VALUES(1,1,100),(2,2,120),(3,3,80);
SELECT d.name, e.name, s.amount
FROM departments d JOIN employees e ON d.id = e.dept_id
JOIN salaries s ON e.id = s.emp_id ORDER BY d.name, e.name;
"

# 62b. LEFT JOIN chain
oracle "cat62_left_join_chain" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, val TEXT);
CREATE TABLE t3(id INTEGER PRIMARY KEY, t2_id INT, val TEXT);
CREATE INDEX idx2 ON t2(t1_id);
CREATE INDEX idx3 ON t3(t2_id);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,1,'x'),(2,1,'y');
INSERT INTO t3 VALUES(1,1,'p');
SELECT t1.val, t2.val, t3.val
FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id
ORDER BY t1.id, t2.id, t3.id;
"

# 62c. Mixed INNER and LEFT JOIN
oracle "cat62_mixed_joins" "
CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE products(id INTEGER PRIMARY KEY, cat_id INT, name TEXT);
CREATE TABLE reviews(id INTEGER PRIMARY KEY, prod_id INT, score INT);
CREATE INDEX idx_p ON products(cat_id);
CREATE INDEX idx_r ON reviews(prod_id);
INSERT INTO categories VALUES(1,'electronics'),(2,'books');
INSERT INTO products VALUES(1,1,'phone'),(2,1,'laptop'),(3,2,'novel');
INSERT INTO reviews VALUES(1,1,5),(2,1,4),(3,3,3);
SELECT c.name, p.name, r.score
FROM categories c JOIN products p ON c.id = p.cat_id
LEFT JOIN reviews r ON p.id = r.prod_id ORDER BY c.name, p.name, r.id;
"

# 62d. Self-join with three levels
oracle "cat62_self_join_three" "
CREATE TABLE nodes(id INTEGER PRIMARY KEY, parent_id INT, name TEXT);
CREATE INDEX idx ON nodes(parent_id);
INSERT INTO nodes VALUES(1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'a1'),(5,2,'a2');
SELECT n1.name as lvl1, n2.name as lvl2, n3.name as lvl3
FROM nodes n1 JOIN nodes n2 ON n1.id = n2.parent_id
JOIN nodes n3 ON n2.id = n3.parent_id
WHERE n1.parent_id IS NULL ORDER BY lvl2, lvl3;
"

# ════════════════════════════════════════════════════════════════════
# Category 63: Multiple UNIQUE constraints
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 63: Multiple UNIQUE constraints ---"

# 63a. Two UNIQUE columns
oracle "cat63_two_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE);
INSERT INTO t VALUES(1,'a@x.com','alice'),(2,'b@x.com','bob');
INSERT OR IGNORE INTO t VALUES(3,'a@x.com','carol');
INSERT OR IGNORE INTO t VALUES(4,'c@x.com','alice');
INSERT INTO t VALUES(5,'c@x.com','carol');
SELECT * FROM t ORDER BY id;
"

# 63b. UNIQUE + regular index
oracle "cat63_unique_plus_regular" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, category INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1,'ABC',1),(2,'DEF',1),(3,'GHI',2);
REPLACE INTO t VALUES(4,'ABC',2);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE category = 1;
SELECT count(*) FROM t WHERE category = 2;
"

# 63c. Multi-column UNIQUE
oracle "cat63_multi_col_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, val TEXT, UNIQUE(a, b));
INSERT INTO t VALUES(1,1,1,'first');
INSERT INTO t VALUES(2,1,2,'second');
INSERT OR REPLACE INTO t VALUES(3,1,1,'replaced');
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# 63d. UNIQUE with NULL columns
oracle "cat63_unique_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b INT);
INSERT INTO t VALUES(1,NULL,10);
INSERT INTO t VALUES(2,NULL,20);
INSERT INTO t VALUES(3,1,30);
INSERT OR IGNORE INTO t VALUES(4,1,40);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 64: CHECK constraints with mutations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 64: CHECK constraints with mutations ---"

# 64a. Basic CHECK constraint
oracle "cat64_check_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT CHECK(val > 0));
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20);
INSERT OR IGNORE INTO t VALUES(3,-5);
INSERT OR IGNORE INTO t VALUES(4,0);
INSERT INTO t VALUES(5,30);
SELECT * FROM t ORDER BY id;
"

# 64b. CHECK on UPDATE
oracle "cat64_check_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT CHECK(val BETWEEN 0 AND 100));
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,50),(2,75);
UPDATE OR IGNORE t SET val = 150 WHERE id = 1;
UPDATE t SET val = 90 WHERE id = 1;
SELECT * FROM t ORDER BY id;
"

# 64c. Multi-column CHECK
oracle "cat64_check_multi" "
CREATE TABLE t(id INTEGER PRIMARY KEY, low INT, high INT, CHECK(low <= high));
INSERT INTO t VALUES(1,10,20),(2,5,15);
INSERT OR IGNORE INTO t VALUES(3,30,10);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 65: DEFAULT values
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 65: DEFAULT values ---"

# 65a. INSERT with DEFAULT
oracle "cat65_default_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT DEFAULT 42, status TEXT DEFAULT 'new');
CREATE INDEX idx ON t(val);
INSERT INTO t(id) VALUES(1),(2),(3);
INSERT INTO t VALUES(4, 99, 'custom');
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE val = 42;
"

# 65b. DEFAULT current_timestamp equivalent
oracle "cat65_default_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, created TEXT DEFAULT (date('now')));
INSERT INTO t(id, val) VALUES(1, 10),(2, 20);
SELECT id, val, typeof(created), length(created) FROM t ORDER BY id;
"

# 65c. INSERT DEFAULT VALUES
oracle "cat65_default_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT DEFAULT 1, b TEXT DEFAULT 'x');
INSERT INTO t DEFAULT VALUES;
INSERT INTO t DEFAULT VALUES;
INSERT INTO t(id, a) VALUES(10, 99);
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 66: INDEXED BY and NOT INDEXED
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 66: INDEXED BY and NOT INDEXED ---"

# 66a. INDEXED BY forces index use
oracle "cat66_indexed_by" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300),(4,10,400);
SELECT id FROM t INDEXED BY idx_a WHERE a = 10 ORDER BY id;
"

# 66b. NOT INDEXED
oracle "cat66_not_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT id FROM t NOT INDEXED WHERE val > 15 ORDER BY id;
"

# 66c. INDEXED BY after mutation
oracle "cat66_indexed_by_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val + 100;
SELECT val FROM t INDEXED BY idx WHERE val > 110 ORDER BY val;
"

# ════════════════════════════════════════════════════════════════════
# Category 67: WITHOUT ROWID with 3+ PK columns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 67: WITHOUT ROWID with 3+ PK columns ---"

# 67a. Three-column PK
oracle "cat67_wr_three_pk" "
CREATE TABLE t(a TEXT, b INT, c INT, d TEXT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,1,'v1'),('x',1,2,'v2'),('x',2,1,'v3');
INSERT INTO t VALUES('y',1,1,'v4');
SELECT * FROM t ORDER BY a, b, c;
UPDATE t SET d = 'updated' WHERE a = 'x' AND b = 1 AND c = 2;
SELECT * FROM t WHERE a = 'x' ORDER BY b, c;
"

# 67b. Three-column PK with secondary index
oracle "cat67_wr_three_pk_secidx" "
CREATE TABLE t(a INT, b INT, c INT, val TEXT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,1,1,'hello'),(1,1,2,'world'),(1,2,1,'foo');
UPDATE t SET val = 'bar' WHERE a = 1 AND b = 1 AND c = 1;
SELECT * FROM t ORDER BY a, b, c;
SELECT a, b, c FROM t WHERE val = 'bar';
PRAGMA integrity_check;
"

# 67c. Three-column PK multi-row UPDATE
oracle "cat67_wr_three_pk_multi_update" "
CREATE TABLE t(a TEXT, b INT, c INT, d INT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,1,10),('x',1,2,20),('x',2,1,30),('x',2,2,40);
UPDATE t SET d = d + 100 WHERE a = 'x' AND b = 1;
SELECT * FROM t ORDER BY a, b, c;
"

# 67d. Three-column PK DELETE
oracle "cat67_wr_three_pk_delete" "
CREATE TABLE t(a INT, b INT, c INT, d INT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
CREATE INDEX idx ON t(d);
INSERT INTO t VALUES(1,1,1,10),(1,1,2,20),(1,2,1,30),(2,1,1,40);
DELETE FROM t WHERE a = 1 AND b = 1;
SELECT * FROM t ORDER BY a, b, c;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 68: Complex ORDER BY
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 68: Complex ORDER BY ---"

# 68a. ORDER BY expression
oracle "cat68_order_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,3),(2,20,1),(3,10,2),(4,30,4);
SELECT id, a, b FROM t ORDER BY a, b DESC;
"

# 68b. ORDER BY with NULLS
oracle "cat68_order_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30),(2,NULL),(3,10),(4,NULL),(5,20);
SELECT id, val FROM t ORDER BY val;
SELECT id, val FROM t ORDER BY val DESC;
"

# 68c. ORDER BY column not in SELECT
oracle "cat68_order_hidden_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INT);
CREATE INDEX idx ON t(score);
INSERT INTO t VALUES(1,'a',30),(2,'b',10),(3,'c',50),(4,'d',20);
SELECT name FROM t ORDER BY score;
SELECT name FROM t ORDER BY score DESC;
"

# 68d. ORDER BY with CASE
oracle "cat68_order_case" "
CREATE TABLE t(id INTEGER PRIMARY KEY, priority TEXT, val INT);
INSERT INTO t VALUES(1,'low',10),(2,'high',20),(3,'medium',30),(4,'high',40);
SELECT id, priority, val FROM t
ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, val DESC;
"

# ════════════════════════════════════════════════════════════════════
# Category 69: Deferred FK constraints
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 69: Deferred FK constraints ---"

# 69a. DEFERRABLE INITIALLY DEFERRED
oracle "cat69_deferred_fk" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
PRAGMA foreign_keys = ON;
BEGIN;
INSERT INTO child VALUES(1, 99);
INSERT INTO parent VALUES(99);
COMMIT;
SELECT * FROM child ORDER BY id;
SELECT * FROM parent ORDER BY id;
"

# 69b. Deferred FK violation at commit
oracle "cat69_deferred_fk_violation" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1);
BEGIN;
INSERT INTO child VALUES(1, 999);
ROLLBACK;
SELECT count(*) FROM child;
SELECT count(*) FROM parent;
"

# ════════════════════════════════════════════════════════════════════
# Category 70: REPLACE with triggers
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 70: REPLACE with triggers ---"

# 70a. REPLACE fires DELETE + INSERT triggers
oracle "cat70_replace_triggers" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE log(action TEXT, tid INT, tval INT);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg_del BEFORE DELETE ON t BEGIN
  INSERT INTO log VALUES('delete', OLD.id, OLD.val);
END;
CREATE TRIGGER trg_ins AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES('insert', NEW.id, NEW.val);
END;
INSERT INTO t VALUES(1, 10);
REPLACE INTO t VALUES(1, 20);
SELECT * FROM t ORDER BY id;
SELECT * FROM log ORDER BY rowid;
"

# 70b. REPLACE with UNIQUE trigger
oracle "cat70_replace_unique_trigger" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, val INT);
CREATE TABLE audit(msg TEXT);
CREATE TRIGGER trg AFTER DELETE ON t BEGIN
  INSERT INTO audit VALUES('deleted ' || OLD.code);
END;
INSERT INTO t VALUES(1,'A',10),(2,'B',20);
REPLACE INTO t VALUES(3,'A',30);
SELECT * FROM t ORDER BY id;
SELECT * FROM audit;
"

# ════════════════════════════════════════════════════════════════════
# Category 71: Complex mutation + SELECT interleaving
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 71: Complex mutation + SELECT interleaving ---"

# 71a. Alternating INSERT and SELECT
oracle "cat71_alternating_insert_select" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10);
SELECT sum(val) FROM t;
INSERT INTO t VALUES(2,20);
SELECT sum(val) FROM t;
INSERT INTO t VALUES(3,30);
SELECT sum(val) FROM t;
SELECT count(*) FROM t;
"

# 71b. UPDATE then immediately verify via index
oracle "cat71_update_verify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
UPDATE t SET a = 99 WHERE id = 2;
SELECT id FROM t WHERE a = 99;
SELECT id FROM t WHERE a = 20;
UPDATE t SET b = 999 WHERE a = 99;
SELECT id, b FROM t WHERE b = 999;
"

# 71c. DELETE then verify absence
oracle "cat71_delete_verify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
DELETE FROM t WHERE val = 30;
SELECT count(*) FROM t WHERE val = 30;
SELECT val FROM t ORDER BY val;
DELETE FROM t WHERE val IN (10, 50);
SELECT val FROM t ORDER BY val;
"

# 71d. Mutation + aggregate + mutation
oracle "cat71_mutation_agg_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val * 2 WHERE val > 15;
SELECT sum(val), min(val), max(val) FROM t;
DELETE FROM t WHERE val > 50;
SELECT count(*), sum(val) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 72: UPSERT advanced patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 72: UPSERT advanced patterns ---"

# 72a. UPSERT incrementing counter
oracle "cat72_upsert_counter" "
CREATE TABLE counts(key TEXT PRIMARY KEY, n INT DEFAULT 0);
INSERT INTO counts VALUES('a', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
INSERT INTO counts VALUES('a', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
INSERT INTO counts VALUES('a', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
INSERT INTO counts VALUES('b', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
SELECT * FROM counts ORDER BY key;
"

# 72b. UPSERT with excluded reference
oracle "cat72_upsert_excluded" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, updated INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 0),(2, 20, 0);
INSERT INTO t VALUES(1, 99, 0) ON CONFLICT(id) DO UPDATE SET val = excluded.val, updated = 1;
INSERT INTO t VALUES(3, 30, 0) ON CONFLICT(id) DO UPDATE SET val = excluded.val, updated = 1;
SELECT * FROM t ORDER BY id;
"

# 72c. UPSERT on WITHOUT ROWID
oracle "cat72_upsert_wr" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x',1,100);
INSERT INTO t VALUES('x',1,200) ON CONFLICT(a,b) DO UPDATE SET c = excluded.c;
INSERT INTO t VALUES('x',2,300) ON CONFLICT(a,b) DO UPDATE SET c = excluded.c;
SELECT * FROM t ORDER BY a, b;
SELECT c FROM t ORDER BY c;
"

# 72d. UPSERT DO NOTHING
oracle "cat72_upsert_do_nothing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, label TEXT);
INSERT INTO t VALUES(1, 10, 'first');
INSERT INTO t VALUES(2, 10, 'second') ON CONFLICT DO NOTHING;
INSERT INTO t VALUES(1, 20, 'third') ON CONFLICT DO NOTHING;
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 73: typeof() and type inspection
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 73: typeof() and type inspection ---"

# 73a. typeof() on various values
oracle "cat73_typeof" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42),(2, 3.14),(3, 'hello'),(4, X'BEEF'),(5, NULL);
SELECT id, typeof(val), val FROM t ORDER BY id;
"

# 73b. typeof() after UPDATE
oracle "cat73_typeof_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10),(2, 'text'),(3, NULL);
UPDATE t SET val = 'now_text' WHERE id = 1;
UPDATE t SET val = 42 WHERE id = 2;
UPDATE t SET val = 3.14 WHERE id = 3;
SELECT id, typeof(val), val FROM t ORDER BY id;
"

# 73c. typeof in WHERE
oracle "cat73_typeof_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
INSERT INTO t VALUES(1,42),(2,'hello'),(3,3.14),(4,NULL),(5,X'FF');
SELECT id FROM t WHERE typeof(val) = 'integer' ORDER BY id;
SELECT id FROM t WHERE typeof(val) = 'text' ORDER BY id;
SELECT id FROM t WHERE typeof(val) = 'null' ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 74: hex/unhex and blob operations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 74: hex/unhex and blob operations ---"

# 74a. hex() on indexed blobs
oracle "cat74_hex" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, X'DEADBEEF'),(2, X'00FF00'),(3, X'CAFE');
SELECT id, hex(data) FROM t ORDER BY data;
"

# 74b. Blob operations
oracle "cat74_blob_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, X'0102030405');
SELECT id, length(data), hex(substr(data, 2, 3)) FROM t;
UPDATE t SET data = X'0A0B0C';
SELECT id, hex(data) FROM t;
"

# 74c. randomblob in indexed column
oracle "cat74_randomblob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, randomblob(8));
INSERT INTO t VALUES(2, randomblob(8));
SELECT count(*) FROM t;
SELECT count(DISTINCT data) FROM t;
SELECT length(data) FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 75: Aggregate FILTER clause
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 75: Aggregate FILTER clause ---"

# 75a. COUNT with FILTER
oracle "cat75_count_filter" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'a',50);
SELECT count(*) FILTER (WHERE grp = 'a') as cnt_a,
       count(*) FILTER (WHERE grp = 'b') as cnt_b FROM t;
"

# 75b. SUM with FILTER
oracle "cat75_sum_filter" "
CREATE TABLE t(id INTEGER PRIMARY KEY, type TEXT, amount INT);
CREATE INDEX idx ON t(type);
INSERT INTO t VALUES(1,'credit',100),(2,'debit',50),(3,'credit',200),(4,'debit',75);
SELECT sum(amount) FILTER (WHERE type='credit') as credits,
       sum(amount) FILTER (WHERE type='debit') as debits FROM t;
"

# 75c. FILTER with GROUP BY
oracle "cat75_filter_group" "
CREATE TABLE t(id INTEGER PRIMARY KEY, dept TEXT, status TEXT, val INT);
CREATE INDEX idx ON t(dept);
INSERT INTO t VALUES(1,'a','active',10),(2,'a','inactive',20),(3,'b','active',30);
INSERT INTO t VALUES(4,'b','active',40),(5,'a','active',50);
SELECT dept,
  count(*) FILTER (WHERE status='active') as active_cnt,
  sum(val) FILTER (WHERE status='active') as active_sum
FROM t GROUP BY dept ORDER BY dept;
"

# ════════════════════════════════════════════════════════════════════
# Category 76: VALUES as table constructor
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 76: VALUES as table constructor ---"

# 76a. VALUES in FROM
oracle "cat76_values_from" "
SELECT * FROM (VALUES(1,'a'),(2,'b'),(3,'c')) ORDER BY column1;
"

# 76b. VALUES in INSERT
oracle "cat76_values_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'z');
SELECT * FROM t ORDER BY id;
"

# 76c. VALUES in IN clause
oracle "cat76_values_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40);
SELECT id FROM t WHERE val IN (VALUES(10),(30)) ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 77: Aliased tables and columns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 77: Aliased tables and columns ---"

# 77a. Table alias in complex query
oracle "cat77_table_alias" "
CREATE TABLE very_long_table_name(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON very_long_table_name(val);
INSERT INTO very_long_table_name VALUES(1,10),(2,20),(3,30);
SELECT t.id, t.val FROM very_long_table_name t WHERE t.val > 15 ORDER BY t.id;
"

# 77b. Column alias in subquery
oracle "cat77_col_alias" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT doubled FROM (SELECT val * 2 as doubled FROM t) sub WHERE doubled > 30 ORDER BY doubled;
"

# 77c. Alias in GROUP BY / HAVING
oracle "cat77_alias_group" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',5);
SELECT grp, sum(val) as total FROM t GROUP BY grp HAVING total > 20 ORDER BY total;
"

# ════════════════════════════════════════════════════════════════════
# Category 78: Large batch operations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 78: Large batch operations ---"

# 78a. Large INSERT + DELETE half
oracle "cat78_large_delete_half" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, x FROM c;
DELETE FROM t WHERE id % 2 = 0;
SELECT count(*) FROM t;
SELECT min(val), max(val) FROM t;
PRAGMA integrity_check;
"

# 78b. Large UPDATE even/odd
oracle "cat78_large_update_split" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, grp TEXT);
CREATE INDEX idx ON t(grp);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, x, CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END FROM c;
UPDATE t SET val = val + 10000 WHERE grp = 'even';
SELECT count(*) FROM t WHERE val > 10000;
SELECT count(*) FROM t WHERE val < 10000;
SELECT grp, min(val), max(val) FROM t GROUP BY grp ORDER BY grp;
"

# 78c. Large batch with multiple indexes
oracle "cat78_large_multi_idx" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<300)
INSERT INTO t SELECT x, x % 10, x % 7, x * 3 FROM r;
UPDATE t SET a = a + 100 WHERE b < 3;
DELETE FROM t WHERE c % 9 = 0;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE a > 100;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 79: Savepoint + WITHOUT ROWID combinations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 79: Savepoint + WITHOUT ROWID ---"

# 79a. Savepoint with WITHOUT ROWID UPDATE
oracle "cat79_sp_wr_update" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE INDEX idx ON t(v);
INSERT INTO t VALUES('a',10),('b',20),('c',30);
SAVEPOINT sp;
UPDATE t SET v = v + 100;
SELECT * FROM t ORDER BY k;
ROLLBACK TO sp;
SELECT * FROM t ORDER BY k;
"

# 79b. Nested savepoints with WITHOUT ROWID
oracle "cat79_nested_sp_wr" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES(1,1,100),(1,2,200),(2,1,300);
SAVEPOINT s1;
UPDATE t SET c = 999 WHERE a = 1;
SAVEPOINT s2;
DELETE FROM t WHERE a = 2;
INSERT INTO t VALUES(3,1,400);
ROLLBACK TO s2;
SELECT * FROM t ORDER BY a, b;
RELEASE s1;
SELECT * FROM t ORDER BY a, b;
PRAGMA integrity_check;
"

# 79c. Savepoint release after mutation
oracle "cat79_sp_release" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SAVEPOINT sp;
UPDATE t SET v = 'x' WHERE k = 2;
DELETE FROM t WHERE k = 3;
INSERT INTO t VALUES(4,'d');
RELEASE sp;
SELECT * FROM t ORDER BY k;
"

# ════════════════════════════════════════════════════════════════════
# Category 80: Integrity checks after complex operations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 80: Integrity checks after complex operations ---"

# 80a. Integrity after interleaved ops on multiple indexes
oracle "cat80_integrity_multi_idx" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT, c REAL);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<100)
INSERT INTO t SELECT x, x%10, 'val_'||(x%5), x*1.1 FROM r;
UPDATE t SET a = a + 50 WHERE id <= 30;
DELETE FROM t WHERE b = 'val_0';
UPDATE t SET c = c * 2 WHERE a < 10;
INSERT INTO t VALUES(200, 99, 'new', 999.9);
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 80b. Integrity after WITHOUT ROWID stress
oracle "cat80_integrity_wr_stress" "
CREATE TABLE t(a TEXT, b INT, c INT, d TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
CREATE INDEX idx_d ON t(d);
INSERT INTO t VALUES('x',1,10,'p'),('x',2,20,'q'),('x',3,30,'r');
INSERT INTO t VALUES('y',1,40,'s'),('y',2,50,'t');
UPDATE t SET c = c + 1000, d = d || '!' WHERE a = 'x';
DELETE FROM t WHERE b = 2;
INSERT INTO t VALUES('z',1,60,'u'),('z',2,70,'v');
UPDATE t SET c = c + 1 WHERE a = 'z';
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

# 80c. Integrity after savepoint rollback with indexes
oracle "cat80_integrity_sp_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
SAVEPOINT sp;
UPDATE t SET a = 99, b = 99;
DELETE FROM t WHERE id = 2;
INSERT INTO t VALUES(4,40,400);
ROLLBACK TO sp;
SELECT count(*) FROM t;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# 80d. Integrity after REPLACE chain
oracle "cat80_integrity_replace_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, label TEXT);
CREATE INDEX idx ON t(label);
INSERT INTO t VALUES(1,10,'a'),(2,20,'b'),(3,30,'c');
REPLACE INTO t VALUES(4,10,'d');
REPLACE INTO t VALUES(5,20,'e');
REPLACE INTO t VALUES(6,30,'f');
SELECT count(*) FROM t;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 81: Window function edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 81: Window function edge cases ---"

# 81a. FIRST_VALUE / LAST_VALUE
oracle "cat81_first_last_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',30),(3,'a',20),(4,'b',40),(5,'b',50);
SELECT id, grp, val,
  first_value(val) OVER (PARTITION BY grp ORDER BY val) as fv,
  last_value(val) OVER (PARTITION BY grp ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lv
FROM t ORDER BY grp, val;
"

# 81b. NTH_VALUE
oracle "cat81_nth_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id, val,
  nth_value(val, 2) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second,
  nth_value(val, 4) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fourth
FROM t ORDER BY id;
"

# 81c. Window with empty partition
oracle "cat81_empty_partition" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20);
SELECT grp, val, sum(val) OVER (PARTITION BY grp) as s,
  count(*) OVER (PARTITION BY grp) as c FROM t ORDER BY grp, val;
"

# 81d. Multiple windows in one query
oracle "cat81_multi_window" "
CREATE TABLE t(id INTEGER PRIMARY KEY, dept TEXT, val INT);
CREATE INDEX idx ON t(dept);
INSERT INTO t VALUES(1,'a',10),(2,'a',30),(3,'b',20),(4,'b',40),(5,'a',20);
SELECT id, dept, val,
  row_number() OVER w1 as rn,
  sum(val) OVER w2 as dept_total
FROM t
WINDOW w1 AS (ORDER BY id), w2 AS (PARTITION BY dept)
ORDER BY id;
"

# 81e. RANGE vs ROWS frame
oracle "cat81_range_vs_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,10),(3,20),(4,20),(5,30);
SELECT id, val,
  count(*) OVER (ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as range_cnt,
  count(*) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rows_cnt
FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 82: Recursive CTE patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 82: Recursive CTE patterns ---"

# 82a. Fibonacci sequence
oracle "cat82_fibonacci" "
WITH RECURSIVE fib(n, a, b) AS (
  VALUES(1, 0, 1)
  UNION ALL
  SELECT n+1, b, a+b FROM fib WHERE n < 10
)
SELECT n, a FROM fib ORDER BY n;
"

# 82b. Path finding in graph
oracle "cat82_graph_path" "
CREATE TABLE edges(src INT, dst INT, weight INT);
CREATE INDEX idx ON edges(src);
INSERT INTO edges VALUES(1,2,10),(2,3,20),(3,4,30),(1,3,50),(2,4,15);
WITH RECURSIVE paths(node, total, path) AS (
  SELECT 1, 0, '1'
  UNION ALL
  SELECT e.dst, p.total + e.weight, p.path || '->' || e.dst
  FROM paths p JOIN edges e ON p.node = e.src
  WHERE p.total + e.weight < 100
)
SELECT node, total, path FROM paths WHERE node = 4 ORDER BY total;
"

# 82c. Generate series equivalent
oracle "cat82_generate_series" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE s(x) AS (VALUES(0) UNION ALL SELECT x+10 FROM s WHERE x < 50)
INSERT INTO t SELECT x+1, x FROM s;
SELECT * FROM t ORDER BY id;
"

# 82d. Recursive aggregation
oracle "cat82_recursive_agg" "
CREATE TABLE tree(id INTEGER PRIMARY KEY, parent_id INT, val INT);
CREATE INDEX idx ON tree(parent_id);
INSERT INTO tree VALUES(1,NULL,10),(2,1,20),(3,1,30),(4,2,40),(5,2,50);
WITH RECURSIVE subtree(id, val, depth) AS (
  SELECT id, val, 0 FROM tree WHERE id = 1
  UNION ALL
  SELECT t.id, t.val, s.depth+1 FROM tree t JOIN subtree s ON t.parent_id = s.id
)
SELECT sum(val), count(*), max(depth) FROM subtree;
"

# ════════════════════════════════════════════════════════════════════
# Category 83: Complex aggregate patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 83: Complex aggregate patterns ---"

# 83a. Nested aggregates via subquery
oracle "cat83_nested_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);
SELECT avg(grp_sum) FROM (SELECT grp, sum(val) as grp_sum FROM t GROUP BY grp);
"

# 83b. GROUP BY multiple columns
oracle "cat83_group_multi" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, val INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,'x',1,10),(2,'x',1,20),(3,'x',2,30),(4,'y',1,40),(5,'y',2,50);
SELECT a, b, count(*), sum(val) FROM t GROUP BY a, b ORDER BY a, b;
"

# 83c. Aggregate with DISTINCT
oracle "cat83_agg_distinct" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',10),(3,'a',20),(4,'b',30),(5,'b',30);
SELECT grp, count(DISTINCT val), sum(DISTINCT val) FROM t GROUP BY grp ORDER BY grp;
"

# 83d. GROUP BY with expression
oracle "cat83_group_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, ts TEXT, val INT);
INSERT INTO t VALUES(1,'2026-01-15',10),(2,'2026-01-20',20),(3,'2026-02-10',30),(4,'2026-02-25',40);
SELECT substr(ts,1,7) as month, sum(val) FROM t GROUP BY month ORDER BY month;
"

# ════════════════════════════════════════════════════════════════════
# Category 84: NATURAL JOIN and USING
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 84: NATURAL JOIN and USING ---"

# 84a. NATURAL JOIN
oracle "cat84_natural_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT, val INT);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,'a',10),(2,'x',20),(3,'c',30);
SELECT * FROM t1 NATURAL JOIN t2 ORDER BY id;
"

# 84b. JOIN USING
oracle "cat84_join_using" "
CREATE TABLE t1(id INT, grp TEXT, val1 INT);
CREATE TABLE t2(id INT, grp TEXT, val2 INT);
CREATE INDEX idx1 ON t1(grp);
CREATE INDEX idx2 ON t2(grp);
INSERT INTO t1 VALUES(1,'a',10),(2,'b',20);
INSERT INTO t2 VALUES(1,'a',100),(2,'b',200),(3,'c',300);
SELECT * FROM t1 JOIN t2 USING(grp) ORDER BY t1.id, t2.id;
"

# 84c. LEFT JOIN USING with NULLs
oracle "cat84_left_using" "
CREATE TABLE t1(grp TEXT, val INT);
CREATE TABLE t2(grp TEXT, data TEXT);
INSERT INTO t1 VALUES('a',10),('b',20),('c',30);
INSERT INTO t2 VALUES('a','x'),('c','z');
SELECT t1.grp, t1.val, t2.data FROM t1 LEFT JOIN t2 USING(grp) ORDER BY t1.grp;
"

# ════════════════════════════════════════════════════════════════════
# Category 85: CROSS JOIN patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 85: CROSS JOIN patterns ---"

# 85a. Basic CROSS JOIN
oracle "cat85_cross_basic" "
CREATE TABLE t1(a TEXT);
CREATE TABLE t2(b INT);
INSERT INTO t1 VALUES('x'),('y');
INSERT INTO t2 VALUES(1),(2),(3);
SELECT a, b FROM t1 CROSS JOIN t2 ORDER BY a, b;
"

# 85b. CROSS JOIN with WHERE (becomes INNER)
oracle "cat85_cross_where" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20);
INSERT INTO t2 VALUES(1,1),(2,2),(3,1);
SELECT t1.id, t2.id FROM t1, t2 WHERE t1.id = t2.ref ORDER BY t1.id, t2.id;
"

# ════════════════════════════════════════════════════════════════════
# Category 86: Compound SELECT with mutations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 86: Compound SELECT with mutations ---"

# 86a. UNION after complex mutations
oracle "cat86_union_after_mutations" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,30),(3,40);
DELETE FROM t1 WHERE val = 10;
UPDATE t2 SET val = val + 5;
SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val;
"

# 86b. EXCEPT with indexed mutation
oracle "cat86_except_mutation" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30),(4,40);
INSERT INTO t2 VALUES(1,20),(2,40);
UPDATE t1 SET val = val + 5 WHERE val > 20;
SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val;
"

# 86c. UNION ALL with ORDER BY and LIMIT
oracle "cat86_union_all_limit" "
CREATE TABLE t1(val INT);
CREATE TABLE t2(val INT);
INSERT INTO t1 VALUES(30),(10),(50);
INSERT INTO t2 VALUES(20),(40),(60);
SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val LIMIT 4;
"

# ════════════════════════════════════════════════════════════════════
# Category 87: DELETE with complex conditions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 87: DELETE with complex conditions ---"

# 87a. DELETE with correlated subquery
oracle "cat87_delete_correlated" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE TABLE t2(grp TEXT, min_val INT);
CREATE INDEX idx ON t1(grp);
INSERT INTO t1 VALUES(1,'a',5),(2,'a',15),(3,'b',25),(4,'b',35);
INSERT INTO t2 VALUES('a',10),('b',30);
DELETE FROM t1 WHERE val < (SELECT min_val FROM t2 WHERE t2.grp = t1.grp);
SELECT * FROM t1 ORDER BY id;
"

# 87b. DELETE all rows then verify empty
oracle "cat87_delete_all_verify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,'x'),(2,20,'y'),(3,30,'z');
DELETE FROM t;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE a > 0;
INSERT INTO t VALUES(4,40,'w');
SELECT * FROM t;
PRAGMA integrity_check;
"

# 87c. DELETE with complex boolean
oracle "cat87_delete_bool" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,1,10,'x'),(2,1,20,'y'),(3,2,10,'z'),(4,2,20,'w'),(5,3,30,'v');
DELETE FROM t WHERE (a = 1 AND b > 15) OR (a = 2 AND c = 'z');
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 88: INSERT edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 88: INSERT edge cases ---"

# 88a. INSERT with explicit NULL
oracle "cat88_insert_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1, NULL, NULL);
INSERT INTO t VALUES(2, 10, NULL);
INSERT INTO t VALUES(3, NULL, 'hello');
SELECT id, a, b FROM t ORDER BY id;
SELECT count(*) FROM t WHERE a IS NULL;
"

# 88b. INSERT with SELECT expression
oracle "cat88_insert_select_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, (SELECT 10 + 20));
INSERT INTO t VALUES(2, (SELECT max(val) + 1 FROM t));
INSERT INTO t VALUES(3, (SELECT count(*) FROM t));
SELECT * FROM t ORDER BY id;
"

# 88c. INSERT many rows in one statement
oracle "cat88_insert_many" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100);
SELECT count(*) FROM t;
SELECT sum(val) FROM t;
SELECT min(val), max(val) FROM t;
"

# 88d. INSERT OR ABORT
oracle "cat88_insert_abort" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
INSERT INTO t VALUES(1, 10),(2, 20);
INSERT OR ABORT INTO t VALUES(3, 10);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 89: UPDATE with complex SET expressions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 89: UPDATE with complex SET expressions ---"

# 89a. UPDATE SET with subquery
oracle "cat89_set_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE lookup(id INT, bonus INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
INSERT INTO lookup VALUES(1,100),(3,300);
UPDATE t SET val = val + COALESCE((SELECT bonus FROM lookup WHERE lookup.id = t.id), 0);
SELECT * FROM t ORDER BY id;
"

# 89b. UPDATE SET swapping values
oracle "cat89_set_swap" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200);
UPDATE t SET a = b, b = a;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

# 89c. UPDATE SET with CASE
oracle "cat89_set_case" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, tier TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,''),(2,50,''),(3,90,'');
UPDATE t SET tier = CASE
  WHEN val >= 80 THEN 'gold'
  WHEN val >= 40 THEN 'silver'
  ELSE 'bronze'
END;
SELECT * FROM t ORDER BY id;
"

# 89d. UPDATE with arithmetic chain
oracle "cat89_set_arithmetic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,20,30),(2,40,50,60);
UPDATE t SET a = a + b + c, b = b * 2, c = a;
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 90: Index + trigger interactions
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 90: Index + trigger interactions ---"

# 90a. Trigger maintains denormalized column
oracle "cat90_trigger_denorm" "
CREATE TABLE items(id INTEGER PRIMARY KEY, price INT, qty INT, total INT DEFAULT 0);
CREATE INDEX idx ON items(total);
CREATE TRIGGER calc_total AFTER INSERT ON items BEGIN
  UPDATE items SET total = price * qty WHERE id = NEW.id;
END;
CREATE TRIGGER recalc_total AFTER UPDATE OF price, qty ON items BEGIN
  UPDATE items SET total = NEW.price * NEW.qty WHERE id = NEW.id;
END;
INSERT INTO items(id,price,qty) VALUES(1,10,5),(2,20,3);
SELECT * FROM items ORDER BY id;
UPDATE items SET qty = 10 WHERE id = 1;
SELECT * FROM items ORDER BY total;
"

# 90b. Trigger cascades across indexed tables
oracle "cat90_trigger_cascade_idx" "
CREATE TABLE orders(id INTEGER PRIMARY KEY, status TEXT, total INT);
CREATE TABLE order_log(order_id INT, old_status TEXT, new_status TEXT);
CREATE INDEX idx_status ON orders(status);
CREATE INDEX idx_log ON order_log(order_id);
CREATE TRIGGER log_status AFTER UPDATE OF status ON orders BEGIN
  INSERT INTO order_log VALUES(NEW.id, OLD.status, NEW.status);
END;
INSERT INTO orders VALUES(1,'new',100),(2,'new',200);
UPDATE orders SET status = 'shipped' WHERE total > 150;
UPDATE orders SET status = 'delivered' WHERE status = 'shipped';
SELECT * FROM orders ORDER BY id;
SELECT * FROM order_log ORDER BY rowid;
"

# 90c. BEFORE trigger modifies NEW values
oracle "cat90_before_modify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, validated INT DEFAULT 0);
CREATE INDEX idx ON t(val);
CREATE TRIGGER validate BEFORE INSERT ON t BEGIN
  SELECT RAISE(IGNORE) WHERE NEW.val < 0;
END;
INSERT INTO t VALUES(1, 10, 1);
INSERT INTO t VALUES(2, -5, 1);
INSERT INTO t VALUES(3, 20, 1);
SELECT * FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 91: Covering index edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 91: Covering index edge cases ---"

# 91a. Covering index with all query columns
oracle "cat91_full_cover" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx ON t(a, b, c);
INSERT INTO t VALUES(1,10,100,'x'),(2,20,200,'y'),(3,10,300,'z');
SELECT a, b, c FROM t WHERE a = 10 ORDER BY b;
SELECT a, b FROM t WHERE a > 10 AND b > 100 ORDER BY a;
"

# 91b. Covering index vs table scan after mutation
oracle "cat91_cover_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, key INT, data TEXT);
CREATE INDEX idx ON t(key, data);
INSERT INTO t VALUES(1,10,'old'),(2,20,'old'),(3,30,'old');
UPDATE t SET data = 'new' WHERE key > 15;
SELECT key, data FROM t ORDER BY key;
"

# 91c. COUNT(*) with covering index
oracle "cat91_count_cover" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50);
SELECT a, count(*) FROM t GROUP BY a ORDER BY a;
SELECT count(*) FROM t WHERE a = 1;
"

# ════════════════════════════════════════════════════════════════════
# Category 92: Mixed WITHOUT ROWID and regular table operations
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 92: Mixed WR and regular table ops ---"

# 92a. JOIN between WITHOUT ROWID and regular table
oracle "cat92_wr_join_regular" "
CREATE TABLE regular(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE wr(k TEXT PRIMARY KEY, ref INT, data TEXT) WITHOUT ROWID;
CREATE INDEX idx_r ON regular(val);
CREATE INDEX idx_w ON wr(ref);
INSERT INTO regular VALUES(1,10),(2,20),(3,30);
INSERT INTO wr VALUES('a',1,'x'),('b',2,'y'),('c',9,'z');
SELECT r.id, r.val, w.k, w.data
FROM regular r JOIN wr w ON r.id = w.ref ORDER BY r.id;
"

# 92b. INSERT INTO regular FROM WITHOUT ROWID
oracle "cat92_insert_wr_to_regular" "
CREATE TABLE wr(a TEXT, b INT, c INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE TABLE regular(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON regular(val);
INSERT INTO wr VALUES('x',1,100),('x',2,200),('y',1,300);
INSERT INTO regular SELECT NULL, c FROM wr WHERE a = 'x';
SELECT * FROM regular ORDER BY val;
"

# 92c. UPDATE regular with data from WITHOUT ROWID
oracle "cat92_update_from_wr" "
CREATE TABLE config(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE TABLE data(id INTEGER PRIMARY KEY, val INT, multiplied INT DEFAULT 0);
CREATE INDEX idx ON data(val);
INSERT INTO config VALUES('factor',10);
INSERT INTO data VALUES(1,5,0),(2,10,0),(3,15,0);
UPDATE data SET multiplied = val * (SELECT v FROM config WHERE k = 'factor');
SELECT * FROM data ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 93: Edge case data patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 93: Edge case data patterns ---"

# 93a. All same values in indexed column
oracle "cat93_all_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,42),(2,42),(3,42),(4,42),(5,42);
SELECT count(*) FROM t WHERE val = 42;
SELECT min(id), max(id) FROM t WHERE val = 42;
UPDATE t SET val = 99 WHERE id = 3;
SELECT count(*) FROM t WHERE val = 42;
"

# 93b. Sequential insert/delete same key
oracle "cat93_insert_delete_cycle" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 20);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1, 30);
SELECT * FROM t;
"

# 93c. Very sparse IDs
oracle "cat93_sparse_ids" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(1000,20),(1000000,30);
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE val > 15 ORDER BY id;
UPDATE t SET val = val + 100;
SELECT * FROM t ORDER BY id;
"

# 93d. Decreasing insert order
oracle "cat93_decreasing_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(5,50),(4,40),(3,30),(2,20),(1,10);
SELECT * FROM t ORDER BY id;
SELECT val FROM t ORDER BY val;
"

# ════════════════════════════════════════════════════════════════════
# Category 94: Complex HAVING patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 94: Complex HAVING patterns ---"

# 94a. HAVING with multiple conditions
oracle "cat94_having_multi" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',5),(5,'b',15),(6,'c',100);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp
HAVING count(*) >= 2 AND sum(val) > 15 ORDER BY grp;
"

# 94b. HAVING with subquery
oracle "cat94_having_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);
SELECT grp, sum(val) as s FROM t GROUP BY grp
HAVING sum(val) > (SELECT avg(val) FROM t) ORDER BY grp;
"

# 94c. HAVING with expression
oracle "cat94_having_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category INT, amount INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1,1,100),(2,1,200),(3,2,50),(4,2,75),(5,3,500);
SELECT category, sum(amount) as total, count(*) as cnt FROM t
GROUP BY category HAVING total / cnt > 100 ORDER BY category;
"

# ════════════════════════════════════════════════════════════════════
# Category 95: Complex INSERT patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 95: Complex INSERT patterns ---"

# 95a. INSERT with RETURNING and aggregation
oracle "cat95_insert_returning_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30) RETURNING sum(val) OVER () as total;
"

# 95b. INSERT from complex CTE
oracle "cat95_insert_from_cte" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, rank_val INT);
CREATE INDEX idx ON t(val);
WITH ranked AS (
  SELECT 1 as id, 50 as val
  UNION ALL SELECT 2, 30
  UNION ALL SELECT 3, 70
  UNION ALL SELECT 4, 10
)
INSERT INTO t SELECT id, val, rank() OVER (ORDER BY val) FROM ranked;
SELECT * FROM t ORDER BY id;
"

# 95c. INSERT with complex expression
oracle "cat95_insert_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, hash TEXT, len INT);
INSERT INTO t VALUES(1, hex(randomblob(4)), 4);
INSERT INTO t VALUES(2, hex(randomblob(8)), 8);
INSERT INTO t VALUES(3, hex(randomblob(16)), 16);
SELECT id, length(hash)/2 as bytes, len FROM t ORDER BY id;
"

# ════════════════════════════════════════════════════════════════════
# Category 96: Multiple secondary indexes on WITHOUT ROWID
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 96: Multiple secondary indexes on WR ---"

# 96a. WITHOUT ROWID with three secondary indexes
oracle "cat96_wr_three_secidx" "
CREATE TABLE t(a TEXT, b INT, c INT, d TEXT, e REAL, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
CREATE INDEX idx_d ON t(d);
CREATE INDEX idx_e ON t(e);
INSERT INTO t VALUES('x',1,10,'hello',1.5),('x',2,20,'world',2.5),('y',1,30,'foo',3.5);
UPDATE t SET c = c+100, d = d||'!', e = e*10 WHERE a = 'x';
SELECT * FROM t ORDER BY a, b;
SELECT a, b FROM t WHERE c > 100 ORDER BY c;
SELECT a, b FROM t WHERE d LIKE '%!' ORDER BY d;
PRAGMA integrity_check;
"

# 96b. WITHOUT ROWID: DELETE with multiple secondary indexes
oracle "cat96_wr_delete_multi_secidx" "
CREATE TABLE t(k INT PRIMARY KEY, a INT, b TEXT, c REAL) WITHOUT ROWID;
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES(1,10,'x',1.1),(2,20,'y',2.2),(3,30,'z',3.3),(4,40,'w',4.4);
DELETE FROM t WHERE k IN (2, 4);
SELECT * FROM t ORDER BY k;
SELECT k FROM t WHERE a > 15;
SELECT k FROM t WHERE b > 'x' ORDER BY b;
PRAGMA integrity_check;
"

# 96c. WITHOUT ROWID: REPLACE with multiple secondary indexes
oracle "cat96_wr_replace_multi_secidx" "
CREATE TABLE t(k TEXT PRIMARY KEY, a INT, b TEXT) WITHOUT ROWID;
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES('k1',10,'x'),('k2',20,'y'),('k3',30,'z');
REPLACE INTO t VALUES('k2',99,'replaced');
SELECT * FROM t ORDER BY k;
SELECT k FROM t WHERE a > 50;
SELECT k FROM t WHERE b = 'replaced';
PRAGMA integrity_check;
"

# ════════════════════════════════════════════════════════════════════
# Category 97: Chained operations on same row
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 97: Chained operations on same row ---"

# 97a. Multiple UPDATEs to same row
oracle "cat97_multi_update_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100,1000);
UPDATE t SET a = 20 WHERE id = 1;
UPDATE t SET b = 200 WHERE id = 1;
UPDATE t SET c = 2000 WHERE id = 1;
SELECT * FROM t;
PRAGMA integrity_check;
"

# 97b. UPDATE then DELETE then INSERT same PK
oracle "cat97_update_delete_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10);
UPDATE t SET val = 20 WHERE id = 1;
SELECT * FROM t;
DELETE FROM t WHERE id = 1;
SELECT count(*) FROM t;
INSERT INTO t VALUES(1, 30);
SELECT * FROM t;
"

# 97c. REPLACE chain on same key
oracle "cat97_replace_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, ver INT);
CREATE INDEX idx ON t(val);
REPLACE INTO t VALUES(1, 10, 1);
REPLACE INTO t VALUES(1, 20, 2);
REPLACE INTO t VALUES(1, 30, 3);
SELECT * FROM t;
SELECT count(*) FROM t;
"

# ════════════════════════════════════════════════════════════════════
# Category 98: Extreme value edge cases
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 98: Extreme value edge cases ---"

# 98a. Zero values
oracle "cat98_zeros" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,0),(2,0),(3,0);
SELECT count(*) FROM t WHERE val = 0;
UPDATE t SET val = id WHERE val = 0;
SELECT * FROM t ORDER BY val;
"

# 98b. Negative values in index
oracle "cat98_negatives" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,-100),(2,-50),(3,0),(4,50),(5,100);
SELECT val FROM t ORDER BY val;
SELECT val FROM t WHERE val < 0 ORDER BY val;
SELECT val FROM t WHERE val BETWEEN -75 AND 75 ORDER BY val;
"

# 98c. Mixed positive/negative with mutations
oracle "cat98_mixed_sign" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,-10),(3,20),(4,-20),(5,0);
UPDATE t SET val = -val;
SELECT val FROM t ORDER BY val;
DELETE FROM t WHERE val > 0;
SELECT val FROM t ORDER BY val;
"

# 98d. Empty string operations
oracle "cat98_empty_string_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,''),(2,'a'),(3,''),(4,'b');
SELECT id FROM t WHERE name = '' ORDER BY id;
UPDATE t SET name = 'filled' WHERE name = '';
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE name = '';
"

# ════════════════════════════════════════════════════════════════════
# Category 99: Complex trigger + savepoint
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 99: Complex trigger + savepoint ---"

# 99a. Trigger inside savepoint
oracle "cat99_trigger_in_sp" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE log(msg TEXT);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg AFTER UPDATE ON t BEGIN
  INSERT INTO log VALUES('updated ' || NEW.id || ' to ' || NEW.val);
END;
INSERT INTO t VALUES(1,10),(2,20);
SAVEPOINT sp;
UPDATE t SET val = 99 WHERE id = 1;
SELECT * FROM log;
ROLLBACK TO sp;
SELECT * FROM log;
SELECT * FROM t ORDER BY id;
"

# 99b. Trigger creates index-affecting change in savepoint
oracle "cat99_trigger_idx_sp" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_val INT);
CREATE INDEX idx_p ON parent(val);
CREATE INDEX idx_c ON child(parent_val);
CREATE TRIGGER sync_child AFTER UPDATE ON parent BEGIN
  UPDATE child SET parent_val = NEW.val WHERE parent_val = OLD.val;
END;
INSERT INTO parent VALUES(1, 100);
INSERT INTO child VALUES(1, 100),(2, 100);
SAVEPOINT sp;
UPDATE parent SET val = 200 WHERE id = 1;
SELECT * FROM child ORDER BY id;
ROLLBACK TO sp;
SELECT * FROM child ORDER BY id;
SELECT * FROM parent;
"

# ════════════════════════════════════════════════════════════════════
# Category 100: Grand finale - complex real-world patterns
# ════════════════════════════════════════════════════════════════════
echo ""
echo "--- Category 100: Grand finale - complex patterns ---"

# 100a. E-commerce order processing simulation
oracle "cat100_ecommerce" "
CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price INT, stock INT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, product_id INT, qty INT, status TEXT);
CREATE INDEX idx_stock ON products(stock);
CREATE INDEX idx_status ON orders(status);
CREATE INDEX idx_prod ON orders(product_id);
INSERT INTO products VALUES(1,'Widget',100,50),(2,'Gadget',200,30),(3,'Gizmo',50,100);
INSERT INTO orders VALUES(1,1,5,'pending'),(2,2,3,'pending'),(3,1,10,'pending');
UPDATE products SET stock = stock - (SELECT sum(qty) FROM orders WHERE orders.product_id = products.id AND status = 'pending')
  WHERE id IN (SELECT DISTINCT product_id FROM orders WHERE status = 'pending');
UPDATE orders SET status = 'shipped';
SELECT p.name, p.stock FROM products p ORDER BY p.name;
SELECT o.id, o.status FROM orders o ORDER BY o.id;
"

# 100b. Accounting ledger with running balance
oracle "cat100_ledger" "
CREATE TABLE ledger(id INTEGER PRIMARY KEY, account TEXT, amount INT, ts TEXT);
CREATE INDEX idx_acct ON ledger(account, id);
INSERT INTO ledger VALUES(1,'checking',1000,'2026-01-01');
INSERT INTO ledger VALUES(2,'checking',-200,'2026-01-05');
INSERT INTO ledger VALUES(3,'savings',5000,'2026-01-01');
INSERT INTO ledger VALUES(4,'checking',500,'2026-01-10');
INSERT INTO ledger VALUES(5,'savings',-1000,'2026-01-15');
SELECT account, id, amount,
  sum(amount) OVER (PARTITION BY account ORDER BY id) as balance
FROM ledger ORDER BY account, id;
"

# 100c. Tag system with many-to-many
oracle "cat100_tagging" "
CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE tags(id INTEGER PRIMARY KEY, tag TEXT UNIQUE);
CREATE TABLE item_tags(item_id INT, tag_id INT, PRIMARY KEY(item_id, tag_id));
CREATE INDEX idx_tag ON item_tags(tag_id);
INSERT INTO items VALUES(1,'doc1'),(2,'doc2'),(3,'doc3');
INSERT INTO tags VALUES(1,'urgent'),(2,'review'),(3,'done');
INSERT INTO item_tags VALUES(1,1),(1,2),(2,2),(2,3),(3,1);
SELECT i.name, GROUP_CONCAT(t.tag, ', ') as tags
FROM items i JOIN item_tags it ON i.id = it.item_id
JOIN tags t ON it.tag_id = t.id GROUP BY i.id ORDER BY i.name;
DELETE FROM item_tags WHERE tag_id = (SELECT id FROM tags WHERE tag = 'done');
UPDATE items SET name = name || '_v2' WHERE id IN (SELECT item_id FROM item_tags WHERE tag_id = 1);
SELECT i.name, GROUP_CONCAT(t.tag, ', ') as tags
FROM items i JOIN item_tags it ON i.id = it.item_id
JOIN tags t ON it.tag_id = t.id GROUP BY i.id ORDER BY i.name;
"

# 100d. Hierarchical category with depth
oracle "cat100_hierarchy" "
CREATE TABLE categories(id INTEGER PRIMARY KEY, parent_id INT, name TEXT, depth INT);
CREATE INDEX idx_parent ON categories(parent_id);
CREATE INDEX idx_depth ON categories(depth);
INSERT INTO categories VALUES(1,NULL,'root',0);
INSERT INTO categories VALUES(2,1,'electronics',1),(3,1,'books',1);
INSERT INTO categories VALUES(4,2,'phones',2),(5,2,'laptops',2),(6,3,'fiction',2);
INSERT INTO categories VALUES(7,4,'smartphones',3),(8,4,'feature_phones',3);
WITH RECURSIVE tree(id, name, path, d) AS (
  SELECT id, name, name, 0 FROM categories WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.name, t.path || '/' || c.name, t.d+1
  FROM categories c JOIN tree t ON c.parent_id = t.id
)
SELECT id, path, d FROM tree ORDER BY path;
"

# 100e. Time series with gap detection
oracle "cat100_timeseries" "
CREATE TABLE readings(id INTEGER PRIMARY KEY, sensor TEXT, ts TEXT, val REAL);
CREATE INDEX idx_sensor_ts ON readings(sensor, ts);
INSERT INTO readings VALUES(1,'A','2026-01-01',10.0);
INSERT INTO readings VALUES(2,'A','2026-01-02',12.0);
INSERT INTO readings VALUES(3,'A','2026-01-04',15.0);
INSERT INTO readings VALUES(4,'B','2026-01-01',20.0);
INSERT INTO readings VALUES(5,'B','2026-01-02',22.0);
INSERT INTO readings VALUES(6,'B','2026-01-03',21.0);
SELECT sensor, ts, val,
  val - lag(val) OVER (PARTITION BY sensor ORDER BY ts) as delta
FROM readings ORDER BY sensor, ts;
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
