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
echo ""
echo "================================"
echo "Results: $pass passed, $fail failed"
echo "================================"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
exit 0
