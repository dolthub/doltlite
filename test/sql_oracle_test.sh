#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
pass=0; fail=0

normalize_oracle_output() {
  LC_ALL=C sed -E \
    -e 's/^Error near line [0-9]+: /ERROR: /' \
    -e 's/^Runtime error near line [0-9]+: /ERROR: /' \
    -e 's/ \([0-9]+\)$//'
}

oracle() {
  local name="$1" sql="$2"
  oracle_with_flags "$name" "$sql" ""
}

oracle_with_flags() {
  local name="$1" sql="$2" flags="$3"
  local dl="$TMPDIR/dl_${name}.db" sq="$TMPDIR/sq_${name}.db"
  local out_dl out_sq norm_dl norm_sq rc_dl rc_sq
  rm -f "$dl" "$sq"
  out_dl=$(echo "$sql" | "$DOLTLITE" $flags "$dl" 2>&1)
  rc_dl=$?
  out_sq=$(echo "$sql" | "$SQLITE3" $flags "$sq" 2>&1)
  rc_sq=$?
  norm_dl=$(printf '%s\n' "$out_dl" | normalize_oracle_output)
  norm_sq=$(printf '%s\n' "$out_sq" | normalize_oracle_output)
  if [ "$rc_dl" -eq "$rc_sq" ] && [ "$norm_dl" = "$norm_sq" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo "  FAIL: $name"
    echo "    doltlite rc: $rc_dl"
    echo "    doltlite: $(echo "$out_dl" | head -3)"
    echo "    sqlite3 rc:  $rc_sq"
    echo "    sqlite3:  $(echo "$out_sq" | head -3)"
  fi
}

oracle_unsafe() {
  oracle_with_flags "$1" "$2" "--unsafe-testing"
}

echo "=== Index Oracle Tests ==="
echo ""

echo "--- Category 1: UPDATE with single-column index ---"

for N in 100 1000; do

  oracle "cat1_update_indexed_col_tablescan_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val * 2;
SELECT count(*), sum(val), min(val), max(val) FROM t ORDER BY 1;
"

  oracle "cat1_update_indexed_col_idxscan_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val * 2;
SELECT val FROM t WHERE val >= 0 ORDER BY val LIMIT 5;
SELECT val FROM t WHERE val >= 0 ORDER BY val DESC LIMIT 5;
"

  oracle "cat1_update_nonindexed_col_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, other TEXT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x, 'orig' FROM c;
UPDATE t SET other = 'changed';
SELECT count(*), sum(val), min(val), max(val) FROM t;
SELECT val FROM t WHERE val <= 5 ORDER BY val;
"

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

  oracle "cat1_update_self_ref_N${N}" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<${N})
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val + 1;
SELECT count(*), sum(val), min(val), max(val) FROM t;
SELECT val FROM t ORDER BY val LIMIT 5;
"

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

echo ""
echo "--- Category 2: Multi-column composite index ---"

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

oracle "cat2_delete_composite_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
DELETE FROM t WHERE a = 'grp0' AND b > 200;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE a = 'grp0';
"

oracle "cat2_select_prefix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
SELECT count(*) FROM t WHERE a = 'grp0';
SELECT count(*) FROM t WHERE a = 'grp5';
SELECT min(b), max(b) FROM t WHERE a = 'grp0';
"

oracle "cat2_select_full_key" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL);
CREATE INDEX idx_ab ON t(a, b);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, 'grp' || (x % 10), x, x * 1.5 FROM c;
SELECT id, a, b FROM t WHERE a = 'grp0' AND b = 10 ORDER BY id;
SELECT id, a, b FROM t WHERE a = 'grp0' AND b = 100 ORDER BY id;
SELECT count(*) FROM t WHERE a = 'grp0' AND b = 10;
"

oracle "cat2_insert_or_replace_unique_ab" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL, UNIQUE(a, b));
INSERT INTO t VALUES(1, 'hello', 10, 1.0);
INSERT INTO t VALUES(2, 'hello', 20, 2.0);
INSERT INTO t VALUES(3, 'world', 10, 3.0);
INSERT OR REPLACE INTO t VALUES(4, 'hello', 10, 99.0);
SELECT count(*) FROM t;
SELECT id, a, b, c FROM t ORDER BY id;
"

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

echo ""
echo "--- Category 3: BLOB and mixed-type indexes ---"

oracle "cat3_blob_insert_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, x'DEADBEEF');
INSERT INTO t VALUES(2, x'CAFEBABE');
INSERT INTO t VALUES(3, x'00112233');
UPDATE t SET data = x'FFFFFFFF' WHERE id = 1;
SELECT id, hex(data) FROM t ORDER BY id;
"

oracle "cat3_empty_blob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, zeroblob(0));
INSERT INTO t VALUES(2, x'AA');
INSERT INTO t VALUES(3, zeroblob(0));
SELECT id, hex(data), length(data) FROM t ORDER BY id;
SELECT count(*) FROM t WHERE data = zeroblob(0);
"

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

oracle "cat3_large_blob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, zeroblob(1000));
INSERT INTO t VALUES(2, zeroblob(500));
UPDATE t SET data = zeroblob(2000) WHERE id = 1;
SELECT id, length(data) FROM t ORDER BY id;
SELECT count(*) FROM t;
"

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

echo ""
echo "--- Category 4: Transaction interactions ---"

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

echo ""
echo "--- Category 5: INSERT OR REPLACE / UPSERT ---"

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

oracle "cat5_replace_unique_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, name TEXT);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT INTO t VALUES(3, 30, 'charlie');
INSERT OR REPLACE INTO t VALUES(4, 20, 'new_bob');
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

oracle "cat5_ignore_unique_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, name TEXT);
INSERT INTO t VALUES(1, 10, 'alice');
INSERT INTO t VALUES(2, 20, 'bob');
INSERT OR IGNORE INTO t VALUES(3, 10, 'should_be_ignored');
INSERT OR IGNORE INTO t VALUES(4, 40, 'dave');
SELECT id, val, name FROM t ORDER BY id;
SELECT count(*) FROM t;
"

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

echo ""
echo "--- Category 6: Edge cases ---"

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

oracle "cat6_without_rowid_replace_same_tx" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
BEGIN;
INSERT INTO t VALUES(1, 1, 100);
REPLACE INTO t VALUES(1, 1, 200);
REPLACE INTO t VALUES(1, 1, 300);
SELECT a, b, c FROM t ORDER BY a, b, c;
SELECT count(*) FROM t;
COMMIT;
"

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

echo ""
echo "--- Category 7: Integrity checks ---"

oracle "cat7_integrity_bulk_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<500)
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = val * 2;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

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

oracle "cat7_update_after_savepoint_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
SAVEPOINT sp;
UPDATE t SET a = 99, b = 99;
DELETE FROM t WHERE id = 2;
INSERT INTO t VALUES(4,40,400);
ROLLBACK TO sp;
INSERT INTO t VALUES(5,50,500);
UPDATE t SET b = b + 1;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

oracle "cat7_integrity_create_index_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<300)
INSERT INTO t SELECT x, x FROM c;
CREATE INDEX idx ON t(val);
UPDATE t SET val = val + 5000 WHERE id % 3 = 0;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

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

echo ""
echo "--- Category 8: WITHOUT ROWID tables ---"

oracle "cat8_wr_basic_crud" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200), ('y', 1, 300);
UPDATE t SET c = 999 WHERE a = 'x' AND b = 1;
DELETE FROM t WHERE a = 'y';
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
"

oracle "cat8_wr_single_pk_update" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('a', 1), ('b', 2), ('c', 3);
UPDATE t SET v = v * 10;
SELECT * FROM t ORDER BY k;
"

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

oracle "cat8_wr_replace" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100);
REPLACE INTO t VALUES('x', 1, 999);
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
"

oracle "cat8_wr_update_pk" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100), ('x', 2, 200);
UPDATE t SET a = 'z' WHERE b = 1;
SELECT * FROM t ORDER BY a, b;
"

oracle "cat8_wr_delete_secidx" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES('a', 1, 10), ('a', 2, 20), ('b', 1, 30), ('b', 2, 40);
DELETE FROM t WHERE c > 20;
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t WHERE c <= 20;
"

oracle "cat8_wr_upsert" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 100);
INSERT INTO t VALUES('x', 1, 999) ON CONFLICT(a, b) DO UPDATE SET c = excluded.c;
SELECT * FROM t;
SELECT count(*) FROM t;
"

oracle "cat8_wr_bulk_update_all" "
CREATE TABLE t(k INT PRIMARY KEY, v TEXT, n INT) WITHOUT ROWID;
INSERT INTO t VALUES(1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50);
UPDATE t SET n = n * 100;
SELECT * FROM t ORDER BY k;
"

oracle "cat8_wr_unique_secidx_replace" "
CREATE TABLE t(a INT, b INT, c TEXT UNIQUE, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1, 1, 'hello');
INSERT INTO t VALUES(2, 2, 'world');
REPLACE INTO t VALUES(3, 3, 'hello');
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
"

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

echo ""
echo "--- Category 9: Reverse scans and ORDER BY DESC ---"

oracle "cat9_desc_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 30), (2, 10), (3, 50), (4, 20), (5, 40);
SELECT val FROM t ORDER BY val DESC;
"

oracle "cat9_desc_limit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x * 7 % 100 FROM c;
SELECT val FROM t ORDER BY val DESC LIMIT 5;
"

oracle "cat9_desc_composite_prefix" "
CREATE TABLE t(a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES('x', 3, 100), ('x', 1, 200), ('x', 2, 300);
INSERT INTO t VALUES('y', 2, 400), ('y', 1, 500);
SELECT a, b FROM t WHERE a = 'x' ORDER BY b DESC;
"

oracle "cat9_desc_composite_full" "
CREATE TABLE t(a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES('a', 1, 10), ('a', 2, 20), ('b', 1, 30), ('b', 2, 40);
SELECT a, b FROM t ORDER BY a DESC, b DESC;
"

oracle "cat9_desc_range" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
SELECT val FROM t WHERE val BETWEEN 20 AND 40 ORDER BY val DESC;
"

oracle "cat9_max_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 50), (3, 30), (4, 20), (5, 40);
SELECT max(val) FROM t;
SELECT min(val) FROM t;
"

oracle "cat9_max_where_composite_index" "
CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  data TEXT,
  UNIQUE(aggregate_kind, stream_id, stream_version)
);
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',0,'v0');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',1,'v1');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',2,'v2');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',3,'v3');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',4,'v4');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',5,'v5');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',6,'v6');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',7,'v7');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',8,'v8');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s1',9,'v9');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s2',0,'other');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('thread','s2',1,'other');
INSERT INTO events(aggregate_kind, stream_id, stream_version, data) VALUES('cmd','s1',0,'other_kind');
SELECT MAX(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';
SELECT MIN(stream_version) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';
SELECT COALESCE(MAX(stream_version)+1, 0) FROM events WHERE aggregate_kind='thread' AND stream_id='s1';
"

oracle "cat9_desc_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
UPDATE t SET val = val + 100 WHERE id <= 3;
SELECT val FROM t ORDER BY val DESC;
"

oracle "cat9_desc_without_rowid" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x', 1, 10), ('x', 2, 20), ('x', 3, 30);
INSERT INTO t VALUES('y', 1, 40), ('y', 2, 50);
SELECT a, b FROM t WHERE a = 'x' ORDER BY b DESC;
SELECT a, b FROM t ORDER BY a DESC, b DESC;
"

oracle "cat9_desc_window" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1, 'a', 10), (2, 'a', 30), (3, 'a', 20);
INSERT INTO t VALUES(4, 'b', 40), (5, 'b', 50);
SELECT grp, val, rank() OVER (PARTITION BY grp ORDER BY val DESC) as rnk
FROM t ORDER BY grp, rnk;
"

echo ""
echo "--- Category 10: Range queries and boundary conditions ---"

oracle "cat10_seek_past_end" "
CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1), (3), (5), (7), (9);
SELECT * FROM t WHERE x >= 10;
SELECT * FROM t WHERE x > 9;
SELECT count(*) FROM t WHERE x > 100;
"

oracle "cat10_seek_before_start" "
CREATE TABLE t(x INTEGER PRIMARY KEY);
INSERT INTO t VALUES(10), (20), (30);
SELECT * FROM t WHERE x < 5;
SELECT * FROM t WHERE x <= 0;
SELECT count(*) FROM t WHERE x < 10;
"

oracle "cat10_between" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 5), (2, 10), (3, 15), (4, 20), (5, 25), (6, 30);
SELECT val FROM t WHERE val BETWEEN 10 AND 25 ORDER BY val;
SELECT val FROM t WHERE val BETWEEN 100 AND 200 ORDER BY val;
SELECT val FROM t WHERE val BETWEEN 5 AND 5 ORDER BY val;
"

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

oracle "cat10_boundary_first_last" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
SELECT val FROM t WHERE val >= 10 ORDER BY val LIMIT 1;
SELECT val FROM t WHERE val <= 50 ORDER BY val DESC LIMIT 1;
SELECT val FROM t WHERE val > 10 ORDER BY val LIMIT 1;
SELECT val FROM t WHERE val < 50 ORDER BY val DESC LIMIT 1;
"

oracle "cat10_gaps" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 1000), (3, 1000000);
SELECT val FROM t WHERE val > 10 AND val < 1000000 ORDER BY val;
SELECT val FROM t WHERE val >= 500 ORDER BY val LIMIT 1;
SELECT count(*) FROM t WHERE val BETWEEN 11 AND 999;
"

oracle "cat10_dup_range" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 10), (3, 10), (4, 20), (5, 20), (6, 30);
SELECT count(*) FROM t WHERE val = 10;
SELECT count(*) FROM t WHERE val >= 10 AND val < 20;
SELECT count(*) FROM t WHERE val > 10 AND val <= 20;
SELECT id FROM t WHERE val = 10 ORDER BY id;
"

echo ""
echo "--- Category 11: NULL handling in indexes ---"

oracle "cat11_null_ordering" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL), (2, 10), (3, NULL), (4, 5), (5, 20);
SELECT id, val FROM t ORDER BY val;
SELECT id, val FROM t ORDER BY val DESC;
"

oracle "cat11_null_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1, 'x', NULL), (2, 'x', 10), (3, NULL, 5), (4, NULL, NULL);
SELECT id, a, b FROM t ORDER BY a, b;
SELECT count(*) FROM t WHERE a IS NULL;
SELECT count(*) FROM t WHERE a = 'x' AND b IS NULL;
"

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

oracle "cat11_null_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
INSERT INTO t VALUES(1, NULL);
INSERT INTO t VALUES(2, NULL);
INSERT INTO t VALUES(3, 10);
SELECT count(*) FROM t;
SELECT id, val FROM t ORDER BY id;
"

oracle "cat11_isnull_scan" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL), (2, 'a'), (3, NULL), (4, 'b'), (5, NULL);
SELECT id FROM t WHERE val IS NULL ORDER BY id;
SELECT id FROM t WHERE val IS NOT NULL ORDER BY id;
SELECT count(*) FROM t WHERE val IS NULL;
"

oracle "cat11_null_wr_secondary" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x', 1, NULL), ('x', 2, 10), ('y', 1, NULL);
SELECT a, b, c FROM t WHERE c IS NULL ORDER BY a, b;
SELECT count(*) FROM t WHERE c IS NOT NULL;
UPDATE t SET c = 42 WHERE c IS NULL;
SELECT * FROM t ORDER BY a, b;
"

echo ""
echo "--- Category 12: Type affinity and mixed types ---"

oracle "cat12_int_text_affinity" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42), (2, '42'), (3, 42.0);
SELECT id, typeof(val), val FROM t ORDER BY id;
SELECT id FROM t WHERE val = 42 ORDER BY id;
SELECT id FROM t WHERE val = '42' ORDER BY id;
"

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

oracle "cat12_text_binary_collation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1, 'abc'), (2, 'ABC'), (3, 'Abc'), (4, 'aBC');
SELECT name FROM t ORDER BY name;
SELECT name FROM t WHERE name > 'Abc' ORDER BY name;
SELECT name FROM t WHERE name = 'abc';
"

oracle "cat12_text_ordering" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1, 'banana'), (2, 'apple'), (3, 'cherry'), (4, 'date');
SELECT name FROM t ORDER BY name;
SELECT name FROM t WHERE name >= 'cherry' ORDER BY name;
SELECT name FROM t WHERE name BETWEEN 'b' AND 'd' ORDER BY name;
"

oracle "cat12_empty_string" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, ''), (2, ' '), (3, 'a'), (4, '  ');
SELECT id, length(val) FROM t ORDER BY val;
SELECT id FROM t WHERE val = '' ORDER BY id;
SELECT id FROM t WHERE val > '' ORDER BY val;
"

oracle "cat12_blob_comparison" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, X'00'), (2, X'0000'), (3, X'0001'), (4, X'01'), (5, X'FF');
SELECT id FROM t ORDER BY data;
SELECT id FROM t WHERE data > X'00' ORDER BY data;
SELECT id FROM t WHERE data = X'0000';
"

echo ""
echo "--- Category 13: Savepoint and transaction edge cases ---"

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

echo ""
echo "--- Category 14: REPLACE and UPSERT edge cases ---"

oracle "cat14_replace_cascade_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b INT UNIQUE);
INSERT INTO t VALUES(1, 10, 100);
INSERT INTO t VALUES(2, 20, 200);
INSERT INTO t VALUES(3, 30, 300);
REPLACE INTO t VALUES(4, 10, 200);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

oracle "cat14_upsert_expression" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, count INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 1);
INSERT INTO t VALUES(1, 10, 1) ON CONFLICT(id) DO UPDATE SET count = count + 1, val = val + excluded.val;
INSERT INTO t VALUES(1, 10, 1) ON CONFLICT(id) DO UPDATE SET count = count + 1, val = val + excluded.val;
SELECT * FROM t;
"

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

oracle "cat14_upsert_composite_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, val INT, UNIQUE(a, b));
INSERT INTO t VALUES(1, 'x', 1, 100);
INSERT INTO t VALUES(2, 'x', 2, 200);
INSERT INTO t VALUES(3, 'x', 1, 999)
  ON CONFLICT(a, b) DO UPDATE SET val = excluded.val;
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

oracle "cat14_multi_replace_sequence" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
INSERT INTO t VALUES(1, 10);
REPLACE INTO t VALUES(2, 10);
REPLACE INTO t VALUES(3, 10);
REPLACE INTO t VALUES(4, 10);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

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

echo ""
echo "--- Category 15: Complex UPDATE patterns ---"

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

oracle "cat15_update_from" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE lookup(id INT, factor INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30);
INSERT INTO lookup VALUES(1, 100), (3, 300);
UPDATE t SET val = t.val * lookup.factor FROM lookup WHERE t.id = lookup.id;
SELECT * FROM t ORDER BY id;
"

oracle "cat15_update_correlated_set" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, grp TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b');
UPDATE t SET val = (SELECT sum(val) FROM t AS t2 WHERE t2.grp = t.grp);
SELECT * FROM t ORDER BY id;
"

oracle "cat15_self_ref_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1, 10, 100), (2, 20, 200), (3, 30, 300);
UPDATE t SET a = b, b = a;
SELECT * FROM t ORDER BY id;
"

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

oracle "cat15_update_case" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category TEXT, val INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
UPDATE t SET val = CASE category WHEN 'a' THEN val * 2 WHEN 'b' THEN val * 3 ELSE val END;
SELECT * FROM t ORDER BY id;
SELECT sum(val) FROM t WHERE category = 'a';
"

echo ""
echo "--- Category 16: Complex DELETE patterns ---"

oracle "cat16_delete_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE blacklist(bad_val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
INSERT INTO blacklist VALUES(20), (40);
DELETE FROM t WHERE val IN (SELECT bad_val FROM blacklist);
SELECT * FROM t ORDER BY id;
"

oracle "cat16_delete_in_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x * 10 FROM c;
DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY val DESC LIMIT 5);
SELECT count(*) FROM t;
SELECT max(val) FROM t;
"

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

oracle "cat16_delete_complex_where" "
CREATE TABLE t(a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES('x', 1, 10), ('x', 2, 20), ('x', 3, 30);
INSERT INTO t VALUES('y', 1, 40), ('y', 2, 50);
DELETE FROM t WHERE a = 'x' AND b >= 2;
SELECT * FROM t ORDER BY a, b;
"

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

oracle "cat16_truncate_with_pending_in_txn" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');
BEGIN;
INSERT INTO t VALUES (4,'d'),(5,'e');
DELETE FROM t;
SELECT count(*) FROM t;
COMMIT;
SELECT count(*) FROM t;
"

oracle "cat16_truncate_all_pending" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
DELETE FROM t;
SELECT count(*) FROM t;
COMMIT;
SELECT count(*) FROM t;
"

oracle "cat16_truncate_then_insert_same_txn" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b');
BEGIN;
INSERT INTO t VALUES (4,'d');
DELETE FROM t;
INSERT INTO t VALUES (100,'late');
SELECT id, v FROM t ORDER BY id;
COMMIT;
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_truncate_then_insert_same_pk" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES (1,'old');
DELETE FROM t;
INSERT INTO t VALUES (1,'new');
SELECT id, v FROM t;
COMMIT;
SELECT id, v FROM t;
"

oracle "cat16_savepoint_rollback_after_truncate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');
SAVEPOINT s1;
DELETE FROM t;
SELECT count(*) FROM t;
ROLLBACK TO s1;
RELEASE s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_truncate_one_of_two_tables" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t1 VALUES (1,'a'),(2,'b');
INSERT INTO t2 VALUES (10,'x'),(20,'y');
DELETE FROM t1;
SELECT 't1', count(*) FROM t1;
SELECT 't2', count(*) FROM t2;
COMMIT;
SELECT 't1', count(*) FROM t1;
SELECT 't2', count(*) FROM t2;
"

oracle "cat16_truncate_with_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX t_v ON t(v);
BEGIN;
INSERT INTO t VALUES (1,'aaa'),(2,'bbb'),(3,'ccc');
DELETE FROM t;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE v='aaa';
COMMIT;
SELECT count(*) FROM t;
"

oracle "cat16_rowid_zero_survives_seek_miss" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one'),(2,'two');
DELETE FROM t WHERE rowid=999;
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_rowid_zero_survives_no_match_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one'),(2,'two');
DELETE FROM t WHERE v='nonexistent';
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_rowid_zero_full_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one'),(2,'two');
DELETE FROM t WHERE id<10;
SELECT count(*) FROM t;
INSERT INTO t VALUES(0,'fresh-zero');
SELECT * FROM t;
"

oracle "cat16_empty_blob_key_survives" "
CREATE TABLE t(k BLOB PRIMARY KEY, v TEXT) WITHOUT ROWID;
INSERT INTO t VALUES (x'', 'empty-key'),(x'01','one'),(x'02','two');
DELETE FROM t WHERE k=x'ff';
SELECT hex(k), v FROM t ORDER BY k;
"

oracle "cat16_rowid_zero_replace_no_collateral" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one'),(2,'two');
INSERT OR REPLACE INTO t VALUES(0,'replaced');
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_rowid_zero_update_no_collateral" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(0,'zero'),(1,'one'),(2,'two');
UPDATE t SET v='updated' WHERE id=0;
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_savepos_blob_pk_bulk_update" "
CREATE TABLE t(k BLOB PRIMARY KEY, v TEXT) WITHOUT ROWID;
CREATE INDEX t_v ON t(v);
BEGIN;
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<70000)
  INSERT INTO t SELECT randomblob(32), printf('v%05d', i) FROM c;
UPDATE t SET v = v || 'X' WHERE v < 'v00500';
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE v LIKE '%X';
COMMIT;
SELECT count(*) FROM t WHERE v LIKE '%X';
"

oracle "cat16_savepos_blob_pk_bulk_delete" "
CREATE TABLE t(k BLOB PRIMARY KEY, v TEXT) WITHOUT ROWID;
BEGIN;
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<70000)
  INSERT INTO t SELECT randomblob(32), printf('v%05d', i) FROM c;
DELETE FROM t WHERE v < 'v00500';
SELECT count(*) FROM t;
COMMIT;
SELECT count(*) FROM t;
"

oracle "cat16_truncate_then_drop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES (1,'a');
DELETE FROM t;
DROP TABLE t;
SELECT name FROM sqlite_master WHERE type='table';
COMMIT;
SELECT name FROM sqlite_master WHERE type='table';
"

oracle "cat16_truncate_with_mixed_pending" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');
BEGIN;
DELETE FROM t WHERE id=2;
INSERT INTO t VALUES (4,'d'),(5,'e');
DELETE FROM t;
SELECT count(*) FROM t;
COMMIT;
SELECT count(*) FROM t;
"

oracle "cat16_truncate_multiple_times" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES (1,'a');
DELETE FROM t;
INSERT INTO t VALUES (2,'b');
DELETE FROM t;
INSERT INTO t VALUES (3,'c');
DELETE FROM t;
INSERT INTO t VALUES (99,'final');
SELECT id, v FROM t;
COMMIT;
SELECT id, v FROM t;
"

oracle "cat16_truncate_nested_savepoint_release" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b');
SAVEPOINT s1;
INSERT INTO t VALUES (3,'c');
SAVEPOINT s2;
DELETE FROM t;
INSERT INTO t VALUES (10,'x');
RELEASE s2;
RELEASE s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_truncate_nested_savepoint_inner_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b');
SAVEPOINT s1;
INSERT INTO t VALUES (3,'c');
SAVEPOINT s2;
DELETE FROM t;
ROLLBACK TO s2;
RELEASE s2;
RELEASE s1;
SELECT id, v FROM t ORDER BY id;
"

oracle "cat16_truncate_then_integrity_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX t_v ON t(v);
BEGIN;
INSERT INTO t VALUES (1,'a'),(2,'b');
DELETE FROM t;
PRAGMA integrity_check;
COMMIT;
PRAGMA integrity_check;
"

oracle "cat16_truncate_then_insert_or_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
BEGIN;
INSERT INTO t VALUES (1,'pending');
DELETE FROM t;
INSERT OR REPLACE INTO t VALUES (1,'after');
SELECT id, v FROM t;
COMMIT;
SELECT id, v FROM t;
"

echo ""
echo "--- Category 17: Partial indexes ---"

oracle "cat17_partial_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, status TEXT, val INT);
CREATE INDEX idx ON t(val) WHERE status = 'active';
INSERT INTO t VALUES(1, 'active', 10), (2, 'inactive', 20), (3, 'active', 30);
SELECT val FROM t WHERE status = 'active' ORDER BY val;
UPDATE t SET status = 'active' WHERE id = 2;
SELECT val FROM t WHERE status = 'active' ORDER BY val;
"

oracle "cat17_partial_update_inout" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flag INT, val INT);
CREATE INDEX idx ON t(val) WHERE flag = 1;
INSERT INTO t VALUES(1, 1, 100), (2, 0, 200), (3, 1, 300), (4, 0, 400);
UPDATE t SET flag = 0 WHERE id = 1;
UPDATE t SET flag = 1 WHERE id = 2;
SELECT id, val FROM t WHERE flag = 1 ORDER BY val;
PRAGMA integrity_check;
"

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

oracle "cat17_partial_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(b) WHERE a IS NOT NULL;
INSERT INTO t VALUES(1, 10, 100), (2, NULL, 200), (3, 30, 300), (4, NULL, 400);
SELECT id, b FROM t WHERE a IS NOT NULL ORDER BY b;
UPDATE t SET a = 99 WHERE id = 2;
SELECT id, b FROM t WHERE a IS NOT NULL ORDER BY b;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 18: Multi-table and complex queries ---"

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

oracle "cat18_delete_exists" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INT);
CREATE INDEX idx ON child(parent_id);
INSERT INTO parent VALUES(1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO child VALUES(1, 1), (2, 1), (3, 3);
DELETE FROM parent WHERE NOT EXISTS (SELECT 1 FROM child WHERE child.parent_id = parent.id);
SELECT * FROM parent ORDER BY id;
"

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

oracle "cat18_update_multi_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category INT, val INT);
CREATE TABLE cats(id INTEGER PRIMARY KEY, multiplier INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40);
INSERT INTO cats VALUES(1, 10), (2, 100);
UPDATE t SET val = val * (SELECT multiplier FROM cats WHERE cats.id = t.category);
SELECT * FROM t ORDER BY id;
"

echo ""
echo "--- Category 19: Concurrent cursor / iteration edge cases ---"

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

oracle "cat19_create_index_pending" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1, 10), (2, 20), (3, 30), (4, 20), (5, 10);
CREATE INDEX idx ON t(val);
SELECT val FROM t WHERE val = 20 ORDER BY id;
UPDATE t SET val = val + 1;
SELECT val FROM t ORDER BY val;
PRAGMA integrity_check;
"

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

oracle "cat19_reindex" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 30), (2, 10), (3, 20);
UPDATE t SET val = val + 100;
REINDEX;
SELECT val FROM t ORDER BY val;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 20: Stress and scale edge cases ---"

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

echo ""
echo "--- Category 21: Aggregate queries with indexes ---"

oracle "cat21_group_by_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40),(5,'c',50);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat21_group_by_having" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100);
SELECT grp, sum(val) as s FROM t GROUP BY grp HAVING sum(val) > 20 ORDER BY grp;
"

oracle "cat21_distinct_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,10),(4,30),(5,20),(6,10);
SELECT DISTINCT val FROM t ORDER BY val;
"

oracle "cat21_count_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, status TEXT, val INT);
CREATE INDEX idx ON t(status);
INSERT INTO t VALUES(1,'active',10),(2,'inactive',20),(3,'active',30),(4,'active',40);
SELECT status, count(*) FROM t GROUP BY status ORDER BY status;
SELECT count(*) FROM t WHERE status = 'active';
"

oracle "cat21_agg_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50);
UPDATE t SET grp = 1 WHERE id = 5;
SELECT grp, count(*), sum(val), min(val), max(val) FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat21_distinct_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,'x',1),(2,'x',1),(3,'x',2),(4,'y',1),(5,'y',1);
SELECT DISTINCT a, b FROM t ORDER BY a, b;
"

oracle "cat21_group_by_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,15),(2,25),(3,35),(4,12),(5,28),(6,31);
SELECT (val / 10) * 10 as bucket, count(*) FROM t GROUP BY bucket ORDER BY bucket;
"

oracle "cat21_agg_null_groups" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,NULL,10),(2,'a',20),(3,NULL,30),(4,'a',40),(5,'b',50);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;
"

echo ""
echo "--- Category 22: Window functions ---"

oracle "cat22_row_number" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',30),(2,'a',10),(3,'a',20),(4,'b',50),(5,'b',40);
SELECT id, grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) as rn
FROM t ORDER BY grp, rn;
"

oracle "cat22_rank" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,20),(4,30),(5,30),(6,30);
SELECT id, val, rank() OVER (ORDER BY val) as rnk,
       dense_rank() OVER (ORDER BY val) as drnk
FROM t ORDER BY id;
"

oracle "cat22_sum_frame" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id, val,
  sum(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as s
FROM t ORDER BY id;
"

oracle "cat22_lag_lead" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id, val, lag(val,1) OVER (ORDER BY id) as prev,
       lead(val,1) OVER (ORDER BY id) as next
FROM t ORDER BY id;
"

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

oracle "cat22_ntile" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60);
SELECT id, val, ntile(3) OVER (ORDER BY val) as tile FROM t ORDER BY id;
"

oracle "cat22_window_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
UPDATE t SET val = val + 100 WHERE grp = 'a';
SELECT grp, val, sum(val) OVER (PARTITION BY grp ORDER BY val) as running
FROM t ORDER BY grp, val;
"

echo ""
echo "--- Category 23: CTEs (WITH clause) ---"

oracle "cat23_cte_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
WITH top3 AS (SELECT * FROM t WHERE val > 20 ORDER BY val)
SELECT id, val FROM top3 ORDER BY val DESC;
"

oracle "cat23_cte_recursive_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<50)
INSERT INTO t SELECT x, x * x FROM r;
SELECT count(*) FROM t;
SELECT val FROM t WHERE val > 2400 ORDER BY val;
"

oracle "cat23_cte_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, bonus INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0),(4,40,0),(5,50,0);
WITH high AS (SELECT id FROM t WHERE val > 25)
UPDATE t SET bonus = 100 WHERE id IN (SELECT id FROM high);
SELECT * FROM t ORDER BY id;
"

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

oracle "cat23_multi_cte" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);
WITH
  grp_sums AS (SELECT grp, sum(val) as total FROM t GROUP BY grp),
  big_grps AS (SELECT grp FROM grp_sums WHERE total > 25)
SELECT t.* FROM t WHERE grp IN (SELECT grp FROM big_grps) ORDER BY id;
"

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

echo ""
echo "--- Category 24: JOINs with indexes ---"

oracle "cat24_inner_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, label TEXT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,'a'),(2,1,'b'),(3,2,'c'),(4,9,'orphan');
SELECT t1.id, t1.val, t2.label FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id, t2.id;
"

oracle "cat24_left_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, val INT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,1,100),(2,1,200),(3,3,300);
SELECT t1.name, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id, t2.id;
"

oracle "cat24_self_join" "
CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, mgr_id INT);
CREATE INDEX idx ON emp(mgr_id);
INSERT INTO emp VALUES(1,'Alice',NULL),(2,'Bob',1),(3,'Carol',1),(4,'Dave',2);
SELECT e.name, m.name as manager
FROM emp e LEFT JOIN emp m ON e.mgr_id = m.id ORDER BY e.id;
"

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

oracle "cat24_join_composite" "
CREATE TABLE t1(a TEXT, b INT, val INT, PRIMARY KEY(a, b));
CREATE TABLE t2(id INTEGER PRIMARY KEY, a TEXT, b INT, extra TEXT);
CREATE INDEX idx ON t2(a, b);
INSERT INTO t1 VALUES('x',1,10),('x',2,20),('y',1,30);
INSERT INTO t2 VALUES(1,'x',1,'p'),(2,'x',2,'q'),(3,'y',1,'r'),(4,'z',1,'s');
SELECT t1.a, t1.b, t1.val, t2.extra
FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b ORDER BY t1.a, t1.b;
"

echo ""
echo "--- Category 25: UNION / INTERSECT / EXCEPT ---"

oracle "cat25_union" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,30),(3,40);
SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val;
"

oracle "cat25_union_all" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t1 VALUES(1,10),(2,20);
INSERT INTO t2 VALUES(1,20),(2,30);
SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val;
"

oracle "cat25_intersect" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,30),(3,40);
SELECT val FROM t1 INTERSECT SELECT val FROM t2 ORDER BY val;
"

oracle "cat25_except" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,20),(2,40);
SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val;
"

oracle "cat25_union_nulls" "
CREATE TABLE t1(val INT);
CREATE TABLE t2(val INT);
INSERT INTO t1 VALUES(NULL),(1),(2);
INSERT INTO t2 VALUES(NULL),(2),(3);
SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val;
"

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

echo ""
echo "--- Category 26: IN operator with indexes ---"

oracle "cat26_in_list" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val IN (10, 30, 50) ORDER BY id;
"

oracle "cat26_in_subquery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, target INT);
CREATE INDEX idx ON t1(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
INSERT INTO t2 VALUES(1,20),(2,40);
SELECT id, val FROM t1 WHERE val IN (SELECT target FROM t2) ORDER BY id;
"

oracle "cat26_not_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val NOT IN (20, 40) ORDER BY id;
"

oracle "cat26_in_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,NULL),(2,10),(3,20),(4,NULL);
SELECT id FROM t WHERE val IN (10, NULL) ORDER BY id;
SELECT id FROM t WHERE val NOT IN (10) ORDER BY id;
"

oracle "cat26_in_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, c INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,'x',1,10),(2,'x',2,20),(3,'y',1,30),(4,'y',2,40),(5,'z',1,50);
SELECT id, a, b FROM t WHERE a IN ('x', 'z') ORDER BY a, b;
"

oracle "cat26_multi_col_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,1,10),(2,1,20),(3,2,10),(4,2,20),(5,3,30);
SELECT id FROM t WHERE (a, b) IN ((1,10),(2,20),(3,30)) ORDER BY id;
"

echo ""
echo "--- Category 27: LIKE and pattern matching with indexes ---"

oracle "cat27_like_prefix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'alice'),(2,'bob'),(3,'alice2'),(4,'carol'),(5,'ali');
SELECT name FROM t WHERE name LIKE 'ali%' ORDER BY name;
"

oracle "cat27_like_exact" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'hello'),(2,'world'),(3,'HELLO');
SELECT id FROM t WHERE name LIKE 'hello' ORDER BY id;
"

oracle "cat27_glob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'abc'),(2,'abd'),(3,'bcd'),(4,'abc123');
SELECT name FROM t WHERE name GLOB 'ab*' ORDER BY name;
"

oracle "cat27_like_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT);
CREATE INDEX idx ON t(tag);
INSERT INTO t VALUES(1,'test_a'),(2,'test_b'),(3,'prod_a'),(4,'prod_b');
UPDATE t SET tag = 'staging_' || substr(tag, 6) WHERE tag LIKE 'test_%';
SELECT tag FROM t ORDER BY tag;
SELECT tag FROM t WHERE tag LIKE 'staging_%' ORDER BY tag;
"

oracle "cat27_like_escape" "
CREATE TABLE t(id INTEGER PRIMARY KEY, path TEXT);
CREATE INDEX idx ON t(path);
INSERT INTO t VALUES(1,'a%b'),(2,'a_b'),(3,'axb'),(4,'a%bc');
SELECT path FROM t WHERE path LIKE 'a!%b%' ESCAPE '!' ORDER BY path;
"

oracle "cat27_like_contains_literal" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES
  (1,'abc'),(2,'xabcx'),(3,'ABC'),(4,'xabcy'),(5,'ab'),
  (6,'a_c'),(7,'a%c'),(8,'za!bc'),(9,'éabc'),(10,'éABC'),
  (11,''),(12,char(97,0,98,99));
SELECT id FROM t WHERE v LIKE '%abc%' ORDER BY id;
SELECT count(*) FROM t WHERE v LIKE '%ABC%';
PRAGMA case_sensitive_like=ON;
SELECT id FROM t WHERE v LIKE '%ABC%' ORDER BY id;
SELECT id FROM t WHERE v LIKE '%a_c%' ORDER BY id;
SELECT id FROM t WHERE v LIKE '%a!_c%' ESCAPE '!' ORDER BY id;
SELECT id FROM t WHERE v LIKE '%a!%c%' ESCAPE '!' ORDER BY id;
SELECT count(*) FROM t WHERE v LIKE '%%';
SELECT count(*) FROM t WHERE v LIKE '%' || char(0) || '%';
"

oracle "cat27_nocase_secondary_index_embedded_nul" "
CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);
CREATE INDEX ts ON t(s);
INSERT INTO t(s) VALUES
  ('a'||char(0)||'b'),('a'||char(0)||'c'),('A'||char(0)||'B'),
  ('a'||char(0)||'bb'),('a'||char(0)),('a');
SELECT group_concat(id) FROM (SELECT id FROM t WHERE s='a'||char(0)||'c' ORDER BY id);
SELECT hex(s),count(*) FROM t GROUP BY s ORDER BY s;
SELECT count(DISTINCT s) FROM t;
SELECT hex(s) FROM t ORDER BY s,id;
SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY ts WHERE s>='a'||char(0)||'b' ORDER BY id);
CREATE TABLE q(s TEXT COLLATE NOCASE);
INSERT INTO q VALUES('a'||char(0)||'c');
SELECT group_concat(id) FROM (SELECT t.id FROM t JOIN q ON t.s=q.s ORDER BY t.id);
ANALYZE;
SELECT group_concat(id) FROM (SELECT id FROM t WHERE s='a'||char(0)||'c' ORDER BY id);
CREATE TABLE u(s TEXT COLLATE NOCASE UNIQUE);
INSERT OR IGNORE INTO u VALUES('x'||char(0)||'b'),('x'||char(0)||'c');
SELECT count(*) FROM u;
"

echo ""
echo "--- Category 28: ALTER TABLE with indexes ---"

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

oracle "cat28_rename_table" "
CREATE TABLE old_name(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON old_name(val);
INSERT INTO old_name VALUES(1,10),(2,20),(3,30);
ALTER TABLE old_name RENAME TO new_name;
SELECT val FROM new_name WHERE val > 15 ORDER BY val;
INSERT INTO new_name VALUES(4,25);
SELECT val FROM new_name ORDER BY val;
"

oracle "cat28_rename_column" "
CREATE TABLE t(id INTEGER PRIMARY KEY, old_col INT);
CREATE INDEX idx ON t(old_col);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
ALTER TABLE t RENAME COLUMN old_col TO new_col;
SELECT new_col FROM t WHERE new_col > 15 ORDER BY new_col;
UPDATE t SET new_col = new_col + 100;
SELECT new_col FROM t ORDER BY new_col;
"

oracle "cat28_add_column_wr" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,100),('y',2,200);
ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'new';
SELECT * FROM t ORDER BY a, b;
INSERT INTO t VALUES('z',3,300,'custom');
SELECT * FROM t ORDER BY a;
"

echo ""
echo "--- Category 29: VACUUM and maintenance ---"

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

oracle "cat29_analyze" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x % 10 FROM c;
ANALYZE;
SELECT count(*) FROM t WHERE val = 5;
"

echo ""
echo "--- Category 30: Autoincrement ---"

oracle "cat30_autoincrement_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT);
INSERT INTO t(val) VALUES('a'),('b'),('c');
SELECT * FROM t ORDER BY id;
DELETE FROM t WHERE id = 2;
INSERT INTO t(val) VALUES('d');
SELECT * FROM t ORDER BY id;
"

oracle "cat30_autoincrement_no_reuse" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT);
INSERT INTO t(val) VALUES('first'),('second'),('third');
DELETE FROM t;
INSERT INTO t(val) VALUES('after_delete');
SELECT * FROM t;
"

oracle "cat30_autoincrement_explicit" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT);
INSERT INTO t(val) VALUES('a');
INSERT INTO t VALUES(100, 'b');
INSERT INTO t(val) VALUES('c');
SELECT * FROM t ORDER BY id;
"

echo ""
echo "--- Category 31: Expression indexes ---"

oracle "cat31_expr_upper" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(UPPER(name));
INSERT INTO t VALUES(1,'Alice'),(2,'BOB'),(3,'carol');
SELECT id, name FROM t WHERE UPPER(name) = 'ALICE';
SELECT id, name FROM t WHERE UPPER(name) > 'B' ORDER BY UPPER(name);
"

oracle "cat31_expr_arithmetic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a + b);
INSERT INTO t VALUES(1,10,5),(2,3,12),(3,8,8),(4,1,1);
SELECT id, a, b FROM t WHERE a + b = 15 ORDER BY id;
SELECT id, a + b as total FROM t ORDER BY total;
"

oracle "cat31_expr_substr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX idx ON t(substr(code, 1, 3));
INSERT INTO t VALUES(1,'ABC-001'),(2,'ABC-002'),(3,'DEF-001'),(4,'ABC-003');
SELECT id, code FROM t WHERE substr(code, 1, 3) = 'ABC' ORDER BY id;
"

oracle "cat31_expr_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val * 2);
INSERT INTO t VALUES(1,5),(2,10),(3,15);
UPDATE t SET val = val + 10;
SELECT id, val FROM t WHERE val * 2 = 30 ORDER BY id;
SELECT id, val FROM t ORDER BY val * 2;
"

echo ""
echo "--- Category 32: Covering index scans ---"

oracle "cat32_covering_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,10,100,'x'),(2,20,200,'y'),(3,10,300,'z');
SELECT a, b FROM t WHERE a = 10 ORDER BY b;
"

oracle "cat32_covering_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
SELECT grp, sum(val) FROM t GROUP BY grp ORDER BY grp;
SELECT grp, min(val), max(val) FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat32_covering_after_mutations" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
UPDATE t SET b = b + 1000 WHERE a = 20;
DELETE FROM t WHERE a = 10;
INSERT INTO t VALUES(4,40,400);
SELECT a, b FROM t ORDER BY a;
"

echo ""
echo "--- Category 33: Foreign key cascades ---"

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

echo ""
echo "--- Category 34: RETURNING clause ---"

oracle "cat34_insert_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20) RETURNING id, val;
"

oracle "cat34_update_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val + 100 WHERE id >= 2 RETURNING id, val;
"

oracle "cat34_delete_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
DELETE FROM t WHERE val > 15 RETURNING id, val;
"

oracle "cat34_replace_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10);
INSERT OR REPLACE INTO t VALUES(1,99) RETURNING id, val;
SELECT * FROM t;
"

oracle "cat34_returning_wr" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,100),('y',2,200) RETURNING *;
UPDATE t SET c = c + 1000 RETURNING a, b, c;
"

echo ""
echo "--- Category 35: Complex subqueries ---"

oracle "cat35_scalar_subquery" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INT, amount INT);
CREATE INDEX idx ON t2(t1_id);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,100),(2,1,200),(3,2,300);
SELECT id, val, (SELECT sum(amount) FROM t2 WHERE t2.t1_id = t1.id) as total
FROM t1 ORDER BY id;
"

oracle "cat35_exists" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1),(2,3);
SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.id) ORDER BY id;
SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.id) ORDER BY id;
"

oracle "cat35_derived_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
SELECT d.grp, d.total FROM (SELECT grp, sum(val) as total FROM t GROUP BY grp) d
WHERE d.total > 25 ORDER BY d.grp;
"

oracle "cat35_nested" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT * FROM t WHERE val > (SELECT avg(val) FROM t WHERE val < (SELECT max(val) FROM t))
ORDER BY id;
"

oracle "cat35_correlated_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',30),(3,'b',20),(4,'b',40),(5,'a',20);
SELECT * FROM t t1 WHERE val = (SELECT max(val) FROM t t2 WHERE t2.grp = t1.grp)
ORDER BY id;
"

echo ""
echo "--- Category 36: Temp tables and attached databases ---"

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

oracle "cat36_temp_main_interaction" "
CREATE TABLE main_t(id INTEGER PRIMARY KEY, val INT);
CREATE TEMP TABLE temp_t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO main_t VALUES(1,10),(2,20);
INSERT INTO temp_t VALUES(1,100),(2,200);
SELECT m.val, t.val FROM main_t m JOIN temp_t t ON m.id = t.id ORDER BY m.id;
"

oracle "cat36_attach_read" "
ATTACH ':memory:' AS db2;
CREATE TABLE db2.t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO db2.t VALUES(1,10),(2,20),(3,30);
SELECT * FROM db2.t ORDER BY val;
SELECT count(*) FROM db2.t WHERE val > 15;
DETACH db2;
"

oracle "cat36_cross_db" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t1 VALUES(1,10),(2,20);
ATTACH ':memory:' AS db2;
CREATE TABLE db2.t2(id INTEGER PRIMARY KEY, ref INT, label TEXT);
INSERT INTO db2.t2 VALUES(1,1,'a'),(2,2,'b');
SELECT t1.val, t2.label FROM t1 JOIN db2.t2 t2 ON t1.id = t2.ref ORDER BY t1.id;
DETACH db2;
"

echo ""
echo "--- Category 37: Generated columns ---"

oracle "cat37_generated_stored" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED);
CREATE INDEX idx ON t(c);
INSERT INTO t(id, a, b) VALUES(1, 10, 5),(2, 20, 3),(3, 5, 15);
SELECT * FROM t ORDER BY c;
SELECT * FROM t WHERE c > 15 ORDER BY id;
"

oracle "cat37_generated_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, doubled INT GENERATED ALWAYS AS (val * 2) STORED);
CREATE INDEX idx ON t(doubled);
INSERT INTO t(id, val) VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val + 5;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE doubled > 40 ORDER BY id;
"

oracle "cat37_generated_virtual" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first TEXT, last TEXT, full_name TEXT GENERATED ALWAYS AS (first || ' ' || last) VIRTUAL);
INSERT INTO t(id, first, last) VALUES(1,'John','Doe'),(2,'Jane','Smith');
SELECT full_name FROM t ORDER BY id;
UPDATE t SET first = 'Bob' WHERE id = 1;
SELECT full_name FROM t ORDER BY id;
"

echo ""
echo "--- Category 38: Complex trigger chains ---"

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

echo ""
echo "--- Category 39: OR optimization and multi-index ---"

oracle "cat39_or_separate_idx" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,100),(4,10,300),(5,50,500);
SELECT id FROM t WHERE a = 10 OR b = 100 ORDER BY id;
"

oracle "cat39_or_same_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val = 10 OR val = 30 OR val = 50 ORDER BY id;
"

oracle "cat39_or_and_mix" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_ab ON t(a, b);
INSERT INTO t VALUES(1,1,10,100),(2,1,20,200),(3,2,10,300),(4,2,20,400);
SELECT id FROM t WHERE (a = 1 AND b = 10) OR (a = 2 AND b = 20) ORDER BY id;
"

echo ""
echo "--- Category 40: Misc edge cases ---"

oracle "cat40_replace_wr_no_secidx" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES('a', 1);
REPLACE INTO t VALUES('a', 2);
REPLACE INTO t VALUES('b', 3);
SELECT * FROM t ORDER BY k;
SELECT count(*) FROM t;
"

oracle "cat40_coalesce_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,NULL),(2,10),(3,NULL),(4,20);
SELECT id FROM t WHERE COALESCE(val, 0) > 5 ORDER BY id;
"

oracle "cat40_case_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, status TEXT, val INT);
CREATE INDEX idx ON t(status);
INSERT INTO t VALUES(1,'high',10),(2,'low',20),(3,'medium',30),(4,'high',40);
SELECT * FROM t ORDER BY CASE status WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, val;
"

oracle "cat40_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT * FROM t WHERE val IN (VALUES(10),(30)) ORDER BY id;
"

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

oracle "cat40_large_sum" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 1000000000),(2, 1000000000),(3, 1000000000);
SELECT sum(val) FROM t;
SELECT sum(val) FROM t WHERE val > 0;
"

oracle "cat40_empty_vs_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, NULL),(2, ''),(3, 'a'),(4, NULL),(5, '');
SELECT id, typeof(val), val FROM t ORDER BY val, id;
SELECT count(*) FROM t WHERE val IS NULL;
SELECT count(*) FROM t WHERE val = '';
SELECT count(*) FROM t WHERE val IS NOT NULL;
"

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

oracle "cat40_bool_expr_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a > b);
INSERT INTO t VALUES(1,10,5),(2,3,7),(3,5,5),(4,8,2);
SELECT id, a, b, a > b FROM t ORDER BY (a > b), id;
"

oracle "cat40_zeroblob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, zeroblob(0)),(2, zeroblob(4)),(3, X'00000000');
SELECT id, length(data), hex(data) FROM t ORDER BY data, id;
"

echo ""
echo "--- Category 41: Views with indexed base tables ---"

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

oracle "cat41_view_join" "
CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders(id INTEGER PRIMARY KEY, cust_id INT, amount INT);
CREATE INDEX idx ON orders(cust_id);
INSERT INTO customers VALUES(1,'Alice'),(2,'Bob');
INSERT INTO orders VALUES(1,1,150),(2,2,50),(3,1,200);
CREATE VIEW v AS SELECT o.id as oid, c.name, o.amount FROM orders o JOIN customers c ON o.cust_id = c.id;
SELECT * FROM v WHERE amount > 100 ORDER BY amount DESC;
"

oracle "cat41_view_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
CREATE VIEW v AS SELECT grp, sum(val) as total, count(*) as cnt FROM t GROUP BY grp;
SELECT * FROM v ORDER BY grp;
INSERT INTO t VALUES(5,'a',50);
SELECT * FROM v ORDER BY grp;
"

oracle "cat41_view_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
CREATE VIEW high AS SELECT * FROM t WHERE val > 25;
SELECT * FROM t WHERE id IN (SELECT id FROM high) ORDER BY id;
"

oracle "cat41_nested_views" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300),(4,40,400);
CREATE VIEW v1 AS SELECT id, a FROM t WHERE a > 10;
CREATE VIEW v2 AS SELECT * FROM v1 WHERE a < 40;
SELECT * FROM v2 ORDER BY id;
"

echo ""
echo "--- Category 42: JSON functions with indexed tables ---"

oracle "cat42_json_extract" "
CREATE TABLE data(id INTEGER PRIMARY KEY, doc TEXT, status INT);
CREATE INDEX idx ON data(status);
INSERT INTO data VALUES(1,'{\"name\":\"Alice\",\"age\":30}',1);
INSERT INTO data VALUES(2,'{\"name\":\"Bob\",\"age\":25}',1);
INSERT INTO data VALUES(3,'{\"name\":\"Carol\",\"age\":35}',0);
SELECT id, json_extract(doc,'$.name') FROM data WHERE status=1 ORDER BY id;
"

oracle "cat42_json_each" "
CREATE TABLE items(id INTEGER PRIMARY KEY, tags TEXT, type INT);
CREATE INDEX idx ON items(type);
INSERT INTO items VALUES(1,'[\"a\",\"b\",\"c\"]',1);
INSERT INTO items VALUES(2,'[\"x\",\"y\"]',1);
INSERT INTO items VALUES(3,'[\"p\",\"q\"]',2);
SELECT items.id, j.value FROM items, json_each(items.tags) j WHERE items.type=1 ORDER BY items.id, j.key;
"

oracle "cat42_json_where" "
CREATE TABLE data(id INTEGER PRIMARY KEY, doc TEXT);
INSERT INTO data VALUES(1,'{\"score\":85}');
INSERT INTO data VALUES(2,'{\"score\":42}');
INSERT INTO data VALUES(3,'{\"score\":97}');
SELECT id FROM data WHERE json_extract(doc,'$.score') > 80 ORDER BY id;
"

oracle "cat42_json_after_update" "
CREATE TABLE config(id INTEGER PRIMARY KEY, settings TEXT, active INT);
CREATE INDEX idx ON config(active);
INSERT INTO config VALUES(1,'{\"theme\":\"dark\"}',1);
INSERT INTO config VALUES(2,'{\"theme\":\"light\"}',0);
UPDATE config SET settings = json_set(settings,'$.theme','blue') WHERE active=1;
SELECT id, json_extract(settings,'$.theme') FROM config ORDER BY id;
"

echo ""
echo "--- Category 43: Date/time functions with indexes ---"

oracle "cat43_date_where" "
CREATE TABLE events(id INTEGER PRIMARY KEY, event_date TEXT, importance INT);
CREATE INDEX idx ON events(importance);
INSERT INTO events VALUES(1,'2026-01-15',5),(2,'2026-02-10',3),(3,'2026-03-20',5),(4,'2026-04-05',2);
SELECT id FROM events WHERE date(event_date) > date('2026-02-01') AND importance > 2 ORDER BY id;
"

oracle "cat43_date_order" "
CREATE TABLE tasks(id INTEGER PRIMARY KEY, due TEXT, priority INT);
CREATE INDEX idx ON tasks(priority);
INSERT INTO tasks VALUES(1,'2026-03-15',1),(2,'2026-01-10',2),(3,'2026-02-20',1);
SELECT id, due FROM tasks WHERE priority=1 ORDER BY date(due);
"

oracle "cat43_date_arithmetic" "
CREATE TABLE deadlines(id INTEGER PRIMARY KEY, base_date TEXT);
INSERT INTO deadlines VALUES(1,'2026-01-01'),(2,'2026-06-15'),(3,'2026-12-31');
SELECT id, date(base_date,'+30 days') as extended FROM deadlines ORDER BY id;
SELECT id FROM deadlines WHERE date(base_date,'+30 days') > '2026-07-01' ORDER BY id;
"

echo ""
echo "--- Category 44: Math functions with indexes ---"

oracle "cat44_abs" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val REAL);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,-10.5),(2,20.3),(3,-5.0),(4,15.7);
SELECT id, abs(val) as a FROM t ORDER BY a;
SELECT id FROM t WHERE abs(val) > 10 ORDER BY id;
"

oracle "cat44_min_max" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30),(2,10),(3,50),(4,20),(5,40);
SELECT min(val), max(val) FROM t;
UPDATE t SET val = val + 100 WHERE val < 20;
SELECT min(val), max(val) FROM t;
"

oracle "cat44_total_sum" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,NULL),(3,20),(4,NULL),(5,30);
SELECT sum(val), total(val), count(val), count(*) FROM t;
"

oracle "cat44_round" "
CREATE TABLE t(id INTEGER PRIMARY KEY, price REAL, qty INT);
CREATE INDEX idx ON t(qty);
INSERT INTO t VALUES(1,9.99,3),(2,14.50,2),(3,7.25,5);
SELECT id, round(price * qty, 2) as total FROM t ORDER BY total;
SELECT id FROM t WHERE round(price * qty, 2) > 25 ORDER BY id;
"

echo ""
echo "--- Category 45: CAST expressions with indexes ---"

oracle "cat45_cast_text_int" "
CREATE TABLE t(id INTEGER PRIMARY KEY, num_str TEXT, flag INT);
CREATE INDEX idx ON t(flag);
INSERT INTO t VALUES(1,'100',1),(2,'50',1),(3,'200',0),(4,'75',1);
SELECT id FROM t WHERE CAST(num_str AS INTEGER) > 60 AND flag=1 ORDER BY id;
"

oracle "cat45_cast_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'42'),(2,NULL),(3,'99'),(4,'abc');
SELECT id, CAST(val AS INTEGER) FROM t ORDER BY id;
"

oracle "cat45_cast_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount TEXT, grp TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'10','a'),(2,'20','a'),(3,'30','b'),(4,'40','b');
SELECT grp, sum(CAST(amount AS INTEGER)) as total FROM t GROUP BY grp ORDER BY grp;
"

echo ""
echo "--- Category 46: Multi-column UPDATE with multiple indexes ---"

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

oracle "cat46_composite_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT, z INT);
CREATE INDEX idx_xy ON t(x, y);
INSERT INTO t VALUES(1,1,10,100),(2,2,20,200),(3,1,10,300);
UPDATE t SET x = x*2, y = y*2 WHERE z > 150;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE x = 2 ORDER BY id;
PRAGMA integrity_check;
"

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

echo ""
echo "--- Category 47: INSERT INTO SELECT patterns ---"

oracle "cat47_insert_select_transform" "
CREATE TABLE src(id INTEGER PRIMARY KEY, val INT, mult INT);
CREATE TABLE dst(id INTEGER PRIMARY KEY, computed INT);
CREATE INDEX idx_src ON src(mult);
CREATE INDEX idx_dst ON dst(computed);
INSERT INTO src VALUES(1,10,2),(2,20,3),(3,30,1),(4,40,2);
INSERT INTO dst SELECT NULL, val * mult FROM src WHERE mult > 1;
SELECT * FROM dst ORDER BY computed;
"

oracle "cat47_insert_self" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, gen INT DEFAULT 1);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,1),(2,20,1),(3,30,1);
INSERT INTO t SELECT NULL, val + 100, 2 FROM t WHERE gen = 1;
SELECT val, gen FROM t ORDER BY val;
"

oracle "cat47_insert_grouped" "
CREATE TABLE raw(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE TABLE summary(grp TEXT PRIMARY KEY, total INT, cnt INT);
CREATE INDEX idx ON raw(grp);
INSERT INTO raw VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'a',50);
INSERT INTO summary SELECT grp, sum(val), count(*) FROM raw GROUP BY grp;
SELECT * FROM summary ORDER BY grp;
"

echo ""
echo "--- Category 48: CASE WHEN with indexes ---"

oracle "cat48_case_select" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INT, active INT);
CREATE INDEX idx ON t(active);
INSERT INTO t VALUES(1,95,1),(2,45,1),(3,75,0),(4,30,1),(5,85,1);
SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END as grade
FROM t WHERE active=1 ORDER BY score DESC;
"

oracle "cat48_case_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, label TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,''),(2,20,''),(3,30,''),(4,40,'');
UPDATE t SET label = CASE WHEN val > 25 THEN 'high' WHEN val > 15 THEN 'mid' ELSE 'low' END;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE label = 'high' ORDER BY id;
"

oracle "cat48_case_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, type TEXT, val INT);
CREATE INDEX idx ON t(type);
INSERT INTO t VALUES(1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40);
SELECT id, val FROM t WHERE CASE type WHEN 'a' THEN val > 20 WHEN 'b' THEN val > 30 END ORDER BY id;
"

echo ""
echo "--- Category 49: String aggregates ---"

oracle "cat49_group_concat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INT, tag TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,1,'red'),(2,1,'blue'),(3,2,'green'),(4,1,'yellow'),(5,2,'purple');
SELECT grp, GROUP_CONCAT(tag,', ') FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat49_group_concat_distinct" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INT, val TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,1,'a'),(2,1,'b'),(3,1,'a'),(4,2,'c'),(5,2,'c');
SELECT grp, GROUP_CONCAT(DISTINCT val) FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat49_group_concat_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, item TEXT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'x','a'),(2,'x','b'),(3,'y','c');
UPDATE t SET grp = 'x' WHERE id = 3;
SELECT grp, GROUP_CONCAT(item,'+') FROM t GROUP BY grp ORDER BY grp;
"

echo ""
echo "--- Category 50: Multi-statement transactions ---"

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

echo ""
echo "--- Category 51: Unicode and special characters ---"

oracle "cat51_unicode_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'hello'),(2,'café'),(3,'naïve'),(4,'über');
SELECT name FROM t ORDER BY name;
SELECT id FROM t WHERE name = 'café';
"

oracle "cat51_unicode_composite" "
CREATE TABLE t(id INTEGER PRIMARY KEY, city TEXT, code INT);
CREATE INDEX idx ON t(city, code);
INSERT INTO t VALUES(1,'Zürich',1),(2,'München',2),(3,'Zürich',3);
SELECT city, code FROM t WHERE city = 'Zürich' ORDER BY code;
"

oracle "cat51_special_chars" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'line1' || char(10) || 'line2');
INSERT INTO t VALUES(2,'tab' || char(9) || 'separated');
INSERT INTO t VALUES(3,'normal text');
SELECT id, length(val) FROM t ORDER BY id;
"

oracle "cat51_long_text" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, substr(hex(randomblob(500)),1,1000));
INSERT INTO t VALUES(2, 'short');
INSERT INTO t VALUES(3, substr(hex(randomblob(500)),1,1000));
SELECT id, length(val) FROM t ORDER BY length(val);
SELECT count(*) FROM t WHERE length(val) > 100;
"

echo ""
echo "--- Category 52: ROWID edge cases ---"

oracle "cat52_rowid_explicit" "
CREATE TABLE t(a INT, b TEXT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(10,'x'),(20,'y'),(30,'z');
SELECT rowid, a, b FROM t ORDER BY rowid;
SELECT rowid, a FROM t WHERE a > 15 ORDER BY rowid;
"

oracle "cat52_ipk_alias" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(100,10),(200,20),(300,30);
SELECT rowid, id, val FROM t ORDER BY id;
SELECT rowid = id FROM t ORDER BY id;
"

oracle "cat52_negative_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(-5,'a'),(-1,'b'),(0,'c'),(1,'d'),(5,'e');
SELECT id, val FROM t ORDER BY id;
SELECT id FROM t WHERE id < 0 ORDER BY id;
"

oracle "cat52_large_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1000000,1),(2000000,2),(3000000,3);
UPDATE t SET val = val + 100;
SELECT * FROM t ORDER BY id;
"

echo ""
echo "--- Category 53: Complex WHERE clauses ---"

oracle "cat53_nested_and_or" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,1,10,100),(2,2,20,200),(3,1,20,300),(4,2,10,400);
SELECT id FROM t WHERE (a = 1 AND b > 15) OR (a = 2 AND c > 300) ORDER BY id;
"

oracle "cat53_not" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE NOT (val > 20 AND val < 50) ORDER BY id;
"

oracle "cat53_is_isnot" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,NULL),(2,0),(3,1),(4,NULL),(5,0);
SELECT id FROM t WHERE val IS NULL ORDER BY id;
SELECT id FROM t WHERE val IS NOT NULL ORDER BY id;
SELECT id FROM t WHERE val IS 0 ORDER BY id;
"

oracle "cat53_expr_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,5),(2,20,15),(3,30,25),(4,40,35);
SELECT id FROM t WHERE a - b > 5 AND a * b > 200 ORDER BY id;
"

echo ""
echo "--- Category 54: COALESCE / IFNULL / NULLIF ---"

oracle "cat54_coalesce" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,NULL,20,30),(2,10,NULL,30),(3,NULL,NULL,30),(4,10,20,NULL);
SELECT id, COALESCE(a, b, c, 0) as first_val FROM t ORDER BY id;
"

oracle "cat54_ifnull_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, flag INT);
CREATE INDEX idx ON t(flag);
INSERT INTO t VALUES(1,NULL,1),(2,10,1),(3,NULL,0),(4,20,1);
SELECT id FROM t WHERE IFNULL(val, 0) > 5 AND flag = 1 ORDER BY id;
"

oracle "cat54_nullif" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES(1,10,10),(2,20,30),(3,30,30),(4,40,50);
SELECT id, NULLIF(a, b) FROM t ORDER BY id;
SELECT count(*) FROM t WHERE NULLIF(a, b) IS NULL;
"

echo ""
echo "--- Category 55: String functions with indexes ---"

oracle "cat55_length" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,'a'),(2,'bb'),(3,'ccc'),(4,'dddd'),(5,'eeeee');
SELECT id FROM t WHERE length(name) > 3 ORDER BY id;
SELECT id, length(name) FROM t ORDER BY length(name) DESC;
"

oracle "cat55_substr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT);
CREATE INDEX idx ON t(code);
INSERT INTO t VALUES(1,'ABC-001'),(2,'DEF-002'),(3,'ABC-003'),(4,'GHI-004');
SELECT id FROM t WHERE substr(code,1,3) = 'ABC' ORDER BY id;
"

oracle "cat55_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, path TEXT);
CREATE INDEX idx ON t(path);
INSERT INTO t VALUES(1,'/old/file1.txt'),(2,'/old/file2.txt'),(3,'/new/file3.txt');
UPDATE t SET path = replace(path, '/old/', '/new/');
SELECT * FROM t ORDER BY id;
SELECT path FROM t WHERE path LIKE '/new/%' ORDER BY path;
"

oracle "cat55_trim_instr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'  hello  '),(2,'world'),(3,' spaces ');
SELECT id, trim(val), length(trim(val)) FROM t ORDER BY id;
SELECT id FROM t WHERE instr(val, 'o') > 0 ORDER BY id;
"

oracle "cat55_concat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first TEXT, last TEXT);
CREATE INDEX idx ON t(last);
INSERT INTO t VALUES(1,'John','Doe'),(2,'Jane','Smith'),(3,'Bob','Doe');
SELECT first || ' ' || last as full_name FROM t WHERE last = 'Doe' ORDER BY first;
"

echo ""
echo "--- Category 56: Subquery patterns ---"

oracle "cat56_all_any" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id FROM t WHERE val > ALL(SELECT val FROM t WHERE id <= 2) ORDER BY id;
"

oracle "cat56_subquery_having" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100);
SELECT grp, sum(val) as s FROM t GROUP BY grp
HAVING sum(val) > (SELECT avg(val) FROM t) ORDER BY grp;
"

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

oracle "cat56_correlated_exists_mutation" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1),(2,3);
DELETE FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.id);
SELECT * FROM t1 ORDER BY id;
"

oracle "cat56_self_ref_insert_txn" "
CREATE TABLE t(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT, sid TEXT, ver INTEGER,
  UNIQUE(kind, sid, ver)
);
BEGIN;
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
INSERT INTO t(kind,sid,ver) VALUES('thread','t1',
  COALESCE((SELECT ver+1 FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver DESC LIMIT 1), 0));
COMMIT;
SELECT ver FROM t WHERE kind='thread' AND sid='t1' ORDER BY ver;
"

oracle "cat56_self_ref_max_pattern" "
CREATE TABLE events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  data TEXT,
  UNIQUE(aggregate_kind, stream_id, stream_version)
);
BEGIN;
INSERT INTO events(aggregate_kind,stream_id,stream_version,data) VALUES('thread','s1',
  COALESCE((SELECT MAX(stream_version)+1 FROM events WHERE aggregate_kind='thread' AND stream_id='s1'), 0), 'e0');
INSERT INTO events(aggregate_kind,stream_id,stream_version,data) VALUES('thread','s1',
  COALESCE((SELECT MAX(stream_version)+1 FROM events WHERE aggregate_kind='thread' AND stream_id='s1'), 0), 'e1');
INSERT INTO events(aggregate_kind,stream_id,stream_version,data) VALUES('thread','s1',
  COALESCE((SELECT MAX(stream_version)+1 FROM events WHERE aggregate_kind='thread' AND stream_id='s1'), 0), 'e2');
COMMIT;
SELECT stream_version FROM events WHERE aggregate_kind='thread' AND stream_id='s1' ORDER BY stream_version;
"

echo ""
echo "--- Category 57: Index interactions with schema changes ---"

oracle "cat57_late_index" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
CREATE INDEX idx ON t(a);
SELECT id FROM t WHERE a > 15 ORDER BY id;
UPDATE t SET a = a + 100;
SELECT id FROM t WHERE a > 115 ORDER BY id;
PRAGMA integrity_check;
"

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

echo ""
echo "--- Category 58: Edge cases with empty/single results ---"

oracle "cat58_empty_results" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT * FROM t WHERE val > 100;
SELECT count(*) FROM t WHERE val > 100;
SELECT max(val) FROM t WHERE val > 100;
SELECT sum(val) FROM t WHERE val > 100;
"

oracle "cat58_delete_all_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20);
DELETE FROM t;
SELECT count(*) FROM t;
INSERT INTO t VALUES(3,30);
SELECT * FROM t;
"

oracle "cat58_update_no_match" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = 999 WHERE val > 100;
SELECT * FROM t ORDER BY id;
"

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

echo ""
echo "--- Category 59: Implicit type coercion ---"

oracle "cat59_int_text_cmp" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10),(2, '10'),(3, 10.0);
SELECT id, typeof(val) FROM t ORDER BY id;
SELECT id FROM t WHERE val = 10 ORDER BY id;
SELECT id FROM t WHERE val > 5 ORDER BY id;
"

oracle "cat59_numeric_string_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'9'),(2,'10'),(3,'100'),(4,'2'),(5,'20');
SELECT val FROM t ORDER BY val;
SELECT val FROM t ORDER BY CAST(val AS INTEGER);
"

oracle "cat59_bool" "
CREATE TABLE t(id INTEGER PRIMARY KEY, flag INT, val INT);
CREATE INDEX idx ON t(flag);
INSERT INTO t VALUES(1,0,10),(2,1,20),(3,0,30),(4,1,40),(5,NULL,50);
SELECT id FROM t WHERE flag ORDER BY id;
SELECT id FROM t WHERE NOT flag ORDER BY id;
SELECT id FROM t WHERE flag IS NOT NULL AND NOT flag ORDER BY id;
"

echo ""
echo "--- Category 60: WITHOUT ROWID stress patterns ---"

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

oracle "cat60_wr_single_pk_update_all" "
CREATE TABLE t(k TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITHOUT ROWID;
CREATE INDEX idx1 ON t(v1);
INSERT INTO t VALUES('a',10,'x'),('b',20,'y'),('c',30,'z');
UPDATE t SET v1 = v1 * 10, v2 = v2 || '!';
SELECT * FROM t ORDER BY k;
SELECT k FROM t WHERE v1 > 100 ORDER BY k;
PRAGMA integrity_check;
"

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

oracle "cat60_wr_range_after_mutation" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
INSERT INTO t VALUES(1,1,10),(1,2,20),(1,3,30),(2,1,40),(2,2,50);
UPDATE t SET c = c + 1000 WHERE a = 1 AND b >= 2;
DELETE FROM t WHERE a = 2 AND b = 1;
SELECT * FROM t WHERE a = 1 ORDER BY b;
SELECT * FROM t WHERE a >= 1 ORDER BY a, b;
"

echo ""
echo "--- Category 61: LIMIT and OFFSET with indexes ---"

oracle "cat61_limit_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x*10 FROM c;
SELECT val FROM t ORDER BY val LIMIT 5;
SELECT val FROM t ORDER BY val DESC LIMIT 5;
"

oracle "cat61_limit_offset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x*10 FROM c;
SELECT val FROM t ORDER BY val LIMIT 5 OFFSET 5;
SELECT val FROM t ORDER BY val LIMIT 3 OFFSET 17;
"

oracle "cat61_limit_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',40),(5,'b',50),(6,'a',60);
SELECT val FROM t WHERE grp = 'a' ORDER BY val LIMIT 2;
SELECT val FROM t WHERE grp = 'a' ORDER BY val DESC LIMIT 2;
"

oracle "cat61_limit_one" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30),(2,10),(3,50),(4,20),(5,40);
SELECT val FROM t ORDER BY val LIMIT 1;
SELECT val FROM t ORDER BY val DESC LIMIT 1;
SELECT val FROM t WHERE val > 25 ORDER BY val LIMIT 1;
"

oracle "cat61_limit_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
DELETE FROM t WHERE val = 30;
UPDATE t SET val = val + 100 WHERE val >= 40;
SELECT val FROM t ORDER BY val LIMIT 3;
"

echo ""
echo "--- Category 62: Multi-way JOINs ---"

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

oracle "cat62_self_join_three" "
CREATE TABLE nodes(id INTEGER PRIMARY KEY, parent_id INT, name TEXT);
CREATE INDEX idx ON nodes(parent_id);
INSERT INTO nodes VALUES(1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'a1'),(5,2,'a2');
SELECT n1.name as lvl1, n2.name as lvl2, n3.name as lvl3
FROM nodes n1 JOIN nodes n2 ON n1.id = n2.parent_id
JOIN nodes n3 ON n2.id = n3.parent_id
WHERE n1.parent_id IS NULL ORDER BY lvl2, lvl3;
"

echo ""
echo "--- Category 63: Multiple UNIQUE constraints ---"

oracle "cat63_two_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE);
INSERT INTO t VALUES(1,'a@x.com','alice'),(2,'b@x.com','bob');
INSERT OR IGNORE INTO t VALUES(3,'a@x.com','carol');
INSERT OR IGNORE INTO t VALUES(4,'c@x.com','alice');
INSERT INTO t VALUES(5,'c@x.com','carol');
SELECT * FROM t ORDER BY id;
"

oracle "cat63_unique_plus_regular" "
CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT UNIQUE, category INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1,'ABC',1),(2,'DEF',1),(3,'GHI',2);
REPLACE INTO t VALUES(4,'ABC',2);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE category = 1;
SELECT count(*) FROM t WHERE category = 2;
"

oracle "cat63_multi_col_unique" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, val TEXT, UNIQUE(a, b));
INSERT INTO t VALUES(1,1,1,'first');
INSERT INTO t VALUES(2,1,2,'second');
INSERT OR REPLACE INTO t VALUES(3,1,1,'replaced');
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

oracle "cat63_unique_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT UNIQUE, b INT);
INSERT INTO t VALUES(1,NULL,10);
INSERT INTO t VALUES(2,NULL,20);
INSERT INTO t VALUES(3,1,30);
INSERT OR IGNORE INTO t VALUES(4,1,40);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

echo ""
echo "--- Category 64: CHECK constraints with mutations ---"

oracle "cat64_check_basic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT CHECK(val > 0));
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20);
INSERT OR IGNORE INTO t VALUES(3,-5);
INSERT OR IGNORE INTO t VALUES(4,0);
INSERT INTO t VALUES(5,30);
SELECT * FROM t ORDER BY id;
"

oracle "cat64_check_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT CHECK(val BETWEEN 0 AND 100));
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,50),(2,75);
UPDATE OR IGNORE t SET val = 150 WHERE id = 1;
UPDATE t SET val = 90 WHERE id = 1;
SELECT * FROM t ORDER BY id;
"

oracle "cat64_check_multi" "
CREATE TABLE t(id INTEGER PRIMARY KEY, low INT, high INT, CHECK(low <= high));
INSERT INTO t VALUES(1,10,20),(2,5,15);
INSERT OR IGNORE INTO t VALUES(3,30,10);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

echo ""
echo "--- Category 65: DEFAULT values ---"

oracle "cat65_default_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT DEFAULT 42, status TEXT DEFAULT 'new');
CREATE INDEX idx ON t(val);
INSERT INTO t(id) VALUES(1),(2),(3);
INSERT INTO t VALUES(4, 99, 'custom');
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE val = 42;
"

oracle "cat65_default_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, created TEXT DEFAULT (date('now')));
INSERT INTO t(id, val) VALUES(1, 10),(2, 20);
SELECT id, val, typeof(created), length(created) FROM t ORDER BY id;
"

oracle "cat65_default_values" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT DEFAULT 1, b TEXT DEFAULT 'x');
INSERT INTO t DEFAULT VALUES;
INSERT INTO t DEFAULT VALUES;
INSERT INTO t(id, a) VALUES(10, 99);
SELECT * FROM t ORDER BY id;
"

echo ""
echo "--- Category 66: INDEXED BY and NOT INDEXED ---"

oracle "cat66_indexed_by" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300),(4,10,400);
SELECT id FROM t INDEXED BY idx_a WHERE a = 10 ORDER BY id;
"

oracle "cat66_not_indexed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT id FROM t NOT INDEXED WHERE val > 15 ORDER BY id;
"

oracle "cat66_indexed_by_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val + 100;
SELECT val FROM t INDEXED BY idx WHERE val > 110 ORDER BY val;
"

echo ""
echo "--- Category 67: WITHOUT ROWID with 3+ PK columns ---"

oracle "cat67_wr_three_pk" "
CREATE TABLE t(a TEXT, b INT, c INT, d TEXT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,1,'v1'),('x',1,2,'v2'),('x',2,1,'v3');
INSERT INTO t VALUES('y',1,1,'v4');
SELECT * FROM t ORDER BY a, b, c;
UPDATE t SET d = 'updated' WHERE a = 'x' AND b = 1 AND c = 2;
SELECT * FROM t WHERE a = 'x' ORDER BY b, c;
"

oracle "cat67_wr_three_pk_secidx" "
CREATE TABLE t(a INT, b INT, c INT, val TEXT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,1,1,'hello'),(1,1,2,'world'),(1,2,1,'foo');
UPDATE t SET val = 'bar' WHERE a = 1 AND b = 1 AND c = 1;
SELECT * FROM t ORDER BY a, b, c;
SELECT a, b, c FROM t WHERE val = 'bar';
PRAGMA integrity_check;
"

oracle "cat67_wr_three_pk_multi_update" "
CREATE TABLE t(a TEXT, b INT, c INT, d INT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
INSERT INTO t VALUES('x',1,1,10),('x',1,2,20),('x',2,1,30),('x',2,2,40);
UPDATE t SET d = d + 100 WHERE a = 'x' AND b = 1;
SELECT * FROM t ORDER BY a, b, c;
"

oracle "cat67_wr_three_pk_delete" "
CREATE TABLE t(a INT, b INT, c INT, d INT, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
CREATE INDEX idx ON t(d);
INSERT INTO t VALUES(1,1,1,10),(1,1,2,20),(1,2,1,30),(2,1,1,40);
DELETE FROM t WHERE a = 1 AND b = 1;
SELECT * FROM t ORDER BY a, b, c;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 68: Complex ORDER BY ---"

oracle "cat68_order_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,3),(2,20,1),(3,10,2),(4,30,4);
SELECT id, a, b FROM t ORDER BY a, b DESC;
"

oracle "cat68_order_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30),(2,NULL),(3,10),(4,NULL),(5,20);
SELECT id, val FROM t ORDER BY val;
SELECT id, val FROM t ORDER BY val DESC;
"

oracle "cat68_order_hidden_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INT);
CREATE INDEX idx ON t(score);
INSERT INTO t VALUES(1,'a',30),(2,'b',10),(3,'c',50),(4,'d',20);
SELECT name FROM t ORDER BY score;
SELECT name FROM t ORDER BY score DESC;
"

oracle "cat68_order_case" "
CREATE TABLE t(id INTEGER PRIMARY KEY, priority TEXT, val INT);
INSERT INTO t VALUES(1,'low',10),(2,'high',20),(3,'medium',30),(4,'high',40);
SELECT id, priority, val FROM t
ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, val DESC;
"

echo ""
echo "--- Category 69: Deferred FK constraints ---"

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

echo ""
echo "--- Category 70: REPLACE with triggers ---"

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

echo ""
echo "--- Category 71: Complex mutation + SELECT interleaving ---"

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

oracle "cat71_mutation_agg_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
UPDATE t SET val = val * 2 WHERE val > 15;
SELECT sum(val), min(val), max(val) FROM t;
DELETE FROM t WHERE val > 50;
SELECT count(*), sum(val) FROM t;
"

echo ""
echo "--- Category 72: UPSERT advanced patterns ---"

oracle "cat72_upsert_counter" "
CREATE TABLE counts(key TEXT PRIMARY KEY, n INT DEFAULT 0);
INSERT INTO counts VALUES('a', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
INSERT INTO counts VALUES('a', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
INSERT INTO counts VALUES('a', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
INSERT INTO counts VALUES('b', 1) ON CONFLICT(key) DO UPDATE SET n = n + 1;
SELECT * FROM counts ORDER BY key;
"

oracle "cat72_upsert_excluded" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, updated INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10, 0),(2, 20, 0);
INSERT INTO t VALUES(1, 99, 0) ON CONFLICT(id) DO UPDATE SET val = excluded.val, updated = 1;
INSERT INTO t VALUES(3, 30, 0) ON CONFLICT(id) DO UPDATE SET val = excluded.val, updated = 1;
SELECT * FROM t ORDER BY id;
"

oracle "cat72_upsert_wr" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a, b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x',1,100);
INSERT INTO t VALUES('x',1,200) ON CONFLICT(a,b) DO UPDATE SET c = excluded.c;
INSERT INTO t VALUES('x',2,300) ON CONFLICT(a,b) DO UPDATE SET c = excluded.c;
SELECT * FROM t ORDER BY a, b;
SELECT c FROM t ORDER BY c;
"

oracle "cat72_upsert_do_nothing" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, label TEXT);
INSERT INTO t VALUES(1, 10, 'first');
INSERT INTO t VALUES(2, 10, 'second') ON CONFLICT DO NOTHING;
INSERT INTO t VALUES(1, 20, 'third') ON CONFLICT DO NOTHING;
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

echo ""
echo "--- Category 73: typeof() and type inspection ---"

oracle "cat73_typeof" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 42),(2, 3.14),(3, 'hello'),(4, X'BEEF'),(5, NULL);
SELECT id, typeof(val), val FROM t ORDER BY id;
"

oracle "cat73_typeof_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, 10),(2, 'text'),(3, NULL);
UPDATE t SET val = 'now_text' WHERE id = 1;
UPDATE t SET val = 42 WHERE id = 2;
UPDATE t SET val = 3.14 WHERE id = 3;
SELECT id, typeof(val), val FROM t ORDER BY id;
"

oracle "cat73_typeof_where" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val);
INSERT INTO t VALUES(1,42),(2,'hello'),(3,3.14),(4,NULL),(5,X'FF');
SELECT id FROM t WHERE typeof(val) = 'integer' ORDER BY id;
SELECT id FROM t WHERE typeof(val) = 'text' ORDER BY id;
SELECT id FROM t WHERE typeof(val) = 'null' ORDER BY id;
"

echo ""
echo "--- Category 74: hex/unhex and blob operations ---"

oracle "cat74_hex" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, X'DEADBEEF'),(2, X'00FF00'),(3, X'CAFE');
SELECT id, hex(data) FROM t ORDER BY data;
"

oracle "cat74_blob_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, X'0102030405');
SELECT id, length(data), hex(substr(data, 2, 3)) FROM t;
UPDATE t SET data = X'0A0B0C';
SELECT id, hex(data) FROM t;
"

oracle "cat74_randomblob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB);
CREATE INDEX idx ON t(data);
INSERT INTO t VALUES(1, randomblob(8));
INSERT INTO t VALUES(2, randomblob(8));
SELECT count(*) FROM t;
SELECT count(DISTINCT data) FROM t;
SELECT length(data) FROM t ORDER BY id;
"

echo ""
echo "--- Category 75: Aggregate FILTER clause ---"

oracle "cat75_count_filter" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'a',50);
SELECT count(*) FILTER (WHERE grp = 'a') as cnt_a,
       count(*) FILTER (WHERE grp = 'b') as cnt_b FROM t;
"

oracle "cat75_sum_filter" "
CREATE TABLE t(id INTEGER PRIMARY KEY, type TEXT, amount INT);
CREATE INDEX idx ON t(type);
INSERT INTO t VALUES(1,'credit',100),(2,'debit',50),(3,'credit',200),(4,'debit',75);
SELECT sum(amount) FILTER (WHERE type='credit') as credits,
       sum(amount) FILTER (WHERE type='debit') as debits FROM t;
"

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

echo ""
echo "--- Category 76: VALUES as table constructor ---"

oracle "cat76_values_from" "
SELECT * FROM (VALUES(1,'a'),(2,'b'),(3,'c')) ORDER BY column1;
"

oracle "cat76_values_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,'x'),(2,'y'),(3,'z');
SELECT * FROM t ORDER BY id;
"

oracle "cat76_values_in" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40);
SELECT id FROM t WHERE val IN (VALUES(10),(30)) ORDER BY id;
"

echo ""
echo "--- Category 77: Aliased tables and columns ---"

oracle "cat77_table_alias" "
CREATE TABLE very_long_table_name(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON very_long_table_name(val);
INSERT INTO very_long_table_name VALUES(1,10),(2,20),(3,30);
SELECT t.id, t.val FROM very_long_table_name t WHERE t.val > 15 ORDER BY t.id;
"

oracle "cat77_col_alias" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
SELECT doubled FROM (SELECT val * 2 as doubled FROM t) sub WHERE doubled > 30 ORDER BY doubled;
"

oracle "cat77_alias_group" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',5);
SELECT grp, sum(val) as total FROM t GROUP BY grp HAVING total > 20 ORDER BY total;
"

echo ""
echo "--- Category 78: Large batch operations ---"

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

echo ""
echo "--- Category 79: Savepoint + WITHOUT ROWID ---"

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

oracle "cat79_update_inserted_rows_once" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
BEGIN;
INSERT INTO t1 VALUES(1,100),(2,200);
UPDATE t1 SET val = val + 50;
SELECT id, val FROM t1 ORDER BY id;
COMMIT;
"

oracle "cat79_update_inserted_rows_cross_table" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT, data TEXT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(ref);
BEGIN;
INSERT INTO t1 VALUES(1,100),(2,200);
INSERT INTO t2 VALUES(1,1,'a'),(2,2,'b');
UPDATE t1 SET val = val + 50;
SELECT id, val FROM t1 ORDER BY id;
SELECT id, ref, data FROM t2 ORDER BY id;
COMMIT;
"

echo ""
echo "--- Category 80: Integrity checks after complex operations ---"

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

echo ""
echo "--- Category 81: Window function edge cases ---"

oracle "cat81_first_last_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',30),(3,'a',20),(4,'b',40),(5,'b',50);
SELECT id, grp, val,
  first_value(val) OVER (PARTITION BY grp ORDER BY val) as fv,
  last_value(val) OVER (PARTITION BY grp ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lv
FROM t ORDER BY grp, val;
"

oracle "cat81_nth_value" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT id, val,
  nth_value(val, 2) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second,
  nth_value(val, 4) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fourth
FROM t ORDER BY id;
"

oracle "cat81_empty_partition" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20);
SELECT grp, val, sum(val) OVER (PARTITION BY grp) as s,
  count(*) OVER (PARTITION BY grp) as c FROM t ORDER BY grp, val;
"

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

oracle "cat81_range_vs_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(1,10),(2,10),(3,20),(4,20),(5,30);
SELECT id, val,
  count(*) OVER (ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as range_cnt,
  count(*) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rows_cnt
FROM t ORDER BY id;
"

echo ""
echo "--- Category 82: Recursive CTE patterns ---"

oracle "cat82_fibonacci" "
WITH RECURSIVE fib(n, a, b) AS (
  VALUES(1, 0, 1)
  UNION ALL
  SELECT n+1, b, a+b FROM fib WHERE n < 10
)
SELECT n, a FROM fib ORDER BY n;
"

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

oracle "cat82_generate_series" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE s(x) AS (VALUES(0) UNION ALL SELECT x+10 FROM s WHERE x < 50)
INSERT INTO t SELECT x+1, x FROM s;
SELECT * FROM t ORDER BY id;
"

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

echo ""
echo "--- Category 83: Complex aggregate patterns ---"

oracle "cat83_nested_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);
SELECT avg(grp_sum) FROM (SELECT grp, sum(val) as grp_sum FROM t GROUP BY grp);
"

oracle "cat83_group_multi" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INT, val INT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,'x',1,10),(2,'x',1,20),(3,'x',2,30),(4,'y',1,40),(5,'y',2,50);
SELECT a, b, count(*), sum(val) FROM t GROUP BY a, b ORDER BY a, b;
"

oracle "cat83_agg_distinct" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',10),(3,'a',20),(4,'b',30),(5,'b',30);
SELECT grp, count(DISTINCT val), sum(DISTINCT val) FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat83_group_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, ts TEXT, val INT);
INSERT INTO t VALUES(1,'2026-01-15',10),(2,'2026-01-20',20),(3,'2026-02-10',30),(4,'2026-02-25',40);
SELECT substr(ts,1,7) as month, sum(val) FROM t GROUP BY month ORDER BY month;
"

echo ""
echo "--- Category 84: NATURAL JOIN and USING ---"

oracle "cat84_natural_join" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT, val INT);
INSERT INTO t1 VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO t2 VALUES(1,'a',10),(2,'x',20),(3,'c',30);
SELECT * FROM t1 NATURAL JOIN t2 ORDER BY id;
"

oracle "cat84_join_using" "
CREATE TABLE t1(id INT, grp TEXT, val1 INT);
CREATE TABLE t2(id INT, grp TEXT, val2 INT);
CREATE INDEX idx1 ON t1(grp);
CREATE INDEX idx2 ON t2(grp);
INSERT INTO t1 VALUES(1,'a',10),(2,'b',20);
INSERT INTO t2 VALUES(1,'a',100),(2,'b',200),(3,'c',300);
SELECT * FROM t1 JOIN t2 USING(grp) ORDER BY t1.id, t2.id;
"

oracle "cat84_left_using" "
CREATE TABLE t1(grp TEXT, val INT);
CREATE TABLE t2(grp TEXT, data TEXT);
INSERT INTO t1 VALUES('a',10),('b',20),('c',30);
INSERT INTO t2 VALUES('a','x'),('c','z');
SELECT t1.grp, t1.val, t2.data FROM t1 LEFT JOIN t2 USING(grp) ORDER BY t1.grp;
"

echo ""
echo "--- Category 85: CROSS JOIN patterns ---"

oracle "cat85_cross_basic" "
CREATE TABLE t1(a TEXT);
CREATE TABLE t2(b INT);
INSERT INTO t1 VALUES('x'),('y');
INSERT INTO t2 VALUES(1),(2),(3);
SELECT a, b FROM t1 CROSS JOIN t2 ORDER BY a, b;
"

oracle "cat85_cross_where" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20);
INSERT INTO t2 VALUES(1,1),(2,2),(3,1);
SELECT t1.id, t2.id FROM t1, t2 WHERE t1.id = t2.ref ORDER BY t1.id, t2.id;
"

echo ""
echo "--- Category 86: Compound SELECT with mutations ---"

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

oracle "cat86_except_mutation" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx1 ON t1(val);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30),(4,40);
INSERT INTO t2 VALUES(1,20),(2,40);
UPDATE t1 SET val = val + 5 WHERE val > 20;
SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val;
"

oracle "cat86_union_all_limit" "
CREATE TABLE t1(val INT);
CREATE TABLE t2(val INT);
INSERT INTO t1 VALUES(30),(10),(50);
INSERT INTO t2 VALUES(20),(40),(60);
SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val LIMIT 4;
"

echo ""
echo "--- Category 87: DELETE with complex conditions ---"

oracle "cat87_delete_correlated" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE TABLE t2(grp TEXT, min_val INT);
CREATE INDEX idx ON t1(grp);
INSERT INTO t1 VALUES(1,'a',5),(2,'a',15),(3,'b',25),(4,'b',35);
INSERT INTO t2 VALUES('a',10),('b',30);
DELETE FROM t1 WHERE val < (SELECT min_val FROM t2 WHERE t2.grp = t1.grp);
SELECT * FROM t1 ORDER BY id;
"

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

oracle "cat87_delete_bool" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx ON t(a, b);
INSERT INTO t VALUES(1,1,10,'x'),(2,1,20,'y'),(3,2,10,'z'),(4,2,20,'w'),(5,3,30,'v');
DELETE FROM t WHERE (a = 1 AND b > 15) OR (a = 2 AND c = 'z');
SELECT * FROM t ORDER BY id;
"

echo ""
echo "--- Category 88: INSERT edge cases ---"

oracle "cat88_insert_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1, NULL, NULL);
INSERT INTO t VALUES(2, 10, NULL);
INSERT INTO t VALUES(3, NULL, 'hello');
SELECT id, a, b FROM t ORDER BY id;
SELECT count(*) FROM t WHERE a IS NULL;
"

oracle "cat88_insert_select_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1, (SELECT 10 + 20));
INSERT INTO t VALUES(2, (SELECT max(val) + 1 FROM t));
INSERT INTO t VALUES(3, (SELECT count(*) FROM t));
SELECT * FROM t ORDER BY id;
"

oracle "cat88_insert_many" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100);
SELECT count(*) FROM t;
SELECT sum(val) FROM t;
SELECT min(val), max(val) FROM t;
"

oracle "cat88_insert_abort" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE);
INSERT INTO t VALUES(1, 10),(2, 20);
INSERT OR ABORT INTO t VALUES(3, 10);
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
"

oracle "cat88_insert_sorted_explicit_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
BEGIN;
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
INSERT OR IGNORE INTO t VALUES(2,'dup');
INSERT INTO t VALUES(5,'e');
INSERT INTO t VALUES(4,'d');
COMMIT;
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

oracle "cat88_insert_append_existing_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
BEGIN;
INSERT INTO t VALUES(4,'d');
INSERT INTO t VALUES(5,'e');
INSERT OR IGNORE INTO t VALUES(4,'dup');
INSERT INTO t VALUES(7,'g');
INSERT INTO t VALUES(6,'f');
COMMIT;
SELECT id, val FROM t ORDER BY id;
SELECT count(*) FROM t;
"

echo ""
echo "--- Category 89: UPDATE with complex SET expressions ---"

oracle "cat89_set_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE lookup(id INT, bonus INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
INSERT INTO lookup VALUES(1,100),(3,300);
UPDATE t SET val = val + COALESCE((SELECT bonus FROM lookup WHERE lookup.id = t.id), 0);
SELECT * FROM t ORDER BY id;
"

oracle "cat89_set_swap" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
INSERT INTO t VALUES(1,10,100),(2,20,200);
UPDATE t SET a = b, b = a;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

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

oracle "cat89_set_arithmetic" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,10,20,30),(2,40,50,60);
UPDATE t SET a = a + b + c, b = b * 2, c = a;
SELECT * FROM t ORDER BY id;
"

echo ""
echo "--- Category 90: Index + trigger interactions ---"

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

echo ""
echo "--- Category 91: Covering index edge cases ---"

oracle "cat91_full_cover" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, c TEXT);
CREATE INDEX idx ON t(a, b, c);
INSERT INTO t VALUES(1,10,100,'x'),(2,20,200,'y'),(3,10,300,'z');
SELECT a, b, c FROM t WHERE a = 10 ORDER BY b;
SELECT a, b FROM t WHERE a > 10 AND b > 100 ORDER BY a;
"

oracle "cat91_cover_after_mutation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, key INT, data TEXT);
CREATE INDEX idx ON t(key, data);
INSERT INTO t VALUES(1,10,'old'),(2,20,'old'),(3,30,'old');
UPDATE t SET data = 'new' WHERE key > 15;
SELECT key, data FROM t ORDER BY key;
"

oracle "cat91_count_cover" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a);
INSERT INTO t VALUES(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50);
SELECT a, count(*) FROM t GROUP BY a ORDER BY a;
SELECT count(*) FROM t WHERE a = 1;
"

echo ""
echo "--- Category 92: Mixed WR and regular table ops ---"

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

oracle "cat92_insert_wr_to_regular" "
CREATE TABLE wr(a TEXT, b INT, c INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE TABLE regular(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON regular(val);
INSERT INTO wr VALUES('x',1,100),('x',2,200),('y',1,300);
INSERT INTO regular SELECT NULL, c FROM wr WHERE a = 'x';
SELECT * FROM regular ORDER BY val;
"

oracle "cat92_update_from_wr" "
CREATE TABLE config(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE TABLE data(id INTEGER PRIMARY KEY, val INT, multiplied INT DEFAULT 0);
CREATE INDEX idx ON data(val);
INSERT INTO config VALUES('factor',10);
INSERT INTO data VALUES(1,5,0),(2,10,0),(3,15,0);
UPDATE data SET multiplied = val * (SELECT v FROM config WHERE k = 'factor');
SELECT * FROM data ORDER BY id;
"

echo ""
echo "--- Category 93: Edge case data patterns ---"

oracle "cat93_all_same" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,42),(2,42),(3,42),(4,42),(5,42);
SELECT count(*) FROM t WHERE val = 42;
SELECT min(id), max(id) FROM t WHERE val = 42;
UPDATE t SET val = 99 WHERE id = 3;
SELECT count(*) FROM t WHERE val = 42;
"

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

oracle "cat93_sparse_ids" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(1000,20),(1000000,30);
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE val > 15 ORDER BY id;
UPDATE t SET val = val + 100;
SELECT * FROM t ORDER BY id;
"

oracle "cat93_decreasing_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(5,50),(4,40),(3,30),(2,20),(1,10);
SELECT * FROM t ORDER BY id;
SELECT val FROM t ORDER BY val;
"

echo ""
echo "--- Category 94: Complex HAVING patterns ---"

oracle "cat94_having_multi" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',5),(5,'b',15),(6,'c',100);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp
HAVING count(*) >= 2 AND sum(val) > 15 ORDER BY grp;
"

oracle "cat94_having_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);
SELECT grp, sum(val) as s FROM t GROUP BY grp
HAVING sum(val) > (SELECT avg(val) FROM t) ORDER BY grp;
"

oracle "cat94_having_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category INT, amount INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1,1,100),(2,1,200),(3,2,50),(4,2,75),(5,3,500);
SELECT category, sum(amount) as total, count(*) as cnt FROM t
GROUP BY category HAVING total / cnt > 100 ORDER BY category;
"

echo ""
echo "--- Category 95: Complex INSERT patterns ---"

oracle "cat95_insert_returning_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30) RETURNING sum(val) OVER () as total;
"

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

oracle "cat95_insert_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, hash TEXT, len INT);
INSERT INTO t VALUES(1, hex(randomblob(4)), 4);
INSERT INTO t VALUES(2, hex(randomblob(8)), 8);
INSERT INTO t VALUES(3, hex(randomblob(16)), 16);
SELECT id, length(hash)/2 as bytes, len FROM t ORDER BY id;
"

echo ""
echo "--- Category 96: Multiple secondary indexes on WR ---"

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

oracle "cat96_wr_delete_multi_secidx" "
CREATE TABLE t(k INT PRIMARY KEY, a INT, b TEXT, c REAL) WITHOUT ROWID;
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
INSERT INTO t VALUES(1,10,'x',1.1),(2,20,'y',2.2),(3,30,'z',3.3),(4,40,'w',4.4);
DELETE FROM t WHERE k IN (2, 4);
SELECT * FROM t ORDER BY k;
PRAGMA integrity_check;
"

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

echo ""
echo "--- Category 97: Chained operations on same row ---"

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

oracle "cat97_replace_chain" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, ver INT);
CREATE INDEX idx ON t(val);
REPLACE INTO t VALUES(1, 10, 1);
REPLACE INTO t VALUES(1, 20, 2);
REPLACE INTO t VALUES(1, 30, 3);
SELECT * FROM t;
SELECT count(*) FROM t;
"

echo ""
echo "--- Category 98: Extreme value edge cases ---"

oracle "cat98_zeros" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,0),(2,0),(3,0);
SELECT count(*) FROM t WHERE val = 0;
UPDATE t SET val = id WHERE val = 0;
SELECT * FROM t ORDER BY val;
"

oracle "cat98_negatives" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,-100),(2,-50),(3,0),(4,50),(5,100);
SELECT val FROM t ORDER BY val;
SELECT val FROM t WHERE val < 0 ORDER BY val;
SELECT val FROM t WHERE val BETWEEN -75 AND 75 ORDER BY val;
"

oracle "cat98_mixed_sign" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,-10),(3,20),(4,-20),(5,0);
UPDATE t SET val = -val;
SELECT val FROM t ORDER BY val;
DELETE FROM t WHERE val > 0;
SELECT val FROM t ORDER BY val;
"

oracle "cat98_empty_string_ops" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx ON t(name);
INSERT INTO t VALUES(1,''),(2,'a'),(3,''),(4,'b');
SELECT id FROM t WHERE name = '' ORDER BY id;
UPDATE t SET name = 'filled' WHERE name = '';
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t WHERE name = '';
"

echo ""
echo "--- Category 99: Complex trigger + savepoint ---"

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

echo ""
echo "--- Category 100: Grand finale - complex patterns ---"

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

echo ""
echo "--- Category 101: WITHOUT ROWID + window functions ---"

oracle "cat101_wr_window_basic" "
CREATE TABLE t(a TEXT, b INT, c INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
INSERT INTO t VALUES('x',1,10),('x',2,20),('x',3,30),('y',1,40),('y',2,50);
UPDATE t SET c = c + 100 WHERE a = 'x';
SELECT a, b, c, sum(c) OVER (PARTITION BY a ORDER BY b) as running
FROM t ORDER BY a, b;
"

oracle "cat101_wr_window_rank" "
CREATE TABLE t(dept TEXT, emp TEXT, salary INT, PRIMARY KEY(dept, emp)) WITHOUT ROWID;
INSERT INTO t VALUES('eng','Alice',120),('eng','Bob',100),('eng','Carol',110);
INSERT INTO t VALUES('sales','Dave',90),('sales','Eve',95);
UPDATE t SET salary = salary + 10 WHERE dept = 'eng';
SELECT dept, emp, salary, rank() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk
FROM t ORDER BY dept, rnk;
"

echo ""
echo "--- Category 102: Partial index + UPSERT combo ---"

oracle "cat102_partial_upsert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, active INT);
CREATE INDEX idx ON t(val) WHERE active = 1;
INSERT INTO t VALUES(1,10,1),(2,20,0),(3,30,1);
INSERT INTO t VALUES(1,99,1) ON CONFLICT(id) DO UPDATE SET val = excluded.val;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE active = 1 AND val > 25 ORDER BY id;
PRAGMA integrity_check;
"

oracle "cat102_partial_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, status TEXT);
CREATE INDEX idx ON t(val) WHERE status != 'deleted';
INSERT INTO t VALUES(1,10,'active'),(2,20,'active'),(3,30,'deleted');
REPLACE INTO t VALUES(1,15,'active');
UPDATE t SET status = 'deleted' WHERE id = 2;
SELECT id, val FROM t WHERE status != 'deleted' ORDER BY val;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 103: Expression index + mutations ---"

oracle "cat103_expr_idx_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, first TEXT, last TEXT);
CREATE INDEX idx ON t(lower(last), lower(first));
INSERT INTO t VALUES(1,'Alice','SMITH'),(2,'Bob','JONES'),(3,'Carol','SMITH');
UPDATE t SET last = 'Brown' WHERE id = 2;
SELECT id, first, last FROM t WHERE lower(last) = 'smith' ORDER BY lower(first);
PRAGMA integrity_check;
"

oracle "cat103_expr_idx_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(abs(val));
INSERT INTO t VALUES(1,-30),(2,20),(3,-10),(4,40);
DELETE FROM t WHERE abs(val) > 25;
SELECT * FROM t ORDER BY abs(val);
PRAGMA integrity_check;
"

oracle "cat103_expr_idx_complex" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
CREATE INDEX idx ON t(a + b, a - b);
INSERT INTO t VALUES(1,10,5),(2,3,12),(3,8,8),(4,1,1);
SELECT id, a+b, a-b FROM t WHERE a+b > 10 ORDER BY a+b;
UPDATE t SET a = a + 10;
SELECT id, a+b, a-b FROM t ORDER BY a+b;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 104: WITHOUT ROWID + UNION/INTERSECT ---"

oracle "cat104_wr_union" "
CREATE TABLE t1(a TEXT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE TABLE t2(a TEXT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
INSERT INTO t1 VALUES('x',1),('x',2),('y',1);
INSERT INTO t2 VALUES('x',2),('y',1),('z',1);
SELECT a, b FROM t1 UNION SELECT a, b FROM t2 ORDER BY a, b;
SELECT a, b FROM t1 INTERSECT SELECT a, b FROM t2 ORDER BY a, b;
SELECT a, b FROM t1 EXCEPT SELECT a, b FROM t2 ORDER BY a, b;
"

oracle "cat104_wr_union_after_mutation" "
CREATE TABLE t1(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
CREATE TABLE t2(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t1 VALUES('a',1),('b',2),('c',3);
INSERT INTO t2 VALUES('b',20),('c',30),('d',40);
UPDATE t1 SET v = v * 10;
DELETE FROM t2 WHERE k = 'd';
SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2 ORDER BY k, v;
"

echo ""
echo "--- Category 105: Generated columns + mutations ---"

oracle "cat105_gen_col_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT, sum_ab INT GENERATED ALWAYS AS (a+b) STORED);
CREATE INDEX idx ON t(sum_ab);
INSERT INTO t(id,a,b) VALUES(1,10,20),(2,30,40),(3,50,60);
UPDATE t SET a = a + 100 WHERE id <= 2;
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE sum_ab > 100 ORDER BY sum_ab;
PRAGMA integrity_check;
"

oracle "cat105_gen_col_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, price INT, qty INT, total INT GENERATED ALWAYS AS (price*qty) STORED);
CREATE INDEX idx ON t(total);
INSERT INTO t(id,price,qty) VALUES(1,10,5),(2,20,3),(3,5,10);
DELETE FROM t WHERE total < 55;
SELECT * FROM t ORDER BY id;
PRAGMA integrity_check;
"

oracle "cat105_gen_col_replace" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, doubled INT GENERATED ALWAYS AS (val*2) STORED);
CREATE INDEX idx ON t(doubled);
INSERT INTO t(id,val) VALUES(1,10),(2,20);
REPLACE INTO t(id,val) VALUES(1,50);
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE doubled > 30 ORDER BY id;
"

echo ""
echo "--- Category 106: CTE + INSERT + trigger combo ---"

oracle "cat106_cte_insert_trigger" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE log(msg TEXT);
CREATE INDEX idx ON t(val);
CREATE TRIGGER trg AFTER INSERT ON t BEGIN
  INSERT INTO log VALUES('inserted ' || NEW.id);
END;
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<5)
INSERT INTO t SELECT x, x*10 FROM r;
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM log;
"

oracle "cat106_cte_update_verify" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, grp TEXT);
CREATE INDEX idx ON t(grp, val);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<10)
INSERT INTO t SELECT x, x*10, CASE WHEN x%2=0 THEN 'even' ELSE 'odd' END FROM r;
WITH targets AS (SELECT id FROM t WHERE grp = 'even' AND val > 50)
UPDATE t SET val = val + 1000 WHERE id IN (SELECT id FROM targets);
SELECT grp, count(*), sum(val) FROM t GROUP BY grp ORDER BY grp;
"

echo ""
echo "--- Category 107: REPLACE + FK interaction ---"

oracle "cat107_replace_fk_cascade" "
CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE, val INT);
CREATE INDEX idx ON child(pid);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1,'a'),(2,'b');
INSERT INTO child VALUES(1,1,10),(2,1,20),(3,2,30);
REPLACE INTO parent VALUES(1,'a_replaced');
SELECT * FROM parent ORDER BY id;
SELECT * FROM child ORDER BY id;
"

oracle "cat107_replace_fk_set_null" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL);
CREATE INDEX idx ON child(pid);
PRAGMA foreign_keys = ON;
INSERT INTO parent VALUES(1),(2);
INSERT INTO child VALUES(1,1),(2,2),(3,1);
REPLACE INTO parent VALUES(1);
SELECT * FROM child ORDER BY id;
"

echo ""
echo "--- Category 108: Deeply nested subqueries ---"

oracle "cat108_triple_nested" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
SELECT * FROM t WHERE val > (
  SELECT avg(val) FROM t WHERE val > (
    SELECT min(val) FROM t
  )
) ORDER BY id;
"

oracle "cat108_nested_exists" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT);
CREATE TABLE t3(id INTEGER PRIMARY KEY, ref INT);
CREATE INDEX idx2 ON t2(ref);
CREATE INDEX idx3 ON t3(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1),(2,2);
INSERT INTO t3 VALUES(1,1);
SELECT * FROM t1 WHERE EXISTS (
  SELECT 1 FROM t2 WHERE t2.ref = t1.id AND EXISTS (
    SELECT 1 FROM t3 WHERE t3.ref = t2.id
  )
) ORDER BY t1.id;
"

oracle "cat108_scalar_in_scalar" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
SELECT id, val, val - (SELECT avg(val) FROM t t2 WHERE t2.grp = t.grp) as diff
FROM t ORDER BY id;
"

echo ""
echo "--- Category 109: Window functions after mutations ---"

oracle "cat109_window_after_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',40),(5,'b',50);
DELETE FROM t WHERE val = 20;
SELECT grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) as rn
FROM t ORDER BY grp, rn;
"

oracle "cat109_window_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,50),(2,10),(3,40),(4,20),(5,30);
UPDATE t SET val = val + 100 WHERE val > 30;
SELECT id, val, rank() OVER (ORDER BY val) as rnk FROM t ORDER BY rnk;
"

oracle "cat109_running_sum_after_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, amount INT);
INSERT INTO t VALUES(1,100),(2,200),(3,300);
INSERT INTO t VALUES(4,50),(5,150);
SELECT id, amount, sum(amount) OVER (ORDER BY id) as running FROM t ORDER BY id;
"

echo ""
echo "--- Category 110: Multi-table JOIN + mutation ---"

oracle "cat110_join_after_cascade_delete" "
CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE, val INT);
CREATE INDEX idx ON c(pid);
PRAGMA foreign_keys = ON;
INSERT INTO p VALUES(1,'a'),(2,'b'),(3,'c');
INSERT INTO c VALUES(1,1,10),(2,1,20),(3,2,30),(4,3,40);
DELETE FROM p WHERE id = 1;
SELECT p.name, c.val FROM p JOIN c ON p.id = c.pid ORDER BY p.name, c.val;
"

oracle "cat110_join_after_update_both" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, ref INT, data TEXT);
CREATE INDEX idx1 ON t1(val);
CREATE INDEX idx2 ON t2(ref);
INSERT INTO t1 VALUES(1,10),(2,20),(3,30);
INSERT INTO t2 VALUES(1,1,'a'),(2,2,'b'),(3,3,'c');
UPDATE t1 SET val = val * 10;
UPDATE t2 SET data = upper(data);
SELECT t1.val, t2.data FROM t1 JOIN t2 ON t1.id = t2.ref ORDER BY t1.id;
"

echo ""
echo "--- Category 111: LIMIT + OFFSET + mutation ---"

oracle "cat111_paginate_after_delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<30)
INSERT INTO t SELECT x, x*10 FROM c;
DELETE FROM t WHERE id % 3 = 0;
SELECT val FROM t ORDER BY val LIMIT 5 OFFSET 0;
SELECT val FROM t ORDER BY val LIMIT 5 OFFSET 5;
SELECT val FROM t ORDER BY val LIMIT 5 OFFSET 10;
"

oracle "cat111_paginate_after_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, score INT);
CREATE INDEX idx ON t(score);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET score = score + 100 WHERE id <= 10;
SELECT score FROM t ORDER BY score DESC LIMIT 5;
SELECT score FROM t ORDER BY score DESC LIMIT 5 OFFSET 5;
"

echo ""
echo "--- Category 112: Self-referential UPDATE ---"

oracle "cat112_self_ref_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, prev_val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0);
UPDATE t SET prev_val = val, val = val * 2;
SELECT * FROM t ORDER BY id;
"

oracle "cat112_update_from_self_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT, grp_avg INT DEFAULT 0);
CREATE INDEX idx ON t(grp);
INSERT INTO t VALUES(1,'a',10,0),(2,'a',20,0),(3,'b',30,0),(4,'b',40,0);
UPDATE t SET grp_avg = (SELECT avg(val) FROM t t2 WHERE t2.grp = t.grp);
SELECT * FROM t ORDER BY id;
"

oracle "cat112_update_rank" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, rnk INT DEFAULT 0);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,30,0),(2,10,0),(3,50,0),(4,20,0);
UPDATE t SET rnk = (SELECT count(*) FROM t t2 WHERE t2.val <= t.val);
SELECT * FROM t ORDER BY rnk;
"

echo ""
echo "--- Category 113: Index rebuild stress ---"

oracle "cat113_drop_create_cycle" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES(1,10,100),(2,20,200),(3,30,300);
CREATE INDEX idx ON t(a);
SELECT id FROM t WHERE a > 15 ORDER BY id;
DROP INDEX idx;
UPDATE t SET a = a + 1;
CREATE INDEX idx ON t(a);
SELECT id FROM t WHERE a > 20 ORDER BY id;
DROP INDEX idx;
DELETE FROM t WHERE id = 2;
CREATE INDEX idx ON t(a);
SELECT * FROM t ORDER BY a;
PRAGMA integrity_check;
"

oracle "cat113_reindex" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
INSERT INTO t SELECT x, x FROM c;
UPDATE t SET val = 1000 - val;
REINDEX idx;
SELECT val FROM t ORDER BY val LIMIT 3;
SELECT val FROM t ORDER BY val DESC LIMIT 3;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 114: Adversarial mutation patterns ---"

oracle "cat114_update_all_then_some" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40),(5,50);
UPDATE t SET val = 0;
UPDATE t SET val = id * 100 WHERE id IN (2,4);
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE val > 0 ORDER BY val;
"

oracle "cat114_delete_insert_same_vals" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10),(2,20),(3,30);
DELETE FROM t WHERE val = 20;
INSERT INTO t VALUES(4,20);
SELECT id, val FROM t WHERE val = 20;
SELECT * FROM t ORDER BY val;
"

oracle "cat114_replace_all_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT UNIQUE, label TEXT);
INSERT INTO t VALUES(1,10,'a'),(2,20,'b'),(3,30,'c');
REPLACE INTO t VALUES(1,10,'a2');
REPLACE INTO t VALUES(2,20,'b2');
REPLACE INTO t VALUES(3,30,'c2');
SELECT * FROM t ORDER BY id;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

oracle "cat114_zigzag_update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4),(5,5);
UPDATE t SET val = val + 10 WHERE id % 2 = 1;
UPDATE t SET val = val - 5 WHERE id % 2 = 0;
UPDATE t SET val = val * 2;
SELECT * FROM t ORDER BY val;
"

echo ""
echo "--- Category 115: WITHOUT ROWID + trigger + index ---"

oracle "cat115_wr_trigger_update" "
CREATE TABLE t(k TEXT PRIMARY KEY, v INT, version INT DEFAULT 0) WITHOUT ROWID;
CREATE TABLE audit(k TEXT, old_v INT, new_v INT);
CREATE INDEX idx ON t(v);
CREATE TRIGGER trg AFTER UPDATE ON t BEGIN
  INSERT INTO audit VALUES(NEW.k, OLD.v, NEW.v);
END;
INSERT INTO t VALUES('a',10,0),('b',20,0),('c',30,0);
UPDATE t SET v = v + 100, version = version + 1;
SELECT * FROM t ORDER BY k;
SELECT * FROM audit ORDER BY k;
"

oracle "cat115_wr_trigger_delete" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE TABLE deleted_log(a INT, b INT, c INT);
CREATE INDEX idx ON t(c);
CREATE TRIGGER trg BEFORE DELETE ON t BEGIN
  INSERT INTO deleted_log VALUES(OLD.a, OLD.b, OLD.c);
END;
INSERT INTO t VALUES(1,1,100),(1,2,200),(2,1,300);
DELETE FROM t WHERE c > 150;
SELECT * FROM t ORDER BY a, b;
SELECT * FROM deleted_log ORDER BY a, b;
"

echo ""
echo "--- Category 116: Complex GROUP BY + ORDER BY ---"

oracle "cat116_group_order_limit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, category TEXT, val INT);
CREATE INDEX idx ON t(category);
INSERT INTO t VALUES(1,'a',10),(2,'b',50),(3,'a',30),(4,'c',20),(5,'b',40),(6,'a',60);
SELECT category, sum(val) as total FROM t GROUP BY category ORDER BY total DESC LIMIT 2;
"

oracle "cat116_group_multi_agg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INT);
CREATE INDEX idx ON t(grp, val);
INSERT INTO t VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'b',5),(5,'b',15);
SELECT grp, count(*), min(val), max(val), sum(val),
  max(val) - min(val) as range FROM t GROUP BY grp ORDER BY grp;
"

oracle "cat116_group_having_order" "
CREATE TABLE t(id INTEGER PRIMARY KEY, dept TEXT, salary INT);
CREATE INDEX idx ON t(dept);
INSERT INTO t VALUES(1,'eng',100),(2,'eng',120),(3,'eng',110);
INSERT INTO t VALUES(4,'sales',80),(5,'sales',90),(6,'hr',200);
SELECT dept, avg(salary) as avg_sal, count(*) as cnt FROM t
GROUP BY dept HAVING cnt > 1 ORDER BY avg_sal DESC;
"

echo ""
echo "--- Category 117: Complex RETURNING patterns ---"

oracle "cat117_update_returning_expr" "
CREATE TABLE t(id INTEGER PRIMARY KEY, price INT, qty INT);
CREATE INDEX idx ON t(price);
INSERT INTO t VALUES(1,10,5),(2,20,3),(3,30,2);
UPDATE t SET price = price + 5 WHERE qty > 2 RETURNING id, price * qty as total;
"

oracle "cat117_delete_returning_join_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, label TEXT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10,'keep'),(2,20,'remove'),(3,30,'keep'),(4,40,'remove');
DELETE FROM t WHERE label = 'remove' RETURNING id, val;
"

oracle "cat117_upsert_returning" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT, ver INT DEFAULT 1);
INSERT INTO t VALUES(1, 10, 1);
INSERT INTO t VALUES(1, 20, 1)
  ON CONFLICT(id) DO UPDATE SET val = excluded.val, ver = ver + 1
  RETURNING id, val, ver;
"

echo ""
echo "--- Category 118: Large WITHOUT ROWID operations ---"

oracle "cat118_wr_large_multi_update" "
CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE INDEX idx ON t(c);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<200)
INSERT INTO t SELECT x/20, x, x*3 FROM r;
UPDATE t SET c = c + 10000 WHERE a = 5;
SELECT count(*) FROM t WHERE c > 10000;
SELECT count(*) FROM t WHERE c < 10000;
PRAGMA integrity_check;
"

oracle "cat118_wr_large_delete" "
CREATE TABLE t(k INT PRIMARY KEY, v INT, tag TEXT) WITHOUT ROWID;
CREATE INDEX idx_v ON t(v);
CREATE INDEX idx_tag ON t(tag);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<300)
INSERT INTO t SELECT x, x*7, CASE WHEN x%3=0 THEN 'del' ELSE 'keep' END FROM r;
DELETE FROM t WHERE tag = 'del';
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE tag = 'keep';
PRAGMA integrity_check;
"

echo ""
echo "--- Category 119: Multi-index mutation verification ---"

oracle "cat119_verify_all_indexes" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b TEXT, c REAL);
CREATE INDEX idx_a ON t(a);
CREATE INDEX idx_b ON t(b);
CREATE INDEX idx_c ON t(c);
CREATE INDEX idx_ab ON t(a, b);
CREATE INDEX idx_bc ON t(b, c);
INSERT INTO t VALUES(1,10,'x',1.5),(2,20,'y',2.5),(3,30,'z',3.5);
UPDATE t SET a = a+100, b = b||'!', c = c*10 WHERE id = 2;
DELETE FROM t WHERE id = 3;
INSERT INTO t VALUES(4,40,'w',4.5);
SELECT * FROM t ORDER BY id;
SELECT id FROM t WHERE a = 120;
SELECT id FROM t WHERE b = 'y!';
SELECT id FROM t WHERE c = 25.0;
SELECT id FROM t WHERE a > 30 AND b < 'z' ORDER BY a;
PRAGMA integrity_check;
"

oracle "cat119_verify_after_batch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x INT, y INT);
CREATE INDEX idx_x ON t(x);
CREATE INDEX idx_y ON t(y);
CREATE INDEX idx_xy ON t(x, y);
WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<50)
INSERT INTO t SELECT n, n%7, n%11 FROM r;
UPDATE t SET x = x + 100 WHERE y = 0;
DELETE FROM t WHERE x = 3;
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE x > 100;
SELECT count(*) FROM t WHERE y = 0;
PRAGMA integrity_check;
"

echo ""
echo "--- Category 120: Stress combinations ---"

oracle "cat120_wr_multiop_secidx_integrity" "
CREATE TABLE t(a TEXT, b INT, c INT, d TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
CREATE INDEX idx_c ON t(c);
CREATE INDEX idx_d ON t(d);
INSERT INTO t VALUES('p',1,10,'alpha'),('p',2,20,'beta'),('p',3,30,'gamma');
INSERT INTO t VALUES('q',1,40,'delta'),('q',2,50,'epsilon');
UPDATE t SET c = c + 1000 WHERE a = 'p';
DELETE FROM t WHERE d = 'delta';
INSERT INTO t VALUES('r',1,60,'zeta');
UPDATE t SET d = d || '_v2' WHERE b = 1;
REPLACE INTO t VALUES('p',2,99,'replaced');
SELECT * FROM t ORDER BY a, b;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

oracle "cat120_rapid_pk_reuse" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,10);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1,20);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1,30);
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES(1,40);
SELECT * FROM t;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

oracle "cat120_many_small_updates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
CREATE INDEX idx ON t(val);
INSERT INTO t VALUES(1,0);
UPDATE t SET val = 1 WHERE id = 1;
UPDATE t SET val = 2 WHERE id = 1;
UPDATE t SET val = 3 WHERE id = 1;
UPDATE t SET val = 4 WHERE id = 1;
UPDATE t SET val = 5 WHERE id = 1;
UPDATE t SET val = 6 WHERE id = 1;
UPDATE t SET val = 7 WHERE id = 1;
UPDATE t SET val = 8 WHERE id = 1;
UPDATE t SET val = 9 WHERE id = 1;
UPDATE t SET val = 10 WHERE id = 1;
SELECT * FROM t;
SELECT count(*) FROM t;
PRAGMA integrity_check;
"

oracle "cat120_full_lifecycle" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score INT, active INT DEFAULT 1);
CREATE INDEX idx_name ON t(name);
CREATE INDEX idx_score ON t(score);
CREATE INDEX idx_active ON t(active);
WITH RECURSIVE r(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM r WHERE x<50)
INSERT INTO t SELECT x, 'user_' || x, (x * 7) % 100, 1 FROM r;
UPDATE t SET score = score + 50 WHERE score < 30;
UPDATE t SET active = 0 WHERE score > 80;
DELETE FROM t WHERE active = 0 AND score > 90;
INSERT INTO t VALUES(100, 'admin', 99, 1);
REPLACE INTO t VALUES(1, 'superuser', 100, 1);
SELECT count(*) FROM t;
SELECT count(*) FROM t WHERE active = 1;
SELECT name FROM t WHERE score = 100 ORDER BY name;
SELECT min(score), max(score), sum(score) FROM t WHERE active = 1;
PRAGMA integrity_check;
"

oracle "cat120_count_not_null_column" "
CREATE TABLE t(k INTEGER NOT NULL, v INTEGER);
INSERT INTO t VALUES (1,NULL),(2,7),(3,NULL);
SELECT count(k), count(*), count(v) FROM t;
CREATE TABLE a(k INTEGER NOT NULL);
CREATE TABLE b(k INTEGER NOT NULL);
INSERT INTO a VALUES(1),(2),(3);
INSERT INTO b VALUES(1),(3);
SELECT count(b.k), count(*) FROM a LEFT JOIN b USING(k);
"

echo ""
echo "--- Category 121: Defensively-walled internal paths ---"

oracle "cat121_drop_sqlite_master_rejected" "
CREATE TABLE t(x INT);
DROP TABLE sqlite_master;
"

oracle "cat121_drop_sqlite_master_writable_schema_rejected" "
PRAGMA writable_schema = 1;
CREATE TABLE t(x INT);
DROP TABLE sqlite_master;
"

oracle_unsafe "cat121_writable_schema_delete_master" "
CREATE TABLE keep(x INT);
INSERT INTO keep VALUES(1);
PRAGMA writable_schema = 1;
DELETE FROM sqlite_master WHERE type='table' AND name='keep';
PRAGMA writable_schema = 0;
SELECT name FROM sqlite_master WHERE type='table';
"

oracle "cat121_backup_empty_source" "
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.unused(x INT);
DELETE FROM aux.unused;
SELECT count(*) FROM aux.unused;
SELECT name FROM aux.sqlite_master WHERE type='table';
"

oracle_unsafe "cat121_integrity_after_writable_schema" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a'),(2,'b');
PRAGMA writable_schema = 1;
UPDATE sqlite_master SET name = name WHERE type='table';
PRAGMA writable_schema = 0;
PRAGMA integrity_check;
SELECT count(*) FROM t;
"

oracle "cat121_create_table_pending_master_survives" "
BEGIN;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'a');
COMMIT;
SELECT type, name FROM sqlite_master WHERE name='t';
SELECT * FROM t;
"

echo ""
echo "--- Category 122: BLOBKEY table root classification cache ---"

oracle "cat122_blobkey_table_update_with_secondary_index" "
CREATE TABLE t(a TEXT PRIMARY KEY, b INT, c TEXT);
CREATE INDEX tb ON t(b);
INSERT INTO t VALUES('x',1,'one'),('y',2,'two');
UPDATE t SET b=3 WHERE a='x';
SELECT a,b,c FROM t ORDER BY a;
SELECT a FROM t WHERE b=3;
"

echo ""
echo "--- Category 123: Indexed COUNT(*) over key ranges ---"

oracle "cat123_count_range_negative_ints" "
CREATE TABLE t(x);
CREATE INDEX i ON t(x);
INSERT INTO t VALUES(-2),(-1),(-0.97),(0),(5);
SELECT count(*) FROM t WHERE x BETWEEN -2 AND -1;
SELECT group_concat(x) FROM t WHERE x BETWEEN -2 AND -1;
"

oracle "cat123_count_range_negative_reals" "
CREATE TABLE t(x);
CREATE INDEX i ON t(x);
INSERT INTO t VALUES(-3.5),(-2.25),(-2.0),(-1.75),(-0.5),(0.5);
SELECT count(*) FROM t WHERE x BETWEEN -3.5 AND -2.0;
SELECT count(*) FROM t WHERE x BETWEEN -2.25 AND -1.75;
SELECT count(*) FROM t WHERE x >= -3.5 AND x <= -0.5;
"

oracle "cat123_count_range_spanning_zero" "
CREATE TABLE t(x);
CREATE INDEX i ON t(x);
WITH RECURSIVE c(x) AS (VALUES(-50) UNION ALL SELECT x+1 FROM c WHERE x<50)
INSERT INTO t SELECT x FROM c;
SELECT count(*) FROM t WHERE x BETWEEN -50 AND -1;
SELECT count(*) FROM t WHERE x BETWEEN -10 AND 10;
SELECT count(*) FROM t WHERE x BETWEEN -1 AND 0;
SELECT count(*) FROM t WHERE x <= -1;
"

oracle "cat123_count_range_extreme_bounds" "
CREATE TABLE t(x);
CREATE INDEX i ON t(x);
INSERT INTO t VALUES(-9223372036854775808),(-1),(0),(1),(9223372036854775807);
SELECT count(*) FROM t WHERE x BETWEEN -9223372036854775808 AND -1;
SELECT count(*) FROM t WHERE x BETWEEN 0 AND 9223372036854775807;
SELECT count(*) FROM t WHERE x BETWEEN -9223372036854775808 AND 9223372036854775807;
"

oracle "cat123_count_range_composite_index" "
CREATE TABLE t(a,b);
CREATE INDEX i ON t(a,b);
INSERT INTO t VALUES(-2,1),(-2,2),(-1,1),(-1,2),(-0.5,1),(0,1);
SELECT count(*) FROM t WHERE a BETWEEN -2 AND -1;
SELECT count(*) FROM t WHERE a = -1;
SELECT count(*) FROM t WHERE a BETWEEN -2 AND -1 AND b = 1;
"

oracle "cat123_count_range_text_keys" "
CREATE TABLE t(s TEXT);
CREATE INDEX i ON t(s);
INSERT INTO t VALUES('a'),('b'),('ba'),('bz'),('c'),('ca');
SELECT count(*) FROM t WHERE s BETWEEN 'a' AND 'b';
SELECT count(*) FROM t WHERE s BETWEEN 'b' AND 'c';
SELECT count(*) FROM t WHERE s >= 'b' AND s < 'c';
"

oracle "cat123_count_range_blob_keys" "
CREATE TABLE t(k BLOB);
CREATE INDEX i ON t(k);
INSERT INTO t VALUES(x'41'),(x'41ff'),(x'42'),(x'4200'),(x'42ff'),(x'43');
SELECT count(*) FROM t WHERE k BETWEEN x'41' AND x'41ff';
SELECT count(*) FROM t WHERE k BETWEEN x'42' AND x'42ff';
SELECT count(*) FROM t WHERE k BETWEEN x'41ff' AND x'4200';
"

oracle "cat123_count_range_without_rowid" "
CREATE TABLE t(pk INT PRIMARY KEY, v INT) WITHOUT ROWID;
INSERT INTO t VALUES(-3,1),(-2,2),(-1,3),(0,4),(1,5);
SELECT count(*) FROM t WHERE pk BETWEEN -3 AND -1;
SELECT count(*) FROM t WHERE pk BETWEEN -1 AND 1;
"

oracle "cat123_count_range_after_mutation" "
CREATE TABLE t(x);
CREATE INDEX i ON t(x);
INSERT INTO t VALUES(-4),(-3),(-2),(-1),(0);
DELETE FROM t WHERE x = -3;
UPDATE t SET x = -5 WHERE x = 0;
SELECT count(*) FROM t WHERE x BETWEEN -5 AND -1;
SELECT count(*) FROM t WHERE x BETWEEN -4 AND -2;
SELECT group_concat(x) FROM (SELECT x FROM t ORDER BY x);
"

echo ""
echo "================================"
echo "Results: $pass passed, $fail failed"
echo "================================"
if [ "$fail" -gt 0 ]; then
  exit 1
fi
exit 0
