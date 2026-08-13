#!/bin/bash
# Secondary-index row-delta paths (conflicts resolve, merge, workspace) must
# use the same KeyInfo-aware encoding as VDBE. BINARY-only sort keys leave
# ghost entries on NOCASE/RTRIM/DESC indexes and fail integrity_check.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Index Row-Delta Unification ==="
echo ""

# --- NOCASE: conflicts_resolve --theirs renames the indexed column -----------
DB=/tmp/test_idx_delta_nocase_resolve_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE, v INT);
CREATE UNIQUE INDEX idx_name ON t(name COLLATE NOCASE);
INSERT INTO t VALUES (1, 'Alpha', 10), (2, 'Beta', 20);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET name = 'Gamma', v = 21 WHERE id = 1;
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET name = 'Delta', v = 31 WHERE id = 1;
SELECT dolt_commit('-Am', 'main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--theirs', 't');
COMMIT;
EOF

run_test "nocase_resolve_row" \
  "SELECT id || ':' || name || ':' || v FROM t WHERE id=1;" "1:Gamma:21" "$DB"
run_test "nocase_resolve_seek_gamma" \
  "SELECT id FROM t INDEXED BY idx_name WHERE name = 'gamma';" "1" "$DB"
run_test "nocase_resolve_seek_delta_gone" \
  "SELECT count(*) FROM t INDEXED BY idx_name WHERE name = 'delta';" "0" "$DB"
run_test "nocase_resolve_seek_alpha_gone" \
  "SELECT count(*) FROM t INDEXED BY idx_name WHERE name = 'alpha';" "0" "$DB"
run_test_lastline "nocase_resolve_integrity" "PRAGMA integrity_check;" "ok" "$DB"

PRE=$(echo "SELECT group_concat(id || ':' || name, ',') FROM (SELECT id, name FROM t INDEXED BY idx_name ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
echo "REINDEX idx_name;" | $DOLTLITE "$DB" > /dev/null 2>&1
POST=$(echo "SELECT group_concat(id || ':' || name, ',') FROM (SELECT id, name FROM t INDEXED BY idx_name ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
if [ -n "$PRE" ] && [ "$PRE" = "$POST" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocase_resolve_reindex_idempotent\n  pre:  $PRE\n  post: $POST"
fi
rm -f "$DB"

# --- RTRIM: conflict resolve ------------------------------------------------
DB=/tmp/test_idx_delta_rtrim_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT COLLATE RTRIM, v INT);
CREATE INDEX idx_tag ON t(tag COLLATE RTRIM);
INSERT INTO t VALUES (1, 'x', 1);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET tag = 'y  ', v = 2 WHERE id = 1;
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET tag = 'z', v = 3 WHERE id = 1;
SELECT dolt_commit('-Am', 'main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--theirs', 't');
COMMIT;
EOF

run_test "rtrim_resolve_row" \
  "SELECT id || ':' || quote(tag) || ':' || v FROM t WHERE id=1;" "1:'y  ':2" "$DB"
run_test "rtrim_resolve_seek" \
  "SELECT id FROM t INDEXED BY idx_tag WHERE tag = 'y';" "1" "$DB"
run_test_lastline "rtrim_resolve_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# --- DESC + NOCASE merge (inline patch, not post-merge REINDEX-only) --------
DB=/tmp/test_idx_delta_desc_merge_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE, v INT);
CREATE INDEX idx_nd ON t(name COLLATE NOCASE DESC, v);
INSERT INTO t VALUES (1, 'a', 1), (2, 'B', 2);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (3, 'c', 3);
UPDATE t SET v = 20 WHERE id = 2;
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 'D', 4);
SELECT dolt_commit('-Am', 'main');
SELECT dolt_merge('feat');
EOF

run_test "desc_merge_count" "SELECT count(*) FROM t;" "4" "$DB"
run_test "desc_merge_seek_b" \
  "SELECT id || ':' || v FROM t INDEXED BY idx_nd WHERE name = 'b';" "2:20" "$DB"
run_test_lastline "desc_merge_integrity" "PRAGMA integrity_check;" "ok" "$DB"
PRE=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_nd WHERE name >= 'a' ORDER BY name DESC, v, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
echo "REINDEX idx_nd;" | $DOLTLITE "$DB" > /dev/null 2>&1
POST=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_nd WHERE name >= 'a' ORDER BY name DESC, v, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
if [ -n "$PRE" ] && [ "$PRE" = "$POST" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: desc_merge_reindex_idempotent\n  pre:  $PRE\n  post: $POST"
fi
rm -f "$DB"

# Expression-index keys must include the expression value. Theirs-added
# rows used to be stored under a rowid-only key, so INDEXED BY missed them
# and integrity_check reported the row missing from the index.
DB=/tmp/test_idx_delta_expr_merge_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX tv ON t(lower(v));
INSERT INTO t VALUES(1,'A');
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3,'Feat');
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'Main');
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
EOF

run_test "expr_merge_rows" \
  "SELECT group_concat(id||'|'||v, ',') FROM (SELECT id,v FROM t ORDER BY id);" \
  "1|A,2|Main,3|Feat" "$DB"
run_test "expr_merge_indexed_by" \
  "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY tv WHERE lower(v)>='a' ORDER BY id);" \
  "1,2,3" "$DB"
run_test_lastline "expr_merge_integrity" "PRAGMA integrity_check;" "ok" "$DB"
PRE=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY tv WHERE lower(v)>='a' ORDER BY id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
echo "REINDEX tv;" | $DOLTLITE "$DB" > /dev/null 2>&1
POST=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY tv WHERE lower(v)>='a' ORDER BY id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
if [ -n "$PRE" ] && [ "$PRE" = "$POST" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: expr_merge_reindex_idempotent\n  pre:  $PRE\n  post: $POST"
fi
rm -f "$DB"

# Partial unique indexes only constrain rows that match WHERE. Both
# branches inserting v=-1 must merge cleanly; v=9 on both is a real
# unique violation.
DB=/tmp/test_idx_delta_partial_unique_neg_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE UNIQUE INDEX tv ON t(v) WHERE v>0;
INSERT INTO t VALUES(1, 1);
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3, -1);
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, -1);
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
EOF

run_test_match "partial_unique_neg_merge" \
  "SELECT length(dolt_hashof('HEAD'));" "^40$" "$DB"
run_test "partial_unique_neg_rows" \
  "SELECT group_concat(id||':'||v, ',') FROM (SELECT id,v FROM t ORDER BY id);" \
  "1:1,2:-1,3:-1" "$DB"
run_test "partial_unique_neg_no_cv" \
  "SELECT coalesce(sum(num_violations),0) FROM dolt_constraint_violations;" \
  "0" "$DB"
run_test_lastline "partial_unique_neg_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

DB=/tmp/test_idx_delta_partial_unique_pos_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE UNIQUE INDEX tv ON t(v) WHERE v>0;
INSERT INTO t VALUES(1, 1);
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(3, 9);
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2, 9);
SELECT dolt_commit('-Am','main');
EOF
out=$(echo "BEGIN; SELECT dolt_merge('feat'); SELECT coalesce(sum(num_violations),0) FROM dolt_constraint_violations; ROLLBACK;" | $DOLTLITE "$DB" 2>/dev/null | tail -1)
if [ "$out" = "2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: partial_unique_pos_still_violates\n  expected: 2\n  got:      $out"
fi
rm -f "$DB"

DB=/tmp/test_idx_delta_partial_unique_wr_neg_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(k TEXT PRIMARY KEY, v INT);
CREATE UNIQUE INDEX tv ON t(v) WHERE v>0;
INSERT INTO t VALUES('a', 1);
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES('c', -1);
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('b', -1);
SELECT dolt_commit('-Am','main');
SELECT dolt_merge('feat');
EOF

run_test "partial_unique_wr_neg_rows" \
  "SELECT group_concat(k||':'||v, ',') FROM (SELECT k,v FROM t ORDER BY k);" \
  "a:1,b:-1,c:-1" "$DB"
run_test "partial_unique_wr_neg_no_cv" \
  "SELECT coalesce(sum(num_violations),0) FROM dolt_constraint_violations;" \
  "0" "$DB"
run_test_lastline "partial_unique_wr_neg_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# --- Workspace stage of NOCASE index row ------------------------------------
DB=/tmp/test_idx_delta_ws_nocase_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT COLLATE NOCASE);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES(1,'Abc'),(2,'Def');
SELECT dolt_commit('-A','-m','seed');
INSERT INTO t VALUES(3,'GHI');
UPDATE t SET v = 'xyz' WHERE id = 1;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id IN (1, 3);
SELECT dolt_commit('-m','stage nocase rows');
EOF

run_test "ws_nocase_seek_xyz" \
  "SELECT id FROM t INDEXED BY idx_t_v WHERE v = 'XYZ';" "1" "$DB"
run_test "ws_nocase_seek_ghi" \
  "SELECT id FROM t INDEXED BY idx_t_v WHERE v = 'ghi';" "3" "$DB"
run_test "ws_nocase_seek_abc_gone" \
  "SELECT count(*) FROM t INDEXED BY idx_t_v WHERE v = 'abc';" "0" "$DB"
run_test_lastline "ws_nocase_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

# --- BINARY unique index still works on resolve (regression) ----------------
DB=/tmp/test_idx_delta_binary_$$.db
rm -f "$DB"
cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, v INT);
CREATE UNIQUE INDEX idx_name ON t(name);
INSERT INTO t VALUES (1, 'Alpha', 10);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET name = 'Gamma', v = 21 WHERE id = 1;
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
UPDATE t SET name = 'Delta', v = 31 WHERE id = 1;
SELECT dolt_commit('-Am', 'main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--theirs', 't');
COMMIT;
EOF

run_test "binary_resolve_seek" \
  "SELECT id FROM t INDEXED BY idx_name WHERE name = 'Gamma';" "1" "$DB"
run_test_lastline "binary_resolve_integrity" "PRAGMA integrity_check;" "ok" "$DB"
rm -f "$DB"

dltest_finish
