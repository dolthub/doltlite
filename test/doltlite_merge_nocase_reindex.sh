#!/bin/bash
# NOCASE secondary indexes are patched inline during three-way merge with a
# real KeyInfo (same path as conflicts resolve / workspace). This suite checks
# that the post-merge index matches a full rebuild: same INDEXED BY results
# before/after REINDEX, and after drop+recreate.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Merge NOCASE Index Reindex Property ==="
echo ""

DB=/tmp/test_merge_nocase_$$.db
rm -f "$DB"

cat <<'EOF' | $DOLTLITE "$DB" > /dev/null 2>&1
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  name TEXT COLLATE NOCASE,
  score INT
);
CREATE INDEX idx_name ON t(name COLLATE NOCASE);
INSERT INTO t VALUES
  (1, 'Alpha', 10),
  (2, 'beta', 20),
  (3, 'GAMMA', 30);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET score = 21 WHERE id = 2;
INSERT INTO t VALUES (4, 'delta', 40);
SELECT dolt_commit('-Am', 'feat_side');
SELECT dolt_checkout('main');
UPDATE t SET score = 11 WHERE id = 1;
INSERT INTO t VALUES (5, 'EPSILON', 50);
SELECT dolt_commit('-Am', 'main_side');
SELECT dolt_merge('feat');
EOF

run_test "nocase_merge_row_count" \
  "SELECT count(*) FROM t;" "5" "$DB"
run_test "nocase_merge_scores" \
  "SELECT group_concat(id || ':' || score, ',') FROM (SELECT id, score FROM t ORDER BY id);" \
  "1:11,2:21,3:30,4:40,5:50" "$DB"

# Index seek uses NOCASE: 'alpha' matches Alpha.
run_test "nocase_merge_idx_seek_alpha" \
  "SELECT id || ':' || score FROM t INDEXED BY idx_name WHERE name = 'alpha';" \
  "1:11" "$DB"
run_test "nocase_merge_idx_seek_delta" \
  "SELECT id || ':' || score FROM t INDEXED BY idx_name WHERE name = 'DELTA';" \
  "4:40" "$DB"
run_test "nocase_merge_idx_order" \
  "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_name WHERE name >= 'a' ORDER BY name, id);" \
  "1,2,4,5,3" "$DB"

PRE_REINDEX=$(echo "SELECT group_concat(id || ':' || name || ':' || score, ',') FROM (SELECT id, name, score FROM t INDEXED BY idx_name WHERE name >= 'a' ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)

echo "REINDEX idx_name;" | $DOLTLITE "$DB" > /dev/null 2>&1
POST_REINDEX=$(echo "SELECT group_concat(id || ':' || name || ':' || score, ',') FROM (SELECT id, name, score FROM t INDEXED BY idx_name WHERE name >= 'a' ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)

if [ -n "$PRE_REINDEX" ] && [ "$PRE_REINDEX" = "$POST_REINDEX" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocase_merge_reindex_idempotent\n  pre:  $PRE_REINDEX\n  post: $POST_REINDEX"
fi

echo "DROP INDEX idx_name; CREATE INDEX idx_name ON t(name COLLATE NOCASE);" | $DOLTLITE "$DB" > /dev/null 2>&1
FRESH=$(echo "SELECT group_concat(id || ':' || name || ':' || score, ',') FROM (SELECT id, name, score FROM t INDEXED BY idx_name WHERE name >= 'a' ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)

if [ -n "$POST_REINDEX" ] && [ "$POST_REINDEX" = "$FRESH" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocase_merge_matches_fresh_index\n  reindex: $POST_REINDEX\n  fresh:   $FRESH"
fi

run_test_lastline "nocase_merge_integrity" "PRAGMA integrity_check;" "ok" "$DB"

# DESC + NOCASE also uses KeyInfo-aware inline index patching.
DB2=/tmp/test_merge_nocase_desc_$$.db
rm -f "$DB2"
cat <<'EOF' | $DOLTLITE "$DB2" > /dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE, v INT);
CREATE INDEX idx_nd ON t(name COLLATE NOCASE DESC, v);
INSERT INTO t VALUES (1, 'a', 1), (2, 'B', 2);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (3, 'c', 3);
SELECT dolt_commit('-Am', 'feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 'D', 4);
SELECT dolt_commit('-Am', 'main');
SELECT dolt_merge('feat');
EOF

run_test "nocase_desc_merge_count" "SELECT count(*) FROM t;" "4" "$DB2"
run_test "nocase_desc_idx_seek" \
  "SELECT id FROM t INDEXED BY idx_nd WHERE name = 'b';" "2" "$DB2"
PRE2=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_nd WHERE name >= 'a' ORDER BY name DESC, v, id);" | $DOLTLITE "$DB2" 2>/dev/null | tail -1)
echo "REINDEX idx_nd;" | $DOLTLITE "$DB2" > /dev/null 2>&1
POST2=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_nd WHERE name >= 'a' ORDER BY name DESC, v, id);" | $DOLTLITE "$DB2" 2>/dev/null | tail -1)
if [ -n "$PRE2" ] && [ "$PRE2" = "$POST2" ]; then
  PASS=$((PASS+1))
else
  FAIL=$((FAIL+1))
  ERRORS="$ERRORS\nFAIL: nocase_desc_reindex_idempotent\n  pre:  $PRE2\n  post: $POST2"
fi
rm -f "$DB2"
rm -f "$DB"

dltest_finish
