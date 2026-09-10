#!/bin/bash
# NOCASE indexes are patched inline with KeyInfo; post-merge must match a full rebuild.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Merge NOCASE Index Reindex Property ==="
echo ""

DB=/tmp/test_merge_nocase_$$.db
rm -f "$DB"

NOCASE_SETUP="
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
"

if dltest_require "nocase_setup" "$DB" "$NOCASE_SETUP"; then
  if [ ! -f "$DB" ]; then
    dltest_fail "nocase_setup" "  expected a database file after setup"
  else
  run_test_match "nocase_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB"
  run_test "nocase_merge_log" "SELECT count(*) FROM dolt_log;" "5" "$DB"

  run_test "nocase_merge_row_count" \
    "SELECT count(*) FROM t;" "5" "$DB"
  run_test "nocase_merge_scores" \
    "SELECT group_concat(id || ':' || score, ',') FROM (SELECT id, score FROM t ORDER BY id);" \
    "1:11,2:21,3:30,4:40,5:50" "$DB"

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

  if dltest_require "nocase_reindex" "$DB" "REINDEX idx_name;"; then
    POST_REINDEX=$(echo "SELECT group_concat(id || ':' || name || ':' || score, ',') FROM (SELECT id, name, score FROM t INDEXED BY idx_name WHERE name >= 'a' ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)

    if [ -n "$PRE_REINDEX" ] && [ "$PRE_REINDEX" = "$POST_REINDEX" ]; then
      dltest_pass
    else
      dltest_fail "nocase_merge_reindex_idempotent" \
        "  pre:  $PRE_REINDEX\n  post: $POST_REINDEX"
    fi

    if dltest_require "nocase_fresh_index" "$DB" \
      "DROP INDEX idx_name; CREATE INDEX idx_name ON t(name COLLATE NOCASE);"; then
      FRESH=$(echo "SELECT group_concat(id || ':' || name || ':' || score, ',') FROM (SELECT id, name, score FROM t INDEXED BY idx_name WHERE name >= 'a' ORDER BY name, id);" | $DOLTLITE "$DB" 2>/dev/null | tail -1)

      if [ -n "$POST_REINDEX" ] && [ "$POST_REINDEX" = "$FRESH" ]; then
        dltest_pass
      else
        dltest_fail "nocase_merge_matches_fresh_index" \
          "  reindex: $POST_REINDEX\n  fresh:   $FRESH"
      fi
    fi
  fi

  run_test_lastline "nocase_merge_integrity" "PRAGMA integrity_check;" "ok" "$DB"
  fi
fi
rm -f "$DB"

DB2=/tmp/test_merge_nocase_desc_$$.db
rm -f "$DB2"
DESC_SETUP="
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
"

if dltest_require "nocase_desc_setup" "$DB2" "$DESC_SETUP"; then
  if [ ! -f "$DB2" ]; then
    dltest_fail "nocase_desc_setup" "  expected a database file after setup"
  else
  run_test_match "nocase_desc_merge" "SELECT dolt_merge('feat');" "^[0-9a-f]{40}$" "$DB2"
  run_test "nocase_desc_merge_log" "SELECT count(*) FROM dolt_log;" "5" "$DB2"
  run_test "nocase_desc_merge_count" "SELECT count(*) FROM t;" "4" "$DB2"
  run_test "nocase_desc_idx_seek" \
    "SELECT id FROM t INDEXED BY idx_nd WHERE name = 'b';" "2" "$DB2"
  PRE2=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_nd WHERE name >= 'a' ORDER BY name DESC, v, id);" | $DOLTLITE "$DB2" 2>/dev/null | tail -1)
  if dltest_require "nocase_desc_reindex" "$DB2" "REINDEX idx_nd;"; then
    POST2=$(echo "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_nd WHERE name >= 'a' ORDER BY name DESC, v, id);" | $DOLTLITE "$DB2" 2>/dev/null | tail -1)
    if [ -n "$PRE2" ] && [ "$PRE2" = "$POST2" ]; then
      dltest_pass
    else
      dltest_fail "nocase_desc_reindex_idempotent" \
        "  pre:  $PRE2\n  post: $POST2"
    fi
  fi
  fi
fi
rm -f "$DB2"

dltest_finish
