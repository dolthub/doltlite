#!/bin/bash

set -euo pipefail

source "$(dirname "$0")/lib/doltlite_test_common.sh"

DB=/tmp/doltlite_workspace_$$.db
rm -rf "$DB"
trap 'rm -rf "$DB"' EXIT

dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, confidence INT);
INSERT INTO t VALUES(1,10,0),(2,20,0),(3,30,0);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v=v+100, confidence=CASE id WHEN 1 THEN 1 WHEN 2 THEN -1 ELSE 2 END;
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_confidence > from_confidence;
SELECT dolt_commit('-m','partial');
" "$DB" >/dev/null

run_test "workspace_partial_commit_keeps_unstaged_diff" \
  "SELECT to_id || '|' || to_v || '|' || from_v || '|' || diff_type
     FROM dolt_diff_t('HEAD', 'WORKING')
    ORDER BY to_id;" \
  "2|120|20|modified" "$DB"

run_test "workspace_partial_commit_visible_rows" \
  "SELECT id || '|' || v || '|' || confidence FROM t ORDER BY id;" \
  $'1|110|1\n2|120|-1\n3|130|2' "$DB"

IDX_DB=/tmp/doltlite_workspace_index_$$.db
rm -rf "$IDX_DB"
trap 'rm -rf "$DB"; rm -rf "$IDX_DB"' EXIT

dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v=2;
" "$IDX_DB" >/dev/null

if "$DOLTLITE" "$IDX_DB" "UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=1;" \
     >/tmp/doltlite_ws_idx.out 2>/tmp/doltlite_ws_idx.err; then
  dltest_pass
else
  dltest_fail "workspace_indexed_table_staged" "$(cat /tmp/doltlite_ws_idx.err)"
fi

"$DOLTLITE" "$IDX_DB" "SELECT dolt_commit('-m','stage indexed row');" >/dev/null 2>&1
idx_integ=$("$DOLTLITE" "$IDX_DB" "PRAGMA integrity_check;" 2>&1)
if [ "$idx_integ" = "ok" ]; then
  dltest_pass
else
  dltest_fail "workspace_indexed_integrity_check" "$idx_integ"
fi

idx_new=$("$DOLTLITE" "$IDX_DB" "SELECT id FROM t WHERE v=2;" 2>&1)
scan_new=$("$DOLTLITE" "$IDX_DB" "SELECT id FROM t WHERE +v=2;" 2>&1)
idx_old=$("$DOLTLITE" "$IDX_DB" "SELECT id FROM t WHERE v=1;" 2>&1)
scan_old=$("$DOLTLITE" "$IDX_DB" "SELECT id FROM t WHERE +v=1;" 2>&1)
if [ "$idx_new" = "1" ] && [ "$idx_new" = "$scan_new" ] \
   && [ -z "$idx_old" ] && [ -z "$scan_old" ]; then
  dltest_pass
else
  dltest_fail "workspace_indexed_query" \
    "idx_new=[$idx_new] scan_new=[$scan_new] idx_old=[$idx_old] scan_old=[$scan_old]"
fi

NC_DB=/tmp/doltlite_workspace_nocase_$$.db
rm -rf "$NC_DB"
trap 'rm -rf "$DB"; rm -rf "$IDX_DB"; rm -rf "$NC_DB"' EXIT
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT COLLATE NOCASE);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES(1,'Abc'),(2,'Def');
SELECT dolt_commit('-A','-m','seed');
INSERT INTO t VALUES(3,'GHI');
UPDATE dolt_workspace_t SET staged=TRUE WHERE to_id=3;
SELECT dolt_commit('-m','stage nocase row');
" "$NC_DB" >/dev/null
nc_integ=$("$DOLTLITE" "$NC_DB" "PRAGMA integrity_check;" 2>&1)
if [ "$nc_integ" = "ok" ]; then
  dltest_pass
else
  dltest_fail "workspace_indexed_nocase_integrity" "$nc_integ"
fi
nc_ci=$("$DOLTLITE" "$NC_DB" "SELECT id FROM t WHERE v='ghi';" 2>&1)
if [ "$nc_ci" = "3" ]; then
  dltest_pass
else
  dltest_fail "workspace_indexed_nocase_query" "ci=[$nc_ci]"
fi

DEL_DB=/tmp/doltlite_workspace_delete_$$.db
rm -rf "$DEL_DB"
trap 'rm -rf "$DB"; rm -rf "$IDX_DB"; rm -rf "$NC_DB"; rm -rf "$DEL_DB"' EXIT

# Discard unstaged insert
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
SELECT dolt_commit('-A','-m','seed empty');
INSERT INTO t VALUES(42,42),(43,43);
DELETE FROM dolt_workspace_t WHERE to_id=42;
" "$DEL_DB" >/dev/null
run_test "workspace_delete_discards_insert_data" \
  "SELECT id || '|' || val FROM t ORDER BY id;" "43|43" "$DEL_DB"
run_test "workspace_delete_discards_insert_ws" \
  "SELECT count(*) || '|' || group_concat(to_id) FROM dolt_workspace_t;" \
  "1|43" "$DEL_DB"

# Discard unstaged delete (restore row)
rm -rf "$DEL_DB"
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(42,42),(43,43);
SELECT dolt_commit('-A','-m','seed');
DELETE FROM t;
DELETE FROM dolt_workspace_t WHERE from_id=42;
" "$DEL_DB" >/dev/null
run_test "workspace_delete_restores_removed_row" \
  "SELECT id || '|' || val FROM t ORDER BY id;" "42|42" "$DEL_DB"
run_test "workspace_delete_restores_ws_remaining" \
  "SELECT count(*) || '|' || group_concat(from_id) FROM dolt_workspace_t;" \
  "1|43" "$DEL_DB"

# Discard unstaged modify
rm -rf "$DEL_DB"
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
INSERT INTO t VALUES(42,42),(43,43);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET val=val*2;
DELETE FROM dolt_workspace_t WHERE to_id=42;
" "$DEL_DB" >/dev/null
run_test "workspace_delete_reverts_modify" \
  "SELECT id || '|' || val FROM t ORDER BY id;" $'42|42\n43|86' "$DEL_DB"

# Cannot delete staged rows
rm -rf "$DEL_DB"
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
SELECT dolt_commit('-A','-m','seed');
INSERT INTO t VALUES(42,42);
SELECT dolt_add('t');
" "$DEL_DB" >/dev/null
if "$DOLTLITE" "$DEL_DB" "DELETE FROM dolt_workspace_t WHERE id=1;" \
     >/tmp/doltlite_ws_del.out 2>/tmp/doltlite_ws_del.err; then
  dltest_fail "workspace_delete_staged_rejected" "expected error, got success"
else
  if grep -qi 'cannot delete staged' /tmp/doltlite_ws_del.err; then
    dltest_pass
  else
    dltest_fail "workspace_delete_staged_rejected" "$(cat /tmp/doltlite_ws_del.err)"
  fi
fi

# Discard after unstage
rm -rf "$DEL_DB"
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INT);
SELECT dolt_commit('-A','-m','seed');
INSERT INTO t VALUES(42,42),(43,43);
SELECT dolt_add('t');
UPDATE dolt_workspace_t SET staged=FALSE WHERE to_id=42;
DELETE FROM dolt_workspace_t WHERE to_id=42;
" "$DEL_DB" >/dev/null
run_test "workspace_delete_after_unstage" \
  "SELECT id || '|' || val FROM t ORDER BY id;" "43|43" "$DEL_DB"
run_test "workspace_delete_after_unstage_staged_remains" \
  "SELECT staged || '|' || to_id FROM dolt_workspace_t;" "1|43" "$DEL_DB"

# Secondary indexes stay consistent when discarding a modify
rm -rf "$DEL_DB"
dltest_run_sql "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v=2;
DELETE FROM dolt_workspace_t WHERE to_id=1;
" "$DEL_DB" >/dev/null
run_test "workspace_delete_index_integrity" \
  "PRAGMA integrity_check;" "ok" "$DEL_DB"
run_test "workspace_delete_index_query_old" \
  "SELECT id FROM t WHERE v=1;" "1" "$DEL_DB"
run_test "workspace_delete_index_query_new" \
  "SELECT id FROM t WHERE v=2;" "" "$DEL_DB"

# Two cursors are open at once here. Rows used to accumulate on the table, so
# the second xFilter freed the array the first was still indexing into: the
# join returned extra rows carrying ids that were never assigned.
MC_DB=/tmp/doltlite_workspace_multicursor_$$.db
rm -rf "$MC_DB"
dltest_run_sql "
CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_commit('-A','-m','seed');
UPDATE t SET v='x' WHERE pk=1;
UPDATE t SET v='y' WHERE pk=2;
UPDATE t SET v='z' WHERE pk=3;
" "$MC_DB" >/dev/null

run_test "workspace_single_cursor_rows" \
  "SELECT count(*) FROM dolt_workspace_t;" "3" "$MC_DB"
run_test "workspace_self_join_row_count" \
  "SELECT count(*) FROM dolt_workspace_t a, dolt_workspace_t b;" "9" "$MC_DB"
run_test "workspace_self_join_values_intact" \
  "SELECT group_concat(a.id || ':' || coalesce(a.to_v,'~'), ' ')
     FROM dolt_workspace_t a, dolt_workspace_t b;" \
  "1:x 1:x 1:x 2:y 2:y 2:y 3:z 3:z 3:z" "$MC_DB"
run_test "workspace_subquery_same_table" \
  "SELECT count(*) FROM dolt_workspace_t
     WHERE id IN (SELECT id FROM dolt_workspace_t);" "3" "$MC_DB"
run_test "workspace_correlated_subquery" \
  "SELECT count(*) FROM dolt_workspace_t a
     WHERE EXISTS (SELECT 1 FROM dolt_workspace_t b WHERE b.id=a.id);" "3" "$MC_DB"

# xUpdate has no cursor and resolves a rowid by replaying the scan, so it has
# to replay the staged filter the rowid came from.
run_test "workspace_stage_one_row" \
  "UPDATE dolt_workspace_t SET staged=1 WHERE id=1;
   SELECT group_concat(id || ':' || staged, ' ') FROM dolt_workspace_t;" \
  "1:1 2:0 3:0" "$MC_DB"
run_test "workspace_delete_via_filtered_rowid" \
  "DELETE FROM dolt_workspace_t
     WHERE id=(SELECT id FROM dolt_workspace_t WHERE staged=0 LIMIT 1);
   SELECT group_concat(id || ':' || staged, ' ') FROM dolt_workspace_t;" \
  "1:1 2:0" "$MC_DB"

rm -rf "$MC_DB"

# An uncommitted key-shape recreate renders the head row under the head
# shape: before the per-side fix, from_pk showed the raw text sort key
# decoded as a sign-flipped integer.
WS_DB=/tmp/test_ws_shape_$$.db; rm -f "$WS_DB"
echo "CREATE TABLE t(pk TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;
INSERT INTO t VALUES('alpha', 1);
SELECT dolt_commit('-Am','c1');
DROP TABLE t;
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(42, 2);" | $DOLTLITE "$WS_DB" > /dev/null 2>&1

run_test "ws_shape_removed_side" \
  "SELECT from_pk || '=' || from_v FROM dolt_workspace_t WHERE diff_type='removed';" \
  "alpha=1" "$WS_DB"
run_test "ws_shape_added_side" \
  "SELECT to_pk || '=' || to_v FROM dolt_workspace_t WHERE diff_type='added';" \
  "42=2" "$WS_DB"
rm -f "$WS_DB"

dltest_finish
