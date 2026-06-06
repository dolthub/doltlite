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

if "$DOLTLITE" "$IDX_DB" "UPDATE dolt_workspace_t SET staged=TRUE;" >/tmp/doltlite_ws_idx.out 2>/tmp/doltlite_ws_idx.err; then
  dltest_fail "workspace_indexed_table_rejected" "expected indexed table staging to fail"
else
  dltest_pass
fi
if ! grep -q "indexed tables is not yet supported" /tmp/doltlite_ws_idx.err; then
  dltest_fail "workspace_indexed_table_error_message" "$(cat /tmp/doltlite_ws_idx.err)"
else
  dltest_pass
fi

dltest_finish
