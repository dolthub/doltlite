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

dltest_finish
