#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize_state() {
  tr -d '"\r' \
    | sed -E \
        -e 's/\|false\|/|0|/g' \
        -e 's/\|true\|/|1|/g' \
        -e 's/^L\|Revert "/L|Revert /' \
        -e 's/"$//' \
    | awk -F'|' '$1=="L" || $1=="S" || $1=="T" || $1=="M" { print }' \
    | sort
}

oracle_state() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.setup.out" \
    "$dir/dl.setup.err" "$setup" || true
  local dl_out
  dl_out=$(
    {
      printf ".headers off\n.mode list\n.separator '|'\n%s\n" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.setup.err" \
      | normalize_state
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.setup.out" \
    "$dir/dt.setup.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.setup.err" \
      | tail -n +2 \
      | normalize_state
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_revert ==="
echo ""

echo "--- clean revert cases ---"

oracle_state "revert_head_table_creation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-Am', 'add row');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));
" "
SELECT 'L|' || message FROM dolt_log
  WHERE message IN ('add row', 'Initialize data repository')
     OR message LIKE 'Revert%';
" "
SELECT CONCAT('L|', message) FROM dolt_log
  WHERE message IN ('add row', 'Initialize data repository')
     OR message LIKE 'Revert%';
"

oracle_state "revert_non_head_clean" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-Am', 'schema');
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-Am', 'add 1');
INSERT INTO t VALUES(2, 'b');
SELECT dolt_commit('-Am', 'add 2');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='add 1'));
" "
SELECT 'T|' || id || '|' || x FROM t ORDER BY id;
SELECT 'L|' || message FROM dolt_log
  WHERE message IN ('schema', 'add 1', 'add 2')
     OR message LIKE 'Revert%';
" "
SELECT CONCAT('T|', id, '|', x) FROM t ORDER BY id;
SELECT CONCAT('L|', message) FROM dolt_log
  WHERE message IN ('schema', 'add 1', 'add 2')
     OR message LIKE 'Revert%';
"

echo "--- dirty working set cases ---"

oracle_state "revert_head_dirty_unrelated_table_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am', 'schema');
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-Am', 'add row');
INSERT INTO meta VALUES(1, 'side');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));
" "
SELECT 'T|' || id || '|' || x FROM t ORDER BY id;
SELECT 'M|' || id || '|' || note FROM meta ORDER BY id;
SELECT 'S|' || table_name || '|' || staged || '|' || status
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT 'L|' || message FROM dolt_log
  WHERE message IN ('schema', 'add row') OR message LIKE 'Revert%';
" "
SELECT CONCAT('T|', id, '|', x) FROM t ORDER BY id;
SELECT CONCAT('M|', id, '|', note) FROM meta ORDER BY id;
SELECT CONCAT('S|', table_name, '|', staged, '|', status)
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT CONCAT('L|', message) FROM dolt_log
  WHERE message IN ('schema', 'add row') OR message LIKE 'Revert%';
"

oracle_state "revert_non_head_dirty_unrelated_table_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am', 'schema');
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-Am', 'add 1');
INSERT INTO t VALUES(2, 'b');
SELECT dolt_commit('-Am', 'add 2');
INSERT INTO meta VALUES(1, 'side');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='add 1'));
" "
SELECT 'T|' || id || '|' || x FROM t ORDER BY id;
SELECT 'M|' || id || '|' || note FROM meta ORDER BY id;
SELECT 'S|' || table_name || '|' || staged || '|' || status
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT 'L|' || message FROM dolt_log
  WHERE message IN ('schema', 'add 1', 'add 2') OR message LIKE 'Revert%';
" "
SELECT CONCAT('T|', id, '|', x) FROM t ORDER BY id;
SELECT CONCAT('M|', id, '|', note) FROM meta ORDER BY id;
SELECT CONCAT('S|', table_name, '|', staged, '|', status)
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT CONCAT('L|', message) FROM dolt_log
  WHERE message IN ('schema', 'add 1', 'add 2') OR message LIKE 'Revert%';
"

oracle_state "revert_head_dirty_same_table_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_commit('-Am', 'schema');
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-Am', 'add row');
INSERT INTO t VALUES(2, 'side');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));
" "
SELECT 'T|' || id || '|' || x FROM t ORDER BY id;
SELECT 'S|' || table_name || '|' || staged || '|' || status
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT 'L|' || message FROM dolt_log
  WHERE message IN ('schema', 'add row') OR message LIKE 'Revert%';
" "
SELECT CONCAT('T|', id, '|', x) FROM t ORDER BY id;
SELECT CONCAT('S|', table_name, '|', staged, '|', status)
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT CONCAT('L|', message) FROM dolt_log
  WHERE message IN ('schema', 'add row') OR message LIKE 'Revert%';
"

oracle_state "revert_head_staged_unrelated_table_rejected" "
CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE meta(id INTEGER PRIMARY KEY, note TEXT);
SELECT dolt_commit('-Am', 'schema');
INSERT INTO t VALUES(1, 'a');
SELECT dolt_commit('-Am', 'add row');
INSERT INTO meta VALUES(1, 'side');
SELECT dolt_add('meta');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log LIMIT 1));
" "
SELECT 'T|' || id || '|' || x FROM t ORDER BY id;
SELECT 'M|' || id || '|' || note FROM meta ORDER BY id;
SELECT 'S|' || table_name || '|' || staged || '|' || status
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT 'L|' || message FROM dolt_log
  WHERE message IN ('schema', 'add row') OR message LIKE 'Revert%';
" "
SELECT CONCAT('T|', id, '|', x) FROM t ORDER BY id;
SELECT CONCAT('M|', id, '|', note) FROM meta ORDER BY id;
SELECT CONCAT('S|', table_name, '|', staged, '|', status)
  FROM dolt_status ORDER BY table_name, staged, status;
SELECT CONCAT('L|', message) FROM dolt_log
  WHERE message IN ('schema', 'add row') OR message LIKE 'Revert%';
"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
