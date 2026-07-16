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

normalize() {
  tr -d '\r'
}

oracle_status() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT table_name || char(9) || staged || char(9) || status FROM dolt_status ORDER BY table_name, staged, status;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(table_name, char(9), staged, char(9), status) FROM dolt_status ORDER BY table_name, staged, status;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(vc_oracle_tail_csv_body "$dir/dt.raw" | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_status_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT table_name || char(9) || staged || char(9) || status FROM dolt_status ORDER BY table_name, staged, status;\n" "$dl_setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(table_name, char(9), staged, char(9), status) FROM dolt_status ORDER BY table_name, staged, status;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(vc_oracle_tail_csv_body "$dir/dt.raw" | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

status_filter_pushdown() {
  local name="$1" setup="$2" filter="$3" expected_idx="$4"
  local dir="$TMPROOT/${name}_pushdown"
  mkdir -p "$dir"

  local script
  script="$setup
.headers off
.mode list
.separator '	'
CREATE TEMP TABLE expected_status AS
  SELECT table_name, staged, status FROM dolt_status;
SELECT 'A|' || table_name || '|' || staged || '|' || status
  FROM expected_status
 WHERE $filter
 ORDER BY table_name, staged, status;
SELECT 'B|' || table_name || '|' || staged || '|' || status
  FROM dolt_status
 WHERE $filter
 ORDER BY table_name, staged, status;
EXPLAIN QUERY PLAN SELECT * FROM dolt_status WHERE $filter;"

  local out actual expected plan
  out=$(printf "%s\n" "$script" \
        | "$DOLTLITE" "$dir/db" 2>"$dir/err" \
        | tr -d '\r')
  expected=$(printf "%s\n" "$out" | grep '^A|' | sed 's/^A|//')
  actual=$(printf "%s\n" "$out" | grep '^B|' | sed 's/^B|//')
  plan=$(printf "%s\n" "$out" | grep "VIRTUAL TABLE INDEX $expected_idx")

  if [ "$actual" = "$expected" ] && [ -n "$plan" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected:"; echo "$expected" | sed 's/^/      /'
    echo "    actual:";   echo "$actual" | sed 's/^/      /'
    echo "    plan:";     printf "%s\n" "$out" | grep "dolt_status" | sed 's/^/      /'
    echo "    stderr:";   sed 's/^/      /' "$dir/err"
  fi
}

echo "=== Version Control Oracle Tests: dolt_status schema objects ==="
echo ""

echo "--- indexes ---"

oracle_status "create_index_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_t_v ON t(v);
"

oracle_status "create_index_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_t_v ON t(v);
SELECT dolt_add('t');
"

oracle_status "create_index_staged_then_data_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_t_v ON t(v);
SELECT dolt_add('t');
INSERT INTO t VALUES (2, 20);
"

oracle_status_dual "drop_index_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx_t_v;
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx_t_v ON t;
"

oracle_status_dual "replace_index_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES (1, 10, 100);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx_t_v;
CREATE INDEX idx_t_w ON t(w);
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX idx_t_v ON t(v);
INSERT INTO t VALUES (1, 10, 100);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx_t_v ON t;
CREATE INDEX idx_t_w ON t(w);
"

oracle_status "unique_index_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE UNIQUE INDEX idx_t_v_unique ON t(v);
"

oracle_status "composite_index_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
INSERT INTO t VALUES (1, 10, 100);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_t_v_w ON t(v, w);
"

echo "--- columns and table constraints ---"

oracle_status "add_column_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t ADD COLUMN note TEXT;
"

oracle_status "add_column_default_staged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t ADD COLUMN flag INT DEFAULT 0;
SELECT dolt_add('t');
"

oracle_status "add_column_staged_then_data_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t ADD COLUMN flag INT DEFAULT 0;
SELECT dolt_add('t');
INSERT INTO t(id, v, flag) VALUES (2, 20, 1);
"

oracle_status "create_check_table_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t VALUES (1, 10);
"

oracle_status "create_unique_table_unstaged" "
CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE);
INSERT INTO t VALUES (1, 'a@example.com');
"

echo "--- foreign keys ---"

oracle_status "create_fk_child_unstaged" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
INSERT INTO parent VALUES (1);
SELECT dolt_commit('-Am', 'parent');
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id));
INSERT INTO child VALUES (1, 1);
"

oracle_status "create_fk_child_staged" "
CREATE TABLE parent(id INTEGER PRIMARY KEY);
INSERT INTO parent VALUES (1);
SELECT dolt_commit('-Am', 'parent');
CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id));
INSERT INTO child VALUES (1, 1);
SELECT dolt_add('child');
"

echo "--- mixed schema objects ---"

oracle_status "two_tables_one_schema_object_staged" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_a_v ON a(v);
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('a');
"

oracle_status "schema_object_and_table_create_mix" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_a_v ON a(v);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO b VALUES (1, 10);
"

echo "--- filtered scans ---"

status_filter_pushdown "filtered_index_change_matches_unfiltered" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_a_v ON a(v);
CREATE INDEX idx_b_v ON b(v);
" "table_name='a'" "2"

status_filter_pushdown "filtered_staged_index_change_matches_unfiltered" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx_a_v ON a(v);
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('a');
" "staged=1 AND table_name='a'" "3"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
