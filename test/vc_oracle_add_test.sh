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

normalize() { tr -d '\r'; }

oracle() {
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
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"' | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_error_poststate() {
  local name="$1" dl_setup="$2" dl_call="$3" dl_query="$4" dolt_setup="${5:-$2}" dolt_call="${6:-$3}" dolt_query="${7:-$4}"
  local dir="$TMPROOT/${name}_posterr"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_setup
$dl_call"
  dl_rc=$?
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.post.err" \
      | tr -d '\r' \
      | grep '^Q|'
  )

  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$(vc_oracle_translate_for_dolt "$dolt_setup
$dolt_call")"
  dt_rc=$?
  local dt_out
  (
    cd "$dir/dt" || exit 1
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>"$dir/dt.post.err"
  ) > "$dir/dt.raw"
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"\r' | grep '^Q|')

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

oracle_same_session() {
  local name="$1" dl_setup="$2" dl_query="$3" dolt_setup="${4:-$2}" dolt_query="${5:-$3}"
  local dir="$TMPROOT/${name}_ss"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    {
      printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_setup" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>&1 \
      | tr -d '\r' \
      | awk '/^Q\|/ {print; next} /[Nn]o such savepoint:|SAVEPOINT .*does not exist/ {print "E|savepoint"}'
  )

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$(vc_oracle_translate_for_dolt "$dolt_setup")"
      printf '%s\n' "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>&1 \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk '/^Q\|/ {print; next} /[Nn]o such savepoint:|SAVEPOINT .*does not exist/ {print "E|savepoint"}'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_reopen() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_reopen"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.post.err" \
      | tr -d '\r' \
      | grep '^Q|'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  local dt_out
  (
    cd "$dir/dt" || exit 1
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>"$dir/dt.post.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^Q|')

  if [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ] && [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_add ==="
echo ""

echo "--- argument shapes ---"

oracle "single_table_by_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
"

oracle "two_tables_by_name" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a', 'b');
"

oracle "all_dash_capital_A" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('-A');
"

oracle "all_dash_lowercase_a" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('-a');
"

oracle "all_long_flag" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('--all');
"

oracle "dot_pathspec" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('.');
"

echo "--- idempotency and additivity ---"

oracle "idempotent_repeat" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_add('t');
"

oracle "additive_separate_calls" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a');
SELECT dolt_add('b');
"

oracle "stage_subset_leaves_others_unstaged" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1);
SELECT dolt_add('a');
"

echo "--- working tree advances past staged ---"

oracle "stage_then_modify_more" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
INSERT INTO t VALUES (3, 30);
"

echo "--- staging deletions ---"

oracle "stage_dropped_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
SELECT dolt_add('t');
"

oracle "stage_dropped_table_via_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE t;
SELECT dolt_add('-A');
"

echo "--- schema edge cases ---"

oracle "renamed_and_modified_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 'x');
SELECT dolt_add('b');
"

oracle "stage_renamed_and_modified_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 'x');
SELECT dolt_add('b');
"

oracle "recreated_same_name_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('a');
"

oracle "stage_recreated_table_as_modified" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('a');
"

oracle "all_mixed_deleted_and_modified" "
CREATE TABLE a(id INTEGER PRIMARY KEY);
CREATE TABLE b(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1);
INSERT INTO b VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE a;
INSERT INTO b VALUES (2, 'x');
SELECT dolt_add('-A');
"

oracle_error_poststate "mixed_valid_missing_preserves_working" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 'x');
" "SELECT dolt_add('b', 'missing');" "
SELECT 'Q|' || table_name || '|' || staged || '|' || status
  FROM dolt_status
 ORDER BY table_name, staged, status;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 'x');
" "CALL dolt_add('b', 'missing');" "
SELECT concat('Q|', table_name, '|', staged, '|', status)
  FROM dolt_status
 ORDER BY table_name, staged, status;"

oracle_error_poststate "missing_with_valid_path_preserves_unstaged_state" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
INSERT INTO b VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE a;
INSERT INTO b VALUES (2, 'x');
" "SELECT dolt_add('a', 'missing');" "
SELECT 'Q|' || table_name || '|' || staged || '|' || status
  FROM dolt_status
 ORDER BY table_name, staged, status;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
INSERT INTO b VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'seed');
DROP TABLE a;
INSERT INTO b VALUES (2, 'x');
" "CALL dolt_add('a', 'missing');" "
SELECT concat('Q|', table_name, '|', staged, '|', status)
  FROM dolt_status
 ORDER BY table_name, staged, status;"

echo "--- savepoint and reopen parity ---"

oracle_same_session "stage_dropped_table_savepoint_invalidated" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
SAVEPOINT sp1;
DROP TABLE a;
SELECT dolt_add('a');
" "ROLLBACK TO sp1;
SELECT 'Q|' || table_name || '|' || staged || '|' || status
  FROM dolt_status
 ORDER BY table_name, staged, status;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'seed');
SAVEPOINT sp1;
DROP TABLE a;
CALL dolt_add('a');
" "ROLLBACK TO sp1;
SELECT concat('Q|', table_name, '|', staged, '|', status)
  FROM dolt_status
 ORDER BY table_name, staged, status;"

oracle_same_session "stage_recreated_table_savepoint_invalidated" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
SAVEPOINT sp1;
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('a');
" "ROLLBACK TO sp1;
SELECT 'Q|' || table_name || '|' || staged || '|' || status
  FROM dolt_status
 ORDER BY table_name, staged, status;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'seed');
SAVEPOINT sp1;
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
CALL dolt_add('a');
" "ROLLBACK TO sp1;
SELECT concat('Q|', table_name, '|', staged, '|', status)
  FROM dolt_status
 ORDER BY table_name, staged, status;"

oracle_reopen "stage_renamed_and_modified_table_persists" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
ALTER TABLE a RENAME TO b;
INSERT INTO b VALUES (2, 'x');
SELECT dolt_add('b');
" "SELECT 'Q|' || table_name || '|' || staged || '|' || status
     FROM dolt_status
    ORDER BY table_name, staged, status;" \
"SELECT concat('Q|', table_name, '|', staged, '|', status)
   FROM dolt_status
  ORDER BY table_name, staged, status;"

oracle_reopen "stage_recreated_table_persists" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('a');
" "SELECT 'Q|' || table_name || '|' || staged || '|' || status
     FROM dolt_status
    ORDER BY table_name, staged, status;
SELECT 'Q|' || group_concat(name || ':' || replace(lower(type), 'integer', 'int'), '|')
  FROM pragma_table_info('a');
SELECT 'Q|' || k || '|' || n FROM a;" \
"SELECT concat('Q|', table_name, '|', staged, '|', status)
   FROM dolt_status
  ORDER BY table_name, staged, status;
SELECT concat('Q|', group_concat(concat(column_name, ':', replace(lower(column_type), 'integer', 'int')) ORDER BY ordinal_position SEPARATOR '|'))
  FROM information_schema.columns
 WHERE table_schema = database() AND table_name = 'a';
SELECT concat('Q|', k, '|', n) FROM a;"

echo "--- noop and clean states ---"

oracle "all_on_empty_repo" "
SELECT dolt_add('-A');
" "EXPECT_EMPTY"

oracle "all_after_commit_no_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
SELECT dolt_add('-A');
" "EXPECT_EMPTY"

echo "--- error paths ---"

oracle_error "no_args" "
SELECT dolt_add();
"

oracle_error "nonexistent_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('nonexistent');
"


# ── schema objects through staging ────────────────────────────────
# Views and triggers surface as dolt_schemas in status; index changes
# mark their parent table. Named adds must not stage either kind of
# entry-less object, while -A carries them.

oracle "view_unstaged_after_named_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
UPDATE t SET v = 11 WHERE id = 1;
CREATE VIEW vv AS SELECT id FROM t;
SELECT dolt_add('t');
"

oracle "view_staged_by_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE VIEW vv AS SELECT id FROM t;
SELECT dolt_add('-A');
"

oracle "view_dropped_then_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE VIEW vv AS SELECT id FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
DROP VIEW vv;
SELECT dolt_add('-A');
"

oracle "index_change_unstaged_after_named_add_other" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE o(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
INSERT INTO o VALUES (1);
SELECT dolt_add('o');
"

oracle "index_staged_with_named_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
SELECT dolt_add('t');
"

oracle "index_staged_by_all" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
SELECT dolt_add('-A');
"

oracle "index_staged_then_more_data" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX iv ON t(v);
SELECT dolt_add('t');
INSERT INTO t VALUES (2, 20);
"

echo "--- rename identity: content pairing, one numbering domain ---"

DROP_CREATE_SEED="
CREATE TABLE m(a INT PRIMARY KEY, b INT);
CREATE TABLE n(a INT PRIMARY KEY);
INSERT INTO m VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
DROP TABLE m;
CREATE TABLE a(x INT PRIMARY KEY, y TEXT);
INSERT INTO a VALUES (9, 'nine');
"

oracle "add_named_keeps_unstaged_drop_out" "
$DROP_CREATE_SEED
SELECT dolt_add('a');
"

oracle_reopen "add_named_then_commit_keeps_dropped_table_in_head" "
$DROP_CREATE_SEED
SELECT dolt_add('a');
SELECT dolt_commit('-m', 'add a only');
" "SELECT concat('Q|', table_name, '|', staged, '|', status) FROM dolt_status;"

RENAME_SHIFT_SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE u(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
INSERT INTO u VALUES (2, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
ALTER TABLE t RENAME TO z;
"

oracle "add_bystander_during_order_shifting_rename" "
$RENAME_SHIFT_SEED
INSERT INTO u VALUES (3, 3);
SELECT dolt_add('u');
"

oracle_reopen "add_bystander_then_commit_preserves_rename" "
$RENAME_SHIFT_SEED
INSERT INTO u VALUES (3, 3);
SELECT dolt_add('u');
SELECT dolt_commit('-m', 'commit u only');
" "SELECT concat('Q|', table_name, '|', staged, '|', status) FROM dolt_status;"

oracle "add_renamed_table_order_shifting" "
$RENAME_SHIFT_SEED
SELECT dolt_add('z');
"

oracle_reopen "add_renamed_table_then_commit_renames_in_head" "
$RENAME_SHIFT_SEED
SELECT dolt_add('z');
SELECT dolt_commit('-m', 'commit rename');
" "SELECT concat('Q|z|', id, '|', v) FROM z ORDER BY id;
SELECT concat('Q|u|', id, '|', v) FROM u ORDER BY id;
SELECT concat('Q|nstatus|', count(*)) FROM dolt_status;"

oracle "add_named_empty_drop_create_not_a_rename" "
CREATE TABLE e1(a INT PRIMARY KEY);
CREATE TABLE n(a INT PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
DROP TABLE e1;
CREATE TABLE e2(b VARCHAR(32) PRIMARY KEY, c INT);
SELECT dolt_add('e2');
"

oracle "add_ambiguous_content_match_is_not_a_rename" "
CREATE TABLE orig(id INT PRIMARY KEY, v INT);
INSERT INTO orig VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
DROP TABLE orig;
CREATE TABLE alpha(id INT PRIMARY KEY, v INT);
INSERT INTO alpha VALUES (1, 10);
CREATE TABLE beta(id INT PRIMARY KEY, v INT);
INSERT INTO beta VALUES (1, 10);
SELECT dolt_add('alpha');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
