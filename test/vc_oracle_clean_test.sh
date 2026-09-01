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
  tr -d '\r"' \
    | awk -F'\t' 'NF >= 4 && $1 == "S" { print }
        NF >= 2 && $1 == "T" && $2 !~ /^(dolt_|sqlite_)/ { print }' \
    | sort
}

oracle() {
  local name="$1" setup="$2"
  local compare="${3:-all}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt" "$dir/dt.rows"

  local dl_status dl_rows
  dl_status=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S' || char(9) || table_name || char(9) || staged || char(9) || status FROM dolt_status;\nSELECT 'T' || char(9) || name FROM pragma_table_list WHERE schema='main' AND type IN ('table','virtual');\n" "$setup" \
    | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" | normalize_state)
  dl_rows=$(printf "%s\nSELECT count(*) FROM t;\n" "$setup" \
    | "$DOLTLITE" "$dir/dl.rows" 2>>"$dir/dl.err" | tail -1)

  local dolt_setup dt_status dt_rows
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;" 2>>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('T', char(9), table_name) FROM information_schema.tables WHERE table_schema=database() AND table_type='BASE TABLE';" 2>>"$dir/dt.err"
  ) >"$dir/dt.status"
  dt_status=$(tail -n +2 "$dir/dt.status" | normalize_state)
  if [ "$compare" = "tables" ]; then
    dl_status=$(printf '%s\n' "$dl_status" | awk -F'\t' '$1 == "T"')
    dt_status=$(printf '%s\n' "$dt_status" | awk -F'\t' '$1 == "T"')
  fi
  (
    cd "$dir/dt.rows" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.rows.err"
    "$DOLT" sql -r csv -q "SELECT count(*) FROM t;" 2>>"$dir/dt.rows.err"
  ) >"$dir/dt.rows.out"
  dt_rows=$(tail -n +2 "$dir/dt.rows.out" | tr -d '\r"')

  vc_oracle_assert_match "$name" "$dl_status|$dl_rows" "$dt_status|$dt_rows"
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"
  local dl_rc dt_rc dolt_setup
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error; doltlite=$dl_rc dolt=$dt_rc)"
  fi
}

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
"

echo "=== Version Control Oracle Tests: dolt_clean ==="

oracle "clean_named" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
CREATE TABLE v(id INTEGER PRIMARY KEY);
SELECT dolt_clean('u');
"

oracle "clean_all" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
CREATE TABLE v(id INTEGER PRIMARY KEY);
SELECT dolt_clean();
"

oracle "clean_all_preserves_ignored" "
$SEED
INSERT INTO dolt_ignore VALUES ('ig_%', 1);
CREATE TABLE ig_temp(id INTEGER PRIMARY KEY);
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_clean();
" tables

oracle "clean_named_overrides_ignore" "
$SEED
INSERT INTO dolt_ignore VALUES ('ig_%', 1);
CREATE TABLE ig_temp(id INTEGER PRIMARY KEY);
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_clean('ig_temp');
" tables

oracle "clean_dry_run" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_clean('--dry-run');
"

oracle "clean_keeps_staged_new" "
$SEED
CREATE TABLE staged_new(id INTEGER PRIMARY KEY);
SELECT dolt_add('staged_new');
CREATE TABLE unstaged_new(id INTEGER PRIMARY KEY);
SELECT dolt_clean();
"

oracle "clean_named_tracked_keeps_changes" "
$SEED
INSERT INTO t VALUES (2, 'working');
SELECT dolt_clean('t');
"

oracle "clean_unstaged_rename" "
$SEED
ALTER TABLE t RENAME TO renamed_t;
SELECT dolt_clean();
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
"

oracle "clean_untracked_foreign_key_pair" "
$SEED
CREATE TABLE z_parent(id INTEGER PRIMARY KEY);
CREATE TABLE a_child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES z_parent(id));
INSERT INTO z_parent VALUES (1);
INSERT INTO a_child VALUES (1, 1);
SELECT dolt_clean();
"

oracle_error "clean_unknown" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_clean('missing');
"

oracle_error "clean_null" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_clean(NULL);
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
