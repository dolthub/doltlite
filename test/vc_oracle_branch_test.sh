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
  tr -d '\r' | awk -F'\t' '
    {
      h = $2
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $2 = seen[h]
      print $1 "\t" $2 "\t" $3
    }
  '
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT name || char(9) || hash || char(9) || dirty FROM dolt_branches ORDER BY name'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
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
    "$DOLT" sql -r csv -q "SELECT concat(name, char(9), hash, char(9), dirty) FROM dolt_branches ORDER BY name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" \
           | tr -d '"' \
           | sed -E 's/\ttrue$/\t1/; s/\tfalse$/\t0/' \
           | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
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

oracle_with_rows() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_rows"
  mkdir -p "$dir/dl" "$dir/dt"

  local q_br='SELECT name || char(9) || hash || char(9) || dirty FROM dolt_branches ORDER BY name'
  local q_rows="SELECT id || char(9) || v FROM t ORDER BY id"

  local dl_br dl_rows
  dl_br=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q_br" \
          | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
          | grep -v '^[0-9]*$' \
          | grep -v '^[0-9a-f]\{40\}$' \
          | normalize)
  dl_rows=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q_rows" \
            | "$DOLTLITE" "$dir/dl/db.rows" 2>>"$dir/dl.err" \
            | grep -v '^[0-9]*$' \
            | grep -v '^[0-9a-f]\{40\}$')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "SELECT concat(name, char(9), hash, char(9), dirty) FROM dolt_branches ORDER BY name;"
      echo "SELECT concat(id, char(9), v) FROM t ORDER BY id;"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_br dt_rows
  dt_br=$(tr -d '"' < "$dir/dt.raw" \
          | awk -F'\t' 'NF==3 && $1 !~ /^[0-9]+$/ {print}' \
          | sed -E 's/\ttrue$/\t1/; s/\tfalse$/\t0/' \
          | normalize)
  dt_rows=$(tr -d '"' < "$dir/dt.raw" \
            | awk -F'\t' 'NF==2 && $1 ~ /^[0-9]+$/ {print}')

  local dl_combined dt_combined
  dl_combined="$dl_br"$'\n'"$dl_rows"
  dt_combined="$dt_br"$'\n'"$dt_rows"

  vc_oracle_assert_match "$name" "$dl_combined" "$dt_combined"
}

oracle_same_session() {
  local name="$1" setup="$2" dl_query="${3:-}"
  local dolt_query="${4:-$dl_query}"
  local dir="$TMPROOT/${name}_ss"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    {
      printf "%s\n.headers off\n.mode list\n.separator '\t'\n" "$setup"
      if [ -n "$dl_query" ]; then
        printf "%s\n" "$dl_query"
      fi
    } | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
      | tr -d '\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf "%s\n" "$dolt_setup"
      if [ -n "$dolt_query" ]; then
        printf "%s\n" "$dolt_query"
      fi
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk -F'\t' '$1=="Q"{print}'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_branch ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- create ---"

oracle "create_simple" "
$SEED
SELECT dolt_branch('feature');
"

oracle "create_after_end_options" "
$SEED
SELECT dolt_branch('--', 'feature');
"

oracle "create_at_start_point" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_branch('historical', (SELECT commit_hash FROM dolt_log WHERE message='c1'));
"

oracle "create_at_tag_ref" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_tag('v1', 'HEAD~1');
SELECT dolt_branch('from_tag', 'v1');
"

oracle "create_at_parent_ref" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
SELECT dolt_merge('feature');
SELECT dolt_branch('p1', 'HEAD^1');
SELECT dolt_branch('p2', 'HEAD^2');
"

oracle "create_from_branch_ref" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('copy_of_feature', 'feature');
"

echo "--- delete ---"

oracle "delete_existing" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-d', 'feature');
"

oracle "delete_multiple" "
$SEED
SELECT dolt_branch('one');
SELECT dolt_branch('two');
SELECT dolt_branch('-d', 'one', 'two');
"

oracle "delete_force" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-D', 'feature');
"

oracle_error "delete_unmerged_requires_force" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
SELECT dolt_branch('-d', 'feature');
"

oracle "delete_unmerged_force" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
SELECT dolt_branch('-D', 'feature');
"

oracle "delete_multiple_unmerged_force" "
$SEED
SELECT dolt_branch('one');
SELECT dolt_branch('two');
SELECT dolt_checkout('one');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'one');
SELECT dolt_checkout('main');
SELECT dolt_branch('-D', 'one', 'two');
"

echo "--- copy ---"

oracle "copy_from_main" "
$SEED
SELECT dolt_branch('-c', 'main', 'copy');
"

oracle "copy_long_flag" "
$SEED
SELECT dolt_branch('--copy', 'main', 'copy');
"

echo "--- move / rename ---"

oracle "move_non_current" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-m', 'feature', 'renamed');
"

oracle "move_current_branch" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
SELECT dolt_branch('-m', 'feature', 'renamed');
"

oracle "move_long_flag" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('--move', 'feature', 'other');
"

echo "--- force create ---"

oracle "force_create_overwrites" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_branch('-f', 'feature');
"

oracle_with_rows "force_create_resets_destination_working_set" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_checkout('main');
SELECT dolt_branch('-f', 'feature', 'HEAD');
SELECT dolt_checkout('feature');
"

oracle_with_rows "force_copy_resets_destination_working_set" "
$SEED
SELECT dolt_branch('destination');
SELECT dolt_checkout('destination');
INSERT INTO t VALUES (2, 20);
SELECT dolt_checkout('main');
SELECT dolt_branch('-c', '-f', 'main', 'destination');
SELECT dolt_checkout('destination');
"

oracle_with_rows "force_move_replaces_destination_working_set" "
$SEED
SELECT dolt_branch('destination');
SELECT dolt_checkout('destination');
INSERT INTO t VALUES (2, 20);
SELECT dolt_checkout('main');
SELECT dolt_branch('source');
SELECT dolt_checkout('source');
INSERT INTO t VALUES (3, 30);
SELECT dolt_branch('-m', '-f', 'source', 'destination');
"

echo "--- explicit transaction parity ---"

oracle_with_rows "branch_create_inside_txn_seals_row_state" "
$SEED
BEGIN;
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_branch('txb');
ROLLBACK;
"

echo "--- savepoint error parity ---"

oracle_same_session "delete_current_inside_savepoint_invalidates" "
$SEED
SAVEPOINT sp1;
SELECT dolt_branch('-d', 'main');
SELECT 'Q' || char(9) || 'after_delete' || char(9) || active_branch();
ROLLBACK TO sp1;
SELECT 'Q' || char(9) || 'after_rb' || char(9) || active_branch();
" "" "SELECT concat('Q', char(9), 'after_delete', char(9), active_branch());
ROLLBACK TO sp1;
SELECT concat('Q', char(9), 'after_rb', char(9), active_branch());"

oracle_same_session "delete_missing_inside_savepoint_invalidates" "
$SEED
SAVEPOINT sp1;
SELECT dolt_branch('-d', 'nope');
SELECT 'Q' || char(9) || 'after_delete' || char(9) || active_branch();
ROLLBACK TO sp1;
SELECT 'Q' || char(9) || 'after_rb' || char(9) || active_branch();
" "" "SELECT concat('Q', char(9), 'after_delete', char(9), active_branch());
ROLLBACK TO sp1;
SELECT concat('Q', char(9), 'after_rb', char(9), active_branch());"

echo "--- error paths ---"

oracle_error "delete_nonexistent" "
$SEED
SELECT dolt_branch('-d', 'nope');
"

oracle_error "force_delete_nonexistent" "
$SEED
SELECT dolt_branch('-D', 'nope');
"

oracle_error "copy_source_missing" "
$SEED
SELECT dolt_branch('-c', 'nope', 'dest');
"

oracle_error "move_source_missing" "
$SEED
SELECT dolt_branch('-m', 'nope', 'dest');
"

oracle_error "create_duplicate" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('feature');
"

oracle_error "create_empty_name" "
$SEED
SELECT dolt_branch('');
"

oracle_error "create_reserved_head" "
$SEED
SELECT dolt_branch('HEAD');
"

oracle_error "create_reserved_lower_head" "
$SEED
SELECT dolt_branch('head');
"

oracle_error "create_invalid_double_dot" "
$SEED
SELECT dolt_branch('bad..name');
"

oracle_error "create_invalid_dot_component" "
$SEED
SELECT dolt_branch('feature/.hidden');
"

oracle_error "create_invalid_lock_suffix" "
$SEED
SELECT dolt_branch('feature.lock');
"

oracle_error "create_invalid_space" "
$SEED
SELECT dolt_branch('bad name');
"

oracle_error "create_invalid_reflog_syntax" "
$SEED
SELECT dolt_branch('bad@{name}');
"

oracle_error "copy_empty_source" "
$SEED
SELECT dolt_branch('-c', '', 'dest');
"

oracle_error "copy_empty_dest" "
$SEED
SELECT dolt_branch('-c', 'main', '');
"

oracle_error "copy_invalid_dest" "
$SEED
SELECT dolt_branch('-c', 'main', 'bad..copy');
"

oracle_error "move_empty_source" "
$SEED
SELECT dolt_branch('-m', '', 'dest');
"

oracle_error "move_empty_dest" "
$SEED
SELECT dolt_branch('-m', 'main', '');
"

oracle_error "move_invalid_dest" "
$SEED
SELECT dolt_branch('source');
SELECT dolt_branch('-m', 'source', 'bad..move');
"

oracle_error "create_extra_arg" "
$SEED
SELECT dolt_branch('feature', 'main', 'extra');
"

oracle_error "copy_extra_arg" "
$SEED
SELECT dolt_branch('src');
SELECT dolt_branch('-c', 'src', 'dest', 'extra');
"

oracle_error "move_extra_arg" "
$SEED
SELECT dolt_branch('src');
SELECT dolt_branch('-m', 'src', 'dest', 'extra');
"

oracle_error "delete_multiple_with_missing" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_branch('-d', 'feature', 'missing');
"

oracle_error "no_args" "
SELECT dolt_branch();
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
