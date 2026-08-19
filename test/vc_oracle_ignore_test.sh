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

normalize() { tr -d '\r' | grep -v '^S|dolt_ignore|' | sort; }

oracle() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S|' || table_name || '|' || staged || '|' || status FROM dolt_status;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S|' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\nSELECT concat('S|', table_name, '|', staged, '|', status) FROM dolt_status;\n" "$dolt_setup" \
      | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(grep '^S|' "$dir/dt.raw" | tr -d '"' | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

doltlite_schema_reject() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_rej"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if grep -qi 'dolt_ignore' "$dir/out" \
     && grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite to reject)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
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

echo "=== Version Control Oracle Tests: dolt_ignore ==="
echo ""

echo "--- schema & bootstrap ---"

oracle "empty_ignore" "
CREATE TABLE t(x INT PRIMARY KEY);
CREATE TABLE u(x INT PRIMARY KEY);
"

echo "--- basic pattern matching ---"

oracle "literal" "
INSERT INTO dolt_ignore VALUES ('secret', 1);
CREATE TABLE secret(x INT PRIMARY KEY);
CREATE TABLE public(x INT PRIMARY KEY);
"

oracle "star_wild" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_a(x INT PRIMARY KEY);
CREATE TABLE tmp_b(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle "percent_wild" "
INSERT INTO dolt_ignore VALUES ('tmp_%', 1);
CREATE TABLE tmp_a(x INT PRIMARY KEY);
CREATE TABLE tmp_b(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle "trailing_wild" "
INSERT INTO dolt_ignore VALUES ('%_temp', 1);
CREATE TABLE foo_temp(x INT PRIMARY KEY);
CREATE TABLE foo(x INT PRIMARY KEY);
"

oracle "mid_wild" "
INSERT INTO dolt_ignore VALUES ('a_%_z', 1);
CREATE TABLE a_b_z(x INT PRIMARY KEY);
CREATE TABLE a_bc_z(x INT PRIMARY KEY);
CREATE TABLE a_b(x INT PRIMARY KEY);
CREATE TABLE b_z(x INT PRIMARY KEY);
"

oracle "question_mark" "
INSERT INTO dolt_ignore VALUES ('d_?', 1);
CREATE TABLE d_x(x INT PRIMARY KEY);
CREATE TABLE d_xy(x INT PRIMARY KEY);
CREATE TABLE d_(x INT PRIMARY KEY);
"

oracle "question_mark_multi" "
INSERT INTO dolt_ignore VALUES ('e??', 1);
CREATE TABLE eab(x INT PRIMARY KEY);
CREATE TABLE ea(x INT PRIMARY KEY);
CREATE TABLE eabc(x INT PRIMARY KEY);
"

oracle "question_mark_utf8" "
INSERT INTO dolt_ignore VALUES ('tmp_?', 1);
CREATE TABLE \"tmp_é\"(x INT PRIMARY KEY);
CREATE TABLE tmp_xy(x INT PRIMARY KEY);
"

oracle "case_sensitive" "
INSERT INTO dolt_ignore VALUES ('TMP_*', 1);
CREATE TABLE tmp_a(x INT PRIMARY KEY);
"

echo "--- specificity ---"

oracle "exact_over_wild" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
INSERT INTO dolt_ignore VALUES ('tmp_keep', 0);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE tmp_keep(x INT PRIMARY KEY);
"

oracle "cascade" "
INSERT INTO dolt_ignore VALUES ('*', 1);
INSERT INTO dolt_ignore VALUES ('data_*', 0);
INSERT INTO dolt_ignore VALUES ('data_secret', 1);
CREATE TABLE data_public(x INT PRIMARY KEY);
CREATE TABLE data_secret(x INT PRIMARY KEY);
CREATE TABLE random_thing(x INT PRIMARY KEY);
"

oracle "unignore_then_ignore_again" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('foo_keep_%', 0);
INSERT INTO dolt_ignore VALUES ('foo_keep_never', 1);
CREATE TABLE foo_drop(x INT PRIMARY KEY);
CREATE TABLE foo_keep_a(x INT PRIMARY KEY);
CREATE TABLE foo_keep_never(x INT PRIMARY KEY);
"

echo "--- conflicts ---"

oracle_error "conflict_two_wild" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('%_bar', 0);
CREATE TABLE foo_bar(x INT PRIMARY KEY);
SELECT table_name FROM dolt_status;
"

oracle_error "conflict_dolt_add_A" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('%_bar', 0);
CREATE TABLE foo_bar(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

oracle_error "conflict_dolt_add_name" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('%_bar', 0);
CREATE TABLE foo_bar(x INT PRIMARY KEY);
SELECT dolt_add('foo_bar');
"

oracle_error "conflict_incomparable_specificity" "
INSERT INTO dolt_ignore VALUES ('a*bc', 1);
INSERT INTO dolt_ignore VALUES ('ab*', 0);
CREATE TABLE abxbc(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

echo "--- dolt_add ---"

oracle "add_dash_A_skips_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

oracle "add_dot_skips_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('.');
"

oracle "add_explicit_ignored_is_noop" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('tmp_foo');
"

oracle "add_explicit_non_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('keep');
"

echo "--- dolt_commit -A ---"

oracle "commit_A_skips_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
INSERT INTO tmp_foo VALUES (1);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'first');
CREATE TABLE tmp_bar(x INT PRIMARY KEY);
INSERT INTO keep VALUES (1);
"

echo "--- scope: only gates new tables ---"

oracle "already_tracked_still_shows" "
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
INSERT INTO tmp_foo VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add pattern');
INSERT INTO tmp_foo VALUES (2);
"

oracle "tracked_ignored_not_staged_by_A" "
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
INSERT INTO tmp_foo VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add pattern');
INSERT INTO tmp_foo VALUES (2);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

echo "--- dynamic pattern changes ---"

oracle "remove_pattern_exposes" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
DELETE FROM dolt_ignore WHERE pattern='tmp_*';
"

echo "--- persistence ---"

oracle "persists_across_commit" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

echo "--- no special-case for dolt_ignore ---"

oracle "star_hides_dolt_ignore" "
INSERT INTO dolt_ignore VALUES ('*', 1);
CREATE TABLE foo(x INT PRIMARY KEY);
" "EXPECT_EMPTY"

echo "--- schema guard ---"

doltlite_schema_reject "schema_one_column" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL PRIMARY KEY);
"

doltlite_schema_reject "schema_three_columns" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, extra INT, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_wrong_first_name" "
CREATE TABLE dolt_ignore(foo TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(foo));
"

doltlite_schema_reject "schema_wrong_second_name" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, flagged TINYINT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_wrong_pattern_type" "
CREATE TABLE dolt_ignore(pattern INTEGER NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_wrong_ignored_type" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TEXT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_pattern_nullable" "
CREATE TABLE dolt_ignore(pattern TEXT, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_ignored_nullable" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_compound_pk" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern, ignored));
"

doltlite_schema_reject "schema_no_pk" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL);
"

doltlite_schema_accept() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_acc"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if ! grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite to accept)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_schema_accept "schema_varchar_boolean" "
CREATE TABLE dolt_ignore(pattern VARCHAR(255) NOT NULL, ignored BOOLEAN NOT NULL, PRIMARY KEY(pattern));
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT * FROM dolt_ignore;
"

doltlite_schema_accept "schema_integer_ignored" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored INTEGER NOT NULL, PRIMARY KEY(pattern));
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT * FROM dolt_ignore;
"

doltlite_runtime_expect() {
  local name="$1" sql="$2" expect="$3"
  local dir="$TMPROOT/${name}_rt"
  mkdir -p "$dir/dl"
  printf "%s\n.headers off\n.mode list\n.separator '|'\nSELECT table_name || '|' || staged || '|' || status FROM dolt_status;\n" "$sql" \
    | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  local got
  got=$(grep '^[^|].*|' "$dir/out" | grep -v '^dolt_ignore|' | sort)
  if [ "$got" = "$expect" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (unexpected doltlite status)"
    echo "    expected:"; echo "$expect" | sed 's/^/      /'
    echo "    got:"; echo "$got" | sed 's/^/      /'
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_runtime_reject() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_rtrej"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if grep -qi 'unexpected schema' "$dir/out" \
     && grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite runtime reject)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_runtime_expect "temp_shadow_ignored_main_wins" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TEMP TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
INSERT INTO temp.dolt_ignore VALUES ('tmp_*', 0);
CREATE TABLE tmp_shadowed(x INT PRIMARY KEY);
" ""

doltlite_runtime_reject "runtime_wrong_shape_view" "
CREATE VIEW dolt_ignore AS SELECT 'tmp_*' AS pattern;
CREATE TABLE tmp_bad(x INT PRIMARY KEY);
SELECT * FROM dolt_status;
"

echo "--- cross-branch + merge + reset ---"

oracle "pattern_survives_branch_create" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'add pattern');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle "pattern_different_on_branches" "
SELECT dolt_checkout('-b', 'bA');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'bA ignores tmp_');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b', 'bB');
INSERT INTO dolt_ignore VALUES ('cache_*', 1);
SELECT dolt_commit('-A', '-m', 'bB ignores cache_');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE cache_bar(x INT PRIMARY KEY);
"

oracle "pattern_committed_then_reset" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'add pattern');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
SELECT dolt_reset('--hard', 'HEAD~1');
CREATE TABLE tmp_bar(x INT PRIMARY KEY);
"

oracle "merge_adds_pattern_ff" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'feature adds pattern');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
CREATE TABLE tmp_new(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle_error "merge_conflicting_patterns" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'b1');
INSERT INTO dolt_ignore VALUES ('shared_pat', 1);
SELECT dolt_commit('-A', '-m', 'b1 ignores shared_pat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b', 'b2');
INSERT INTO dolt_ignore VALUES ('shared_pat', 0);
SELECT dolt_commit('-A', '-m', 'b2 unignores shared_pat');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
