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

normalize_tests() { tr -d '\r"' | grep '^T|' | sort; }
normalize_results() { tr -d '\r"' | grep '^R|'; }
normalize_status() { tr -d '\r"' | grep '^S|' | sort; }

TESTS_QUERY_DL="SELECT 'T|' || test_name || '|' || coalesce(test_group,'') || '|' || test_query || '|' || assertion_type || '|' || assertion_comparator || '|' || coalesce(assertion_value,'NULL') FROM dolt_tests;"
TESTS_QUERY_DT="SELECT concat('T|', test_name, '|', coalesce(test_group,''), '|', test_query, '|', assertion_type, '|', assertion_comparator, '|', coalesce(assertion_value,'NULL')) FROM dolt_tests;"
STATUS_QUERY_DL="SELECT 'S|' || table_name || '|' || staged || '|' || status FROM dolt_status;"
STATUS_QUERY_DT="SELECT concat('S|', table_name, '|', staged, '|', status) FROM dolt_status;"

oracle_query() {
  local kind="$1" name="$2" setup="$3" query_dl="$4" query_dt="$5" allow_empty="${6:-}"
  local dir="$TMPROOT/$name"
  local norm
  [ "$kind" = "tests" ] && norm=normalize_tests
  [ "$kind" = "results" ] && norm=normalize_results
  [ "$kind" = "status" ] && norm=normalize_status
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query_dl" \
    | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" | $norm)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n%s\n" "$dolt_setup" "$query_dt" \
      | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) >"$dir/dt.raw"
  local dt_out
  dt_out=$($norm < "$dir/dt.raw")

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_tests() { oracle_query tests "$1" "$2" "$TESTS_QUERY_DL" "$TESTS_QUERY_DT" "${3:-}"; }
oracle_status() { oracle_query status "$1" "$2" "$STATUS_QUERY_DL" "$STATUS_QUERY_DT" "${3:-}"; }

oracle_results() {
  local name="$1" setup="$2" args="${3:-}"
  local dl="SELECT 'R|' || test_name || '|' || test_group_name || '|' || query || '|' || status || '|' || message FROM dolt_test_run($args);"
  local dt="SELECT concat('R|', test_name, '|', test_group_name, '|', query, '|', status, '|', message) FROM dolt_test_run($args);"
  oracle_query results "$name" "$setup" "$dl" "$dt"
}

oracle_error() {
  local name="$1" setup="$2" query_dl="$3" query_dt="$4"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup
$query_dl"
  local dl_rc=$?
  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup
$query_dt"
  local dt_rc=$?
  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error; doltlite=$dl_rc dolt=$dt_rc)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_tests and dolt_test_run ==="
echo ""

echo "--- lazy table and writes ---"

oracle_tests "fresh_select_empty" "SELECT * FROM dolt_tests;" "EXPECT_EMPTY"
oracle_status "fresh_read_no_status" "SELECT * FROM dolt_tests;" "EXPECT_EMPTY"

oracle_tests "insert_materializes" "
INSERT INTO dolt_tests VALUES ('row_count', 'basic', 'SELECT 1', 'expected_rows', '==', '1');
"

oracle_status "zero_row_delete_materializes" "
DELETE FROM dolt_tests WHERE test_name='missing';
"

oracle_query tests "rollback_first_write" "
BEGIN;
INSERT INTO dolt_tests VALUES ('rolled',NULL,'SELECT 1','expected_rows','==','1');
ROLLBACK;
" \
  "SELECT 'T|' || (SELECT count(*) FROM dolt_tests) || '|' ||
      (SELECT count(*) FROM dolt_status WHERE table_name='dolt_tests');" \
  "SELECT concat('T|', (SELECT count(*) FROM dolt_tests), '|',
      (SELECT count(*) FROM dolt_status WHERE table_name='dolt_tests'));"

oracle_query tests "rollback_after_zero_row_materialization" "
DELETE FROM dolt_tests WHERE test_name='missing';
BEGIN;
INSERT INTO dolt_tests VALUES ('rolled',NULL,'SELECT 1','expected_rows','==','1');
ROLLBACK;
" \
  "SELECT 'T|' || (SELECT count(*) FROM dolt_tests) || '|' ||
      (SELECT count(*) FROM dolt_status WHERE table_name='dolt_tests');" \
  "SELECT concat('T|', (SELECT count(*) FROM dolt_tests), '|',
      (SELECT count(*) FROM dolt_status WHERE table_name='dolt_tests'));"

oracle_tests "update_replace_delete" "
INSERT INTO dolt_tests VALUES ('one', 'g', 'SELECT 1', 'expected_rows', '==', '1');
INSERT INTO dolt_tests VALUES ('two', NULL, 'SELECT 2', 'expected_single_value', '==', '2');
UPDATE dolt_tests SET test_group='updated' WHERE test_name='one';
REPLACE INTO dolt_tests VALUES ('two', 'replaced', 'SELECT 3', 'expected_single_value', '==', '3');
DELETE FROM dolt_tests WHERE test_name='one';
"

oracle_tests "nullable_group_value" "
INSERT INTO dolt_tests(test_name,test_query,assertion_type,assertion_comparator)
VALUES ('null_value', 'SELECT NULL', 'expected_single_value', '==');
"

oracle_error "duplicate_pk" \
  "INSERT INTO dolt_tests VALUES ('dup',NULL,'SELECT 1','expected_rows','==','1');" \
  "INSERT INTO dolt_tests VALUES ('dup',NULL,'SELECT 1','expected_rows','==','1');" \
  "INSERT INTO dolt_tests VALUES ('dup',NULL,'SELECT 1','expected_rows','==','1');"

oracle_error "invalid_assertion" "" \
  "INSERT INTO dolt_tests VALUES ('bad',NULL,'SELECT 1','row_count','==','1');" \
  "INSERT INTO dolt_tests VALUES ('bad',NULL,'SELECT 1','row_count','==','1');"

oracle_error "invalid_comparator" "" \
  "INSERT INTO dolt_tests VALUES ('bad',NULL,'SELECT 1','expected_rows','=','1');" \
  "INSERT INTO dolt_tests VALUES ('bad',NULL,'SELECT 1','expected_rows','=','1');"

echo "--- version control ---"

oracle_status "clean_after_commit" "
INSERT INTO dolt_tests VALUES ('committed',NULL,'SELECT 1','expected_rows','==','1');
SELECT dolt_commit('-A','-m','add test');
" "EXPECT_EMPTY"

oracle_status "modified_after_commit" "
INSERT INTO dolt_tests VALUES ('committed',NULL,'SELECT 1','expected_rows','==','1');
SELECT dolt_commit('-A','-m','add test');
UPDATE dolt_tests SET assertion_value='2';
"

oracle_tests "branch_isolation" "
INSERT INTO dolt_tests VALUES ('main_test',NULL,'SELECT 1','expected_rows','==','1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feature');
INSERT INTO dolt_tests VALUES ('feature_test',NULL,'SELECT 1','expected_rows','==','1');
SELECT dolt_checkout('main');
"

oracle_tests "clean_merge" "
INSERT INTO dolt_tests VALUES ('base',NULL,'SELECT 1','expected_rows','==','1');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_checkout('-b','feature');
INSERT INTO dolt_tests VALUES ('feature',NULL,'SELECT 1','expected_rows','==','1');
SELECT dolt_commit('-A','-m','feature');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- runner selection and assertions ---"

BASE_SETUP="
CREATE TABLE fixture(i INT PRIMARY KEY, s TEXT);
INSERT INTO fixture VALUES (1,'one'),(2,'two');
INSERT INTO dolt_tests VALUES
 ('a_rows_pass','group_a','SELECT i FROM fixture','expected_rows','==','2'),
 ('b_rows_fail','group_a','SELECT i FROM fixture','expected_rows','==','3'),
 ('c_cols_pass','group_b','SELECT i,s FROM fixture LIMIT 1','expected_columns','==','2'),
 ('d_value_pass','group_b','SELECT count(*) FROM fixture','expected_single_value','>=','2'),
 ('e_text_pass',NULL,'SELECT s FROM fixture WHERE i=1','expected_single_value','==','one');
"

oracle_results "no_args_runs_all" "$BASE_SETUP"
oracle_results "wildcard_runs_all" "$BASE_SETUP" "'*'"
oracle_results "individual_name" "$BASE_SETUP" "'e_text_pass'"
oracle_results "group_name" "$BASE_SETUP" "'group_b'"
oracle_results "multiple_arguments" "$BASE_SETUP" "'e_text_pass','group_b'"

oracle_results "all_comparators" "
INSERT INTO dolt_tests VALUES
 ('eq','cmp','SELECT 2','expected_single_value','==','2'),
 ('ne','cmp','SELECT 2','expected_single_value','!=','3'),
 ('lt','cmp','SELECT 2','expected_single_value','<','3'),
 ('le','cmp','SELECT 2','expected_single_value','<=','2'),
 ('gt','cmp','SELECT 2','expected_single_value','>','1'),
 ('ge','cmp','SELECT 2','expected_single_value','>=','2');
" "'cmp'"

oracle_results "null_values" "
INSERT INTO dolt_tests(test_name,test_group,test_query,assertion_type,assertion_comparator) VALUES
 ('null_eq','nulls','SELECT NULL','expected_single_value','=='),
 ('null_ne','nulls','SELECT NULL','expected_single_value','!=');
" "'nulls'"

oracle_results "result_shape_errors" "
CREATE TABLE fixture(i INT PRIMARY KEY, s TEXT);
INSERT INTO fixture VALUES (1,'one'),(2,'two');
INSERT INTO dolt_tests VALUES
 ('many_cols','shape','SELECT i,s FROM fixture LIMIT 1','expected_single_value','==','1'),
 ('many_rows','shape','SELECT i FROM fixture','expected_single_value','==','1'),
 ('zero_rows','shape','SELECT i FROM fixture WHERE 0','expected_single_value','==','1');
" "'shape'"

oracle_results "query_validation" "
INSERT INTO dolt_tests VALUES
 ('multi','validation','SELECT 1; SELECT 2','expected_rows','==','1'),
 ('recursive','validation','SELECT * FROM dolt_test_run()','expected_rows','==','1'),
 ('write','validation','CREATE TABLE nope(x INT)','expected_rows','==','1');
" "'validation'"

oracle_query results "attach_rejected" "
INSERT INTO dolt_tests VALUES
 ('attach',NULL,'ATTACH '':memory:'' AS aux','expected_rows','==','0');
" \
  "SELECT 'R|' || test_name || '|' || status FROM dolt_test_run('attach');" \
  "SELECT concat('R|', test_name, '|', status) FROM dolt_test_run('attach');"

oracle_results "non_integer_counts" "
INSERT INTO dolt_tests VALUES
 ('bad_cols','bad_int','SELECT 1','expected_columns','==','0.5'),
 ('bad_rows','bad_int','SELECT 1','expected_rows','==','0.5');
" "'bad_int'"

oracle_results "scalar_types" "
CREATE TABLE typed_values(d DATE, f DOUBLE, b BOOLEAN, s TEXT);
INSERT INTO typed_values VALUES ('2025-08-22',3.14159,true,'String');
INSERT INTO dolt_tests VALUES
 ('boolean','types','SELECT b FROM typed_values','expected_single_value','==','true'),
 ('date','types','SELECT d FROM typed_values','expected_single_value','<','2025-08-23'),
 ('float','types','SELECT f FROM typed_values','expected_single_value','<','3.2'),
 ('string','types','SELECT s FROM typed_values','expected_single_value','==','String');
" "'types'"

oracle_results "scalar_type_failures" "
CREATE TABLE typed_failures(
 d DATE, dt DATETIME, f DOUBLE, amount DECIMAL(10,2), b BOOLEAN, s TEXT
);
INSERT INTO typed_failures VALUES
 ('2025-08-22','2025-08-22 09:00:00',3.14159,10.4,true,'String');
INSERT INTO dolt_tests VALUES
 ('bool_fail','type_failures','SELECT b FROM typed_failures','expected_single_value','==','false'),
 ('date_fail','type_failures','SELECT d FROM typed_failures','expected_single_value','>','2025-08-23'),
 ('decimal_fail','type_failures','SELECT amount FROM typed_failures','expected_single_value','>','10.5'),
 ('datetime_fail','type_failures','SELECT dt FROM typed_failures','expected_single_value','>','2025-08-22 09:00:01'),
 ('float_fail','type_failures','SELECT f FROM typed_failures','expected_single_value','>','3.2'),
 ('string_fail','type_failures','SELECT s FROM typed_failures','expected_single_value','!=','String');
" "'type_failures'"

oracle_results "zero_row_columns" "
INSERT INTO dolt_tests VALUES
 ('zero_cols','zero','SELECT 1,2 WHERE 0','expected_columns','==','0');
" "'zero'"

oracle_results "delimiter_optional" "
INSERT INTO dolt_tests VALUES
 ('no_semicolon','delimiter','SELECT 1','expected_rows','==','1'),
 ('semicolon','delimiter','SELECT 1;','expected_rows','==','1');
" "'delimiter'"

oracle_results "name_precedes_group" "
INSERT INTO dolt_tests VALUES
 ('chosen','other','SELECT 1','expected_rows','==','1'),
 ('group_member','chosen','SELECT 1','expected_rows','==','1');
" "'chosen'"

oracle_results "duplicate_arguments_repeat_results" "
INSERT INTO dolt_tests VALUES
 ('repeat','repeats','SELECT 1','expected_rows','==','1');
" "'repeat','repeat'"

oracle_results "query_error" "
INSERT INTO dolt_tests VALUES
 ('missing_table','errors','SELECT * FROM absent','expected_rows','==','0');
" "'errors'"

oracle_error "missing_argument" "$BASE_SETUP" \
  "SELECT * FROM dolt_test_run('missing');" \
  "SELECT * FROM dolt_test_run('missing');"

oracle_error "nonliteral_argument" "$BASE_SETUP" \
  "SELECT * FROM dolt_test_run(upper('group_a'));" \
  "SELECT * FROM dolt_test_run(upper('group_a'));"

oracle_error "no_tests_wildcard" "" \
  "SELECT * FROM dolt_test_run();" \
  "SELECT * FROM dolt_test_run();"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
