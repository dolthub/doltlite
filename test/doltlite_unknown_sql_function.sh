#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== unknown() SQL function ==="
echo ""

DB=:memory:

run_test "unknown_typeof_is_null" \
  "SELECT typeof(unknown(1));" \
  "null" \
  "$DB"

run_test "unknown_variadic_is_null" \
  "SELECT typeof(unknown(1,2,3));" \
  "null" \
  "$DB"

run_test "unknown_in_function_list" \
  "SELECT name, narg FROM pragma_function_list WHERE name='unknown';" \
  "unknown|-1" \
  "$DB"

run_test "unknown_compile_option" \
  "SELECT compile_options FROM pragma_compile_options WHERE compile_options='ENABLE_UNKNOWN_SQL_FUNCTION';" \
  "ENABLE_UNKNOWN_SQL_FUNCTION" \
  "$DB"

run_test_match "nosuchfn_still_errors" \
  "SELECT nosuchfn(1);" \
  "no such function: nosuchfn" \
  "$DB"

run_test_match "explain_substitutes_unknown" \
  "EXPLAIN SELECT nosuchfn(1);" \
  "unknown\\(-1\\)" \
  "$DB"

dltest_finish
