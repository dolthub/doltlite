#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== EXPLAIN comment column ==="
echo ""

DB=:memory:

# The coverage build links the canonical objects (DOLTLITE_CLI_OBJECTS=1)
# and has no comment column by design; the suite only applies to the
# amalgamation-embedded CLI.
HAS_FLAG=$(echo "SELECT count(*) FROM pragma_compile_options WHERE compile_options='ENABLE_EXPLAIN_COMMENTS';" | $DOLTLITE "$DB" 2>/dev/null)
if [ "$HAS_FLAG" != "1" ]; then
  echo "  SKIP: CLI built without EXPLAIN comments (object-linked build)"
  dltest_finish
  exit 0
fi

run_test "explain_comments_compile_option" \
  "SELECT compile_options FROM pragma_compile_options WHERE compile_options='ENABLE_EXPLAIN_COMMENTS';" \
  "ENABLE_EXPLAIN_COMMENTS" \
  "$DB"

run_test_match "explain_init_comment" \
  "EXPLAIN SELECT abs(-1);" \
  "Start at " \
  "$DB"

run_test_match "explain_integer_comment" \
  "EXPLAIN SELECT abs(-1);" \
  "r\\[[0-9]+\\]=-1" \
  "$DB"

# The comments annotate the stream; the opcodes themselves are unchanged.
run_test_match "explain_opcodes_unchanged_init" \
  "EXPLAIN SELECT abs(-1);" \
  "Init" \
  "$DB"

run_test_match "explain_opcodes_unchanged_function" \
  "EXPLAIN SELECT abs(-1);" \
  "Function" \
  "$DB"

dltest_finish
