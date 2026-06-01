#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== DoltLite Branding Tests ==="
echo ""

run_test "engine_func" "SELECT doltlite_engine();" "prolly" ":memory:"

run_test_match "engine_old_gone" "SELECT doltite_engine();" "no such function" ":memory:"

VER=$($DOLTLITE -version 2>&1)
if echo "$VER" | grep -q "DoltLite"; then dltest_pass; else dltest_fail "version_flag" "  expected: DoltLite\n  got:      $VER"; fi

if echo "$VER" | grep -q "SQLite"; then dltest_pass; else dltest_fail "version_sqlite" "  expected: SQLite\n  got:      $VER"; fi

run_script() {
  if [ "$(uname)" = "Darwin" ]; then
    script -q /dev/null "$@" 2>&1
  else
    script -qc "$*" /dev/null 2>&1
  fi
}

BANNER=$(run_script $DOLTLITE :memory: <<'EOF' | head -5
.quit
EOF
)

if echo "$BANNER" | grep -q "DoltLite"; then dltest_pass; else dltest_fail "banner" "  expected: DoltLite\n  got:      $BANNER"; fi

if echo "$BANNER" | grep -q "SQLite version"; then dltest_fail "no_sqlite_version" "  should not contain: SQLite version\n  got:      $BANNER"; else dltest_pass; fi

PROMPT=$(run_script $DOLTLITE :memory: <<'PEOF' | cat
SELECT 1;
.quit
PEOF
)
if echo "$PROMPT" | grep -q "doltlite>"; then dltest_pass; else dltest_fail "prompt" "  expected: doltlite>\n  got:      $PROMPT"; fi

dltest_finish
