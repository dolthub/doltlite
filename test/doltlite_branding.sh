#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== DoltLite Branding Tests ==="
echo ""

run_test "engine_func" "SELECT doltlite_engine();" "prolly" ":memory:"

run_test_match "engine_old_gone" "SELECT doltite_engine();" "no such function" ":memory:"

VER=$($DOLTLITE -version 2>&1)
if echo "$VER" | grep -q "DoltLite"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: version_flag\n  expected: DoltLite\n  got:      $VER"; fi

if echo "$VER" | grep -q "SQLite"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: version_sqlite\n  expected: SQLite\n  got:      $VER"; fi

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

if echo "$BANNER" | grep -q "DoltLite"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: banner\n  expected: DoltLite\n  got:      $BANNER"; fi

if echo "$BANNER" | grep -q "SQLite version"; then FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: no_sqlite_version\n  should not contain: SQLite version\n  got:      $BANNER"; else PASS=$((PASS+1)); fi

PROMPT=$(run_script $DOLTLITE :memory: <<'PEOF' | cat
SELECT 1;
.quit
PEOF
)
if echo "$PROMPT" | grep -q "doltlite>"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: prompt\n  expected: doltlite>\n  got:      $PROMPT"; fi

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
