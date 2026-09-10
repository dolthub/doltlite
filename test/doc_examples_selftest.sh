#!/usr/bin/env bash
# The doc examples runner must fail, not pass, when the engine cannot run or a
# page smuggles a shell escape. Each case runs the real runner on a copy of the
# docs with one page altered, and checks both the verdict and that nothing
# escaped (no marker file).
#
# Usage: doc_examples_selftest.sh [doltlite]

DOLTLITE="${1:-./doltlite}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/oracle_doc_examples_test.sh"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
case "$DOLTLITE" in /*) ;; *) DOLTLITE="$PWD/$DOLTLITE" ;; esac
pass=0; fail=0
ok()  { pass=$((pass+1)); echo "PASS: $1"; }
bad() { fail=$((fail+1)); echo "FAIL: $1"; [ -n "${2:-}" ] && echo "  $2"; }

# expect_fail <name> <marker-or-empty> <runner args...>: the runner must exit
# non-zero and, if a marker path is given, must not have created it.
expect_fail() {
  local name="$1" marker="$2"; shift 2
  local out rc
  out=$("$@" 2>&1); rc=$?
  if [ "$rc" -eq 0 ]; then bad "$name" "runner exited 0: $(echo "$out" | tail -1)"; return; fi
  if [ -n "$marker" ] && [ -e "$marker" ]; then bad "$name" "marker file was created"; return; fi
  ok "$name"
}

# A doc tree with one page replaced by attacker content.
tree_with_page() {  # tree_with_page <dir> <page-content>
  rm -rf "$1"; mkdir -p "$1/doc/doltlite" "$1/test"
  cp "$SCRIPT_DIR"/../doc/doltlite/dolt_tag.md "$1/doc/doltlite/dolt_tag.md"
  printf '%s\n' "$2" >> "$1/doc/doltlite/dolt_tag.md"
  cp "$RUNNER" "$1/test/"
}

# 1. Engine that cannot run, or is not doltlite.
expect_fail "nonexistent binary" "" bash "$RUNNER" "$TMP/does-not-exist"
expect_fail "/bin/true as engine" "" bash "$RUNNER" /bin/true
printf '#!/bin/bash\n"%s" "$@"; exit 23\n' "$DOLTLITE" > "$TMP/exit23"; chmod +x "$TMP/exit23"
expect_fail "engine exits 23" "" bash "$RUNNER" "$TMP/exit23"
printf '#!/bin/bash\nif [ $# -eq 1 ] && [ ! -t 0 ]; then in=$(cat); case "$in" in *Fixture*) echo "Error: setup" >&2; exit 1;; esac; printf %%s "$in" | exec "%s" "$@"; fi\nexec "%s" "$@"\n' "$DOLTLITE" "$DOLTLITE" > "$TMP/fixfail"; chmod +x "$TMP/fixfail"
expect_fail "fixture fails" "" bash "$RUNNER" "$TMP/fixfail"

# 2. Shell escapes inside a sql block.
M="$TMP/marker"
tree_with_page "$TMP/t1" $'```sql\n.shell touch '"$M"$'\n```'
expect_fail "dot-command" "$M" bash "$TMP/t1/test/oracle_doc_examples_test.sh" "$DOLTLITE" x dolt_tag.md
tree_with_page "$TMP/t2" $'```sql\n   .system touch '"$M"$'\n```'
expect_fail "indented dot-command" "$M" bash "$TMP/t2/test/oracle_doc_examples_test.sh" "$DOLTLITE" x dolt_tag.md
tree_with_page "$TMP/t3" $'```sql\nSELECT writefile(\''"$M"$'\', \'x\');\n```'
expect_fail "writefile" "$M" bash "$TMP/t3/test/oracle_doc_examples_test.sh" "$DOLTLITE" x dolt_tag.md
tree_with_page "$TMP/t4" $'```sql\nSELECT WriteFile\n  (\''"$M"$'\', \'x\');\n```'
expect_fail "writefile split across lines" "$M" bash "$TMP/t4/test/oracle_doc_examples_test.sh" "$DOLTLITE" x dolt_tag.md
tree_with_page "$TMP/t5" $'```sql\nSELECT "writefile" /* c */ (\''"$M"$'\', \'x\');\n```'
expect_fail "quoted name with comment" "$M" bash "$TMP/t5/test/oracle_doc_examples_test.sh" "$DOLTLITE" x dolt_tag.md
tree_with_page "$TMP/t6" $'```sql\nSELECT edit(\'x\');\n```'
expect_fail "edit()" "" bash "$TMP/t6/test/oracle_doc_examples_test.sh" "$DOLTLITE" x dolt_tag.md

# 3. The unaltered page still passes, so the guards are not just failing everything.
if bash "$RUNNER" "$DOLTLITE" x dolt_tag.md >/dev/null 2>&1; then ok "clean page passes"; else bad "clean page passes"; fi

echo ""
echo "================================"
echo "Results: $pass passed, $fail failed"
echo "================================"
[ "$fail" -gt 0 ] && exit 1
exit 0
