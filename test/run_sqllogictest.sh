#!/bin/bash

set -uo pipefail

DOLTLITE_RUNNER="${1:?Usage: run_sqllogictest.sh <doltlite-runner> <stock-runner> <test-dir> [divergence-file]}"
STOCK_RUNNER="${2:?Missing stock runner}"
TESTDIR="${3:?Missing test dir}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIVERGENCE_FILE="${4:-$SCRIPT_DIR/known_sqllogictest_divergences.txt}"

PER_FILE_TIMEOUT=300

expected_for() {
  local rel="$1"
  [ -f "$DIVERGENCE_FILE" ] || return 0
  awk -v f="$rel" '
    { sub(/#.*/, ""); gsub(/^[ \t]+|[ \t]+$/, "")
      if ($0 == "") next
      if ($1 == f) print $2 }
  ' "$DIVERGENCE_FILE"
}

listed_files() {
  [ -f "$DIVERGENCE_FILE" ] || return 0
  awk '{ sub(/#.*/, ""); gsub(/^[ \t]+|[ \t]+$/, ""); if ($0=="") next; print $1 }' "$DIVERGENCE_FILE" | sort -u
}

tokens() {
  timeout "$PER_FILE_TIMEOUT" "$1" --verify "$2" 2>&1 \
    | grep -oE '!DIVERGE [0-9]+' | awk '{print $2}' | LC_ALL=C sort -u
}

mapfile -t TEST_FILES < <(find "$TESTDIR" -name '*.test' -type f | sort)
if [ ${#TEST_FILES[@]} -eq 0 ]; then
  echo "ERROR: No .test files found under $TESTDIR"
  exit 1
fi

echo "============================================"
echo "SQL Logic Test: per-assertion divergence gate"
echo "============================================"
echo "Test directory:  $TESTDIR"
echo "Test files:      ${#TEST_FILES[@]}"
echo "Divergence list: $DIVERGENCE_FILE ($(grep -cvE '^[[:space:]]*(#|$)' "$DIVERGENCE_FILE" 2>/dev/null || echo 0) entries)"
echo ""

n_files=0
n_div_files=0
total_div=0
total_unexpected=0
total_fixed=0
total_crash=0
unexpected_lines=""
fixed_lines=""
crash_lines=""

seen_file_list=""

for f in "${TEST_FILES[@]}"; do
  rel="${f#"$TESTDIR"/}"
  n_files=$((n_files + 1))
  seen_file_list="$seen_file_list$rel"$'\n'

  dl_out=$(timeout "$PER_FILE_TIMEOUT" "$DOLTLITE_RUNNER" --verify "$f" 2>&1) && dl_rc=0 || dl_rc=$?
  if [ "$dl_rc" -eq 124 ] || ! echo "$dl_out" | grep -q 'errors out of'; then
    echo "CRASH/TIMEOUT: $rel (doltlite rc=$dl_rc)"
    total_crash=$((total_crash + 1))
    crash_lines="$crash_lines"$'\n'"  $rel (rc=$dl_rc)"
    continue
  fi
  dl=$(echo "$dl_out" | grep -oE '!DIVERGE [0-9]+' | awk '{print $2}' | LC_ALL=C sort -u)
  st=$(tokens "$STOCK_RUNNER" "$f")

  divg=$(comm -23 <(printf '%s\n' "$dl" | grep -v '^$') <(printf '%s\n' "$st" | grep -v '^$'))
  exp=$(expected_for "$rel" | LC_ALL=C sort -u)

  [ -z "$divg" ] && [ -z "$exp" ] && continue

  unexpected=$(comm -23 <(printf '%s\n' "$divg" | grep -v '^$') <(printf '%s\n' "$exp" | grep -v '^$'))
  fixed=$(comm -13 <(printf '%s\n' "$divg" | grep -v '^$') <(printf '%s\n' "$exp" | grep -v '^$'))

  n_div=$(printf '%s\n' "$divg" | grep -c .)
  n_unexp=$(printf '%s\n' "$unexpected" | grep -c .)
  n_fix=$(printf '%s\n' "$fixed" | grep -c .)
  total_div=$((total_div + n_div))
  [ "$n_div" -gt 0 ] && n_div_files=$((n_div_files + 1))

  if [ "$n_unexp" -eq 0 ] && [ "$n_fix" -eq 0 ]; then
    [ "$n_div" -gt 0 ] && echo "OK: $rel ($n_div known divergences)"
  else
    if [ "$n_unexp" -gt 0 ]; then
      echo "FAIL: $rel — unexpected divergences (not in list):"
      printf '%s\n' "$unexpected" | grep . | sed 's/^/    line /'
      total_unexpected=$((total_unexpected + n_unexp))
      while IFS= read -r ln; do [ -n "$ln" ] && unexpected_lines="$unexpected_lines"$'\n'"  $rel $ln"; done <<< "$unexpected"
    fi
    if [ "$n_fix" -gt 0 ]; then
      echo "FIXED: $rel — listed entries that no longer diverge (remove from list):"
      printf '%s\n' "$fixed" | grep . | sed 's/^/    line /'
      total_fixed=$((total_fixed + n_fix))
      while IFS= read -r ln; do [ -n "$ln" ] && fixed_lines="$fixed_lines"$'\n'"  $rel $ln"; done <<< "$fixed"
    fi
  fi
done

while IFS= read -r lf; do
  [ -z "$lf" ] && continue
  if ! printf '%s' "$seen_file_list" | grep -Fxq -- "$lf"; then
    echo "STALE: $lf is in the divergence list but not present in the corpus"
    n=$(expected_for "$lf" | grep -c .)
    total_fixed=$((total_fixed + n))
    fixed_lines="$fixed_lines"$'\n'"  $lf (file missing, $n entries)"
  fi
done < <(listed_files)

echo ""
echo "============================================"
echo "  files:                  $n_files"
echo "  files with divergences: $n_div_files"
echo "  known divergences:      $total_div"
if [ "$total_unexpected" -gt 0 ] || [ "$total_crash" -gt 0 ] || [ "$total_fixed" -gt 0 ]; then
  [ "$total_unexpected" -gt 0 ] && { echo "  UNEXPECTED divergences:  $total_unexpected$unexpected_lines"; }
  [ "$total_crash" -gt 0 ]      && { echo "  crashes/timeouts:        $total_crash$crash_lines"; }
  [ "$total_fixed" -gt 0 ]      && { echo "  list entries to remove:  $total_fixed$fixed_lines"; }
  echo "============================================"
  echo "::error::sqllogictest divergence gate failed (unexpected=$total_unexpected, crashes=$total_crash, to-remove=$total_fixed)"
  exit 1
fi
echo "============================================"
echo "OK: all doltlite divergences are accounted for in the allow-list."
exit 0
