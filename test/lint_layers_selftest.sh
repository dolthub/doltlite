#!/usr/bin/env bash

# Guards after the verdict never run. Structural check plus behavioural rejects.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
LINT="$HERE/lint_layers.sh"
SRC="$REPO_ROOT/src"

PASS=0
FAIL=0
ERRORS=""
ok() { PASS=$((PASS+1)); }
bad() { FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $1\n  $2"; }

echo "=== lint_layers self-test ==="


verdict_line=$(grep -n '^NFAIL=' "$LINT" | head -1 | cut -d: -f1)
if [ -z "$verdict_line" ]; then
  bad "verdict_line_found" "no NFAIL= line in lint_layers.sh; update this self-test"
else
  ok
  stranded=$(awk -v v="$verdict_line" 'NR>v && /lint "/ {print NR": "$0}' "$LINT")
  if [ -n "$stranded" ]; then
    bad "no_guard_after_verdict" \
        "these guards sit below the verdict at line $verdict_line and can never run:
$stranded"
  else
    ok
  fi
fi


WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Abort if the fixture is incomplete rather than reporting false coverage.
setup_failed() {
  echo "SETUP FAILED: $1" >&2
  echo "  The behavioural checks below would pass against an incomplete fixture" >&2
  echo "  and prove nothing, so this is an error rather than a test failure." >&2
  exit 2
}

cp_err="$WORK/.cp.err"
if ! cp "$SRC"/*.c "$SRC"/*.h "$WORK"/ 2>"$cp_err"; then
  setup_failed "could not copy src/ into $WORK:
$(sed 's/^/    /' "$cp_err")"
fi
rm -f "$cp_err"

# cp can succeed after a partial copy; compare counts.
want=$(ls "$SRC"/*.c "$SRC"/*.h 2>/dev/null | wc -l | tr -d ' ')
got=$(ls "$WORK"/*.c "$WORK"/*.h 2>/dev/null | wc -l | tr -d ' ')
if [ "$want" -eq 0 ]; then
  setup_failed "no .c/.h files found in $SRC"
fi
if [ "$got" -ne "$want" ]; then
  setup_failed "fixture is incomplete: copied $got of $want files from $SRC"
fi

expect_clean() {
  local name="$1" out
  out=$(bash "$LINT" "$WORK" 2>&1)
  if [ $? -eq 0 ]; then ok; else bad "$name" "expected a clean pass, got:
$out"; fi
}

# The message must name the offending file.
expect_violation() {
  local name="$1" needle="$2" out rc
  out=$(bash "$LINT" "$WORK" 2>&1)
  rc=$?
  if [ "$rc" -eq 0 ]; then
    bad "$name" "expected a violation, but lint passed"
  elif ! printf '%s' "$out" | grep -q -- "$needle"; then
    bad "$name" "exited $rc but never mentioned '$needle':
$out"
  else
    ok
  fi
}

expect_clean "baseline_unperturbed_src_passes"

# GUARDED is "<file>:<cap>".
GUARDED="
chunk_store_lock.c:1200
prolly_btree_cursor_count.c:1500
prolly_btree_cursor_payload.c:1500
doltlite_branches.c:1500
doltlite_merge_pass1.c:1500
"

for entry in $GUARDED; do
  file="${entry%%:*}"
  cap="${entry##*:}"
  [ -f "$WORK/$file" ] || { bad "guarded_file_present:$file" "not in src/"; continue; }

  mv "$WORK/$file" "$WORK/.hidden"
  expect_violation "missing_is_rejected:$file" "$file"
  mv "$WORK/.hidden" "$WORK/$file"

  cp "$WORK/$file" "$WORK/.orig"
  python3 - "$WORK/$file" "$cap" <<'PY'
import sys
path, cap = sys.argv[1], int(sys.argv[2])
have = open(path).read().count("\n")
with open(path, "a") as fh:
    fh.write("\n".join(["/* pad */"] * (cap + 6 - have)) + "\n")
PY
  expect_violation "oversized_is_rejected:$file" "$file"
  mv "$WORK/.orig" "$WORK/$file"
done

expect_clean "restored_src_passes_again"

echo
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
