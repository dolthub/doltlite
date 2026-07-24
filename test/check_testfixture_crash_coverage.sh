#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CRASH_FILE="${1:-$SCRIPT_DIR/known_testfixture_crashes.txt}"
COVERAGE_FILE="${2:-$SCRIPT_DIR/known_testfixture_crash_coverage.txt}"
BUCKET_DIR="${3:-$SCRIPT_DIR/regression-buckets}"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

FAIL=0
report_set() {
  local label="$1"
  local file="$2"
  if [ -s "$file" ]; then
    echo "ERROR: $label:"
    sed 's/^/  /' "$file"
    FAIL=1
  fi
}

awk '
  { sub(/#.*/, ""); if (NF) print $1 }
' "$CRASH_FILE" | sort -u > "$TMP_DIR/crashes"

awk '
  { sub(/#.*/, ""); if (NF) print $1 }
' "$BUCKET_DIR"/*.txt | sort -u > "$TMP_DIR/bucketed"

comm -23 "$TMP_DIR/crashes" "$TMP_DIR/bucketed" > "$TMP_DIR/excluded"

if ! awk '
  {
    sub(/#.*/, "")
    if (NF == 0) next
    if (NF != 3) {
      printf "%s:%d: expected 3 fields, found %d\n", FILENAME, NR, NF
      bad = 1
    }
  }
  END { exit bad }
' "$COVERAGE_FILE"; then
  exit 1
fi

awk '
  { sub(/#.*/, ""); if (NF) print $1 }
' "$COVERAGE_FILE" | sort -u > "$TMP_DIR/mapped"

comm -23 "$TMP_DIR/excluded" "$TMP_DIR/mapped" > "$TMP_DIR/missing"
comm -13 "$TMP_DIR/excluded" "$TMP_DIR/mapped" > "$TMP_DIR/stale"
report_set "unbucketed crash entries without replacement coverage" "$TMP_DIR/missing"
report_set "coverage mappings for tests that are not unbucketed crashes" "$TMP_DIR/stale"

awk '
  { sub(/#.*/, ""); if (NF) print $1, $2, $3 }
' "$COVERAGE_FILE" | sort | uniq -d > "$TMP_DIR/duplicates"
report_set "duplicate coverage mappings" "$TMP_DIR/duplicates"

while read -r source kind target; do
  [ -n "${source:-}" ] || continue
  case "$kind" in
    testfixture)
      if [ ! -f "$SCRIPT_DIR/$target.test" ]; then
        echo "ERROR: $source maps to missing testfixture test/$target.test"
        FAIL=1
      elif ! grep -Fqx "$target" "$BUCKET_DIR"/*.txt; then
        echo "ERROR: $source maps to testfixture $target, but it is not in a regression bucket"
        FAIL=1
      fi
      ;;
    c-test)
      if [ ! -f "$SCRIPT_DIR/$target.c" ]; then
        echo "ERROR: $source maps to missing C test test/$target.c"
        FAIL=1
      elif ! sed -n -E \
          '/^(COVERAGE_TESTS|SPECIALIZED_TESTS)=\(/,/^\)/p' \
          "$SCRIPT_DIR/run_c_tests.sh" \
          | grep -Eq "^[[:space:]]*$target[[:space:]]*$"; then
        echo "ERROR: $source maps to C test $target, but it is not gating"
        FAIL=1
      fi
      ;;
    workflow-script)
      if [ ! -f "$SCRIPT_DIR/$target" ]; then
        echo "ERROR: $source maps to missing script test/$target"
        FAIL=1
      elif ! grep -R -Fq "$target" "$ROOT_DIR/.github/workflows"; then
        echo "ERROR: $source maps to script $target, but no workflow runs it"
        FAIL=1
      fi
      ;;
    *)
      echo "ERROR: $source has unknown coverage kind: $kind"
      FAIL=1
      ;;
  esac
done < <(awk '{ sub(/#.*/, ""); if (NF) print $1, $2, $3 }' "$COVERAGE_FILE")

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi

echo "OK: $(wc -l < "$TMP_DIR/excluded" | tr -d ' ') unbucketed crash tests have gated replacement coverage"
