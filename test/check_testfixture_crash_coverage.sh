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
    if (NF != 4) {
      printf "%s:%d: expected 4 fields, found %d\n", FILENAME, NR, NF
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
  { sub(/#.*/, ""); if (NF) print $1, $2, $3, $4 }
' "$COVERAGE_FILE" | sort | uniq -d > "$TMP_DIR/duplicates"
report_set "duplicate coverage mappings" "$TMP_DIR/duplicates"

# Resolve <case> to a line in the replacement and echo "<line>\t<text>". The
# case has to match exactly one candidate: zero means the replacement no longer
# covers the behavior this mapping claims, and several means the locator points
# nowhere in particular. Candidates are restricted per kind so that a weak case
# name like a bare test number cannot collide with ordinary content.
resolve_case() {
  local path="$1" kind="$2" name="$3" pattern
  local escaped
  escaped=$(printf '%s' "$name" | sed -E 's/[][(){}.*+?^$|\\]/\\&/g')
  case "$kind" in
    testfixture)
      pattern="do_[a-z_]*test[[:space:]]+$escaped([[:space:]]|\{|$)"
      ;;
    c-test)
      # a function definition, or an entry in a dispatch table like kOps[]
      pattern="^([a-z]+[[:space:]]+)*[a-z]+[[:space:]]+$escaped\(|\"$escaped\""
      ;;
    workflow-script)
      pattern="^[[:space:]]*echo \"---.*[^[:alnum:]]$escaped([^[:alnum:]]|\$)|^$escaped\(\)"
      ;;
  esac
  # No match is an ordinary outcome, so keep grep's exit status from tripping
  # set -e through the command substitution.
  local matches
  matches=$(grep -nE "$pattern" "$path" 2>/dev/null || true)
  if [ -z "$matches" ]; then
    printf 'none\n'
    return 0
  fi
  # Split on the first colon only: matched lines contain colons of their own
  # ("--- Scenario 1: ..."), which -F: would eat. Truncation happens in awk
  # rather than through head, which would SIGPIPE the writer under pipefail.
  printf '%s\n' "$matches" | awk '
    {
      p = index($0, ":")
      if (NR <= 2) {
        line[NR] = substr($0, 1, p - 1)
        t = substr($0, p + 1)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", t)
        text[NR] = t
      }
    }
    END {
      if (NR > 1) { printf "many\t%s,%s\n", line[1], line[2]; exit }
      printf "%s\t%s\n", line[1], text[1]
    }'
}

check_case() {
  local source="$1" path="$2" kind="$3" name="$4" got
  got=$(resolve_case "$path" "$kind" "$name")
  case "${got%%$'\t'*}" in
    none)
      echo "ERROR: $source maps to $kind $path case '$name', which does not exist"
      FAIL=1
      ;;
    many)
      echo "ERROR: $source maps to $kind $path case '$name', which is ambiguous" \
           "(lines ${got#*$'\t'})"
      FAIL=1
      ;;
    *)
      RESOLVED="$RESOLVED$source -> $path:${got%%$'\t'*} (${got#*$'\t'})
"
      ;;
  esac
}

RESOLVED=""
while read -r source kind target acase; do
  [ -n "${source:-}" ] || continue
  case "$kind" in
    testfixture)
      if [ ! -f "$SCRIPT_DIR/$target.test" ]; then
        echo "ERROR: $source maps to missing testfixture test/$target.test"
        FAIL=1
      elif ! grep -Fqx "$target" "$BUCKET_DIR"/*.txt; then
        echo "ERROR: $source maps to testfixture $target, but it is not in a regression bucket"
        FAIL=1
      else
        check_case "$source" "test/$target.test" "$kind" "$acase"
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
      else
        check_case "$source" "test/$target.c" "$kind" "$acase"
      fi
      ;;
    workflow-script)
      if [ ! -f "$SCRIPT_DIR/$target" ]; then
        echo "ERROR: $source maps to missing script test/$target"
        FAIL=1
      elif ! grep -R -Fq "$target" "$ROOT_DIR/.github/workflows"; then
        echo "ERROR: $source maps to script $target, but no workflow runs it"
        FAIL=1
      else
        check_case "$source" "test/$target" "$kind" "$acase"
      fi
      ;;
    *)
      echo "ERROR: $source has unknown coverage kind: $kind"
      FAIL=1
      ;;
  esac
done < <(awk '{ sub(/#.*/, ""); if (NF) print $1, $2, $3, $4 }' "$COVERAGE_FILE")

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi

printf '%s' "$RESOLVED" | sort | sed 's/^/  /'
echo "OK: $(wc -l < "$TMP_DIR/excluded" | tr -d ' ') unbucketed crash tests have gated replacement coverage"
