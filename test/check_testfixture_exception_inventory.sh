#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INVENTORY_FILE="${1:-$SCRIPT_DIR/known_testfixture_exception_inventory.txt}"
DIVERGENCE_FILE="${2:-$SCRIPT_DIR/known_testfixture_divergences.txt}"
CRASH_FILE="${3:-$SCRIPT_DIR/known_testfixture_crashes.txt}"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

for path in "$INVENTORY_FILE" "$DIVERGENCE_FILE" "$CRASH_FILE"; do
  if [ ! -f "$path" ]; then
    echo "ERROR: missing testfixture exception inventory input: $path"
    exit 1
  fi
done

if ! awk '
  {
    sub(/#.*/, "")
    if (NF == 0) next
    if (NF != 3) {
      printf "%s:%d: expected 3 fields, found %d\n", FILENAME, NR, NF
      bad = 1
      next
    }
    if ($1 !~ /^[A-Za-z0-9_.-]+$/) {
      printf "%s:%d: invalid suite name: %s\n", FILENAME, NR, $1
      bad = 1
    }
    if ($2 != "intentional" && $2 != "unsupported" &&
        $2 != "harness" && $2 != "engine-gap") {
      printf "%s:%d: invalid category: %s\n", FILENAME, NR, $2
      bad = 1
    }
    actionable = ($2 == "unsupported" || $2 == "engine-gap")
    issue = ($3 ~ /^https:\/\/github.com\/dolthub\/doltlite\/issues\/[0-9]+$/)
    if (actionable && !issue) {
      printf "%s:%d: %s requires a DoltLite issue URL\n",
             FILENAME, NR, $2
      bad = 1
    }
    # intentional/harness rows carry no work item, but they may still cite the
    # issue that documents the limitation -- rowid keying, for one, explains a
    # large share of the list and previously had nowhere to be attributed. Field
    # 3 stays either "-" or a well-formed DoltLite issue URL.
    if (!actionable && $3 != "-" && !issue) {
      printf "%s:%d: %s must use - or a DoltLite issue URL\n",
             FILENAME, NR, $2
      bad = 1
    }
  }
  END { exit bad }
' "$INVENTORY_FILE"; then
  exit 1
fi

awk '
  { sub(/#.*/, ""); if (NF) print $1 }
' "$DIVERGENCE_FILE" "$CRASH_FILE" | sort -u > "$TMP_DIR/exceptions"

awk '
  { sub(/#.*/, ""); if (NF) print $1 }
' "$INVENTORY_FILE" | sort > "$TMP_DIR/inventory"

uniq -d "$TMP_DIR/inventory" > "$TMP_DIR/duplicates"
comm -23 "$TMP_DIR/exceptions" "$TMP_DIR/inventory" > "$TMP_DIR/missing"
comm -13 "$TMP_DIR/exceptions" "$TMP_DIR/inventory" > "$TMP_DIR/stale"

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

report_set "duplicate exception inventory entries" "$TMP_DIR/duplicates"
report_set "exception suites without a disposition" "$TMP_DIR/missing"
report_set "stale exception inventory entries" "$TMP_DIR/stale"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi

TOTAL=$(wc -l < "$TMP_DIR/inventory" | tr -d ' ')
COUNTS=$(awk '
  { sub(/#.*/, ""); if (NF) count[$2]++ }
  END {
    printf "intentional=%d unsupported=%d harness=%d engine-gap=%d",
      count["intentional"], count["unsupported"],
      count["harness"], count["engine-gap"]
  }
' "$INVENTORY_FILE")
echo "OK: $TOTAL testfixture exception suites classified ($COUNTS)"
