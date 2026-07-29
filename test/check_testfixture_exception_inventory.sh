#!/usr/bin/env bash
#
# Every gated testfixture divergence and expected crash must carry a
# disposition, and the disposition set must match the gate set exactly in both
# directions -- no gate without a reason, no reason left behind for a gate that
# is no longer excepted.
#
# Keyed per gate, not per suite. A suite-level label blankets every assertion in
# the file (incrvacuum alone gates 1000), and real bugs have hidden behind a
# plausible suite-level "intentional" before (#1155, #1156, #1157), so
# recategorising one assertion has to be expressible.
#
# Issue URLs are validated for shape only, which is all this can do offline;
# check_testfixture_inventory_issues_alive.sh resolves them on a schedule.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INVENTORY_FILE="${1:-$SCRIPT_DIR/known_testfixture_exception_inventory.txt}"
DIVERGENCE_FILE="${2:-$SCRIPT_DIR/known_testfixture_divergences.txt}"
CRASH_FILE="${3:-$SCRIPT_DIR/known_testfixture_crashes.txt}"
RATCHET_FILE="${4:-$SCRIPT_DIR/known_testfixture_exception_ratchet.txt}"

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
    if (NF != 4) {
      printf "%s:%d: expected 4 fields, found %d\n", FILENAME, NR, NF
      bad = 1
      next
    }
    if ($1 !~ /^[A-Za-z0-9_.-]+$/) {
      printf "%s:%d: invalid suite name: %s\n", FILENAME, NR, $1
      bad = 1
    }
    if ($2 != "*" && $2 !~ /^[A-Za-z0-9_.()-]+$/) {
      printf "%s:%d: invalid gate: %s\n", FILENAME, NR, $2
      bad = 1
    }
    if ($3 != "intentional" && $3 != "unsupported" &&
        $3 != "harness" && $3 != "engine-gap") {
      printf "%s:%d: invalid category: %s\n", FILENAME, NR, $3
      bad = 1
    }
    actionable = ($3 == "unsupported" || $3 == "engine-gap")
    issue = ($4 ~ /^https:\/\/github.com\/dolthub\/doltlite\/issues\/[0-9]+$/)
    if (actionable && !issue) {
      printf "%s:%d: %s requires a DoltLite issue URL\n", FILENAME, NR, $3
      bad = 1
    }
    # intentional/harness carry no work item but may cite the issue that
    # documents the limitation; field 4 stays "-" or a well-formed URL.
    if (!actionable && $4 != "-" && !issue) {
      printf "%s:%d: %s must use - or a DoltLite issue URL\n", FILENAME, NR, $3
      bad = 1
    }
  }
  END { exit bad }
' "$INVENTORY_FILE"; then
  exit 1
fi

# Gate keys. A divergence gates one assertion; a crash row gates a whole file,
# spelled * so both live in one keyspace.
awk '{ sub(/#.*/, ""); if (NF) printf "%s %s\n", $1, $2 }' \
  "$DIVERGENCE_FILE" | sort > "$TMP_DIR/gates"
awk '{ sub(/#.*/, ""); if (NF) printf "%s *\n", $1 }' \
  "$CRASH_FILE" | sort >> "$TMP_DIR/gates"
sort -o "$TMP_DIR/gates" "$TMP_DIR/gates"

awk '{ sub(/#.*/, ""); if (NF) printf "%s %s\n", $1, $2 }' \
  "$INVENTORY_FILE" | sort > "$TMP_DIR/inventory"

uniq -d "$TMP_DIR/gates" > "$TMP_DIR/dup_gates"
uniq -d "$TMP_DIR/inventory" > "$TMP_DIR/duplicates"
comm -23 "$TMP_DIR/gates" "$TMP_DIR/inventory" > "$TMP_DIR/missing"
comm -13 "$TMP_DIR/gates" "$TMP_DIR/inventory" > "$TMP_DIR/stale"

FAIL=0
report_set() {
  local label="$1" file="$2"
  if [ -s "$file" ]; then
    echo "ERROR: $label:"
    sed 's/^/  /' "$file" | head -20
    if [ "$(wc -l < "$file")" -gt 20 ]; then
      echo "  ... and $(( $(wc -l < "$file") - 20 )) more"
    fi
    FAIL=1
  fi
}

report_set "duplicate gates in the divergence/crash lists" "$TMP_DIR/dup_gates"
report_set "duplicate inventory entries" "$TMP_DIR/duplicates"
report_set "gates without a disposition" "$TMP_DIR/missing"
report_set "stale inventory entries" "$TMP_DIR/stale"

[ "$FAIL" -eq 0 ] || exit 1

TOTAL=$(wc -l < "$TMP_DIR/inventory" | tr -d ' ')
counts_for() {
  awk -v want="$1" '
    { sub(/#.*/, ""); if (NF && $3 == want) n++ }
    END { print n + 0 }
  ' "$INVENTORY_FILE"
}
INTENTIONAL=$(counts_for intentional)
UNSUPPORTED=$(counts_for unsupported)
HARNESS=$(counts_for harness)
ENGINE_GAP=$(counts_for engine-gap)

echo "OK: $TOTAL testfixture gates classified (intentional=$INTENTIONAL" \
     "unsupported=$UNSUPPORTED harness=$HARNESS engine-gap=$ENGINE_GAP)"

# Ratchet. Every count is pinned: growing means more of the engine is excepted
# from upstream's suite, and shrinking has to be locked in or it silently drifts
# back up later.
if [ ! -f "$RATCHET_FILE" ]; then
  echo "ERROR: missing ratchet baseline: $RATCHET_FILE"
  exit 1
fi

ratchet_for() {
  awk -v want="$1" '
    { sub(/#.*/, ""); if (NF >= 2 && $1 == want) { print $2; found = 1 } }
    END { if (!found) print "missing" }
  ' "$RATCHET_FILE"
}

RATCHET_FAIL=0
check_ratchet() {
  local name="$1" actual="$2" baseline
  baseline=$(ratchet_for "$name")
  if [ "$baseline" = "missing" ]; then
    echo "ERROR: ratchet baseline has no '$name' line"
    RATCHET_FAIL=1
  elif ! printf '%s' "$baseline" | grep -qE '^[0-9]+$'; then
    echo "ERROR: ratchet baseline for '$name' is not a number: $baseline"
    RATCHET_FAIL=1
  elif [ "$actual" -gt "$baseline" ]; then
    echo "ERROR: $name grew from $baseline to $actual."
    echo "       A new exception means more of upstream's suite is excluded."
    echo "       Fix the engine, or raise the baseline deliberately in this PR."
    RATCHET_FAIL=1
  elif [ "$actual" -lt "$baseline" ]; then
    echo "ERROR: $name dropped from $baseline to $actual -- nice."
    echo "       Lower the '$name' line in $(basename "$RATCHET_FILE") to $actual"
    echo "       so the gain is locked in and cannot drift back."
    RATCHET_FAIL=1
  fi
}

check_ratchet gates "$TOTAL"
check_ratchet intentional "$INTENTIONAL"
check_ratchet unsupported "$UNSUPPORTED"
check_ratchet harness "$HARNESS"
check_ratchet engine-gap "$ENGINE_GAP"

[ "$RATCHET_FAIL" -eq 0 ] || exit 1

echo "OK: testfixture exception ratchet holds"
