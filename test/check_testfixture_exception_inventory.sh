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
    exact = ($2 ~ /^[A-Za-z0-9_.()-]+$/)
    counted = ($2 ~ /^[A-Za-z0-9_.()-]+[.][*][{][1-9][0-9]*[}]$/)
    if ($2 != "*" && !exact && !counted) {
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

if ! awk '
  function is_counted(g) {
    return g ~ /^[A-Za-z0-9_.()-]+[.][*][{][1-9][0-9]*[}]$/
  }
  function counted_prefix(g, p) {
    p = g
    sub(/[.][*][{][1-9][0-9]*[}]$/, "", p)
    return p "."
  }
  {
    sub(/#.*/, "")
    if (NF == 0) next
    if (NF < 2) {
      printf "%s:%d: expected at least 2 fields\n", FILENAME, NR
      bad = 1
      next
    }
    exact = ($2 ~ /^[A-Za-z0-9_.()-]+$/)
    counted = is_counted($2)
    if (!exact && !counted) {
      printf "%s:%d: invalid gate: %s\n", FILENAME, NR, $2
      bad = 1
    }
    for (i = 3; i <= NF; i++) {
      if ($i != "@linux" && $i != "@darwin" && $i != "@windows" &&
          $i != "@coverage" && $i != "@no-coverage") {
        printf "%s:%d: invalid qualifier: %s\n", FILENAME, NR, $i
        bad = 1
      }
    }
    suite[++n] = $1
    gate[n] = $2
    if (counted) counted_gate[++n_counted] = n
  }
  END {
    for (k = 1; k <= n_counted; k++) {
      i = counted_gate[k]
      p = counted_prefix(gate[i])
      for (j = 1; j <= n; j++) {
        if (i == j) continue
        if (suite[i] != suite[j]) continue
        if (is_counted(gate[j])) {
          if (j < i) continue
          q = counted_prefix(gate[j])
          overlap = (index(p, q) == 1 || index(q, p) == 1)
        } else {
          overlap = (index(gate[j], p) == 1)
        }
        if (overlap) {
          printf "%s: overlapping gates for %s: %s and %s\n",
                 FILENAME, suite[i], gate[i], gate[j]
          bad = 1
        }
      }
    }
    exit bad
  }
' "$DIVERGENCE_FILE"; then
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

TOTAL=$(awk '
  function weight(g, n) {
    if (g ~ /[.][*][{][1-9][0-9]*[}]$/) {
      n = g
      sub(/^.*[.][*][{]/, "", n)
      sub(/[}]$/, "", n)
      return n + 0
    }
    return 1
  }
  { sub(/#.*/, ""); if (NF) n += weight($2) }
  END { print n + 0 }
' "$INVENTORY_FILE")
counts_for() {
  awk -v want="$1" '
    function weight(g, n) {
      if (g ~ /[.][*][{][1-9][0-9]*[}]$/) {
        n = g
        sub(/^.*[.][*][{]/, "", n)
        sub(/[}]$/, "", n)
        return n + 0
      }
      return 1
    }
    { sub(/#.*/, ""); if (NF && $3 == want) n += weight($2) }
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

# Exactly one line per name. Printing every match let a duplicated name yield a
# multiline value that then failed every numeric comparison without recording a
# failure, so the ratchet reported that it held while enforcing nothing.
ratchet_for() {
  awk -v want="$1" '
    { sub(/#.*/, ""); if (NF >= 2 && $1 == want) { value = $2; n++ } }
    END {
      if (n == 0) { print "missing"; exit }
      if (n > 1) { print "duplicate"; exit }
      print value
    }
  ' "$RATCHET_FILE"
}

# Whole-string test. grep -E '^[0-9]+$' matches line by line, so it accepts a
# multiline value on the strength of its first line.
is_count() {
  case "$1" in
    '' | *[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

RATCHET_FAIL=0
check_ratchet() {
  local name="$1" actual="$2" baseline
  baseline=$(ratchet_for "$name")
  if [ "$baseline" = "missing" ]; then
    echo "ERROR: ratchet baseline has no '$name' line"
    RATCHET_FAIL=1
  elif [ "$baseline" = "duplicate" ]; then
    echo "ERROR: ratchet baseline lists '$name' more than once"
    RATCHET_FAIL=1
  elif ! is_count "$baseline"; then
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
