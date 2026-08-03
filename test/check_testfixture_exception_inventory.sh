#!/usr/bin/env bash
#
# Every gated testfixture divergence and expected crash must carry a
# disposition. The disposition lives on the gate's own line as class=, so a gate
# without a reason is a parse error on that line rather than a set difference
# against a second list that has to be kept in step.
#
# Keyed per gate, not per suite. A suite-level label blankets every assertion in
# the file (incrvacuum alone gates 1000), and real bugs have hidden behind a
# plausible suite-level "intentional" before (#1155, #1156, #1157), so
# recategorising one assertion has to be expressible.
#
# Issue references are validated for shape only, which is all this can do
# offline; check_testfixture_inventory_issues_alive.sh resolves them on a
# schedule.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIVERGENCE_FILE="${1:-$SCRIPT_DIR/known_testfixture_divergences.txt}"
CRASH_FILE="${2:-$SCRIPT_DIR/known_testfixture_crashes.txt}"
RATCHET_FILE="${3:-$SCRIPT_DIR/known_testfixture_exception_ratchet.txt}"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

for path in "$DIVERGENCE_FILE" "$CRASH_FILE"; do
  if [ ! -f "$path" ]; then
    echo "ERROR: missing testfixture exception input: $path"
    exit 1
  fi
done

# nkey is how many leading fields name the gate: a divergence names suite and
# assertion, a crash row names the suite alone and gates the whole file.
validate_records() {
  awk -v nkey="$2" '
    function is_counted(g) {
      return g ~ /^[A-Za-z0-9_.()-]+[.][*][{][1-9][0-9]*[}]$/
    }
    {
      sub(/#.*/, "")
      if (NF == 0) next
      if (NF < nkey + 1) {
        printf "%s:%d: expected at least %d fields (gate plus class=)\n",
               FILENAME, NR, nkey + 1
        bad = 1
        next
      }
      if ($1 !~ /^[A-Za-z0-9_.-]+$/) {
        printf "%s:%d: invalid suite name: %s\n", FILENAME, NR, $1
        bad = 1
      }
      if (nkey == 2 && $2 !~ /^[A-Za-z0-9_.()-]+$/ && !is_counted($2)) {
        printf "%s:%d: invalid gate: %s\n", FILENAME, NR, $2
        bad = 1
      }
      category = ""
      issue = ""
      nclass = 0
      nissue = 0
      for (i = nkey + 1; i <= NF; i++) {
        if ($i ~ /^@/) {
          if ($i != "@linux" && $i != "@darwin" && $i != "@windows" &&
              $i != "@coverage" && $i != "@no-coverage") {
            printf "%s:%d: invalid qualifier: %s\n", FILENAME, NR, $i
            bad = 1
          }
          continue
        }
        if ($i ~ /^class=/) { nclass++; category = substr($i, 7); continue }
        if ($i ~ /^issue=/) { nissue++; issue = substr($i, 7); continue }
        printf "%s:%d: unrecognized field: %s\n", FILENAME, NR, $i
        bad = 1
      }
      if (nclass != 1) {
        printf "%s:%d: expected exactly one class=, found %d\n",
               FILENAME, NR, nclass
        bad = 1
        next
      }
      if (nissue > 1) {
        printf "%s:%d: expected at most one issue=, found %d\n",
               FILENAME, NR, nissue
        bad = 1
      }
      if (category != "intentional" && category != "unsupported" &&
          category != "harness" && category != "engine-gap") {
        printf "%s:%d: invalid category: %s\n", FILENAME, NR, category
        bad = 1
      }
      if (nissue && issue !~ /^[1-9][0-9]*$/) {
        printf "%s:%d: issue= takes a DoltLite issue number: %s\n",
               FILENAME, NR, issue
        bad = 1
      }
      # unsupported and engine-gap are work items and need somewhere to track
      # them. intentional and harness may cite the issue that documents the
      # limitation, but are not required to.
      if ((category == "unsupported" || category == "engine-gap") && !nissue) {
        printf "%s:%d: %s requires issue=<number>\n", FILENAME, NR, category
        bad = 1
      }
    }
    END { exit bad }
  ' "$1"
}

validate_records "$DIVERGENCE_FILE" 2 || exit 1
validate_records "$CRASH_FILE" 1 || exit 1

# Overlapping counted gates would double-count against the ratchet.
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
    suite[++n] = $1
    gate[n] = $2
    if (is_counted($2)) counted_gate[++n_counted] = n
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

# One line per gate. A crash row gates a whole file, spelled * so both kinds
# live in one keyspace.
awk '{ sub(/#.*/, ""); if (NF) printf "%s %s\n", $1, $2 }' \
  "$DIVERGENCE_FILE" | sort > "$TMP_DIR/gates"
awk '{ sub(/#.*/, ""); if (NF) printf "%s *\n", $1 }' \
  "$CRASH_FILE" | sort >> "$TMP_DIR/gates"
sort -o "$TMP_DIR/gates" "$TMP_DIR/gates"
uniq -d "$TMP_DIR/gates" > "$TMP_DIR/dup_gates"

if [ -s "$TMP_DIR/dup_gates" ]; then
  echo "ERROR: duplicate gates in the divergence/crash lists:"
  sed 's/^/  /' "$TMP_DIR/dup_gates" | head -20
  if [ "$(wc -l < "$TMP_DIR/dup_gates")" -gt 20 ]; then
    echo "  ... and $(( $(wc -l < "$TMP_DIR/dup_gates") - 20 )) more"
  fi
  exit 1
fi

# Counted gates stand for N assertions each; every other gate weighs 1. Passing
# an empty category totals every gate regardless of class.
weigh() {
  awk -v want="$2" '
    function weight(g, n) {
      if (g ~ /[.][*][{][1-9][0-9]*[}]$/) {
        n = g
        sub(/^.*[.][*][{]/, "", n)
        sub(/[}]$/, "", n)
        return n + 0
      }
      return 1
    }
    {
      sub(/#.*/, "")
      if (NF == 0) next
      category = ""
      for (i = 1; i <= NF; i++) if ($i ~ /^class=/) category = substr($i, 7)
      if (want == "" || category == want) n += weight($2)
    }
    END { print n + 0 }
  ' "$1"
}

sum_for() {
  echo $(( $(weigh "$DIVERGENCE_FILE" "$1") + $(weigh "$CRASH_FILE" "$1") ))
}

TOTAL=$(sum_for "")
INTENTIONAL=$(sum_for intentional)
UNSUPPORTED=$(sum_for unsupported)
HARNESS=$(sum_for harness)
ENGINE_GAP=$(sum_for engine-gap)

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
