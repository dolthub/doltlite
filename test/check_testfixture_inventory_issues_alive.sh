#!/usr/bin/env bash
#
# Every `unsupported` / `engine-gap` row in the testfixture exception inventory
# has to justify itself with a DoltLite issue. check_testfixture_exception_
# inventory.sh validates the URL's shape offline, which is all PR CI can do
# without a network dependency -- and shape alone let 67 of 78 rows go on
# citing closed issues, including the triage process tracker that had been
# closed for weeks. This check resolves each one and fails on anything closed
# or missing, so a justification cannot quietly rot.
#
# Scheduled rather than per-PR: it needs the GitHub API, and a network blip
# must not redden an unrelated pull request.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INVENTORY_FILE="${1:-$SCRIPT_DIR/known_testfixture_exception_inventory.txt}"

if [ ! -f "$INVENTORY_FILE" ]; then
  echo "ERROR: missing inventory: $INVENTORY_FILE"
  exit 1
fi
if ! command -v gh >/dev/null 2>&1; then
  echo "SKIP: gh not available"
  exit 0
fi

# Unique issue numbers only: thousands of gates share a handful of issues, so
# this is a few API calls rather than one per gate.
numbers=$(awk '
  { sub(/#.*/, ""); if (NF >= 4 && $4 != "-") print $4 }
' "$INVENTORY_FILE" | sed 's|.*/||' | sort -un)

if [ -z "$numbers" ]; then
  echo "OK: no issue references in the inventory"
  exit 0
fi

# One issue can justify four figures' worth of gates, so report the count and a
# handful of examples rather than every gate.
cited_by() {
  awk -v n="$1" '
    { sub(/#.*/, ""); if (NF >= 4 && $4 ~ ("/" n "$")) print $1 " " $2 }
  ' "$INVENTORY_FILE" | awk '
    { total++; if (total <= 3) printf "%s%s", (total > 1 ? ", " : ""), $0 }
    END { printf " (%d gate%s)", total, (total == 1 ? "" : "s") }
  '
}

fail=0
alive=0
for n in $numbers; do
  state=$(gh issue view "$n" --json state -q .state 2>/dev/null || echo "")
  case "$state" in
    OPEN)
      alive=$((alive + 1))
      ;;
    CLOSED)
      echo "ERROR: #$n is CLOSED but still justifies: $(cited_by "$n")"
      fail=1
      ;;
    *)
      echo "ERROR: #$n did not resolve (missing, or the API failed):" \
           "$(cited_by "$n")"
      fail=1
      ;;
  esac
done

if [ "$fail" -ne 0 ]; then
  echo
  echo "Every unsupported/engine-gap row must cite an open DoltLite issue."
  echo "Either reopen the issue, file one that describes the limitation as it"
  echo "stands now, or -- if the limitation is gone -- drop the exception."
  exit 1
fi

echo "OK: all $alive issue(s) referenced by the testfixture exception inventory are open"
