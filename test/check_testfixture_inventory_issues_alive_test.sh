#!/usr/bin/env bash
#
# Self-test for the issue-liveness checker. The real thing only runs on a
# schedule, so without this a bug in it would sit undetected until the next
# Monday. A `gh` shim on PATH stands in for the API, keeping this offline and
# runnable in PR CI.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKER="$SCRIPT_DIR/check_testfixture_inventory_issues_alive.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

INVENTORY="$TMP_DIR/inventory"
mkdir -p "$TMP_DIR/bin"
cat > "$TMP_DIR/bin/gh" <<'SHIM'
#!/bin/sh
# gh issue view <n> --json state -q .state
for arg in "$@"; do case "$arg" in [0-9]*) n="$arg"; break ;; esac; done
case "$n" in
  100) echo OPEN ;;
  200) echo CLOSED ;;
  *) exit 1 ;;
esac
SHIM
chmod +x "$TMP_DIR/bin/gh"
export PATH="$TMP_DIR/bin:$PATH"

run_check() { bash "$CHECKER" "$INVENTORY" >/dev/null 2>&1; }

expect_failure() {
  if run_check; then echo "ERROR: checker accepted $1"; exit 1; fi
}

B=https://github.com/dolthub/doltlite/issues

printf '%s\n' \
  "alpha intentional -" \
  "beta unsupported $B/100" \
  "gamma intentional $B/100" > "$INVENTORY"
run_check

printf '%s\n' \
  "alpha intentional -" \
  "beta unsupported $B/200" > "$INVENTORY"
expect_failure "a closed issue"

printf '%s\n' \
  "alpha intentional -" \
  "beta unsupported $B/999" > "$INVENTORY"
expect_failure "an unresolvable issue"

printf '%s\n' \
  "alpha intentional -" \
  "gamma intentional $B/200" > "$INVENTORY"
expect_failure "a closed issue cited by an intentional row"

echo "OK: testfixture inventory issue-liveness self-test"
