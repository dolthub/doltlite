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

DIVERGENCES="$TMP_DIR/divergences"
CRASHES="$TMP_DIR/crashes"
: > "$CRASHES"
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

run_check() { bash "$CHECKER" "$DIVERGENCES" "$CRASHES" >/dev/null 2>&1; }

expect_failure() {
  if run_check; then echo "ERROR: checker accepted $1"; exit 1; fi
}

printf '%s\n' \
  "alpha alpha-1 class=intentional" \
  "beta beta-1 class=unsupported issue=100" \
  "gamma gamma-1 class=intentional issue=100" > "$DIVERGENCES"
run_check

printf '%s\n' \
  "alpha alpha-1 class=intentional" \
  "beta beta-1 class=unsupported issue=200" > "$DIVERGENCES"
expect_failure "a closed issue"

printf '%s\n' \
  "alpha alpha-1 class=intentional" \
  "beta beta-1 class=unsupported issue=999" > "$DIVERGENCES"
expect_failure "an unresolvable issue"

printf '%s\n' \
  "alpha alpha-1 class=intentional" \
  "gamma gamma-1 class=intentional issue=200" > "$DIVERGENCES"
expect_failure "a closed issue cited by an intentional gate"

# A crash row is keyed by suite alone, so its issue= sits in field 2 and the
# gate is reported as "suite *".
printf '%s\n' "alpha alpha-1 class=intentional" > "$DIVERGENCES"
printf '%s\n' "delta class=engine-gap issue=200" > "$CRASHES"
expect_failure "a closed issue cited by a crash row"
: > "$CRASHES"

echo "OK: testfixture inventory issue-liveness self-test"
