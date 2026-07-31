#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNNER="$SCRIPT_DIR/run_testfixture.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir "$TMP_DIR/build"
touch "$TMP_DIR/crashes"

cat > "$TMP_DIR/build/testfixture" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  *two.test)
    names="two-1.transient.41 two-1.transient.99"
    ;;
  *shifted.test)
    names="shifted-1.transient.52 shifted-1.transient.104"
    ;;
  *three.test)
    names="three-1.transient.41 three-1.transient.99 three-1.transient.120"
    ;;
  *one.test)
    names="one-1.transient.41"
    ;;
  *extra.test)
    names="extra-1.transient.41 extra-1.transient.99 extra-2.1"
    ;;
esac
count=$(wc -w <<< "$names" | tr -d ' ')
echo "$count errors out of 10 tests"
echo "!Failures on these tests: $names"
EOF
chmod +x "$TMP_DIR/build/testfixture"

run_case() {
  (
    cd "$TMP_DIR/build"
    DIVERGENCE_FILE="$TMP_DIR/divergences" \
      CRASH_FILE="$TMP_DIR/crashes" \
      bash "$RUNNER" self-test 10 "$1"
  ) >/dev/null 2>&1
}

expect_pass() {
  if ! run_case "$1"; then
    echo "ERROR: runner rejected $2"
    exit 1
  fi
}

expect_failure() {
  if run_case "$1"; then
    echo "ERROR: runner accepted $2"
    exit 1
  fi
}

printf '%s\n' \
  'two two-1.transient.41' \
  'two two-1.transient.99' > "$TMP_DIR/divergences"
expect_pass two "exact gates"

printf '%s\n' 'shifted shifted-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_pass shifted "a counted index shift"

printf '%s\n' 'three three-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_failure three "a counted pattern with an added failure"

printf '%s\n' 'one one-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_failure one "a counted pattern with a resolved failure"

printf '%s\n' 'extra extra-1.transient.*{2}' > "$TMP_DIR/divergences"
expect_failure extra "an unrelated failure beside a satisfied pattern"

printf '%s\n' \
  'two two-1.transient.*{2}' \
  'two two-1.transient.41' > "$TMP_DIR/divergences"
expect_failure two "overlapping exact and counted gates"

printf '%s\n' \
  'shifted shifted-1.transient.41' \
  'shifted shifted-1.transient.99' > "$TMP_DIR/divergences"
expect_failure shifted "shifted exact gates"

echo "OK: testfixture runner self-test"
