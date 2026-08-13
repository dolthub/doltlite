#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
script="$root/.github/scripts/upgrade-dolt-oracle.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

make_fixture() {
  local run=$1
  local pinned=${2:-v2.2.3}
  rm -rf "$tmp/work" "$tmp/bin" "$tmp/log"
  mkdir -p "$tmp/work/.github/scripts" "$tmp/bin"
  cp "$script" "$tmp/work/.github/scripts/upgrade-dolt-oracle.sh"
  printf '%s\n' "$pinned" > "$tmp/work/.dolt-oracle-version"
  git -C "$tmp/work" init -q
  git -C "$tmp/work" config user.name test
  git -C "$tmp/work" config user.email test@example.com
  git -C "$tmp/work" add .
  git -C "$tmp/work" commit -qm base
  printf '%s' "$run" > "$tmp/run.json"

  cat > "$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh %s\n' "$*" >> "$TEST_LOG"
case "$1 $2" in
  'api repos/dolthub/dolt/releases/latest') printf '%s\n' v2.2.4 ;;
  'pr list') printf '[{"number":7,"headRefName":"automation/dolt-oracle-v2.2.3","headRefOid":"abc123"}]\n' ;;
  'run list') cat "$TEST_RUN" ;;
  'pr merge'|'pr comment'|'pr close'|'pr create') ;;
  *) echo "unexpected gh invocation: $*" >&2; exit 1 ;;
esac
EOF

  cat > "$tmp/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'git %s\n' "$*" >> "$TEST_LOG"
case "$1" in
  fetch) ;;
  show) printf '%s\n' v2.2.3 ;;
  checkout)
    printf '%s\n' v2.2.3 > "$REPO_ROOT/.dolt-oracle-version"
    ;;
  config|add|commit|push) ;;
  *) echo "unexpected git invocation: $*" >&2; exit 1 ;;
esac
EOF
  chmod +x "$tmp/bin/gh" "$tmp/bin/git"
}

run_upgrade() {
  PATH="$tmp/bin:$PATH" \
  REPO_ROOT="$tmp/work" \
  GITHUB_REPOSITORY=dolthub/doltlite \
  TEST_LOG="$tmp/log" \
  TEST_RUN="$tmp/run.json" \
  DOLT_ORACLE_MERGE_WAIT_SECONDS=0 \
  bash "$script"
}

assert_has() {
  if ! grep -Fq "$1" "$tmp/log"; then
    echo "missing invocation: $1" >&2
    cat "$tmp/log" >&2
    exit 1
  fi
}

assert_lacks() {
  if grep -Fq "$1" "$tmp/log"; then
    echo "unexpected invocation: $1" >&2
    cat "$tmp/log" >&2
    exit 1
  fi
}

make_fixture '[{"status":"completed","conclusion":"success","headSha":"abc123","databaseId":10}]'
run_upgrade
assert_has 'gh pr merge 7'
assert_lacks 'gh pr close 7'
assert_has 'gh pr create'
test "$(cat "$tmp/work/.dolt-oracle-version")" = v2.2.4

make_fixture '[{"status":"completed","conclusion":"failure","headSha":"abc123","databaseId":10}]'
run_upgrade
assert_has 'gh pr comment 7'
assert_has 'gh pr close 7'
assert_lacks 'gh pr merge 7'
assert_has 'gh pr create'

make_fixture '[{"status":"in_progress","conclusion":"","headSha":"abc123","databaseId":10}]'
run_upgrade
assert_has 'gh pr close 7'
assert_lacks 'gh pr merge 7'

rm -rf "$tmp/work" "$tmp/bin" "$tmp/log"
mkdir -p "$tmp/work/.github/scripts" "$tmp/bin"
cp "$script" "$tmp/work/.github/scripts/upgrade-dolt-oracle.sh"
printf '%s\n' v2.2.4 > "$tmp/work/.dolt-oracle-version"
cat > "$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh %s\n' "$*" >> "$TEST_LOG"
case "$1 $2" in
  'api repos/dolthub/dolt/releases/latest') printf '%s\n' v2.2.4 ;;
  'pr list') printf '[]\n' ;;
  *) echo "unexpected gh invocation: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/bin/gh"
PATH="$tmp/bin:$PATH" REPO_ROOT="$tmp/work" \
  GITHUB_REPOSITORY=dolthub/doltlite TEST_LOG="$tmp/log" \
  bash "$script"
assert_lacks 'gh pr create'

printf '%s\n' v2.2.5 > "$tmp/work/.dolt-oracle-version"
PATH="$tmp/bin:$PATH" REPO_ROOT="$tmp/work" \
  GITHUB_REPOSITORY=dolthub/doltlite TEST_LOG="$tmp/log" \
  bash "$script"
assert_lacks 'gh pr create'

for workflow in build-test.yml sanitizers.yml test.yml; do
  grep -Fq 'bash .github/scripts/install-dolt-oracle.sh' \
    "$root/.github/workflows/$workflow"
done
if grep -R -Fq 'releases/latest/download/dolt-' "$root/.github"; then
  echo "found an unpinned Dolt download" >&2
  exit 1
fi

echo "Dolt oracle upgrade automation tests passed"
