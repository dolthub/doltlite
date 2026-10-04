#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
if [ "${1:-}" = --selftest ]; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  mkdir -p "$tmp/test"
  for definition in 'run_test() { :; }' 'function run_test_match { :; }' 'run_test_error_match () { :; }'; do
    printf '%s\n' '. "lib/doltlite_test_common.sh"' "$definition" > "$tmp/test/doltlite_probe.sh"
    if ROOT="$tmp" bash "$0" > "$tmp/log" 2>&1; then
      echo "FAIL: private helper accepted: $definition" >&2
      exit 1
    fi
    grep -q 'private run_test' "$tmp/log"
  done
  printf '%s\n' 'run_test_match probe "SELECT 1;" "^1$" :memory:' > "$tmp/test/doltlite_probe.sh"
  if ROOT="$tmp" bash "$0" > "$tmp/log" 2>&1; then
    echo 'FAIL: shared helper used without sourcing the harness' >&2
    exit 1
  fi
  printf '%s\n' '. "lib/doltlite_test_common.sh"' 'run_test_match probe "SELECT 1;" "^1$" :memory:' > "$tmp/test/doltlite_probe.sh"
  ROOT="$tmp" bash "$0"
  echo 'lint_native_test_helpers: selftest passed'
  exit 0
fi
python3 - "${ROOT:-$REPO_ROOT}" <<'PY'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
bad = []
for path in sorted((root / 'test').glob('doltlite_*.sh')):
    text = path.read_text()
    definitions = re.findall(r'^\s*(?:function\s+)?(run_test\w*)\s*(?:\(\s*\))?\s*\{', text, re.M)
    if definitions:
        bad.append('%s: private %s helpers must use doltlite_test_common.sh' % (path.name, ', '.join(definitions)))
    uses = re.search(r'^\s*run_test\w*\b', text, re.M)
    shared = re.search(r'^\s*(?:source|\.)\s+[^\n]*doltlite_test_common\.sh', text, re.M)
    if uses and not shared:
        bad.append('%s: source doltlite_test_common.sh before using shared helpers' % path.name)
if bad:
    print('\n'.join(bad))
    sys.exit(1)
print('lint_native_test_helpers: ok')
PY
