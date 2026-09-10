#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/test/lib" "$tmp/test/regression-buckets" "$tmp/.github/workflows" \
  "$tmp/.github/actions/nested/probes"
cp "$root/test/lint_orphaned_suites.sh" "$tmp/test/"
touch "$tmp/test/ci_suite_allowlist.txt" "$tmp/test/ci_suite_quarantine.txt" \
  "$tmp/test/lib/doltlite_suite_manifest.sh" "$tmp/test/run_c_tests.sh" \
  "$tmp/test/regression-buckets/core.txt" "$tmp/main.mk" \
  "$tmp/test/fixture_test.sh" "$tmp/test/fixture_test.c" \
  "$tmp/test/oracle_fixture_test.sh"
cat > "$tmp/test/sql_differential_fuzzer.py" <<'PY'
GROUPS = [
    "fixture-group",
]
PY
cat > "$tmp/.github/workflows/ci.yml" <<'YAML'
jobs:
  probes:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/nested/probes
YAML
cat > "$tmp/.github/actions/nested/probes/action.yml" <<'YAML'
runs:
  using: composite
  steps:
    - shell: bash
      run: |
        bash test/fixture_test.sh
        build/fixture_test
        for suite in test/oracle_*_test.sh; do bash "$suite"; done
        DOLTLITE_DIFF_GROUPS=fixture-group python3 test/sql_differential_fuzzer.py
YAML

bash "$tmp/test/lint_orphaned_suites.sh"
mv "$tmp/.github/actions/nested/probes/action.yml" \
  "$tmp/.github/actions/nested/probes/action.yaml"
bash "$tmp/test/lint_orphaned_suites.sh"

rm "$tmp/.github/actions/nested/probes/action.yaml"
if bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1; then
  echo 'ERROR: guard accepted suites after their action references were removed' >&2
  exit 1
fi
for needle in fixture_test.sh fixture_test.c oracle_fixture_test.sh fixture-group; do
  grep -Fq -- "  - $needle" "$tmp/output"
done

printf '%s\n' 'build/fixture_test' > "$tmp/main.mk"
if bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1; then
  echo 'ERROR: guard accepted a C suite that is only built' >&2
  exit 1
fi
grep -Fq -- '  - fixture_test.c' "$tmp/output"
echo 'Orphaned-suite guard action tests passed'
