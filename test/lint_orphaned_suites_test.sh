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
  "$tmp/test/doltlite_fixture.c" "$tmp/test/oracle_fixture_test.sh"
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
        build/doltlite_fixture
        for suite in test/oracle_*_test.sh; do bash "$suite"; done
        DOLTLITE_DIFF_GROUPS=fixture-group python3 test/sql_differential_fuzzer.py
YAML

cat > "$tmp/.github/workflows/regress.yml" <<'YAML'
jobs:
  regress:
    strategy:
      matrix:
        bucket:
          - core
    steps:
      - run: cat test/regression-buckets/${{ matrix.bucket }}.txt
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
for needle in fixture_test.sh fixture_test.c doltlite_fixture.c \
  oracle_fixture_test.sh fixture-group; do
  grep -Fq -- "  - $needle" "$tmp/output"
done

printf '%s\n' 'build/fixture_test' 'build/doltlite_fixture' > "$tmp/main.mk"
if bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1; then
  echo 'ERROR: guard accepted a C suite that is only built' >&2
  exit 1
fi
grep -Fq -- '  - fixture_test.c' "$tmp/output"
grep -Fq -- '  - doltlite_fixture.c' "$tmp/output"

printf '%s\n' 'build/doltlite_fixture_extended' > "$tmp/test/run_c_tests.sh"
bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1 || true
if ! grep -Fq -- '  - doltlite_fixture.c' "$tmp/output"; then
  echo 'ERROR: a longer suite name counted as coverage for a shorter one' >&2
  exit 1
fi

printf '%s\n' '# TODO: run doltlite_fixture' > "$tmp/test/run_c_tests.sh"
bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1 || true
if ! grep -Fq -- '  - doltlite_fixture.c' "$tmp/output"; then
  echo 'ERROR: a commented-out mention counted as a runner' >&2
  exit 1
fi
: > "$tmp/test/run_c_tests.sh"

printf '%s\n' 'doltlite_fixture.c' > "$tmp/test/ci_suite_quarantine.txt"
bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1 || true
if grep -Fq -- 'doltlite_fixture.c' "$tmp/output"; then
  echo 'ERROR: guard reported a quarantined DoltLite C suite as an orphan' >&2
  exit 1
fi
: > "$tmp/test/ci_suite_quarantine.txt"
printf '%s\n' 'build/doltlite_fixture' > "$tmp/test/run_c_tests.sh"
bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1 || true
if grep -Fq -- 'doltlite_fixture.c' "$tmp/output"; then
  echo 'ERROR: guard reported a wired DoltLite C suite as an orphan' >&2
  exit 1
fi
expect_lint_error() {  # expect_lint_error <needle> <what>
  if bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1; then
    echo "ERROR: guard accepted $2" >&2
    exit 1
  fi
  if ! grep -Fq -- "$1" "$tmp/output"; then
    echo "ERROR: guard did not name $1 for $2" >&2
    cat "$tmp/output" >&2
    exit 1
  fi
}
expect_lint_ok() {  # expect_lint_ok <what>
  if ! bash "$tmp/test/lint_orphaned_suites.sh" > "$tmp/output" 2>&1; then
    echo "ERROR: guard rejected $1" >&2
    cat "$tmp/output" >&2
    exit 1
  fi
}

cat > "$tmp/.github/actions/nested/probes/action.yml" <<'YAML'
runs:
  using: composite
  steps:
    - shell: bash
      run: |
        bash test/fixture_test.sh
        build/fixture_test
        build/doltlite_fixture
        for suite in test/oracle_*_test.sh; do bash "$suite"; done
        DOLTLITE_DIFF_GROUPS=fixture-group python3 test/sql_differential_fuzzer.py
YAML
expect_lint_ok "a clean tree"

mkdir -p "$tmp/ext/widget/test"
touch "$tmp/ext/widget/test/widget1.test"
expect_lint_error ext/widget/test/widget1.test "an extension suite in no bucket"

printf '%s\n' 'ext/widget/test/*.test class=unsupported  # not compiled in' \
  > "$tmp/test/known_testfixture_unrun.txt"
expect_lint_ok "an extension suite listed as unrun"

printf '%s\n' widget1 > "$tmp/test/regression-buckets/core.txt"
expect_lint_error "bucketed and listed" "a suite both bucketed and listed as unrun"
: > "$tmp/test/known_testfixture_unrun.txt"
expect_lint_ok "a bucketed extension suite"

printf '%s\n' 'ext/gone/*.test class=harness  # removed' \
  > "$tmp/test/known_testfixture_unrun.txt"
expect_lint_error "matches no file" "an unrun entry that matches nothing"
: > "$tmp/test/known_testfixture_unrun.txt"

touch "$tmp/test/widget1.test"
expect_lint_error "  - widget1" "a basename shared by two suites"
rm "$tmp/test/widget1.test"

printf '%s\n' 'nothere class=harness  # skips' > "$tmp/test/known_testfixture_vacuous.txt"
expect_lint_error "nothere (listed in known_testfixture_vacuous.txt" "a vacuous entry for an unbucketed suite"
: > "$tmp/test/known_testfixture_vacuous.txt"

touch "$tmp/test/regression-buckets/extra.txt"
expect_lint_error "regression-buckets/extra.txt" "a bucket no matrix selects"
rm "$tmp/test/regression-buckets/extra.txt"

mkdir -p "$tmp/test/oracle-buckets"
touch "$tmp/test/oracle-buckets/vc.txt"
cat > "$tmp/.github/workflows/oracles.yml" <<'YAML'
jobs:
  oracles:
    strategy:
      matrix:
        include:
          - suite: oracles
            bucket: other vc
    steps:
      - run: cat test/oracle-buckets/${{ matrix.bucket }}.txt
YAML
expect_lint_ok "an oracle bucket in a grouped matrix row"
sed -i.bak 's/bucket: other vc/bucket: other/' "$tmp/.github/workflows/oracles.yml"
expect_lint_error "oracle-buckets/vc.txt (not selected by .github/workflows/oracles.yml)" \
  "an oracle bucket one workflow skips"

echo 'Orphaned-suite guard action tests passed'
