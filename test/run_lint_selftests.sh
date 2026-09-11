#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
source "$root/.github/scripts/parallel-compile.sh"
ci_compile_init "${DOLTLITE_LINT_JOBS:-1}"
for script in test/lint_layers_selftest.sh test/stock_oracle_harness_test.sh \
              .github/scripts/ci-optimization-test.sh; do
  ci_compile bash "$root/$script"
done
ci_compile_wait
