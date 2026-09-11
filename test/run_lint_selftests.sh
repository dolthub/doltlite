#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
source "$root/.github/scripts/parallel-compile.sh"
if [ "$#" -eq 0 ]; then
  echo 'Usage: run_lint_selftests.sh suite ...' >&2
  exit 1
fi
ci_compile_init "${DOLTLITE_LINT_JOBS:-1}"
for script in "$@"; do
  ci_compile bash "$root/$script"
done
ci_compile_wait
