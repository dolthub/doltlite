#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
source "$script_dir/parallel-compile.sh"
source "$script_dir/ci-selftests.sh"
ci_compile_init "${DOLTLITE_LINT_JOBS:-1}"
ci_selftests
ci_compile_wait
