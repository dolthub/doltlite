#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FIXTURE="${1:?Usage: run_sqlite_file_tests.sh <testfixture> [sqlite|mixed]}"
FIXTURE="$(cd "$(dirname "$FIXTURE")" && pwd)/$(basename "$FIXTURE")"
MODE="${2:-all}"
case "$MODE" in all|sqlite|mixed) ;; *) exit 2 ;; esac
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/doltlite-sqlite-files.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT
touch "$WORK_DIR/exceptions"

run_mode() {
  local mode="$1"
  shift
  mkdir "$WORK_DIR/$mode"
  ln -s "$FIXTURE" "$WORK_DIR/$mode/testfixture"
  (
    cd "$WORK_DIR/$mode"
    SQLITE_FILE_TEST_MODE="$mode" \
    TESTFIXTURE_PRELUDE="$SCRIPT_DIR/lib/sqlite_file_permutation.tcl" \
    DIVERGENCE_FILE="$WORK_DIR/exceptions" \
    TERMINATION_FILE="$WORK_DIR/exceptions" \
      bash "$SCRIPT_DIR/run_testfixture.sh" "SQLite files ($mode)" 120 "$@"
  )
}

if [[ "$MODE" == all || "$MODE" == mixed ]]; then
  run_mode mixed attach attach3
fi
if [[ "$MODE" == all || "$MODE" == sqlite ]]; then
  suites=()
  while IFS= read -r suite; do
    [[ -z "$suite" ]] || suites+=("$suite")
  done < "$SCRIPT_DIR/sqlite-file-suites.txt"
  run_mode sqlite "${suites[@]}"
fi
