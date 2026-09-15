#!/bin/bash
set -euo pipefail

echo "=== DoltLite Regression Tests ==="
arg=all
case "$(uname -s 2>/dev/null || echo unknown)" in
  MINGW*|MSYS*|CYGWIN*) arg=all_except=backup_safety ;;
esac
bash "$(dirname "$0")/run_doltlite_regression_case.sh" "$arg"
echo "__SUITE_COMPLETE__"
