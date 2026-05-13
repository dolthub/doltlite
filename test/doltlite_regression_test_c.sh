#!/bin/bash
set -euo pipefail

echo "=== DoltLite Regression Tests ==="
bash "$(dirname "$0")/run_doltlite_regression_case.sh" all
