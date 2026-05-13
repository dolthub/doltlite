#!/bin/bash
set -euo pipefail

echo "=== Checkout Persist Failure Repro ==="
bash "$(dirname "$0")/run_doltlite_regression_case.sh" checkout_persist_failure
