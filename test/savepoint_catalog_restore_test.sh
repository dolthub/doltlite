#!/bin/bash
set -euo pipefail

echo "=== Savepoint Catalog Restore Repro ==="
bash "$(dirname "$0")/run_doltlite_regression_case.sh" savepoint_catalog_restore
