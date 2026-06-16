#!/bin/bash

set -euo pipefail

echo "=== Concurrent Refs Repro ==="
bash "$(dirname "$0")/run_doltlite_regression_case.sh" concurrent_refs
