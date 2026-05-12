#!/bin/bash
#
# Build and run the concurrent refs repro.
# This is intentionally a focused repro for the stale-refs overwrite bug.
# It is not part of the default suite until the underlying bug is fixed.

set -euo pipefail

echo "=== Concurrent Refs Repro ==="
bash "$(dirname "$0")/run_doltlite_regression_case.sh" concurrent_refs
