#!/bin/bash
#
# Build and run the concurrent refs repro.
# This is intentionally a focused repro for the stale-refs overwrite bug.
# It is not part of the default suite until the underlying bug is fixed.

set -euo pipefail

echo "=== Concurrent Refs Repro ==="
cc -g -I. -I../src -o doltlite_regression_test_c \
  ../test/doltlite_regression_test_c.c libdoltlite.a -lz -lpthread -lm
./doltlite_regression_test_c concurrent_refs
