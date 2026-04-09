#!/bin/bash
#
# Build and run the concurrent refs repro.
# This is intentionally a focused repro for the stale-refs overwrite bug.
# It is not part of the default suite until the underlying bug is fixed.

set -euo pipefail

echo "=== Concurrent Refs Repro ==="
cc -g -I. -o concurrent_refs_test ../test/concurrent_refs_test.c libdoltlite.a -lz -lpthread -lm
./concurrent_refs_test
