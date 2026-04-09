#!/bin/bash
set -euo pipefail

echo "=== Savepoint Catalog Restore Repro ==="
cc -g -I. -I../src -o savepoint_catalog_restore_test \
  ../test/savepoint_catalog_restore_test.c libdoltlite.a -lz -lpthread -lm
./savepoint_catalog_restore_test
