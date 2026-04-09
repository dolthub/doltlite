#!/bin/bash
set -euo pipefail

echo "=== Checkout Persist Failure Repro ==="
cc -g -I. -o checkout_persist_failure_test \
  ../test/checkout_persist_failure_test.c libdoltlite.a -lz -lpthread -lm
./checkout_persist_failure_test
