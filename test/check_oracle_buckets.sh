#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUCKET_DIR="$SCRIPT_DIR/oracle-buckets"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

find "$SCRIPT_DIR" -maxdepth 1 -type f -name 'vc_oracle_*_test.sh' \
  -exec basename {} \; | LC_ALL=C sort > "$TMP_DIR/actual"
cat "$BUCKET_DIR"/*.txt | sed '/^[[:space:]]*$/d' \
  | LC_ALL=C sort > "$TMP_DIR/listed"

duplicates="$(cat "$BUCKET_DIR"/*.txt | sed '/^[[:space:]]*$/d' \
  | LC_ALL=C sort | uniq -d)"
if [ -n "$duplicates" ]; then
  echo "ERROR: version-control oracles listed in multiple buckets:"
  echo "$duplicates"
  exit 1
fi

if ! diff -u "$TMP_DIR/actual" "$TMP_DIR/listed"; then
  echo "ERROR: version-control oracle buckets do not match test/vc_oracle_*_test.sh"
  exit 1
fi

echo "OK: every version-control oracle is in exactly one logical bucket"
