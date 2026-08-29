#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DOLTLITE="${1:-$REPO_ROOT/build/doltlite}"
OUTPUT="${2:-$SCRIPT_DIR/seed.db}"
REMOTE="/tmp/doltlite-format-corpus-v12-origin.db"
TMP="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP"
  rm -f "$REMOTE" "$REMOTE-lock" "$REMOTE-name-lock"
}
trap cleanup EXIT

rm -f "$REMOTE" "$REMOTE-lock" "$REMOTE-name-lock"
"$DOLTLITE" "$TMP/seed.db" < "$SCRIPT_DIR/seed.sql" >/dev/null
cp "$TMP/seed.db" "$OUTPUT"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$OUTPUT"
else
  shasum -a 256 "$OUTPUT"
fi
