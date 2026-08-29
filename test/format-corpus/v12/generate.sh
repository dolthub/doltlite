#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DOLTLITE="${1:-$REPO_ROOT/build/doltlite}"
OUTPUT="${2:-$SCRIPT_DIR/seed.db}"
TMP="$(mktemp -d)"
REMOTE="$TMP/origin.db"
SEED_SQL="$TMP/seed.sql"

cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT

sed "s|@REMOTE_URI@|file://$REMOTE|g" "$SCRIPT_DIR/seed.sql" > "$SEED_SQL"
"$DOLTLITE" "$TMP/seed.db" < "$SEED_SQL" >/dev/null
cp "$TMP/seed.db" "$OUTPUT"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$OUTPUT"
else
  shasum -a 256 "$OUTPUT"
fi
