#!/bin/bash
#
# Package-level smoke test for doltlite-go. Stages the module, then builds the
# smoke test against the staged copy through a module replace, so the test
# exercises what would actually be pushed -- notably whether the vendored
# amalgamation is present -- rather than the working tree.
#
# Usage: test-package.sh [build_dir]   (default: ./build)

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"
BUILD_DIR="$(cd "${1:-$ROOT/build}" && pwd)"

command -v go >/dev/null || { echo "ERROR: go is required" >&2; exit 1; }

STAGE="$(mktemp -d)"
CONSUMER="$(mktemp -d)"
trap 'chmod -R u+w "$STAGE" "$CONSUMER" 2>/dev/null; rm -rf "$STAGE" "$CONSUMER"' EXIT

bash "$PKG_DIR/assemble.sh" "0.0.0" "$STAGE/pkg" "$BUILD_DIR"

[ -f "$STAGE/pkg/doltlite.c" ] || {
  echo "ERROR: staged module has no vendored amalgamation" >&2; exit 1; }

cp "$PKG_DIR/smoke-test/main.go" "$CONSUMER/"
cp "$PKG_DIR/smoke-test/go.mod"  "$CONSUMER/"
cd "$CONSUMER"
go mod edit -replace "github.com/dolthub/doltlite-go=$STAGE/pkg"
go mod tidy >/dev/null 2>&1 || true
CGO_ENABLED=1 go run .
