#!/bin/bash
#
# Package-level smoke test for the doltlite crate. Stages the crate, packages
# it the way `cargo publish` would (honoring the manifest's include list),
# extracts that package, and builds the smoke test against the extracted copy.
# This exercises the real published payload -- notably whether the vendored
# amalgamation is actually included -- not just the working tree.
#
# Usage: test-package.sh [build_dir]   (default: ./build)

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"
BUILD_DIR="$(cd "${1:-$ROOT/build}" && pwd)"

command -v cargo >/dev/null || { echo "ERROR: cargo is required" >&2; exit 1; }

STAGE="$(mktemp -d)"
CONSUMER="$(mktemp -d)"
trap 'rm -rf "$STAGE" "$CONSUMER"' EXIT

bash "$PKG_DIR/assemble.sh" "0.0.0" "$STAGE/pkg" "$BUILD_DIR"

# Package exactly what would be published, then build against that copy.
( cd "$STAGE/pkg" && cargo package --no-verify --allow-dirty --quiet )
TARBALL="$STAGE/pkg/target/package/doltlite-0.0.0.crate"
[ -f "$TARBALL" ] || { echo "ERROR: cargo package produced no .crate" >&2; exit 1; }
mkdir -p "$STAGE/extracted"
tar -xzf "$TARBALL" -C "$STAGE/extracted"
EXTRACTED="$STAGE/extracted/doltlite-0.0.0"
[ -f "$EXTRACTED/vendor/doltlite.c" ] || {
  echo "ERROR: packaged crate has no vendored amalgamation" >&2; exit 1; }

cp -R "$PKG_DIR/smoke-test/." "$CONSUMER/"
# Point the consumer at the extracted package rather than crates.io.
sed -i.bak "s|^doltlite = .*|doltlite = { path = \"$EXTRACTED\" }|" \
  "$CONSUMER/Cargo.toml"
rm -f "$CONSUMER/Cargo.toml.bak"
cd "$CONSUMER"
cargo run --release --quiet
