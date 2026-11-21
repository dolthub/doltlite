#!/bin/bash
#
# Package-level smoke test for the doltlite crate. Stages the crate, packages
# it the way `cargo publish` would (honoring the manifest's include list),
# extracts that package, and builds the smoke test against the extracted copy.
# This exercises the real published payload -- notably whether the vendored
# amalgamation is actually included -- not just the working tree.
#
# Usage: test-package.sh [build_dir]
#        test-package.sh --staged <version> <crate_dir>

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"

command -v cargo >/dev/null || { echo "ERROR: cargo is required" >&2; exit 1; }

if [ -n "${DOLTLITE_PACKAGE_WORK_ROOT:-}" ]; then
  mkdir -p "$DOLTLITE_PACKAGE_WORK_ROOT/rust"
  STAGE="$DOLTLITE_PACKAGE_WORK_ROOT/rust/stage"
  CONSUMER="$DOLTLITE_PACKAGE_WORK_ROOT/rust/consumer"
  mkdir "$STAGE" "$CONSUMER"
else
  STAGE="$(mktemp -d)"
  CONSUMER="$(mktemp -d)"
fi
trap 'rm -rf "$STAGE" "$CONSUMER"' EXIT

if [ "${1:-}" = --staged ]; then
  version="${2:?crate version required}"
  package="$(cd "${3:?crate directory required}" && pwd)"
  set -- --locked
else
  version=0.0.0
  package="$STAGE/pkg"
  bash "$PKG_DIR/assemble.sh" "$version" "$package" "${1:-$ROOT/build}"
  set --
fi

# Package exactly what would be published, then build against that copy.
(cd "$package" && CARGO_TARGET_DIR="$STAGE/target" cargo package --no-verify --allow-dirty --quiet "$@")
TARBALL="$STAGE/target/package/doltlite-$version.crate"
[ -f "$TARBALL" ] || { echo "ERROR: cargo package produced no .crate" >&2; exit 1; }
mkdir -p "$STAGE/extracted"
tar -xzf "$TARBALL" -C "$STAGE/extracted"
EXTRACTED="$STAGE/extracted/doltlite-$version"
[ -f "$EXTRACTED/vendor/doltlite.c" ] || {
  echo "ERROR: packaged crate has no vendored amalgamation" >&2; exit 1; }
# The vendored amalgamation carries mbedtls/ed25519/BLAKE3, whose notices live
# in LICENSE.md.
[ -f "$EXTRACTED/LICENSE.md" ] || {
  echo "ERROR: packaged crate has no LICENSE.md" >&2; exit 1; }

cp -R "$PKG_DIR/smoke-test/." "$CONSUMER/"
# Point the consumer at the extracted package rather than crates.io.
sed -i.bak "s|^doltlite = .*|doltlite = { path = \"$EXTRACTED\" }|" \
  "$CONSUMER/Cargo.toml"
rm -f "$CONSUMER/Cargo.toml.bak"
cd "$CONSUMER"
if [ -n "${DOLTLITE_PACKAGE_TARGET_DIR:-}" ]; then
  bash "$PKG_DIR/refresh-package.sh" "$EXTRACTED"
  export CARGO_TARGET_DIR="$DOLTLITE_PACKAGE_TARGET_DIR"
fi
DOLTLITE_EXPECT_VERSION="v$version" cargo run --release --quiet
