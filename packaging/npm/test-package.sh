#!/bin/bash
#
# Package-level smoke test for @dolthub/doltlite-wasm. Stages the package from
# the freshly built wasm distributables (ext/wasm/jswasm, produced by
# `make -C ext/wasm npm`), packs it the way `npm publish` would (honoring the
# "files" allowlist), installs the tarball into a throwaway consumer project,
# and runs smoke-test.mjs against it. This exercises the real package metadata
# -- exports map, files list, index.mjs -- not just the raw jswasm files.
#
# Assumes `make -C ext/wasm npm` has already run.

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"

STAGE="$(mktemp -d)"
CONSUMER="$(mktemp -d)"
trap 'rm -rf "$STAGE" "$CONSUMER"' EXIT

# Stage the package (version is irrelevant for the smoke test).
bash "$PKG_DIR/assemble.sh" "0.0.0-ci" "$STAGE/pkg"

# Pack exactly what would be published, then install that tarball.
TGZ="$(cd "$STAGE/pkg" && npm pack --silent)"
cd "$CONSUMER"
npm init -y >/dev/null 2>&1
npm install --no-audit --no-fund --silent "$STAGE/pkg/$TGZ"

# Run the smoke test from the consumer so it resolves the installed package.
node "$PKG_DIR/smoke-test.mjs"
