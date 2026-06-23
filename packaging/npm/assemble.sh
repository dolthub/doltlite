#!/bin/bash
#
# Stage the @dolthub/doltlite-wasm npm package into an output directory ready
# for `npm publish`. Copies the WASM distributables produced by
# `make -C ext/wasm npm` (they land in ext/wasm/jswasm) alongside this
# directory's package.json/index.mjs/README, stamps the version, and drops in
# the repo LICENSE.
#
# Usage: assemble.sh <version> <out_dir>
#   version  release version without the leading "v" (e.g. 0.11.15)
#   out_dir  directory to create and populate (e.g. dist/npm)

set -euo pipefail

VERSION="${1:?usage: assemble.sh <version> <out_dir>}"
OUT_DIR="${2:?usage: assemble.sh <version> <out_dir>}"

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"
JSWASM="$ROOT/ext/wasm/jswasm"

# The distributables `make -C ext/wasm npm` produces (see the npm_files list
# in ext/wasm/GNUmakefile). Keep this in sync with package.json "files".
ARTIFACTS=(
  sqlite3.mjs
  sqlite3.wasm
  sqlite3-bundler-friendly.mjs
  sqlite3-node.mjs
  sqlite3-opfs-async-proxy.js
  sqlite3-worker1.mjs
  sqlite3-worker1-bundler-friendly.mjs
  sqlite3-worker1-promiser.mjs
)

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

missing=0
for f in "${ARTIFACTS[@]}"; do
  if [ ! -f "$JSWASM/$f" ]; then
    echo "ERROR: missing wasm artifact: $JSWASM/$f (did 'make -C ext/wasm npm' run?)" >&2
    missing=1
    continue
  fi
  cp "$JSWASM/$f" "$OUT_DIR/$f"
done
[ "$missing" = 0 ] || exit 1

cp "$PKG_DIR/index.mjs" "$OUT_DIR/index.mjs"
cp "$PKG_DIR/README.md" "$OUT_DIR/README.md"
cp "$ROOT/LICENSE.md"   "$OUT_DIR/LICENSE.md"
cp "$PKG_DIR/package.json" "$OUT_DIR/package.json"

# Stamp the version into the staged package.json without depending on jq.
node -e '
  const fs = require("fs");
  const p = process.argv[1], v = process.argv[2];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  j.version = v;
  fs.writeFileSync(p, JSON.stringify(j, null, 2) + "\n");
' "$OUT_DIR/package.json" "$VERSION"

echo "Staged @dolthub/doltlite-wasm@$VERSION in $OUT_DIR:"
ls -la "$OUT_DIR"
