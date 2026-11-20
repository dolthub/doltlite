#!/usr/bin/env bash
set -euo pipefail

binding="$(cd "${1:?Node binding checkout required}" && pwd)"
amalgamation="$(cd "${2:?amalgamation directory required}" && pwd)"
root="$(cd "$(dirname "$0")/../.." && pwd)"
version="0.0.0-ci.$(git -C "$root" rev-parse --short=12 HEAD)"
source_dir="$binding/amalgamation/.source"
consumer="$(mktemp -d)"
trap 'rm -rf "$consumer"' EXIT

mkdir -p "$source_dir/src" "$source_dir/ext/blake3"
cp "$amalgamation"/sqlite3.c "$amalgamation"/sqlite3.h "$amalgamation"/sqlite3ext.h "$source_dir/"
cp "$root"/src/*.h "$source_dir/src/"
cp "$root"/ext/blake3/*.[ch] "$source_dir/ext/blake3/"
printf 'v%s\n' "$version" > "$source_dir/.dolt_release_version"

cd "$binding"
npm ci --ignore-scripts --no-audit --no-fund
node -e '
  const fs = require("node:fs");
  const pkg = require("./package.json");
  pkg.version = process.argv[1];
  fs.writeFileSync("package.json", JSON.stringify(pkg, null, 2) + "\n");
' "$version"
node scripts/build-amalgamation.js
npm run build -- --jobs 2

mkdir -p "$consumer/node_modules/@dolthub"
ln -s "$binding" "$consumer/node_modules/@dolthub/doltlite"
cp scripts/consumer.test.cjs test/native.test.cjs "$consumer/"
cd "$consumer"
DOLTLITE_TEST_PACKAGE=@dolthub/doltlite DOLTLITE_EXPECT_VERSION="v$version" \
  node --expose-gc --test consumer.test.cjs native.test.cjs
