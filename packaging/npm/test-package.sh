#!/bin/bash
set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
browser=0
if [ "${1:-}" = --browser ]; then
  browser=1
  shift
fi
if [ "$#" -gt 1 ]; then
  echo "usage: test-package.sh [--browser] [tarball]" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
if [ "$#" -eq 1 ]; then
  tarball="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
  test -f "$tarball"
else
  bash "$PKG_DIR/assemble.sh" "0.0.0-ci" "$WORK/pkg"
  tarball="$WORK/$(cd "$WORK/pkg" && npm pack --silent --pack-destination "$WORK")"
fi

mkdir "$WORK/consumer"
cd "$WORK/consumer"
npm init -y >/dev/null 2>&1
npm install --no-audit --no-fund --silent "$tarball"

node "$PKG_DIR/smoke-test.mjs"
if [ "$browser" = 1 ]; then
  node --conditions=browser "$PKG_DIR/browser-smoke-test.mjs"
fi
