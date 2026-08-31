#!/bin/bash
#
# Package-level smoke test for dolthub/doltlite-php. Stages the package from
# a build directory's shared library, installs it into a throwaway consumer
# project through Composer (path repository, offline), and runs
# smoke-test.php against the installed package. This exercises the real
# package metadata -- autoload map, bundled library resolution -- not just
# the raw source files.
#
# Usage: test-package.sh [build_dir]   (default: ./build)

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"
BUILD_DIR="$(cd "${1:-$ROOT/build}" && pwd)"

command -v php >/dev/null || { echo "ERROR: php is required" >&2; exit 1; }
command -v composer >/dev/null || { echo "ERROR: composer is required" >&2; exit 1; }
php -r 'exit(extension_loaded("ffi") ? 0 : 1);' \
  || { echo "ERROR: the php ffi extension is required" >&2; exit 1; }

STAGE="$(mktemp -d)"
CONSUMER="$(mktemp -d)"
trap 'rm -rf "$STAGE" "$CONSUMER"' EXIT

bash "$PKG_DIR/assemble.sh" "0.0.0" "$STAGE/pkg" "$BUILD_DIR"

cd "$CONSUMER"
cat > composer.json <<EOF
{
  "repositories": [
    { "type": "path", "url": "$STAGE/pkg", "options": { "symlink": false } },
    { "packagist.org": false }
  ],
  "require": { "dolthub/doltlite-php": "0.0.0" }
}
EOF
composer install --no-interaction --quiet

cp "$PKG_DIR/smoke-test.php" .
php -d ffi.enable=1 smoke-test.php
