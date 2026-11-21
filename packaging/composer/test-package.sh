#!/bin/bash
#
# Package-level smoke test for dolthub/doltlite-php. Stages the package from
# a build directory's shared library, installs it into a throwaway consumer
# project through Composer (path repository, offline), and runs
# smoke-test.php against the installed package. This exercises the real
# package metadata -- autoload map, bundled library resolution -- not just
# the raw source files.
#
# Usage: test-package.sh [build_dir]
#        test-package.sh --staged <version> <package_dir> <engine_version>

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"

command -v php >/dev/null || { echo "ERROR: php is required" >&2; exit 1; }
command -v composer >/dev/null || { echo "ERROR: composer is required" >&2; exit 1; }
php -r 'exit(extension_loaded("ffi") ? 0 : 1);' \
  || { echo "ERROR: the php ffi extension is required" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [ "${1:-}" = --staged ]; then
  version="${2:?package version required}"
  package="$(cd "${3:?package directory required}" && pwd)"
  export DOLTLITE_EXPECT_VERSION="${4:?engine version required}"
else
  version=0.0.0
  package="$WORK/pkg"
  bash "$PKG_DIR/assemble.sh" "$version" "$package" "${1:-$ROOT/build}"
fi

mkdir "$WORK/consumer"
cd "$WORK/consumer"
cat > composer.json <<EOF
{
  "repositories": [
    { "type": "path", "url": "$package", "options": { "symlink": false } },
    { "packagist.org": false }
  ],
  "require": { "dolthub/doltlite-php": "$version" }
}
EOF
composer install --no-interaction --quiet

cp "$PKG_DIR/smoke-test.php" .
env -u DOLTLITE_PHP_LIB DOLTLITE_PACKAGE_VERSION="$version" php -d ffi.enable=1 smoke-test.php
