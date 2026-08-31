#!/bin/bash
#
# Package-level smoke test for DoltHub.Doltlite. Packs the package from a
# build directory's shared library, installs it into a throwaway consumer
# project from a local NuGet feed, and runs the smoke test against the
# installed package. This exercises the real package payload -- the managed
# init assembly and the runtimes/<rid>/native library resolution -- not just
# the raw source files. Dependencies (SQLitePCLRaw, Microsoft.Data.Sqlite.Core)
# restore from nuget.org.
#
# Usage: test-package.sh [build_dir]   (default: ./build)

set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$PKG_DIR/../.." && pwd)"
BUILD_DIR="$(cd "${1:-$ROOT/build}" && pwd)"

command -v dotnet >/dev/null || { echo "ERROR: dotnet is required" >&2; exit 1; }

STAGE="$(mktemp -d)"
CONSUMER="$(mktemp -d)"
trap 'rm -rf "$STAGE" "$CONSUMER"' EXIT

bash "$PKG_DIR/assemble.sh" "0.0.0-ci" "$STAGE/feed" "$BUILD_DIR"

cp "$PKG_DIR/smoke-test/smoke-test.csproj" "$CONSUMER/"
cp "$PKG_DIR/smoke-test/Program.cs" "$CONSUMER/"

# NuGet reads the source as a literal path, and under Git Bash a shell path
# like /tmp/x resolves to C:\tmp\x for the native dotnet. Hand it the OS path.
FEED="$STAGE/feed"
if command -v cygpath >/dev/null 2>&1; then
  FEED="$(cygpath -w "$FEED")"
fi

cat > "$CONSUMER/nuget.config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="local" value="$FEED" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
EOF

cd "$CONSUMER"
dotnet run -c Release --nologo
