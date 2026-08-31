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

# Per-run package cache. The staged package always carries the same version,
# and NuGet keys its global cache on id+version alone -- without this it
# restores whatever 0.0.0-ci it extracted first and silently ignores the
# package built from the current tree.
export NUGET_PACKAGES="$CONSUMER/.nuget"

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

# A library that loads but is not an engine, for the rejection check. Built
# rather than borrowed from the system: what a system library resolves varies
# by platform (on macOS the system SQLite is reachable through libSystem).
case "$(uname -s)" in
  Darwin) DECOY_EXT=dylib ;;
  MINGW*|MSYS*|CYGWIN*) DECOY_EXT=dll ;;
  *) DECOY_EXT=so ;;
esac
printf 'int doltlite_decoy(void){ return 0; }\n' > "$CONSUMER/decoy.c"
if cc -shared -o "$CONSUMER/decoy.$DECOY_EXT" "$CONSUMER/decoy.c" 2>/dev/null; then
  DECOY="$CONSUMER/decoy.$DECOY_EXT"
  if command -v cygpath >/dev/null 2>&1; then
    DECOY="$(cygpath -w "$DECOY")"
  fi
  export DOLTLITE_TEST_DECOY="$DECOY"
fi

cd "$CONSUMER"
dotnet run -c Release --nologo
