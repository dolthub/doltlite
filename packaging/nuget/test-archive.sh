#!/usr/bin/env bash
set -euo pipefail

archive="${1:?usage: test-archive.sh <archive> <package-version> [engine-version]}"
version="${2:?package version is required}"
export DOLTLITE_TEST_ENGINE_VERSION="${3:-}"
unset DOLTLITE_NET_LIB DOLTLITE_TEST_DECOY

pkg_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
consumer="$(mktemp -d)"
trap 'rm -rf "$consumer"' EXIT
mkdir "$consumer/feed"
cp "$archive" "$consumer/feed/"
cp "$pkg_dir/smoke-test/smoke-test.csproj" "$pkg_dir/smoke-test/Program.cs" "$consumer/"

cd "$consumer"
cat > NuGet.Config <<'EOF'
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="candidate" value="feed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="candidate">
      <package pattern="DoltHub.Doltlite" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
EOF

case "$(uname -s)" in
  Darwin) decoy_ext=dylib ;;
  MINGW*|MSYS*|CYGWIN*) decoy_ext=dll ;;
  *) decoy_ext=so ;;
esac
printf 'int doltlite_decoy(void){ return 0; }\n' > decoy.c
if cc -shared -o "decoy.$decoy_ext" decoy.c 2>/dev/null; then
  decoy="$PWD/decoy.$decoy_ext"
  if command -v cygpath >/dev/null 2>&1; then
    decoy="$(cygpath -w "$decoy")"
  fi
  export DOLTLITE_TEST_DECOY="$decoy"
fi

version_property="-p:DoltlitePackageVersion=$version"
dotnet restore --configfile NuGet.Config --packages .packages "$version_property"
dotnet build --no-restore -c Release -o out --nologo "$version_property"
dotnet out/smoke-test.dll
