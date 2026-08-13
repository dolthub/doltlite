#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
version=$(tr -d '[:space:]' < "$repo_root/.dolt-oracle-version")
install_root=${1:-/usr/local}
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) platform=linux-amd64 ;;
  Linux:aarch64|Linux:arm64) platform=linux-arm64 ;;
  Darwin:x86_64) platform=darwin-amd64 ;;
  Darwin:arm64) platform=darwin-arm64 ;;
  *)
    echo "unsupported Dolt oracle platform: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac
archive=${RUNNER_TEMP:-${TMPDIR:-/tmp}}/dolt-${platform}-${version}.tar.gz

if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid Dolt oracle version: $version" >&2
  exit 1
fi

curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
  -o "$archive" \
  "https://github.com/dolthub/dolt/releases/download/${version}/dolt-${platform}.tar.gz"

if [ -w "$install_root" ]; then
  tar -xzf "$archive" -C "$install_root" --strip-components=1
else
  sudo tar -xzf "$archive" -C "$install_root" --strip-components=1
fi

installed=$("$install_root/bin/dolt" version)
printf '%s\n' "$installed"
installed_version=$(awk '$1=="dolt" && $2=="version" {print "v" $3; exit}' <<<"$installed")
if [ "$installed_version" != "$version" ]; then
  echo "installed Dolt reports $installed_version, expected $version" >&2
  exit 1
fi
