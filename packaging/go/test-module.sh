#!/usr/bin/env bash
set -euo pipefail

version="${1:?module version required}"
module_dir="$(cd "${2:?staged module directory required}" && pwd)"
pkg_dir="$(cd "$(dirname "$0")" && pwd)"
module=github.com/dolthub/doltlite-driver
work="$(mktemp -d)"
trap 'chmod -R u+w "$work" && rm -rf "$work"' EXIT

repo="$work/driver"
mkdir "$repo" "$work/consumer"
cp -R "$module_dir/." "$repo/"
git -C "$repo" init --quiet
git -C "$repo" add -A
git -C "$repo" -c user.name='DoltLite Tests' -c user.email='tests@dolthub.com' commit --quiet -m "doltlite v$version"
git -C "$repo" tag "v$version"

cp "$pkg_dir/smoke-test/"*.go "$pkg_dir/smoke-test/go.mod" "$work/consumer/"
cd "$work/consumer"
export CGO_ENABLED=1 GOWORK=off GOPROXY=direct GOSUMDB=off GOMODCACHE="$work/modules"
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="url.file://$repo.insteadOf" GIT_CONFIG_VALUE_0="https://$module"
export GIT_ALLOW_PROTOCOL=file
go mod edit -require "$module@v$version"
go mod download "$module"
DOLTLITE_EXPECT_VERSION="$version" go run .
