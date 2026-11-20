#!/usr/bin/env bash
set -euo pipefail

export PACKAGE_VERSION="${1:?gem version required}"
ruby_checkout="$(cd "${2:?Ruby package checkout required}" && pwd)"
assets="$(cd "${3:?native release assets required}" && pwd)"
candidate="${4:?candidate directory required}"
mkdir -p "$candidate"
candidate="$(cd "$candidate" && pwd)"

cd "$ruby_checkout"
ruby -pi -e 'sub(/VERSION = "[^"]*"/, %(VERSION = "#{ENV.fetch("PACKAGE_VERSION")}"))' lib/doltlite/version.rb
bash script/build-gems.sh "$PACKAGE_VERSION" "$assets"
cp pkg/*.gem lib/doltlite/version.rb "$candidate/"
