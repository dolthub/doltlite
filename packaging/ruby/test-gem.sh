#!/usr/bin/env bash
set -euo pipefail

archive="${1:?gem archive required}"
version="${2:?gem version required}"
platform="${3:?gem platform required}"
test -f "$archive"
archive="$(cd "$(dirname "$archive")" && pwd)/$(basename "$archive")"
test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

export GEM_HOME="$work/gems"
export GEM_PATH="$GEM_HOME"
unset RUBYLIB RUBYOPT
cd "$work"
gem install --norc --no-document "$archive"
cmp "$archive" "$GEM_HOME/cache/$(basename "$archive")"
ruby "$test_dir/test-gem.rb" "$version" "$platform"
