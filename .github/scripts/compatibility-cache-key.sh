#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
if ! tags=$(bash test/doltlite_compat_test.sh --list-tags); then
  printf '%s\n' "$tags" >&2
  exit 1
fi
{
  for tag in $tags; do
    printf '%s\n' "$tag"
    git rev-parse "$tag^{commit}"
  done
  sha256sum test/doltlite_compat_test.sh .github/scripts/compatibility-cache-key.sh \
    .github/actions/compatibility-cache/action.yml "$(command -v cc)"
  cc --version
  make --version
  ld --version
  uname -m
  dpkg-query -W gcc libc6-dev binutils make tcl-dev zlib1g-dev
  printf '%s\n' "${ImageOS:-}" "${ImageVersion:-}" "${CC:-}" \
    "${CFLAGS:-}" "${CPPFLAGS:-}" "${LDFLAGS:-}" "${DOLTLITE_COMPAT_JOBS:-4}"
} | sha256sum | cut -d ' ' -f 1
