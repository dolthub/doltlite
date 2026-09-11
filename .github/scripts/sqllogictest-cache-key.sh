#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
{
  sha256sum .github/scripts/sqllogictest-reference.sh .github/scripts/sqllogictest-cache-key.sh \
    test/patch_sqllogictest.pl "$(command -v gcc)" "$(command -v as)" "$(command -v ld)"
  gcc --version
  gcc -dumpmachine
  ld --version
  fossil version
  perl -V
  dpkg-query -W gcc libc6-dev binutils unixodbc-dev libodbc2 libodbcinst2
  printf '%s\n' "${ImageOS:-}" "${ImageVersion:-}" \
    "${CPATH:-}" "${C_INCLUDE_PATH:-}" "${LIBRARY_PATH:-}" "${GCC_EXEC_PREFIX:-}"
} | sha256sum | cut -d ' ' -f 1
