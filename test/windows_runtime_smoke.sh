#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build=$(cd "${1:-$root/build}" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

"${CC:-cc}" ${CFLAGS:-} -Wall -Wextra -Werror -static -municode \
  -I"$build" "$root/test/windows_runtime_smoke.c" -o "$work/probe.exe"
cp "$build/libdoltlite.dll" "$work/doltlite.dll"
bash "$root/test/windows_runtime_dependencies.sh" "$work/probe.exe" "$work/doltlite.dll"
cd "$work"
./probe.exe "$(cygpath -w "$work/doltlite.dll")"
