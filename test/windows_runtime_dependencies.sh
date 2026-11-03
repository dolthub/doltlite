#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo 'Usage: windows_runtime_dependencies.sh <exe-or-dll> [...]' >&2
  exit 2
fi
for artifact in "$@"; do
  imports=$("${OBJDUMP:-objdump}" -p "$artifact")
  dependencies=$(printf '%s\n' "$imports" | sed -n 's/.*DLL Name: //p')
  if [ -z "$dependencies" ]; then
    echo "ERROR: no PE imports found in $artifact" >&2
    exit 1
  fi
  echo "$artifact imports:"
  printf '%s\n' "$dependencies"
  if printf '%s\n' "$dependencies" | grep -Ei \
      '^(libgcc[^/]*|libwinpthread[^/]*|libstdc\+\+[^/]*|libssp[^/]*|zlib1)\.dll[[:space:]]*$'; then
    echo "ERROR: $artifact requires a non-system runtime DLL" >&2
    exit 1
  fi
done
