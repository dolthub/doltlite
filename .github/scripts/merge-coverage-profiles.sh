#!/usr/bin/env bash

set -euo pipefail

dir="${1:?Usage: merge-coverage-profiles.sh <profile-dir> [output-name]}"
name="${2:-coverage}"
list=$(mktemp)
trap 'rm -f "$list"' EXIT
find "$dir" -type f -name '*.profraw' | LC_ALL=C sort > "$list"
if [ ! -s "$list" ]; then
  echo "no profraw files to merge"
  exit 0
fi
echo "merging $(wc -l < "$list") profraw files"
llvm-profdata merge -sparse -f "$list" -o "$dir/$name.profdata"
while IFS= read -r profile; do
  printf '%s\0' "$profile"
done < "$list" | xargs -0 rm -f
