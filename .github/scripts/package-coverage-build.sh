#!/usr/bin/env bash
set -euo pipefail

source_dir="${1:?coverage artifact directory required}"
output_dir="${2:-.}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
cli=(build-coverage/doltlite build-coverage/doltlite-remotesrv \
     build-coverage/sqlite3-stock)
for file in "${cli[@]}" build-coverage/testfixture; do
  test -x "$source_dir/$file"
done
bash "$script_dir/package-ci-build.sh" "$output_dir/coverage-build.tar.gz" \
  -C "$source_dir" build-coverage
bash "$script_dir/package-ci-build.sh" "$output_dir/coverage-cli.tar.gz" \
  -C "$source_dir" "${cli[@]}"
bash "$script_dir/package-ci-build.sh" "$output_dir/coverage-fixture.tar.gz" \
  -C "$source_dir" "${cli[@]}" build-coverage/testfixture
