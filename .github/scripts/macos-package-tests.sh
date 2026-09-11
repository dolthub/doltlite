#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-build}"
logs=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/doltlite-package-logs.XXXXXX")
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT
chmod +x packaging/go/assemble.sh packaging/go/test-package.sh \
  packaging/rust/assemble.sh packaging/rust/test-package.sh
(
  export CC="ccache ${CC:-$(go env CC)}"
  bash packaging/go/test-package.sh "$build_dir"
) > "$logs/go.log" 2>&1 &
pids+=("$!")
(
  export CC="ccache ${CC:-cc}"
  bash packaging/rust/test-package.sh "$build_dir"
  bash packaging/rust/test-cache.sh
) > "$logs/rust.log" 2>&1 &
pids+=("$!")

status=0
names=(go rust)
for i in 0 1; do
  rc=0
  wait "${pids[$i]}" || rc=$?
  echo "::group::${names[$i]} package checks (exit $rc)"
  cat "$logs/${names[$i]}.log"
  echo '::endgroup::'
  if [ "$rc" -ne 0 ]; then
    echo "::error::${names[$i]} package checks failed with exit $rc"
    status=1
  fi
done
trap - EXIT
exit "$status"
