#!/usr/bin/env bash
set -euo pipefail

ccache --show-stats --verbose
ccache --print-stats | awk '
  $1 == "cache_miss" || $1 == "direct_cache_hit" || $1 == "preprocessed_cache_hit" { calls += $2 }
  $1 == "files_in_cache" { files = $2 }
  END { exit calls == 0 || files == 0 }
'
test -d "${CCACHE_DIR:?}"
