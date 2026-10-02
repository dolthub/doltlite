#!/bin/bash

dltest_expect_integrity() {
  if [ -z "${DLTEST_INTEGRITY_EXPECTATIONS:-}" ]; then
    export DLTEST_INTEGRITY_EXPECTATIONS=$(mktemp "${TMPDIR:-/tmp}/dltest_integrity.XXXXXX")
  fi
  printf '%s\t%s\n' "$1" "$2" >> "$DLTEST_INTEGRITY_EXPECTATIONS"
}

dltest_checked_engine() {
  if [ "${DOLTLITE##*/}" = dltest_engine_guard.pl ]; then
    "$DOLTLITE" "$@"
  else
    DLTEST_REAL_DOLTLITE="$DOLTLITE" "$_dltest_integrity_guard" "$@"
  fi
}

_dltest_integrity_guard="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dltest_engine_guard.pl"
export -f dltest_expect_integrity
