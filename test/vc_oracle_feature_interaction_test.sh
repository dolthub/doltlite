#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$SCRIPT_DIR/lib/vc_oracle_common.sh"

normalize() {
  tr -d '\r' | grep -v '^$' | sort
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  printf "%s\n" "$setup" | "$DOLTLITE" "$dir/dl/db" \
      >/dev/null 2>"$dir/dl.err"
  local dl_out
  dl_out=$(printf ".headers off\n.mode csv\n%s\n" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
           | grep -vi 'already up to date' \
           | grep -vi 'Fast-forward' \
           | tr -d '"' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dolt_query
  dolt_query=$(vc_oracle_translate_for_dolt "$query")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n" "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>>"$dir/dt.err" \
      | tail -n +2 | tr -d '"'
  ) 2>/dev/null
  dt_out=$(echo "$dt_out" | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Feature Interaction Oracle Tests ==="
echo ""

for family in merge history schema query stress; do
  source "$SCRIPT_DIR/lib/vc_oracle_feature_interactions/$family.sh"
done

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
