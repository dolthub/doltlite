#!/usr/bin/env bash
set -euo pipefail
DOLTLITE="${DOLTLITE_SYSTEM:-${1:-./doltlite}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/vc_stateful_merge_model_test.py" "$DOLTLITE"
echo "__SUITE_COMPLETE__"
