#!/bin/bash

set -euo pipefail

DOLTLITE="${1:-./doltlite}"
# Windows CI wraps the engine in a bash script; Python needs the PE path.
DOLTLITE="${DOLTLITE_SYSTEM:-$DOLTLITE}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python3 "$SCRIPT_DIR/vc_stateful_fuzzer.py" "$DOLTLITE"
