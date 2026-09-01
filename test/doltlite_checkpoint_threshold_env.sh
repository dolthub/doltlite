#!/usr/bin/env bash
# The CLI and libdoltlite are not SQLITE_TEST builds. Production must ignore
# DOLTLITE_WAL_CHECKPOINT_THRESHOLD; testfixture still honors it.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"
DOLTLITE="${1:-./doltlite}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 required to read checkpoint stamps"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-ckpt-env.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
DB="$TMP/t.db"

checkpoint_magic() {
  python3 -c '
import struct, sys
path = sys.argv[1]
with open(path, "rb") as f:
    f.seek(0, 2)
    n = f.tell()
    if n < 169:
        print(0)
        raise SystemExit
    f.seek(n - 169)
    rec = f.read(169)
print(struct.unpack_from("<I", rec, 1 + 8)[0])
' "$1"
}

echo "=== WAL checkpoint threshold env is test-only ==="

export DOLTLITE_WAL_CHECKPOINT_THRESHOLD=1
"$DOLTLITE" "$DB" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB);
   INSERT INTO t VALUES(1, randomblob(8192));
   SELECT dolt_commit('-A','-m','env-threshold');" >/dev/null
magic="$(checkpoint_magic "$DB")"
if [ "$magic" = "0" ]; then
  dltest_pass
else
  dltest_fail "cli_ignores_checkpoint_threshold_env" \
    "  expected checkpoint magic 0 (64MiB default), got $magic"
fi

dltest_finish
