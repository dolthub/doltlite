#!/usr/bin/env bash
# Bounding open time is a deployment decision, so the CLI and libdoltlite
# honor DOLTLITE_WAL_CHECKPOINT_THRESHOLD and DOLTLITE_WAL_CHECKPOINT_CHUNKS
# rather than reserving them for SQLITE_TEST builds.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"
DOLTLITE="${1:-$DOLTLITE}"

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
        sys.stderr.write("file too short for a checkpoint stamp: %d bytes\n" % n)
        raise SystemExit(2)
    f.seek(n - 169)
    rec = f.read(169)
print(struct.unpack_from("<I", rec, 1 + 8)[0])
' "$1"
}

# CS_WAL_CHECKPOINT_MAGIC_V2.
CHECKPOINT_V2=844122947

seed_db() {
  local db="$1"
  dltest_require "cli_commit" "$db" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB);
     INSERT INTO t VALUES(1, randomblob(8192));
     SELECT dolt_commit('-A','-m','env-threshold');"
}

check_magic() {
  local name="$1" db="$2" want="$3" magic crc
  magic=$(checkpoint_magic "$db")
  crc=$?
  if [ "$crc" -ne 0 ]; then
    dltest_fail "$name" "  could not read checkpoint stamp (rc=$crc)"
  elif [ "$magic" = "$want" ]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected checkpoint magic $want, got $magic"
  fi
}

echo "=== WAL checkpoint limits are settable in production builds ==="

export DOLTLITE_WAL_CHECKPOINT_THRESHOLD=1
if ! seed_db "$DB"; then dltest_finish; fi
if [ ! -f "$DB" ]; then
  dltest_fail "cli_commit" "  expected a database file after commit"
  dltest_finish
fi
run_test "cli_commit_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"
check_magic "cli_honors_checkpoint_threshold_env" "$DB" "$CHECKPOINT_V2"
unset DOLTLITE_WAL_CHECKPOINT_THRESHOLD

CHUNK_DB="$TMP/chunks.db"
export DOLTLITE_WAL_CHECKPOINT_CHUNKS=1
if ! seed_db "$CHUNK_DB"; then dltest_finish; fi
check_magic "cli_honors_checkpoint_chunks_env" "$CHUNK_DB" "$CHECKPOINT_V2"
unset DOLTLITE_WAL_CHECKPOINT_CHUNKS

# A single small commit is far below both defaults, so nothing checkpoints
# when the environment says nothing.
UNSET_DB="$TMP/unset.db"
if ! seed_db "$UNSET_DB"; then dltest_finish; fi
check_magic "cli_defaults_do_not_checkpoint_one_commit" "$UNSET_DB" "0"

dltest_finish
