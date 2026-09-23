#!/usr/bin/env bash
# A WAL checkpoint keeps older index runs and rewrites only its newest ones,
# so a stream of small commits writes index bytes linear in its length.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"
DOLTLITE="${1:-$DOLTLITE}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 required to read checkpoint stamps"
  exit 0
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-ckpt-runs.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
export DOLTLITE_WAL_CHECKPOINT_CHUNKS=16

inserts() {
  python3 -c "
print('CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);')
print('PRAGMA synchronous=OFF;')
for i in range(1, $1+1): print(\"INSERT INTO t VALUES(%d,'v%d');\" % (i, i))
"
}

# Prints the tail stamp's checkpoint magic and, for a V3 stamp, its run count.
stamp() {
  python3 -c '
import struct, sys
d = open(sys.argv[1], "rb").read()
r = d[-169:]
magic = struct.unpack_from("<I", r, 1 + 8)[0]
off = struct.unpack_from("<q", r, 1 + 12)[0]
runs = struct.unpack_from("<I", d, off + 25 + 4)[0] if magic == 0x33504b43 else 0
print(magic, runs)
' "$1"
}

echo "=== WAL checkpoints keep older index runs ==="

SMALL="$TMP/small.db"
BIG="$TMP/big.db"
inserts 1500 | "$DOLTLITE" "$SMALL" >/dev/null 2>&1
inserts 3000 | "$DOLTLITE" "$BIG" >/dev/null 2>&1

read -r magic runs <<<"$(stamp "$BIG")"
if [ "$magic" = "860900163" ] && [ "$runs" -ge 2 ]; then
  dltest_pass
else
  dltest_fail "checkpoint_has_runs" "  expected a V3 stamp with 2+ runs, got magic=$magic runs=$runs"
fi

# A full rewrite per checkpoint grows the file ~4x per doubling here.
ratio=$(python3 -c "import os,sys; print('%.2f' % (os.path.getsize(sys.argv[2])/os.path.getsize(sys.argv[1])))" "$SMALL" "$BIG")
if python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) < 2.8 else 1)" "$ratio"; then
  dltest_pass
else
  dltest_fail "checkpoint_bytes_linear" "  file grew ${ratio}x when the commit count doubled"
fi

run_test "checkpoint_runs_reopen_count" "SELECT count(*), sum(id) FROM t;" "3000|4501500" "$BIG"
run_test "checkpoint_runs_lookup_oldest" "SELECT v FROM t WHERE id=1;" "v1" "$BIG"
run_test "checkpoint_runs_lookup_middle" "SELECT v FROM t WHERE id=1777;" "v1777" "$BIG"
run_test "checkpoint_runs_lookup_newest" "SELECT v FROM t WHERE id=3000;" "v3000" "$BIG"
run_test "checkpoint_runs_integrity" "PRAGMA integrity_check;" "ok" "$BIG"

run_test "checkpoint_runs_write_after_reopen" \
  "INSERT INTO t VALUES(3001,'v3001'); SELECT count(*) FROM t;" "3001" "$BIG"
run_test "checkpoint_runs_gc" "SELECT dolt_gc() IS NOT NULL;" "1" "$BIG"
run_test "checkpoint_runs_after_gc" \
  "SELECT count(*) FROM t; PRAGMA integrity_check;" "3001
ok" "$BIG"
python3 -c "
for i in range(3002, 3302): print(\"INSERT INTO t VALUES(%d,'v%d');\" % (i, i))
" | "$DOLTLITE" "$BIG" >/dev/null 2>&1
run_test "checkpoint_runs_rebuilt_after_gc" \
  "SELECT count(*), max(id) FROM t; PRAGMA integrity_check;" "3301|3301
ok" "$BIG"

dltest_finish
