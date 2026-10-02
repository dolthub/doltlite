#!/usr/bin/env bash
#
# Randomized differential test: run a generated SQL script through doltlite and
# through stock SQLite and require identical output. The generator pauses a
# SELECT after two rows (.scan-pause), runs a write, a savepoint, and DDL on
# that same connection, then finishes the scan. Every two-hundredth seed also
# bulk-loads wide rows so the prolly tree has more than one leaf, then reopens
# the database and repeats the reads.
#
# The stock reference must be built from this tree with DOLTLITE_PROLLY=0
# (`make DOLTLITE_PROLLY=0 sqlite3`), same as the other oracle suites use. A
# different SQLite version is not a valid reference here: text rendering of
# large reals changed in 3.44, so version skew shows up as a false divergence.
#
# Usage: sql_differential_test.sh [doltlite] [stock] [first-seed] [last-seed]
#
# Feature groups are selected with DOLTLITE_DIFF_GROUPS: a space-separated list,
# "all" for every group, "default" (the default) for every group that is clean,
# or "" for the base single-table workload.
# Running one group is how a divergence gets attributed. Groups:
#   large-ints desc expr agg setops cte window joins writesel ddl
#   constraints triggers returning generated fkeys rowid
#
# DOLTLITE_DIFF_BULK is the generate_series row count (default 600, 0 disables).
# DOLTLITE_DIFF_BULK_EVERY is how often a seed takes that load (default 200).
# 600 wide rows is enough for several leaves; 5k-50k still works, it just does
# not fit on every seed of the pull-request shard.

set -uo pipefail

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3-stock}"
FIRST="${3:-${DOLTLITE_DIFF_FIRST_SEED:-1}}"
LAST="${4:-${DOLTLITE_DIFF_LAST_SEED:-200}}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GEN="$SCRIPT_DIR/sql_differential_fuzzer.py"
SWEEP="$SCRIPT_DIR/sql_differential_sweep.py"

for bin in "$DOLTLITE" "$SQLITE3"; do
  if [ ! -x "$bin" ]; then
    echo "ERROR: not executable: $bin"
    exit 1
  fi
done
if [ ! -f "$GEN" ] || [ ! -f "$SWEEP" ]; then
  echo "ERROR: missing generator or sweep runner"
  exit 1
fi

# A reference sharing doltlite's storage would compare the engine against
# itself and pass no matter what broke. Asking whether the dolt_* functions are
# linked in is the wrong question -- they can be present while the storage is
# stock -- so defer to the shared check, which asks what the binary writes and
# is the same check CI runs where each reference is built.
if ! bash "$SCRIPT_DIR/assert_stock_reference.sh" "$SQLITE3" "$DOLTLITE"; then
  exit 1
fi
if ! bash "$SCRIPT_DIR/lib/assert_doltlite_engine.sh" "$DOLTLITE"; then
  exit 1
fi
if python3 -c 'import os,sys; sys.exit(0 if os.path.samefile(sys.argv[1], sys.argv[2]) else 1)' \
     "$DOLTLITE" "$SQLITE3" 2>/dev/null; then
  echo "ERROR: candidate and reference are the same file: $DOLTLITE"
  echo "       sql-differential would compare stock sqlite3 with itself and pass."
  exit 1
fi

# Not named GROUPS: bash keeps that as the caller's group-id array and silently
# ignores assignments to it.
SEL_GROUPS="${DOLTLITE_DIFF_GROUPS-default}"
GENFLAGS=""
if [ "$SEL_GROUPS" = "all" ]; then
  GENFLAGS="--all"
elif [ "$SEL_GROUPS" = "default" ]; then
  # Every pull-request seed runs the cheap value axes and rowid allocation.
  # One more group rotates with the seed, so triggers, foreign keys, DDL, and
  # the rest each appear in a pull request without every seed paying for all
  # of them. The nightly still runs every group on every seed.
  for g in large-ints desc rowid; do
    GENFLAGS="$GENFLAGS --include-$g"
  done
  GENFLAGS="$GENFLAGS --rotate"
else
  for g in $SEL_GROUPS; do
    GENFLAGS="$GENFLAGS --include-$g"
  done
fi

# A few hundred wide rows cross a prolly leaf. Doing that on every seed blows
# the 15-minute pull-request shard and the nightly window, so most seeds stay
# on the small script and every Kth seed takes the load.
BULK_N="${DOLTLITE_DIFF_BULK:-600}"
BULK_EVERY="${DOLTLITE_DIFF_BULK_EVERY:-200}"
if [ "$BULK_N" != "0" ]; then
  GENFLAGS="$GENFLAGS --bulk=$BULK_N --bulk-every=$BULK_EVERY"
fi

echo "=== SQL differential sweep: seeds $FIRST..$LAST ==="
echo "    doltlite: $DOLTLITE"
echo "    stock:    $SQLITE3"
[ -n "$GENFLAGS" ] && echo "    groups:  $GENFLAGS"
echo ""

python3 "$SCRIPT_DIR/sql_differential_sweep_test.py" -q || exit 1

# The header's leaf-boundary claim is a property of the generated script, not
# of a hand-written insert. One 600-row load must produce an internal prolly
# node (a single leaf has none).
python3 - "$GEN" "$DOLTLITE" <<'PY' || exit 1
import importlib.util
import os
import subprocess
import sys
import tempfile

gen_path, binary = sys.argv[1], sys.argv[2]
spec = importlib.util.spec_from_file_location("sql_differential_fuzzer", gen_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
sql = mod.Gen(1, [], bulk=600).run()
# Schema plus the bulk insert. Later random statements are not what makes
# the tree tall, and a rejected one would hide a leaf-count failure.
kept = []
for line in sql.splitlines():
    kept.append(line)
    if "generate_series" in line:
        break
else:
    sys.stderr.write("generated script has no bulk insert\n")
    sys.exit(1)
sql = "\n".join(kept) + "\n"
work = tempfile.mkdtemp()
db = os.path.join(work, "t.db")
proc = subprocess.run([binary, db], input=sql.encode(),
                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
if proc.returncode != 0:
    sys.stderr.write(proc.stdout.decode("utf-8", "replace")[:2000])
    sys.stderr.write("\nbulk script failed rc=%d\n" % proc.returncode)
    sys.exit(1)
blob = b""
paths = [db]
if os.path.isdir(db):
    for root, dirs, files in os.walk(db):
        for name in files:
            paths.append(os.path.join(root, name))
else:
    for suffix in ("-wal", "-shm"):
        paths.append(db + suffix)
for path in paths:
    if os.path.isfile(path):
        with open(path, "rb") as fh:
            blob += fh.read()
leaves, internals = mod.count_prolly_nodes(blob)
sys.stdout.write("prolly nodes: %d leaves, %d internal\n" % (leaves, internals))
if leaves < 2 or internals < 1:
    sys.stderr.write("bulk load did not build a multi-leaf prolly tree\n")
    sys.exit(1)
PY

python3 -u "$SWEEP" "$DOLTLITE" "$SQLITE3" "$FIRST" "$LAST" $GENFLAGS
