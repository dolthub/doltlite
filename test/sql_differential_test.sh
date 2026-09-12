#!/usr/bin/env bash
#
# Randomized differential test: run a generated SQL script through doltlite and
# through stock SQLite and require identical output. The generator shapes the
# workload around explicit BEGIN/SAVEPOINT blocks with reads interleaved between
# writes, which is the state space the merged-cursor bugs lived in and which no
# hand-written suite covers.
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
#   constraints triggers returning generated fkeys

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
  # The value axes only. Every group is clean now, so this is about what belongs
  # in a pull request's path: these two are the cheapest per seed, and a gate
  # that blocks unrelated work should be the narrow one. The nightly runs every
  # group over a much wider window, and reports what it finds as an issue
  # instead of blocking anyone.
  for g in large-ints desc; do
    GENFLAGS="$GENFLAGS --include-$g"
  done
else
  for g in $SEL_GROUPS; do
    GENFLAGS="$GENFLAGS --include-$g"
  done
fi

echo "=== SQL differential sweep: seeds $FIRST..$LAST ==="
echo "    doltlite: $DOLTLITE"
echo "    stock:    $SQLITE3"
[ -n "$GENFLAGS" ] && echo "    groups:  $GENFLAGS"
echo ""

python3 "$SCRIPT_DIR/sql_differential_sweep_test.py" -q || exit 1
python3 -u "$SWEEP" "$DOLTLITE" "$SQLITE3" "$FIRST" "$LAST" $GENFLAGS
