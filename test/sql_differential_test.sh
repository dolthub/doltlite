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

for bin in "$DOLTLITE" "$SQLITE3"; do
  if [ ! -x "$bin" ]; then
    echo "ERROR: not executable: $bin"
    exit 1
  fi
done
if [ ! -f "$GEN" ]; then
  echo "ERROR: missing generator: $GEN"
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

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "=== SQL differential sweep: seeds $FIRST..$LAST ==="
echo "    doltlite: $DOLTLITE"
echo "    stock:    $SQLITE3"
[ -n "$GENFLAGS" ] && echo "    groups:  $GENFLAGS"
echo ""

pass=0
fail=0
errored=0
failed_seeds=""

for seed in $(seq "$FIRST" "$LAST"); do
  sql="$WORK/case.sql"
  if ! python3 "$GEN" "$seed" $GENFLAGS > "$sql" 2>"$WORK/gen.err"; then
    echo "  ERROR: generator failed for seed $seed"
    cat "$WORK/gen.err"
    fail=$((fail + 1))
    continue
  fi

  rm -f "$WORK/dl.db" "$WORK/dl.db-lock" "$WORK/dl.db-wal" "$WORK/sq.db"
  out_dl=$("$DOLTLITE" "$WORK/dl.db" < "$sql" 2>&1)
  rc_dl=$?
  out_sq=$("$SQLITE3" "$WORK/sq.db" < "$sql" 2>&1)
  rc_sq=$?

  if [ "$rc_dl" -eq "$rc_sq" ] && [ "$out_dl" = "$out_sq" ]; then
    pass=$((pass + 1))
    # Some workloads conflict on purpose: INSERT OR ROLLBACK, or a CHECK
    # violation from the constraints group. Both engines rejecting a statement
    # and agreeing on how is the comparison working, not the script breaking,
    # so count those rather than leave a nonzero exit looking accidental.
    case "$out_dl" in
      *Error*|*error*) errored=$((errored + 1)) ;;
    esac
    continue
  fi

  fail=$((fail + 1))
  failed_seeds="$failed_seeds $seed"
  # Every failing seed keeps its script, so a long sweep does not end up with
  # more failures than reproducers. Only the first few print a diff, because
  # that is about keeping the log readable.
  if [ -n "${DOLTLITE_DIFF_SAVE_DIR:-}" ]; then
    cp "$sql" "$DOLTLITE_DIFF_SAVE_DIR/seed_$seed.sql" 2>/dev/null || true
  fi
  if [ "$fail" -le 5 ]; then
    echo "  FAIL: seed $seed (doltlite rc=$rc_dl, stock rc=$rc_sq)"
    diff <(printf '%s\n' "$out_dl") <(printf '%s\n' "$out_sq") \
      | head -20 | sed 's/^/    /'
  elif [ "$fail" -eq 6 ]; then
    echo "  ... further diffs omitted; every failing seed is listed below and"
    echo "      its script saved to the artifact directory"
  fi
done

echo ""
echo "Results: $pass passed, $fail failed out of $((pass + fail)) seeds"
if [ "$errored" -gt 0 ]; then
  echo "          ($errored of the passing seeds had a statement both engines"
  echo "           rejected, and they agreed on the rejection)"
fi
if [ "$fail" -gt 0 ]; then
  echo "Failing seeds:$failed_seeds"
  echo "Reproduce with: python3 test/sql_differential_fuzzer.py <seed>$GENFLAGS"
  exit 1
fi
