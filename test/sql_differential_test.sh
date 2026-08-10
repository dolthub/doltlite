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
# Two axes are off by default because they have known open bugs; enabling them
# is how this suite grows once those land:
#   DOLTLITE_DIFF_LARGE_INTS=1  integers beyond 2^53 (needs PR #2075)
#   DOLTLITE_DIFF_DESC=1        DESC indexes (issue #2077)

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

# A doltlite build answering as the stock reference would compare the engine
# against itself and pass no matter what broke.
if "$SQLITE3" :memory: \
     "SELECT 1 FROM pragma_function_list WHERE name='dolt_version';" \
     2>/dev/null | grep -q 1; then
  echo "ERROR: $SQLITE3 provides dolt_version, so it is not a stock reference"
  exit 1
fi

GENFLAGS=""
[ "${DOLTLITE_DIFF_LARGE_INTS:-0}" = "1" ] && GENFLAGS="$GENFLAGS --include-large-ints"
[ "${DOLTLITE_DIFF_DESC:-0}" = "1" ] && GENFLAGS="$GENFLAGS --include-desc"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "=== SQL differential sweep: seeds $FIRST..$LAST ==="
echo "    doltlite: $DOLTLITE"
echo "    stock:    $SQLITE3"
[ -n "$GENFLAGS" ] && echo "    axes:    $GENFLAGS"
echo ""

pass=0
fail=0
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
    continue
  fi

  fail=$((fail + 1))
  failed_seeds="$failed_seeds $seed"
  if [ "$fail" -le 5 ]; then
    echo "  FAIL: seed $seed (doltlite rc=$rc_dl, stock rc=$rc_sq)"
    diff <(printf '%s\n' "$out_dl") <(printf '%s\n' "$out_sq") \
      | head -20 | sed 's/^/    /'
    cp "$sql" "${DOLTLITE_DIFF_SAVE_DIR:-$WORK}/seed_$seed.sql" 2>/dev/null || true
  fi
done

echo ""
echo "Results: $pass passed, $fail failed out of $((pass + fail)) seeds"
if [ "$fail" -gt 0 ]; then
  echo "Failing seeds:$failed_seeds"
  echo "Reproduce with: python3 test/sql_differential_fuzzer.py <seed>$GENFLAGS"
  exit 1
fi
