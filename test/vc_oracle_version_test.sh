#!/bin/bash
#
# Version-control oracle test: dolt_version
#
# dolt_version() is a 0-arg scalar that returns the build's version
# string. It's useful for:
#
#   - Peer negotiation in the decentralized use case — a client can
#     check the server's version before attempting format-sensitive
#     operations (clone, push, fetch).
#   - Debugging / bug reports: paste the output of SELECT dolt_version()
#     in an issue and we know exactly which build produced it.
#   - Schema migrations that behave differently per engine version.
#
# The interesting surface is small:
#
#   1. Zero args only. One arg or more must error, matching Dolt
#      which does the same ("function 'dolt_version' expected 0
#      arguments").
#
#   2. Deterministic: two invocations in the same session and
#      across re-opens return the same string.
#
#   3. Non-empty, no whitespace, plausible version shape. We
#      don't assert an exact format because doltlite uses
#      `git describe` output (e.g. v0.7.3-29-g61abf71) while
#      Dolt uses semver (1.83.5), and both are valid. The shape
#      check is that the string matches [A-Za-z0-9.+-]+ — any
#      reasonable version identifier lands in that alphabet.
#
#   4. Dolt conformance: both engines respect the same argcount
#      contract (0 args passes, 1+ errors).
#
# Usage: bash vc_oracle_version_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Run a single SQL statement against a fresh doltlite db and return
# the one-column scalar result, stripped of any shell/commit noise
# by tail -1.
dl_scalar() {
  local name="$1" sql="$2"
  local db="$TMPROOT/$name.db"
  rm -f "$db"
  "$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$name.err"
}

# Run a single SQL statement against a fresh Dolt repo and return
# the scalar result (column 1 of the first non-header row).
dolt_scalar() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_dt"
  mkdir -p "$dir"
  (
    cd "$dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$sql" | "$DOLT" sql -r csv 2>"$TMPROOT/$name.dt.err" \
      | tail -1 \
      | tr -d '"'
  )
}

# Run a statement that should error. Returns 0 if doltlite printed
# any "error"/"Error"/"ERROR" token, 1 otherwise.
dl_errored() {
  local name="$1" sql="$2"
  local db="$TMPROOT/${name}_err.db"
  rm -f "$db"
  "$DOLTLITE" "$db" "$sql" >"$TMPROOT/${name}.out" 2>"$TMPROOT/${name}.err"
  grep -qiE 'error|Error' "$TMPROOT/${name}.out" "$TMPROOT/${name}.err" 2>/dev/null
}

# Like dl_errored but for Dolt.
dolt_errored() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_dt_err"
  mkdir -p "$dir"
  (
    cd "$dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$sql" | "$DOLT" sql >"$TMPROOT/${name}.dt.out" 2>"$TMPROOT/${name}.dt.err"
  )
  grep -qiE 'error|Error' "$TMPROOT/${name}.dt.out" "$TMPROOT/${name}.dt.err" 2>/dev/null
}

expect_equal() {
  local name="$1" a="$2" b="$3"
  if [ "$a" = "$b" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    a=|$a|"
    echo "    b=|$b|"
  fi
}

expect_nonempty() {
  local name="$1" val="$2"
  if [ -n "$val" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (empty)"
  fi
}

expect_shape() {
  local name="$1" val="$2"
  if echo "$val" | grep -qE '^[A-Za-z0-9.+-]+$'; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (bad shape: |$val|)"
  fi
}

expect_true() {
  local name="$1" cond="$2"
  if [ "$cond" = "1" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
  fi
}

echo "=== Version Control Oracle Tests: dolt_version ==="
echo ""

# ── 1. Basic invocation ───────────────────────────────────
echo "--- 1. Returns a non-empty version string ---"

V=$(dl_scalar "basic" "SELECT dolt_version();")
expect_nonempty "dolt_version_returns_nonempty" "$V"
expect_shape    "dolt_version_plausible_shape" "$V"

echo ""

# ── 2. Determinism across calls ───────────────────────────
echo "--- 2. Deterministic across same-session calls ---"

V1=$(dl_scalar "det_a" "SELECT dolt_version();")
V2=$(dl_scalar "det_b" "SELECT dolt_version();")
expect_equal "dolt_version_same_twice" "$V1" "$V2"

DB="$TMPROOT/session.db"
rm -f "$DB"
V3=$("$DOLTLITE" "$DB" "SELECT dolt_version();" 2>"$TMPROOT/session.err")
V4=$("$DOLTLITE" "$DB" "SELECT dolt_version();" 2>>"$TMPROOT/session.err")
expect_equal "dolt_version_stable_across_reopen" "$V3" "$V4"

echo ""

# ── 3. Argcount contract ──────────────────────────────────
echo "--- 3. Rejects non-zero argcount ---"

if dl_errored "one_arg"   "SELECT dolt_version('x');"; then
  pass=$((pass+1)); echo "  PASS: rejects_one_arg"
else
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES rejects_one_arg"
  echo "  FAIL: rejects_one_arg (no error on extra arg)"
fi

if dl_errored "two_args"  "SELECT dolt_version('x', 'y');"; then
  pass=$((pass+1)); echo "  PASS: rejects_two_args"
else
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES rejects_two_args"
  echo "  FAIL: rejects_two_args (no error on two args)"
fi

echo ""

# ── 4. Works inside a transaction and alongside DML ───────
echo "--- 4. Callable mid-transaction ---"

DB="$TMPROOT/mid_txn.db"
rm -f "$DB"
V_MID=$(printf 'CREATE TABLE t(id INT PRIMARY KEY);\nBEGIN;\nINSERT INTO t VALUES(1);\nSELECT dolt_version();\nROLLBACK;\n' \
  | "$DOLTLITE" "$DB" 2>"$TMPROOT/mid_txn.err" | tail -1)
expect_nonempty "dolt_version_mid_transaction" "$V_MID"
expect_equal    "dolt_version_same_in_txn"     "$V_MID" "$V"

echo ""

# ── 5. Dolt conformance ───────────────────────────────────
echo "--- 5. Dolt conformance ---"

DT_V=$(dolt_scalar "dt_basic" "SELECT DOLT_VERSION();")
expect_nonempty "dolt_version_dolt_returns_nonempty" "$DT_V"
expect_shape    "dolt_version_dolt_plausible_shape" "$DT_V"

if dolt_errored "dt_one_arg" "SELECT DOLT_VERSION('x');"; then
  pass=$((pass+1)); echo "  PASS: dolt_rejects_one_arg"
else
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES dolt_rejects_one_arg"
  echo "  FAIL: dolt_rejects_one_arg"
fi

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
