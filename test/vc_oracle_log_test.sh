#!/bin/bash
#
# Version-control oracle test: dolt_log
#
# Runs identical commit scenarios against doltlite and Dolt, then compares
# the normalized `dolt_log` output. Catches divergence in commit ordering,
# message handling, and commit-graph shape.
#
# Usage: bash vc_oracle_log_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Normalize a `commit_hash,message` CSV stream:
#   - drop Dolt's seed "Initialize data repository" row
#   - replace each distinct hash with H1, H2, ... in first-appearance order
#   - trim trailing whitespace
normalize() {
  tr -d '\r' | awk -F, '
    $2 == "Initialize data repository" { next }
    {
      h = $1
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $1 = seen[h]
      print $1 "," $2
    }
  ' OFS=,
}

# Run an oracle scenario. $1=name, $2=setup SQL using doltlite syntax
# (SELECT dolt_xxx(...)). The harness rewrites it to CALL dolt_xxx(...)
# for Dolt. Setup SQL must not depend on function return values.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # doltlite side
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode csv\nSELECT commit_hash, message FROM dolt_log;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tail -n +1 \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  # Dolt side: rewrite SELECT dolt_*(...) -> CALL dolt_*(...)
  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT commit_hash, message FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  fi
}

echo "=== Version Control Oracle Tests: dolt_log ==="
echo ""

# ─── Category 1: linear commit chains ────────────────────────────────
echo "--- linear chains ---"

oracle "single_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'first');
"

oracle "three_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'c2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'c3');
"

oracle "commit_all_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'seed');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-a', '-m', 'second');
"

oracle "empty_message_rejected_or_accepted" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', '');
"

oracle "message_with_special_chars" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'fix: handle x<y & z>w, OK?');
"

oracle "amend_like_via_reset" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'one');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'two');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'three');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
