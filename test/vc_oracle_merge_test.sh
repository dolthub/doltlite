#!/bin/bash
#
# Version-control oracle test: dolt_merge
#
# Runs identical merge scenarios against doltlite and Dolt and compares
# the resulting dolt_log post-state. Covers fast-forward, three-way no
# conflict, --no-ff (force a merge commit even when fast-forward would
# work), -m / --message overrides, --abort, and a few error paths.
#
# IMPORTANT: every scenario uses INTEGER PRIMARY KEY (the sqlite rowid
# alias) for its merged tables. doltlite stores rows keyed by sqlite's
# auto-rowid, which is allocated independently per branch. With a non-
# alias PK like `id INT PRIMARY KEY`, two branches that each insert one
# new row both land on rowid=2 and the row-level merge sees a spurious
# MM conflict on rowid 2 even though the user's PK values are distinct.
# Dolt keys by user PK directly, so the same scenario merges cleanly
# there. Until doltlite's storage layer supports user-PK-keyed rows,
# the oracle scenarios stick to INTEGER PRIMARY KEY where the user's
# value IS the rowid and the keys never collide. The conflict scenario
# below also uses INTEGER PRIMARY KEY and exercises a true MM conflict
# (UPDATE on the same row from both sides) which doltlite handles
# correctly.
#
# Conflict scenarios are checked via oracle_error_or_state because
# doltlite enters a merge state on conflict while Dolt rolls back the
# transaction under autocommit. The two engines diverge in HOW the
# conflict is surfaced, but both refuse to produce a clean merge
# commit, which is the property the oracle gates on.
#
# Usage: bash vc_oracle_merge_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  # 1. Strip CRs.
  # 2. Drop non-tab lines (function-result text like "Already up to date"
  #    leaks through stdout in list mode).
  # 3. Sort by message (column 2) so the H1/H2/... assignment in step 4 is
  #    deterministic across both engines regardless of native log walk
  #    order. After a merge, the two engines disagree on which parent
  #    appears first in dolt_log; sorting by message canonicalizes.
  # 4. Replace each distinct hash with H1, H2, ... in first-appearance
  #    order on the sorted output.
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 { print }' \
    | sort -t$'\t' -k2 \
    | awk -F'\t' '
        {
          h = $1
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print seen[h] "\t" $2
        }
      '
}

# Compare post-state via dolt_log: (commit_hash normalized, message),
# in order from newest to oldest. Same shape as the log oracle.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT commit_hash || char(9) || message FROM dolt_log'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"' | normalize)

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

# A merge that should not produce a new merge commit (because both
# engines diverge on how they surface conflict). Compares only the
# property that BOTH engines refuse to advance the branch tip to a
# clean merge commit — checked by counting commits on the current
# branch and asserting both report the same pre-merge count.
oracle_no_merge_commit() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_nm"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_count
  dl_count=$(printf "%s\n.headers off\n.mode list\nSELECT count(*) FROM dolt_log;\n" "$setup" \
             | "$DOLTLITE" "$dir/dl/db" 2>/dev/null \
             | grep -E '^[0-9]+$' | tail -1)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_count
  dt_count=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>&1
    "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_log;" 2>/dev/null \
      | tail -n +2
  )

  if [ "$dl_count" = "$dt_count" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite log count: $dl_count"
    echo "    dolt log count:     $dt_count"
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_err=0
  echo "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then dl_err=1; fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_err=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
  )
  if grep -qiE 'error|fail' "$dir/dt.out" "$dir/dt.err" 2>/dev/null; then dt_err=1; fi

  if [ "$dl_err" = 1 ] && [ "$dt_err" = 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite errored: $([ $dl_err = 1 ] && echo yes || echo NO)"
    echo "    dolt errored:     $([ $dt_err = 1 ] && echo yes || echo NO)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_merge ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
"

echo "--- fast forward ---"

oracle "fast_forward_clean" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- three-way no conflict ---"

oracle "three_way_independent_inserts" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

oracle "three_way_disjoint_columns_modified" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT);
INSERT INTO t VALUES (1, 0, 0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET a = 10 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_a');
SELECT dolt_checkout('feature');
UPDATE t SET b = 20 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_b');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- --no-ff ---"

oracle "no_ff_creates_merge_commit" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '--no-ff');
"

echo "--- custom message ---"

oracle "merge_with_custom_message" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '-m', 'release sync');
"

oracle "merge_with_message_long_flag" "
$SEED
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (20, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature', '--message', 'release sync');
"

echo "--- conflict (no merge commit) ---"

oracle_no_merge_commit "modify_modify_conflict_blocks_merge" "
$SEED
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
"

echo "--- already up to date ---"

oracle "merge_same_branch_no_op" "
$SEED
SELECT dolt_merge('main');
"

echo "--- error paths ---"

oracle_error "merge_nonexistent_branch" "
$SEED
SELECT dolt_merge('nope');
"

oracle_error "merge_unknown_flag" "
$SEED
SELECT dolt_merge('feature', '--bogus');
"

oracle_error "merge_no_args" "
SELECT dolt_merge();
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
