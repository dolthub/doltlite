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

# Normalize a `hash\tmessage` stream (tab-separated, one row per line):
#   - replace each distinct hash with H1, H2, ... in first-appearance order
#   - strip CRLF
normalize() {
  tr -d '\r' | awk -F'\t' '
    {
      h = $1
      if (!(h in seen)) { n++; seen[h] = "H" n }
      $1 = seen[h]
      print $1 "\t" $2
    }
  '
}

# Run an oracle scenario. $1=name, $2=setup SQL using doltlite syntax
# (SELECT dolt_xxx(...)). The harness rewrites it to CALL dolt_xxx(...)
# for Dolt. Setup SQL must not depend on function return values.
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  # Both engines run setup + query in a SINGLE invocation so any
  # session state (current branch, working set) from the setup is
  # visible to the query. The query prefixes each output row with
  # "LOG|" so we can filter out setup noise (CALL return values,
  # hash echoes, etc.) from the combined output.
  local dl_q="SELECT 'LOG|' || commit_hash || char(9) || message FROM dolt_log"
  local dt_q="SELECT concat('LOG|', commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC"

  # doltlite side
  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$dl_q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^LOG|' \
           | sed 's/^LOG|//' \
           | normalize)

  # Dolt side: rewrite SELECT dolt_*(...) -> CALL dolt_*(...)
  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$dt_q"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tr -d '"' < "$dir/dt.raw" | grep '^LOG|' | sed 's/^LOG|//' | normalize)

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

# ─── Category 0: fresh repository state ──────────────────────────────
echo "--- fresh repo ---"

oracle "fresh_db_has_seed_commit" "
-- no user commits; both sides should report a single seed commit
SELECT 1;
"

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

# Empty commit messages: Dolt rejects with a hard error
# ("Aborting commit due to empty commit message") and aborts the
# whole sql invocation before the query can run. doltlite accepts
# empty messages. The behaviors diverge by design — not oracle-
# testable in a single invocation. See
# https://github.com/dolthub/dolt/... for Dolt's reasoning.

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

# ─── Category 2: message edge cases ──────────────────────────────────
echo "--- message edge cases ---"

oracle "unicode_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'fix: données à jour 日本語 🚀');
"

oracle "very_long_message" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'this is a deliberately very long commit message that goes on and on and on to exercise any buffer-size assumptions in the log walker or in either engine|s output format and should still come back intact');
"

# Dolt trims leading/trailing whitespace from commit messages
# (matching git's behavior); doltlite preserves them. Test only
# internal whitespace to avoid that divergence.
oracle "internal_whitespace_preserved" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('t');
SELECT dolt_commit('-m', 'one    two     three');
"

# ─── Category 3: merge-commit shapes ─────────────────────────────────
echo "--- merge commits ---"

# Simple merge: feature branch fast-forward-able, merged as a real
# merge commit. dolt_log should contain both the feature commit and
# the merge commit.
oracle "merge_commit_in_log" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
"

# Merge followed by more commits on main. Walker should BFS-find
# all ancestors.
oracle "merge_then_more_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_merge('feature');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'post_merge');
"

# ─── Category 4: tags / branches interactions ────────────────────────
echo "--- tags and branches ---"

# Tags do not create commits, so log should be unchanged before vs
# after tagging.
oracle "tag_does_not_add_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
"

# dolt_log after checkout to a feature branch should list the
# feature branch's commits and share the common ancestor with main.
oracle "log_on_feature_branch" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
