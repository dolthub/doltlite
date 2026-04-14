#!/bin/bash
#
# Version-control oracle test: dolt_rebase
#
# Runs identical rebase scenarios against doltlite and Dolt. Commit
# hashes don't match across engines (prolly vs noms) so the oracle
# compares the sequence of commit messages via dolt_log, which on
# linear history gives HEAD-first first-parent ordering on both
# engines.
#
# dolt_rebase spec (from real Dolt 1.83.5):
#
#   CALL dolt_rebase(<upstream>)
#
#     Replays each commit that's reachable from HEAD but NOT from
#     <upstream> on top of <upstream>, preserving original commit
#     messages. Fails if:
#       - the upstream ref can't be resolved
#       - the set of commits to replay is empty
#       - the working tree has uncommitted changes
#       - a replayed commit would create conflicts (auto-abort in
#         default session; staged + --continue path when
#         @@dolt_allow_commit_conflicts is on — not supported in
#         the initial doltlite implementation)
#
#   CALL dolt_rebase('--abort')
#     Valid only while an interactive rebase is in progress. In the
#     non-interactive default path, errors with "no rebase in
#     progress" because non-interactive rebases are atomic.
#
# Usage: bash vc_oracle_rebase_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Compare commit-message ordered sequences after running setup SQL
# on both engines. The tag "LOG|<message>" is used to cleanly
# extract log rows from the surrounding noise CALL statements emit.
oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^LOG|')

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$query"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^LOG|')

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"
    echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"
    echo "$dt_out" | sed 's/^/      /'
  fi
}

# Error oracle: expect both engines to error on the same SQL.
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

echo "=== Version Control Oracle Tests: dolt_rebase ==="
echo ""

echo "--- linear rebase onto diverged upstream ---"

LINEAR_SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_c1');
INSERT INTO t VALUES (3, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_c2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_c2');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
"

oracle "linear_log_order" "$LINEAR_SETUP" \
  "SELECT CONCAT('LOG|', message) FROM dolt_log;"

oracle "linear_table_state" "$LINEAR_SETUP" \
  "SELECT CONCAT('LOG|', id, '=', v) FROM t ORDER BY id;"

echo "--- rebase when feat is strict descendant of main (noop-ish: feat's commits replay onto unchanged main) ---"

DESCEND_SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_only');
SELECT dolt_rebase('main');
"

oracle "descendant_log_order" "$DESCEND_SETUP" \
  "SELECT CONCAT('LOG|', message) FROM dolt_log;"

echo "--- rebase with multi-commit chain preserving order ---"

MULTI_SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
INSERT INTO t VALUES (3, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f2');
INSERT INTO t VALUES (4, 4);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (100, 100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
"

oracle "multi_commit_log" "$MULTI_SETUP" \
  "SELECT CONCAT('LOG|', message) FROM dolt_log;"

oracle "multi_commit_table" "$MULTI_SETUP" \
  "SELECT CONCAT('LOG|', id) FROM t ORDER BY id;"

echo "--- error paths ---"

oracle_error "no_divergent_commits" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_rebase('main');
"

oracle_error "behind_upstream" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
"

oracle_error "uncommitted_changes" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (1);
SELECT dolt_rebase('main');
"

oracle_error "unknown_upstream" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
SELECT dolt_rebase('nope');
"

oracle_error "abort_without_active_rebase" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_rebase('--abort');
"

oracle_error "conflict_rebase" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f');
SELECT dolt_checkout('main');
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
"

oracle_error "no_args" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_rebase();
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
