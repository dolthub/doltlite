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
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

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
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      echo "$dolt_setup"
      echo "$query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
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

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_savepoint_abort_poststate() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out dolt_setup dt_out

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_out=$(printf ".headers off\n.mode list\nSELECT active_branch() || '|AB';\nSELECT count(*) || '|RB' FROM dolt_branches WHERE name='dolt_rebase_feat';\nSELECT count(*) || '|T' FROM t;\n" \
           | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
           | tr -d '\r')

  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  (
    cd "$dir/dt" || exit 1
    {
      "$DOLT" sql -r csv -q "SELECT active_branch();" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"'
      "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_branches WHERE name='dolt_rebase_feat';" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"'
      "$DOLT" sql -r csv -q "SELECT count(*) FROM t;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"'
    } > "$dir/dt.post"
  )
  dt_out=$(awk 'NR==1{print $0 "|AB"} NR==2{print $0 "|RB"} NR==3{print $0 "|T"}' "$dir/dt.post")

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

oracle_error_reopen() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_reopen"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n%s\n" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.post.err" \
           | tr -d '\r' \
           | grep '^LOG|')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  local dt_out
  (
    cd "$dir/dt" || exit 1
    printf "%s\n" "$query" | "$DOLT" sql -c -r csv 2>"$dir/dt.post.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^LOG|')

  if [ "$dl_rc" -ne 0 ] && [ "$dt_rc" -ne 0 ] && [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite:"
    echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"
    echo "$dt_out" | sed 's/^/      /'
  fi
}

oracle_reopen() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_reopen_ok"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n%s\n" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.post.err" \
           | tr -d '\r' \
           | grep '^LOG|')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  local dt_out
  (
    cd "$dir/dt" || exit 1
    printf "%s\n" "$query" | "$DOLT" sql -c -r csv 2>"$dir/dt.post.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^LOG|')

  if [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ] && [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite:"
    echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"
    echo "$dt_out" | sed 's/^/      /'
  fi
}

oracle_poststate() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_post"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n%s\n" "$dl_query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.post.err" \
           | tr -d '\r')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>"$dir/dt.post.err"
  ) > "$dir/dt.raw"
  dt_out=$(tail -n +2 "$dir/dt.raw" | tr -d '"\r')

  if [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ] && [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
    echo "    doltlite: |$dl_out|"
    echo "    dolt:     |$dt_out|"
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

echo "--- schema-edge replay ---"

oracle_poststate "rebase_disjoint_add_table_plus_check" "
CREATE TABLE base(id INTEGER PRIMARY KEY, v INT);
INSERT INTO base VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
CREATE TABLE base_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO base_new SELECT * FROM base;
DROP TABLE base;
ALTER TABLE base_new RENAME TO base;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main check');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(k INTEGER PRIMARY KEY, w TEXT);
INSERT INTO feat_tbl VALUES (1, 'x');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat table');
SELECT dolt_rebase('main');
" "SELECT (SELECT count(*) FROM sqlite_master WHERE type='table' AND name='feat_tbl') || '|' ||
          (SELECT count(*) FROM feat_tbl) || '|' ||
          (SELECT count(*) FROM base)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM information_schema.tables WHERE table_name='feat_tbl'), '|', (SELECT COUNT(*) FROM feat_tbl), '|', (SELECT COUNT(*) FROM base))"

oracle_poststate "rebase_disjoint_add_indexes_current_dolt_behavior" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main idx');
SELECT dolt_checkout('feat');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat idx');
SELECT dolt_rebase('main');
" "SELECT (SELECT count(*) FROM pragma_index_list('a') WHERE name='idx_a_v') || '|' ||
          (SELECT count(*) FROM pragma_index_list('b') WHERE name='idx_b_v')" \
  "SELECT CONCAT((SELECT COUNT(*) FROM information_schema.statistics WHERE table_name='a' AND index_name='idx_a_v'), '|', (SELECT COUNT(*) FROM information_schema.statistics WHERE table_name='b' AND index_name='idx_b_v'))"

oracle_poststate "rebase_disjoint_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main check');
SELECT dolt_checkout('feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat fk tables');
SELECT dolt_rebase('main');
" "SELECT (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT((SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_poststate "rebase_self_ref_fk_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_descendant');
SELECT dolt_rebase('main');
DELETE FROM t WHERE id = 1;
" "SELECT (SELECT count(*) FROM t) || '|' ||
          (SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id) AS ordered_rows)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', IFNULL(parent_id, -1)) ORDER BY id SEPARATOR ',') FROM t))"

oracle_poststate "rebase_fk_chain_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_rebase('main');
DELETE FROM gp WHERE id = 1;
" "SELECT (SELECT count(*) FROM gp) || '|' ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM gp), '|', (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c))"

oracle_error_reopen "rebase_fk_violation_rolls_back" "
CREATE TABLE parent(pk INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE child(pk INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES parent(u));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (1,1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
DELETE FROM parent WHERE pk=2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'main_drop_parent');
SELECT dolt_checkout('feat');
INSERT INTO child VALUES (2,2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_rebase('main');
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|CV|', count(*)) FROM dolt_constraint_violations;
SELECT CONCAT('LOG|P|', pk, ':', u) FROM parent ORDER BY pk;
SELECT CONCAT('LOG|C|', pk, ':', u) FROM child ORDER BY pk;
"

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

oracle_error "too_many_args" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main', 'extra');
"

oracle_error "unknown_flag" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_rebase('--bogus');
"

echo "--- interactive rebase ---"

# All interactive scenarios share this setup: three commits f1,f2,f3 on
# feat atop one commit m on main that diverged after init.
INTERACTIVE_SETUP="
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
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
"

# After -i, the working branch is dolt_rebase_feat and dolt_rebase has
# the default pick plan. Default pick/continue should match non-interactive.
oracle "interactive_default_plan" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', message) FROM dolt_log;"

oracle "interactive_default_table" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', id) FROM t ORDER BY id;"

# Drop the middle commit f2 from the plan.
oracle "interactive_drop_one" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'drop' WHERE commit_message = 'f2';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', message) FROM dolt_log;"

oracle "interactive_drop_table" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'drop' WHERE commit_message = 'f2';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', id) FROM t ORDER BY id;"

# Reorder: move f3 before f1 using a fractional rebase_order.
oracle "interactive_reorder" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET rebase_order = 0.5 WHERE commit_message = 'f3';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', message) FROM dolt_log;"

# Reword f1; message in the plan overrides the original commit message.
oracle "interactive_reword" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'reword', commit_message = 'f1 renamed' WHERE commit_message = 'f1';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', message) FROM dolt_log;"

# Squash f2 into f1: expect a single combined commit with message 'f1 | f2'
# (we replace newlines with ' | ' in the oracle so shell/csv escaping
# doesn't diverge between engines on the literal newline characters).
oracle "interactive_squash" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'squash' WHERE commit_message = 'f2';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', REPLACE(message, CHAR(10), ' | ')) FROM dolt_log;"

# Fixup f3 into f2: combined commit keeps only f2's message.
oracle "interactive_fixup" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET action = 'fixup' WHERE commit_message = 'f3';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', message) FROM dolt_log;"

# Longer plan with mixed actions to stress replay ordering and state
# transitions. We intentionally combine reorder, drop, squash, reword,
# and fixup in one plan.
LONG_INTERACTIVE_SETUP="
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
INSERT INTO t VALUES (5, 5);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f4');
INSERT INTO t VALUES (6, 6);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f5');
INSERT INTO t VALUES (7, 7);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f6');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (100, 100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm');
SELECT dolt_checkout('feat');
"

oracle "interactive_long_mixed_plan_log" "
$LONG_INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET rebase_order = 0.5 WHERE commit_message = 'f6';
UPDATE dolt_rebase SET action = 'drop' WHERE commit_message = 'f2';
UPDATE dolt_rebase SET action = 'squash' WHERE commit_message = 'f4';
UPDATE dolt_rebase SET action = 'reword', commit_message = 'f5 renamed' WHERE commit_message = 'f5';
UPDATE dolt_rebase SET action = 'fixup' WHERE commit_message = 'f3';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', REPLACE(message, CHAR(10), ' | ')) FROM dolt_log;"

oracle "interactive_long_mixed_plan_table" "
$LONG_INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
UPDATE dolt_rebase SET rebase_order = 0.5 WHERE commit_message = 'f6';
UPDATE dolt_rebase SET action = 'drop' WHERE commit_message = 'f2';
UPDATE dolt_rebase SET action = 'squash' WHERE commit_message = 'f4';
UPDATE dolt_rebase SET action = 'reword', commit_message = 'f5 renamed' WHERE commit_message = 'f5';
UPDATE dolt_rebase SET action = 'fixup' WHERE commit_message = 'f3';
SELECT dolt_rebase('--continue');
" "SELECT CONCAT('LOG|', id) FROM t ORDER BY id;"

# --abort leaves the original branch untouched and restores us to it.
oracle "interactive_abort_log" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--abort');
" "SELECT CONCAT('LOG|', message) FROM dolt_log;"

oracle "interactive_abort_branch" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--abort');
" "SELECT CONCAT('LOG|', active_branch());"

oracle_savepoint_abort_poststate "interactive_abort_savepoint_poststate" "
$INTERACTIVE_SETUP
SAVEPOINT sp1;
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--abort');
ROLLBACK TO sp1;
"

oracle_reopen "interactive_abort_explicit_txn_reopen" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main');
BEGIN;
SELECT dolt_rebase('--abort');
COMMIT;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|T|', count(*)) FROM t;
SELECT CONCAT('LOG|L|', count(*)-1) FROM dolt_log;
"

oracle_reopen "interactive_abort_after_resolve_explicit_txn_reopen" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET v = 2 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
UPDATE t SET v = 3 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i', 'main');
SELECT dolt_conflicts_resolve('--theirs', 't');
BEGIN;
SELECT dolt_rebase('--abort');
COMMIT;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|C|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('LOG|V|', v) FROM t WHERE id = 1;
SELECT CONCAT('LOG|L|', count(*)-1) FROM dolt_log;
"

# --continue is an error when not in an interactive rebase.
oracle_error "continue_without_active" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_rebase('--continue');
"

oracle_error "interactive_too_many_args" "
$INTERACTIVE_SETUP
SELECT dolt_rebase('-i', 'main', 'extra');
"

oracle_error_reopen "interactive_continue_conflict_abort_poststate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET v = 2 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
UPDATE t SET v = 3 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--continue');
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|C|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('LOG|V|', v) FROM t WHERE id = 1;
"

oracle_error_reopen "interactive_continue_constraint_abort_poststate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v INT);
INSERT INTO t VALUES (1, 1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 2, 3);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--continue');
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|CV|', count(*)) FROM dolt_constraint_violations;
SELECT CONCAT('LOG|T|', count(*)) FROM t;
"

oracle_error_reopen "interactive_resolve_theirs_top_savepoint_poststate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
UPDATE t SET v = 2 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
UPDATE t SET v = 3 WHERE id = 1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i', 'main');
SAVEPOINT sp1;
SELECT dolt_conflicts_resolve('--theirs', 't');
ROLLBACK TO sp1;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|C|', count(*)) FROM dolt_conflicts;
SELECT CONCAT('LOG|V|', v) FROM t WHERE id = 1;
"

oracle_reopen "interactive_continue_nested_savepoint_success_reopen" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('-i', 'main');
BEGIN;
SAVEPOINT sp1;
SELECT dolt_rebase('--continue');
COMMIT;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|T|', count(*)) FROM t;
SELECT CONCAT('LOG|L|', count(*)-1) FROM dolt_log;
"

oracle_error_reopen "interactive_continue_top_savepoint_success_reopen" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
SAVEPOINT sp1;
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--continue');
ROLLBACK TO sp1;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|T|', count(*)) FROM t;
SELECT CONCAT('LOG|L|', count(*)-1) FROM dolt_log;
"

oracle_error_reopen "interactive_continue_preexisting_nested_savepoint_success_reopen" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
BEGIN;
SAVEPOINT sp1;
SELECT dolt_rebase('-i', 'main');
SELECT dolt_rebase('--continue');
ROLLBACK TO sp1;
COMMIT;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|T|', count(*)) FROM t;
SELECT CONCAT('LOG|L|', count(*)-1) FROM dolt_log;
"

oracle_error_reopen "interactive_start_preexisting_nested_savepoint_poststate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'f1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, 10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'm1');
SELECT dolt_checkout('feat');
BEGIN;
SAVEPOINT sp1;
SELECT dolt_rebase('-i', 'main');
ROLLBACK TO sp1;
COMMIT;
" "
SELECT CONCAT('LOG|B|', active_branch());
SELECT CONCAT('LOG|W|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|P|', count(*)) FROM dolt_branches WHERE name='dolt_rebase_feat';
SELECT CONCAT('LOG|T|', count(*)) FROM t;
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
