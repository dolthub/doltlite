#!/bin/bash
#
# Cross-op oracle: when a single version-control op produces BOTH row conflicts
# and constraint violations, merge / cherry-pick / revert / pull must leave the
# same unfinished post-state as Dolt under an open transaction:
#   conflicts count, constraint-violation count, and the surviving row set.
#
# Autocommit rolls both engines back (covered elsewhere). This suite focuses on
# the plain BEGIN path that keeps both surfaces inspectable — the finish-path
# alignment after pull started calling merge internals directly.
#
# Dolt needs @@autocommit=0 and @@dolt_allow_commit_conflicts=1 so the failed
# op does not abort the session before the post-state query. Doltlite injects
# BEGIN immediately before the op under test (same pattern as
# vc_oracle_constraint_violations_test.sh).

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize() {
  tr -d '\r' | sed -E 's/, /,/g' | sort
}

# Post-state fingerprint: conflicts | CVs | ordered rows.
POST_QUERY_DL="SELECT 'S|' || (SELECT count(*) FROM dolt_conflicts) || '|' ||
  (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
  (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM
    (SELECT id, u, v FROM t ORDER BY id));"
POST_QUERY_DT="SELECT CONCAT('S|',
  (SELECT COUNT(*) FROM dolt_conflicts), '|',
  (SELECT COUNT(*) FROM dolt_constraint_violations), '|',
  (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t));"

# Dual-outcome seed: same cell edit (conflict on v) plus unique collision on u.
# Shared by merge and cherry-pick.
DUAL_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base'),(2,2,'base');
SELECT dolt_commit('-Am','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v='feat' WHERE id=1;
UPDATE t SET u=9 WHERE id=2;
SELECT dolt_commit('-Am','feat');
SELECT dolt_checkout('main');
UPDATE t SET v='main', u=9 WHERE id=1;
SELECT dolt_commit('-Am','main');
"

# Inject BEGIN immediately before the named VC op so the open txn preserves
# conflict/CV state for the following query (Dolt uses @@autocommit=0 instead).
oracle_tx_poststate() {
  local name="$1" setup="$2" op_pat="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_script dl_out
  dl_script=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$POST_QUERY_DL" \
              | perl -0pe "s/\nSELECT ${op_pat}\\(/\nBEGIN;\\nSELECT ${op_pat}\\(/")
  dl_out=$(printf "%s" "$dl_script" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S|' \
           | tr -d '"' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_setup"
      printf '%s\n' "$POST_QUERY_DT"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^S|' | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: cross-op conflict + constraint violations ==="
echo ""

echo "--- merge: dual conflict + unique CV under open txn ---"
oracle_tx_poststate "merge_conflict_and_cv" \
"$DUAL_SEED
SELECT dolt_merge('feat');
" "dolt_merge"

echo "--- cherry-pick: dual conflict + unique CV under open txn ---"
oracle_tx_poststate "cherry_pick_conflict_and_cv" \
"$DUAL_SEED
SELECT dolt_cherry_pick('feat');
" "dolt_cherry_pick"

echo "--- revert: dual conflict + unique CV under open txn ---"
# History: base -> main sets u=9 on id=1 -> feat-side unique on id=2 lands via
# a later commit that also edits v on id=1 so revert of the mid commit conflicts
# on v while the unique CV remains from the side-effect shape.
# Simpler revert dual path: after init, commit A sets (1,9,a); commit B sets
# (2,1,b) taking u=1; revert of A wants to restore id=1 to (1,1,base) but u=1 is
# now on id=2 → unique CV, and if B also changed id=1's v we get a conflict.
# Revert of HEAD~1 where HEAD diverged cell-wise from what A introduced:
oracle_tx_poststate "revert_conflict_and_cv" \
"
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base'),(2,2,'base');
SELECT dolt_commit('-Am','init');
UPDATE t SET u=9, v='mid' WHERE id=1;
SELECT dolt_commit('-Am','c_mid');
UPDATE t SET u=1, v='end2' WHERE id=2;
UPDATE t SET v='end1' WHERE id=1;
SELECT dolt_commit('-Am','c_end');
SELECT dolt_revert('HEAD~1');
" "dolt_revert"

echo "--- pull: divergent remote merge yields dual conflict + CV ---"
# Build a file:// remote at the dual-outcome state by pushing both sides from
# a source that holds main and feat, then clone onto main and pull feat through
# a tracking ref is awkward. Instead: push main at the dual-seed main tip, push
# feat, clone, checkout main (at main tip), then pull origin feat after renaming
# tracking — doltlite pull merges origin/branch into current when non-ff.
#
# Practical shape matching remotes suites: source pushes main (base+main side)
# and feat (base+feat side). Consumer clones (on main). Consumer is already at
# main tip; pull origin main is up-to-date. To force a merge pull on main:
# consumer rewinds? Not exposed. Alternative: consumer starts from clone of
# base-only remote, makes main-side edits, remote advances with feat-side
# merged into main on the source.
pull_dual_flow() {
  local name="$1"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir"

  local remote="file://$dir/remote.db"
  local src="$dir/src.db" con="$dir/con.db"

  # Source: base, push; main-side commit; push. feat side is applied on source
  # main as a second push after consumer has forked with the dual main side...
  # Cleaner dual:
  # 1) source: init base, push remote
  # 2) clone consumer
  # 3) source: apply feat-side changes on main, push
  # 4) consumer: apply main-side changes (conflict+unique), commit
  # 5) consumer: pull origin main → merge of source's feat-side into local
  printf '%s\n' "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base'),(2,2,'base');
SELECT dolt_commit('-Am','init');
SELECT dolt_remote('add','origin','$remote');
SELECT dolt_push('origin','main');
" | "$DOLTLITE" "$src" >/dev/null 2>"$dir/dl_seed.err"

  printf "SELECT dolt_clone('%s');\n" "$remote" \
    | "$DOLTLITE" "$con" >/dev/null 2>"$dir/dl_clone.err"

  printf '%s\n' "
UPDATE t SET v='feat' WHERE id=1;
UPDATE t SET u=9 WHERE id=2;
SELECT dolt_commit('-Am','src_feat_side');
SELECT dolt_push('origin','main');
" | "$DOLTLITE" "$src" >/dev/null 2>"$dir/dl_src_adv.err"

  local dl_out
  dl_out=$(printf '%s\n' "
UPDATE t SET v='main', u=9 WHERE id=1;
SELECT dolt_commit('-Am','con_main_side');
BEGIN;
SELECT dolt_pull('origin','main');
.headers off
.mode list
$POST_QUERY_DL
" | "$DOLTLITE" "$con" 2>"$dir/dl_pull.err" | grep '^S|' | tr -d '"' | normalize)

  # Dolt remote is a directory repo.
  local dt_remote="$dir/dt_remote" dt_src="$dir/dt_src" dt_con="$dir/dt_con"
  mkdir -p "$dt_remote" "$dt_src" "$dt_con"
  (
    cd "$dt_src" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    "$DOLT" sql -c -q "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base'),(2,2,'base');
CALL dolt_commit('-Am','init');
" >/dev/null 2>"$dir/dt_seed.err"
    "$DOLT" remote add origin "file://$dt_remote" >/dev/null 2>>"$dir/dt_seed.err"
    # Dolt file remotes are bare clones of the repo path.
    "$DOLT" push --set-upstream origin main >/dev/null 2>>"$dir/dt_seed.err" || \
      "$DOLT" push origin main >/dev/null 2>>"$dir/dt_seed.err"
  )

  (
    cd "$dt_con" || exit 1
    "$DOLT" clone "file://$dt_remote" . >/dev/null 2>"$dir/dt_clone.err" || \
      "$DOLT" clone "$dt_remote" . >/dev/null 2>>"$dir/dt_clone.err"
  )

  (
    cd "$dt_src" || exit 1
    "$DOLT" sql -c -q "
UPDATE t SET v='feat' WHERE id=1;
UPDATE t SET u=9 WHERE id=2;
CALL dolt_commit('-Am','src_feat_side');
" >/dev/null 2>"$dir/dt_src_adv.err"
    "$DOLT" push origin main >/dev/null 2>>"$dir/dt_src_adv.err"
  )

  local dt_out
  dt_out=$(
    cd "$dt_con" || exit 1
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf "UPDATE t SET v='main', u=9 WHERE id=1;\n"
      printf "CALL dolt_commit('-Am','con_main_side');\n"
      printf "CALL dolt_pull('origin','main');\n"
      printf '%s\n' "$POST_QUERY_DT"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt_pull.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^S|' | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

pull_dual_flow "pull_conflict_and_cv"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
exit 0
