#!/bin/bash
# Dual conflict+CV post-state vs Dolt under an open txn (autocommit rolls back).
# Dolt: @@autocommit=0 and @@dolt_allow_commit_conflicts=1. Doltlite: inject BEGIN.

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

POST_QUERY_DL="SELECT 'S|' || (SELECT count(*) FROM dolt_conflicts) || '|' ||
  (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
  (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM
    (SELECT id, u, v FROM t ORDER BY id));"
POST_QUERY_DT="SELECT CONCAT('S|',
  (SELECT COUNT(*) FROM dolt_conflicts), '|',
  (SELECT COUNT(*) FROM dolt_constraint_violations), '|',
  (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t));"

# Same-cell edit (conflict on v) plus unique collision on u.
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

# BEGIN just before the VC op so the open txn preserves conflict/CV state.
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
# Revert of A: restore id=1 to (1,1,base) but u=1 is now on id=2 (unique CV);
# B also changed id=1's v (conflict).
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
# Clone of base-only remote, local main-side edits, remote advances with feat-side.
pull_dual_flow() {
  local name="$1"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir"

  local remote="file://$dir/remote.db"
  local src="$dir/src.db" con="$dir/con.db"

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
