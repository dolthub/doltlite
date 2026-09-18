#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib/vc_oracle_common.sh"
DOLTLITE=$(vc_oracle_resolve_binary "${1:-build/doltlite}")
DOLT=$(vc_oracle_resolve_binary "${2:-dolt}")
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0
fail=0
FAILED_NAMES=""
mkdir -p "$TMPROOT/dt" "$TMPROOT/dt_remote"
(cd "$TMPROOT/dt" && vc_oracle_init_repo)

run_dl() { "$DOLTLITE" -bail "$TMPROOT/dl.db" "$1"; }
run_dt() { (cd "$TMPROOT/dt" && "$DOLT" sql -r csv -q "$1"); }

setup="CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('feature');
SELECT dolt_branch('other');"
run_dl "$setup SELECT dolt_remote('add','origin','file://$TMPROOT/remote.db');
SELECT dolt_push('origin','main'); SELECT dolt_push('origin','feature');
SELECT dolt_push('origin','other');" >/dev/null
run_dt "$(vc_oracle_translate_for_dolt "$setup")
CALL dolt_remote('add','origin','file://$TMPROOT/dt_remote');
CALL dolt_push('origin','main'); CALL dolt_push('origin','feature');
CALL dolt_push('origin','other');" >/dev/null

compare_state() {
  local name="$1" dl dt
  dl=$(run_dl "SELECT 'local|'||name FROM dolt_branches
UNION ALL SELECT 'remote|'||name FROM dolt_remote_branches ORDER BY 1;")
  dt=$(run_dt "SELECT CONCAT('local|',name) AS n FROM dolt_branches
UNION ALL SELECT CONCAT('remote|',name) FROM dolt_remote_branches ORDER BY 1;" | tail -n +2 | tr -d '\r"')
  vc_oracle_assert_match "$name" "$dl" "$dt"
}

compare_state before_delete
run_dl "SELECT dolt_push('origin',':feature');" >/dev/null
run_dt "CALL dolt_push('origin',':feature');" >/dev/null
compare_state delete_removes_only_target_tracking
run_dl "SELECT dolt_fetch('origin');" >/dev/null
run_dt "CALL dolt_fetch('origin');" >/dev/null
compare_state fetch_does_not_resurrect_deleted_branch
dl_rc=0; dt_rc=0
run_dl "SELECT dolt_push('origin',':feature');" >"$TMPROOT/dl.err" 2>&1 || dl_rc=$?
run_dt "CALL dolt_push('origin',':feature');" >"$TMPROOT/dt.err" 2>&1 || dt_rc=$?
if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
  pass=$((pass+1))
else
  echo "FAIL: repeated delete must report missing tracking ref ($dl_rc/$dt_rc)"
  fail=$((fail+1))
fi
compare_state repeated_delete_preserves_other_refs
run_dl "SELECT dolt_push('origin','feature'); SELECT dolt_fetch('origin');" >/dev/null
run_dt "CALL dolt_push('origin','feature'); CALL dolt_fetch('origin');" >/dev/null
compare_state recreate_deleted_branch

"$DOLTLITE" -bail "$TMPROOT/peer.db" "
SELECT dolt_clone('file://$TMPROOT/remote.db');
SELECT dolt_push('origin',':feature');" >/dev/null
(cd "$TMPROOT" && "$DOLT" clone "file://$TMPROOT/dt_remote" peer >/dev/null)
(cd "$TMPROOT/peer" && "$DOLT" sql -q "CALL dolt_push('origin',':feature');" >/dev/null)
compare_state peer_delete_leaves_stale_tracking
run_dl "SELECT dolt_push('origin',':feature');" >/dev/null
run_dt "CALL dolt_push('origin',':feature');" >/dev/null
compare_state delete_cleans_stale_tracking

vc_oracle_finish
