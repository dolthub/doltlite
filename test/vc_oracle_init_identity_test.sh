#!/bin/bash

set -u
set -o pipefail
DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

mkdir -p "$TMPROOT/dt-a" "$TMPROOT/dt-b" "$TMPROOT/remote"
for side in a b; do
  id=1
  [ "$side" = b ] && id=2
  setup="CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);
INSERT INTO t VALUES($id,'$side-only');
SELECT dolt_commit('-Am','$side');"
  printf '%s\n' "$setup" | vc_oracle_run_doltlite "$TMPROOT/$side.db" >"$TMPROOT/dl-$side.out" 2>"$TMPROOT/dl-$side.err"
  (
    cd "$TMPROOT/dt-$side" || exit 1
    vc_oracle_init_repo || exit 1
    vc_oracle_translate_for_dolt "$setup" | "$DOLT" sql -c
  ) >"$TMPROOT/dt-$side.out" 2>"$TMPROOT/dt-$side.err"
done

printf "SELECT dolt_remote('add','other','file://%s/b.db'); SELECT dolt_fetch('other');\n" "$TMPROOT" \
  | vc_oracle_run_doltlite "$TMPROOT/a.db" >"$TMPROOT/dl-fetch.out" 2>"$TMPROOT/dl-fetch.err"
(
  cd "$TMPROOT/dt-b" || exit 1
  "$DOLT" remote add target "file://$TMPROOT/remote" || exit 1
  "$DOLT" push target main
) >"$TMPROOT/dt-push.out" 2>"$TMPROOT/dt-push.err"
(
  cd "$TMPROOT/dt-a" || exit 1
  "$DOLT" remote add other "file://$TMPROOT/remote" || exit 1
  "$DOLT" fetch other
) >"$TMPROOT/dt-fetch.out" 2>"$TMPROOT/dt-fetch.err"

printf "SELECT dolt_merge('other/main');\n" \
  | vc_oracle_run_doltlite --error "$TMPROOT/a.db" >"$TMPROOT/dl-merge.out" 2>"$TMPROOT/dl-merge.err"
dl_rc=$?
(cd "$TMPROOT/dt-a" && "$DOLT" sql -q "CALL dolt_merge('other/main');") >"$TMPROOT/dt-merge.out" 2>"$TMPROOT/dt-merge.err"
dt_rc=$?
dl_result=unexpected
dt_result=unexpected
if vc_oracle_is_clean_error "$dl_rc" && grep -qi 'no common ancestor' "$TMPROOT/dl-merge.err"; then dl_result=refused; fi
if vc_oracle_is_clean_error "$dt_rc" && grep -qi 'no common ancestor' "$TMPROOT/dt-merge.err"; then dt_result=refused; fi
vc_oracle_assert_match "reference_refuses_unrelated_repository" "$dt_result" refused
vc_oracle_assert_match "unrelated_repository_has_no_common_ancestor" "$dl_result" "$dt_result"

query="SELECT concat('R|',id,'|',v) FROM t;
SELECT concat('R|log|',count(*)) FROM dolt_log;
SELECT concat('R|status|',count(*)) FROM dolt_status;"
dl_out=$(printf '%s\n' "$query" | vc_oracle_run_doltlite "$TMPROOT/a.db" 2>"$TMPROOT/dl-state.err" | tr -d '\r' | grep '^R|' | sort)
dt_out=$(cd "$TMPROOT/dt-a" && "$DOLT" sql -r csv -q "$query" 2>"$TMPROOT/dt-state.err" | tr -d '"\r' | grep '^R|' | sort)
vc_oracle_assert_match "unrelated_merge_preserves_rows_and_history" "$dl_out" "$dt_out"
vc_oracle_finish
