#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

oracle_error() {
  local name="$1" setup="$2" dl_query="$3" dt_query="$4"
  local dir="$TMPROOT/$name"
  local dl_rc dt_rc dt_setup
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" \
    "$dir/dl.err" "$(printf '%s\n%s\n' "$setup" "$dl_query")"
  dl_rc=$?

  dt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" \
    "$dir/dt.err" "$(printf '%s\n%s\n' "$dt_setup" "$dt_query")"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (doltlite rc=$dl_rc, dolt rc=$dt_rc)"
  fi
}

BASE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES(2,'feature');
SELECT dolt_commit('-A','-m','feature');
SELECT dolt_checkout('main');
"

DIRTY="$BASE
INSERT INTO t VALUES(3,'dirty');
"

STAGED="$DIRTY
SELECT dolt_add('t');
"

echo "=== Version Control Oracle Tests: NULL arguments ==="

oracle_error "at_null_ref" "$BASE" \
  "SELECT count(*) FROM dolt_at_t(NULL);" \
  "SELECT count(*) FROM t AS OF NULL;"

oracle_error "history_null_ref" "$BASE" \
  "SELECT count(*) FROM dolt_history_t(NULL);" \
  "SELECT count(*) FROM dolt_history_t AS OF NULL;"

oracle_error "schema_diff_null_table" "$BASE" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD',NULL);" \
  "SELECT count(*) FROM dolt_schema_diff('HEAD~1','HEAD',NULL);"

oracle_error "revert_null_ref" "$BASE" \
  "SELECT dolt_revert(NULL);" \
  "CALL dolt_revert(NULL);"

oracle_error "reset_null_ref" "$STAGED" \
  "SELECT dolt_reset(NULL);" \
  "CALL dolt_reset(NULL);"

oracle_error "reset_hard_null_and_ref" "$STAGED" \
  "SELECT dolt_reset(NULL,'--hard','HEAD');" \
  "CALL dolt_reset(NULL,'--hard','HEAD');"

oracle_error "add_drops_null_arg" "$DIRTY" \
  "SELECT dolt_add(NULL,'t');" \
  "CALL dolt_add(NULL,'t');"

oracle_error "branch_drops_null_arg" "$BASE" \
  "SELECT dolt_branch(NULL,'newbranch');" \
  "CALL dolt_branch(NULL,'newbranch');"

oracle_error "checkout_drops_null_arg" "$BASE" \
  "SELECT dolt_checkout(NULL,'main');" \
  "CALL dolt_checkout(NULL,'main');"

oracle_error "commit_drops_null_arg" "$DIRTY" \
  "SELECT dolt_commit(NULL,'-A','-m','commit');" \
  "CALL dolt_commit(NULL,'-A','-m','commit');"

oracle_error "tag_drops_null_arg" "$BASE" \
  "SELECT dolt_tag(NULL,'newtag');" \
  "CALL dolt_tag(NULL,'newtag');"

oracle_error "merge_drops_null_arg" "$BASE" \
  "SELECT dolt_merge(NULL,'feature');" \
  "CALL dolt_merge(NULL,'feature');"

oracle_error "cherry_pick_drops_null_arg" "$BASE" \
  "SELECT dolt_cherry_pick(NULL,dolt_hashof('feature'));" \
  "CALL dolt_cherry_pick(NULL,dolt_hashof('feature'));"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ "$fail" -ne 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
