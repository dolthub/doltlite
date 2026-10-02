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

oracle_replay() {
  local name="$1" setup="$2" query="$3" dt_query="$4" expected="$5"
  local dir="$TMPROOT/$name" dl_rc dt_rc dl_out dt_out
  mkdir -p "$dir/dl" "$dir/dt"
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" \
    "$setup" "--$expected"
  dl_rc=$?
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$(vc_oracle_translate_for_dolt "$setup")"
  dt_rc=$?
  if { [ "$expected" = success ] && [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ]; } \
     || { [ "$expected" = expect-error ] \
          && vc_oracle_is_clean_error "$dl_rc" \
          && vc_oracle_is_clean_error "$dt_rc"; }; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES ${name}_execution"
    echo "  FAIL: $name (expected $expected; doltlite=$dl_rc dolt=$dt_rc)"
    sed 's/^/    /' "$dir/dl.err" "$dir/dt.err"
  fi
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$query" \
    | vc_oracle_run_doltlite "$dir/dl/db" 2>"$dir/dl.post.err" | grep '^Q|')
  (
    cd "$dir/dt" || exit 1
    printf '%s\n' "$dt_query" | "$DOLT" sql -r csv \
      >"$dir/dt.post.out" 2>"$dir/dt.post.err"
  )
  dt_rc=$?
  if [ "$dt_rc" -ne 0 ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES ${name}_poststate"
    echo "  FAIL: $name (dolt poststate query failed; rc=$dt_rc)"
    sed 's/^/    /' "$dir/dt.post.err"
  fi
  dt_out=$(tr -d '"\r' < "$dir/dt.post.out" | grep '^Q|')
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

for position in middle trailing; do
  columns='a INT, c INT'
  [ "$position" = middle ] && columns='c INT, a INT'
  changes='edited-only edited-kept null-kept kept-only'
  [ "$position" = middle ] && changes="$changes null-only"
  for operation in cherry_pick revert rebase; do
    for change in $changes; do
      setup="CREATE TABLE t(id INT PRIMARY KEY, $columns);
INSERT INTO t(id,a,c) VALUES(1,1,1),(2,2,2);
SELECT dolt_commit('-Am','base');"
      if [ "$operation" != revert ]; then
        setup="$setup
SELECT dolt_checkout('-b','feat');"
      fi
      if [ "$operation" = rebase ]; then
        setup="$setup
UPDATE t SET a=20 WHERE id=2;
SELECT dolt_commit('-am','early clean edit');"
      fi
      edit='c=30'
      case "$change" in
        edited-kept) edit='c=30,a=10' ;;
        null-only) edit='c=NULL' ;;
        null-kept) edit='c=NULL,a=10' ;;
        kept-only) edit='a=10' ;;
      esac
      if [ "$operation" = revert ] && [[ "$change" = null-* ]]; then
        setup="$setup
UPDATE t SET c=NULL WHERE id=1;
SELECT dolt_commit('-am','null base');"
        edit='c=30'
        [ "$change" = null-kept ] && edit='c=30,a=10'
      fi
      setup="$setup
UPDATE t SET $edit WHERE id=1;
SELECT dolt_commit('-am','edit');"
      if [ "$operation" != revert ]; then
        setup="$setup
SELECT dolt_checkout('main');"
      fi
      setup="$setup
ALTER TABLE t DROP COLUMN c;
SELECT dolt_commit('-am','drop');"
      query=''
      case "$operation" in
        cherry_pick) apply="SELECT dolt_cherry_pick('feat');" ;;
        revert) apply="SELECT dolt_revert('HEAD~1');" ;;
        rebase)
          setup="$setup
SELECT dolt_checkout('feat');"
          apply="SELECT dolt_rebase('main');"
          query="SELECT dolt_checkout('feat');"
          ;;
      esac
      expected=expect-error
      [ "$change" = kept-only ] && expected=success
      query="$query
SELECT CONCAT('Q|row|',id,'|',a) FROM t ORDER BY id;
SELECT CONCAT('Q|branch|',name) FROM dolt_branches ORDER BY name;
SELECT CONCAT('Q|status|',COUNT(*)) FROM dolt_status;"
      if [ "$operation" = rebase ] && [ "$expected" = expect-error ]; then
        query="$query
SELECT CONCAT('Q|cell|',id,'|',COALESCE(c,-1)) FROM t ORDER BY id;"
      fi
      dt_query="$(vc_oracle_translate_for_dolt "$query")
SELECT CONCAT('Q|column|',column_name) FROM information_schema.columns
 WHERE table_schema=DATABASE() AND table_name='t' ORDER BY ordinal_position;"
      query="$query
SELECT CONCAT('Q|column|',name) FROM pragma_table_info('t') ORDER BY cid;"
      oracle_replay "$position-$operation-$change" "$setup
$apply" "$query" "$dt_query" "$expected"
    done
  done
done

vc_oracle_finish
