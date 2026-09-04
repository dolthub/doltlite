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

record_failure() {
  local name="$1"
  fail=$((fail+1))
  FAILED_NAMES="$FAILED_NAMES $name"
  echo "  FAIL: $name"
}

remote_tag_flow() {
  local name="$1" seed="$2" advance="$3" consume="$4"
  local dl_query="$5" dt_query="$6"
  local dir="$TMPROOT/$name"
  local dl_remote="file://$dir/remote.db"
  local dl_src="$dir/src.db" dl_con="$dir/con.db"
  local dl_seed_rc dl_clone_rc dl_advance_rc dl_consume_rc dl_query_rc
  local dt_seed_rc dt_clone_rc dt_advance_rc dt_consume_rc dt_query_rc
  local dl_out dt_out dt_seed dt_advance dt_consume dt_q
  mkdir -p "$dir"

  printf '%s\n' "${seed//@REMOTE@/$dl_remote}" \
    | "$DOLTLITE" "$dl_src" >"$dir/dl_seed.out" 2>"$dir/dl_seed.err"
  dl_seed_rc=$?
  printf "SELECT dolt_clone('%s');\n" "$dl_remote" \
    | "$DOLTLITE" "$dl_con" >"$dir/dl_clone.out" 2>"$dir/dl_clone.err"
  dl_clone_rc=$?
  printf '%s\n' "${advance//@REMOTE@/$dl_remote}" \
    | "$DOLTLITE" "$dl_src" >"$dir/dl_advance.out" 2>"$dir/dl_advance.err"
  dl_advance_rc=$?
  printf '%s\n' "$consume" \
    | "$DOLTLITE" "$dl_con" >"$dir/dl_consume.out" 2>"$dir/dl_consume.err"
  dl_consume_rc=$?
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_query" \
    | "$DOLTLITE" "$dl_con" 2>"$dir/dl_query.err" \
    | tr -d '\r' | grep '^R|' | sort)
  dl_query_rc=$?

  dt_seed=$(vc_oracle_translate_for_dolt "${seed//@REMOTE@/file://$dir/dt_remote}")
  dt_advance=$(vc_oracle_translate_for_dolt "${advance//@REMOTE@/file://$dir/dt_remote}")
  dt_consume=$(vc_oracle_translate_for_dolt "$consume")
  dt_q=$(vc_oracle_translate_for_dolt "$dt_query")
  mkdir -p "$dir/dsrc" "$dir/dt_remote"
  (cd "$dir/dsrc" && vc_oracle_init_repo)
  (cd "$dir/dsrc" && printf '%s\n' "$dt_seed" \
    | "$DOLT" sql -c >"$dir/dt_seed.out" 2>"$dir/dt_seed.err")
  dt_seed_rc=$?
  (cd "$dir" && "$DOLT" clone "file://$dir/dt_remote" dcon \
    >"$dir/dt_clone.out" 2>"$dir/dt_clone.err")
  dt_clone_rc=$?
  (cd "$dir/dsrc" && printf '%s\n' "$dt_advance" \
    | "$DOLT" sql -c >"$dir/dt_advance.out" 2>"$dir/dt_advance.err")
  dt_advance_rc=$?
  (cd "$dir/dcon" && printf '%s\n' "$dt_consume" \
    | "$DOLT" sql -c >"$dir/dt_consume.out" 2>"$dir/dt_consume.err")
  dt_consume_rc=$?
  (cd "$dir/dcon" && printf '%s\n' "$dt_q" \
    | "$DOLT" sql -r csv 2>"$dir/dt_query.err") >"$dir/dt.raw"
  dt_query_rc=$?
  dt_out=$(tr -d '"\r' <"$dir/dt.raw" | grep '^R|' | sort)

  if [ "$dl_seed_rc" -ne 0 ] || [ "$dl_clone_rc" -ne 0 ] \
     || [ "$dl_advance_rc" -ne 0 ] || [ "$dl_consume_rc" -ne 0 ] \
     || [ "$dl_query_rc" -ne 0 ] || [ "$dt_seed_rc" -ne 0 ] \
     || [ "$dt_clone_rc" -ne 0 ] || [ "$dt_advance_rc" -ne 0 ] \
     || [ "$dt_consume_rc" -ne 0 ] || [ "$dt_query_rc" -ne 0 ]; then
    record_failure "$name"
    echo "    doltlite rc: $dl_seed_rc/$dl_clone_rc/$dl_advance_rc/$dl_consume_rc/$dl_query_rc"
    echo "    dolt rc:     $dt_seed_rc/$dt_clone_rc/$dt_advance_rc/$dt_consume_rc/$dt_query_rc"
    return
  fi
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

remote_tag_error_flow() {
  local name="$1" side="$2" seed="$3" action="$4" after="$5"
  local dl_query="$6" dt_query="$7"
  local dir="$TMPROOT/$name"
  local dl_remote="file://$dir/remote.db"
  local dl_src="$dir/src.db" dl_con="$dir/con.db" dl_action_db
  local dl_seed_rc dl_clone_rc dl_action_rc dl_after_rc dl_query_rc
  local dt_seed_rc dt_clone_rc dt_action_rc dt_after_rc dt_query_rc
  local dl_out dt_out dt_seed dt_action dt_after dt_q dt_action_dir
  mkdir -p "$dir"

  printf '%s\n' "${seed//@REMOTE@/$dl_remote}" \
    | "$DOLTLITE" "$dl_src" >"$dir/dl_seed.out" 2>"$dir/dl_seed.err"
  dl_seed_rc=$?
  printf "SELECT dolt_clone('%s');\n" "$dl_remote" \
    | "$DOLTLITE" "$dl_con" >"$dir/dl_clone.out" 2>"$dir/dl_clone.err"
  dl_clone_rc=$?
  if [ "$side" = "src" ]; then dl_action_db="$dl_src"; else dl_action_db="$dl_con"; fi
  printf '%s\n' "$action" | "$DOLTLITE" "$dl_action_db" \
    >"$dir/dl_action.out" 2>"$dir/dl_action.err"
  dl_action_rc=$?
  printf '%s\n' "$after" | "$DOLTLITE" "$dl_con" \
    >"$dir/dl_after.out" 2>"$dir/dl_after.err"
  dl_after_rc=$?
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_query" \
    | "$DOLTLITE" "$dl_con" 2>"$dir/dl_query.err" \
    | tr -d '\r' | grep '^R|' | sort)
  dl_query_rc=$?

  dt_seed=$(vc_oracle_translate_for_dolt "${seed//@REMOTE@/file://$dir/dt_remote}")
  dt_action=$(vc_oracle_translate_for_dolt "$action")
  dt_after=$(vc_oracle_translate_for_dolt "$after")
  dt_q=$(vc_oracle_translate_for_dolt "$dt_query")
  mkdir -p "$dir/dsrc" "$dir/dt_remote"
  (cd "$dir/dsrc" && vc_oracle_init_repo)
  (cd "$dir/dsrc" && printf '%s\n' "$dt_seed" \
    | "$DOLT" sql -c >"$dir/dt_seed.out" 2>"$dir/dt_seed.err")
  dt_seed_rc=$?
  (cd "$dir" && "$DOLT" clone "file://$dir/dt_remote" dcon \
    >"$dir/dt_clone.out" 2>"$dir/dt_clone.err")
  dt_clone_rc=$?
  if [ "$side" = "src" ]; then dt_action_dir="$dir/dsrc"; else dt_action_dir="$dir/dcon"; fi
  (cd "$dt_action_dir" && printf '%s\n' "$dt_action" \
    | "$DOLT" sql >"$dir/dt_action.out" 2>"$dir/dt_action.err")
  dt_action_rc=$?
  (cd "$dir/dcon" && printf '%s\n' "$dt_after" \
    | "$DOLT" sql -c >"$dir/dt_after.out" 2>"$dir/dt_after.err")
  dt_after_rc=$?
  (cd "$dir/dcon" && printf '%s\n' "$dt_q" \
    | "$DOLT" sql -r csv 2>"$dir/dt_query.err") >"$dir/dt.raw"
  dt_query_rc=$?
  dt_out=$(tr -d '"\r' <"$dir/dt.raw" | grep '^R|' | sort)

  if [ "$dl_seed_rc" -ne 0 ] || [ "$dl_clone_rc" -ne 0 ] \
     || ! vc_oracle_is_clean_error "$dl_action_rc" \
     || [ "$dl_after_rc" -ne 0 ] || [ "$dl_query_rc" -ne 0 ] \
     || [ "$dt_seed_rc" -ne 0 ] || [ "$dt_clone_rc" -ne 0 ] \
     || ! vc_oracle_is_clean_error "$dt_action_rc" \
     || [ "$dt_after_rc" -ne 0 ] || [ "$dt_query_rc" -ne 0 ]; then
    record_failure "$name"
    echo "    doltlite rc: $dl_seed_rc/$dl_clone_rc/$dl_action_rc/$dl_after_rc/$dl_query_rc"
    echo "    dolt rc:     $dt_seed_rc/$dt_clone_rc/$dt_action_rc/$dt_after_rc/$dt_query_rc"
    return
  fi
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

BASE_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'one');
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','-m','first release');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
"

echo "=== Version Control Oracle Tests: remote tags ==="
echo ""
echo "--- push and fetch ---"

remote_tag_flow "branch_push_excludes_tags" "$BASE_SEED" "" "" \
  "SELECT 'R|'||count(*) FROM dolt_tags;" \
  "SELECT CONCAT('R|',count(*)) FROM dolt_tags;"

remote_tag_flow "named_tag_push_fetches_on_unchanged_branch" "$BASE_SEED" \
  "SELECT dolt_push('origin','v1');" \
  "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|'||tag_name||'|'||message FROM dolt_tags;
   SELECT 'R|rows|'||count(*) FROM dolt_at_t WHERE commit_ref='v1';" \
  "SELECT CONCAT('R|',tag_name,'|',message) FROM dolt_tags;
   SELECT CONCAT('R|rows|',count(*)) FROM t AS OF 'v1';"

remote_tag_flow "annotated_tag_metadata_survives_push" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('release','--author','Alice Example <alice@example.com>','-m','annotated release');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
" "SELECT dolt_push('origin','release');" \
  "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|'||tag_name||'|'||tagger||'|'||email||'|'||message FROM dolt_tags;" \
  "SELECT CONCAT('R|',tag_name,'|',tagger,'|',email,'|',message) FROM dolt_tags;"

remote_tag_flow "fetch_follows_multiple_reachable_tags" "$BASE_SEED" "
INSERT INTO t VALUES (2,'two');
SELECT dolt_commit('-Am','c2');
SELECT dolt_tag('v2','-m','second release');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
SELECT dolt_push('origin','v2');
" "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|'||tag_name||'|'||message FROM dolt_tags ORDER BY tag_name;
   SELECT 'R|v1-rows|'||count(*) FROM dolt_at_t WHERE commit_ref='v1';
   SELECT 'R|v2-rows|'||count(*) FROM dolt_at_t WHERE commit_ref='v2';" \
  "SELECT CONCAT('R|',tag_name,'|',message) FROM dolt_tags ORDER BY tag_name;
   SELECT CONCAT('R|v1-rows|',count(*)) FROM t AS OF 'v1';
   SELECT CONCAT('R|v2-rows|',count(*)) FROM t AS OF 'v2';"

remote_tag_flow "fetch_replaces_same_named_local_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'one');
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','-m','remote old');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
" "
INSERT INTO t VALUES (2,'two');
SELECT dolt_commit('-Am','c2');
SELECT dolt_tag('-d','v1');
SELECT dolt_tag('v1','-m','remote replacement');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
" "
SELECT dolt_tag('-d','v1');
SELECT dolt_tag('v1','-m','local collision');
SELECT dolt_fetch('origin','main');
" "SELECT 'R|'||message FROM dolt_tags WHERE tag_name='v1';
   SELECT 'R|rows|'||count(*) FROM dolt_at_t WHERE commit_ref='v1';" \
  "SELECT CONCAT('R|',message) FROM dolt_tags WHERE tag_name='v1';
   SELECT CONCAT('R|rows|',count(*)) FROM t AS OF 'v1';"

remote_tag_flow "fetch_replaces_metadata_on_unchanged_target" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','-m','old message');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
" "
SELECT dolt_tag('-d','v1');
SELECT dolt_tag('v1','-m','new message');
SELECT dolt_push('origin','v1');
" "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|'||message FROM dolt_tags WHERE tag_name='v1';" \
  "SELECT CONCAT('R|',message) FROM dolt_tags WHERE tag_name='v1';"

remote_tag_flow "fetch_skips_tag_with_unfetched_target" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','main c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
" "
SELECT dolt_checkout('-b','private');
INSERT INTO t VALUES (2);
SELECT dolt_commit('-Am','private c2');
SELECT dolt_tag('private-tag','-m','not reachable from main');
SELECT dolt_push('origin','private-tag');
" "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|tags|'||count(*) FROM dolt_tags;
   SELECT 'R|rows|'||count(*) FROM t;" \
  "SELECT CONCAT('R|tags|',count(*)) FROM dolt_tags;
   SELECT CONCAT('R|rows|',count(*)) FROM t;"

remote_tag_flow "fetch_installs_older_reachable_tag" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','HEAD','-m','historical');
INSERT INTO t VALUES (2);
SELECT dolt_commit('-Am','c2');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
" "SELECT dolt_push('origin','v1');" \
  "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|head|'||count(*) FROM t;
   SELECT 'R|tag|'||count(*) FROM dolt_at_t WHERE commit_ref='v1';" \
  "SELECT CONCAT('R|head|',count(*)) FROM t;
   SELECT CONCAT('R|tag|',count(*)) FROM t AS OF 'v1';"

remote_tag_flow "pull_installs_tag_and_target_commit" "$BASE_SEED" "
INSERT INTO t VALUES (2,'two');
SELECT dolt_commit('-Am','c2');
SELECT dolt_tag('v2','-m','pulled release');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v2');
" "SELECT dolt_pull('origin','main');" \
  "SELECT 'R|rows|'||count(*) FROM t;
   SELECT 'R|'||tag_name||'|'||message FROM dolt_tags;" \
  "SELECT CONCAT('R|rows|',count(*)) FROM t;
   SELECT CONCAT('R|',tag_name,'|',message) FROM dolt_tags;"

remote_tag_flow "forced_tag_push_is_idempotent" "$BASE_SEED" \
  "SELECT dolt_push('origin','v1','--force');
   SELECT dolt_push('origin','v1','--force');" \
  "SELECT dolt_fetch('origin','main');
   SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|'||count(*)||'|'||max(message) FROM dolt_tags;" \
  "SELECT CONCAT('R|',count(*),'|',max(message)) FROM dolt_tags;"

echo "--- failure recovery ---"

remote_tag_error_flow "push_missing_ref_preserves_remote" "src" "$BASE_SEED" \
  "SELECT dolt_push('origin','missing-tag');" \
  "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|tags|'||count(*) FROM dolt_tags;
   SELECT 'R|rows|'||count(*) FROM t;" \
  "SELECT CONCAT('R|tags|',count(*)) FROM dolt_tags;
   SELECT CONCAT('R|rows|',count(*)) FROM t;"

remote_tag_error_flow "push_tag_to_missing_remote_preserves_clone" "src" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','-m','release');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
" "SELECT dolt_push('missing','v1');" "" \
  "SELECT 'R|'||count(*)||'|'||max(message) FROM dolt_tags;
   SELECT 'R|rows|'||count(*) FROM dolt_at_t WHERE commit_ref='v1';" \
  "SELECT CONCAT('R|',count(*),'|',max(message)) FROM dolt_tags;
   SELECT CONCAT('R|rows|',count(*)) FROM t AS OF 'v1';"

remote_tag_error_flow "fetch_missing_branch_preserves_tags" "con" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','-m','release');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
" "SELECT dolt_fetch('origin','missing-branch');" "" \
  "SELECT 'R|'||count(*)||'|'||max(message) FROM dolt_tags;
   SELECT 'R|rows|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*),'|',max(message)) FROM dolt_tags;
   SELECT CONCAT('R|rows|',count(*)) FROM t;"

remote_tag_error_flow "pull_missing_branch_preserves_tags" "con" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES (1);
SELECT dolt_commit('-Am','c1');
SELECT dolt_tag('v1','-m','release');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','v1');
" "SELECT dolt_pull('origin','missing-branch');" "" \
  "SELECT 'R|'||count(*)||'|'||max(message) FROM dolt_tags;
   SELECT 'R|rows|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*),'|',max(message)) FROM dolt_tags;
   SELECT CONCAT('R|rows|',count(*)) FROM t;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
