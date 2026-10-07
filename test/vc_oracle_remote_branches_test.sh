#!/bin/bash
# Compare remotes/<remote>/<branch> names, messages, and local/remote split; not hashes/dates/identity.
# latest_author* is unimplemented (commits store one name/email pair).

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# $seed/$advance/$consume; @REMOTE@ is substituted with the remote URL/dir.
remote_flow() {
  local name="$1" seed="$2" advance="$3" consume="$4" dl_query="$5" dt_query="$6"
  local dir="$TMPROOT/$name"; mkdir -p "$dir"

  local dl_remote="file://$dir/remote.db"
  local dl_src="$dir/src.db" dl_con="$dir/con.db"
  printf '%s\n' "${seed//@REMOTE@/$dl_remote}" \
    | vc_oracle_run_doltlite "$dl_src" >/dev/null 2>"$dir/dl_seed.err"
  printf 'SELECT dolt_clone('"'"'%s'"'"');\n' "$dl_remote" \
    | vc_oracle_run_doltlite "$dl_con" >/dev/null 2>"$dir/dl_clone.err"
  if [ -n "$advance" ]; then
    printf '%s\n' "${advance//@REMOTE@/$dl_remote}" \
      | vc_oracle_run_doltlite "$dl_src" >/dev/null 2>"$dir/dl_advance.err"
  fi
  if [ -n "$consume" ]; then
    printf '%s\n' "$consume" | vc_oracle_run_doltlite "$dl_con" >/dev/null 2>"$dir/dl_consume.err"
  fi
  local dl_out
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_query" \
           | vc_oracle_run_doltlite "$dl_con" 2>"$dir/dl.err" | tr -d '\r' | grep '^R|' | sort)

  local dt_remote="$dir/dt_remote"
  local dt_seed dt_advance dt_consume dt_q
  dt_seed=$(vc_oracle_translate_for_dolt "${seed//@REMOTE@/file://$dt_remote}")
  dt_advance=$(vc_oracle_translate_for_dolt "${advance//@REMOTE@/file://$dt_remote}")
  dt_consume=$(vc_oracle_translate_for_dolt "$consume")
  dt_q=$(vc_oracle_translate_for_dolt "$dt_query")
  local dt_out
  (
    mkdir -p "$dir/dsrc" "$dt_remote"
    cd "$dir/dsrc" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    printf '%s\n' "$dt_seed" | "$DOLT" sql -c >/dev/null 2>"$dir/dt_seed.err"
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dt_remote" dcon >/dev/null 2>"$dir/dt_clone.err"
    if [ -n "$dt_advance" ]; then
      ( cd "$dir/dsrc" && printf '%s\n' "$dt_advance" | "$DOLT" sql -c >/dev/null 2>"$dir/dt_advance.err" )
    fi
    cd "$dir/dcon" || exit 1
    if [ -n "$dt_consume" ]; then
      printf '%s\n' "$dt_consume" | "$DOLT" sql -c >/dev/null 2>"$dir/dt_consume.err"
    fi
    printf '%s\n' "$dt_q" | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Test: dolt_remote_branches ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'one');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
"
ADVANCE="
SELECT dolt_checkout('-b','feature');
INSERT INTO t VALUES (2,'two');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_push('origin','feature');
"
FETCH="SELECT dolt_fetch('origin','feature');"

echo "--- tracking refs after clone ---"
remote_flow "clone_tracking_names" "$SEED" "" "" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"

echo "--- tracking refs after fetching a branch pushed post-clone ---"
remote_flow "fetch_new_branch_names" "$SEED" "$ADVANCE" "$FETCH" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"
remote_flow "fetch_new_branch_metadata" "$SEED" "$ADVANCE" "$FETCH" \
  "SELECT 'R|'||name||'|'||latest_commit_message FROM dolt_remote_branches WHERE name='remotes/origin/feature';" \
  "SELECT CONCAT('R|',name,'|',latest_commit_message) FROM dolt_remote_branches WHERE name='remotes/origin/feature';"
remote_flow "fetched_branch_stays_remote_only" "$SEED" "$ADVANCE" "$FETCH" \
  "SELECT 'R|'||(SELECT count(*) FROM dolt_branches WHERE name='feature')||'|'||(SELECT count(*) FROM dolt_remote_branches WHERE name='remotes/origin/feature');" \
  "SELECT CONCAT('R|',(SELECT count(*) FROM dolt_branches WHERE name='feature'),'|',(SELECT count(*) FROM dolt_remote_branches WHERE name='remotes/origin/feature'));"

echo "--- fetch moves an existing tracking ref ---"
MAIN_ADVANCE="
INSERT INTO t VALUES (3,'three');
SELECT dolt_commit('-A','-m','c3');
SELECT dolt_push('origin','main');
"
remote_flow "fetch_advances_tracking_head" "$SEED" "$MAIN_ADVANCE" \
  "SELECT dolt_fetch('origin','main');" \
  "SELECT 'R|'||latest_commit_message FROM dolt_remote_branches WHERE name='remotes/origin/main';" \
  "SELECT CONCAT('R|',latest_commit_message) FROM dolt_remote_branches WHERE name='remotes/origin/main';"

echo "--- multi-branch remote: clone tracks every pushed branch ---"
MULTI_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'one');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_checkout('-b','dev');
INSERT INTO t VALUES (2,'two');
SELECT dolt_commit('-A','-m','c2');
SELECT dolt_checkout('main');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','dev');
"
remote_flow "multi_branch_clone_tracking_names" "$MULTI_SEED" "" "" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"

echo "--- clone makes only the checked-out branch local ---"
remote_flow "multi_branch_clone_local_branches" "$MULTI_SEED" "" "" \
  "SELECT 'R|'||name FROM dolt_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_branches ORDER BY name;"
DEV_ADVANCE="
SELECT dolt_checkout('dev');
INSERT INTO t VALUES (3,'three');
SELECT dolt_commit('-A','-m','c3');
SELECT dolt_push('origin','dev');
"
remote_flow "checkout_after_fetch_sees_fetched_commits" "$MULTI_SEED" "$DEV_ADVANCE" \
  "SELECT dolt_fetch('origin'); SELECT dolt_checkout('dev');" \
  "SELECT 'R|'||count(*) FROM dolt_at_t('dev');" \
  "SELECT CONCAT('R|',count(*)) FROM t AS OF 'dev';"

echo "--- refspec forms ---"
PUSH_FORMS="
SELECT dolt_branch('b1');
SELECT dolt_branch('b2');
SELECT dolt_push('origin','main:renamed');
SELECT dolt_push('origin','refs/heads/b1');
SELECT dolt_push('origin','b2','main');
"
remote_flow "push_refspec_forms" "$SEED" "$PUSH_FORMS" "SELECT dolt_fetch('origin');" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"
remote_flow "fetch_defaults_to_origin" "$SEED" "$ADVANCE" "SELECT dolt_fetch();" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"
remote_flow "fetch_refspec_names_tracking_ref" "$SEED" "$ADVANCE" \
  "SELECT dolt_fetch('origin','feature:refs/remotes/origin/renamed');" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"
PRUNE_ADVANCE="$ADVANCE
SELECT dolt_checkout('main');
SELECT dolt_push('origin',':feature');
"
remote_flow "fetch_prune_drops_deleted_branch" "$SEED" "$ADVANCE" \
  "SELECT dolt_fetch('origin');" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"
remote_flow "fetch_prune_after_remote_delete" "$SEED" "$PRUNE_ADVANCE" \
  "SELECT dolt_fetch('--prune','origin');" \
  "SELECT 'R|'||name FROM dolt_remote_branches ORDER BY name;" \
  "SELECT CONCAT('R|',name) FROM dolt_remote_branches ORDER BY name;"

echo "--- pull defaults and modes ---"
remote_flow "pull_defaults_to_origin_and_branch" "$SEED" "$MAIN_ADVANCE" \
  "SELECT dolt_pull();" \
  "SELECT 'R|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*)) FROM t;"
remote_flow "pull_remote_only" "$SEED" "$MAIN_ADVANCE" \
  "SELECT dolt_pull('origin');" \
  "SELECT 'R|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*)) FROM t;"
remote_flow "pull_no_ff_makes_merge_commit" "$SEED" "$MAIN_ADVANCE" \
  "SELECT dolt_pull('--no-ff','origin','main');" \
  "SELECT 'R|'||count(*) FROM dolt_log; SELECT 'R|rows|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*)) FROM dolt_log; SELECT CONCAT('R|rows|',count(*)) FROM t;"
remote_flow "pull_squash_stages_without_commit" "$SEED" "$MAIN_ADVANCE" \
  "SELECT dolt_pull('--squash','origin','main');" \
  "SELECT 'R|'||count(*) FROM dolt_log; SELECT 'R|'||table_name||'|'||staged||'|'||status FROM dolt_status; SELECT 'R|rows|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*)) FROM dolt_log; SELECT CONCAT('R|',table_name,'|',staged,'|',status) FROM dolt_status; SELECT CONCAT('R|rows|',count(*)) FROM t;"

echo "--- tracking ref name resolves as a ref ---"
remote_flow "tracking_name_resolves" "$SEED" "$ADVANCE" "$FETCH" \
  "SELECT 'R|'||count(*) FROM dolt_at_t('remotes/origin/feature');" \
  "SELECT CONCAT('R|',count(*)) FROM t AS OF 'remotes/origin/feature';"

vc_oracle_finish
