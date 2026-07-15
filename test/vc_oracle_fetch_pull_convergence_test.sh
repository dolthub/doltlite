#!/bin/bash
#
# Oracle: after cloning a remote, fetching/pulling later pushes must converge
# the consumer to the remote the same way real Dolt does -- fast-forward
# pulls, divergent (merge) pulls, and multi-branch fetch + tracking checkout.
# The existing vc_oracle_remotes suite covers a single fetch/checkout; this
# adds the incremental "remote advances, consumer catches up" surface.
#
# doltlite consumes with dolt_clone/dolt_fetch/dolt_pull against a file:// .db
# remote; Dolt clones a directory remote and fetches/pulls there. Commit
# hashes and wall-clock dates differ by design and are never compared; row
# sets, counts, commit messages, branch lists, and active branch are.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

# Run a full doltlite-vs-dolt remote flow and compare one query. The flow is
# expressed as three phases of engine-agnostic SQL (SELECT dolt_*; the dolt
# side is auto-translated to CALL): $seed builds+pushes the source, $advance
# pushes more from the source, $consume runs on the freshly cloned consumer.
# The remote URL/dir is substituted for the token @REMOTE@.
remote_flow() {
  local name="$1" seed="$2" advance="$3" consume="$4" dl_query="$5" dt_query="$6"
  local dir="$TMPROOT/$name"; mkdir -p "$dir"

  # ---------- doltlite ----------
  local dl_remote="file://$dir/remote.db"
  local dl_src="$dir/src.db" dl_con="$dir/con.db"
  printf '%s\n' "${seed//@REMOTE@/$dl_remote}" \
    | "$DOLTLITE" "$dl_src" >/dev/null 2>"$dir/dl_seed.err"
  printf 'SELECT dolt_clone('"'"'%s'"'"');\n' "$dl_remote" \
    | "$DOLTLITE" "$dl_con" >/dev/null 2>"$dir/dl_clone.err"
  if [ -n "$advance" ]; then
    printf '%s\n' "${advance//@REMOTE@/$dl_remote}" \
      | "$DOLTLITE" "$dl_src" >/dev/null 2>"$dir/dl_advance.err"
  fi
  printf '%s\n' "$consume" | "$DOLTLITE" "$dl_con" >/dev/null 2>"$dir/dl_consume.err"
  local dl_out
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_query" \
           | "$DOLTLITE" "$dl_con" 2>"$dir/dl.err" | tr -d '\r' | grep '^R|' | sort)

  # ---------- dolt ----------
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
    printf '%s\n' "$dt_consume" | "$DOLT" sql -c >/dev/null 2>"$dir/dt_consume.err"
    printf '%s\n' "$dt_q" | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Test: fetch/pull convergence ==="
echo ""

# ---- fast-forward pull: remote gains two commits, consumer pulls ----
FF_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'one');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
"
FF_ADVANCE="
INSERT INTO t VALUES (2,'two');
SELECT dolt_commit('-A','-m','c2');
INSERT INTO t VALUES (3,'three');
SELECT dolt_commit('-A','-m','c3');
SELECT dolt_push('origin','main');
"
FF_PULL="SELECT dolt_pull('origin','main');"

echo "--- fast-forward pull ---"
remote_flow "ff_contents" "$FF_SEED" "$FF_ADVANCE" "$FF_PULL" \
  "SELECT 'R|'||id||'|'||v FROM t;" \
  "SELECT CONCAT('R|',id,'|',v) FROM t;"
remote_flow "ff_log_messages" "$FF_SEED" "$FF_ADVANCE" "$FF_PULL" \
  "SELECT 'R|'||message FROM dolt_log;" \
  "SELECT CONCAT('R|',message) FROM dolt_log;"
remote_flow "ff_log_count" "$FF_SEED" "$FF_ADVANCE" "$FF_PULL" \
  "SELECT 'R|'||count(*) FROM dolt_log;" \
  "SELECT CONCAT('R|',count(*)) FROM dolt_log;"

# ---- fetch (no merge): remote advances, consumer fetches; working stays put,
#      origin/main advances. Compare the tracked-ref row count via a topic
#      branch off origin/main. ----
echo "--- fetch then track origin/main ---"
FETCH_TRACK="
SELECT dolt_fetch('origin','main');
SELECT dolt_checkout('-b','topic','origin/main');
"
remote_flow "fetch_track_contents" "$FF_SEED" "$FF_ADVANCE" "$FETCH_TRACK" \
  "SELECT 'R|'||active_branch()||'|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',active_branch(),'|',count(*)) FROM t;"

# ---- divergent pull: consumer commits a disjoint row locally while the
#      remote gains a disjoint row; pull fetches origin/main and merges it. ----
echo "--- divergent pull auto-merge (no conflict) ---"
MERGE_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
"
MERGE_ADVANCE="
INSERT INTO t VALUES (200,'from-remote');
SELECT dolt_commit('-A','-m','remote row');
SELECT dolt_push('origin','main');
"
MERGE_PULL="
INSERT INTO t VALUES (100,'from-local');
SELECT dolt_commit('-A','-m','local row');
SELECT dolt_pull('origin','main');
"
remote_flow "merge_contents" "$MERGE_SEED" "$MERGE_ADVANCE" "$MERGE_PULL" \
  "SELECT 'R|'||id||'|'||v FROM t;" \
  "SELECT CONCAT('R|',id,'|',v) FROM t;"
remote_flow "merge_rowcount" "$MERGE_SEED" "$MERGE_ADVANCE" "$MERGE_PULL" \
  "SELECT 'R|'||count(*) FROM t;" \
  "SELECT CONCAT('R|',count(*)) FROM t;"
remote_flow "merge_log_count" "$MERGE_SEED" "$MERGE_ADVANCE" "$MERGE_PULL" \
  "SELECT 'R|'||count(*) FROM dolt_log;" \
  "SELECT CONCAT('R|',count(*)) FROM dolt_log;"

# ---- divergent pull with compatible schema/data changes: the remote changes
#      schema while the consumer commits data against the old schema. ----
echo "--- divergent pull auto-merge (schema + data) ---"
SCHEMA_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
"
SCHEMA_ADVANCE="
ALTER TABLE t ADD COLUMN note TEXT DEFAULT 'remote-default';
UPDATE t SET note='remote-note' WHERE id=1;
INSERT INTO t(id,v,note) VALUES (200,'from-remote','remote-row');
SELECT dolt_commit('-A','-m','remote schema');
SELECT dolt_push('origin','main');
"
SCHEMA_PULL="
INSERT INTO t VALUES (100,'from-local');
SELECT dolt_commit('-A','-m','local row');
SELECT dolt_pull('origin','main');
"
remote_flow "merge_schema_rows" "$SCHEMA_SEED" "$SCHEMA_ADVANCE" "$SCHEMA_PULL" \
  "SELECT 'R|'||id||'|'||v||'|'||IFNULL(note,'NULL') FROM t;" \
  "SELECT CONCAT('R|',id,'|',v,'|',IFNULL(note,'NULL')) FROM t;"
remote_flow "merge_schema_shape" "$SCHEMA_SEED" "$SCHEMA_ADVANCE" "$SCHEMA_PULL" \
  "SELECT 'R|'||name FROM pragma_table_info('t') ORDER BY cid;" \
  "SELECT CONCAT('R|',column_name) FROM information_schema.columns WHERE table_name='t' ORDER BY ordinal_position;"

# ---- divergent pull where the remote adds a table and the consumer changes
#      existing data. ----
echo "--- divergent pull auto-merge (new table + local row) ---"
TABLE_ADVANCE="
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u VALUES (7,'remote-table');
SELECT dolt_commit('-A','-m','remote table');
SELECT dolt_push('origin','main');
"
remote_flow "merge_new_table_and_local_row" "$MERGE_SEED" "$TABLE_ADVANCE" "$MERGE_PULL" \
  "SELECT 'R|t|'||id||'|'||v FROM t UNION ALL SELECT 'R|u|'||id||'|'||v FROM u;" \
  "SELECT CONCAT('R|t|',id,'|',v) FROM t UNION ALL SELECT CONCAT('R|u|',id,'|',v) FROM u;"

# ---- divergent pull with a data conflict: both sides update the same row.
#      Compare the post-state, not exact error text. ----
echo "--- divergent pull conflict post-state ---"
CONFLICT_ADVANCE="
UPDATE t SET v='from-remote' WHERE id=1;
SELECT dolt_commit('-A','-m','remote update');
SELECT dolt_push('origin','main');
"
CONFLICT_PULL="
UPDATE t SET v='from-local' WHERE id=1;
SELECT dolt_commit('-A','-m','local update');
SELECT dolt_pull('origin','main');
"
remote_flow "pull_conflict_poststate" "$MERGE_SEED" "$CONFLICT_ADVANCE" "$CONFLICT_PULL" \
  "SELECT 'R|'||(SELECT count(*) FROM dolt_conflicts)||'|'||(SELECT v FROM t WHERE id=1);" \
  "SELECT CONCAT('R|',(SELECT COUNT(*) FROM dolt_conflicts),'|',(SELECT v FROM t WHERE id=1));"

# ---- divergent pull where both sides add independent indexes and rows. ----
echo "--- divergent pull auto-merge (independent indexes) ---"
INDEX_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(32), tag VARCHAR(32));
INSERT INTO t VALUES (1,'base','base-tag');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
"
INDEX_ADVANCE="
CREATE INDEX t_v_idx ON t(v);
INSERT INTO t VALUES (200,'from-remote','remote-tag');
SELECT dolt_commit('-A','-m','remote index');
SELECT dolt_push('origin','main');
"
INDEX_PULL="
CREATE INDEX t_tag_idx ON t(tag);
INSERT INTO t VALUES (100,'from-local','local-tag');
SELECT dolt_commit('-A','-m','local index');
SELECT dolt_pull('origin','main');
"
remote_flow "merge_independent_indexes_rows" "$INDEX_SEED" "$INDEX_ADVANCE" "$INDEX_PULL" \
  "SELECT 'R|row|'||id||'|'||v||'|'||tag FROM t
   UNION ALL
   SELECT 'R|idx|'||name FROM pragma_index_list('t') WHERE name IN ('t_tag_idx','t_v_idx');" \
  "SELECT CONCAT('R|row|',id,'|',v,'|',tag) FROM t
   UNION ALL
   SELECT CONCAT('R|idx|',index_name) FROM information_schema.statistics
    WHERE table_name='t' AND index_name IN ('t_tag_idx','t_v_idx');"

# ---- divergent pull where the remote renames/adds columns while the
#      consumer inserts a row against the old schema. ----
echo "--- divergent pull auto-merge (rename/add column + local insert) ---"
RENAME_ADVANCE="
ALTER TABLE t RENAME COLUMN v TO val;
ALTER TABLE t ADD COLUMN note TEXT DEFAULT 'remote-default';
UPDATE t SET note='remote-note' WHERE id=1;
INSERT INTO t(id,val,note) VALUES (200,'from-remote','remote-row');
SELECT dolt_commit('-A','-m','remote rename');
SELECT dolt_push('origin','main');
"
remote_flow "merge_rename_add_column_local_insert" "$SCHEMA_SEED" "$RENAME_ADVANCE" "$SCHEMA_PULL" \
  "SELECT 'R|col|'||name FROM pragma_table_info('t')
   UNION ALL
   SELECT 'R|row|'||id||'|'||val||'|'||IFNULL(note,'NULL') FROM t;" \
  "SELECT CONCAT('R|col|',column_name) FROM information_schema.columns WHERE table_name='t'
   UNION ALL
   SELECT CONCAT('R|row|',id,'|',val,'|',IFNULL(note,'NULL')) FROM t;"

# ---- divergent pull where the remote drops a table while the consumer edits
#      that table. Compare conflict/table existence post-state. ----
echo "--- divergent pull conflict (remote drop + local edit) ---"
DROP_ADVANCE="
DROP TABLE t;
SELECT dolt_commit('-A','-m','remote drop');
SELECT dolt_push('origin','main');
"
DROP_PULL="
UPDATE t SET v='from-local' WHERE id=1;
SELECT dolt_commit('-A','-m','local edit');
SELECT dolt_pull('origin','main');
"
remote_flow "pull_drop_edit_conflict_poststate" "$MERGE_SEED" "$DROP_ADVANCE" "$DROP_PULL" \
  "SELECT 'R|conflicts|'||(SELECT count(*) FROM dolt_conflicts)
   UNION ALL
   SELECT 'R|tables|'||count(*) FROM sqlite_master WHERE type='table' AND name='t';" \
  "SELECT CONCAT('R|conflicts|',(SELECT COUNT(*) FROM dolt_conflicts))
   UNION ALL
   SELECT CONCAT('R|tables|',count(*)) FROM information_schema.tables
    WHERE table_schema=database() AND table_name='t';"

# ---- multi-branch fetch: two branches pushed; consumer fetches and tracks
#      each; compare contents per tracked branch. ----
echo "--- multi-branch fetch + tracking ---"
MB_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1,'base');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_remote('add','origin','@REMOTE@');
SELECT dolt_push('origin','main');
SELECT dolt_checkout('-b','featA');
INSERT INTO t VALUES (2,'a');
SELECT dolt_commit('-A','-m','a');
SELECT dolt_push('origin','featA');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b','featB');
INSERT INTO t VALUES (3,'b');
SELECT dolt_commit('-A','-m','b');
SELECT dolt_push('origin','featB');
SELECT dolt_checkout('main');
"
remote_flow "mb_featA" "$MB_SEED" "" \
  "SELECT dolt_fetch('origin','featA'); SELECT dolt_checkout('-b','ta','origin/featA');" \
  "SELECT 'R|'||id||'|'||v FROM t;" \
  "SELECT CONCAT('R|',id,'|',v) FROM t;"
remote_flow "mb_featB" "$MB_SEED" "" \
  "SELECT dolt_fetch('origin','featB'); SELECT dolt_checkout('-b','tb','origin/featB');" \
  "SELECT 'R|'||id||'|'||v FROM t;" \
  "SELECT CONCAT('R|',id,'|',v) FROM t;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
