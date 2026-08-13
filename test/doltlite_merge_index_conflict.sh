#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0
fail=0
FAILED_NAMES=""

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    expected: $expected"
    echo "    actual:   $actual"
  fi
}

DB="$TMPROOT/ours.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2 WHERE id=1;
INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
INSERT INTO t VALUES(3,30);
SELECT dolt_commit('-A','-m','main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t ORDER BY id;")
check "ours_table_state" "1|3
2|20
3|30" "$out"
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "ours_iv_three_rows" "1|3
2|20
3|30" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t INDEXED BY iv WHERE v > 0;")
check "ours_iv_count_matches" "3" "$out"

DB="$TMPROOT/theirs.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2 WHERE id=1;
INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
INSERT INTO t VALUES(3,30);
SELECT dolt_commit('-A','-m','main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--theirs','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t ORDER BY id;")
check "theirs_table_state" "1|2
2|20
3|30" "$out"
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "theirs_iv_three_rows" "1|2
2|20
3|30" "$out"

DB="$TMPROOT/clean.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,30);
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "clean_merge_iv_complete" "1|1
2|20
3|30" "$out"

DB="$TMPROOT/twoidx.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT);
CREATE INDEX iv ON t(v);
CREATE INDEX iw ON t(w);
INSERT INTO t VALUES(1,1,10);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=2, w=20 WHERE id=1;
INSERT INTO t VALUES(2,200,2000);
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
UPDATE t SET v=3, w=30 WHERE id=1;
INSERT INTO t VALUES(3,300,3000);
SELECT dolt_commit('-A','-m','main');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "two_idx_iv" "1|3
2|200
3|300" "$out"
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || w FROM t INDEXED BY iw WHERE w > 0 ORDER BY w;")
check "two_idx_iw" "1|30
2|2000
3|3000" "$out"

DB="$TMPROOT/delmod.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id=1;
SELECT dolt_commit('-A','-m','feat-delete');
SELECT dolt_checkout('main');
UPDATE t SET v=3 WHERE id=1;
SELECT dolt_commit('-A','-m','main-update');
BEGIN;
SELECT dolt_merge('feat');
SELECT dolt_conflicts_resolve('--ours','t');
SELECT dolt_commit('-A','-m','resolved');
COMMIT;
EOF
out=$("$DOLTLITE" "$DB" "SELECT id || '|' || v FROM t INDEXED BY iv WHERE v > 0 ORDER BY v;")
check "delmod_iv_one_row" "1|3" "$out"

DB="$TMPROOT/index_vs_table_drop.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE keep(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
CREATE INDEX iv ON t(v);
SELECT dolt_commit('-A','-m','main-index');
EOF
"$DOLTLITE" "$DB/feat" <<'EOF' >/dev/null 2>&1
DROP TABLE t;
CREATE TABLE added(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','feat-drop');
EOF
out=$("$DOLTLITE" "$DB" <<'EOF' 2>/dev/null | tail -1
BEGIN;
SELECT dolt_merge('feat');
SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' ||
       (SELECT count(*) FROM dolt_status WHERE status='schema conflict') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='t') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='added');
ROLLBACK;
EOF
)
check "added_index_vs_table_drop_conflicts" "1|1|1|1" "$out"

DB="$TMPROOT/table_drop_vs_index.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE keep(id INTEGER PRIMARY KEY);
CREATE TABLE obsolete(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
DROP TABLE t;
CREATE TABLE added(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','main-drop');
EOF
"$DOLTLITE" "$DB/feat" <<'EOF' >/dev/null 2>&1
DROP TABLE obsolete;
CREATE INDEX iv ON t(v);
SELECT dolt_commit('-A','-m','feat-index');
EOF
out=$("$DOLTLITE" "$DB" <<'EOF' 2>/dev/null | tail -1
BEGIN;
SELECT dolt_merge('feat');
SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' ||
       (SELECT count(*) FROM dolt_status WHERE status='schema conflict') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='t') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='added') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='obsolete');
ROLLBACK;
EOF
)
check "table_drop_vs_added_index_conflicts" "1|1|0|1|0" "$out"

DB="$TMPROOT/index_on_conflicted_add.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, c1 TEXT);
CREATE TABLE kv(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('src');
CREATE TABLE ours_only(id INTEGER PRIMARY KEY, payload TEXT, c2 TEXT);
CREATE TABLE shared_name(id INTEGER PRIMARY KEY, payload TEXT, c3 TEXT);
CREATE INDEX idx_t ON t(payload);
CREATE INDEX idx_ours_1 ON shared_name(payload);
CREATE INDEX idx_ours_2 ON shared_name(payload);
SELECT dolt_commit('-A','-m','ours');
EOF
"$DOLTLITE" "$DB/src" <<'EOF' >/dev/null 2>&1
DROP TABLE t;
CREATE TABLE shared_name(id INTEGER PRIMARY KEY, payload TEXT);
CREATE TABLE theirs_only(id INTEGER PRIMARY KEY, payload TEXT);
CREATE INDEX idx_theirs ON shared_name(payload);
SELECT dolt_commit('-A','-m','theirs');
EOF
out=$("$DOLTLITE" "$DB" <<'EOF' 2>/dev/null | tail -1
BEGIN;
SELECT dolt_merge('src');
SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='idx_theirs') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='idx_t');
ROLLBACK;
EOF
)
check "source_index_on_conflicted_table_is_not_adopted" "2|0|1" "$out"

DB="$TMPROOT/rename_vs_drop.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE first(id INTEGER PRIMARY KEY);
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
DROP TABLE first;
ALTER TABLE t RENAME TO renamed;
SELECT dolt_commit('-A','-m','rename');
EOF
"$DOLTLITE" "$DB/feat" <<'EOF' >/dev/null 2>&1
DROP TABLE t;
CREATE TABLE replacement(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-A','-m','drop-add');
EOF
# A table renamed on one side and dropped on the other is now kept, matching
# Dolt, so this cherry-pick no longer quietly discards `renamed` and its index.
# It cannot be applied either: the current branch dropped t and gave its catalog
# number to `replacement`, so the rename does not resolve and the catalog rows
# conflict. The pick is refused with nothing committed and the schema untouched.
# Dolt refuses this scenario too, reporting "no changes were made, nothing to
# commit" -- a different message for the same outcome, and both are preferable
# to the silent table drop this used to assert.
out=$("$DOLTLITE" "$DB/feat" "SELECT dolt_cherry_pick('main');" 2>/dev/null)
rc=$?
check "rename_vs_drop_cherry_pick_refuses" "1" "$rc"
out=$("$DOLTLITE" "$DB/feat" \
  "SELECT group_concat(name, ',') FROM sqlite_schema WHERE name IN ('first','t','renamed','iv','replacement') ORDER BY name;")
check "rename_vs_drop_leaves_schema_untouched" "first,replacement" "$out"
out=$("$DOLTLITE" "$DB/feat" \
  "SELECT (SELECT count(*) FROM dolt_status) || '|' || (SELECT message FROM dolt_log LIMIT 1);")
check "rename_vs_drop_commits_nothing" "0|drop-add" "$out"

DB="$TMPROOT/index_drop_vs_parent_rename.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE dropme(id INTEGER PRIMARY KEY);
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX iv ON t(v);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
DROP TABLE dropme;
ALTER TABLE t RENAME TO renamed;
SELECT dolt_commit('-A','-m','drop-and-rename');
EOF
"$DOLTLITE" "$DB/feat" <<'EOF' >/dev/null 2>&1
DROP INDEX iv;
SELECT dolt_commit('-A','-m','drop-index');
EOF
out=$("$DOLTLITE" "$DB/feat" <<'EOF' 2>/dev/null | tail -1
BEGIN;
SELECT dolt_cherry_pick('main');
SELECT (SELECT count(*) FROM dolt_status WHERE status='schema conflict') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='t') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='renamed') || '|' ||
       (SELECT count(*) FROM sqlite_schema WHERE name='iv');
ROLLBACK;
EOF
)
check "index_drop_vs_parent_rename_conflicts" "1|1|0|0" "$out"

DB="$TMPROOT/index_retarget_to_divergent_rename.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE a(id INTEGER PRIMARY KEY, payload TEXT, n INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, payload TEXT, n INT);
CREATE INDEX i1 ON b(payload);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
ALTER TABLE a RENAME TO ours_a;
DROP TABLE b;
SELECT dolt_commit('-A','-m','ours');
EOF
"$DOLTLITE" "$DB/feat" <<'EOF' >/dev/null 2>&1
ALTER TABLE a RENAME TO theirs_a;
ALTER TABLE theirs_a ADD COLUMN extra TEXT;
DROP TABLE b;
CREATE INDEX i1 ON theirs_a(payload);
CREATE INDEX i2 ON theirs_a(payload);
SELECT dolt_commit('-A','-m','theirs');
EOF
out=$("$DOLTLITE" "$DB" <<'EOF' 2>/dev/null | tail -1
BEGIN;
SELECT dolt_merge('feat');
SELECT (SELECT coalesce(sum(num_conflicts),0) FROM dolt_conflicts) || '|' ||
       (SELECT group_concat(name, ',') FROM
          (SELECT name FROM sqlite_schema
           WHERE name IN ('ours_a','theirs_a','i1','i2') ORDER BY name));
ROLLBACK;
EOF
)
check "retarget_to_divergent_rename_keeps_index_catalog_valid" \
  "0|i1,i2,ours_a,theirs_a" "$out"

DB="$TMPROOT/index_on_excluded_rename.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE a(id INTEGER PRIMARY KEY, payload TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, payload TEXT);
CREATE INDEX ic ON c(payload);
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
ALTER TABLE a RENAME TO ours_a;
DROP INDEX ic;
SELECT dolt_commit('-A','-m','ours');
EOF
"$DOLTLITE" "$DB/feat" <<'EOF' >/dev/null 2>&1
ALTER TABLE a RENAME TO theirs_a;
DROP TABLE c;
CREATE INDEX ic ON theirs_a(payload);
CREATE INDEX incoming_new ON theirs_a(payload);
SELECT dolt_commit('-A','-m','theirs');
EOF
out=$("$DOLTLITE" "$DB" <<'EOF' 2>/dev/null | tail -1
BEGIN;
SELECT dolt_merge('feat');
SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' ||
       (SELECT group_concat(name, ',') FROM
          (SELECT name FROM sqlite_schema ORDER BY name));
ROLLBACK;
EOF
)
check "index_on_excluded_rename_is_not_adopted" "1|c,ours_a" "$out"

# A modify/modify change that touches disjoint columns merges cell-wise rather
# than conflicting. The index delta for that merged row must be taken against
# ours' row, since the index it patches is built over ours' root. Indexes
# spanning both an ours-changed and a theirs-changed column are what expose a
# base-keyed delete: it misses, and ours' entry survives beside the merged one.
DB="$TMPROOT/cellmerge.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(pk INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
CREATE INDEX iab ON t(a,b);
CREATE INDEX iabc ON t(a,b,c);
INSERT INTO t VALUES(1,'a0','b0','c0'),(2,'p0','q0','r0');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET b='b1' WHERE pk=1;
UPDATE t SET b='q1' WHERE pk=2;
SELECT dolt_commit('-A','-m','theirs');
SELECT dolt_checkout('main');
UPDATE t SET a='a1' WHERE pk=1;
UPDATE t SET a='p1' WHERE pk=2;
SELECT dolt_commit('-A','-m','ours');
SELECT dolt_merge('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b,' ') FROM (SELECT pk,a,b FROM t NOT INDEXED ORDER BY pk);")
check "cellmerge_table_scan_rows" "1:a1:b1 2:p1:q1" "$out"
out=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b,' ') FROM (SELECT pk,a,b FROM t INDEXED BY iab WHERE a>='' ORDER BY a,b,pk);")
check "cellmerge_composite_index_has_no_stale_entry" "1:a1:b1 2:p1:q1" "$out"
out=$("$DOLTLITE" "$DB" "SELECT count(*) FROM (SELECT pk FROM t INDEXED BY iab WHERE a>='');")
check "cellmerge_composite_index_count" "2" "$out"
out=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b||':'||c,' ') FROM (SELECT pk,a,b,c FROM t INDEXED BY iabc WHERE a>='' ORDER BY a,b,c,pk);")
check "cellmerge_three_col_index_has_no_stale_entry" "1:a1:b1:c0 2:p1:q1:r0" "$out"
out=$("$DOLTLITE" "$DB" "SELECT pk||':'||a||':'||b FROM t INDEXED BY iab WHERE a='a1';")
check "cellmerge_index_seek_serves_merged_row_only" "1:a1:b1" "$out"

PRE=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b,' ') FROM (SELECT pk,a,b FROM t INDEXED BY iab WHERE a>='' ORDER BY a,b,pk);")
"$DOLTLITE" "$DB" "REINDEX iab;" >/dev/null 2>&1
POST=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b,' ') FROM (SELECT pk,a,b FROM t INDEXED BY iab WHERE a>='' ORDER BY a,b,pk);")
check "cellmerge_index_matches_full_rebuild" "$POST" "$PRE"

# Same shape reached through cherry-pick and revert, which replay onto ours
# through the identical row-merge path.
DB="$TMPROOT/cellmerge_pick.db"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(pk INTEGER PRIMARY KEY, a TEXT, b TEXT);
CREATE INDEX iab ON t(a,b);
INSERT INTO t VALUES(1,'a0','b0');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_checkout('-b','feat');
UPDATE t SET b='b1' WHERE pk=1;
SELECT dolt_commit('-A','-m','theirs');
SELECT dolt_checkout('main');
UPDATE t SET a='a1' WHERE pk=1;
SELECT dolt_commit('-A','-m','ours');
SELECT dolt_cherry_pick('feat');
EOF
out=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b,' ') FROM (SELECT pk,a,b FROM t NOT INDEXED ORDER BY pk);")
check "cherry_pick_cellmerge_table_scan_rows" "1:a1:b1" "$out"
out=$("$DOLTLITE" "$DB" "SELECT group_concat(pk||':'||a||':'||b,' ') FROM (SELECT pk,a,b FROM t INDEXED BY iab WHERE a>='' ORDER BY a,b,pk);")
check "cherry_pick_cellmerge_index_has_no_stale_entry" "1:a1:b1" "$out"

# The shape itself, in both directions and for cherry-pick: a table renamed on
# one branch and dropped on the other is kept with its rows and its index, and
# the merge is clean. Dolt 2.2.2 answers the same for all three.
for direction in theirs ours; do
  DB="$TMPROOT/rename_over_drop_$direction.db"
  rm -rf "$DB"
  if [ "$direction" = theirs ]; then ren=feat; drp=main; else ren=main; drp=feat; fi
  "$DOLTLITE" "$DB" <<EOF >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX i_v ON t(v);
INSERT INTO t VALUES(1,'a'),(2,'b');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('$ren');
ALTER TABLE t RENAME TO t2;
SELECT dolt_commit('-Am','rename');
SELECT dolt_checkout('$drp');
DROP TABLE t;
SELECT dolt_commit('-Am','drop');
SELECT dolt_checkout('main');
EOF
  if [ "$direction" = theirs ]; then merge_from=feat; else merge_from=feat; fi
  out=$("$DOLTLITE" "$DB" "SELECT length(dolt_merge('$merge_from'));" 2>/dev/null)
  check "rename_over_drop_${direction}_merge_clean" "40" "$out"
  out=$("$DOLTLITE" "$DB" \
    "SELECT (SELECT count(*) FROM t2) || '|' ||
            (SELECT group_concat(name,',') FROM (SELECT name FROM sqlite_schema WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name)) || '|' ||
            (SELECT * FROM pragma_integrity_check LIMIT 1);" 2>/dev/null)
  check "rename_over_drop_${direction}_keeps_table" "2|i_v|ok" "$out"
done

# Both sides rename the same table to different names. Dolt treats each rename
# as delete+add and keeps both tables with no conflicts. A phantom
# sqlite_master modify/modify used to refuse the merge (or, earlier, succeed
# and drop theirs). Both tables stay, and autocommit can finish.
DB="$TMPROOT/rename_vs_rename.db"; rm -rf "$DB"
"$DOLTLITE" "$DB" <<'EOF' >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME TO theirs_t;
SELECT dolt_commit('-Am','theirs rename');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME TO ours_t;
SELECT dolt_commit('-Am','ours rename');
EOF
out=$("$DOLTLITE" "$DB" "SELECT length(dolt_merge('feat'));" 2>/dev/null)
check "rename_vs_rename_merge_clean" "40" "$out"
out=$("$DOLTLITE" "$DB" \
  "SELECT (SELECT group_concat(name,',') FROM (SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ours_t','theirs_t','t') ORDER BY name)) || '|' ||
          (SELECT count(*) FROM ours_t) || '|' ||
          (SELECT count(*) FROM theirs_t) || '|' ||
          (SELECT coalesce(sum(num_conflicts),0) FROM dolt_conflicts);" 2>/dev/null)
check "rename_vs_rename_keeps_both_tables" "ours_t,theirs_t|1|1|0" "$out"

echo
echo "doltlite_merge_index_conflict: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
