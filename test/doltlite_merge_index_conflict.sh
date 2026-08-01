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
out=$("$DOLTLITE" "$DB/feat" "SELECT dolt_cherry_pick('main');" 2>/dev/null)
rc=$?
check "rename_vs_drop_cherry_pick_succeeds" "0" "$rc"
out=$("$DOLTLITE" "$DB/feat" \
  "SELECT group_concat(name, ',') FROM sqlite_schema WHERE name IN ('first','t','renamed','iv','replacement') ORDER BY name;")
check "rename_vs_drop_keeps_current_schema" "replacement" "$out"
out=$("$DOLTLITE" "$DB/feat" \
  "SELECT (SELECT count(*) FROM dolt_status) || '|' || (SELECT message FROM dolt_log LIMIT 1);")
check "rename_vs_drop_is_clean_commit" "0|rename" "$out"

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
  "3|i1,i2,ours_a,theirs_a" "$out"

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

echo
echo "doltlite_merge_index_conflict: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
