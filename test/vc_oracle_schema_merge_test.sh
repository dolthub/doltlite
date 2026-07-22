#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

dl() {
  local db="$1" sql="$2" tag="$3"
  "$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$tag.err"
}

dl_setup() {
  local db="$1" tag="$2"
  "$DOLTLITE" "$db" >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
}

dl_errors() {
  local db="$1" sql="$2" tag="$3"
  "$DOLTLITE" "$db" "$sql" >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
  grep -qiE 'error|Error|conflict' "$TMPROOT/$tag.out" "$TMPROOT/$tag.err" 2>/dev/null
}

expect_eq() {
  local name="$1" want="$2" got="$3"
  if [ "$want" = "$got" ]; then pass_name "$name"
  else
    fail_name "$name"
    echo "    want: |$want|"
    echo "    got:  |$got|"
  fi
}

expect_merge_ok() {
  local name="$1" db="$2"
  if dl_errors "$db" "SELECT dolt_merge('feat');" "$name"; then
    fail_name "$name"
    echo "    merge errored: $(cat $TMPROOT/$name.out $TMPROOT/$name.err 2>/dev/null | head -3)"
  else
    pass_name "$name"
  fi
}

expect_merge_conflict() {
  local name="$1" db="$2"
  if dl_errors "$db" "SELECT dolt_merge('feat');" "$name"; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    merge succeeded but expected conflict"
  fi
}

setup_base() {
  local db="$1" tag="$2" schema="$3"
  rm -f "$db"
  cat <<SQL | dl_setup "$db" "$tag"
$schema
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SQL
}

echo "=== Schema Merge Oracle (Dolt spec) ==="
echo ""

echo "--- Tables ---"

DB="$TMPROOT/t1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t1"
CREATE TABLE anchor(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE newtbl(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','feat_add');
SELECT dolt_checkout('main');
CREATE TABLE newtbl(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','main_add');
SQL
expect_merge_ok "table_both_add_identical" "$DB"

DB="$TMPROOT/t2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t2"
CREATE TABLE anchor(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE newtbl(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','feat_add');
SELECT dolt_checkout('main');
CREATE TABLE newtbl(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_commit('-Am','main_add');
SQL
expect_merge_conflict "table_both_add_different" "$DB"
expect_eq "schema_conflicts_columns" \
  "table_name|base_schema|our_schema|their_schema|description" \
  "$(dl "$DB" "SELECT group_concat(name, '|') FROM (SELECT name FROM pragma_table_info('dolt_schema_conflicts') ORDER BY cid);" "t2_columns")"
expect_eq "schema_conflicts_autocommit_rollback" "0|0|0" \
  "$(dl "$DB" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" "t2_autocommit")"
cat <<'SQL' | dl_setup "$DB" "t2_transaction"
BEGIN;
SELECT dolt_merge('feat');
COMMIT;
SQL
expect_eq "schema_conflicts_transaction_state" "1|1|0|1" \
  "$(dl "$DB" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT coalesce(sum(num_conflicts),-1) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" "t2_state")"
expect_eq "schema_conflicts_schema_rows" "newtbl|1|1|1" \
  "$(dl "$DB" "SELECT table_name || '|' || (base_schema='<deleted>') || '|' || (our_schema LIKE 'CREATE TABLE newtbl%') || '|' || (their_schema LIKE 'CREATE TABLE newtbl%') FROM dolt_schema_conflicts;" "t2_rows")"
if dl_errors "$DB" "SELECT dolt_conflicts_resolve('--ours','newtbl');" "t2_resolve"; then
  pass_name "schema_conflicts_resolve_refused"
else
  fail_name "schema_conflicts_resolve_refused"
fi
dl "$DB" "SELECT dolt_merge('--abort');" "t2_abort" >/dev/null
expect_eq "schema_conflicts_abort_clears" "0|0|0" \
  "$(dl "$DB" "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" "t2_cleared")"

DB="$TMPROOT/t3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t3"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
SELECT dolt_commit('-Am','feat_drop');
SELECT dolt_checkout('main');
DROP TABLE t;
SELECT dolt_commit('-Am','main_drop');
SQL
expect_merge_ok "table_both_delete" "$DB"

DB="$TMPROOT/t4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t4"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-Am','feat_modify');
SELECT dolt_checkout('main');
DROP TABLE t;
SELECT dolt_commit('-Am','main_drop');
SQL
expect_merge_conflict "table_modify_vs_delete" "$DB"

DB="$TMPROOT/t5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t5"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
UPDATE t SET v='b' WHERE id=1;
SELECT dolt_commit('-Am','feat_update');
SELECT dolt_checkout('main');
UPDATE t SET v='b' WHERE id=1;
SELECT dolt_commit('-Am','main_update');
SQL
expect_merge_ok "table_both_modify_identical" "$DB"

DB="$TMPROOT/t5b.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t5b"
CREATE TABLE anchor(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','feat_add_table');
SELECT dolt_checkout('main');
CREATE TABLE main_tbl(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','main_add_table');
SQL
expect_merge_ok "table_both_add_different_tables" "$DB"

echo ""

echo "--- Columns ---"

DB="$TMPROOT/c1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c1"
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'x';
SELECT dolt_commit('-Am','feat_add_col');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'x';
SELECT dolt_commit('-Am','main_add_col');
SQL
expect_merge_ok "col_both_add_identical" "$DB"

DB="$TMPROOT/c2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c2"
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN v TEXT;
SELECT dolt_commit('-Am','feat_add_text');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN v INTEGER;
SELECT dolt_commit('-Am','main_add_int');
SQL
expect_merge_conflict "col_both_add_different_type" "$DB"

DB="$TMPROOT/c3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c3"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN v;
SELECT dolt_commit('-Am','feat_drop');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN v;
SELECT dolt_commit('-Am','main_drop');
SQL
expect_merge_ok "col_both_delete" "$DB"

DB="$TMPROOT/c4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c4"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t DROP COLUMN v;
SELECT dolt_commit('-Am','feat_drop_v');
SELECT dolt_checkout('main');
ALTER TABLE t DROP COLUMN v;
ALTER TABLE t ADD COLUMN v TEXT NOT NULL DEFAULT 'modified';
SELECT dolt_commit('-Am','main_modify_v');
SQL
expect_merge_conflict "col_modify_vs_delete" "$DB"

DB="$TMPROOT/c5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c5"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN w INT DEFAULT 0;
SELECT dolt_commit('-Am','feat_add_w');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'b');
SELECT dolt_commit('-Am','main_insert');
SQL
expect_merge_ok "col_one_adds" "$DB"
W=$(dl "$DB" "SELECT w FROM t WHERE id=2;" "c5_check")
expect_eq "col_one_adds_default_filled" "0" "$W"

echo ""

echo "--- Foreign Keys ---"

DB="$TMPROOT/fk1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk1"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_add_fk');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_add_fk');
SQL
expect_merge_ok "fk_both_add_identical" "$DB"

DB="$TMPROOT/fk2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk2"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE parent2(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO parent VALUES(1);
INSERT INTO parent2 VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_add_fk_parent');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent2(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_add_fk_parent2');
SQL
expect_merge_conflict "fk_both_add_different" "$DB"

echo ""

echo "--- Indexes ---"

DB="$TMPROOT/ix1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix1"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX idx_v ON t(v);
SELECT dolt_commit('-Am','feat_add_idx');
SELECT dolt_checkout('main');
CREATE INDEX idx_v ON t(v);
SELECT dolt_commit('-Am','main_add_idx');
SQL
expect_merge_ok "idx_both_add_identical" "$DB"

DB="$TMPROOT/ix2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix2"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX idx_x ON t(v);
SELECT dolt_commit('-Am','feat_add_idx_v');
SELECT dolt_checkout('main');
CREATE INDEX idx_x ON t(w);
SELECT dolt_commit('-Am','main_add_idx_w');
SQL
expect_merge_conflict "idx_both_add_different" "$DB"

DB="$TMPROOT/ix3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix3"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE INDEX idx_v ON t(v);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP INDEX idx_v;
SELECT dolt_commit('-Am','feat_drop_idx');
SELECT dolt_checkout('main');
DROP INDEX idx_v;
SELECT dolt_commit('-Am','main_drop_idx');
SQL
expect_merge_ok "idx_both_delete" "$DB"

echo ""

echo "--- Check Constraints ---"

DB="$TMPROOT/ck1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck1"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','feat_add_check');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_add_check');
SQL
expect_merge_ok "check_both_add_identical" "$DB"

DB="$TMPROOT/ck2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck2"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','feat_add_check_pos');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 5));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_add_check_gt5');
SQL
expect_merge_conflict "check_both_add_different" "$DB"

echo "--- Tables (additional) ---"

DB="$TMPROOT/t6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t6"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN w INT DEFAULT 0;
SELECT dolt_commit('-Am','feat_add_w');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN x TEXT DEFAULT '';
SELECT dolt_commit('-Am','main_add_x');
SQL
expect_merge_ok "table_both_add_different_columns" "$DB"
COLS=$(dl "$DB" "PRAGMA table_info(t);" "t6_cols" | wc -l | tr -d ' ')
expect_eq "table_both_add_different_columns_col_count" "4" "$COLS"

DB="$TMPROOT/t7.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "t7"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','feat_notnull');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
SELECT dolt_commit('-Am','main_retype');
SQL
expect_merge_conflict "table_both_modify_same_col_differently" "$DB"

echo ""

echo "--- Columns (additional) ---"

DB="$TMPROOT/c6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c6"
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN v TEXT NOT NULL DEFAULT 'x';
SELECT dolt_commit('-Am','feat_add_v_notnull');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'x';
SELECT dolt_commit('-Am','main_add_v_nullable');
SQL
expect_merge_conflict "col_both_add_same_type_diff_constraints" "$DB"

DB="$TMPROOT/c7.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c7"
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'alpha';
SELECT dolt_commit('-Am','feat_add_v_alpha');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN v TEXT DEFAULT 'beta';
SELECT dolt_commit('-Am','main_add_v_beta');
SQL
expect_merge_conflict "col_both_add_same_type_diff_default" "$DB"

DB="$TMPROOT/c8.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c8"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','feat_add_notnull');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','main_add_notnull');
SQL
expect_merge_ok "col_both_modify_identical" "$DB"

DB="$TMPROOT/c9.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "c9"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','feat_add_notnull');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT DEFAULT 'x');
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','main_add_default');
SQL
expect_merge_conflict "col_both_modify_differently" "$DB"

echo ""

echo "--- Foreign Keys (additional) ---"

DB="$TMPROOT/fk3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk3"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_drop_fk');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_drop_fk');
SQL
expect_merge_ok "fk_both_delete" "$DB"

DB="$TMPROOT/fk4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk4"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_modify_fk');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_drop_fk');
SQL
expect_merge_conflict "fk_modify_vs_delete" "$DB"

DB="$TMPROOT/fk5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk5"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_add_cascade');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_add_cascade');
SQL
expect_merge_ok "fk_both_modify_identical" "$DB"

DB="$TMPROOT/fk6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk6"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_cascade');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_setnull');
SQL
expect_merge_conflict "fk_both_modify_differently" "$DB"

echo ""

echo "--- Indexes (additional) ---"

DB="$TMPROOT/ix4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix4"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
CREATE INDEX idx_v ON t(v);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP INDEX idx_v;
CREATE INDEX idx_v ON t(v, w);
SELECT dolt_commit('-Am','feat_modify_idx');
SELECT dolt_checkout('main');
DROP INDEX idx_v;
SELECT dolt_commit('-Am','main_drop_idx');
SQL
expect_merge_conflict "idx_modify_vs_delete" "$DB"

DB="$TMPROOT/ix5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix5"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
CREATE INDEX idx_v ON t(v);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP INDEX idx_v;
CREATE INDEX idx_v ON t(v, w);
SELECT dolt_commit('-Am','feat_expand_idx');
SELECT dolt_checkout('main');
DROP INDEX idx_v;
CREATE INDEX idx_v ON t(v, w);
SELECT dolt_commit('-Am','main_expand_idx');
SQL
expect_merge_ok "idx_both_modify_identical" "$DB"

DB="$TMPROOT/ix6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix6"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
CREATE INDEX idx_v ON t(v);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP INDEX idx_v;
CREATE INDEX idx_v ON t(v, w);
SELECT dolt_commit('-Am','feat_idx_vw');
SELECT dolt_checkout('main');
DROP INDEX idx_v;
CREATE INDEX idx_v ON t(w);
SELECT dolt_commit('-Am','main_idx_w');
SQL
expect_merge_conflict "idx_both_modify_differently" "$DB"

echo ""

echo "--- Check Constraints (additional) ---"

DB="$TMPROOT/ck3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck3"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','feat_drop_check');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_drop_check');
SQL
expect_merge_ok "check_both_delete" "$DB"

DB="$TMPROOT/ck4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck4"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 5));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','feat_modify_check');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_drop_check');
SQL
expect_merge_conflict "check_modify_vs_delete" "$DB"

DB="$TMPROOT/ck5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck5"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 5));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','feat_tighten');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 5));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_tighten');
SQL
expect_merge_ok "check_both_modify_identical" "$DB"

DB="$TMPROOT/ck6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck6"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 5));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','feat_check_gt5');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v >= 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_check_ge0');
SQL
expect_merge_conflict "check_both_modify_differently" "$DB"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
