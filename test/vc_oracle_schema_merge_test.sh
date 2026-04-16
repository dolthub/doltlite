#!/bin/bash
#
# Schema merge oracle — test every scenario from the Dolt spec:
# https://docs.dolthub.com/concepts/dolt/git/conflicts#schema
#
# Categories: Tables, Columns, Foreign Keys, Indexes, Check Constraints.
# Each case has two branches that diverge from a common ancestor and
# merge. The test checks whether the merge auto-resolves, produces
# a schema conflict error, or produces a data conflict.
#
# Usage: bash vc_oracle_schema_merge_test.sh [path/to/doltlite]
#

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

# Run SQL against a db, capture stdout+stderr
dl() {
  local db="$1" sql="$2" tag="$3"
  "$DOLTLITE" "$db" "$sql" 2>"$TMPROOT/$tag.err"
}

# Bulk setup via stdin
dl_setup() {
  local db="$1" tag="$2"
  "$DOLTLITE" "$db" >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
}

# Returns 0 (true) if the command produced an error
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

# Expect merge succeeds (no error)
expect_merge_ok() {
  local name="$1" db="$2"
  if dl_errors "$db" "SELECT dolt_merge('feat');" "$name"; then
    fail_name "$name"
    echo "    merge errored: $(cat $TMPROOT/$name.out $TMPROOT/$name.err 2>/dev/null | head -3)"
  else
    pass_name "$name"
  fi
}

# Expect merge fails with schema conflict
expect_merge_conflict() {
  local name="$1" db="$2"
  if dl_errors "$db" "SELECT dolt_merge('feat');" "$name"; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    merge succeeded but expected conflict"
  fi
}

# Helper: create a fresh db with a base table, commit, branch 'feat'
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

# ════════════════════════════════════════════════════
# TABLES
# ════════════════════════════════════════════════════
echo "--- Tables ---"

# T1: Both add same table with identical schema → auto-resolve
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

# T2: Both add same table with different schema → conflict
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

# T3: Both delete same table → auto-resolve
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

# T4: One modifies, one deletes → conflict
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

# T5: Both modify same table, identical result → auto-resolve
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

echo ""

# ════════════════════════════════════════════════════
# COLUMNS
# ════════════════════════════════════════════════════
echo "--- Columns ---"

# C1: Both add column with same definition → auto-resolve
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

# C2: Both add column with different type → conflict
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

# C3: Both delete same column → auto-resolve
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

# C4: One modifies, one deletes column → conflict
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

# C5: One side adds a column, other doesn't touch → auto-resolve
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

# ════════════════════════════════════════════════════
# FOREIGN KEYS
# ════════════════════════════════════════════════════
echo "--- Foreign Keys ---"

# FK1: Both add same FK with identical definition → auto-resolve
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

# FK2: Both add FK with different definition → conflict
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

# ════════════════════════════════════════════════════
# INDEXES
# ════════════════════════════════════════════════════
echo "--- Indexes ---"

# IX1: Both add same index → auto-resolve
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

# IX2: Both add index with different definitions → conflict
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

# IX3: Both delete same index → auto-resolve
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

# ════════════════════════════════════════════════════
# CHECK CONSTRAINTS
# ════════════════════════════════════════════════════
echo "--- Check Constraints ---"

# CK1: Both add same CHECK → auto-resolve
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

# CK2: Both add different CHECK → conflict
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

# ════════════════════════════════════════════════════
# TABLES (additional)
# ════════════════════════════════════════════════════
echo "--- Tables (additional) ---"

# T6: Both add different columns to same table → auto-resolve (different names merge cleanly)
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
# Verify both columns are present after merge
COLS=$(dl "$DB" "PRAGMA table_info(t);" "t6_cols" | wc -l | tr -d ' ')
expect_eq "table_both_add_different_columns_col_count" "4" "$COLS"

# T7: Both modify same column differently → conflict
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

# ════════════════════════════════════════════════════
# COLUMNS (additional)
# ════════════════════════════════════════════════════
echo "--- Columns (additional) ---"

# C6: Both add column, same type, different constraints → conflict
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

# C7: Both add column, same type+constraints, different default data → conflict
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

# C8: Both modify column identically → auto-resolve
# (SQLite ALTER TABLE doesn't support ALTER COLUMN, so we simulate via
# DROP TABLE + recreate with modified column. Both sides do the same change.)
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

# C9: Both modify column differently → conflict
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

# ════════════════════════════════════════════════════
# FOREIGN KEYS (additional)
# ════════════════════════════════════════════════════
echo "--- Foreign Keys (additional) ---"

# FK3: Both delete same FK → auto-resolve
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

# FK4: One modifies FK, other deletes → conflict
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

# FK5: Both modify FK identically → auto-resolve
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

# FK6: Both modify FK differently → conflict
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

# ════════════════════════════════════════════════════
# INDEXES (additional)
# ════════════════════════════════════════════════════
echo "--- Indexes (additional) ---"

# IX4: One modifies index, other deletes → conflict
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

# IX5: Both modify index identically → auto-resolve
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

# IX6: Both modify index differently → conflict
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

# ════════════════════════════════════════════════════
# CHECK CONSTRAINTS (additional)
# ════════════════════════════════════════════════════
echo "--- Check Constraints (additional) ---"

# CK3: Both delete same CHECK → auto-resolve
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

# CK4: One modifies CHECK, other deletes → conflict
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

# CK5: Both modify CHECK identically → auto-resolve
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

# CK6: Both modify CHECK differently → conflict
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
