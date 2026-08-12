#!/bin/bash

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

dl_setup() {
  local db="$1" tag="$2"
  local sql dt_sql repo dl_rc dt_rc
  sql=$(cat)
  repo=$(dt_repo_for_db "$db")
  mkdir -p "$repo"
  if [ ! -d "$repo/.dolt" ]; then
    (cd "$repo" && vc_oracle_init_repo)
  fi

  printf '%s\n' "$sql" | "$DOLTLITE" "$db" \
    >"$TMPROOT/$tag.out" 2>"$TMPROOT/$tag.err"
  dl_rc=$?
  dt_sql=$(printf '%s\n' "$sql" | schema_sql_for_dolt)
  (cd "$repo" && printf '%s\n' "$dt_sql" | "$DOLT" sql -c) \
    >"$TMPROOT/$tag.dt.out" 2>"$TMPROOT/$tag.dt.err"
  dt_rc=$?

  if output_is_error "$dl_rc" "$TMPROOT/$tag.out" "$TMPROOT/$tag.err" \
     || output_is_error "$dt_rc" "$TMPROOT/$tag.dt.out" "$TMPROOT/$tag.dt.err"; then
    fail_name "${tag}_setup"
    echo "    setup failed: doltlite rc=$dl_rc, dolt rc=$dt_rc"
    echo "    doltlite stderr: $(head -2 "$TMPROOT/$tag.err")"
    echo "    dolt stderr: $(head -2 "$TMPROOT/$tag.dt.err")"
  fi
}

dt_repo_for_db() {
  local db="$1" base
  base=$(basename "$db")
  printf '%s/dolt_%s\n' "$TMPROOT" "${base%.db}"
}

schema_sql_for_dolt() {
  local sql
  sql=$(cat)
  vc_oracle_translate_for_dolt "$sql" \
    | sed -E 's/DROP INDEX ([a-zA-Z0-9_]+);/DROP INDEX \1 ON t;/g'
}

output_is_error() {
  local rc="$1" out="$2" err="$3"
  [ "$rc" -ne 0 ] || grep -qiE '(^|[^a-z])(error|failed)([ :]|$)' "$out" "$err" 2>/dev/null
}

run_merge_outcome() {
  local engine="$1" db="$2" tag="$3"
  local rc repo
  if [ "$engine" = "doltlite" ]; then
    "$DOLTLITE" "$db" "SELECT dolt_merge('feat');" \
      >"$TMPROOT/$tag.dl.out" 2>"$TMPROOT/$tag.dl.err"
    rc=$?
    if output_is_error "$rc" "$TMPROOT/$tag.dl.out" "$TMPROOT/$tag.dl.err"; then
      printf 'conflict\n'
    else
      printf 'ok\n'
    fi
  else
    repo=$(dt_repo_for_db "$db")
    (cd "$repo" && "$DOLT" sql -r csv -q "CALL dolt_merge('feat');") \
      >"$TMPROOT/$tag.dt.out" 2>"$TMPROOT/$tag.dt.err"
    rc=$?
    if output_is_error "$rc" "$TMPROOT/$tag.dt.out" "$TMPROOT/$tag.dt.err" \
       || tail -n +2 "$TMPROOT/$tag.dt.out" | grep -qi 'conflict'; then
      printf 'conflict\n'
    else
      printf 'ok\n'
    fi
  fi
}

expect_merge_outcome() {
  local name="$1" db="$2" want="$3" dl_got dt_got
  dl_got=$(run_merge_outcome doltlite "$db" "$name")
  dt_got=$(run_merge_outcome dolt "$db" "$name")
  if [ "$dl_got" = "$want" ] && [ "$dt_got" = "$want" ]; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    expected $want; doltlite=$dl_got dolt=$dt_got"
    echo "    doltlite: $(cat "$TMPROOT/$name.dl.out" "$TMPROOT/$name.dl.err" 2>/dev/null | head -2)"
    echo "    dolt: $(cat "$TMPROOT/$name.dt.out" "$TMPROOT/$name.dt.err" 2>/dev/null | head -2)"
  fi
}

expect_merge_ok() { expect_merge_outcome "$1" "$2" ok; }
expect_merge_conflict() { expect_merge_outcome "$1" "$2" conflict; }

run_dual_command_outcome() {
  local name="$1" db="$2" dl_sql="$3" dt_sql="$4" want="$5"
  local repo dl_rc dt_rc dl_got=ok dt_got=ok
  repo=$(dt_repo_for_db "$db")
  printf '%s\n' "$dl_sql" | "$DOLTLITE" "$db" \
    >"$TMPROOT/$name.dl.out" 2>"$TMPROOT/$name.dl.err"
  dl_rc=$?
  (cd "$repo" && printf '%s\n' "$dt_sql" | "$DOLT" sql) \
    >"$TMPROOT/$name.dt.out" 2>"$TMPROOT/$name.dt.err"
  dt_rc=$?
  if output_is_error "$dl_rc" "$TMPROOT/$name.dl.out" "$TMPROOT/$name.dl.err"; then dl_got=error; fi
  if output_is_error "$dt_rc" "$TMPROOT/$name.dt.out" "$TMPROOT/$name.dt.err"; then dt_got=error; fi
  if [ "$dl_got" = "$want" ] && [ "$dt_got" = "$want" ]; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    expected $want; doltlite=$dl_got dolt=$dt_got"
  fi
}

query_doltlite_scalar() {
  "$DOLTLITE" "$1" "$2" 2>"$TMPROOT/$3.dl.query.err" | tr -d '\r"'
}

query_dolt_scalar() {
  local repo
  repo=$(dt_repo_for_db "$1")
  (cd "$repo" && "$DOLT" sql -r csv -q "$2") \
    2>"$TMPROOT/$3.dt.query.err" | tail -n +2 | tr -d '\r"'
}

expect_dual_value() {
  local name="$1" db="$2" want="$3" dl_sql="$4" dt_sql="$5"
  local dl_got dt_got
  dl_got=$(query_doltlite_scalar "$db" "$dl_sql" "$name")
  dt_got=$(query_dolt_scalar "$db" "$dt_sql" "$name")
  if [ "$dl_got" = "$want" ] && [ "$dt_got" = "$want" ]; then
    pass_name "$name"
  else
    fail_name "$name"
    echo "    expected: |$want|"
    echo "    doltlite: |$dl_got|"
    echo "    dolt:     |$dt_got|"
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
expect_dual_value "schema_conflicts_autocommit_rollback" "$DB" "0|0|0" \
  "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" \
  "SELECT CONCAT((SELECT count(*) FROM dolt_schema_conflicts), '|', (SELECT count(*) FROM dolt_conflicts), '|', (SELECT count(*) FROM dolt_status WHERE status='schema conflict'));"

DB="$TMPROOT/sc1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "sc1"
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-Am','feat_schema');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN extra INTEGER;
SELECT dolt_commit('-Am','main_schema');
SQL
expect_merge_conflict "schema_conflict_existing_column_autocommit" "$DB"
expect_dual_value "schema_conflicts_existing_autocommit_rollback" "$DB" "0|0|0" \
  "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" \
  "SELECT CONCAT((SELECT count(*) FROM dolt_schema_conflicts), '|', (SELECT count(*) FROM dolt_conflicts), '|', (SELECT count(*) FROM dolt_status WHERE status='schema conflict'));"
printf '%s\n' "BEGIN; SELECT dolt_merge('feat'); COMMIT;" \
  | "$DOLTLITE" "$DB" >"$TMPROOT/schema_conflicts_persist.dl.out" \
      2>"$TMPROOT/schema_conflicts_persist.dl.err" || true
DT_T2=$(dt_repo_for_db "$DB")
(cd "$DT_T2" && printf '%s\n' \
  "SET @@dolt_allow_commit_conflicts=1; SET autocommit=0; CALL dolt_merge('feat'); COMMIT;" \
  | "$DOLT" sql -c) >"$TMPROOT/schema_conflicts_persist.dt.out" \
      2>"$TMPROOT/schema_conflicts_persist.dt.err" || true
expect_dual_value "schema_conflicts_transaction_state" "$DB" "1|1|0|1" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT coalesce(sum(num_conflicts),-1) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" \
  "SELECT CONCAT((SELECT count(*) FROM dolt_schema_conflicts), '|', (SELECT count(*) FROM dolt_conflicts), '|', (SELECT coalesce(sum(num_conflicts),-1) FROM dolt_conflicts), '|', (SELECT count(*) FROM dolt_status WHERE status='schema conflict'));"
expect_dual_value "schema_conflicts_schema_rows" "$DB" "t|1|1|1" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT table_name || '|' || (base_schema LIKE '%CREATE TABLE%') || '|' || (our_schema LIKE '%extra%') || '|' || (their_schema LIKE '%extra%') FROM dolt_schema_conflicts;" \
  "SELECT CONCAT(table_name, '|', base_schema LIKE '%CREATE TABLE%', '|', our_schema LIKE '%extra%', '|', their_schema LIKE '%extra%') FROM dolt_schema_conflicts;"
run_dual_command_outcome "schema_conflicts_resolve_refused" "$DB" \
  "BEGIN; SELECT dolt_merge('feat'); SELECT dolt_conflicts_resolve('--ours','t');" \
  "CALL dolt_conflicts_resolve('--ours','t');" error
# No outcome comparison for the abort itself: DoltLite can only reach an active
# merge inside the transaction that created it, and that transaction necessarily
# carries the expected conflict error, which output_is_error cannot tell apart
# from a real failure. The abort is compared by its effect in
# schema_conflicts_abort_clears below, and exercised directly in
# test/doltlite_schema_merge.sh. Dolt still holds a committed conflicted merge at
# this point, so it needs the abort run for the next comparison to line up.
DT_AB=$(dt_repo_for_db "$DB")
(cd "$DT_AB" && printf '%s\n' "CALL dolt_merge('--abort');" | "$DOLT" sql) \
  >"$TMPROOT/schema_conflicts_abort.dt.out" 2>"$TMPROOT/schema_conflicts_abort.dt.err" || true
expect_dual_value "schema_conflicts_abort_clears" "$DB" "0|0|0" \
  "SELECT (SELECT count(*) FROM dolt_schema_conflicts) || '|' || (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT count(*) FROM dolt_status WHERE status='schema conflict');" \
  "SELECT CONCAT((SELECT count(*) FROM dolt_schema_conflicts), '|', (SELECT count(*) FROM dolt_conflicts), '|', (SELECT count(*) FROM dolt_status WHERE status='schema conflict'));"

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
expect_dual_value "col_one_adds_default_filled" "$DB" "0" \
  "SELECT w FROM t WHERE id=2;" "SELECT w FROM t WHERE id=2;"

DB="$TMPROOT/ccase.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ccase"
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN same_col TEXT;
SELECT dolt_commit('-Am','feat_add_text');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN same_col text;
SELECT dolt_commit('-Am','main_add_text');
SQL
expect_merge_ok "col_both_add_equivalent_type_case" "$DB"
expect_dual_value "col_both_add_equivalent_type_case_once" "$DB" "1" \
  "SELECT count(*) FROM pragma_table_info('t') WHERE name='same_col';" \
  "SELECT count(*) FROM information_schema.columns WHERE table_schema=database() AND table_name='t' AND column_name='same_col';"

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
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_add_fk');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
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
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_add_fk_parent');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent2(id));
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_add_fk_parent2');
SQL
expect_merge_conflict "fk_both_add_different" "$DB"

echo ""

echo "--- Indexes ---"

DB="$TMPROOT/ix1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix1"
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64));
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
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), w INT);
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
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64));
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
expect_dual_value "table_both_add_different_columns_col_count" "$DB" "4" \
  "SELECT count(*) FROM pragma_table_info('t');" \
  "SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='t';"

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
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
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
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_modify_fk');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_drop_fk');
SQL
expect_merge_ok "fk_modify_vs_delete" "$DB"
expect_dual_value "fk_modify_vs_delete_keeps_delete" "$DB" "0" \
  "SELECT count(*) FROM pragma_foreign_key_list('child');" \
  "SELECT count(*) FROM information_schema.key_column_usage WHERE table_schema=database() AND table_name='child' AND referenced_table_name IS NOT NULL;"

DB="$TMPROOT/fk5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk5"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_add_cascade');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_add_cascade');
SQL
expect_merge_ok "fk_both_modify_identical" "$DB"

DB="$TMPROOT/fk6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk6"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','feat_cascade');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_setnull');
SQL
expect_merge_conflict "fk_both_modify_differently" "$DB"

DB="$TMPROOT/fk7.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk7"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
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
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_modify_fk');
SQL
expect_merge_ok "fk_delete_vs_modify_reverse" "$DB"
expect_dual_value "fk_delete_vs_modify_reverse_keeps_delete" "$DB" "0" \
  "SELECT count(*) FROM pragma_foreign_key_list('child');" \
  "SELECT count(*) FROM information_schema.key_column_usage WHERE table_schema=database() AND table_name='child' AND referenced_table_name IS NOT NULL;"

DB="$TMPROOT/fk8.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk8"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
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
ALTER TABLE child ADD COLUMN main_col TEXT DEFAULT 'seed';
UPDATE child SET main_col='main' WHERE id=1;
SELECT dolt_commit('-Am','main_add_column');
SQL
expect_merge_ok "fk_delete_vs_column_add" "$DB"
expect_dual_value "fk_delete_vs_column_add_composes" "$DB" "0|main" \
  "SELECT (SELECT count(*) FROM pragma_foreign_key_list('child')) || '|' || main_col FROM child WHERE id=1;" \
  "SELECT CONCAT((SELECT count(*) FROM information_schema.key_column_usage WHERE table_schema=database() AND table_name='child' AND referenced_table_name IS NOT NULL), '|', main_col) FROM child WHERE id=1;"

DB="$TMPROOT/fk9.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "fk9"
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY(pid) REFERENCES parent(id));
INSERT INTO parent VALUES(1);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE child ADD COLUMN feat_col TEXT DEFAULT 'seed';
UPDATE child SET feat_col='feat' WHERE id=1;
SELECT dolt_commit('-Am','feat_add_column');
SELECT dolt_checkout('main');
DROP TABLE child;
CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER);
INSERT INTO child VALUES(1,1);
SELECT dolt_commit('-Am','main_drop_fk');
SQL
expect_merge_ok "fk_delete_vs_column_add_reverse" "$DB"
expect_dual_value "fk_delete_vs_column_add_reverse_composes" "$DB" "0|feat" \
  "SELECT (SELECT count(*) FROM pragma_foreign_key_list('child')) || '|' || feat_col FROM child WHERE id=1;" \
  "SELECT CONCAT((SELECT count(*) FROM information_schema.key_column_usage WHERE table_schema=database() AND table_name='child' AND referenced_table_name IS NOT NULL), '|', feat_col) FROM child WHERE id=1;"

echo ""

echo "--- Indexes (additional) ---"

DB="$TMPROOT/ix4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix4"
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), w INT);
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
expect_merge_ok "idx_modify_vs_delete" "$DB"
expect_dual_value "idx_modify_vs_delete_keeps_modify" "$DB" "2" \
  "SELECT count(*) FROM pragma_index_info('idx_v');" \
  "SELECT count(*) FROM information_schema.statistics WHERE table_schema=database() AND table_name='t' AND index_name='idx_v';"

DB="$TMPROOT/ix5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix5"
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), w INT);
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
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), w INT);
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

DB="$TMPROOT/ix7.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ix7"
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(64), w INT);
CREATE INDEX idx_v ON t(v);
INSERT INTO t VALUES(1,'a',10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP INDEX idx_v;
SELECT dolt_commit('-Am','feat_drop_idx');
SELECT dolt_checkout('main');
DROP INDEX idx_v;
CREATE INDEX idx_v ON t(v, w);
SELECT dolt_commit('-Am','main_modify_idx');
SQL
expect_merge_ok "idx_delete_vs_modify_reverse" "$DB"
expect_dual_value "idx_delete_vs_modify_reverse_keeps_modify" "$DB" "2" \
  "SELECT count(*) FROM pragma_index_info('idx_v');" \
  "SELECT count(*) FROM information_schema.statistics WHERE table_schema=database() AND table_name='t' AND index_name='idx_v';"

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
expect_merge_ok "check_modify_vs_delete" "$DB"
expect_dual_value "check_modify_vs_delete_keeps_modify" "$DB" "1" \
  "SELECT count(*) FROM sqlite_master WHERE name='t' AND sql LIKE '%CHECK(v > 5)%';" \
  "SELECT count(*) FROM information_schema.check_constraints WHERE check_clause LIKE '%> 5%';"

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

DB="$TMPROOT/ck7.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck7"
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
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 5));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_modify_check');
SQL
expect_merge_ok "check_delete_vs_modify_reverse" "$DB"
expect_dual_value "check_delete_vs_modify_reverse_keeps_modify" "$DB" "1" \
  "SELECT count(*) FROM sqlite_master WHERE name='t' AND sql LIKE '%CHECK(v > 5)%';" \
  "SELECT count(*) FROM information_schema.check_constraints WHERE check_clause LIKE '%> 5%';"

DB="$TMPROOT/ck8.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck8"
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
ALTER TABLE t ADD COLUMN main_col TEXT DEFAULT 'seed';
UPDATE t SET main_col='main' WHERE id=1;
SELECT dolt_commit('-Am','main_add_column');
SQL
expect_merge_ok "check_delete_vs_column_add" "$DB"
expect_dual_value "check_delete_vs_column_add_composes" "$DB" "0|main" \
  "SELECT (SELECT count(*) FROM sqlite_master WHERE name='t' AND sql LIKE '%CHECK%') || '|' || main_col FROM t WHERE id=1;" \
  "SELECT CONCAT((SELECT count(*) FROM information_schema.check_constraints), '|', main_col) FROM t WHERE id=1;"

DB="$TMPROOT/ck9.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "ck9"
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v > 0));
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN feat_col TEXT DEFAULT 'seed';
UPDATE t SET feat_col='feat' WHERE id=1;
SELECT dolt_commit('-Am','feat_add_column');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_commit('-Am','main_drop_check');
SQL
expect_merge_ok "check_delete_vs_column_add_reverse" "$DB"
expect_dual_value "check_delete_vs_column_add_reverse_composes" "$DB" "0|feat" \
  "SELECT (SELECT count(*) FROM sqlite_master WHERE name='t' AND sql LIKE '%CHECK%') || '|' || feat_col FROM t WHERE id=1;" \
  "SELECT CONCAT((SELECT count(*) FROM information_schema.check_constraints), '|', feat_col) FROM t WHERE id=1;"

echo ""
echo "--- Generated Columns ---"

DB="$TMPROOT/gc1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc1"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
INSERT INTO t(id,a) VALUES(2,7);
SELECT dolt_commit('-Am','feat_generated');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,11);
SELECT dolt_commit('-Am','main_row');
SQL
expect_merge_ok "generated_add_vs_insert" "$DB"
expect_dual_value "generated_add_vs_insert_values" "$DB" "1:5:10,2:7:14,3:11:22" \
  "SELECT group_concat(id || ':' || a || ':' || doubled, ',') FROM (SELECT id,a,doubled FROM t ORDER BY id);" \
  "SELECT GROUP_CONCAT(CONCAT(id, ':', a, ':', doubled) ORDER BY id SEPARATOR ',') FROM t;"

DB="$TMPROOT/gc2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc2"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
INSERT INTO t(id,a) VALUES(2,7);
SELECT dolt_commit('-Am','feat_generated');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
INSERT INTO t(id,a) VALUES(3,11);
SELECT dolt_commit('-Am','main_generated');
SQL
expect_merge_ok "generated_both_add_identical" "$DB"
expect_dual_value "generated_both_add_identical_values" "$DB" "1:10,2:14,3:22" \
  "SELECT group_concat(id || ':' || doubled, ',') FROM (SELECT id,doubled FROM t ORDER BY id);" \
  "SELECT GROUP_CONCAT(CONCAT(id, ':', doubled) ORDER BY id SEPARATOR ',') FROM t;"

DB="$TMPROOT/gc3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc3"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN computed INT AS (a * 2) VIRTUAL;
INSERT INTO t(id,a) VALUES(2,7);
SELECT dolt_commit('-Am','feat_generated');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN computed INT GENERATED ALWAYS AS (a * 3) VIRTUAL;
INSERT INTO t(id,a) VALUES(3,11);
SELECT dolt_commit('-Am','main_generated');
SQL
expect_merge_ok "generated_both_add_different_expression_ours_wins" "$DB"
expect_dual_value "generated_both_add_different_expression_ours_value" "$DB" \
  "1:15,2:21,3:33" \
  "SELECT group_concat(id || ':' || computed, ',') FROM (SELECT id,computed FROM t ORDER BY id);" \
  "SELECT GROUP_CONCAT(CONCAT(id, ':', computed) ORDER BY id SEPARATOR ',') FROM t;"

DB="$TMPROOT/gc4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc4"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
SELECT dolt_commit('-Am','feat_doubled');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN tripled INT GENERATED ALWAYS AS (a * 3) VIRTUAL;
SELECT dolt_commit('-Am','main_tripled');
SQL
expect_merge_ok "generated_both_add_different_columns" "$DB"
expect_dual_value "generated_both_add_different_columns_values" "$DB" "10|15" \
  "SELECT doubled || '|' || tripled FROM t WHERE id=1;" \
  "SELECT CONCAT(doubled, '|', tripled) FROM t WHERE id=1;"

DB="$TMPROOT/gc5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc5"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
CREATE INDEX idx_doubled ON t(doubled);
INSERT INTO t(id,a) VALUES(2,7);
SELECT dolt_commit('-Am','feat_generated_index');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,11);
SELECT dolt_commit('-Am','main_row');
SQL
expect_merge_ok "generated_index_vs_insert" "$DB"
expect_dual_value "generated_index_vs_insert_seek" "$DB" "2" \
  "SELECT id FROM t WHERE doubled=14;" \
  "SELECT id FROM t WHERE doubled=14;"

DB="$TMPROOT/gc6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc6"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, label TEXT);
INSERT INTO t VALUES(1,5,'one');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
INSERT INTO t(id,a,label) VALUES(2,7,'two');
SELECT dolt_commit('-Am','feat_generated');
SELECT dolt_checkout('main');
ALTER TABLE t RENAME COLUMN label TO note;
INSERT INTO t(id,a,note) VALUES(3,11,'three');
SELECT dolt_commit('-Am','main_rename_unrelated');
SQL
expect_merge_ok "generated_add_vs_unrelated_rename" "$DB"
expect_dual_value "generated_add_vs_unrelated_rename_values" "$DB" \
  "1:one:10,2:two:14,3:three:22" \
  "SELECT group_concat(id || ':' || note || ':' || doubled, ',') FROM (SELECT id,note,doubled FROM t ORDER BY id);" \
  "SELECT GROUP_CONCAT(CONCAT(id, ':', note, ':', doubled) ORDER BY id SEPARATOR ',') FROM t;"

DB="$TMPROOT/gc6r.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc6r"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, label TEXT);
INSERT INTO t VALUES(1,5,'one');
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t RENAME COLUMN label TO note;
INSERT INTO t(id,a,note) VALUES(2,7,'two');
SELECT dolt_commit('-Am','feat_rename_unrelated');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN doubled INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
INSERT INTO t(id,a,label) VALUES(3,11,'three');
SELECT dolt_commit('-Am','main_generated');
SQL
expect_merge_ok "generated_add_vs_unrelated_rename_reverse" "$DB"
expect_dual_value "generated_add_vs_unrelated_rename_reverse_values" "$DB" \
  "1:one:10,2:two:14,3:three:22" \
  "SELECT group_concat(id || ':' || note || ':' || doubled, ',') FROM (SELECT id,note,doubled FROM t ORDER BY id);" \
  "SELECT GROUP_CONCAT(CONCAT(id, ':', note, ':', doubled) ORDER BY id SEPARATOR ',') FROM t;"

DB="$TMPROOT/gc7.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc7"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN x INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
SELECT dolt_commit('-Am','feat_generated');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN x INT;
SELECT dolt_commit('-Am','main_plain');
SQL
expect_merge_ok "generated_vs_plain_ours_plain" "$DB"
expect_dual_value "generated_vs_plain_ours_plain_value" "$DB" "null" \
  "SELECT CASE WHEN x IS NULL THEN 'null' ELSE CAST(x AS TEXT) END FROM t WHERE id=1;" \
  "SELECT CASE WHEN x IS NULL THEN 'null' ELSE CAST(x AS CHAR) END FROM t WHERE id=1;"

DB="$TMPROOT/gc8.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "gc8"
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT);
INSERT INTO t VALUES(1,5);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
ALTER TABLE t ADD COLUMN x INT;
INSERT INTO t(id,a,x) VALUES(2,7,99);
SELECT dolt_commit('-Am','feat_plain');
SELECT dolt_checkout('main');
ALTER TABLE t ADD COLUMN x INT GENERATED ALWAYS AS (a * 2) VIRTUAL;
INSERT INTO t(id,a) VALUES(3,11);
SELECT dolt_commit('-Am','main_generated');
SQL
expect_merge_ok "generated_vs_plain_ours_generated" "$DB"
expect_dual_value "generated_vs_plain_ours_generated_value" "$DB" \
  "1:10,2:14,3:22" \
  "SELECT group_concat(id || ':' || x, ',') FROM (SELECT id,x FROM t ORDER BY id);" \
  "SELECT GROUP_CONCAT(CONCAT(id, ':', x) ORDER BY id SEPARATOR ',') FROM t;"

echo ""
echo "--- Primary key changes ---"

DB="$TMPROOT/pk1.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "pk1"
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('AAAAA',1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(pk BIGINT PRIMARY KEY, v INT);
INSERT INTO t VALUES(5,50);
SELECT dolt_commit('-Am','feat_recreate');
SELECT dolt_checkout('main');
INSERT INTO t VALUES('BBBBB',2);
SELECT dolt_commit('-Am','main_rows');
SQL
expect_merge_conflict "pk_change_vs_row_change" "$DB"
expect_dual_value "pk_change_vs_row_change_local_intact" "$DB" "2" \
  "SELECT count(*) FROM t;" "SELECT count(*) FROM t;"

DB="$TMPROOT/pk2.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "pk2"
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('AAAAA',1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(pk BIGINT PRIMARY KEY, v INT);
INSERT INTO t VALUES(5,50);
SELECT dolt_commit('-Am','feat_recreate');
SELECT dolt_checkout('main');
CREATE TABLE other(a BIGINT PRIMARY KEY);
SELECT dolt_commit('-Am','main_other');
SQL
expect_merge_conflict "pk_change_vs_untouched_table" "$DB"

DB="$TMPROOT/pk3.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "pk3"
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('AAAAA',1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(pk BIGINT PRIMARY KEY, v INT);
INSERT INTO t VALUES(5,50);
SELECT dolt_commit('-Am','feat_recreate');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(pk VARBINARY(16) PRIMARY KEY, v INT);
INSERT INTO t VALUES(X'AB',7);
SELECT dolt_commit('-Am','main_recreate');
SQL
expect_merge_conflict "pk_change_both_sides_differ" "$DB"

DB="$TMPROOT/pk4.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "pk4"
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('AAAAA',1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(pk BIGINT PRIMARY KEY, v INT);
INSERT INTO t VALUES(5,50);
SELECT dolt_commit('-Am','feat_recreate');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(pk BIGINT PRIMARY KEY, v INT);
INSERT INTO t VALUES(7,70);
SELECT dolt_commit('-Am','main_recreate');
SQL
expect_merge_conflict "pk_change_agreed_but_differs_from_ancestor" "$DB"

DB="$TMPROOT/pk5.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "pk5"
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('AAAAA',1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE t;
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('feat',50);
SELECT dolt_commit('-Am','feat_recreate');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('main',7);
SELECT dolt_commit('-Am','main_recreate');
SQL
expect_merge_ok "identical_recreate_merges" "$DB"
expect_dual_value "identical_recreate_rows" "$DB" "feat:50,main:7" \
  "SELECT group_concat(pk || ':' || v, ',') FROM (SELECT pk,v FROM t ORDER BY pk);" \
  "SELECT GROUP_CONCAT(CONCAT(pk, ':', v) ORDER BY pk SEPARATOR ',') FROM t;"

DB="$TMPROOT/pk6.db"; rm -f "$DB"
cat <<'SQL' | dl_setup "$DB" "pk6"
CREATE TABLE t(pk VARCHAR(32) PRIMARY KEY, v INT);
INSERT INTO t VALUES('AAAAA',1);
SELECT dolt_commit('-Am','ancestor');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(id BIGINT PRIMARY KEY);
SELECT dolt_commit('-Am','feat_other');
SELECT dolt_checkout('main');
DROP TABLE t;
CREATE TABLE t(pk BIGINT PRIMARY KEY, v INT);
INSERT INTO t VALUES(7,70);
SELECT dolt_commit('-Am','main_recreate');
SQL
expect_merge_ok "pk_change_ours_only_theirs_untouched" "$DB"
expect_dual_value "pk_change_ours_only_kept" "$DB" "7:70" \
  "SELECT pk || ':' || v FROM t;" "SELECT CONCAT(pk, ':', v) FROM t;"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
