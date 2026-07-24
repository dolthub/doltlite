#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

oracle() {
  local name="$1" setup="$2" from_ref="$3" to_ref="$4" tbl="${5:-}" allow_empty="${6:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local args
  if [ -n "$tbl" ]; then
    args="'$from_ref','$to_ref','$tbl'"
  else
    args="'$from_ref','$to_ref'"
  fi

  local q="SELECT CONCAT('ROW|', from_table_name, '|', to_table_name, '|', \
            CASE WHEN from_create_statement IS NULL OR from_create_statement='' THEN 'N' ELSE 'Y' END, '|', \
            CASE WHEN to_create_statement   IS NULL OR to_create_statement=''   THEN 'N' ELSE 'Y' END \
          ) FROM dolt_schema_diff($args) ORDER BY from_table_name, to_table_name;"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ROW|' \
           | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ROW|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_error() {
  local name="$1" setup="$2" q="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  local dl_sql
  dl_sql=$(printf "%s\n%s\n" "$setup" "$q")
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_sql"
  dl_rc=$?

  local dolt_setup
  local dt_rc
  local dt_sql
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dt_sql=$(printf "%s\n%s\n" "$dolt_setup" "$q")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dt_sql"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_query() {
  local name="$1" setup="$2" q="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^ROW|' \
           | sort)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$q"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^ROW|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_schema_diff ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- added table ---"

oracle "added_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "HEAD~1" "HEAD"

echo "--- dropped table ---"

oracle "dropped_table" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t');
" "HEAD~1" "HEAD"

oracle "drop_one_of_two_tables" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t');
" "HEAD~1" "HEAD"

oracle "drop_multiple_in_one_commit" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
CREATE TABLE w(id INTEGER PRIMARY KEY, y TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u_w');
DROP TABLE t;
DROP TABLE u;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t_u');
" "HEAD~1" "HEAD"

oracle "drop_table_with_data" "
$SEED
INSERT INTO t VALUES (2, 20), (3, 30), (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_rows');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_with_data');
" "HEAD~1" "HEAD"

oracle_query "drop_via_range_syntax" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t');
" "SELECT CONCAT('ROW|', from_table_name, '|', to_table_name, '|',
       CASE WHEN from_create_statement IS NULL OR from_create_statement='' THEN 'N' ELSE 'Y' END, '|',
       CASE WHEN to_create_statement   IS NULL OR to_create_statement=''   THEN 'N' ELSE 'Y' END
     ) FROM dolt_schema_diff('HEAD~1..HEAD');"

oracle "drop_filter_by_table_name" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t');
" "HEAD~1" "HEAD" "t"

oracle "drop_filter_excludes_other_changes" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
DROP TABLE t;
ALTER TABLE u ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_t_alter_u');
" "HEAD~1" "HEAD" "t"

echo "--- modified table (add column) ---"

oracle "modified_add_col" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
" "HEAD~1" "HEAD"

oracle "modified_drop_col" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_col');
" "HEAD~1" "HEAD"

oracle "modified_rename_col" "
$SEED
ALTER TABLE t RENAME COLUMN v TO vv;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_col');
" "HEAD~1" "HEAD"

oracle "modified_rename_table" "
$SEED
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_table');
" "HEAD~1" "HEAD"

oracle "modified_rename_table_filter_old_name" "
$SEED
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_table');
" "HEAD~1" "HEAD" "t"

oracle "modified_rename_table_filter_new_name" "
$SEED
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_table');
" "HEAD~1" "HEAD" "t2"

oracle "modified_add_not_null_default" "
$SEED
ALTER TABLE t ADD COLUMN extra VARCHAR(32) NOT NULL DEFAULT '';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_not_null');
" "HEAD~1" "HEAD"

oracle "modified_add_nullable_default" "
$SEED
ALTER TABLE t ADD COLUMN extra VARCHAR(32) DEFAULT 'hi';
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_nullable');
" "HEAD~1" "HEAD"

oracle "modified_add_nullable_no_default" "
$SEED
ALTER TABLE t ADD COLUMN extra VARCHAR(32);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_bare');
" "HEAD~1" "HEAD"

oracle "modified_net_addcol_dropcol_range" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
UPDATE t SET extra = 'x' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'populate');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_col_again');
" "HEAD~3" "HEAD" "" "EXPECT_EMPTY"

# Positive control for the net-zero range above: the same multi-commit span,
# stopped one commit early (before the drop), must show the column add. This
# proves the empty full-range result is a genuine net-zero, not a range-walk
# that silently returns nothing over spans deeper than one commit.
oracle "net_addcol_dropcol_midrange_shows_add" "
$SEED
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_col');
UPDATE t SET extra = 'x' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'populate');
ALTER TABLE t DROP COLUMN extra;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_col_again');
" "HEAD~3" "HEAD~1"

oracle "multiple_alters_single_commit" "
$SEED
ALTER TABLE t ADD COLUMN a TEXT;
ALTER TABLE t ADD COLUMN b INT;
ALTER TABLE t RENAME COLUMN v TO vv;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'many_alters');
" "HEAD~1" "HEAD"

oracle "create_then_alter_same_commit" "
$SEED
CREATE TABLE u(id INT PRIMARY KEY);
ALTER TABLE u ADD COLUMN v TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'create_and_alter');
" "HEAD~1" "HEAD"

echo "--- multiple changes in one diff ---"

oracle "multi_change" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi');
" "HEAD~1" "HEAD"

oracle "multi_change_add_drop_modify" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x INT);
INSERT INTO u VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
ALTER TABLE t ADD COLUMN extra TEXT;
DROP TABLE u;
CREATE TABLE w(id INTEGER PRIMARY KEY, z TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_drop_modify');
" "HEAD~1" "HEAD"

oracle "modify_two_tables_one_commit" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
ALTER TABLE t RENAME COLUMN v TO vv;
ALTER TABLE u ADD COLUMN y TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'modify_two');
" "HEAD~1" "HEAD"

oracle "rename_and_add_col_same_commit" "
$SEED
ALTER TABLE t RENAME COLUMN v TO vv;
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'rename_plus_add');
" "HEAD~1" "HEAD"

echo "--- no changes ---"

oracle "no_changes" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_only');
" "HEAD~1" "HEAD" "" "EXPECT_EMPTY"

oracle "self_diff" "
$SEED
" "HEAD" "HEAD" "" "EXPECT_EMPTY"

echo "--- branch refs ---"

oracle "branch_diff" "
$SEED
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add');
" "main" "feat"

oracle "branch_from_tag_diff" "
$SEED
SELECT dolt_tag('v1');
SELECT dolt_branch('tagfeat', 'v1');
SELECT dolt_checkout('tagfeat');
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'tagfeat_add_col');
" "v1" "tagfeat"

echo "--- tag refs ---"

oracle "tag_diff" "
$SEED
SELECT dolt_tag('v1');
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'after_tag');
" "v1" "HEAD"

echo "--- table_name filter ---"

oracle "filter_added_table_only" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi');
" "HEAD~1" "HEAD" "u"

oracle "filter_modified_table_only" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'multi');
" "HEAD~1" "HEAD" "t"

oracle "filter_nonexistent_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "HEAD~1" "HEAD" "no_such_table" "EXPECT_EMPTY"

oracle_query "single_arg_range" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
" "SELECT CONCAT('ROW|', from_table_name, '|', to_table_name, '|', \
      CASE WHEN from_create_statement IS NULL OR from_create_statement='' THEN 'N' ELSE 'Y' END, '|', \
      CASE WHEN to_create_statement   IS NULL OR to_create_statement=''   THEN 'N' ELSE 'Y' END \
    ) FROM dolt_schema_diff('HEAD~1..HEAD') ORDER BY from_table_name, to_table_name;"

echo "--- merge parent refs ---"

oracle "first_parent_to_merge" "
$SEED
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_data_only');
SELECT dolt_merge('feat');
" "HEAD^1" "HEAD"

oracle "second_parent_to_merge" "
$SEED
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_data_only');
SELECT dolt_checkout('main');
CREATE TABLE m(id INTEGER PRIMARY KEY, y TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_m');
SELECT dolt_merge('feat');
" "HEAD^2" "HEAD"

echo "--- same-name drop / recreate ---"

oracle "drop_recreate_same_name" "
$SEED
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY, vv TEXT, extra INT);
INSERT INTO t VALUES (1, 'recreated', 7);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'recreate_t');
" "HEAD~1" "HEAD"

echo "--- replay after schema changes ---"

oracle "merge_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "HEAD^1" "HEAD" "u"

oracle "cherrypick_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "HEAD~1" "HEAD" "u"

oracle "revert_schema_change_with_later_added_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message='main_check' LIMIT 1));
" "HEAD~1" "HEAD" "t"

oracle "rebase_replay_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "main" "feat" "u"

oracle "merge_replay_multi_pk_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(a INTEGER, b INTEGER, w TEXT, PRIMARY KEY(a, b));
INSERT INTO u VALUES (1, 1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "HEAD^1" "HEAD" "u"

oracle "cherrypick_replay_multi_pk_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(a INTEGER, b INTEGER, w TEXT, PRIMARY KEY(a, b));
INSERT INTO u VALUES (1, 1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "HEAD~1" "HEAD" "u"

oracle "rebase_replay_multi_pk_add_table_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE u(a INTEGER, b INTEGER, w TEXT, PRIMARY KEY(a, b));
INSERT INTO u VALUES (1, 1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_u');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "main" "feat" "u"

oracle "merge_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_merge('feat');
" "HEAD^1" "HEAD" "p,c" "EXPECT_EMPTY"

oracle "cherrypick_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "HEAD~1" "HEAD" "p,c" "EXPECT_EMPTY"

oracle "rebase_replay_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_fk_tables');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "main" "feat" "p,c" "EXPECT_EMPTY"

echo "--- error paths ---"

oracle_error "bad_from_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff('nope','HEAD');"

oracle_error "bad_to_ref" "$SEED" \
  "SELECT * FROM dolt_schema_diff('HEAD','nope');"

oracle_error "bad_single_arg" "$SEED" \
  "SELECT * FROM dolt_schema_diff('nope');"

echo ""
echo "--- schema objects (pinned per-system shapes) ---"

# The two systems intentionally present schema objects differently:
# doltlite emits one row per SQLite schema object (an index or view change
# is its own row, named by the object), while Dolt folds index changes into
# the owning table's row and stores views/triggers as rows of the
# dolt_schemas table (whose own schema only changes when it is created or
# dropped, so a view modification produces NO dolt_schema_diff row there).
# Cross-comparing outputs is meaningless here; instead each system is
# pinned to its own expected shape so a change on either side surfaces.
DL_SHAPE_Q="SELECT 'ROW|' || coalesce(nullif(from_table_name,''),'~') || '|' || coalesce(nullif(to_table_name,''),'~') || '|' || (from_create_statement IS NOT NULL AND from_create_statement!='') || '|' || (to_create_statement IS NOT NULL AND to_create_statement!='') FROM dolt_schema_diff('HEAD~1','HEAD') ORDER BY 1;"
DT_SHAPE_Q="SELECT CONCAT('ROW|', coalesce(nullif(from_table_name,''),'~'), '|', coalesce(nullif(to_table_name,''),'~'), '|', (from_create_statement IS NOT NULL AND from_create_statement!=''), '|', (to_create_statement IS NOT NULL AND to_create_statement!='')) FROM dolt_schema_diff('HEAD~1','HEAD') ORDER BY 1;"

pinned_shapes() {
  local name="$1" dl_setup="$2" dl_expected="$3" dt_setup="$4" dt_expected="$5"
  local dir="$TMPROOT/${name}_pin"
  mkdir -p "$dir/dl" "$dir/dt"
  local ok=1

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$dl_setup" "$DL_SHAPE_Q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' | grep -a '^ROW|' | LC_ALL=C sort)
  if [ "$dl_out" != "$dl_expected" ]; then
    ok=0
    echo "  FAIL: $name (doltlite shape)"
    echo "    want:"; echo "$dl_expected" | sed 's/^/      /'
    echo "    got:";  echo "$dl_out" | sed 's/^/      /'
  fi

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    { printf '%s\n' "$dolt_setup"; printf '%s\n' "$DT_SHAPE_Q"; } \
      | "$DOLT" sql -c -r csv 2>"$dir/dt.err" \
      | tr -d '"\r' | grep -a '^ROW|' | LC_ALL=C sort
  )
  if [ "$dt_out" != "$dt_expected" ]; then
    ok=0
    echo "  FAIL: $name (dolt shape)"
    echo "    want:"; echo "$dt_expected" | sed 's/^/      /'
    echo "    got:";  echo "$dt_out" | sed 's/^/      /'
  fi

  if [ "$ok" -eq 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
  fi
}

pinned_shapes "shape_index_add" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
SELECT dolt_commit('-Am','base');
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','c');
" "ROW|~|idx|0|1" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
SELECT dolt_commit('-Am','base');
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','c');
" "ROW|t|t|1|1"

pinned_shapes "shape_index_drop" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','base');
DROP INDEX idx;
SELECT dolt_commit('-Am','c');
" "ROW|idx|~|1|0" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','base');
ALTER TABLE t DROP INDEX idx;
SELECT dolt_commit('-Am','c');
" "ROW|t|t|1|1"

pinned_shapes "shape_index_modify" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT, c INT);
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','base');
DROP INDEX idx;
CREATE INDEX idx ON t(c);
SELECT dolt_commit('-Am','c');
" "ROW|idx|idx|1|1" "
CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT);
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','base');
ALTER TABLE t DROP INDEX idx;
CREATE INDEX idx ON t(c);
SELECT dolt_commit('-Am','c');
" "ROW|t|t|1|1"

pinned_shapes "shape_view_add" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','base');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','c');
" "ROW|~|v|0|1" "
CREATE TABLE t(a INT PRIMARY KEY);
SELECT dolt_commit('-Am','base');
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','c');
" "ROW|~|dolt_schemas|0|1"

pinned_shapes "shape_view_drop_last" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','base');
DROP VIEW v;
SELECT dolt_commit('-Am','c');
" "ROW|v|~|1|0" "
CREATE TABLE t(a INT PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','base');
DROP VIEW v;
SELECT dolt_commit('-Am','c');
" "ROW|dolt_schemas|~|1|0"

pinned_shapes "shape_view_modify" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','base');
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am','c');
" "ROW|v|v|1|1" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','base');
DROP VIEW v;
CREATE VIEW v AS SELECT b FROM t;
SELECT dolt_commit('-Am','c');
" ""

pinned_shapes "shape_trigger_add_existing_schemas" "
CREATE TABLE t(a INTEGER PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','base');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END;
SELECT dolt_commit('-Am','c');
" "ROW|~|trg|0|1" "
CREATE TABLE t(a INT PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t;
SELECT dolt_commit('-Am','base');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW SET @x = 1;
SELECT dolt_commit('-Am','c');
" ""

pinned_shapes "shape_mixed_commit" "
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
CREATE TABLE u(x INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am','base');
CREATE INDEX idx ON t(b);
CREATE VIEW v AS SELECT a FROM t;
ALTER TABLE u ADD COLUMN y INT;
SELECT dolt_commit('-Am','c');
" "ROW|u|u|1|1
ROW|~|idx|0|1
ROW|~|v|0|1" "
CREATE TABLE t(a INT PRIMARY KEY, b INT);
CREATE TABLE u(x INT PRIMARY KEY);
SELECT dolt_commit('-Am','base');
CREATE INDEX idx ON t(b);
CREATE VIEW v AS SELECT a FROM t;
ALTER TABLE u ADD COLUMN y INT;
SELECT dolt_commit('-Am','c');
" "ROW|t|t|1|1
ROW|u|u|1|1
ROW|~|dolt_schemas|0|1"

# doltlite's third argument filters by SQLite schema-object name; the
# owning table's filter shows only the table's own DDL changes.
DL_FILTER_SETUP="
CREATE TABLE t(a INTEGER PRIMARY KEY, b INT);
SELECT dolt_commit('-Am','base');
CREATE INDEX idx ON t(b);
SELECT dolt_commit('-Am','c');
"
filter_check() {
  local name="$1" filter="$2" expected="$3"
  local dir="$TMPROOT/${name}_flt"
  mkdir -p "$dir"
  local q="SELECT 'ROW|' || coalesce(nullif(from_table_name,''),'~') || '|' || coalesce(nullif(to_table_name,''),'~') FROM dolt_schema_diff('HEAD~1','HEAD','$filter') ORDER BY 1;"
  local out
  out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$DL_FILTER_SETUP" "$q" \
        | "$DOLTLITE" "$dir/db" 2>"$dir/err" \
        | tr -d '\r' | grep -a '^ROW|' | sort)
  if [ "$out" = "$expected" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    want: |$expected|"
    echo "    got:  |$out|"
  fi
}
filter_check "filter_by_index_name" "idx" "ROW|~|idx"
filter_check "filter_by_owning_table" "t" ""

echo "--- WORKING / STAGED pseudo-refs ---"

oracle "working_added_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
" "HEAD" "WORKING"

oracle "working_dropped_table" "
$SEED
DROP TABLE t;
" "HEAD" "WORKING"

oracle "working_no_change" "
$SEED
" "HEAD" "WORKING" "" "EXPECT_EMPTY"

oracle "staged_added_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
" "HEAD" "STAGED"

oracle "staged_to_working" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT);
SELECT dolt_add('-A');
CREATE TABLE w2(id INTEGER PRIMARY KEY);
" "STAGED" "WORKING"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
