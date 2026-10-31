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
  local name="$1" script="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"
  local dl_out dt_out
  dl_out=$(printf "%s\n" "$script" \
           | vc_oracle_run_doltlite --allow-error "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^R|' | tr -d '\r')
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    vc_oracle_translate_for_dolt "$script" \
      | "$DOLT" sql --continue -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"\r' | grep '^R|')
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_nonlocal_tables ==="

oracle "nonlocal_alias_to_main" "
CREATE TABLE aliased_table (pk VARCHAR(8) PRIMARY KEY);
INSERT INTO aliased_table VALUES ('amzmapqt');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('nonlocal_table', 'main', 'aliased_table', 'immediate');
INSERT INTO nonlocal_table VALUES ('eesekkgo');
SELECT CONCAT('R|other|', pk) FROM nonlocal_table ORDER BY pk;
SELECT CONCAT('R|other-status|', table_name, '|', status) FROM dolt_status ORDER BY table_name;
SELECT dolt_checkout('main');
SELECT CONCAT('R|main|', pk) FROM aliased_table ORDER BY pk;
SELECT CONCAT('R|main-status|', table_name, '|', status) FROM dolt_status ORDER BY table_name;
"

oracle "nonlocal_wildcard_default_ref" "
CREATE TABLE global_a (id INT PRIMARY KEY);
CREATE TABLE global_b (id INT PRIMARY KEY);
INSERT INTO global_a VALUES (1);
INSERT INTO global_b VALUES (2);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('global_*', 'main', '', 'immediate');
INSERT INTO global_a VALUES (10);
INSERT INTO global_b VALUES (20);
SELECT dolt_commit('-Am', 'rules');
SELECT dolt_checkout('main');
SELECT CONCAT('R|main-a|', id) FROM global_a ORDER BY id;
SELECT CONCAT('R|main-b|', id) FROM global_b ORDER BY id;
SELECT dolt_checkout('other');
SELECT CONCAT('R|other-a|', id) FROM global_a ORDER BY id;
"

oracle "nonlocal_tag_target" "
CREATE TABLE cfg (k VARCHAR(10) PRIMARY KEY, v VARCHAR(10));
INSERT INTO cfg VALUES ('mode', 'v1');
SELECT dolt_commit('-Am', 'c1');
SELECT dolt_tag('v1');
UPDATE cfg SET v = 'v2';
SELECT dolt_commit('-am', 'c2');
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('frozen', 'v1', 'cfg', 'immediate');
SELECT CONCAT('R|frozen|', k, '|', v) FROM frozen;
SELECT CONCAT('R|cfg|', k, '|', v) FROM cfg;
"

oracle "nonlocal_txn_with_local" "
CREATE TABLE users (id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
CREATE TABLE notes (id INT PRIMARY KEY);
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('users', 'main', '', 'immediate');
SELECT dolt_commit('-Am', 'rules');
BEGIN;
INSERT INTO users VALUES (1);
INSERT INTO notes VALUES (1);
COMMIT;
BEGIN;
INSERT INTO users VALUES (2);
INSERT INTO notes VALUES (2);
ROLLBACK;
SELECT CONCAT('R|other-notes|', id) FROM notes ORDER BY id;
SELECT CONCAT('R|other-users|', id) FROM users ORDER BY id;
SELECT CONCAT('R|other-status|', table_name, '|', status) FROM dolt_status ORDER BY table_name;
SELECT dolt_commit('-am', 'notes');
SELECT dolt_checkout('main');
SELECT CONCAT('R|main-users|', id) FROM users ORDER BY id;
SELECT CONCAT('R|main-status|', table_name, '|', status) FROM dolt_status ORDER BY table_name;
"

oracle "nonlocal_clean_keeps_backing_table" "
CREATE TABLE t (id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('keep_me', '', '', 'immediate');
SELECT dolt_commit('-Am', 'rules');
CREATE TABLE keep_me (id INT PRIMARY KEY);
CREATE TABLE scratch (id INT PRIMARY KEY);
SELECT dolt_clean();
SELECT CONCAT('R|status|', table_name, '|', status) FROM dolt_status ORDER BY table_name;
"

oracle "nonlocal_fk_child_insert" "
CREATE TABLE aliased_table (pk VARCHAR(8) PRIMARY KEY);
INSERT INTO aliased_table VALUES ('amzmapqt');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
PRAGMA foreign_keys = ON;
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('nonlocal_table', 'main', 'aliased_table', 'immediate');
INSERT INTO nonlocal_table VALUES ('eesekkgo');
CREATE TABLE local_table (pk VARCHAR(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES nonlocal_table(pk));
INSERT INTO local_table VALUES ('eesekkgo');
INSERT INTO local_table VALUES ('fdnfjfjf');
SELECT CONCAT('R|local|', pk) FROM local_table ORDER BY pk;
SELECT dolt_verify_constraints('--all');
SELECT CONCAT('R|cv-count|', count(*)) FROM dolt_constraint_violations;
"

oracle "nonlocal_fk_parent_removed" "
CREATE TABLE aliased_table (pk VARCHAR(8) PRIMARY KEY);
INSERT INTO aliased_table VALUES ('amzmapqt');
INSERT INTO aliased_table VALUES ('eesekkgo');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options)
  VALUES ('nonlocal_table', 'main', 'aliased_table', 'immediate');
INSERT INTO nonlocal_table VALUES ('remote');
CREATE TABLE local_table (pk VARCHAR(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES nonlocal_table(pk));
INSERT INTO local_table VALUES ('remote');
DELETE FROM nonlocal_table WHERE pk = 'remote';
set @@dolt_force_transaction_commit = 1;
SELECT dolt_verify_constraints('--all');
SELECT CONCAT('R|cv|', \`table\`, '|', num_violations) FROM dolt_constraint_violations;
"

for cmd in "dolt_add('.')" "dolt_reset('--hard')" "dolt_branch('nb')" \
           "dolt_commit('-Am', 'x')" "dolt_checkout('-b', 'nb2')" \
           "dolt_checkout('notes')" "dolt_clean()" "dolt_merge('side')"; do
  oracle "nonlocal_vc_in_txn_$(printf '%s' "$cmd" | tr -c 'a-z0-9' '_')" "
CREATE TABLE users (id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('side');
SELECT dolt_checkout('-b', 'other');
CREATE TABLE notes (id INT PRIMARY KEY);
INSERT INTO dolt_nonlocal_tables VALUES ('users', 'main', '', 'immediate');
SELECT dolt_commit('-Am', 'rules');
BEGIN;
INSERT INTO users VALUES (1);
INSERT INTO notes VALUES (1);
SELECT $cmd;
SELECT CONCAT('R|in-txn-users|', count(*)) FROM users;
SELECT CONCAT('R|in-txn-notes|', count(*)) FROM notes;
COMMIT;
SELECT CONCAT('R|branch|', active_branch());
SELECT CONCAT('R|br|', name) FROM dolt_branches ORDER BY name;
SELECT dolt_checkout('other');
SELECT CONCAT('R|other-notes|', count(*)) FROM notes;
SELECT CONCAT('R|other-status|', table_name, '|', staged, '|', status) FROM dolt_status ORDER BY table_name;
SELECT dolt_checkout('main');
SELECT CONCAT('R|main-users|', count(*)) FROM users;
SELECT CONCAT('R|main-status|', table_name, '|', staged, '|', status) FROM dolt_status ORDER BY table_name;
"
done

oracle "nonlocal_target_branch_deleted_renamed" "
CREATE TABLE users (id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('shared');
SELECT dolt_branch('shared2');
SELECT dolt_checkout('-b', 'other');
INSERT INTO dolt_nonlocal_tables VALUES
  ('users', 'shared', '', 'immediate'),
  ('u2', 'shared2', 'users', 'immediate');
SELECT dolt_commit('-Am', 'rules');
INSERT INTO users VALUES (1);
SELECT CONCAT('R|users|', count(*)) FROM users;
SELECT CONCAT('R|u2|', count(*)) FROM u2;
SELECT dolt_branch('-D', 'shared');
SELECT CONCAT('R|after-delete|', count(*)) FROM users;
INSERT INTO users VALUES (2);
SELECT dolt_branch('-m', 'shared2', 'renamed');
SELECT CONCAT('R|after-rename|', count(*)) FROM u2;
SELECT CONCAT('R|br|', name) FROM dolt_branches ORDER BY name;
SELECT dolt_branch('shared');
SELECT CONCAT('R|recreated|', count(*)) FROM users;
"

COMMIT_ALL_SETUP="
CREATE TABLE users (id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'other');
CREATE TABLE notes (id INT PRIMARY KEY);
INSERT INTO dolt_nonlocal_tables VALUES ('users', 'main', '', 'immediate');
SELECT dolt_commit('-Am', 'rules');
"
COMMIT_ALL_STATE="
SELECT CONCAT('R|other-log|', message) FROM dolt_log LIMIT 2;
SELECT CONCAT('R|other-status|', table_name, '|', staged, '|', status) FROM dolt_status ORDER BY table_name;
SELECT CONCAT('R|other-notes|', count(*)) FROM notes;
SELECT dolt_checkout('main');
SELECT CONCAT('R|main-log|', message) FROM dolt_log LIMIT 2;
SELECT CONCAT('R|main-status|', table_name, '|', staged, '|', status) FROM dolt_status ORDER BY table_name;
SELECT CONCAT('R|main-users|', count(*)) FROM users;
"

oracle "nonlocal_commit_all_both_branches" "$COMMIT_ALL_SETUP
BEGIN;
INSERT INTO users VALUES (1);
INSERT INTO notes VALUES (1);
SELECT dolt_commit_all('-am', 'both');
INSERT INTO notes VALUES (2);
ROLLBACK;
$COMMIT_ALL_STATE"

oracle "nonlocal_commit_all_nothing_staged" "$COMMIT_ALL_SETUP
BEGIN;
INSERT INTO users VALUES (1);
INSERT INTO notes VALUES (1);
SELECT dolt_add('notes');
SELECT dolt_commit_all('-m', 'staged only');
ROLLBACK;
$COMMIT_ALL_STATE"

oracle "nonlocal_commit_all_clean" "$COMMIT_ALL_SETUP
SELECT dolt_commit_all('--allow-empty', '-m', 'empty');
$COMMIT_ALL_STATE"

tables_oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"
  local dl_out dt_out
  dl_out=$(printf "%s\n.tables\n" "$setup" \
           | vc_oracle_run_doltlite --allow-error "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -s ' \t' '\n\n' | grep -E '^[A-Za-z_]+$' \
           | grep -v '^dolt_' | sort | sed 's/^/R|t|/')
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    { vc_oracle_translate_for_dolt "$setup"; echo "SHOW TABLES;"; } \
      | "$DOLT" sql --continue -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"\r' | sed -n '/^Tables_in_/,$p' | tail -n +2 \
           | grep -v '^dolt_' | sort | sed 's/^/R|t|/')
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

tables_oracle "nonlocal_tables_listing" "
CREATE TABLE users (id INT PRIMARY KEY);
CREATE TABLE cfg (k VARCHAR(10) PRIMARY KEY);
CREATE TABLE glob_a (id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'other');
CREATE TABLE notes (id INT PRIMARY KEY);
DROP TABLE users;
DROP TABLE glob_a;
INSERT INTO dolt_nonlocal_tables VALUES
  ('users', 'main', '', 'immediate'),
  ('settings', 'main', 'cfg', 'immediate'),
  ('glob_*', 'main', '', 'immediate'),
  ('ghost', 'main', '', 'immediate');
"

vc_oracle_finish
