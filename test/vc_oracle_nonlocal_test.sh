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

vc_oracle_finish
