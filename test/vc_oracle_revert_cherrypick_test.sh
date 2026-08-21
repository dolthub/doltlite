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

normalize_log() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 3 && $1 == "L" { print }' \
    | awk -F'\t' '
        {
          msg = $3
          lower = tolower(msg)
          if (match(lower, /^cherry-pick[: ]/) > 0) {
            sub(/^[Cc]herry-?[Pp]ick[: ]+/, "", msg)
            gsub(/^"|"$|^'\''|'\''$/, "", msg)
            msg = "CP:" msg
          } else if (match(lower, /^revert/) > 0) {
            sub(/^[Rr]evert[ ]+/, "", msg)
            gsub(/^"|"$|^'\''|'\''$/, "", msg)
            msg = "RV:" msg
          }
          print "L\t" $2 "\t" msg
        }
      ' \
    | sort -t$'\t' -k3 \
    | awk -F'\t' '
        {
          h = $2
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print "L\t" seen[h] "\t" $3
        }
      '
}

normalize_table() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "T" { print }' \
    | sort
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_log dl_table
  dl_log=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'L' || char(9) || commit_hash || char(9) || message FROM dolt_log;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_log)
  dl_table=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'T' || char(9) || coalesce(id, '') || char(9) || coalesce(v, '') FROM t;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_table)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_log dt_table
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('L', char(9), commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.log.raw"
  dt_log=$(tail -n +2 "$dir/dt.log.raw" | tr -d '"' | normalize_log)

  (
    mkdir -p "$dir/dt.s" && cd "$dir/dt.s" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.s.err"
    "$DOLT" sql -r csv -q "SELECT concat('T', char(9), coalesce(id, ''), char(9), coalesce(v, '')) FROM t;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.table.raw"
  dt_table=$(tail -n +2 "$dir/dt.table.raw" | tr -d '"' | normalize_table)

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_table"
  dt_combined="$dt_log"$'\n'"$dt_table"

  vc_oracle_assert_match "$name" "$dl_combined" "$dt_combined"
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
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

oracle_error_poststate() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_post"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup" || true
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s;\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_poststate() {
  local name="$1" setup="$2" dl_query="$3" dolt_query="${4:-$3}"
  local dir="$TMPROOT/${name}_okpost"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup" || {
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (doltlite setup failed)"
    return
  }
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s;\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || {
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (dolt setup failed)"
    return
  }
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dolt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_revert / dolt_cherry_pick ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
"

echo "--- cherry-pick: basic ---"

oracle "cherry_pick_branch_tip" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature');
"

oracle "cherry_pick_update_non_pk" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_update');
SELECT dolt_tag('upd-source');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('upd-source');
"

oracle "cherry_pick_by_tag" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_tag('cherry-source');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('cherry-source');
"

oracle "cherry_pick_by_branch_relative" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature~1');
"

echo "--- cherry-pick: chains ---"

oracle "cherry_pick_two_in_sequence" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_3');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature~1');
SELECT dolt_cherry_pick('feature');
"

oracle_poststate "cherry_pick_disjoint_add_table_plus_check" "
CREATE TABLE base(id INTEGER PRIMARY KEY, v INT);
INSERT INTO base VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TABLE feat_tbl(k INTEGER PRIMARY KEY, w TEXT);
INSERT INTO feat_tbl VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat adds table');
SELECT dolt_checkout('main');
CREATE TABLE base_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO base_new SELECT * FROM base;
DROP TABLE base;
ALTER TABLE base_new RENAME TO base;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main adds check');
SELECT dolt_cherry_pick('feat');
" "SELECT (SELECT count(*) FROM sqlite_master WHERE type='table' AND name='feat_tbl') || '|' ||
          (SELECT count(*) FROM feat_tbl) || '|' ||
          (SELECT count(*) FROM base)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'feat_tbl'), '|', (SELECT COUNT(*) FROM feat_tbl), '|', (SELECT COUNT(*) FROM base))"

oracle_poststate "cherry_pick_disjoint_add_indexes" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat idx');
SELECT dolt_checkout('main');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main idx');
SELECT dolt_cherry_pick('feat');
" "SELECT (SELECT count(*) FROM pragma_index_list('a') WHERE name = 'idx_a_v') || '|' ||
          (SELECT count(*) FROM pragma_index_list('b') WHERE name = 'idx_b_v')" \
  "SELECT CONCAT((SELECT COUNT(*) FROM information_schema.statistics WHERE table_name = 'a' AND index_name = 'idx_a_v'), '|', (SELECT COUNT(*) FROM information_schema.statistics WHERE table_name = 'b' AND index_name = 'idx_b_v'))"

oracle_poststate "cherry_pick_disjoint_fk_tables_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
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
" "SELECT (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT((SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_poststate "cherry_pick_recreate_fk_family_plus_check" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (1, 100);
INSERT INTO c VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
DROP TABLE c;
DROP TABLE p;
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE, label TEXT);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES p(u));
INSERT INTO p VALUES (2, 200, 'x');
INSERT INTO c VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_recreate_fk_family');
SELECT dolt_checkout('main');
CREATE TABLE t_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO t_new SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t_new RENAME TO t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_check');
SELECT dolt_cherry_pick('feat');
" "SELECT (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c) || '|' ||
          (SELECT count(*) FROM pragma_foreign_key_list('c'))" \
  "SELECT CONCAT((SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c), '|', (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE table_name = 'c' AND referenced_table_name = 'p'))"

oracle_poststate "cherry_pick_self_ref_fk_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE t(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE CASCADE
);
INSERT INTO t VALUES (1, NULL), (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (3, 2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_descendant');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (10, NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_cherry_pick('feat');
DELETE FROM t WHERE id = 1;
" "SELECT (SELECT count(*) FROM t) || '|' ||
          (SELECT group_concat(id || ':' || ifnull(parent_id, -1), ',') FROM (SELECT id, parent_id FROM t ORDER BY id) AS ordered_rows)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM t), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', IFNULL(parent_id, -1)) ORDER BY id SEPARATOR ',') FROM t))"

oracle_poststate "cherry_pick_fk_chain_cascade" "
PRAGMA foreign_keys = ON;
CREATE TABLE gp(id INTEGER PRIMARY KEY);
CREATE TABLE p(
  id INTEGER PRIMARY KEY,
  gp_id INTEGER,
  FOREIGN KEY (gp_id) REFERENCES gp(id) ON DELETE CASCADE
);
CREATE TABLE c(
  id INTEGER PRIMARY KEY,
  p_id INTEGER,
  FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE
);
INSERT INTO gp VALUES (1);
INSERT INTO p VALUES (1, 1);
INSERT INTO c VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO c VALUES (2, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_child');
SELECT dolt_checkout('main');
INSERT INTO gp VALUES (2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_add_root');
SELECT dolt_cherry_pick('feat');
DELETE FROM gp WHERE id = 1;
" "SELECT (SELECT count(*) FROM gp) || '|' ||
          (SELECT count(*) FROM p) || '|' ||
          (SELECT count(*) FROM c)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM gp), '|', (SELECT COUNT(*) FROM p), '|', (SELECT COUNT(*) FROM c))"

echo "--- revert: basic ---"

oracle "revert_undoes_last_insert" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c2_add_2'));
"

oracle "revert_undoes_update" "
$SEED
UPDATE t SET v = 999 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_update');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c2_update'));
"

oracle "revert_undoes_delete" "
$SEED
DELETE FROM t WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_delete');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c2_delete'));
"

oracle "revert_by_tag" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
SELECT dolt_tag('to-revert');
SELECT dolt_revert('to-revert');
"

oracle "revert_head" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
SELECT dolt_revert('HEAD');
"

oracle "revert_head_relative" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_add_3');
SELECT dolt_revert('HEAD~1');
"

echo "--- revert: chains ---"

oracle "revert_two_in_reverse_order" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_add_2');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_add_3');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c4_add_4');
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c4_add_4'));
SELECT dolt_revert((SELECT commit_hash FROM dolt_log WHERE message = 'c3_add_3'));
"

oracle_poststate "revert_old_add_table_preserves_later_check" "
CREATE TABLE base(id INTEGER PRIMARY KEY, v INT);
INSERT INTO base VALUES (1, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE TABLE feat_tbl(k INTEGER PRIMARY KEY, w TEXT);
INSERT INTO feat_tbl VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add table');
CREATE TABLE base_new(id INTEGER PRIMARY KEY, v INT CHECK (v > 0));
INSERT INTO base_new SELECT * FROM base;
DROP TABLE base;
ALTER TABLE base_new RENAME TO base;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add check');
SELECT dolt_revert('HEAD~1');
" "SELECT (SELECT count(*) FROM sqlite_master WHERE type='table' AND name='feat_tbl') || '|' ||
          (SELECT count(*) FROM base)" \
  "SELECT CONCAT((SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'feat_tbl'), '|', (SELECT COUNT(*) FROM base))"

oracle_poststate "revert_old_index_preserves_later_index" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE INDEX idx_b_v ON b(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add idx b');
CREATE INDEX idx_a_v ON a(v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add idx a');
SELECT dolt_revert('HEAD~1');
" "SELECT (SELECT count(*) FROM pragma_index_list('a') WHERE name = 'idx_a_v') || '|' ||
          (SELECT count(*) FROM pragma_index_list('b') WHERE name = 'idx_b_v')" \
  "SELECT CONCAT((SELECT COUNT(*) FROM information_schema.statistics WHERE table_name = 'a' AND index_name = 'idx_a_v'), '|', (SELECT COUNT(*) FROM information_schema.statistics WHERE table_name = 'b' AND index_name = 'idx_b_v'))"

echo "--- conflicts ---"

oracle_no_merge_commit() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_nm"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_count
  printf '%s\n' "$setup" | "$DOLTLITE" "$dir/dl/db" >/dev/null 2>"$dir/dl.err" || true
  dl_count=$(printf ".headers off\n.mode list\nSELECT count(*) FROM dolt_log;\n" \
             | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
             | grep -E '^[0-9]+$' | tail -1)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_count
  dt_count=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err" || true
    "$DOLT" sql -r csv -q "SELECT count(*) FROM dolt_log;" 2>>"$dir/dt.err" \
      | tail -n +2
  )

  if [ "$dl_count" = "$dt_count" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite log count: $dl_count"
    echo "    dolt log count:     $dt_count"
  fi
}

oracle_no_merge_commit "cherry_pick_modify_modify_conflict" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_99');
SELECT dolt_tag('feat-conflict');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_11');
SELECT dolt_cherry_pick('feat-conflict');
"

oracle_no_merge_commit "revert_with_later_overlap_conflict" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_set_99');
SELECT dolt_revert('HEAD~1');
"

oracle_error_poststate "cherry_pick_conflict_rolls_back" "
$SEED
SELECT dolt_checkout('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_99');
SELECT dolt_tag('feat-conflict');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_11');
SELECT dolt_cherry_pick('feat-conflict');
 " "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "revert_conflict_rolls_back" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c3_set_99');
SELECT dolt_revert('HEAD~1');
" "SELECT (SELECT count(*) FROM dolt_conflicts) || '|' || (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_conflicts), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "revert_added_column_edit_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t ADD COLUMN d TEXT;
SELECT dolt_commit('-Am', 'add d');
UPDATE t SET d = 'x' WHERE id = 1;
SELECT dolt_commit('-Am', 'set d');
SELECT dolt_revert('HEAD~1');
" "SELECT 'Q|' || d || '|' || (SELECT message FROM dolt_log LIMIT 1) FROM t WHERE id = 1" \
"SELECT CONCAT('Q|', d, '|', (SELECT message FROM dolt_log LIMIT 1)) FROM t WHERE id = 1"

oracle_error_poststate "cherry_pick_drop_edited_column_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, d TEXT);
INSERT INTO t VALUES (1, 'a', NULL);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'dropper');
ALTER TABLE t DROP COLUMN d;
SELECT dolt_commit('-Am', 'drop d');
SELECT dolt_checkout('main');
UPDATE t SET d = 'x' WHERE id = 1;
SELECT dolt_commit('-Am', 'set d');
SELECT dolt_cherry_pick('dropper');
" "SELECT 'Q|' || d || '|' || (SELECT message FROM dolt_log LIMIT 1) FROM t WHERE id = 1" \
"SELECT CONCAT('Q|', d, '|', (SELECT message FROM dolt_log LIMIT 1)) FROM t WHERE id = 1"

echo "--- error paths ---"

oracle_error "cherry_pick_no_args" "
$SEED
SELECT dolt_cherry_pick();
"

oracle_error "cherry_pick_nonexistent_ref" "
$SEED
SELECT dolt_cherry_pick('does-not-exist');
"

oracle_error "cherry_pick_extra_arg" "
$SEED
SELECT dolt_cherry_pick('HEAD', 'extra');
"

oracle_error "cherry_pick_dirty_working_set" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_cherry_pick('feature');
"

oracle_error "cherry_pick_staged_changes" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_checkout('main');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_cherry_pick('feature');
"

oracle "revert_no_args_is_noop" "
$SEED
SELECT dolt_revert();
"

oracle_error "revert_nonexistent_ref" "
$SEED
SELECT dolt_revert('does-not-exist');
"

oracle_error "revert_extra_arg" "
$SEED
SELECT dolt_revert('HEAD', 'extra');
"

oracle_error "revert_dirty_working_set" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_revert('HEAD');
"

oracle_error "revert_staged_changes" "
$SEED
UPDATE t SET v = 50 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2_set_50');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_revert('HEAD');
"

oracle_error "cherry_pick_initial_commit" "
$SEED
SELECT dolt_cherry_pick((SELECT commit_hash FROM dolt_log WHERE message = 'Initialize data repository'));
"

oracle_error_poststate "cherry_pick_constraint_violation_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET u=9, v='feat2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_unique');
SELECT dolt_cherry_pick('feature');
" "SELECT (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "revert_constraint_violation_rolls_back" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
UPDATE t SET u=9, v='c1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1_set_9');
UPDATE t SET u=1, v='c2_take_1' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2_take_1');
SELECT dolt_revert('HEAD~1');
" "SELECT (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "cherry_pick_constraint_violation_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET u=9, v='feat2' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_unique');
BEGIN;
SELECT dolt_cherry_pick('feature');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "revert_constraint_violation_txn_rollback" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES (1,1,'base1'),(2,2,'base2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
UPDATE t SET u=9, v='c1' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c1_set_9');
UPDATE t SET u=1, v='c2_take_1' WHERE id=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','c2_take_1');
BEGIN;
SELECT dolt_revert('HEAD~1');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_constraint_violations) || '|' || (SELECT group_concat(id || ':' || u || ':' || v, ',') FROM (SELECT id,u,v FROM t ORDER BY id) AS ordered_rows)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', u, ':', v) ORDER BY id SEPARATOR ',') FROM t))"

oracle_error_poststate "cherry_pick_fk_violation_rolls_back" "
CREATE TABLE parent(pk INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE child(pk INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES parent(u));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO child VALUES (2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_drop_parent');
SELECT dolt_cherry_pick('feature');
" "SELECT (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
          (SELECT group_concat(pk || ':' || u, ',') FROM (SELECT pk,u FROM parent ORDER BY pk) AS ordered_parent) || '|' ||
          (SELECT group_concat(pk || ':' || u, ',') FROM (SELECT pk,u FROM child ORDER BY pk) AS ordered_child)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(pk, ':', u) ORDER BY pk SEPARATOR ',') FROM parent), '|', (SELECT GROUP_CONCAT(CONCAT(pk, ':', u) ORDER BY pk SEPARATOR ',') FROM child))"

oracle_error_poststate "cherry_pick_fk_violation_txn_rollback" "
CREATE TABLE parent(pk INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE child(pk INTEGER PRIMARY KEY, u INT, FOREIGN KEY (u) REFERENCES parent(u));
INSERT INTO parent VALUES (1,1),(2,2);
INSERT INTO child VALUES (1,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO child VALUES (2,2);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat_add_child');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main_drop_parent');
BEGIN;
SELECT dolt_cherry_pick('feature');
ROLLBACK;
" "SELECT (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
          (SELECT group_concat(pk || ':' || u, ',') FROM (SELECT pk,u FROM parent ORDER BY pk) AS ordered_parent) || '|' ||
          (SELECT group_concat(pk || ':' || u, ',') FROM (SELECT pk,u FROM child ORDER BY pk) AS ordered_child)" \
"SELECT CONCAT((SELECT COUNT(*) FROM dolt_constraint_violations), '|', (SELECT GROUP_CONCAT(CONCAT(pk, ':', u) ORDER BY pk SEPARATOR ',') FROM parent), '|', (SELECT GROUP_CONCAT(CONCAT(pk, ':', u) ORDER BY pk SEPARATOR ',') FROM child))"

echo "--- savepoint parity ---"

oracle_error_poststate "cherry_pick_savepoint_invalidated" "
$SEED
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_add_2');
SELECT dolt_checkout('main');
SAVEPOINT sp1;
SELECT dolt_cherry_pick('feature');
ROLLBACK TO sp1;
" "SELECT active_branch() || '|' ||
          (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered) || '|' ||
          (SELECT count(*) FROM dolt_log);" \
  "SELECT CONCAT(active_branch(), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t), '|', (SELECT COUNT(*) FROM dolt_log))"

oracle_error_poststate "revert_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v = 20 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SAVEPOINT sp1;
SELECT dolt_revert('HEAD');
ROLLBACK TO sp1;
" "SELECT active_branch() || '|' ||
          (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered) || '|' ||
          (SELECT count(*) FROM dolt_log);" \
  "SELECT CONCAT(active_branch(), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t), '|', (SELECT COUNT(*) FROM dolt_log))"

oracle_error_poststate "cherry_pick_conflict_top_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'feature' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feature edit');
SELECT dolt_tag('feat-conflict');
SELECT dolt_checkout('main');
UPDATE t SET v = 'main' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main edit');
SAVEPOINT sp1;
SELECT dolt_cherry_pick('feat-conflict');
ROLLBACK TO sp1;
" "SELECT active_branch() || '|' ||
          (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered) || '|' ||
          (SELECT count(*) FROM dolt_conflicts);" \
  "SELECT CONCAT(active_branch(), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t), '|', (SELECT COUNT(*) FROM dolt_conflicts))"

oracle_error_poststate "revert_conflict_top_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
UPDATE t SET v = 'mid' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'mid edit');
UPDATE t SET v = 'head' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'head edit');
SAVEPOINT sp1;
SELECT dolt_revert('HEAD~1');
ROLLBACK TO sp1;
" "SELECT active_branch() || '|' ||
          (SELECT group_concat(id || ':' || v, ',') FROM (SELECT id, v FROM t ORDER BY id) AS ordered) || '|' ||
          (SELECT count(*) FROM dolt_conflicts);" \
  "SELECT CONCAT(active_branch(), '|', (SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM t), '|', (SELECT COUNT(*) FROM dolt_conflicts))"

echo ""
echo "--- schema objects (views, triggers, indexes) ---"

# Trigger syntax differs (BEGIN...END vs FOR EACH ROW); per-engine setups.
oracle_dual_poststate() {
  local name="$1" dl_setup="$2" dt_setup="$3" dl_query="$4" dt_query="$5"
  local dir="$TMPROOT/${name}_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$dl_setup" || true
  local dl_out
  dl_out=$(
    printf ".headers off\n.mode list\n%s;\n" "$dl_query" \
      | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
      | tr -d '\r'
  )

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" || true
  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "$dt_query" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr -d '"\r'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

DL_VCOUNT="SELECT 'V|' || count(*) FROM sqlite_master WHERE type = 'view'"
DT_VCOUNT="SELECT concat('V|', count(*)) FROM dolt_schemas WHERE type = 'view'"

oracle_poststate "revert_view_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_commit('-Am', 'base');
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_commit('-Am', 'add view');
SELECT dolt_revert('HEAD');
" "$DL_VCOUNT" "$DT_VCOUNT"

oracle_poststate "revert_view_drop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_commit('-Am', 'base');
DROP VIEW w;
SELECT dolt_commit('-Am', 'drop view');
SELECT dolt_revert('HEAD');
" "SELECT 'Q|' || group_concat(id, ',') FROM (SELECT id FROM w ORDER BY id)" \
"SELECT concat('Q|', group_concat(id ORDER BY id SEPARATOR ',')) FROM w"

oracle_poststate "revert_index_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
CREATE INDEX idx ON t(v);
SELECT dolt_commit('-Am', 'add index');
INSERT INTO t VALUES (2, 20);
SELECT dolt_commit('-am', 'more data');
SELECT dolt_revert('HEAD~1');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT count(*) FROM t)" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT count(*) FROM t))"

oracle_dual_poststate "revert_index_drop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX idx ON t(v);
INSERT INTO t VALUES (1, 10), (2, 10);
SELECT dolt_commit('-Am', 'base');
DROP INDEX idx;
SELECT dolt_commit('-Am', 'drop index');
SELECT dolt_revert('HEAD');
" "
CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE INDEX idx ON t(v);
INSERT INTO t VALUES (1, 10), (2, 10);
SELECT dolt_commit('-Am', 'base');
ALTER TABLE t DROP INDEX idx;
SELECT dolt_commit('-Am', 'drop index');
SELECT dolt_revert('HEAD');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT group_concat(id, ',') FROM (SELECT id FROM t WHERE v = 10 ORDER BY id))" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT group_concat(id ORDER BY id SEPARATOR ',') FROM t WHERE v = 10))"

oracle_error "revert_view_add_after_modify_conflicts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_commit('-Am', 'base');
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_commit('-Am', 'add view');
DROP VIEW w;
CREATE VIEW w AS SELECT v FROM t;
SELECT dolt_commit('-Am', 'modify view');
SELECT dolt_revert('HEAD~1');
"

oracle_poststate "revert_preserves_unstaged_view" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_commit('-Am', 'base');
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-am', 'data');
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_revert('HEAD');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE type = 'view') || '|' || (SELECT count(*) FROM t)" \
"SELECT concat('Q|', (SELECT count(*) FROM dolt_schemas WHERE type = 'view'), '|', (SELECT count(*) FROM t))"

oracle_poststate "cherry_pick_view_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_commit('-Am', 'add view');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE type = 'view') || '|' || (SELECT group_concat(id, ',') FROM (SELECT id FROM w ORDER BY id))" \
"SELECT concat('Q|', (SELECT count(*) FROM dolt_schemas WHERE type = 'view'), '|', (SELECT group_concat(id ORDER BY id SEPARATOR ',') FROM w))"

oracle_error "cherry_pick_view_add_conflicts_same_name" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_commit('-Am', 'feat view');
SELECT dolt_checkout('main');
CREATE VIEW w AS SELECT v FROM t;
SELECT dolt_commit('-Am', 'main view');
SELECT dolt_cherry_pick('feat');
"

oracle_poststate "cherry_pick_view_drop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE VIEW w AS SELECT id FROM t;
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
DROP VIEW w;
SELECT dolt_commit('-Am', 'drop view');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "$DL_VCOUNT" "$DT_VCOUNT"

oracle_poststate "cherry_pick_index_add" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
CREATE INDEX idx ON t(v);
SELECT dolt_commit('-Am', 'add index');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 10);
SELECT dolt_commit('-am', 'main data');
SELECT dolt_cherry_pick('feat');
" "SELECT 'Q|' || (SELECT count(*) FROM sqlite_master WHERE name = 'idx') || '|' || (SELECT group_concat(id, ',') FROM (SELECT id FROM t WHERE v = 10 ORDER BY id))" \
"SELECT concat('Q|', (SELECT count(*) FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx'), '|', (SELECT group_concat(id ORDER BY id SEPARATOR ',') FROM t WHERE v = 10))"

oracle_dual_poststate "revert_trigger_add_stops_firing" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE audit(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END;
SELECT dolt_commit('-Am', 'add trigger');
SELECT dolt_revert('HEAD');
INSERT INTO t VALUES (5);
" "
CREATE TABLE t(id INT PRIMARY KEY);
CREATE TABLE audit(id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.id);
SELECT dolt_commit('-Am', 'add trigger');
SELECT dolt_revert('HEAD');
INSERT INTO t VALUES (5);
" "SELECT 'Q|' || count(*) FROM audit" \
"SELECT concat('Q|', count(*)) FROM audit"

oracle_dual_poststate "revert_trigger_drop_fires_again" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE audit(id INTEGER PRIMARY KEY);
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END;
SELECT dolt_commit('-Am', 'base');
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'drop trigger');
SELECT dolt_revert('HEAD');
INSERT INTO t VALUES (5);
" "
CREATE TABLE t(id INT PRIMARY KEY);
CREATE TABLE audit(id INT PRIMARY KEY);
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.id);
SELECT dolt_commit('-Am', 'base');
DROP TRIGGER trg;
SELECT dolt_commit('-Am', 'drop trigger');
SELECT dolt_revert('HEAD');
INSERT INTO t VALUES (5);
" "SELECT 'Q|' || count(*) || '|' || coalesce((SELECT id FROM audit), '~') FROM audit" \
"SELECT concat('Q|', count(*), '|', coalesce((SELECT id FROM audit), '~')) FROM audit"

oracle_dual_poststate "cherry_pick_trigger_add_fires" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
CREATE TABLE audit(id INTEGER PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END;
SELECT dolt_commit('-Am', 'add trigger');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
INSERT INTO t VALUES (5);
" "
CREATE TABLE t(id INT PRIMARY KEY);
CREATE TABLE audit(id INT PRIMARY KEY);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feat');
CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.id);
SELECT dolt_commit('-Am', 'add trigger');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
INSERT INTO t VALUES (5);
" "SELECT 'Q|' || count(*) || '|' || coalesce((SELECT id FROM audit), '~') FROM audit" \
"SELECT concat('Q|', count(*), '|', coalesce((SELECT id FROM audit), '~')) FROM audit"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
