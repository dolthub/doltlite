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
    | sort -t$'\t' -k3 \
    | awk -F'\t' '
        {
          h = $2
          if (!(h in seen)) { n++; seen[h] = "H" n }
          print "L\t" seen[h] "\t" $3
        }
      '
}

normalize_status() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 4 && $1 == "S" { print }' \
    | sort -t$'\t' -k2,2 -k3,3 -k4,4
}

oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_log dl_status
  dl_log=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'L' || char(9) || commit_hash || char(9) || message FROM dolt_log;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize_log)
  dl_status=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S' || char(9) || table_name || char(9) || staged || char(9) || status FROM dolt_status;\n" "$setup" \
              | "$DOLTLITE" "$dir/dl/db.s" 2>>"$dir/dl.err" \
              | grep -v '^[0-9]*$' \
              | grep -v '^[0-9a-f]\{40\}$' \
              | normalize_status)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_log dt_status
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('L', char(9), commit_hash, char(9), message) FROM dolt_log ORDER BY commit_order DESC;" 2>>"$dir/dt.err"
  ) > "$dir/dt.log.raw"
  dt_log=$(tail -n +2 "$dir/dt.log.raw" | tr -d '"' | normalize_log)

  (
    cd "$dir/dt.s" 2>/dev/null || mkdir -p "$dir/dt.s" && cd "$dir/dt.s" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.s.err"
    "$DOLT" sql -r csv -q "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.status.raw"
  dt_status=$(tail -n +2 "$dir/dt.status.raw" | tr -d '"' | normalize_status)

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_status"
  dt_combined="$dt_log"$'\n'"$dt_status"

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

oracle_error_match() {
  local name="$1" setup="$2" pattern="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"

  if grep -qE "$pattern" "$dir/dl.err" "$dir/dl.out" \
    && grep -qE "$pattern" "$dir/dt.err" "$dir/dt.out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to match error pattern)"
    echo "    pattern: $pattern"
  fi
}

oracle_same_session() {
  local name="$1" dl_setup="$2" dl_query="$3" dolt_setup="${4:-$2}" dolt_query="${5:-$3}"
  local dir="$TMPROOT/${name}_ss"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(
    {
      printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s\n" "$dl_setup" "$dl_query"
    } | "$DOLTLITE" "$dir/dl/db" 2>&1 \
      | tr -d '\r' \
      | awk '/^Q\|/ {print; next} /[Nn]o such savepoint:|SAVEPOINT .*does not exist/ {print "E|savepoint"}'
  )

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$(vc_oracle_translate_for_dolt "$dolt_setup")"
      printf '%s\n' "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>&1 \
      | tail -n +2 \
      | tr -d '"\r' \
      | awk '/^Q\|/ {print; next} /[Nn]o such savepoint:|SAVEPOINT .*does not exist/ {print "E|savepoint"}'
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: dolt_reset ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- reset with no ref (unstage) ---"

oracle "reset_no_args_unstages_all" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_reset();
"

oracle "reset_soft_no_ref_keeps_staged" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_reset('--soft');
"

oracle "reset_hard_no_ref_clears_everything" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
INSERT INTO t VALUES (3, 30);
SELECT dolt_reset('--hard');
"

oracle "reset_no_changes_to_unstage" "
$SEED
SELECT dolt_reset();
"

oracle "reset_hard_no_changes" "
$SEED
SELECT dolt_reset('--hard');
"

echo "--- reset with ref (move HEAD) ---"

oracle "reset_soft_to_previous_commit" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--soft', 'HEAD~1');
"

oracle "reset_hard_to_previous_commit" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'HEAD~1');
"

oracle "reset_hard_to_branch_name" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'feature');
"

oracle "reset_hard_to_tag" "
$SEED
SELECT dolt_tag('release-1');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'release-1');
"

oracle "reset_hard_to_commit_hash" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', (SELECT commit_hash FROM dolt_log WHERE message = 'c1'));
"

oracle "reset_hard_to_current_head_noop" "
$SEED
SELECT dolt_reset('--hard', 'HEAD');
"

oracle "reset_hard_with_uncommitted_modifications" "
$SEED
INSERT INTO t VALUES (2, 20);
INSERT INTO t VALUES (3, 30);
SELECT dolt_reset('--hard');
"

echo "--- schema-edge hard reset ---"

oracle_same_session "reset_hard_head_parent_restores_dropped_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE a;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop-a');
SELECT dolt_reset('--hard', 'HEAD~1');
" "SELECT 'Q|cols|' || group_concat(name || ':' || replace(lower(type), 'integer', 'int'), '|')
       FROM pragma_table_info('a');
SELECT 'Q|rows|' || count(*) FROM a;
SELECT 'Q|val|' || s FROM a;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE a;
CALL dolt_add('-A');
CALL dolt_commit('-m', 'drop-a');
CALL dolt_reset('--hard', 'HEAD~1');
" "SELECT concat('Q|cols|', group_concat(concat(column_name, ':', replace(lower(column_type), 'integer', 'int')) ORDER BY ordinal_position SEPARATOR '|'))
       FROM information_schema.columns
      WHERE table_schema = database() AND table_name = 'a';
SELECT concat('Q|rows|', count(*)) FROM a;
SELECT concat('Q|val|', s) FROM a;"

oracle_same_session "reset_hard_head_parent_restores_recreated_table_schema" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'recreate-a');
SELECT dolt_reset('--hard', 'HEAD~1');
" "SELECT 'Q|cols|' || group_concat(name || ':' || replace(lower(type), 'integer', 'int'), '|')
       FROM pragma_table_info('a');
SELECT 'Q|rows|' || count(*) FROM a;
SELECT 'Q|val|' || s FROM a;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
CALL dolt_add('-A');
CALL dolt_commit('-m', 'recreate-a');
CALL dolt_reset('--hard', 'HEAD~1');
" "SELECT concat('Q|cols|', group_concat(concat(column_name, ':', replace(lower(column_type), 'integer', 'int')) ORDER BY ordinal_position SEPARATOR '|'))
       FROM information_schema.columns
      WHERE table_schema = database() AND table_name = 'a';
SELECT concat('Q|rows|', count(*)) FROM a;
SELECT concat('Q|val|', s) FROM a;"

oracle_same_session "reset_hard_tag_restores_pre_alter_schema" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_tag('v1');
ALTER TABLE a ADD COLUMN extra INT;
UPDATE a SET extra = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'v1');
" "SELECT 'Q|cols|' || group_concat(name || ':' || replace(lower(type), 'integer', 'int'), '|')
       FROM pragma_table_info('a');
SELECT 'Q|rows|' || count(*) FROM a;
SELECT 'Q|val|' || s FROM a;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
CALL dolt_tag('v1');
ALTER TABLE a ADD COLUMN extra INT;
UPDATE a SET extra = 99 WHERE id = 1;
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c2');
CALL dolt_reset('--hard', 'v1');
" "SELECT concat('Q|cols|', group_concat(concat(column_name, ':', replace(lower(column_type), 'integer', 'int')) ORDER BY ordinal_position SEPARATOR '|'))
       FROM information_schema.columns
      WHERE table_schema = database() AND table_name = 'a';
SELECT concat('Q|rows|', count(*)) FROM a;
SELECT concat('Q|val|', s) FROM a;"

oracle_same_session "reset_hard_head_parent_ref_restores_pre_alter_schema" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE a ADD COLUMN extra INT;
UPDATE a SET extra = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'HEAD^1');
" "SELECT 'Q|cols|' || group_concat(name || ':' || replace(lower(type), 'integer', 'int'), '|')
       FROM pragma_table_info('a');
SELECT 'Q|rows|' || count(*) FROM a;
SELECT 'Q|val|' || s FROM a;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
ALTER TABLE a ADD COLUMN extra INT;
UPDATE a SET extra = 99 WHERE id = 1;
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c2');
CALL dolt_reset('--hard', 'HEAD^1');
" "SELECT concat('Q|cols|', group_concat(concat(column_name, ':', replace(lower(column_type), 'integer', 'int')) ORDER BY ordinal_position SEPARATOR '|'))
       FROM information_schema.columns
      WHERE table_schema = database() AND table_name = 'a';
SELECT concat('Q|rows|', count(*)) FROM a;
SELECT concat('Q|val|', s) FROM a;"

oracle_same_session "reset_hard_head_second_parent_restores_merge_parent" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO a VALUES (2, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard', 'HEAD^2');
" "SELECT 'Q|count|' || count(*) FROM a;
SELECT 'Q|vals|' || group_concat(s, '|') FROM (SELECT s FROM a ORDER BY id);" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'base');
CALL dolt_branch('feat');
CALL dolt_checkout('feat');
INSERT INTO a VALUES (2, 'feat');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'feat');
CALL dolt_checkout('main');
INSERT INTO a VALUES (3, 'main');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'main');
CALL dolt_merge('feat');
CALL dolt_reset('--hard', 'HEAD^2');
" "SELECT concat('Q|count|', count(*)) FROM a;
SELECT concat('Q|vals|', group_concat(s ORDER BY id SEPARATOR '|')) FROM a;"

oracle_same_session "reset_hard_raw_hash_second_parent_restores_merge_parent" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO a VALUES (2, 'feat');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat');
SELECT dolt_checkout('main');
INSERT INTO a VALUES (3, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main');
SELECT dolt_merge('feat');
SELECT dolt_reset('--hard', (SELECT dolt_hashof('HEAD^2')));
" "SELECT 'Q|count|' || count(*) FROM a;
SELECT 'Q|vals|' || group_concat(s, '|') FROM (SELECT s FROM a ORDER BY id);" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'base');
CALL dolt_branch('feat');
CALL dolt_checkout('feat');
INSERT INTO a VALUES (2, 'feat');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'feat');
CALL dolt_checkout('main');
INSERT INTO a VALUES (3, 'main');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'main');
CALL dolt_merge('feat');
CALL dolt_reset('--hard', HASHOF('HEAD^2'));
" "SELECT concat('Q|count|', count(*)) FROM a;
SELECT concat('Q|vals|', group_concat(s ORDER BY id SEPARATOR '|')) FROM a;"

echo "--- table-name positional unstage ---"

oracle "reset_specific_table_unstages_only_that" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO a VALUES (2, 20);
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_reset('a');
"

oracle "reset_multiple_paths_unstages_all" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO a VALUES (2, 20);
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_reset('a', 'b');
"

oracle "reset_multiple_paths_with_missing_unstages_all" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO a VALUES (2, 20);
INSERT INTO b VALUES (2, 200);
SELECT dolt_add('-A');
SELECT dolt_reset('a', 'nope');
"

oracle "reset_multiple_missing_paths_unstages_all" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO a VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_reset('nope', 'nope2');
"

oracle_same_session "reset_path_dropped_table_stays_dropped" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE a;
SELECT dolt_reset('a');
" "SELECT 'Q|' || count(*) FROM dolt_status
      WHERE table_name='a' AND staged=0 AND status='deleted';
SELECT 'Q|' || count(*) FROM dolt_status
      WHERE table_name='a' AND staged=1;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 10);
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE a;
CALL dolt_reset('a');
" "SELECT concat('Q|', count(*)) FROM dolt_status
      WHERE table_name='a' AND staged=false AND status='deleted';
SELECT concat('Q|', count(*)) FROM dolt_status
      WHERE table_name='a' AND staged=true;"

oracle_same_session "reset_path_recreated_table_stays_recreated" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_reset('a');
" "SELECT 'Q|' || k || '|' || n FROM a;
SELECT 'Q|' || count(*) FROM dolt_status
      WHERE table_name='a' AND staged=0 AND status='modified';" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
CALL dolt_reset('a');
" "SELECT concat('Q|', k, '|', n) FROM a;
SELECT concat('Q|', count(*)) FROM dolt_status
      WHERE table_name='a' AND staged=false AND status='modified';"

oracle_same_session "reset_path_staged_dropped_indexed_table_unstages_drop" "
CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT CHECK (length(s) > 0));
CREATE UNIQUE INDEX a_s_idx ON a(s);
INSERT INTO a VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE a;
SELECT dolt_add('-A');
SELECT dolt_reset('a');
" "SELECT 'Q|unstaged|' || count(*) FROM dolt_status
      WHERE table_name='a' AND staged=0 AND status='deleted';
SELECT 'Q|staged|' || count(*) FROM dolt_status
      WHERE table_name='a' AND staged=1;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, s TEXT CHECK (length(s) > 0));
CREATE UNIQUE INDEX a_s_idx ON a(s);
INSERT INTO a VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE a;
CALL dolt_add('-A');
CALL dolt_reset('a');
" "SELECT concat('Q|unstaged|', count(*)) FROM dolt_status
      WHERE table_name='a' AND staged=false AND status='deleted';
SELECT concat('Q|staged|', count(*)) FROM dolt_status
      WHERE table_name='a' AND staged=true;"

oracle_same_session "reset_path_staged_dropped_fk_table_unstages_drop" "
CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES p(id));
INSERT INTO p VALUES (1);
INSERT INTO c VALUES (10, 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE c;
SELECT dolt_add('-A');
SELECT dolt_reset('c');
" "SELECT 'Q|unstaged|' || count(*) FROM dolt_status
      WHERE table_name='c' AND staged=0 AND status='deleted';
SELECT 'Q|staged|' || count(*) FROM dolt_status
      WHERE table_name='c' AND staged=1;
SELECT 'Q|parent|' || count(*) FROM p;" \
"CREATE TABLE p(id INTEGER PRIMARY KEY);
CREATE TABLE c(id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES p(id));
INSERT INTO p VALUES (1);
INSERT INTO c VALUES (10, 1);
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE c;
CALL dolt_add('-A');
CALL dolt_reset('c');
" "SELECT concat('Q|unstaged|', count(*)) FROM dolt_status
      WHERE table_name='c' AND staged=false AND status='deleted';
SELECT concat('Q|staged|', count(*)) FROM dolt_status
      WHERE table_name='c' AND staged=true;
SELECT concat('Q|parent|', count(*)) FROM p;"

echo "--- hard reset preserves untracked schema objects ---"

oracle_same_session "reset_hard_preserves_untracked_index_view_trigger" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE t SET v='dirty' WHERE id=1;
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
CREATE UNIQUE INDEX u_v_idx ON u(v);
CREATE TABLE audit(id INTEGER PRIMARY KEY);
CREATE TRIGGER u_ai AFTER INSERT ON u BEGIN INSERT INTO audit VALUES (NEW.id); END;
CREATE VIEW uv AS SELECT id, v FROM u;
INSERT INTO u VALUES (1, 'one');
SELECT dolt_reset('--hard');
INSERT INTO u VALUES (2, 'two');
" "SELECT 'Q|tracked|' || v FROM t;
SELECT 'Q|u|' || id || '|' || v FROM u ORDER BY id;
SELECT 'Q|idx|' || count(*) FROM pragma_index_list('u') WHERE name='u_v_idx';
SELECT 'Q|view|' || count(*) FROM uv;
SELECT 'Q|audit|' || count(*) FROM audit;" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
UPDATE t SET v='dirty' WHERE id=1;
CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT CHECK (length(v) > 0));
CREATE UNIQUE INDEX u_v_idx ON u(v);
CREATE TABLE audit(id INTEGER PRIMARY KEY);
CREATE TRIGGER u_ai AFTER INSERT ON u FOR EACH ROW INSERT INTO audit VALUES (NEW.id);
CREATE VIEW uv AS SELECT id, v FROM u;
INSERT INTO u VALUES (1, 'one');
CALL dolt_reset('--hard');
INSERT INTO u VALUES (2, 'two');
" "SELECT concat('Q|tracked|', v) FROM t;
SELECT concat('Q|u|', id, '|', v) FROM u ORDER BY id;
SELECT concat('Q|idx|', count(*)) FROM information_schema.statistics
      WHERE table_name='u' AND index_name='u_v_idx';
SELECT concat('Q|view|', count(*)) FROM uv;
SELECT concat('Q|audit|', count(*)) FROM audit;"

oracle_same_session "reset_hard_preserves_untracked_tables_after_target_recreate" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'a1');
INSERT INTO b VALUES (1, 'b1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'recreate-a');
CREATE TABLE u1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u1 VALUES (11, 'u1');
INSERT INTO u2 VALUES (22, 'u2');
SELECT dolt_reset('--hard', 'HEAD~1');
" "SELECT 'Q|a_cols|' || group_concat(name || ':' || replace(lower(type), 'integer', 'int'), '|')
       FROM pragma_table_info('a');
SELECT 'Q|a|' || id || '|' || v FROM a;
SELECT 'Q|b|' || count(*) FROM b;
SELECT 'Q|u1|' || id || '|' || v FROM u1;
SELECT 'Q|u2|' || id || '|' || v FROM u2;" \
"CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'a1');
INSERT INTO b VALUES (1, 'b1');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'c1');
DROP TABLE a;
CREATE TABLE a(k INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO a VALUES (7, 70);
CALL dolt_add('-A');
CALL dolt_commit('-m', 'recreate-a');
CREATE TABLE u1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO u1 VALUES (11, 'u1');
INSERT INTO u2 VALUES (22, 'u2');
CALL dolt_reset('--hard', 'HEAD~1');
" "SELECT concat('Q|a_cols|', group_concat(concat(column_name, ':', replace(lower(column_type), 'integer', 'int')) ORDER BY ordinal_position SEPARATOR '|'))
       FROM information_schema.columns
      WHERE table_schema = database() AND table_name = 'a';
SELECT concat('Q|a|', id, '|', v) FROM a;
SELECT concat('Q|b|', count(*)) FROM b;
SELECT concat('Q|u1|', id, '|', v) FROM u1;
SELECT concat('Q|u2|', id, '|', v) FROM u2;"

echo "--- error paths ---"

oracle_error "reset_to_nonexistent_ref" "
$SEED
SELECT dolt_reset('--hard', 'nope');
"

oracle_error "reset_unknown_flag" "
$SEED
SELECT dolt_reset('--bogus');
"

oracle_error "reset_mixed_no_ref_unsupported" "
$SEED
SELECT dolt_reset('--mixed');
"

oracle_error "reset_mixed_with_ref_unsupported" "
$SEED
SELECT dolt_reset('--mixed', 'HEAD');
"

oracle_error "reset_soft_hard_mutually_exclusive" "
$SEED
SELECT dolt_reset('--soft', '--hard');
"

oracle_error "reset_soft_hard_mutually_exclusive_with_ref" "
$SEED
SELECT dolt_reset('--soft', '--hard', 'HEAD');
"

echo "--- merge conflict guards ---"

oracle_error_match "reset_no_args_during_merge_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_reset();
" "(Merge conflict detected|cannot merge: conflicts detected)"

oracle_error_match "reset_soft_during_merge_conflict" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('feature');
UPDATE t SET v = 99 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
UPDATE t SET v = 11 WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
SELECT dolt_reset('--soft');
" "(Merge conflict detected|cannot merge: conflicts detected)"

echo "--- savepoint parity ---"

oracle_same_session "reset_hard_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-A', '-m', 'c1');
SAVEPOINT sp1;
SELECT dolt_reset('--hard', 'HEAD');
" "SELECT 'Q|' || v FROM t;
ROLLBACK TO sp1;" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_commit('-A', '-m', 'c1');
SAVEPOINT sp1;
CALL dolt_reset('--hard', 'HEAD');
" "SELECT concat('Q|', v) FROM t;
ROLLBACK TO sp1;"

oracle_same_session "reset_bad_ref_savepoint_invalidated" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-A', '-m', 'c1');
UPDATE t SET v='dirty';
SAVEPOINT sp1;
SELECT dolt_reset('--hard', 'bogus');
" "SELECT 'Q|' || v FROM t;
ROLLBACK TO sp1;" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_commit('-A', '-m', 'c1');
UPDATE t SET v='dirty';
SAVEPOINT sp1;
CALL dolt_reset('--hard', 'bogus');
" "SELECT concat('Q|', v) FROM t;
ROLLBACK TO sp1;"

oracle_same_session "reset_bad_ref_nested_savepoint_rolls_back_locally" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_commit('-A', '-m', 'c1');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES (2, 'dirty');
SELECT dolt_reset('--hard', 'bogus');
" "ROLLBACK TO sp1;
COMMIT;
SELECT 'Q|' || count(*) FROM t;" \
"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_commit('-A', '-m', 'c1');
BEGIN;
SAVEPOINT sp1;
INSERT INTO t VALUES (2, 'dirty');
CALL dolt_reset('--hard', 'bogus');
" "ROLLBACK TO sp1;
COMMIT;
SELECT concat('Q|', count(*)) FROM t;"

echo "--- unstaging a staged drop keeps it in the working tree ---"

oracle "reset_path_staged_dropped_table_unstages_drop" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE s(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO s VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE s;
SELECT dolt_add('-A');
SELECT dolt_reset('s');
"

oracle "reset_soft_staged_new_table_stays_new" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
CREATE TABLE n(id INTEGER PRIMARY KEY);
INSERT INTO n VALUES (7);
SELECT dolt_add('-A');
SELECT dolt_reset();
"

echo "--- hard reset preserves untracked tables ---"

UNTRACKED_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE s(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'one');
INSERT INTO s VALUES (1, 'ess');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
DROP TABLE s;
UPDATE t SET v = 'modified' WHERE id = 1;
CREATE TABLE u(x INTEGER PRIMARY KEY, w TEXT);
INSERT INTO u VALUES (9, 'kept');
SELECT dolt_reset('--hard');
"

oracle "reset_hard_preserves_untracked_status" "$UNTRACKED_SEED"

oracle_same_session "reset_hard_preserves_untracked_contents" "$UNTRACKED_SEED" \
  "SELECT concat('Q|t|', id, '|', v) FROM t;
SELECT concat('Q|s|', id, '|', v) FROM s;
SELECT concat('Q|u|', x, '|', w) FROM u;"

echo "--- table takes precedence over a same-named ref ---"

oracle "reset_path_prefers_table_over_same_named_branch" "
CREATE TABLE x(a INT PRIMARY KEY);
INSERT INTO x VALUES (1);
SELECT dolt_add('x');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('x');
INSERT INTO x VALUES (2);
SELECT dolt_add('x');
SELECT dolt_commit('-m', 'c2');
INSERT INTO x VALUES (3);
SELECT dolt_add('x');
SELECT dolt_reset('x');
"

oracle "reset_path_prefers_staged_drop_over_same_named_branch" "
CREATE TABLE x(a INT PRIMARY KEY);
INSERT INTO x VALUES (1);
SELECT dolt_add('x');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('x');
CREATE TABLE y(b INT PRIMARY KEY);
SELECT dolt_add('y');
SELECT dolt_commit('-m', 'c2');
DROP TABLE x;
SELECT dolt_add('x');
SELECT dolt_reset('x');
"

oracle "reset_bare_ref_still_moves_head" "
$SEED
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('HEAD~1');
"

echo "--- reset path: staged objects reset whole (indexes, renames) ---"

oracle "reset_path_staged_new_indexed_table_unstages_cleanly" "
$SEED
CREATE TABLE t2(a INTEGER PRIMARY KEY, b INT);
CREATE INDEX t2i ON t2(b);
INSERT INTO t2 VALUES (1, 2);
SELECT dolt_add('t2');
SELECT dolt_reset('t2');
"

oracle_error "reset_path_staged_new_indexed_table_commit_errors" "
$SEED
CREATE TABLE t2(a INTEGER PRIMARY KEY, b INT);
CREATE INDEX t2i ON t2(b);
INSERT INTO t2 VALUES (1, 2);
SELECT dolt_add('t2');
SELECT dolt_reset('t2');
SELECT dolt_commit('-m', 'nothing is staged');
"

RENAME_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE INDEX tvi ON t(v);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
ALTER TABLE t RENAME TO t2;
SELECT dolt_add('t2');
"

oracle "reset_path_staged_rename_new_name_noop" "
$RENAME_SEED
SELECT dolt_reset('t2');
"

oracle "reset_path_staged_rename_new_name_then_commit_clean" "
$RENAME_SEED
SELECT dolt_reset('t2');
SELECT dolt_commit('-m', 'commit the still-staged rename');
"

oracle "reset_path_staged_rename_old_name_unstages_whole" "
$RENAME_SEED
SELECT dolt_reset('t');
"

oracle_error "reset_path_staged_rename_old_name_commit_errors" "
$RENAME_SEED
SELECT dolt_reset('t');
SELECT dolt_commit('-m', 'nothing is staged');
"

oracle "reset_path_staged_index_only_change_unstages" "
$SEED
CREATE INDEX ti ON t(v);
SELECT dolt_add('t');
SELECT dolt_reset('t');
"

oracle_error "reset_path_staged_index_only_change_commit_errors" "
$SEED
CREATE INDEX ti ON t(v);
SELECT dolt_add('t');
SELECT dolt_reset('t');
SELECT dolt_commit('-m', 'nothing is staged');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
