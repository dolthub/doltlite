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

normalize() {
  tr -d '\r' | sort
}

oracle_schemas() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/${name}_schemas"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || type || char(9) || name || char(9) || fragment FROM dolt_schemas ORDER BY type, name"
  local q_dolt="SELECT concat('S', char(9), type, char(9), name, char(9), fragment) FROM dolt_schemas ORDER BY type, name"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^S' | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_diff_touches_schemas() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/${name}_diff"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'D' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('D', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_schemas_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/${name}_schemas_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || type || char(9) || name FROM dolt_schemas ORDER BY type, name"
  local q_dolt="SELECT concat('S', char(9), type, char(9), name) FROM dolt_schemas ORDER BY type, name"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$dl_setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^S' | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_diff_touches_schemas_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3"
  local dir="$TMPROOT/${name}_diff_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'D' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('D', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$dl_setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
"

echo "=== Version Control Oracle Tests: dolt_schemas ==="
echo ""

echo "--- dolt_schemas table contents ---"

oracle_schemas "no_views" "$SEED" "EXPECT_EMPTY"

oracle_schemas "one_view" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view');
"

oracle_schemas "two_views" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
CREATE VIEW low AS SELECT * FROM t WHERE v < 100;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_views');
"

oracle_schemas "view_then_drop" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view');
DROP VIEW high;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_view');
" "EXPECT_EMPTY"

oracle_schemas "uncommitted_view_visible" "
$SEED
CREATE VIEW pending AS SELECT * FROM t;
"

echo "--- dolt_diff visibility of view-touching commits ---"

oracle_diff_touches_schemas "view_only_commit" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_view');
"

oracle_diff_touches_schemas "table_only_no_schemas_touch" "
CREATE TABLE t(id INT PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_table');
" "EXPECT_EMPTY"

oracle_diff_touches_schemas "mixed_commits" "
$SEED
CREATE VIEW v1 AS SELECT * FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'view_commit_1');
INSERT INTO t VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_commit');
DROP VIEW v1;
CREATE VIEW v2 AS SELECT * FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'view_commit_2');
"

oracle_diff_touches_schemas "combined_table_and_view" "
$SEED
INSERT INTO t VALUES(3, 30);
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'combined');
"

SEED_TRIG="
$SEED
CREATE TABLE log(id INT, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_log');
"

echo "--- dolt_schemas table contents: triggers ---"

oracle_schemas_dual "one_trigger" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
"

oracle_schemas_dual "two_triggers" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.id, old.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_triggers');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES(old.id, old.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_triggers');
"

oracle_schemas_dual "trigger_then_drop" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
DROP TRIGGER t_ai;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
DROP TRIGGER t_ai;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_trigger');
" "EXPECT_EMPTY"

oracle_schemas_dual "uncommitted_trigger_visible" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
"

oracle_schemas_dual "trigger_and_view" "
$SEED_TRIG
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view_and_trigger');
" "
$SEED_TRIG
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view_and_trigger');
"

oracle_schemas_dual "trigger_on_feature_branch_only" "
$SEED_TRIG
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger_on_feat');
SELECT dolt_checkout('main');
" "
$SEED_TRIG
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger_on_feat');
SELECT dolt_checkout('main');
" "EXPECT_EMPTY"

oracle_schemas_dual "modify_trigger_single_commit" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(1, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'initial_trigger');
DROP TRIGGER t_ai;
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(2, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'replaced_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(1, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'initial_trigger');
DROP TRIGGER t_ai;
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(2, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'replaced_trigger');
"

echo "--- dolt_diff visibility of trigger-touching commits ---"

oracle_diff_touches_schemas_dual "trigger_only_commit" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_trigger');
"

oracle_diff_touches_schemas_dual "mixed_trigger_commits" "
$SEED_TRIG
CREATE TRIGGER t1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_1');
INSERT INTO t VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_commit');
DROP TRIGGER t1;
CREATE TRIGGER t2 AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.id, old.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_2');
" "
$SEED_TRIG
CREATE TRIGGER t1 AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_1');
INSERT INTO t VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_commit');
DROP TRIGGER t1;
CREATE TRIGGER t2 AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES(old.id, old.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_2');
"

oracle_diff_touches_schemas_dual "combined_table_and_trigger" "
$SEED_TRIG
INSERT INTO t VALUES(3, 30);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'combined');
" "
$SEED_TRIG
INSERT INTO t VALUES(3, 30);
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'combined');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
