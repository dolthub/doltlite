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

normalize_table() {
  tr -d '\r' \
    | awk -F'\t' 'NF >= 2 && $1 == "T" { print }' \
    | sort
}

oracle() {
  local name="$1" setup="$2" tables="${3:-t}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local table_query=""
  IFS=',' read -ra tarr <<< "$tables"
  for tn in "${tarr[@]}"; do
    if [ -z "$table_query" ]; then
      table_query="SELECT 'T' || char(9) || '$tn' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM $tn"
    else
      table_query="$table_query UNION ALL SELECT 'T' || char(9) || '$tn' || char(9) || coalesce(id,'') || char(9) || coalesce(v,'') FROM $tn"
    fi
  done

  local dl_log dl_status dl_table
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
  dl_table=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$table_query" \
             | "$DOLTLITE" "$dir/dl/db.t" 2>>"$dir/dl.err" \
             | grep -v '^[0-9]*$' \
             | grep -v '^[0-9a-f]\{40\}$' \
             | normalize_table)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  local dt_log dt_status dt_table
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
    "$DOLT" sql -r csv -q "SELECT concat('S', char(9), table_name, char(9), staged, char(9), status) FROM dolt_status;" 2>>"$dir/dt.s.err"
  ) > "$dir/dt.status.raw"
  dt_status=$(tail -n +2 "$dir/dt.status.raw" | tr -d '"' | normalize_status)

  local dolt_table_query=""
  for tn in "${tarr[@]}"; do
    local part="SELECT concat('T', char(9), '$tn', char(9), coalesce(id,''), char(9), coalesce(v,'')) FROM $tn"
    if [ -z "$dolt_table_query" ]; then
      dolt_table_query="$part"
    else
      dolt_table_query="$dolt_table_query UNION ALL $part"
    fi
  done
  (
    mkdir -p "$dir/dt.t" && cd "$dir/dt.t" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.t.err"
    "$DOLT" sql -r csv -q "$dolt_table_query;" 2>>"$dir/dt.t.err"
  ) > "$dir/dt.table.raw"
  dt_table=$(tail -n +2 "$dir/dt.table.raw" | tr -d '"' | normalize_table)

  if [ -z "$dl_log" ] && [ -z "$dt_log" ] && [ -z "$dl_table" ] && [ -z "$dt_table" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (everything empty on both sides — harness bug)"
    return
  fi

  local dl_combined dt_combined
  dl_combined="$dl_log"$'\n'"$dl_status"$'\n'"$dl_table"
  dt_combined="$dt_log"$'\n'"$dt_status"$'\n'"$dt_table"

  vc_oracle_assert_match "$name" "$dl_combined" "$dt_combined"
}

echo "=== Version Control Oracle Tests: HEAD / staged / working state transitions ==="
echo ""

SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
"

echo "--- reset moving things between stages ---"

oracle "reset_unstages_while_working_has_separate_diff" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
INSERT INTO t VALUES (4, 40);
SELECT dolt_reset();
"

oracle "hard_reset_undoes_stage_then_working_delete" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
DELETE FROM t WHERE id = 3;
SELECT dolt_reset('--hard');
"

oracle "mixed_reset_preserves_subsequent_working_change" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 200 WHERE id = 1;
SELECT dolt_reset();
"

oracle "hard_reset_wipes_both_staged_and_working" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 200 WHERE id = 1;
SELECT dolt_reset('--hard');
"

oracle "table_reset_unstages_only_named_table" "
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
" "a,b"

oracle "mixed_reset_to_prev_commit_moves_diff_to_working" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('HEAD~1');
"

oracle "hard_reset_to_prev_commit_drops_diff_entirely" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--hard', 'HEAD~1');
"

echo "--- checkout moving things between stages ---"

oracle "checkout_branch_clean_swaps_working" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
"

oracle "checkout_branch_per_branch_working_set" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
"

oracle "checkout_b_starts_from_head_not_working" "
$SEED
SELECT dolt_branch('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_checkout('feature');
SELECT dolt_checkout('-b', 'feature2');
SELECT dolt_checkout('main');
"

oracle "checkout_table_reverts_only_named_table" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v INT);
INSERT INTO a VALUES (1, 10);
INSERT INTO b VALUES (1, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
UPDATE a SET v = 999 WHERE id = 1;
UPDATE b SET v = 999 WHERE id = 1;
SELECT dolt_checkout('a');
" "a,b"

oracle "checkout_table_clears_both_staged_and_working" "
$SEED
UPDATE t SET v = 100 WHERE id = 1;
SELECT dolt_add('-A');
UPDATE t SET v = 200 WHERE id = 1;
SELECT dolt_checkout('t');
"

echo "--- full cycle ---"

oracle "soft_reset_uncommit_then_recommit" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('--soft', 'HEAD~1');
SELECT dolt_commit('-m', 'c2-recommitted');
"

oracle "mixed_reset_uncommit_then_readd_recommit" "
$SEED
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
SELECT dolt_reset('HEAD~1');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2-take-two');
"

oracle "branch_round_trip_preserves_each_side" "
$SEED
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat1');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SELECT dolt_checkout('feature');
SELECT dolt_checkout('main');
"

echo "--- edge cases ---"

oracle "reset_undoes_staged_table_deletion" "
$SEED
DROP TABLE t;
SELECT dolt_add('-A');
SELECT dolt_reset('--hard');
"

oracle "mixed_reset_does_not_touch_untracked_new_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO u VALUES (1, 99);
SELECT dolt_reset();
" "t,u"

oracle "hard_reset_with_untracked_new_table" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO u VALUES (1, 99);
SELECT dolt_reset('--hard');
" "t,u"

oracle "hard_reset_drops_table_added_after_target" "
$SEED
CREATE TABLE u(id INTEGER PRIMARY KEY, v INT);
INSERT INTO u VALUES (1, 99);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_u');
SELECT dolt_reset('--hard', 'HEAD~1');
" "t"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
