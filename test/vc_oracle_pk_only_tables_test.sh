#!/bin/bash
# PK-only rows store an empty value record; other oracles' non-PK columns masked NULL-decode bugs.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E '
    s/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g
    s/dolt_diff_([a-zA-Z0-9_]+)\(([^)]*)\)/dolt_diff(\2, "\1")/g
  '
}

oracle() {
  local name="$1" setup="$2" query="$3" dolt_query_override="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  if [ -n "$dolt_query_override" ]; then
    dolt_query="$dolt_query_override"
  else
    dolt_query=$(echo "$query" | translate_for_dolt)
  fi

  local dt_out
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Oracle Tests: PK-covers-all-columns tables ==="
echo ""

SETUP_LINK="
CREATE TABLE link(p INTEGER, c INTEGER, PRIMARY KEY(p, c));
INSERT INTO link VALUES (1, 2), (1, 3), (2, 4);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM link WHERE p = 1 AND c = 3;
INSERT INTO link VALUES (5, 6);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

echo "--- Group A: composite INT PK, no other columns ---"

oracle "a_diff_table" "$SETUP_LINK" \
  "SELECT CONCAT('R|', IFNULL(to_p,''), '|', IFNULL(to_c,''), '|', IFNULL(from_p,''), '|', IFNULL(from_c,''), '|', diff_type) FROM dolt_diff_link;"

oracle "a_diff_range" "$SETUP_LINK" \
  "SELECT CONCAT('R|', IFNULL(to_p,''), '|', IFNULL(to_c,''), '|', IFNULL(from_p,''), '|', IFNULL(from_c,''), '|', diff_type) FROM dolt_diff_link('HEAD~1', 'HEAD');"

oracle "a_history" "$SETUP_LINK" \
  "SELECT CONCAT('R|', IFNULL(p,''), '|', IFNULL(c,'')) FROM dolt_history_link;"

oracle "a_as_of" "$SETUP_LINK" \
  "SELECT CONCAT('R|', p, '|', c) FROM dolt_at_link WHERE commit_ref = 'HEAD~1';" \
  "SELECT CONCAT('R|', p, '|', c) FROM link AS OF 'HEAD~1';"

echo "--- Group B: single VARCHAR PK, no other columns ---"

SETUP_TAGS="
CREATE TABLE tags(name VARCHAR(30), PRIMARY KEY(name));
INSERT INTO tags VALUES ('alpha'), ('beta');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM tags WHERE name = 'beta';
INSERT INTO tags VALUES ('gamma');
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "b_diff_table" "$SETUP_TAGS" \
  "SELECT CONCAT('R|', IFNULL(to_name,''), '|', IFNULL(from_name,''), '|', diff_type) FROM dolt_diff_tags;"

oracle "b_history" "$SETUP_TAGS" \
  "SELECT CONCAT('R|', IFNULL(name,'')) FROM dolt_history_tags;"

oracle "b_as_of" "$SETUP_TAGS" \
  "SELECT CONCAT('R|', name) FROM dolt_at_tags WHERE commit_ref = 'HEAD~1';" \
  "SELECT CONCAT('R|', name) FROM tags AS OF 'HEAD~1';"

echo "--- Group C: mixed VARCHAR+INT composite PK, no other columns ---"

SETUP_MIX="
CREATE TABLE ev(kind VARCHAR(20), seq INTEGER, PRIMARY KEY(kind, seq));
INSERT INTO ev VALUES ('boot', 1), ('boot', 2), ('halt', 1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
DELETE FROM ev WHERE kind = 'boot' AND seq = 2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'c2');
"

oracle "c_diff_table" "$SETUP_MIX" \
  "SELECT CONCAT('R|', IFNULL(to_kind,''), '|', IFNULL(to_seq,''), '|', IFNULL(from_kind,''), '|', IFNULL(from_seq,''), '|', diff_type) FROM dolt_diff_ev;"

oracle "c_history" "$SETUP_MIX" \
  "SELECT CONCAT('R|', IFNULL(kind,''), '|', IFNULL(seq,'')) FROM dolt_history_ev;"

echo "--- Group D: workspace (uncommitted) changes ---"

SETUP_WS="
CREATE TABLE link(p INTEGER, c INTEGER, PRIMARY KEY(p, c));
INSERT INTO link VALUES (1, 2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'seed');
INSERT INTO link VALUES (7, 8);
DELETE FROM link WHERE p = 1 AND c = 2;
"

oracle "d_workspace" "$SETUP_WS" \
  "SELECT CONCAT('R|', IFNULL(to_p,''), '|', IFNULL(to_c,''), '|', IFNULL(from_p,''), '|', IFNULL(from_c,''), '|', diff_type) FROM dolt_workspace_link;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ -n "$FAILED_NAMES" ]; then
  echo "Failed:$FAILED_NAMES"
fi
[ "$fail" -eq 0 ]
