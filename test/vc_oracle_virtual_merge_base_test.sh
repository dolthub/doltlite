#!/bin/bash
set -u
set -o pipefail
DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

check_virtual_base() {
  local name="$1" script="$2" expected="$3" rc_expected="${4:-0}"
  local dir="$TMPROOT/$name" dl_rc dt_rc dl_out dt_out
  mkdir -p "$dir/dt"
  vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" \
    "$script" --allow-error
  dl_rc=$?
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$(vc_oracle_translate_for_dolt "$script")" -r csv
  dt_rc=$?
  if [ "$dl_rc" -ne "$rc_expected" ] || [ "$dt_rc" -ne 0 ]; then
    vc_oracle_assert_match "$name execution" failed success
    cat "$dir/dl.err" "$dir/dt.err"
    return
  fi
  dl_out=$(tr -d '\r' < "$dir/dl.out" | grep -E '^[RC]\|')
  dt_out=$(tr -d '"\r' < "$dir/dt.out" | grep -E '^[RC]\|')
  vc_oracle_assert_match "$name reference" "$dt_out" "$expected"
  vc_oracle_assert_match "$name result" "$dl_out" "$dt_out"
}

for key in INTEGER INT; do
  for levels in 1 2 3; do
    for padding in left right equal; do
      setup="CREATE TABLE t(id $key PRIMARY KEY,a INT);
CREATE INDEX ia ON t(a);
INSERT INTO t VALUES(1,0),(2,0);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');"
      for ((i=1;i<=levels;i++)); do
        setup+="SELECT dolt_checkout('main'); UPDATE t SET a=$i WHERE id=1;
SELECT dolt_commit('-am','m$i');"
        if [ "$padding" = left ]; then
          setup+="SELECT dolt_commit('--allow-empty','-m','mp$i');"
        fi
        setup+="SELECT dolt_tag('m$i'); SELECT dolt_checkout('side');
UPDATE t SET a=2*$i WHERE id=2; SELECT dolt_commit('-am','o$i');"
        if [ "$padding" = right ]; then
          setup+="SELECT dolt_commit('--allow-empty','-m','op$i');"
        fi
        setup+="SELECT dolt_checkout('main');
SELECT dolt_merge('side','--no-ff','-m','mm$i');
SELECT dolt_checkout('side'); SELECT dolt_merge('m$i','--no-ff','-m','om$i');"
      done
      setup+="SELECT dolt_checkout('main'); UPDATE t SET a=0 WHERE id=2;
SELECT dolt_commit('-am','undo row 2');"
      for direction in forward reverse; do
        target=side
        checkout=''
        if [ "$direction" = reverse ]; then
          checkout="SELECT dolt_checkout('side');"
          target=main
        fi
        script="$setup
$checkout
SELECT dolt_merge('$target');
SELECT concat('R|',id,'|',a) FROM t ORDER BY id;
SELECT concat('C|',count(*)) FROM dolt_conflicts;"
        expected="$(printf 'R|1|%s\nR|2|0\nC|0' "$levels")"
        check_virtual_base "${key}_${levels}_${padding}_${direction}" "$script" "$expected"
      done
    done
  done
done

setup="CREATE TABLE t(id INT PRIMARY KEY,a INT);
INSERT INTO t VALUES(1,0),(2,0);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');
ALTER TABLE t ADD COLUMN c INT DEFAULT 7;
UPDATE t SET a=1 WHERE id=1; SELECT dolt_commit('-am','left');
SELECT dolt_tag('left'); SELECT dolt_checkout('side');
ALTER TABLE t ADD COLUMN d INT DEFAULT 9;
UPDATE t SET a=2 WHERE id=2; SELECT dolt_commit('-am','right');
SELECT dolt_checkout('main'); SELECT dolt_merge('side');
SELECT dolt_checkout('side'); SELECT dolt_merge('left');
UPDATE t SET c=8 WHERE id=1; SELECT dolt_commit('-am','edit c');
SELECT dolt_checkout('main'); UPDATE t SET a=0 WHERE id=2;
SELECT dolt_commit('-am','undo row 2');
SELECT dolt_merge('side');
SELECT concat('R|',id,'|',a,'|',c,'|',d) FROM t ORDER BY id;"
check_virtual_base schema_additions "$setup" $'R|1|1|8|9\nR|2|0|7|9'
script="$setup
SELECT dolt_reset('--hard','HEAD~1'); ALTER TABLE t ADD COLUMN extra INTEGER;
SELECT dolt_commit('-am','main schema');
SELECT dolt_checkout('side'); ALTER TABLE t ADD COLUMN extra TEXT;
SELECT dolt_commit('-am','side schema'); SELECT dolt_checkout('main');
BEGIN;
SELECT dolt_merge('side');
SELECT concat('C|',count(*)) FROM dolt_schema_conflicts;
SELECT concat('R|base|',replace(lower(base_schema),char(96),'') LIKE '%c int%',
 '|',replace(lower(base_schema),char(96),'') LIKE '%d int%') FROM dolt_schema_conflicts;
ROLLBACK;"
check_virtual_base schema_conflict_base "$script" \
  $'R|1|1|8|9\nR|2|0|7|9\nC|1\nR|base|1|1' 1

setup="CREATE TABLE t(id INT PRIMARY KEY,a INT);
INSERT INTO t VALUES(1,0),(2,0),(3,0);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('b'); SELECT dolt_branch('c');
UPDATE t SET a=1 WHERE id=1; SELECT dolt_commit('-am','a'); SELECT dolt_tag('a');
SELECT dolt_checkout('b'); UPDATE t SET a=2 WHERE id=2;
SELECT dolt_commit('-am','b'); SELECT dolt_tag('b_tip');
SELECT dolt_checkout('c'); UPDATE t SET a=3 WHERE id=3;
SELECT dolt_commit('-am','c'); SELECT dolt_tag('c_tip');
SELECT dolt_checkout('main'); SELECT dolt_merge('b'); SELECT dolt_merge('c');
SELECT dolt_checkout('b'); SELECT dolt_merge('c_tip'); SELECT dolt_merge('a');
SELECT dolt_checkout('main'); UPDATE t SET a=0 WHERE id=2;
SELECT dolt_commit('-am','undo b'); SELECT dolt_merge('b');
SELECT concat('R|',id,'|',a) FROM t ORDER BY id;"
check_virtual_base three_ancestors "$setup" $'R|1|1\nR|2|0\nR|3|3'

vc_oracle_finish
