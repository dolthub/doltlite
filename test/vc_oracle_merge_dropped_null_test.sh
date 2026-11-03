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

check_drop_null() {
  local name="$1" setup="$2" apply="$3" conflict="$4" query="$5" expected="$6"
  local dir="$TMPROOT/$name" dl_rc dt_rc dl_out dt_out
  mkdir -p "$dir/dt"
  local script="$setup
BEGIN;
$apply
SELECT concat('C|',count(*)) FROM dolt_conflicts;
$query
ROLLBACK;"
  vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" "$script" --allow-error
  dl_rc=$?
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$(vc_oracle_translate_for_dolt "$script")" -r csv
  dt_rc=$?
  if [ "$dl_rc" -ne "$conflict" ] || [ "$dt_rc" -ne 0 ]; then
    vc_oracle_assert_match "$name execution" failed success
    cat "$dir/dl.err" "$dir/dt.err"
    return
  fi
  dl_out=$(tr -d '\r' < "$dir/dl.out" | grep -E '^[CR]\|')
  dt_out=$(tr -d '"\r' < "$dir/dt.out" | grep -E '^[CR]\|')
  vc_oracle_assert_match "$name reference" "$dt_out" "$expected"
  vc_oracle_assert_match "$name result" "$dl_out" "$dt_out"
}

for key in INTEGER INT; do
  for shape in add drop add_quiet; do
    for change in from_null to_null null_equal value_equal; do
      initial=NULL
      edited=4
      conflict=1
      case "$change" in
        to_null) initial=4; edited=NULL ;;
        null_equal) edited=NULL; conflict=0 ;;
        value_equal) initial=4; conflict=0 ;;
      esac
      ddl='ALTER TABLE t ADD COLUMN d INT DEFAULT 7;'
      [ "$shape" = drop ] && ddl='ALTER TABLE t DROP COLUMN a;'
      edit_drop='UPDATE t SET b=20 WHERE id=1;'
      row_b=20
      if [ "$shape" = add_quiet ]; then
        edit_drop=''
        row_b=1
      fi
      for operation in merge cherry_pick; do
        setup="CREATE TABLE t(id $key PRIMARY KEY,a INT,b INT,c INT);
CREATE INDEX ib ON t(b);
INSERT INTO t VALUES(1,1,1,$initial),(2,2,2,$initial);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');
$ddl
UPDATE t SET c=$edited WHERE id=1;
SELECT dolt_commit('-am','edit'); SELECT dolt_checkout('side');
ALTER TABLE t DROP COLUMN c; $edit_drop
SELECT dolt_commit('-am','drop'); SELECT dolt_checkout('main');"
        query=''
        expected="C|$conflict"
        if [ "$conflict" -eq 0 ]; then
          query="SELECT concat('R|',id,'|',b) FROM t ORDER BY id;"
          expected+="$(printf '\nR|1|%s\nR|2|2' "$row_b")"
        fi
        check_drop_null "${key}_${shape}_${change}_${operation}" "$setup" \
          "SELECT dolt_${operation}('side');" "$conflict" "$query" "$expected"
      done
    done
  done
done

for kind in from_null to_null value null_equal value_equal; do
  column='c INT'
  edited=4
  conflict=1
  case "$kind" in
    to_null) column='c INT DEFAULT 7'; edited=NULL; conflict=0 ;;
    value) column='c INT DEFAULT 7'; edited=8 ;;
    null_equal) edited=NULL; conflict=0 ;;
    value_equal) column='c INT DEFAULT 7'; edited=7; conflict=0 ;;
  esac
  setup="CREATE TABLE t(id INT PRIMARY KEY,a INT);
INSERT INTO t VALUES(1,1),(2,2); SELECT dolt_commit('-Am','base');
ALTER TABLE t ADD COLUMN $column; SELECT dolt_commit('-am','add c');
ALTER TABLE t ADD COLUMN d INT; UPDATE t SET c=$edited WHERE id=1;
SELECT dolt_commit('-am','add d and edit c');"
  check_drop_null "revert_$kind" "$setup" "SELECT dolt_revert('HEAD~1');" \
    "$conflict" '' "C|$conflict"
done

vc_oracle_finish
