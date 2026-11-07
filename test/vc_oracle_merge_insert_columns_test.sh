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

for layout in rowid clustered composite; do
  case "$layout" in
    rowid) schema='id INTEGER PRIMARY KEY,a INT' ;;
    clustered) schema='id INT PRIMARY KEY,a INT' ;;
    composite) schema='id INT,a INT,tenant INT DEFAULT 0,PRIMARY KEY(tenant,id)' ;;
  esac
  for direction in forward reverse; do
    for kind in value null default explicit-null mismatch shared-null dual-equal dual-null disjoint; do
      name="${layout}_${direction}_${kind}"
      dir="$TMPROOT/$name"
      mkdir -p "$dir/dt"
      setup="CREATE TABLE t($schema); CREATE INDEX ia ON t(a);
INSERT INTO t(id,a) VALUES(1,1);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');"
      our_a=5
      column='c INT'
      their_c=9
      conflict=0
      case "$kind" in
        null) their_c=NULL ;;
        default) column='c INT NOT NULL DEFAULT 7'; their_c=7 ;;
        explicit-null) column='c INT DEFAULT 7'; their_c=NULL ;;
        mismatch) our_a=6; conflict=1 ;;
        shared-null) our_a=NULL; conflict=1 ;;
        dual-equal|dual-null) setup+='ALTER TABLE t ADD COLUMN c INT;' ;;
        disjoint) setup+='ALTER TABLE t ADD COLUMN d INT;' ;;
      esac
      case "$kind" in
        dual-equal) setup+="INSERT INTO t(id,a,c) VALUES(5,$our_a,9);" ;;
        dual-null) setup+="INSERT INTO t(id,a,c) VALUES(5,$our_a,NULL);"; conflict=1 ;;
        disjoint) setup+="INSERT INTO t(id,a,d) VALUES(5,$our_a,8);" ;;
        *) setup+="INSERT INTO t(id,a) VALUES(5,$our_a);" ;;
      esac
      setup+="SELECT dolt_commit('-Am','ours'); SELECT dolt_checkout('side');
ALTER TABLE t ADD COLUMN $column;
INSERT INTO t(id,a,c) VALUES(5,5,$their_c);"
      if [ "$kind" != disjoint ]; then setup+='CREATE INDEX ic ON t(c);'; fi
      setup+="SELECT dolt_commit('-Am','theirs');"
      target=main
      if [ "$direction" = forward ]; then
        setup+="SELECT dolt_checkout('main');"
        target=side
      fi
      probe="SELECT concat('C|',count(*)) FROM dolt_conflicts;"
      expected="C|$conflict"
      if [ "$conflict" -eq 0 ]; then
        probe+="SELECT concat('R|',id,'|',a,'|',coalesce(c,'NULL')) FROM t WHERE id=5;"
        expected+="$(printf '\nR|5|5|%s' "$their_c")"
        if [ "$kind" = disjoint ]; then
          probe+="SELECT concat('D|',d) FROM t WHERE id=5;"
          expected=$(printf '%s\nD|8' "$expected" | sort)
        fi
      fi
      script="$setup
BEGIN;
SELECT dolt_merge('$target');
$probe
ROLLBACK;"
      vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" "$script" --allow-error
      dl_rc=$?
      vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
        "$(vc_oracle_translate_for_dolt "$script")" -r csv
      dt_rc=$?
      if [ "$dl_rc" -ne "$conflict" ] || [ "$dt_rc" -ne 0 ]; then
        vc_oracle_assert_match "$name execution" failed success
        cat "$dir/dl.err" "$dir/dt.err"
        continue
      fi
      dl_out=$(tr -d '\r' <"$dir/dl.out" | grep -E '^[CRD]\|' | sort)
      dt_out=$(tr -d '"\r' <"$dir/dt.out" | grep -E '^[CRD]\|' | sort)
      vc_oracle_assert_match "$name reference" "$dt_out" "$expected"
      vc_oracle_assert_match "$name merge" "$dl_out" "$dt_out"
    done
  done
done

for key in INT TEXT; do
  for direction in forward reverse; do
    name="key_only_${key}_${direction}"
    dir="$TMPROOT/$name"
    mkdir -p "$dir/dt"
    setup="CREATE TABLE t(id $key PRIMARY KEY);
INSERT INTO t VALUES(1); SELECT dolt_commit('-Am','base');
SELECT dolt_branch('side'); INSERT INTO t VALUES(5);
SELECT dolt_commit('-Am','ours'); SELECT dolt_checkout('side');
ALTER TABLE t ADD COLUMN c INT; INSERT INTO t VALUES(5,9);
SELECT dolt_commit('-Am','theirs');"
    target=main
    if [ "$direction" = forward ]; then
      setup+="SELECT dolt_checkout('main');"
      target=side
    fi
    script="$setup SELECT dolt_merge('$target');
SELECT concat('R|',id,'|',c) FROM t WHERE id=5;"
    vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" "$script"
    dl_rc=$?
    dt_script=$(vc_oracle_translate_for_dolt "$script" \
      | sed 's/id TEXT PRIMARY KEY/id VARCHAR(64) PRIMARY KEY/g')
    vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
      "$dt_script" -r csv
    dt_rc=$?
    if [ "$dl_rc" -ne 0 ] || [ "$dt_rc" -ne 0 ]; then
      vc_oracle_assert_match "$name execution" failed success
      cat "$dir/dl.err" "$dir/dt.err"
      continue
    fi
    dl_out=$(tr -d '\r' <"$dir/dl.out" | grep '^R|')
    dt_out=$(tr -d '"\r' <"$dir/dt.out" | grep '^R|')
    vc_oracle_assert_match "$name reference" "$dt_out" 'R|5|9'
    vc_oracle_assert_match "$name merge" "$dl_out" "$dt_out"
  done
done

for key in INTEGER INT; do
  for direction in forward reverse; do
    for operation in merge cherry_pick; do
      for kind in clean null_b differ mirror; do
        name="add_drop_${key}_${direction}_${operation}_${kind}"
        dir="$TMPROOT/$name"
        mkdir -p "$dir/dt"
        conflict=0
        case "$kind" in
          mirror)
            ours="ALTER TABLE t DROP COLUMN b; INSERT INTO t(id,a) VALUES(5,1);"
            theirs="ALTER TABLE t ADD COLUMN c INT; INSERT INTO t(id,a,b,c) VALUES(5,1,2,NULL);"
            ;;
          null_b)
            ours="ALTER TABLE t ADD COLUMN c INT; INSERT INTO t(id,a,b,c) VALUES(5,1,NULL,NULL);"
            theirs="ALTER TABLE t DROP COLUMN b; INSERT INTO t(id,a) VALUES(5,1);"
            ;;
          differ)
            ours="ALTER TABLE t ADD COLUMN c INT; INSERT INTO t(id,a,b,c) VALUES(5,1,2,NULL);"
            theirs="ALTER TABLE t DROP COLUMN b; INSERT INTO t(id,a) VALUES(5,9);"
            conflict=1
            ;;
          *)
            ours="ALTER TABLE t ADD COLUMN c INT; INSERT INTO t(id,a,b,c) VALUES(5,1,2,NULL);"
            theirs="ALTER TABLE t DROP COLUMN b; INSERT INTO t(id,a) VALUES(5,1);"
            ;;
        esac
        setup="CREATE TABLE t(id $key PRIMARY KEY,a INT,b INT); CREATE INDEX ia ON t(a);
INSERT INTO t(id,a,b) VALUES(1,0,0);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');
$ours
SELECT dolt_commit('-Am','ours'); SELECT dolt_checkout('side');
$theirs
SELECT dolt_commit('-Am','theirs');"
        target=main
        if [ "$direction" = forward ]; then
          setup+="SELECT dolt_checkout('main');"
          target=side
        fi
        probe="SELECT concat('C|',count(*)) FROM dolt_conflicts;"
        schema_dl=""
        schema_dt=""
        expected="C|$conflict"
        if [ "$conflict" -eq 0 ]; then
          probe+="SELECT concat('R|',id,'|',a,'|',coalesce(c,'NULL')) FROM t ORDER BY id;"
          schema_dl="SELECT concat('S|',group_concat(name, ',')) FROM (SELECT name FROM pragma_table_info('t') ORDER BY cid);"
          schema_dt="SELECT CONCAT('S|',GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',')) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='t';"
          expected=$(printf '%s\nR|1|0|NULL\nR|5|1|NULL\nS|id,a,c' "$expected")
        fi
        script="$setup
BEGIN;
SELECT dolt_${operation}('$target');
$probe
ROLLBACK;"
        vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" \
          "$script
$schema_dl" --allow-error
        dl_rc=$?
        vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
          "$(vc_oracle_translate_for_dolt "$script")
$schema_dt" -r csv
        dt_rc=$?
        if [ "$dl_rc" -ne "$conflict" ] || [ "$dt_rc" -ne 0 ]; then
          vc_oracle_assert_match "$name execution" failed success
          cat "$dir/dl.err" "$dir/dt.err"
          continue
        fi
        dl_out=$(tr -d '\r' <"$dir/dl.out" | grep -E '^[CRS]\|' | sort)
        dt_out=$(tr -d '"\r' <"$dir/dt.out" | grep -E '^[CRS]\|' | sort)
        vc_oracle_assert_match "$name reference" "$dt_out" "$(printf '%s\n' "$expected" | sort)"
        vc_oracle_assert_match "$name merge" "$dl_out" "$dt_out"
      done
    done
  done
done

vc_oracle_finish
