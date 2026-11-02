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

query="SELECT concat('R|',coalesce(d.to_id,''),'|',coalesce(d.to_v,''),'|',
  coalesce(d.from_id,''),'|',coalesce(d.from_v,''),'|',d.diff_type,'|',
  coalesce(l.message,d.to_commit)) FROM dolt_diff_t d
  LEFT JOIN dolt_log l ON l.commit_hash=d.to_commit;
SELECT concat('C|',count(*)) FROM dolt_diff_t;"

for layout in clustered keyless composite; do
  case "$layout" in
    clustered) schema="CREATE TABLE t(id INT PRIMARY KEY,v INT)" ;;
    keyless) schema="CREATE TABLE t(id INT,v INT)" ;;
    composite) schema="CREATE TABLE t(id INT,v INT,tenant INT DEFAULT 0,PRIMARY KEY(tenant,id))" ;;
  esac
  for gap in 0 2; do
    for phase in empty committed working; do
      name="${layout}_${gap}_${phase}"
      dir="$TMPROOT/$name"
      mkdir -p "$dir/dt"
      setup="$schema;
INSERT INTO t(id,v) VALUES(1,10);
SELECT dolt_commit('-Am','old table');
DROP TABLE t;
SELECT dolt_commit('-Am','drop old table');"
      for ((i=0;i<gap;i++)); do
        setup+="SELECT dolt_commit('--allow-empty','-m','gap $i');"
      done
      setup+="$schema;"
      expected='C|0'
      if [ "$phase" != empty ]; then
        setup+="INSERT INTO t(id,v) VALUES(2,20);"
        message=WORKING
        [ "$phase" != committed ] || message=recreate
        expected="$(printf 'C|1\nR|2|20|||added|%s' "$message")"
      fi
      if [ "$phase" != working ]; then
        setup+="SELECT dolt_commit('-Am','recreate');"
      fi

      if ! vc_oracle_run_doltlite_script "$dir/db" "$dir/dl.out" "$dir/dl.err" "$setup" \
        || ! printf '%s\n' "$query" | vc_oracle_run_doltlite "$dir/db" >"$dir/dl-query.out" 2>>"$dir/dl.err"; then
        vc_oracle_assert_match "$name execution" failed success
        cat "$dir/dl.err"
        continue
      fi
      dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
      if ! vc_oracle_run_dolt_setup_query "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup" "$query"; then
        vc_oracle_assert_match "$name reference execution" failed success
        cat "$dir/dt.err"
        continue
      fi
      dl_out=$(tr -d '\r' <"$dir/dl-query.out" | sort)
      dt_out=$(tr -d '"\r' <"$dir/dt.out" | grep -E '^[RC]\|' | sort)
      vc_oracle_assert_match "$name reference rows" "$dt_out" "$expected"
      vc_oracle_assert_match "$name diff history" "$dl_out" "$dt_out"
    done
  done
done

vc_oracle_finish
