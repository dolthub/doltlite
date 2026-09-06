#!/bin/bash
set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0; FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

oracle() {
  local name="$1" indexed="$2" diverged="$3" merge_sql="$4"
  local dir="$TMPROOT/$name"
  local index_sql="" main_sql="" setup dl_out dt_out dl_rc dt_rc
  mkdir -p "$dir/dl" "$dir/dt"
  if [ "$indexed" = 1 ]; then
    index_sql="CREATE INDEX runtime_jobs_kind ON runtime_jobs(kind);"
  fi
  if [ "$diverged" = 1 ]; then
    main_sql="UPDATE items SET label='main' WHERE id=2;
      SELECT dolt_add('items'); SELECT dolt_commit('-m','main');"
  fi
  setup="CREATE TABLE items(id INTEGER PRIMARY KEY,label VARCHAR(64));
    INSERT INTO items VALUES(1,'base'),(2,'base');
    INSERT INTO dolt_ignore VALUES('runtime_%',1);
    SELECT dolt_add('items','dolt_ignore'); SELECT dolt_commit('-m','base');
    CREATE TABLE runtime_jobs(id INTEGER PRIMARY KEY,kind VARCHAR(64));
    INSERT INTO runtime_jobs VALUES(1,'keep');
    $index_sql
    SELECT dolt_branch('feature'); SELECT dolt_checkout('feature');
    UPDATE items SET label='feature' WHERE id=1;
    CREATE INDEX item_label ON items(label);
    SELECT dolt_add('items'); SELECT dolt_commit('-m','feature');
    SELECT dolt_checkout('main');
    $main_sql
    SELECT dolt_reset('--hard');
    SELECT concat('R|before|',count(*)) FROM dolt_status;
    $merge_sql
    SELECT concat('R|item|',id,'|',label) FROM items ORDER BY id;
    SELECT concat('R|runtime|',id,'|',kind) FROM runtime_jobs ORDER BY id;
    SELECT concat('R|after|',table_name,'|',staged,'|',status)
      FROM dolt_status ORDER BY table_name,staged;"
  printf '.bail on\n%s\n' "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  dl_rc=$?
  vc_oracle_run_dolt_script "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
    "$(vc_oracle_translate_for_dolt "$setup")" -r csv
  dt_rc=$?
  if [ "$dl_rc" -ne 0 ] || [ "$dt_rc" -ne 0 ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "FAIL: $name (DoltLite=$dl_rc, Dolt=$dt_rc)"
    cat "$dir/dl.err" "$dir/dt.err"
    return
  fi
  dl_out=$(grep '^R|' "$dir/dl.out" | tr -d '\r' | sort)
  dt_out=$(grep '^R|' "$dir/dt.out" | tr -d '\r"' | sort)
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

for indexed in 0 1; do
  oracle "ff_$indexed" "$indexed" 0 "SELECT dolt_merge('feature');"
  oracle "noff_$indexed" "$indexed" 0 "SELECT dolt_merge('--no-ff','feature');"
  oracle "threeway_$indexed" "$indexed" 1 "SELECT dolt_merge('feature');"
  oracle "nocommit_$indexed" "$indexed" 1 \
    "SELECT dolt_merge('--no-commit','feature'); SELECT dolt_commit('-m','finish');"
  oracle "abort_$indexed" "$indexed" 1 \
    "SELECT dolt_merge('--no-commit','feature');
     UPDATE runtime_jobs SET kind='updated' WHERE id=1;
     SELECT dolt_merge('--abort');"
done

echo "Results: $pass passed, $fail failed"
if [ "$fail" -ne 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
