#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0
fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

state_sqlite() {
  local label="$1"
  if [ "$label" = BEFORE ]; then
    printf "%s" "SELECT '$label|' ||
      (SELECT count(*) FROM dolt_conflicts) || '|' ||
      (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
      coalesce((SELECT group_concat(id || ':' || v, ',') FROM
        (SELECT id,v FROM t ORDER BY id)), '') || '|' ||
      (SELECT message FROM dolt_log LIMIT 1) || '|' ||
      (SELECT count(*) FROM dolt_log);"
  else
    printf "%s" "SELECT '$label|' ||
      (SELECT count(*) FROM dolt_conflicts) || '|' ||
      (SELECT count(*) FROM dolt_constraint_violations) || '|' ||
      (SELECT count(*) FROM dolt_status) || '|' ||
      coalesce((SELECT group_concat(id || ':' || v, ',') FROM
        (SELECT id,v FROM t ORDER BY id)), '') || '|' ||
      (SELECT message FROM dolt_log LIMIT 1) || '|' ||
      (SELECT count(*) FROM dolt_log);"
  fi
}

state_dolt() {
  local label="$1"
  if [ "$label" = BEFORE ]; then
    printf "%s" "SELECT CONCAT('$label|',
      (SELECT COUNT(*) FROM dolt_conflicts), '|',
      (SELECT COUNT(*) FROM dolt_constraint_violations), '|',
      COALESCE((SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',')
        FROM t), ''), '|',
      (SELECT message FROM dolt_log LIMIT 1), '|',
      (SELECT COUNT(*) FROM dolt_log));"
  else
    printf "%s" "SELECT CONCAT('$label|',
      (SELECT COUNT(*) FROM dolt_conflicts), '|',
      (SELECT COUNT(*) FROM dolt_constraint_violations), '|',
      (SELECT COUNT(*) FROM dolt_status), '|',
      COALESCE((SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',')
        FROM t), ''), '|',
      (SELECT message FROM dolt_log LIMIT 1), '|',
      (SELECT COUNT(*) FROM dolt_log));"
  fi
}

abort_oracle() {
  local name="$1" setup="$2" pick_ref="$3" abort_args="$4"
  local dir="$TMPROOT/$name"
  local dl_out dt_out dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"

  {
    printf '%s\n' "$setup"
    printf '%s\n' "BEGIN;"
    printf "SELECT dolt_cherry_pick('%s');\n" "$pick_ref"
    state_sqlite BEFORE
    printf '\nSELECT dolt_cherry_pick(%s);\n' "$abort_args"
    state_sqlite AFTER
    printf '\nCOMMIT;\n'
  } | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err" || true
  dl_out=$(tr -d '\r' < "$dir/dl.out" | grep -E '^(BEFORE|AFTER)\|' || true)

  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf '%s\n' "$dolt_setup"
      printf '%s\n' "START TRANSACTION;"
      printf "CALL dolt_cherry_pick('%s');\n" "$pick_ref"
      state_dolt BEFORE
      printf '\nCALL dolt_cherry_pick(%s);\n' "$abort_args"
      state_dolt AFTER
      printf '\nCOMMIT;\n'
    } | "$DOLT" sql -c -r csv >"$dir/dt.out" 2>"$dir/dt.err"
  ) || true
  dt_out=$(tr -d '"\r' < "$dir/dt.out" | grep -E '^(BEFORE|AFTER)\|' || true)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

error_oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  local dl_rc dt_rc dolt_setup
  mkdir -p "$dir/dl" "$dir/dt"

  vc_oracle_run_doltlite_script \
    "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error \
    "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
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

echo "=== Version Control Oracle Tests: dolt_cherry_pick --abort ==="
echo ""

CONFLICT_SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'base');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_checkout('-b', 'feature');
UPDATE t SET v='feature' WHERE id=1;
SELECT dolt_commit('-Am', 'feature edit');
SELECT dolt_checkout('main');
UPDATE t SET v='main' WHERE id=1;
SELECT dolt_commit('-Am', 'main edit');
"

abort_oracle "cherry_pick_abort_conflict" \
  "$CONFLICT_SETUP" "feature" "'--abort'"

error_oracle "cherry_pick_abort_rejects_extra_args" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','base');
SELECT dolt_cherry_pick('--abort', 'extra');
"

CV_SETUP="
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE, v TEXT);
INSERT INTO t VALUES(1,1,'base1'),(2,2,'base2');
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','feature');
UPDATE t SET u=9, v='feature' WHERE id=2;
SELECT dolt_commit('-Am','feature unique');
SELECT dolt_checkout('main');
UPDATE t SET u=9, v='main' WHERE id=1;
SELECT dolt_commit('-Am','main unique');
"

abort_oracle "cherry_pick_abort_constraint_violation" \
  "$CV_SETUP" "feature" "'--abort'"

error_oracle "cherry_pick_abort_without_active_operation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','base');
SELECT dolt_cherry_pick('--abort');
"

error_oracle "cherry_pick_abort_after_clean_pick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','base');
SELECT dolt_checkout('-b','feature');
INSERT INTO t VALUES(1,'feature');
SELECT dolt_commit('-Am','feature');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feature');
SELECT dolt_cherry_pick('--abort');
"

echo ""
echo "Results: $pass passed, $fail failed out of $((pass+fail)) tests"
if [ "$fail" -ne 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
