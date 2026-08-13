#!/bin/bash
#
# Oracle coverage for dolt_verify_constraints.
# Doltlite exposes SELECT dolt_verify_constraints(...) -> 0/1.
# Dolt exposes CALL dolt_verify_constraints(...) -> violations column.
# Both sides are normalized to R|rc=N lines plus optional follow-up R|* rows.

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
  tr -d '\r' | sed -E 's/, /,/g' | sort
}

translate_setup_for_dolt() {
  vc_oracle_translate_for_dolt "$1" \
    | sed -E \
      -e 's/PRAGMA[[:space:]]+foreign_keys[[:space:]]*=[[:space:]]*OFF/SET FOREIGN_KEY_CHECKS=0/Ig' \
      -e 's/PRAGMA[[:space:]]+foreign_keys[[:space:]]*=[[:space:]]*ON/SET FOREIGN_KEY_CHECKS=1/Ig' \
      -e 's/PRAGMA[[:space:]]+foreign_keys[[:space:]]*=[[:space:]]*0/SET FOREIGN_KEY_CHECKS=0/Ig' \
      -e 's/PRAGMA[[:space:]]+foreign_keys[[:space:]]*=[[:space:]]*1/SET FOREIGN_KEY_CHECKS=1/Ig'
}

# name setup verify_args follow_sql allow_empty
# verify_args: empty string, or SQL arg list like "'--all'" or "'child'"
run_oracle() {
  local name="$1" setup="$2" verify_args="$3" follow="$4" allow_empty="$5"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_verify
  if [ -z "$verify_args" ]; then
    dl_verify="SELECT CONCAT('R|rc=', dolt_verify_constraints());"
  else
    dl_verify="SELECT CONCAT('R|rc=', dolt_verify_constraints($verify_args));"
  fi

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n%s\n%s\n" \
             "$setup" "$dl_verify" "$follow" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^R|' \
           | tr -d '"' \
           | normalize)

  local dolt_setup call_sql
  dolt_setup=$(translate_setup_for_dolt "$setup")
  if [ -z "$verify_args" ]; then
    call_sql="CALL dolt_verify_constraints();"
  else
    call_sql="CALL dolt_verify_constraints($verify_args);"
  fi

  # One Dolt session: setup + CALL + capture return + follow queries.
  local dt_raw
  dt_raw=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    {
      printf 'SET @@autocommit = 0;\n'
      printf 'SET @@dolt_force_transaction_commit = 1;\n'
      printf 'SET @@dolt_allow_commit_conflicts = 1;\n'
      printf '%s\n' "$dolt_setup"
      printf '%s\n' "$call_sql"
      printf '%s\n' "$(vc_oracle_translate_for_dolt "$follow")"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )

  # CALL emits a header "violations" then 0/1. Grab the last 0/1 line before R| rows.
  local dt_rc
  dt_rc=$(printf '%s\n' "$dt_raw" | tr -d '"' | awk '
    $0=="0" || $0=="1" { v=$0 }
    END { if (v=="") print "missing"; else print v }
  ')
  local dt_follow
  dt_follow=$(printf '%s\n' "$dt_raw" | tr -d '"' | grep '^R|' | normalize)

  local dt_out
  if [ "$dt_rc" = "missing" ]; then
    dt_out=$(printf '%s\n' "$dt_follow" | normalize)
  else
    dt_out=$(printf 'R|rc=%s\n%s\n' "$dt_rc" "$dt_follow" | grep -v '^$' | normalize)
  fi

  if [ "$allow_empty" = "1" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi

  if [ "$fail" -gt 0 ] && echo "$FAILED_NAMES" | grep -q " $name\|$name"; then
    echo "    doltlite stderr: $(tr '\n' ' ' <"$dir/dl.err" | head -c 300)"
    echo "    dolt stderr:     $(tr '\n' ' ' <"$dir/dt.err" | head -c 300)"
    echo "    dolt raw:        $(printf '%s' "$dt_raw" | tr '\n' '|' | head -c 400)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_verify_constraints ==="
echo ""

FK_SETUP="
CREATE TABLE parent(pk INTEGER PRIMARY KEY, v INT UNIQUE);
CREATE TABLE child(pk INTEGER PRIMARY KEY, v INT REFERENCES parent(v));
CREATE TABLE otherTable(pk INTEGER PRIMARY KEY);
INSERT INTO parent VALUES (1,10),(2,20);
SELECT dolt_commit('-Am', 'init');
PRAGMA foreign_keys=OFF;
INSERT INTO child VALUES (1,99);
PRAGMA foreign_keys=ON;
"

# CONCAT + backticks: Dolt is MySQL-dialect (|| is logical OR; "table" is a string).
FOLLOW_AGG="SELECT CONCAT('R|agg=', tn, ':', nv) FROM (SELECT \`table\` AS tn, num_violations AS nv FROM dolt_constraint_violations) x ORDER BY 1;"
FOLLOW_AGG_COUNT="SELECT CONCAT('R|agg_count=', count(*)) FROM dolt_constraint_violations;"
FOLLOW_CHILD_ROWS="SELECT CONCAT('R|row=', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' WHEN 1 THEN 'FK' WHEN 2 THEN 'UQ' WHEN 3 THEN 'CK' ELSE '?' END, ':', pk) FROM dolt_constraint_violations_child ORDER BY pk;"

echo "--- FK: working-set orphan ---"
run_oracle "fk_working_set" "$FK_SETUP" "" \
  "$FOLLOW_AGG
$FOLLOW_CHILD_ROWS" 0

echo "--- FK: named table with violations ---"
run_oracle "fk_named_child" "$FK_SETUP" "'child'" \
  "$FOLLOW_AGG" 0

echo "--- FK: named table with no violations ---"
run_oracle "fk_named_other" "$FK_SETUP" "'otherTable'" \
  "$FOLLOW_AGG_COUNT" 1


# A merge records violations up front. Verifying an unrelated table must not
# retract them: the commit gate reads that record, so clearing it wholesale
# turns a scoped re-check into permission to commit violating rows.
MERGE_CV_SETUP="
CREATE TABLE parent(pk INTEGER PRIMARY KEY);
CREATE TABLE child(pk INTEGER PRIMARY KEY, pid INT REFERENCES parent(pk));
CREATE TABLE otherTable(pk INTEGER PRIMARY KEY);
INSERT INTO parent VALUES (1);
INSERT INTO otherTable VALUES (1);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('br');
SELECT dolt_checkout('br');
INSERT INTO child VALUES (1,1);
SELECT dolt_commit('-Am', 'child row');
SELECT dolt_checkout('main');
DELETE FROM parent WHERE pk=1;
SELECT dolt_commit('-Am', 'drop parent');
BEGIN;
SELECT dolt_merge('br');
"

echo "--- FK: merge-recorded violation survives a scoped verify ---"
run_oracle "fk_merge_cv_survives_named_other" "$MERGE_CV_SETUP" "'otherTable'" \
  "$FOLLOW_AGG_COUNT" 1

echo "--- FK: default clean after force-commit ---"
run_oracle "fk_default_clean_after_commit" \
"$FK_SETUP
SELECT dolt_commit('-Am', 'commit orphan', '--force');
" \
"" \
"$FOLLOW_AGG_COUNT" 1

echo "--- FK: --all after force-commit ---"
run_oracle "fk_all_after_commit" \
"$FK_SETUP
SELECT dolt_commit('-Am', 'commit orphan', '--force');
" \
"'--all'" \
"$FOLLOW_AGG" 0

echo "--- FK: --all --output-only ---"
run_oracle "fk_output_only" \
"$FK_SETUP
SELECT dolt_commit('-Am', 'commit orphan', '--force');
" \
"'--all', '--output-only'" \
"$FOLLOW_AGG_COUNT" 1

UNIQUE_SETUP="
CREATE TABLE otherTable(pk INTEGER PRIMARY KEY);
CREATE TABLE t(pk INTEGER PRIMARY KEY, col1 INT UNIQUE);
SELECT dolt_commit('-Am', 'init');
SELECT dolt_branch('branch1');
INSERT INTO t VALUES (1, 1);
SELECT dolt_commit('-Am', 'insert on main');
SELECT dolt_checkout('branch1');
INSERT INTO t VALUES (2, 1);
SELECT dolt_commit('-Am', 'insert on branch1');
SELECT dolt_checkout('main');
BEGIN;
SELECT dolt_merge('branch1');
COMMIT;
"

FOLLOW_T_ROWS="SELECT CONCAT('R|row=', CASE violation_type WHEN 'foreign key' THEN 'FK' WHEN 'unique index' THEN 'UQ' WHEN 'check constraint' THEN 'CK' WHEN 1 THEN 'FK' WHEN 2 THEN 'UQ' WHEN 3 THEN 'CK' ELSE '?' END, ':', pk, ':', col1) FROM dolt_constraint_violations_t ORDER BY pk;"

echo "--- unique: after merge ---"
run_oracle "unique_after_merge" "$UNIQUE_SETUP" "" \
  "$FOLLOW_AGG
$FOLLOW_T_ROWS" 0

echo "--- unique: re-verify after clearing ---"
run_oracle "unique_reverify" \
"$UNIQUE_SETUP
DELETE FROM dolt_constraint_violations_t;
" \
"" \
"$FOLLOW_AGG
$FOLLOW_T_ROWS" 0

echo "--- unique: named otherTable ---"
run_oracle "unique_named_other" \
"$UNIQUE_SETUP
DELETE FROM dolt_constraint_violations_t;
" \
"'otherTable'" \
"$FOLLOW_AGG_COUNT" 1

echo "--- unique: --all after force-commit ---"
run_oracle "unique_all_after_commit" \
"$UNIQUE_SETUP
SELECT dolt_commit('-Am', 'commit with violations', '--force');
DELETE FROM dolt_constraint_violations_t;
" \
"'--all'" \
"$FOLLOW_AGG
$FOLLOW_T_ROWS" 0

echo "--- unique: --all --output-only ---"
run_oracle "unique_output_only" \
"$UNIQUE_SETUP
SELECT dolt_commit('-Am', 'commit with violations', '--force');
DELETE FROM dolt_constraint_violations_t;
" \
"'--all', '--output-only'" \
"$FOLLOW_AGG_COUNT" 1

echo "--- unique: --output-only preserves recorded violations ---"
run_oracle "unique_output_only_preserves_recorded" \
"$UNIQUE_SETUP
DELETE FROM t WHERE pk=2;
" \
"'--output-only'" \
"$FOLLOW_AGG
$FOLLOW_T_ROWS" 0

echo "--- clean database ---"
run_oracle "clean_no_violations" \
"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT UNIQUE);
INSERT INTO t VALUES (1,1),(2,2);
SELECT dolt_commit('-Am', 'clean');
" \
"" \
"$FOLLOW_AGG_COUNT" 1

run_oracle "clean_all_no_violations" \
"
CREATE TABLE t(pk INTEGER PRIMARY KEY, v INT UNIQUE);
INSERT INTO t VALUES (1,1),(2,2);
SELECT dolt_commit('-Am', 'clean');
" \
"'--all'" \
"$FOLLOW_AGG_COUNT" 1

echo ""
echo "Results: $pass passed, $fail failed"
if [ "$fail" -ne 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
exit 0
