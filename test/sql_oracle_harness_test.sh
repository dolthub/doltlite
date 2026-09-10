#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ORACLE="${1:-$SCRIPT_DIR/sql_oracle_test.sh}"
SQL_ORACLE_TMP=$(mktemp -d)
trap 'rm -rf "$SQL_ORACLE_TMP"' EXIT
source "$SCRIPT_DIR/lib/sql_oracle_common.sh"

DOLTLITE="$SQL_ORACLE_TMP/candidate"
SQLITE3="$SQL_ORACLE_TMP/reference"
cat > "$DOLTLITE" <<'EOF'
#!/usr/bin/env bash
cat >/dev/null
if [ "${expect_unsafe:-0}" = 1 ] && [ "$1" != --unsafe-testing ]; then
  exit 99
fi
case "$0" in
  */candidate) printf '%s' "$candidate_output"; exit "$candidate_rc" ;;
  */reference) printf '%s' "$reference_output"; exit "$reference_rc" ;;
esac
EOF
cp "$DOLTLITE" "$SQLITE3"
chmod +x "$DOLTLITE" "$SQLITE3"

check_case() {
  local name="$1" want_fail="$2" kind="$3" error="${4-}"
  local pass=0 fail=0
  if [ "$kind" = error ]; then
    oracle_error "$name" "SELECT 42;" "$error" > "$SQL_ORACLE_TMP/case.log"
  elif [ "$kind" = unsafe ]; then
    oracle_unsafe "$name" "SELECT 42;" > "$SQL_ORACLE_TMP/case.log"
  else
    oracle "$name" "SELECT 42;" > "$SQL_ORACLE_TMP/case.log"
  fi
  if [ "$fail" -ne "$want_fail" ] || [ "$pass" -ne "$((1-want_fail))" ]; then
    echo "FAIL: $name (pass=$pass fail=$fail)"
    cat "$SQL_ORACLE_TMP/case.log"
    exit 1
  fi
}

export candidate_rc=0 reference_rc=0 candidate_output=42 reference_output=42
check_case successful_query 0 success
export candidate_output= reference_output=
check_case successful_empty_output 0 success
export candidate_rc=1 reference_rc=1
check_case matching_silent_failures 1 success
export candidate_output='shared error' reference_output='shared error'
check_case matching_unexpected_errors 1 success
check_case matching_expected_error 0 error 'shared error'
check_case wrong_expected_error 1 error 'different error'
export candidate_rc=0 reference_rc=0
check_case expected_error_did_not_fail 1 error 'shared error'
export candidate_rc=1 reference_rc=0
check_case candidate_only_failure 1 success
export candidate_rc=0 reference_rc=1
check_case reference_only_failure 1 success
export candidate_rc=0 reference_rc=0 candidate_output=41 reference_output=42
check_case different_rows 1 success
export candidate_rc=134 reference_rc=134
check_case matching_crashes 1 success
export candidate_output='shared error' reference_output='shared error'
check_case crashing_expected_error 1 error 'shared error'
export candidate_rc=127 reference_rc=127
check_case matching_launch_errors 1 error 'shared error'
export candidate_rc=1 reference_rc=1 candidate_output= reference_output=
check_case empty_expected_error 1 error 'shared error'
export candidate_output=$'Error near line 3: UNIQUE constraint failed: t.x (19)\n1'
export reference_output=$'Runtime error near line 7: UNIQUE constraint failed: t.x (19)\n1'
check_case normalized_expected_error 0 error 'UNIQUE constraint failed: t.x'
export candidate_rc=0 reference_rc=0 candidate_output=42 reference_output=42 expect_unsafe=1
check_case unsafe_flag_preserved 0 unsafe
unset expect_unsafe

expect_startup_failure() {
  local name="$1" candidate="$2" reference="$3"
  if bash "$ORACLE" "$candidate" "$reference" > "$SQL_ORACLE_TMP/startup.log" 2>&1; then
    echo "FAIL: oracle accepted $name"
    tail -5 "$SQL_ORACLE_TMP/startup.log"
    exit 1
  fi
}

cat > "$SQL_ORACLE_TMP/fails" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
cat > "$SQL_ORACLE_TMP/empty" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$SQL_ORACLE_TMP/pretends" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' 'sql-oracle-ready|42'
EOF
chmod +x "$SQL_ORACLE_TMP/fails" "$SQL_ORACLE_TMP/empty" "$SQL_ORACLE_TMP/pretends"
expect_startup_failure silent_failure "$SQL_ORACLE_TMP/fails" "$SQL_ORACLE_TMP/fails"
expect_startup_failure empty_success "$SQL_ORACLE_TMP/empty" "$SQL_ORACLE_TMP/empty"
expect_startup_failure missing_executable "$SQL_ORACLE_TMP/missing" "$SQL_ORACLE_TMP/missing"
expect_startup_failure no_stock_database "$SQL_ORACLE_TMP/pretends" "$SQL_ORACLE_TMP/pretends"

echo "SQL oracle harness: 20 checks passed"
