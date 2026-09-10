#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_DIR="${1:-$SCRIPT_DIR}"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
source "$SCRIPT_DIR/lib/stock_oracle_common.sh"
checks=0

check_comparison() {
  local name="$1" want_fail="$2"
  shift 2
  local pass=0 fail=0 nonempty=0 FAILED_NAMES=""
  stock_oracle_assert "$name" "$@" > "$WORK/comparison.log"
  if [ "$fail" -ne "$want_fail" ] || [ "$pass" -ne "$((1-want_fail))" ]; then
    cat "$WORK/comparison.log"
    echo "FAIL: $name (pass=$pass fail=$fail)"
    exit 1
  fi
  checks=$((checks+1))
}

check_comparison matching_rows 0 42 42 0 0
check_comparison different_rows 1 41 42 0 0
check_comparison empty_rows 1 '' '' 0 0
check_comparison expected_empty 0 '' '' 0 0 '' 1
check_comparison unexpected_rows 1 42 42 0 0 '' 1
check_comparison silent_failures 1 '' '' 1 1
check_comparison failed_empty 1 '' '' 1 1 '' 1
check_comparison matching_errors 1 'SQL error' 'SQL error' 1 1
check_comparison expected_error 0 'SQL error' 'SQL error' 1 1 'SQL error'
check_comparison wrong_error 1 'SQL error' 'SQL error' 1 1 'other error'
check_comparison missing_error 1 'SQL error' 'SQL error' 0 0 'SQL error'
check_comparison candidate_failure 1 42 42 1 0
check_comparison reference_failure 1 42 42 0 1
check_comparison matching_crashes 1 'SQL error' 'SQL error' 134 134 'SQL error'
check_comparison launch_failures 1 'SQL error' 'SQL error' 127 127 'SQL error'

check_floor() {
  local name="$1" pass="$2" fail="$3" nonempty="$4" expected="$5"
  local FAILED_NAMES="" rc=0
  stock_oracle_finish > "$WORK/floor.log" || rc=$?
  if [ "$rc" -ne "$expected" ]; then
    cat "$WORK/floor.log"
    echo "FAIL: $name"
    exit 1
  fi
  checks=$((checks+1))
}
check_floor no_cases 0 0 0 1
check_floor only_empty_cases 5 0 0 1
check_floor failed_case 5 1 5 1
check_floor passing_suite 5 0 5 0

for suite in attach dot_commands foreign_keys fts5 generated_columns large_blobs \
             savepoints temp_tables triggers upsert without_rowid; do
  for engine in /usr/bin/true /usr/bin/false "$WORK/missing"; do
    if bash "$SUITE_DIR/oracle_${suite}_test.sh" "$engine" "$engine" \
        > "$WORK/startup.log" 2>&1; then
      echo "FAIL: $suite accepted $engine"
      tail -5 "$WORK/startup.log"
      exit 1
    fi
    checks=$((checks+1))
  done
done

cat > "$WORK/engine" <<'ENGINE'
#!/usr/bin/env bash
if [ "$#" -eq 2 ]; then
  case "$2" in
    "SELECT 'sql-oracle-ready',6*7;") echo 'sql-oracle-ready|42'; exit 0 ;;
    'SELECT sqlite_version();') echo '3.54.0'; exit 0 ;;
    'CREATE TABLE x(y); INSERT INTO x VALUES(1);')
      printf 'SQLite format 3\000' > "$1"; exit 0 ;;
  esac
fi
cat >/dev/null
printf '%s' "$oracle_test_output"
exit "$oracle_test_rc"
ENGINE
chmod +x "$WORK/engine"
for suite in upsert dot_commands fts5; do
  for scenario in empty failure crash; do
    export oracle_test_output='' oracle_test_rc=0
    if [ "$scenario" = failure ]; then
      export oracle_test_output='unexpected engine error' oracle_test_rc=1
    elif [ "$scenario" = crash ]; then
      export oracle_test_output='unexpected engine error' oracle_test_rc=134
    fi
    if bash "$SUITE_DIR/oracle_${suite}_test.sh" "$WORK/engine" "$WORK/engine" \
        > "$WORK/runtime.log" 2>&1; then
      echo "FAIL: $suite accepted runtime $scenario"
      tail -5 "$WORK/runtime.log"
      exit 1
    fi
    if ! grep -q 'FAIL:' "$WORK/runtime.log"; then
      cat "$WORK/runtime.log"
      echo "FAIL: $suite did not reach comparisons"
      exit 1
    fi
    checks=$((checks+1))
  done
done

echo "Stock oracle harness: $checks checks passed"
