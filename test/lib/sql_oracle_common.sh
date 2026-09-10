#!/usr/bin/env bash

sql_oracle_check_binaries() {
  local script_dir="$1" var bin resolved output
  for var in DOLTLITE SQLITE3; do
    bin="${!var}"
    if ! resolved=$(command -v "$bin") || [ ! -x "$resolved" ]; then
      echo "ERROR: not executable: $bin" >&2
      return 1
    fi
    printf -v "$var" '%s' "$resolved"
    bin="$resolved"
    if ! output=$("$bin" :memory: "SELECT 'sql-oracle-ready',6*7;" 2>&1); then
      echo "ERROR: $bin failed the SQL execution probe: $output" >&2
      return 1
    fi
    if [ "$output" != "sql-oracle-ready|42" ]; then
      echo "ERROR: $bin returned unexpected SQL probe output: $output" >&2
      return 1
    fi
  done
  bash "$script_dir/assert_stock_reference.sh" "$SQLITE3" "$DOLTLITE"
}

normalize_oracle_output() {
  LC_ALL=C sed -E \
    -e 's/^Error near line [0-9]+: /ERROR: /' \
    -e 's/^Runtime error near line [0-9]+: /ERROR: /' \
    -e 's/ \([0-9]+\)$//'
}

oracle() {
  oracle_with_flags "$1" "$2" ""
}

oracle_error() {
  oracle_with_flags "$1" "$2" "" "${3:?expected error text required}"
}

oracle_with_flags() {
  local name="$1" sql="$2" flag="$3" expected_error="${4-}"
  local dl="$SQL_ORACLE_TMP/dl_${name}.db" sq="$SQL_ORACLE_TMP/sq_${name}.db"
  local out_dl out_sq norm_dl norm_sq rc_dl=0 rc_sq=0 expected_rc=0
  if [ -n "$expected_error" ]; then expected_rc=1; fi
  rm -f "$dl" "$sq"
  out_dl=$(printf '%s\n' "$sql" | "$DOLTLITE" ${flag:+"$flag"} "$dl" 2>&1) || rc_dl=$?
  out_sq=$(printf '%s\n' "$sql" | "$SQLITE3" ${flag:+"$flag"} "$sq" 2>&1) || rc_sq=$?
  norm_dl=$(printf '%s\n' "$out_dl" | normalize_oracle_output)
  norm_sq=$(printf '%s\n' "$out_sq" | normalize_oracle_output)
  if [ "$rc_dl" -eq "$expected_rc" ] && [ "$rc_sq" -eq "$expected_rc" ] \
     && [ "$norm_dl" = "$norm_sq" ] \
     && { [ "$expected_rc" -eq 0 ] || [[ "$norm_sq" == *"$expected_error"* ]]; }; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    echo "  FAIL: $name"
    echo "    expected rc: $expected_rc${expected_error:+ ($expected_error)}"
    echo "    doltlite rc: $rc_dl"
    echo "    doltlite: $(printf '%s\n' "$out_dl" | head -3)"
    echo "    sqlite3 rc:  $rc_sq"
    echo "    sqlite3:  $(printf '%s\n' "$out_sq" | head -3)"
  fi
}

oracle_unsafe() {
  oracle_with_flags "$1" "$2" "--unsafe-testing"
}
