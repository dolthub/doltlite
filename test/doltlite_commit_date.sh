#!/bin/bash

source "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== dolt_commit --date ==="
echo ""

DB=:memory:

setup() {
  local msg="$1"
  local date="$2"
  printf "%s" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','$msg','--date','$date');
SELECT date FROM dolt_log WHERE message='$msg';"
}

err_setup() {
  local date="$1"
  printf "%s" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','x','--date','$date');"
}

run_test_lastline "date_offset_minus" \
  "$(setup off '2024-01-15T10:00:00-05:00')" \
  "2024-01-15 15:00:00" \
  "$DB"

run_test_lastline "date_offset_plus" \
  "$(setup plus '2024-01-15T10:00:00+05:00')" \
  "2024-01-15 05:00:00" \
  "$DB"

run_test_lastline "date_zulu" \
  "$(setup z '2024-01-15T10:00:00Z')" \
  "2024-01-15 10:00:00" \
  "$DB"

run_test_lastline "date_naive_t" \
  "$(setup naive '2024-01-15T10:00:00')" \
  "2024-01-15 10:00:00" \
  "$DB"

run_test_lastline "date_only" \
  "$(setup day '2024-01-15')" \
  "2024-01-15 00:00:00" \
  "$DB"

run_test_lastline "date_unix_minus_one" \
  "$(setup epoch1 '1969-12-31T23:59:59Z')" \
  "1969-12-31 23:59:59" \
  "$DB"

run_test_lastline "date_fractional_seconds_truncated" \
  "$(setup ms '2024-01-15T10:00:00.5Z')" \
  "2024-01-15 10:00:00" \
  "$DB"

run_test_lastline "date_fractional_comma_truncated" \
  "$(setup cf '2024-01-15T10:00:00,123Z')" \
  "2024-01-15 10:00:00" \
  "$DB"

run_test_lastline "date_year_one" \
  "$(setup y1 '0001-01-01')" \
  "0001-01-01 00:00:00" \
  "$DB"

run_test_lastline "date_before_year_1000" \
  "$(setup y999 '0999-12-31T23:59:59Z')" \
  "0999-12-31 23:59:59" \
  "$DB"

run_test_lastline "date_before_1900" \
  "$(setup y1500 '1500-06-01T12:00:00+02:00')" \
  "1500-06-01 10:00:00" \
  "$DB"

run_test_lastline "date_leap_day" \
  "$(setup leap '2000-02-29')" \
  "2000-02-29 00:00:00" \
  "$DB"

run_test_lastline "date_year_9999" \
  "$(setup y9999 '9999-12-31T23:59:59Z')" \
  "9999-12-31 23:59:59" \
  "$DB"

run_test_error_match "date_century_non_leap_day_rejected" \
  "$(err_setup '1900-02-29')" \
  "could not parse --date" \
  "$DB"

run_test_error_match "date_junk_rejected" \
  "$(err_setup '2024-01-15T10:00:00junk')" \
  "could not parse --date" \
  "$DB"

run_test_error_match "date_space_rejected" \
  "$(err_setup '2024-01-15 10:00:00')" \
  "could not parse --date" \
  "$DB"

run_test_error_match "date_invalid_day_rejected" \
  "$(err_setup '2024-02-31T00:00:00Z')" \
  "could not parse --date" \
  "$DB"

run_test_error_match "date_offset_without_colon_rejected" \
  "$(err_setup '2024-01-15T10:00:00-0500')" \
  "could not parse --date" \
  "$DB"

dltest_finish
