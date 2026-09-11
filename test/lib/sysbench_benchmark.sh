#!/usr/bin/env bash

# Caller must set TMPDIR, READ_TESTS, WRITE_TESTS, WRITE_TESTS_AC.

BENCH_BASELINE_BINARY="${BENCH_BASELINE_BINARY:-$SQLITE3}"
BENCH_CANDIDATE_BINARY="${BENCH_CANDIDATE_BINARY:-$DOLTLITE}"
BENCH_BASELINE_TIMER="${BENCH_BASELINE_TIMER:-$BENCH_TIMER_SQLITE}"
BENCH_CANDIDATE_TIMER="${BENCH_CANDIDATE_TIMER:-$BENCH_TIMER_DOLTLITE}"
BENCH_BASELINE_KIND="${BENCH_BASELINE_KIND:-sqlite}"
BENCH_CANDIDATE_KIND="${BENCH_CANDIDATE_KIND:-doltlite}"
BENCH_BASELINE_LABEL="${BENCH_BASELINE_LABEL:-SQLite}"
BENCH_CANDIDATE_LABEL="${BENCH_CANDIDATE_LABEL:-DoltLite}"
BENCH_GATE_MODE="${BENCH_GATE_MODE:-absolute}"
BENCH_SECTION_MODE="${BENCH_SECTION_MODE:-full}"

BENCH_RESULTS_FILE="$TMPDIR/bench_results.tsv"
BENCH_SAMPLES_FILE="$TMPDIR/bench_samples.tsv"
: > "$BENCH_RESULTS_FILE"
printf 'section\ttest\trun\tbaseline_us\tcandidate_us\n' \
  > "$BENCH_SAMPLES_FILE"

BENCH_REPORT_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sysbench_report.py"

run_bench() {
  local id="$1"
  local kind="$2"
  local binary="$3"
  local timer="$4"
  local sql_file="$5"
  local db_template="$6"
  local db="$db_template"
  if [ "$db" != ":memory:" ]; then
    db="/tmp/bench_${id}_${RANDOM}_$$.db"
    rm -f "$db"
  fi

  local bench_sql_file="$sql_file"
  local sqlite_pragmas="${SQLITE_BENCH_PRAGMAS:-}"
  if [ "$kind" = "sqlite" ] && [ "$db_template" != ":memory:" ]; then
    sqlite_pragmas="$SQLITE_FILE_CACHE_PRAGMA $sqlite_pragmas"
  fi
  if [ "$kind" = "sqlite" ] && [ -n "$sqlite_pragmas" ]; then
    bench_sql_file="$TMPDIR/pragma_${id}_${RANDOM}_$$.sql"
    printf "%s\n" "$sqlite_pragmas" > "$bench_sql_file"
    cat "$sql_file" >> "$bench_sql_file"
  fi

  if [ -x "$timer" ]; then
    "$timer" "$db" "$bench_sql_file"
    if [ "$db" != ":memory:" ]; then rm -f "$db"; fi
    return
  fi

  local output
  output=$(sed \
    -e "s/\.print BENCH_START/SELECT 'TS_START:' || CAST((julianday('now')*86400000000) AS INTEGER);/" \
    -e "s/\.print BENCH_END/SELECT 'TS_END:' || CAST((julianday('now')*86400000000) AS INTEGER);/" \
    "$bench_sql_file" | "$binary" "$db" 2>&1)
  if [ "$db" != ":memory:" ]; then rm -f "$db"; fi
  echo "$output" | python3 -c "
import sys, re
start = end = None
for line in sys.stdin:
    m = re.search(r'TS_START:(\\d+)', line)
    if m: start = int(m.group(1))
    m = re.search(r'TS_END:(\\d+)', line)
    if m: end = int(m.group(1))
if start is not None and end is not None:
    print(end - start)
else:
    print(-1)
"
}

bench_runs_for_test() {
  case "$1" in
    *_ac) echo "$BENCH_AC_WRITE_RUNS" ;;
    *) echo "${BENCH_RUNS:-5}" ;;
  esac
}

bench_runs_summary() {
  local base_runs="${BENCH_RUNS:-5}"
  if [ "$BENCH_AC_WRITE_RUNS" = "$base_runs" ]; then
    echo "median of ${base_runs} paired invocations per test"
  else
    echo "median of ${base_runs} paired invocations per test; autocommit writes use ${BENCH_AC_WRITE_RUNS}"
  fi
}

run_bench_pair_stable() {
  local section="$1"
  local test_name="$2"
  local sql_file="$3"
  local baseline_db="$4"
  local candidate_db="$5"
  local runs i baseline_sample candidate_sample
  runs=$(bench_runs_for_test "$test_name")

  for ((i=1; i<=runs; i++)); do
    if [ $((i % 2)) -eq 1 ]; then
      baseline_sample=$(run_bench \
        baseline "$BENCH_BASELINE_KIND" "$BENCH_BASELINE_BINARY" \
        "$BENCH_BASELINE_TIMER" "$sql_file" "$baseline_db")
      candidate_sample=$(run_bench \
        candidate "$BENCH_CANDIDATE_KIND" "$BENCH_CANDIDATE_BINARY" \
        "$BENCH_CANDIDATE_TIMER" "$sql_file" "$candidate_db")
    else
      candidate_sample=$(run_bench \
        candidate "$BENCH_CANDIDATE_KIND" "$BENCH_CANDIDATE_BINARY" \
        "$BENCH_CANDIDATE_TIMER" "$sql_file" "$candidate_db")
      baseline_sample=$(run_bench \
        baseline "$BENCH_BASELINE_KIND" "$BENCH_BASELINE_BINARY" \
        "$BENCH_BASELINE_TIMER" "$sql_file" "$baseline_db")
    fi
    printf '%s\t%s\t%d\t%s\t%s\n' \
      "$section" "$test_name" "$i" "$baseline_sample" "$candidate_sample" \
      >> "$BENCH_SAMPLES_FILE"
    printf '%s\t%s\t%d\t%s\t%s\n' \
      "$section" "$test_name" "$i" "$baseline_sample" "$candidate_sample"
  done
}

run_section() {
  local section="$1"
  local tests="$2"
  local baseline_db="$3"
  local candidate_db="$4"
  local t pair
  local BENCH_SECTION_SAMPLES_FILE="$TMPDIR/bench_section_samples.tsv"
  : > "$BENCH_SECTION_SAMPLES_FILE"

  echo "| Test | $BENCH_BASELINE_LABEL (us) | $BENCH_CANDIDATE_LABEL (us) | Multiplier |"
  echo "|------|------------:|--------------:|-----------:|"
  for t in $tests; do
    pair=$(run_bench_pair_stable \
      "$section" "$t" "$TMPDIR/$t.sql" "$baseline_db" "$candidate_db")
    if [ -n "$pair" ]; then
      printf '%s\n' "$pair" >> "$BENCH_SECTION_SAMPLES_FILE"
    fi
  done
  python3 "$BENCH_REPORT_SCRIPT" section \
    "$BENCH_SECTION_SAMPLES_FILE" "$section" "$tests" "$BENCH_RESULTS_FILE"
}

benchmark_autocommit_note() {
  if [ "$BENCH_BASELINE_KIND" = "sqlite" ]; then
    echo "_SQLite uses WAL mode with synchronous=FULL in this section so_"
    echo "_the comparison uses SQLite's durable WAL autocommit path._"
  fi
}

benchmark_gate_note() {
  if [ "$BENCH_GATE_MODE" = "absolute" ]; then
    echo "_Individual ratios gated at ${BENCH_MAX_MULTIPLIER}×; section averages gated at ${BENCH_AVG_MAX_MULTIPLIER}×. Autocommit writes use ${BENCH_AC_WRITE_MAX_MULTIPLIER}× / ${BENCH_AC_WRITE_AVG_MAX_MULTIPLIER}×._"
  else
    echo "_Threshold enforcement is applied after all paired suites are aggregated._"
  fi
}

check_ceiling() {
  python3 "$BENCH_REPORT_SCRIPT" ceiling "$BENCH_RESULTS_FILE" "$1" "$2" "$3"
}

check_average_ceiling() {
  python3 "$BENCH_REPORT_SCRIPT" average "$BENCH_RESULTS_FILE" "$1" "$2" "$3"
}

benchmark_copy_results() {
  if [ -n "${BENCH_RESULTS_OUTPUT:-}" ]; then
    mkdir -p "$(dirname "$BENCH_RESULTS_OUTPUT")"
    cp "$BENCH_RESULTS_FILE" "$BENCH_RESULTS_OUTPUT"
  fi
  if [ -n "${BENCH_SAMPLES_OUTPUT:-}" ]; then
    mkdir -p "$(dirname "$BENCH_SAMPLES_OUTPUT")"
    cp "$BENCH_SAMPLES_FILE" "$BENCH_SAMPLES_OUTPUT"
  fi
}

benchmark_finish() {
  local ceiling_ok=0
  benchmark_copy_results

  if [ "$BENCH_GATE_MODE" = "none" ]; then
    echo ""
    echo "Raw measurements recorded; threshold enforcement is deferred."
    return 0
  fi
  if [ "$BENCH_GATE_MODE" != "absolute" ]; then
    echo "unknown BENCH_GATE_MODE: $BENCH_GATE_MODE" >&2
    return 2
  fi

  echo ""
  echo "### Performance Ceiling Check (${BENCH_MAX_MULTIPLIER}x individual, ${BENCH_AVG_MAX_MULTIPLIER}x average; autocommit writes: ${BENCH_AC_WRITE_MAX_MULTIPLIER}x / ${BENCH_AC_WRITE_AVG_MAX_MULTIPLIER}x)"
  echo ""

  if [ "$BENCH_SECTION_MODE" = "autocommit" ]; then
    check_ceiling "ac_reads" "$READ_TESTS" "$BENCH_MAX_MULTIPLIER" \
      || ceiling_ok=1
    check_ceiling "ac_writes" "$WRITE_TESTS_AC" \
      "$BENCH_AC_WRITE_MAX_MULTIPLIER" || ceiling_ok=1
    check_average_ceiling "ac_reads" "$READ_TESTS" \
      "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
    check_average_ceiling "ac_writes" "$WRITE_TESTS_AC" \
      "$BENCH_AC_WRITE_AVG_MAX_MULTIPLIER" || ceiling_ok=1
  else
    check_ceiling "mem_reads" "$READ_TESTS" "$BENCH_MAX_MULTIPLIER" \
      || ceiling_ok=1
    check_ceiling "mem_writes" "$WRITE_TESTS" "$BENCH_MAX_MULTIPLIER" \
      || ceiling_ok=1
    check_ceiling "file_reads" "$READ_TESTS" "$BENCH_MAX_MULTIPLIER" \
      || ceiling_ok=1
    check_ceiling "file_writes" "$WRITE_TESTS" "$BENCH_MAX_MULTIPLIER" \
      || ceiling_ok=1
    check_average_ceiling "mem_reads" "$READ_TESTS" \
      "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
    check_average_ceiling "mem_writes" "$WRITE_TESTS" \
      "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
    check_average_ceiling "file_reads" "$READ_TESTS" \
      "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
    check_average_ceiling "file_writes" "$WRITE_TESTS" \
      "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
    if [ "$BENCH_SECTION_MODE" = "full" ]; then
      check_ceiling "ac_reads" "$READ_TESTS" "$BENCH_MAX_MULTIPLIER" \
        || ceiling_ok=1
      check_ceiling "ac_writes" "$WRITE_TESTS_AC" \
        "$BENCH_AC_WRITE_MAX_MULTIPLIER" || ceiling_ok=1
      check_average_ceiling "ac_reads" "$READ_TESTS" \
        "$BENCH_AVG_MAX_MULTIPLIER" || ceiling_ok=1
      check_average_ceiling "ac_writes" "$WRITE_TESTS_AC" \
        "$BENCH_AC_WRITE_AVG_MAX_MULTIPLIER" || ceiling_ok=1
    fi
  fi

  if [ "$ceiling_ok" = "0" ]; then
    echo "All tests within ceilings."
  else
    echo ""
    echo "**FAILED**: One or more tests exceeded their ceiling."
    return 1
  fi
}
