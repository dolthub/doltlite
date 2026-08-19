#!/usr/bin/env bash

set -euo pipefail

DOLTLITE="${1:-./doltlite}"
COMMITS_PER_STAGE="${DOLTLITE_OPEN_PERF_COMMITS:-64}"
STAGES="${DOLTLITE_OPEN_PERF_STAGES:-3}"
OPENS_PER_SAMPLE="${DOLTLITE_OPEN_PERF_OPENS:-40}"
SAMPLES="${DOLTLITE_OPEN_PERF_SAMPLES:-3}"
MAX_GROWTH_MS="${DOLTLITE_OPEN_PERF_MAX_GROWTH_MS:-750}"
TMP="$(mktemp -d)"
DB="$TMP/open_perf.db"
trap 'rm -rf "$TMP"' EXIT

if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found: $DOLTLITE" >&2
  exit 1
fi
if [ "$STAGES" -lt 2 ]; then
  echo "DOLTLITE_OPEN_PERF_STAGES must be at least 2" >&2
  exit 1
fi

append_commits() {
  local first="$1"
  local count="$2"
  local last=$((first + count - 1))
  local i
  {
    echo "CREATE TABLE IF NOT EXISTS updates(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);"
    for ((i=first; i<=last; i++)); do
      echo "INSERT INTO updates(payload) VALUES(zeroblob(2097152));"
      echo "SELECT dolt_commit('-A','-m','open perf $i');"
    done
  } | "$DOLTLITE" "$DB" >/dev/null
}

time_opens_ms() {
  local start end i
  start=$(python3 -c 'import time; print(time.monotonic_ns())')
  for ((i=0; i<OPENS_PER_SAMPLE; i++)); do
    "$DOLTLITE" "$DB" "SELECT 1;" >/dev/null
  done
  end=$(python3 -c 'import time; print(time.monotonic_ns())')
  echo $(((end - start) / 1000000))
}

median_open_ms() {
  local values=""
  local i
  for ((i=0; i<SAMPLES; i++)); do
    values+="$(time_opens_ms)"$'\n'
  done
  printf '%s' "$values" | sort -n | awk -v n="$SAMPLES" 'NR==int((n+2)/2){print; exit}'
}

echo "=== DoltLite Open-Time Growth Test ==="

declare -a COUNTS TIMES
PREVIOUS=0
TARGET="$COMMITS_PER_STAGE"
for ((stage=0; stage<STAGES; stage++)); do
  append_commits $((PREVIOUS + 1)) $((TARGET - PREVIOUS))
  COUNTS[stage]="$TARGET"
  TIMES[stage]=$(median_open_ms)
  PREVIOUS="$TARGET"
  TARGET=$((TARGET * 2))
done

EXPECTED_ROWS="$PREVIOUS"
ROWS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM updates;")
LOG_ROWS=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_log;")
INTEGRITY=$("$DOLTLITE" "$DB" "PRAGMA integrity_check;")
GROWTH_MS=$((${TIMES[STAGES-1]} - ${TIMES[0]}))

for ((stage=0; stage<STAGES; stage++)); do
  echo "  ${COUNTS[stage]} commits: ${TIMES[stage]}ms median for $OPENS_PER_SAMPLE opens"
done
echo "  growth: ${GROWTH_MS}ms; maximum allowed: ${MAX_GROWTH_MS}ms"

if [ "$ROWS" -ne "$EXPECTED_ROWS" ]; then
  echo "FAIL: expected $EXPECTED_ROWS rows, got $ROWS" >&2
  exit 1
fi
if [ "$LOG_ROWS" -ne $((EXPECTED_ROWS + 1)) ]; then
  echo "FAIL: expected $((EXPECTED_ROWS + 1)) log rows, got $LOG_ROWS" >&2
  exit 1
fi
if [ "$INTEGRITY" != "ok" ]; then
  echo "FAIL: integrity_check returned $INTEGRITY" >&2
  exit 1
fi
if [ "$GROWTH_MS" -gt "$MAX_GROWTH_MS" ]; then
  echo "FAIL: open time grew by ${GROWTH_MS}ms" >&2
  exit 1
fi

echo "PASS: open time stayed consistent across repeated history doublings"
