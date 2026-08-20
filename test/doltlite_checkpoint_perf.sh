#!/usr/bin/env bash

set -euo pipefail

DOLTLITE="${1:-./doltlite}"
THRESHOLD_BYTES="${DOLTLITE_CHECKPOINT_PERF_THRESHOLD_BYTES:-67108864}"
PAYLOAD_BYTES="${DOLTLITE_CHECKPOINT_PERF_PAYLOAD_BYTES:-2097152}"
MAX_APPENDS="${DOLTLITE_CHECKPOINT_PERF_MAX_APPENDS:-128}"
TRIALS="${DOLTLITE_CHECKPOINT_PERF_TRIALS:-5}"
TMP="$(mktemp -d)"
PROBE_DB="$TMP/checkpoint_probe.db"
BASE_DB="$TMP/checkpoint_base.db"
trap 'rm -rf "$TMP"' EXIT

if [ ! -x "$DOLTLITE" ]; then
  echo "doltlite binary not found: $DOLTLITE" >&2
  exit 1
fi
case "$THRESHOLD_BYTES:$PAYLOAD_BYTES:$MAX_APPENDS:$TRIALS" in
  *[!0-9:]*|'')
    echo "checkpoint performance settings must be positive integers" >&2
    exit 1
    ;;
esac
if [ "$THRESHOLD_BYTES" -le 0 ] || [ "$PAYLOAD_BYTES" -le 0 ] \
   || [ "$MAX_APPENDS" -lt 2 ] || [ "$TRIALS" -lt 1 ]; then
  echo "checkpoint performance settings must be positive integers" >&2
  exit 1
fi

export DOLTLITE_WAL_CHECKPOINT_THRESHOLD="$THRESHOLD_BYTES"

checkpoint_stamp() {
  python3 -c '
import os
import struct
import sys

with open(sys.argv[1], "rb") as f:
    # The final root is 169 bytes and its manifest starts at byte 1.
    f.seek(-169, os.SEEK_END)
    manifest = f.read(169)[1:]
    print("%08x %d %d" % (
        struct.unpack_from("<I", manifest, 8)[0],
        struct.unpack_from("<q", manifest, 12)[0],
        struct.unpack_from("<q", manifest, 68)[0]))
' "$1"
}

checkpoint_magic() {
  checkpoint_stamp "$1" | awk '{print $1}'
}

file_size() {
  python3 -c 'import os, sys; print(os.path.getsize(sys.argv[1]))' "$1"
}

sync_file() {
  python3 -c '
import os
import sys

with open(sys.argv[1], "rb+") as f:
    os.fsync(f.fileno())
' "$1"
}

init_db() {
  "$DOLTLITE" "$1" \
    "CREATE TABLE updates(id INTEGER PRIMARY KEY, payload BLOB NOT NULL); INSERT INTO updates VALUES(1,randomblob($PAYLOAD_BYTES)); SELECT dolt_commit('-A','-m','checkpoint perf schema');" \
    >/dev/null
}

append_update() {
  "$DOLTLITE" "$1" \
    "UPDATE updates SET payload=randomblob($PAYLOAD_BYTES) WHERE id=1;" \
    >/dev/null
}

wait_for_marker() {
  local pid="$1"
  local output_file="$2"
  local marker="$3"

  for ((attempt=0; attempt<500; attempt++)); do
    if grep -Fqx "$marker" "$output_file"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.01
  done
  return 1
}

measure_trial() {
  local db="$1"
  local trial="$2"
  local input="$db.$trial.input"
  local output_file="$db.$trial.output"
  local pid
  local elapsed
  local below_stamp
  local checkpoint_stamp_value
  local post_stamp
  local below_wal_bytes

  mkfifo "$input"
  "$DOLTLITE" "$db" <"$input" >"$output_file" 2>&1 &
  pid=$!
  exec 3>"$input"

  printf '%s\n' \
    'SELECT count(*) FROM updates;' \
    '.shell echo warm_done' \
    >&3
  if ! wait_for_marker "$pid" "$output_file" warm_done; then
    printf '%s\n' '.quit' >&3 || true
    exec 3>&-
    wait "$pid" || true
    echo "untimed warm-up did not complete:" >&2
    sed -n '1,120p' "$output_file" >&2
    return 1
  fi

  printf '%s\n' \
    '.timer on' \
    "UPDATE updates SET payload=randomblob($PAYLOAD_BYTES) WHERE id=1;" \
    '.timer off' \
    '.shell echo timed_below_done' \
    >&3
  if ! wait_for_marker "$pid" "$output_file" timed_below_done; then
    printf '%s\n' '.quit' >&3 || true
    exec 3>&-
    wait "$pid" || true
    echo "below-threshold append did not complete:" >&2
    sed -n '1,120p' "$output_file" >&2
    return 1
  fi
  below_stamp=$(checkpoint_stamp "$db")
  below_wal_bytes=$(($(file_size "$db") - 168))

  printf '%s\n' \
    '.timer on' \
    "UPDATE updates SET payload=randomblob($PAYLOAD_BYTES) WHERE id=1;" \
    '.timer off' \
    '.shell echo timed_checkpoint_done' \
    >&3
  if ! wait_for_marker "$pid" "$output_file" timed_checkpoint_done; then
    printf '%s\n' '.quit' >&3 || true
    exec 3>&-
    wait "$pid" || true
    echo "checkpointing append did not complete:" >&2
    sed -n '1,120p' "$output_file" >&2
    return 1
  fi
  checkpoint_stamp_value=$(checkpoint_stamp "$db")

  printf '%s\n' \
    '.timer on' \
    "UPDATE updates SET payload=randomblob($PAYLOAD_BYTES) WHERE id=1;" \
    '.timer off' \
    '.shell echo timed_post_done' \
    >&3
  if ! wait_for_marker "$pid" "$output_file" timed_post_done; then
    printf '%s\n' '.quit' >&3 || true
    exec 3>&-
    wait "$pid" || true
    echo "post-checkpoint append did not complete:" >&2
    sed -n '1,120p' "$output_file" >&2
    return 1
  fi
  post_stamp=$(checkpoint_stamp "$db")

  printf '%s\n' '.quit' >&3 || true
  exec 3>&-
  if ! wait "$pid"; then
    echo "timed append session failed:" >&2
    sed -n '1,120p' "$output_file" >&2
    return 1
  fi

  elapsed=$(awk '
    $1=="Run" && $2=="Time:" && $3=="real" {
      values[++n]=$4
    }
    END {
      if (n==3) printf "%s %s %s\n", values[1], values[2], values[3]
    }
  ' "$output_file")
  set -- $elapsed
  if [ "$#" -ne 3 ]; then
    echo "unable to read three append timings:" >&2
    sed -n '1,120p' "$output_file" >&2
    return 1
  fi
  printf '%s %s %s %s %s %s %s\n' \
    "$1" "$below_wal_bytes" "$below_stamp" \
    "$2" "$checkpoint_stamp_value" "$3" "$post_stamp"
}

median() {
  printf '%s\n' "$@" | sort -n | awk '
    { values[NR]=$1 }
    END {
      middle=int((NR+1)/2)
      if (NR%2) print values[middle]
      else print (values[middle]+values[middle+1])/2
    }
  '
}

init_db "$PROBE_DB"
CROSSING=0
for ((i=1; i<=MAX_APPENDS; i++)); do
  append_update "$PROBE_DB" "$i"
  if [ "$(checkpoint_magic "$PROBE_DB")" = "32504b43" ]; then
    CROSSING="$i"
    break
  fi
done
if [ "$CROSSING" -lt 2 ]; then
  echo "checkpoint did not cross after at least two appends" >&2
  exit 1
fi

init_db "$BASE_DB"
for ((i=1; i<CROSSING-1; i++)); do
  append_update "$BASE_DB" "$i"
done

BELOW_SAMPLES=()
CHECKPOINT_SAMPLES=()
POST_SAMPLES=()
SAMPLE_LINES=()
BELOW_WAL_BYTES=0
for ((trial=1; trial<=TRIALS; trial++)); do
  MEASURE_DB="$TMP/checkpoint_measure_$trial.db"
  cp "$BASE_DB" "$MEASURE_DB"
  # Keep clone writeback out of the first append timing.
  sync_file "$MEASURE_DB"

  if ! read -r BELOW_SECONDS BELOW_WAL_BYTES BELOW_APPEND_MAGIC \
      BELOW_APPEND_OFFSET BELOW_APPEND_REPLAY \
      CHECKPOINT_SECONDS CHECKPOINT_APPEND_MAGIC \
      CHECKPOINT_APPEND_OFFSET CHECKPOINT_APPEND_REPLAY \
      POST_SECONDS POST_APPEND_MAGIC POST_APPEND_OFFSET POST_APPEND_REPLAY \
      < <(measure_trial "$MEASURE_DB" "$trial"); then
    exit 1
  fi
  if [ "$BELOW_APPEND_MAGIC" = "32504b43" ]; then
    echo "append $((CROSSING - 1)) unexpectedly created a checkpoint" >&2
    exit 1
  fi
  if [ "$BELOW_WAL_BYTES" -ge "$THRESHOLD_BYTES" ]; then
    echo "below-threshold WAL reached $BELOW_WAL_BYTES bytes" >&2
    exit 1
  fi

  if [ "$CHECKPOINT_APPEND_MAGIC" != "32504b43" ]; then
    echo "append $CROSSING did not create a checkpoint during the timed append" >&2
    exit 1
  fi

  read -r POST_MAGIC POST_OFFSET POST_REPLAY < <(checkpoint_stamp "$MEASURE_DB")
  if [ "$POST_APPEND_MAGIC" != "32504b43" ] || [ "$POST_MAGIC" != "32504b43" ] \
     || [ "$POST_APPEND_OFFSET" -ne "$CHECKPOINT_APPEND_OFFSET" ] \
     || [ "$POST_APPEND_REPLAY" -ne "$CHECKPOINT_APPEND_REPLAY" ] \
     || [ "$POST_OFFSET" -ne "$CHECKPOINT_APPEND_OFFSET" ] \
     || [ "$POST_REPLAY" -ne "$CHECKPOINT_APPEND_REPLAY" ]; then
    echo "append $((CROSSING + 1)) did not reuse the existing checkpoint" >&2
    exit 1
  fi
  if ! awk -v below="$BELOW_SECONDS" -v checkpoint="$CHECKPOINT_SECONDS" \
      -v post="$POST_SECONDS" \
      'BEGIN { exit !(below>0 && checkpoint>0 && post>0) }'; then
    echo "append times must be positive" >&2
    exit 1
  fi

  ROWS=$("$DOLTLITE" "$MEASURE_DB" "SELECT count(*) FROM updates;")
  INTEGRITY=$("$DOLTLITE" "$MEASURE_DB" "PRAGMA integrity_check;")
  if [ "$ROWS" -ne 1 ]; then
    echo "expected 1 row, got $ROWS" >&2
    exit 1
  fi
  if [ "$INTEGRITY" != "ok" ]; then
    echo "integrity_check returned $INTEGRITY" >&2
    exit 1
  fi

  BELOW_SAMPLES+=("$BELOW_SECONDS")
  CHECKPOINT_SAMPLES+=("$CHECKPOINT_SECONDS")
  POST_SAMPLES+=("$POST_SECONDS")
  CHECKPOINT_RATIO=$(awk -v below="$BELOW_SECONDS" -v checkpoint="$CHECKPOINT_SECONDS" \
    'BEGIN { printf "%.6f", checkpoint/below }')
  POST_RATIO=$(awk -v below="$BELOW_SECONDS" -v post="$POST_SECONDS" \
    'BEGIN { printf "%.6f", post/below }')
  BELOW_MS=$(awk -v n="$BELOW_SECONDS" 'BEGIN { printf "%.3f", n*1000 }')
  CHECKPOINT_MS=$(awk -v n="$CHECKPOINT_SECONDS" 'BEGIN { printf "%.3f", n*1000 }')
  POST_MS=$(awk -v n="$POST_SECONDS" 'BEGIN { printf "%.3f", n*1000 }')
  CHECKPOINT_RATIO_DISPLAY=$(awk -v n="$CHECKPOINT_RATIO" \
    'BEGIN { printf "%.3f", n }')
  POST_RATIO_DISPLAY=$(awk -v n="$POST_RATIO" 'BEGIN { printf "%.3f", n }')
  SAMPLE_LINES+=(
    "  trial $trial: below=${BELOW_MS}ms checkpoint=${CHECKPOINT_MS}ms post=${POST_MS}ms"
    "    checkpoint/below=${CHECKPOINT_RATIO_DISPLAY}x post/below=${POST_RATIO_DISPLAY}x"
  )
done

MEDIAN_BELOW=$(median "${BELOW_SAMPLES[@]}")
MEDIAN_CHECKPOINT=$(median "${CHECKPOINT_SAMPLES[@]}")
MEDIAN_POST=$(median "${POST_SAMPLES[@]}")
MEDIAN_BELOW_MS=$(awk -v n="$MEDIAN_BELOW" 'BEGIN { printf "%.3f", n*1000 }')
MEDIAN_CHECKPOINT_MS=$(awk -v n="$MEDIAN_CHECKPOINT" 'BEGIN { printf "%.3f", n*1000 }')
MEDIAN_POST_MS=$(awk -v n="$MEDIAN_POST" 'BEGIN { printf "%.3f", n*1000 }')
MEDIAN_CHECKPOINT_RATIO=$(awk -v below="$MEDIAN_BELOW" -v checkpoint="$MEDIAN_CHECKPOINT" \
  'BEGIN { printf "%.3f", checkpoint/below }')
MEDIAN_POST_RATIO=$(awk -v below="$MEDIAN_BELOW" -v post="$MEDIAN_POST" \
  'BEGIN { printf "%.3f", post/below }')

echo "=== DoltLite WAL Checkpoint Append Cost ==="
echo "  threshold: $THRESHOLD_BYTES bytes"
echo "  payload per append: $PAYLOAD_BYTES bytes"
echo "  checkpoint created on append: $CROSSING"
echo "  WAL bytes after below-threshold append: $BELOW_WAL_BYTES"
printf '%s\n' "${SAMPLE_LINES[@]}"
echo "  median below-threshold append: ${MEDIAN_BELOW_MS}ms"
echo "  median checkpointing append: ${MEDIAN_CHECKPOINT_MS}ms"
echo "  median post-checkpoint append on same connection: ${MEDIAN_POST_MS}ms"
echo "  median checkpoint/below ratio: ${MEDIAN_CHECKPOINT_RATIO}x"
echo "  median post/below ratio: ${MEDIAN_POST_RATIO}x"
echo "PASS: measured checkpoint and post-checkpoint append ratios; no bound is enforced yet"
