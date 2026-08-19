#!/usr/bin/env bash
set -uo pipefail

attempts="${APT_RETRY_ATTEMPTS:-2}"
timeout_seconds="${APT_RETRY_TIMEOUT_SECONDS:-300}"
delay_seconds="${APT_RETRY_DELAY_SECONDS:-5}"

if ! [[ "$attempts" =~ ^[0-9]+$ ]] || [ "$attempts" -lt 1 ] \
    || ! [[ "$timeout_seconds" =~ ^[0-9]+$ ]] || [ "$timeout_seconds" -lt 1 ] \
    || ! [[ "$delay_seconds" =~ ^[0-9]+$ ]]; then
  echo "invalid apt retry configuration" >&2
  exit 2
fi

rc=1
for ((attempt = 1; attempt <= attempts; attempt++)); do
  echo "apt-get attempt $attempt/$attempts: $*"
  sudo timeout --signal=TERM --kill-after=10s "${timeout_seconds}s" \
    apt-get \
      -o Acquire::Retries=2 \
      -o Acquire::http::Timeout=20 \
      -o Acquire::https::Timeout=20 \
      -o DPkg::Lock::Timeout=30 \
      "$@"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    exit 0
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    wait_seconds=$((attempt * delay_seconds))
    echo "apt-get exited with $rc; retrying in ${wait_seconds}s" >&2
    sleep "$wait_seconds"
  fi
done

echo "apt-get failed after $attempts attempts (exit $rc)" >&2
exit "$rc"
