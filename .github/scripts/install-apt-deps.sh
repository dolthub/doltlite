#!/usr/bin/env bash
set -euo pipefail

for path in /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
  [ -e "$path" ] || continue
  if grep -qiE 'packages[.]microsoft[.]com|azure-cli' "$path"; then
    sudo mv "$path" "$path.disabled"
  fi
done

for attempt in 1 2 3; do
  if sudo apt-get update; then
    break
  fi
  if [ "$attempt" -eq 3 ]; then
    exit 1
  fi
  sleep $((attempt * 5))
done

sudo apt-get install -y "$@"
