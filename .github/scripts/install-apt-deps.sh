#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for path in /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
  [ -e "$path" ] || continue
  if grep -qiE 'packages[.]microsoft[.]com|azure-cli' "$path"; then
    sudo mv "$path" "$path.disabled"
  fi
done

bash "$script_dir/retry-apt-get.sh" update
bash "$script_dir/retry-apt-get.sh" install -y "$@"
