#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runner="$script_dir/retry-apt-get.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
has_timeout=1
if ! command -v timeout >/dev/null 2>&1; then
  has_timeout=0
  cat >"$tmp/bin/timeout" <<'EOF'
#!/usr/bin/env bash
while [[ "$1" == --* ]]; do
  shift
done
shift
exec "$@"
EOF
fi

cat >"$tmp/bin/sudo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >>"$APT_FAKE_SUDO_ARGS"
exec "$@"
EOF

cat >"$tmp/bin/apt-get" <<'EOF'
#!/usr/bin/env bash
count=0
if [ -f "$APT_FAKE_COUNT" ]; then
  count="$(cat "$APT_FAKE_COUNT")"
fi
count=$((count + 1))
printf '%s\n' "$count" >"$APT_FAKE_COUNT"
printf '%s\n' "$@" >>"$APT_FAKE_ARGS"
case "$APT_FAKE_MODE" in
  fail-once) [ "$count" -ge 2 ] || exit 42 ;;
  fail) exit 42 ;;
  hang) sleep 30 ;;
esac
EOF

chmod +x "$tmp/bin/"*

run_fake() (
  export PATH="$tmp/bin:$PATH"
  export APT_FAKE_COUNT="$tmp/count"
  export APT_FAKE_ARGS="$tmp/args"
  export APT_FAKE_SUDO_ARGS="$tmp/sudo-args"
  export APT_FAKE_MODE="$1"
  export APT_RETRY_DELAY_SECONDS=0
  unset APT_RETRY_ATTEMPTS APT_RETRY_TIMEOUT_SECONDS
  if [ "$#" -ge 3 ]; then
    export APT_RETRY_ATTEMPTS="$2"
    export APT_RETRY_TIMEOUT_SECONDS="$3"
  fi
  bash "$runner" update
)

run_fake fail-once
[ "$(cat "$tmp/count")" = 2 ]
grep -Fxq 'Acquire::Retries=2' "$tmp/args"
grep -Fxq 'DPkg::Lock::Timeout=30' "$tmp/args"
grep -Fxq -- '--signal=TERM' "$tmp/sudo-args"
grep -Fxq -- '--kill-after=10s' "$tmp/sudo-args"
grep -Fxq '300s' "$tmp/sudo-args"

rm "$tmp/count" "$tmp/args"
set +e
run_fake fail
rc=$?
set -e
[ "$rc" = 42 ]
[ "$(cat "$tmp/count")" = 2 ]

if [ "$has_timeout" = 1 ]; then
  rm "$tmp/count" "$tmp/args"
  set +e
  run_fake hang 1 1
  rc=$?
  set -e
  [ "$rc" = 124 ]
  [ "$(cat "$tmp/count")" = 1 ]
fi

echo "retry-apt-get tests passed"
