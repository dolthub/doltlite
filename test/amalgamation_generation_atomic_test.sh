#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-.}"
cd "$build_dir"

if [ ! -f sqlite3.c ] || [ ! -d tsrc ]; then
  echo "SKIP: sqlite3.c/tsrc not found in $PWD"
  exit 0
fi

tclsh_bin="${TCLSH:-./jimsh}"
generator="${DOLTLITE_MKSQLITE3C:-../tool/mksqlite3c.tcl}"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-amalg-atomic.XXXXXX")"
probe=tsrc/doltlite_atomic_failure_probe.c
cleanup() {
  rm -f "$probe"
  rm -rf "$tmp"
}
trap cleanup EXIT

cp sqlite3.c "$tmp/sqlite3.c.good"
printf 'int doltliteAtomicFailureProbe;\n' > "$probe"
if "$tclsh_bin" "$generator" --doltlite >"$tmp/stdout" 2>"$tmp/stderr"; then
  echo "FAIL: generation unexpectedly accepted an unlisted source"
  exit 1
fi
if ! grep -q 'doltlite_atomic_failure_probe.c' "$tmp/stderr"; then
  echo "FAIL: generation error did not name the unlisted source"
  exit 1
fi
if ! cmp -s sqlite3.c "$tmp/sqlite3.c.good"; then
  echo "FAIL: failed generation replaced sqlite3.c"
  exit 1
fi
if compgen -G 'sqlite3.c.tmp-*' >/dev/null; then
  echo "FAIL: failed generation left a temporary output"
  exit 1
fi

echo "amalgamation failed-generation preservation: PASS"
