#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
python3 "$script_dir/ci-build-failure-test.py"
python3 "$script_dir/parallel-compile-test.py"
python3 "$script_dir/macos-build-jobs-test.py"
python3 "$script_dir/ci-cache-and-packages-test.py"
python3 "$script_dir/benchmark-build-test.py"
python3 "$script_dir/benchmark-cache-key-test.py"
python3 "$script_dir/../../test/sqllogictest_gate_test.py"
python3 "$script_dir/../../test/sysbench_harness_test.py"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/payload/nested" "$work/profiles with spaces" "$work/bin"
printf 'test data\n' > "$work/payload/nested/file"
chmod 755 "$work/payload/nested/file"
ln -s nested/file "$work/payload/link"
tar -czf "$work/original.tar.gz" -C "$work" payload
bash "$script_dir/package-ci-build.sh" "$work/fast.tar.gz" -C "$work" payload
diff <(tar -tvf "$work/original.tar.gz") <(tar -tvf "$work/fast.tar.gz")
mkdir "$work/original" "$work/fast"
tar -xzf "$work/original.tar.gz" -C "$work/original"
tar -xzf "$work/fast.tar.gz" -C "$work/fast"
diff -r "$work/original" "$work/fast"
[ -x "$work/fast/payload/nested/file" ]
[ "$(readlink "$work/fast/payload/link")" = nested/file ]
if bash "$script_dir/package-ci-build.sh" "$work/missing.tar.gz" \
    -C "$work" missing > "$work/package.log" 2>&1; then
  echo 'FAIL: packaging accepted a missing input'
  exit 1
fi

cat > "$work/bin/llvm-profdata" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "$1" = merge ] && [ "$2" = -sparse ] && [ "$3" = -f ] && [ "$5" = -o ]
cp "$4" "$PROFILE_LIST_COPY"
if [ "${FAIL_MERGE:-0}" = 1 ]; then exit 1; fi
printf 'merged\n' > "$6"
STUB
chmod +x "$work/bin/llvm-profdata"
export PATH="$work/bin:$PATH" PROFILE_LIST_COPY="$work/input-list"
profiles="$work/profiles with spaces"
bash "$script_dir/merge-coverage-profiles.sh" "$profiles"
[ ! -e "$PROFILE_LIST_COPY" ]
printf raw > "$profiles/2.profraw"
printf raw > "$profiles/1.profraw"
printf keep > "$profiles/keep.txt"
if FAIL_MERGE=1 bash "$script_dir/merge-coverage-profiles.sh" "$profiles"; then
  echo 'FAIL: merger accepted a failed llvm-profdata invocation'
  exit 1
fi
[ -f "$profiles/1.profraw" ] && [ -f "$profiles/2.profraw" ]
bash "$script_dir/merge-coverage-profiles.sh" "$profiles" sql-differential
printf '%s\n' "$profiles/1.profraw" "$profiles/2.profraw" > "$work/expected-list"
cmp "$work/expected-list" "$PROFILE_LIST_COPY"
[ -s "$profiles/sql-differential.profdata" ]
[ ! -e "$profiles/1.profraw" ] && [ ! -e "$profiles/2.profraw" ]
[ -f "$profiles/keep.txt" ]
printf raw > "$profiles/3.profraw"
bash "$script_dir/merge-coverage-profiles.sh" "$profiles"
[ -s "$profiles/coverage.profdata" ] && [ ! -e "$profiles/3.profraw" ]
echo 'CI packaging and profile merge checks passed'
