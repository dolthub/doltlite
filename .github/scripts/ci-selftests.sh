ci_selftests() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ci_compile python3 "$script_dir/ci-build-failure-test.py"
  ci_compile python3 "$script_dir/parallel-compile-test.py"
  ci_compile python3 "$script_dir/macos-build-jobs-test.py"
  ci_compile python3 "$script_dir/ci-cache-and-packages-test.py"
  ci_compile python3 "$script_dir/ci-next-optimizations-test.py"
  ci_compile python3 "$script_dir/ci-followup-test.py"
  ci_compile python3 "$script_dir/seed-cache-workflow-test.py"
  ci_compile python3 "$script_dir/benchmark-build-test.py"
  ci_compile python3 "$script_dir/benchmark-cache-key-test.py"
  ci_compile python3 "$script_dir/../../test/sqllogictest_gate_test.py"
  ci_compile python3 "$script_dir/../../test/sysbench_harness_test.py"
  ci_compile bash "$script_dir/ci-artifact-tests.sh"
}
