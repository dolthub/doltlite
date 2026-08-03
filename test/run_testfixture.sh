#!/bin/bash

set -uo pipefail

LABEL="${1:?Usage: run_testfixture.sh <label> <timeout_secs> test1 test2 ...}"
TIMEOUT="${2:?Missing timeout}"
shift 2

ulimit -Sn 1024 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIVERGENCE_FILE="${DIVERGENCE_FILE:-$SCRIPT_DIR/known_testfixture_divergences.txt}"
TERMINATION_FILE="${TERMINATION_FILE:-$SCRIPT_DIR/known_testfixture_terminations.txt}"
# Only used to point at the ratchet when an entry goes stale.
RATCHET_FILE="${RATCHET_FILE:-$SCRIPT_DIR/known_testfixture_exception_ratchet.txt}"
HOST_OS="$(uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]')"
TEST_VARIANT="${TESTFIXTURE_VARIANT:-default}"
case "$HOST_OS" in
  linux*) HOST_OS=linux ;;
  darwin*) HOST_OS=darwin ;;
  msys*|mingw*|cygwin*) HOST_OS=windows ;;
esac

if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD="gtimeout"
elif command -v perl >/dev/null 2>&1; then
  TIMEOUT_CMD="perl"
else
  echo "ERROR: timeout, gtimeout, or perl is required"
  exit 1
fi
run_with_timeout() {
  if [ "$TIMEOUT_CMD" != "perl" ]; then
    "$TIMEOUT_CMD" "$1" "${@:2}"
  else
    perl -e '
      use POSIX qw(setpgid);
      my $seconds = shift @ARGV;
      my $pid = fork();
      die "fork failed: $!" unless defined $pid;
      if ($pid == 0) {
        setpgid(0, 0);
        exec @ARGV;
        exit 127;
      }
      $SIG{ALRM} = sub {
        kill "TERM", -$pid;
        select undef, undef, undef, 0.2;
        kill "KILL", -$pid;
        waitpid($pid, 0);
        exit 124;
      };
      alarm $seconds;
      waitpid($pid, 0);
      alarm 0;
      my $status = $?;
      exit(($status & 127) ? 128 + ($status & 127) : $status >> 8);
    ' "$@"
  fi
}

expected_for() {
  local file="$1"
  [ -f "$DIVERGENCE_FILE" ] || return 0
  awk -v f="$file" -v host_os="$HOST_OS" -v test_variant="$TEST_VARIANT" '
    {
      sub(/#.*/, "")
      gsub(/^[ \t]+|[ \t]+$/, "")
      if ($0 == "") next
      for (i = 3; i <= NF; i++) {
        if ($i !~ /^@/) continue
        qualifier = substr($i, 2)
        if (qualifier == "coverage") {
          if (test_variant != "coverage") next
        } else if (qualifier == "no-coverage") {
          if (test_variant == "coverage") next
        } else if (qualifier != host_os) {
          next
        }
      }
      if ($1 == f) print $2
    }
  ' "$DIVERGENCE_FILE"
}

termination_contracts_for() {
  local file="$1"
  [ -f "$TERMINATION_FILE" ] || return 0
  awk -v f="$file" -v host_os="$HOST_OS" -v test_variant="$TEST_VARIANT" '
    {
      sub(/#.*/, "")
      gsub(/^[ \t]+|[ \t]+$/, "")
      if ($0 == "") next
      for (i = 5; i <= NF; i++) {
        if ($i !~ /^@/) continue
        qualifier = substr($i, 2)
        if (qualifier == "coverage") {
          if (test_variant != "coverage") next
        } else if (qualifier == "no-coverage") {
          if (test_variant == "coverage") next
        } else if (qualifier != host_os) {
          next
        }
      }
      if ($1 == f) print $2, $3, $4
    }
  ' "$TERMINATION_FILE"
}

last_completed_test() {
  awk '
    function header_name(line, name) {
      name = line
      sub(/[.][.][.].*$/, "", name)
      return name
    }
    /^[A-Za-z0-9_.()-]+[.][.][.]/ {
      if (pending != "") last = pending
      pending = header_name($0)
      if ($0 ~ /[.][.][.] (Ok|Omitted)$/) {
        last = pending
        pending = ""
      }
      next
    }
    /^Error:/ {
      if (pending != "") {
        last = pending
        pending = ""
      }
      next
    }
    /^! [A-Za-z0-9_.()-]+ got:/ {
      last = $2
      if (pending == $2) pending = ""
    }
    END { print (last == "" ? "-" : last) }
  '
}

tcl_diagnostic() {
  sed -n 's#^\./testfixture: ##p' | head -1
}

sha256_text() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

has_sanitizer_finding() {
  grep -Eq 'ERROR: (AddressSanitizer|LeakSanitizer|ThreadSanitizer|MemorySanitizer)|SUMMARY: (AddressSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer|MemorySanitizer)|runtime error:'
}

validate_termination_file() {
  [ -f "$TERMINATION_FILE" ] || {
    echo "ERROR: missing termination contract file: $TERMINATION_FILE"
    return 1
  }
  awk '
    {
      sub(/#.*/, "")
      if (NF == 0) next
      if (NF < 4) {
        printf "%s:%d: expected at least 4 fields\n", FILENAME, NR
        bad = 1
        next
      }
      if ($1 !~ /^[A-Za-z0-9_.-]+$/) {
        printf "%s:%d: invalid suite: %s\n", FILENAME, NR, $1
        bad = 1
      }
      if ($2 != "clean-exit" && $2 != "tcl-error" &&
          $2 !~ /^signal-[1-9][0-9]*$/) {
        printf "%s:%d: invalid termination class: %s\n", FILENAME, NR, $2
        bad = 1
      }
      if ($3 != "-" && $3 !~ /^[A-Za-z0-9_.()-]+$/) {
        printf "%s:%d: invalid last completed test: %s\n", FILENAME, NR, $3
        bad = 1
      }
      if ($4 != "-" && $4 !~ /^[0-9a-f]{64}$/) {
        printf "%s:%d: invalid diagnostic hash: %s\n", FILENAME, NR, $4
        bad = 1
      }
      if ($2 == "tcl-error" && $4 == "-") {
        printf "%s:%d: tcl-error requires a diagnostic hash\n", FILENAME, NR
        bad = 1
      }
      for (i = 5; i <= NF; i++) {
        if ($i != "@linux" && $i != "@darwin" && $i != "@windows" &&
            $i != "@coverage" && $i != "@no-coverage") {
          printf "%s:%d: invalid qualifier: %s\n", FILENAME, NR, $i
          bad = 1
        }
      }
      key = $0
      if (seen[key]++) {
        printf "%s:%d: duplicate termination contract\n", FILENAME, NR
        bad = 1
      }
    }
    END { exit bad }
  ' "$TERMINATION_FILE"
}

is_in_set() {
  local needle="$1"
  local haystack="$2"
  grep -Fxq -- "$needle" <<< "$haystack"
}

count_lines() {
  if [ -z "$1" ]; then
    echo 0
  else
    printf '%s\n' "$1" | grep -c .
  fi
}

parse_counted_pattern() {
  COUNTED_PREFIX=""
  COUNTED_EXPECTED=0
  if [[ "$1" =~ ^(.+)\.\*\{([1-9][0-9]*)\}$ ]]; then
    COUNTED_PREFIX="${BASH_REMATCH[1]}."
    COUNTED_EXPECTED="${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

total_pass=0
total_fail_known=0
total_fail_unexpected=0
total_unused=0
total_unexpected_crashes=0
total_unexpected_clean=0
unexpected_failure_lines=""
unused_lines=""

validate_termination_file || exit 1
TESTFIXTURE_BIN="$PWD/testfixture"
[ -x "$TESTFIXTURE_BIN" ] || {
  echo "ERROR: testfixture not found in $PWD"
  exit 1
}

for test in "$@"; do
  expected_specs="$(expected_for "$test")"
  termination_contracts="$(termination_contracts_for "$test")"
  if [ -n "$termination_contracts" ]; then
    test_workdir=$(mktemp -d "${TMPDIR:-/tmp}/doltlite-testfixture.XXXXXX")
    ln -s "$TESTFIXTURE_BIN" "$test_workdir/testfixture"
    if out=$(cd "$test_workdir" && run_with_timeout "$TIMEOUT" ./testfixture "$SCRIPT_DIR/${test}.test" 2>&1); then
      process_status=0
    else
      process_status=$?
    fi
    rm -rf "$test_workdir"
  else
    if out=$(run_with_timeout "$TIMEOUT" ./testfixture "$SCRIPT_DIR/${test}.test" 2>&1); then
      process_status=0
    else
      process_status=$?
    fi
  fi

  done_line=$(echo "$out" | grep "errors out of" | head -1)
  fail_line=$(echo "$out" | grep "^!Failures on these tests:" | head -1)

  if printf '%s\n' "$out" | has_sanitizer_finding; then
    echo "SANITIZER (unexpected): $test"
    total_unexpected_crashes=$((total_unexpected_crashes + 1))
    unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test: sanitizer finding"
    continue
  fi

  if [ -n "$done_line" ] && [ "$process_status" -ne 0 ] && [ "$process_status" -ne 1 ]; then
    if [ "$process_status" -eq 124 ]; then
      completed_termination="timeout"
    elif [ "$process_status" -gt 128 ]; then
      completed_termination="signal-$((process_status - 128))"
    else
      completed_termination="exit-$process_status"
    fi
    echo "TERMINATION (unexpected): $test ($completed_termination after summary)"
    total_unexpected_crashes=$((total_unexpected_crashes + 1))
    unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test: $completed_termination after summary"
    continue
  fi

  if [ -z "$done_line" ]; then
    diagnostic="$(printf '%s\n' "$out" | tcl_diagnostic)"
    diagnostic_hash="-"
    if [ -n "$diagnostic" ]; then
      diagnostic_hash="$(printf '%s' "$diagnostic" | sha256_text)"
    fi
    last_completed="$(printf '%s\n' "$out" | last_completed_test)"
    if [ "$process_status" -eq 124 ]; then
      termination_class="timeout"
    elif [ "$process_status" -gt 128 ]; then
      termination_class="signal-$((process_status - 128))"
    elif [ "$process_status" -eq 1 ] && [ -n "$diagnostic" ]; then
      termination_class="tcl-error"
    elif [ "$process_status" -eq 0 ]; then
      termination_class="clean-exit"
    else
      termination_class="exit-$process_status"
    fi
    actual_contract="$termination_class $last_completed $diagnostic_hash"

    if [ "$termination_class" = "timeout" ]; then
      echo "TIMEOUT (unexpected): $test after ${TIMEOUT}s (last completed: $last_completed)"
      total_unexpected_crashes=$((total_unexpected_crashes + 1))
      unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test: timed out after ${TIMEOUT}s; last completed $last_completed"
    elif is_in_set "$actual_contract" "$termination_contracts"; then
      echo "TERMINATION (expected): $test ($termination_class after $last_completed)"
    else
      echo "TERMINATION (unexpected): $test"
      echo "  actual:   $actual_contract"
      if [ -n "$termination_contracts" ]; then
        echo "  expected:"
        echo "$termination_contracts" | sed 's/^/    /'
      else
        echo "  expected: no termination contract"
      fi
      if [ -n "$diagnostic" ]; then
        echo "  diagnostic: $diagnostic"
      fi
      echo "  --- last testfixture output ---"
      echo "$out" | tail -n 40 | sed 's/^/  | /'
      echo "  --- end testfixture output ---"
      total_unexpected_crashes=$((total_unexpected_crashes + 1))
      unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test: $actual_contract"
    fi
    continue
  fi

  if [ -n "$termination_contracts" ]; then
    echo "FIXED TERMINATION: $test (now produces summary — remove its contract from $TERMINATION_FILE)"
    total_unexpected_clean=$((total_unexpected_clean + 1))
    unused_lines="$unused_lines"$'\n'"  $test (termination contract)"
  fi

  actual=""
  if [ -n "$fail_line" ]; then
    actual=$(echo "$fail_line" | sed 's/^!Failures on these tests://' | tr ' ' '\n' | grep -v '^$' || true)
  fi

  expected=""
  exact_expected=""
  fixed=""
  n_expected_total=0
  if [ -n "$expected_specs" ]; then
    while IFS= read -r spec; do
      [ -z "$spec" ] && continue
      if parse_counted_pattern "$spec"; then
        matches=""
        while IFS= read -r name; do
          [ -z "$name" ] && continue
          if [[ "$name" == "$COUNTED_PREFIX"* ]]; then
            matches="$matches"$'\n'"$name"
          fi
        done <<< "$actual"
        matches="$(echo "$matches" | grep -v '^$' || true)"
        n_matches=$(count_lines "$matches")
        n_expected_total=$((n_expected_total + COUNTED_EXPECTED))
        if [ "$n_matches" -eq "$COUNTED_EXPECTED" ]; then
          expected="$expected"$'\n'"$matches"
        else
          fixed="$fixed"$'\n'"$spec (expected $COUNTED_EXPECTED matches, got $n_matches)"
        fi
      else
        n_expected_total=$((n_expected_total + 1))
        expected="$expected"$'\n'"$spec"
        exact_expected="$exact_expected"$'\n'"$spec"
      fi
    done <<< "$expected_specs"
  fi
  expected="$(echo "$expected" | grep -v '^$' || true)"
  exact_expected="$(echo "$exact_expected" | grep -v '^$' || true)"
  duplicate_expected=$(echo "$expected" | sort | uniq -d | grep -v '^$' || true)
  if [ -n "$duplicate_expected" ]; then
    while IFS= read -r name; do
      fixed="$fixed"$'\n'"overlapping expected gates: $name"
    done <<< "$duplicate_expected"
  fi

  unexpected=""
  if [ -n "$actual" ]; then
    while IFS= read -r name; do
      [ -z "$name" ] && continue
      if ! is_in_set "$name" "$expected"; then
        unexpected="$unexpected"$'\n'"$name"
      fi
    done <<< "$actual"
  fi
  unexpected="$(echo "$unexpected" | grep -v '^$' || true)"

  if [ -n "$exact_expected" ]; then
    while IFS= read -r name; do
      [ -z "$name" ] && continue
      if ! is_in_set "$name" "$actual"; then
        fixed="$fixed"$'\n'"$name"
      fi
    done <<< "$exact_expected"
  fi
  fixed="$(echo "$fixed" | grep -v '^$' || true)"

  n_actual_total=$(count_lines "$actual")
  n_unexpected=$(count_lines "$unexpected")
  n_fixed=$(count_lines "$fixed")

  total_run=$(echo "$done_line" | awk '{print $5}')
  total_pass=$((total_pass + total_run - n_actual_total))
  total_fail_known=$((total_fail_known + n_actual_total - n_unexpected))
  total_fail_unexpected=$((total_fail_unexpected + n_unexpected))
  total_unused=$((total_unused + n_fixed))

  if [ "$n_unexpected" -eq 0 ] && [ "$n_fixed" -eq 0 ]; then
    if [ "$n_expected_total" -gt 0 ]; then
      echo "OK: $test ($total_run tests, $n_expected_total known divergences)"
    else
      echo "OK: $test ($total_run tests)"
    fi
  else
    if [ "$n_unexpected" -gt 0 ]; then
      echo "FAIL: $test — unexpected failures:"
      echo "$unexpected" | sed 's/^/    /'
      echo "  --- last testfixture output ---"
      echo "$out" | tail -n 40 | sed 's/^/  | /'
      echo "  --- end testfixture output ---"
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        unexpected_failure_lines="$unexpected_failure_lines"$'\n'"  $test $name"
      done <<< "$unexpected"
    fi
    if [ "$n_fixed" -gt 0 ]; then
      echo "MISMATCH: $test — these entries no longer match and should be updated in $DIVERGENCE_FILE:"
      echo "$fixed" | sed 's/^/    /'
      while IFS= read -r name; do
        [ -z "$name" ] && continue
        unused_lines="$unused_lines"$'\n'"  $test $name"
      done <<< "$fixed"
    fi
  fi
done

echo
echo "=== $LABEL ==="
echo "  passing tests:                     $total_pass"
echo "  known divergences (still failing): $total_fail_known"
if [ "$total_fail_unexpected" -gt 0 ] || [ "$total_unexpected_crashes" -gt 0 ]; then
  echo "  unexpected failures:               $total_fail_unexpected"
  echo "  unexpected terminations:           $total_unexpected_crashes"
  echo "  unexpected failure list:$unexpected_failure_lines"
  echo "::error::$LABEL: $((total_fail_unexpected + total_unexpected_crashes)) unexpected failure(s)"
  exit 1
fi
if [ "$total_unused" -gt 0 ] || [ "$total_unexpected_clean" -gt 0 ]; then
  echo "  stale/mismatched entries:          $((total_unused + total_unexpected_clean))"
  echo "  stale/mismatched entry list:$unused_lines"
  echo
  echo "  For every entry listed above:"
  echo
  echo "    1. Delete its line from test/$(basename "$DIVERGENCE_FILE")"
  echo "       (or test/$(basename "$TERMINATION_FILE") if it terminated). The"
  echo "       disposition rides on that line, so there is nothing else to remove."
  echo "       Match on the first two fields: lines carry class= and often a"
  echo "       trailing comment that a whole-line match will miss."
  echo "    2. Lower the affected counts in"
  echo "       test/$(basename "$RATCHET_FILE"). Step 3 prints the exact numbers."
  echo "    3. Confirm with: make lint"
  echo
  echo "  Before deleting, check the entry genuinely passes now. An assertion that"
  echo "  errored out or never ran is absent from the failure list without passing,"
  echo "  and belongs in the termination list instead. A per-test Ok line is the"
  echo "  proof; the authoritative failure set for a test is its line beginning"
  echo "  !Failures on these tests:"
  echo "::error::$LABEL: $((total_unused + total_unexpected_clean)) entry/entries should be updated or removed -- see the guidance above"
  exit 1
fi
exit 0
