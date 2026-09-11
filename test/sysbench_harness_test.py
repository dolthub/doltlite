import os
from pathlib import Path
import subprocess
import tempfile


helper = Path(__file__).resolve().parent / "lib/sysbench_benchmark.sh"
script = r'''
set -eu
TMPDIR="$WORK"
SQLITE3=sqlite DOLTLITE=doltlite BENCH_TIMER_SQLITE=timer BENCH_TIMER_DOLTLITE=timer
BENCH_RUNS=4 BENCH_AC_WRITE_RUNS=3
source "$HELPER"
run_bench() {
  local count=0 id="$1" test="${5##*/}"
  test="${test%.sql}"
  if [ -e "$TMPDIR/$id-$test.count" ]; then read -r count < "$TMPDIR/$id-$test.count"; fi
  count=$((count+1))
  echo "$count" > "$TMPDIR/$id-$test.count"
  printf '%s %s %s %s\n' "$id" "$test" "$count" "$6" >> "$TMPDIR/calls"
  case "$test/$id" in
    even/baseline) set -- 1000 4000 3000 2000 ;;
    even/candidate) set -- 8000 2000 4000 6000 ;;
    invalid/baseline) set -- -1 10 -2 30 ;;
    invalid/candidate) set -- -1 -1 -1 -1 ;;
    zero/baseline) set -- 0 0 0 0 ;;
    zero/candidate) set -- 10 20 30 40 ;;
    rounded_ac/baseline) set -- 200 200 200 ;;
    rounded_ac/candidate) set -- 301 301 301 ;;
  esac
  shift $((count-1))
  echo "$1"
}
run_section mem 'even invalid zero' :memory: :memory:
run_section ac 'rounded_ac' baseline.db candidate.db
check_ceiling mem 'even invalid zero absent' 2 || echo 'individual failed'
check_ceiling ac rounded_ac 1.50 || echo 'rounding individual failed'
check_average_ceiling ac rounded_ac 1.50 || echo 'rounding average failed'
check_average_ceiling ac rounded_ac 1.49 || echo 'average failed'
check_average_ceiling mem 'even invalid zero absent' 2 || echo 'missing average failed'
'''
expected_report = '''| Test | SQLite (us) | DoltLite (us) | Multiplier |
|------|------------:|--------------:|-----------:|
| even | 3,000 | 6,000 | 2.00 |
| invalid | 30 | crash | -- |
| zero | 0 | 30 | -- |
| Average |  |  | 2.00 |
| Test | SQLite (us) | DoltLite (us) | Multiplier |
|------|------------:|--------------:|-----------:|
| rounded_ac | 200 | 301 | 1.50 |
| Average |  |  | 1.50 |
individual failed
rounding individual failed
average failed
missing average failed
'''
expected_errors = '''FAIL: mem/invalid did not produce valid timings
FAIL: mem/zero did not produce valid timings
FAIL: mem/absent did not produce valid timings
FAIL: ac/rounded_ac = 1.50x (ceiling: 1.50x)
FAIL: ac average = 1.50x (ceiling: 1.49x)
FAIL: mem average is missing valid timings
'''


def main():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        result = subprocess.run(["bash", "-c", script], text=True, capture_output=True,
                                env=dict(os.environ, WORK=tmp, HELPER=str(helper)), timeout=30)
        assert result.returncode == 0, result
        assert result.stdout == expected_report, result.stdout
        assert result.stderr == expected_errors, result.stderr
        assert (root / "bench_results.tsv").read_text() == (
            "mem\teven\t3000\t6000\nmem\tinvalid\t30\t-1\nmem\tzero\t0\t30\nac\trounded_ac\t200\t301\n")
        calls = []
        for test, count in (("even", 4), ("invalid", 4), ("zero", 4), ("rounded_ac", 3)):
            for i in range(1, count + 1):
                for side in (("baseline", "candidate") if i % 2 else ("candidate", "baseline")):
                    db = f"{side}.db" if test.endswith("_ac") else ":memory:"
                    calls.append(f"{side} {test} {i} {db}")
        assert (root / "calls").read_text().splitlines() == calls
        samples = (root / "bench_samples.tsv").read_text().splitlines()
        assert len(samples) == 16 and samples[0] == "section\ttest\trun\tbaseline_us\tcandidate_us"
        assert samples[1:5] == [f"mem\teven\t{i}\t{b}\t{c}" for i, b, c in
                                 ((1, 1000, 8000), (2, 4000, 2000), (3, 3000, 4000), (4, 2000, 6000))]
    print("Sysbench harness: medians, reports, samples, pairing and ceiling checks passed")


if __name__ == "__main__":
    main()
