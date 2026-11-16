#!/usr/bin/env python3
import os
from pathlib import Path
import subprocess
import sys
import tempfile

script = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="doltlite-scaling-test-") as work:
    root = Path(work)
    engine = root / "engine"
    engine.write_text(r'''#!/bin/bash
echo invoked >> "$SCALING_TEST_CALLS"
case "$SCALING_TEST_FAILURE:$*" in
  setup:*"CREATE TABLE t("*) fail=1 ;;
  seed:*) [ "$#" -eq 1 ] && fail=1 ;;
  commit:*"UPDATE t SET v=v+1"*) fail=1 ;;
  deep-commit:*"'c1000'"*) fail=1 ;;
  postgc-commit:*"'c2400'"*) fail=1 ;;
  log:*"SELECT count(*) FROM dolt_log"*) fail=1 ;;
  checkout:*"dolt_checkout('anchor')"*) fail=1 ;;
  merge:*"dolt_merge('side1a')"*) fail=1 ;;
  gc:*"SELECT dolt_gc()"*) fail=1 ;;
  gc-size:*"size100000 SELECT dolt_gc()"*) fail=1 ;;
  open:*"size100000 SELECT 1;"*) fail=1 ;;
  median-late:*"size100000 SELECT 1;"*)
    [ -f "$SCALING_TEST_SEEN" ] && fail=1
    touch "$SCALING_TEST_SEEN"
    ;;
esac
if [ "${fail:-0}" = 1 ]; then
  echo failed >> "$SCALING_TEST_CALLS"
  echo "injected database failure: $SCALING_TEST_FAILURE" >&2
  exit 17
fi
if [ "$#" -eq 1 ]; then cat >/dev/null; fi
case "$*" in
  *"CREATE TABLE t("*) echo fixture > "$1" ;;
esac
case "$*" in
  *"/size"*" SELECT dolt_gc();"*)
    n=${1##*size}
    if [ "$(cat "$1")" != fixture ]; then
      echo "GC ran on a collected database" >&2
      exit 19
    fi
    echo collected > "$1"
    chunks=$((n/29)); bytes=$((n*131))
    case "$SCALING_TEST_FAILURE:$n" in
      work-regression:10000000) chunks=$((chunks*13/10)) ;;
      bytes-regression:100000000) bytes=$((bytes*13/10)) ;;
      no-counters:*) chunks= ;;
    esac
    echo 1
    [ -n "$chunks" ] && echo "scale-work $chunks $bytes"
    ;;
  *"scale-work"*)
    commits=$(printf '%s' "$*" | grep -o "dolt_commit(" | wc -l)
    [ "$commits" -gt 0 ] || commits=1
    work="$((commits*100)) $((commits*1000))"
    case "$SCALING_TEST_FAILURE:$*" in
      merge-regression:*"dolt_merge('side2"*) work="1700 1000" ;;
      lookup-regression:*"/size100000000 SELECT val FROM t"*) work="100 2100" ;;
    esac
    trailer="scale-work $work"
    ;;
esac
if [ -n "${SCALING_TEST_CLOCK:-}" ]; then
  duration=10
  case "$SCALING_TEST_FAILURE:$*" in
    throttled-disk:*"/size1000000 SELECT dolt_gc();"*) duration=195 ;;
    throttled-disk:*"/size10000000 SELECT dolt_gc();"*) duration=3971 ;;
  esac
  read -r now < "$SCALING_TEST_CLOCK"
  echo "$((now+duration))" > "$SCALING_TEST_CLOCK"
fi
case "$*" in
  *"SELECT count(*), max(v) FROM s"*) echo '6|1' ;;
  *"SELECT sum(v) FROM t"*) echo 2750 ;;
  *"sum(name <>"*)
    n=${1##*size}
    echo "$n|$((n*(n+1)/2))|$((n/1000*499500+3500))|0"
    ;;
  *"SELECT commit_hash FROM dolt_log LIMIT 1"*) echo head ;;
  *"SELECT count(*) FROM dolt_diff_t"*) echo 500 ;;
  *"SELECT dolt_checkout('wipe')"*) printf '0\n0\n' ;;
  *"SELECT dolt_checkout('main')"*) printf '0\n1000\n' ;;
  *"SELECT group_concat(length(data)"*) echo '1048576|16777216' ;;
esac
[ -n "${trailer:-}" ] && echo "$trailer"
exit 0
''')
    engine.chmod(0o755)
    clock = root / "python3"
    clock.write_text('''#!/bin/bash
if [ -n "${SCALING_TEST_CLOCK:-}" ]; then
  cat "$SCALING_TEST_CLOCK"
else
  echo 1000
fi
''')
    clock.chmod(0o755)
    def check_failure(case):
        calls = root / f"{case}.calls"
        env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                   SCALING_TEST_FAILURE=case, SCALING_TEST_CALLS=str(calls),
                   SCALING_TEST_SEEN=str(root / f"{case}.seen"))
        result = subprocess.run(["bash", str(script), str(engine)], env=env,
                                text=True, capture_output=True, timeout=30)
        assert result.returncode == 17, (case, result.returncode, result.stdout,
                                         result.stderr)
        assert f"injected database failure: {case}" in result.stderr, case
        assert "Results:" not in result.stdout, (case, result.stdout)
        assert "FAIL:" not in result.stdout, (case, result.stdout)
        events = calls.read_text().splitlines()
        assert events[-1] == "failed" and events.count("failed") == 1, (case, events)
        print(f"PASS: scaling stops on {case} failure")

    for case in ("setup", "seed", "commit", "deep-commit", "postgc-commit",
                 "log", "checkout", "merge", "gc", "gc-size", "open", "median-late"):
        check_failure(case)
    for case, expected, needle in (
            ("throttled-disk", 0, None),
            ("work-regression", 1,
             "FAIL: dolt_gc, 10000000 vs 1000000 rows: nodes (448275 / 34482 = 13.0x > 12x)"),
            ("bytes-regression", 1,
             "FAIL: dolt_gc, 100000000 vs 10000000 rows: bytes (17030000000 / 1310000000 = 13.0x > 12x)"),
            ("merge-regression", 1,
             "FAIL: merge across divergence, deep vs 200: nodes (1700 / 100 = 17.0x > 16x)"),
            ("lookup-regression", 1,
             "FAIL: 400 point lookups, 100000000 vs 10000000 rows: bytes (2100 / 1000 = 2.1x > 2x)"),
            ("no-counters", 1, None)):
        clockfile = root / f"{case}.clock"
        clockfile.write_text("1000\n")
        env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                   SCALING_TEST_FAILURE=case, SCALING_TEST_CALLS=str(root / f"{case}.calls"),
                   SCALING_TEST_SEEN=str(root / f"{case}.seen"),
                   SCALING_TEST_CLOCK=str(clockfile))
        result = subprocess.run(["bash", str(script), str(engine)], env=env,
                                text=True, capture_output=True, timeout=60)
        assert result.returncode == expected, (case, result.returncode,
                                                result.stdout, result.stderr)
        if case == "no-counters":
            assert "no engine counters after: SELECT dolt_gc();" in result.stderr, result.stderr
            assert "Results:" not in result.stdout, result.stdout
        elif needle:
            assert needle in result.stdout, result.stdout
            assert result.stdout.count("FAIL:") == 1, result.stdout
        else:
            assert "FAIL:" not in result.stdout, result.stdout
            assert "gc=3971ms" in result.stdout, result.stdout
        print(f"PASS: scaling {case} policy")
