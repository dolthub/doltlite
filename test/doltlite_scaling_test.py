#!/usr/bin/env python3
import os
from pathlib import Path
import shlex
import shutil
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
  gc-size-late:*"size100000 SELECT dolt_gc()"*)
    [ -f "$SCALING_TEST_SEEN" ] && fail=1
    touch "$SCALING_TEST_SEEN"
    ;;
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
if [ -n "${SCALING_TEST_CLOCK:-}" ]; then
  duration=10
  case "$*" in
    *"/size"*" SELECT dolt_gc();")
      n=${1##*size}
      if [ "$(cat "$1")" != fixture ]; then
        echo "GC sample reused a collected database" >&2
        exit 19
      fi
      echo collected > "$1"
      countfile="$SCALING_TEST_SEEN.$n"
      count=0
      if [ -f "$countfile" ]; then read -r count < "$countfile"; fi
      count=$((count+1))
      echo "$count" > "$countfile"
      case "$n" in
        100000) duration=35 ;;
        1000000)
          duration=507
          if [ "$SCALING_TEST_FAILURE" = regression ] \
             || { [ "$SCALING_TEST_FAILURE" = fast-sample ] && [ "$count" -eq 1 ]; }; then
            duration=195
          fi
          ;;
        10000000)
          duration=3971
          if [ "$SCALING_TEST_FAILURE" = slow-sample ] && [ "$count" -eq 1 ]; then
            duration=12000
          fi
          ;;
        100000000) duration=51494 ;;
      esac
      ;;
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
    copy = root / "cp"
    copy.write_text(r'''#!/bin/bash
case "$SCALING_TEST_FAILURE" in
  copy-first) fail=1 ;;
  copy-late)
    [ -f "$SCALING_TEST_SEEN" ] && fail=1
    touch "$SCALING_TEST_SEEN"
    ;;
esac
if [ "${fail:-0}" = 1 ]; then
  echo "injected copy failure" >&2
  exit 23
fi
exec ''' + shlex.quote(shutil.which("cp")) + ' "$@"\n')
    copy.chmod(0o755)
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
    for case, expected in (("fast-sample", 0), ("slow-sample", 0), ("regression", 1)):
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
        if case == "regression":
            assert "FAIL: dolt_gc, 10000000 vs 1000000 rows" in result.stdout, result.stdout
            assert "20.3x > 20x" in result.stdout, result.stdout
        else:
            assert "FAIL:" not in result.stdout, result.stdout
        for size in (100000, 1000000, 10000000, 100000000):
            assert (root / f"{case}.seen.{size}").read_text() == "3\n", (case, size)
        print(f"PASS: scaling GC {case} policy")
    check_failure("gc-size-late")
    for case in ("copy-first", "copy-late"):
        env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                   SCALING_TEST_FAILURE=case, SCALING_TEST_CALLS=str(root / f"{case}.calls"),
                   SCALING_TEST_SEEN=str(root / f"{case}.seen"))
        result = subprocess.run(["bash", str(script), str(engine)], env=env,
                                text=True, capture_output=True, timeout=30)
        assert result.returncode == 23, (case, result.returncode, result.stdout, result.stderr)
        assert "injected copy failure" in result.stderr, result.stderr
        assert "Results:" not in result.stdout, result.stdout
        print(f"PASS: scaling stops on {case} failure")
