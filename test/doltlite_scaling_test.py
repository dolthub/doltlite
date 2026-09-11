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
  *"SELECT count(*), max(v) FROM s"*) echo '6|1' ;;
  *"SELECT sum(v) FROM t"*) echo 2750 ;;
esac
exit 0
''')
    engine.chmod(0o755)
    clock = root / "python3"
    clock.write_text('#!/bin/bash\necho 1000\n')
    clock.chmod(0o755)
    for case in ("setup", "seed", "commit", "deep-commit", "postgc-commit",
                 "log", "checkout", "merge", "gc", "open", "median-late"):
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
