import os
from pathlib import Path
import re
import subprocess
import tempfile


workflow = (Path(__file__).resolve().parents[1] / "workflows/ci-build.yml").read_text()
cases = (
    ("checked", "Build Linux-only test binaries", "ubuntu"),
    ("checked", "Build standalone Unix test probes", "ubuntu"),
    ("checked", "Build standalone Unix test probes", "macos"),
    ("tsan-build", "Build", "ubuntu"),
)
stub = """#!/usr/bin/env bash
set -eu
printf '%s\\n' "${0##*/} $*" >> "$COMMAND_LOG"
if [ "$(wc -l < "$COMMAND_LOG")" -eq "$FAIL_AT" ]; then
  echo 'injected compiler failure' >&2
  exit 42
fi
if [ "$EMIT_WARNING" = 1 ]; then echo 'warning: injected compiler warning'; fi
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then touch "$2"; break; fi
  shift
done
"""


def run(script, fail_at, warning, errexit):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "build").mkdir()
        (root / "examples/go").mkdir(parents=True)
        (root / "bin").mkdir()
        for command in ("make", "cc", "gcc", "clang", "go"):
            path = root / "bin" / command
            path.write_text(stub)
            path.chmod(0o755)
        path = root / "step.sh"
        path.write_text(script + '\nprintf packaged > "$PACKAGE_MARKER"\n')
        env = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}",
                   COMMAND_LOG=str(root / "commands"), FAIL_AT=str(fail_at),
                   EMIT_WARNING=str(warning), PACKAGE_MARKER=str(root / "package"),
                   CFLAGS="-O2", TSAN_CFLAGS="-O1 -fsanitize=thread",
                   TSAN_LDFLAGS="-fsanitize=thread")
        result = subprocess.run(
            ["bash", *(["-e"] if errexit else []), str(path)],
            cwd=root, env=env, capture_output=True, text=True, timeout=30,
        )
        commands = (root / "commands").read_text().splitlines()
        log = (root / "build/build.log").read_text()
        return result, commands, log, (root / "package").exists()


checks = 0
for job, step, platform in cases:
    job_match = re.search(rf"^  {re.escape(job)}:\n(.*?)(?=^  \S|\Z)",
                          workflow, re.M | re.S)
    assert job_match, job
    step_match = re.search(
        rf"^    - name: {re.escape(step)}\n.*?^      run: \|\n((?:        [^\n]*\n|\n)+)",
        job_match[1], re.M | re.S,
    )
    assert step_match, (job, step)
    script = "".join(line[8:] if line.startswith("        ") else line
                     for line in step_match[1].splitlines(keepends=True))
    script = script.replace("${{ matrix.platform }}", platform)
    for errexit in (False, True):
        result, commands, log, packaged = run(script, 0, 0, errexit)
        assert result.returncode == 0 and packaged, (step, result.stderr)
        checks += 1
        for fail_at in range(1, len(commands) + 1):
            result, executed, log, packaged = run(script, fail_at, 0, errexit)
            assert result.returncode == 42 and not packaged, (step, fail_at, result)
            assert executed == commands[:fail_at], (step, fail_at, executed)
            assert "injected compiler failure" in log, (step, fail_at, log)
            checks += 1
        result, _, log, packaged = run(script, 0, 1, errexit)
        assert result.returncode != 0 and not packaged, (step, result)
        assert "warning: injected compiler warning" in log, step
        checks += 1

print(f"CI build failure propagation: {checks} checks passed")
