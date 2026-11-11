import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import textwrap


scripts = Path(__file__).resolve().parent
workflow = (scripts.parent / "workflows/ci-build.yml").read_text().split("  benchmark-build:\n", 1)[1].split("\n  asan-ubsan-build:", 1)[0]


def step(name):
    match = re.search(rf"    - name: {re.escape(name)}\n.*?      run: \|\n((?:        [^\n]*\n|\n)+)", workflow, re.S)
    assert match, name
    return textwrap.dedent(match[1])


stub = r'''#!/usr/bin/env bash
set -eu
name="${0##*/}"
printf '%s %s\n' "$name" "$*" >> "$COMMAND_LOG"
if [ "${FAIL_COMMAND:-}" = "$name" ]; then echo 'injected failure' >&2; exit 42; fi
if [ "${WARN_COMMAND:-}" = "$name" ]; then echo 'warning: injected warning' >&2; fi
case "$name" in
  configure) ;;
  make) printf '#!/bin/sh\necho candidate\n' > doltlite; chmod +x doltlite ;;
  cc) printf '#!/bin/sh\necho timer\n' > bench_timer_doltlite; chmod +x bench_timer_doltlite ;;
esac
'''


def main():
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "bin").mkdir()
        (root / "source").mkdir()
        for name in ("make", "cc"):
            path = root / "bin" / name
            path.write_text(stub)
            path.chmod(0o755)
        (root / "bin/find").write_text("#!/bin/sh\nexit 0\n")
        (root / "bin/find").chmod(0o755)
        (root / "source/configure").write_text(stub)
        (root / "source/configure").chmod(0o755)
        log = root / "commands"
        env = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}", COMMAND_LOG=str(log))
        for fail, warning in (("", ""), ("configure", ""), ("make", ""), ("cc", ""), ("", "make"), ("", "cc")):
            log.unlink(missing_ok=True)
            result = subprocess.run(["bash", str(scripts / "build-optimized-benchmark.sh"), str(root / "source")],
                                    env=dict(env, FAIL_COMMAND=fail, WARN_COMMAND=warning), capture_output=True, text=True)
            assert result.returncode == (42 if fail else 1 if warning else 0), result
            commands = log.read_text().splitlines()
            expected = ["configure ", "make -j2 doltlite doltlite-lib", "cc -O2 -Werror -I. -I../src -o bench_timer_doltlite ../test/sysbench_timer.c libdoltlite.a -lpthread -lz -lm"]
            if fail:
                expected = expected[:["configure", "make", "cc"].index(fail) + 1]
            assert commands == expected, commands
            if warning:
                assert "warning: injected warning" in (root / "source/build/build.log").read_text()
            checks += 1
        shutil.copytree(root / "source/build", root / "benchmark-baseline")
        valid_log = "make -j2 doltlite doltlite-lib\n"
        base = root / "benchmark-baseline"
        for bad in ("", "warning", "log", "binary", "permission"):
            (base / "build.log").write_text(valid_log)
            (base / "doltlite").write_text("#!/bin/sh\nexit 0\n")
            (base / "doltlite").chmod(0o755)
            if bad == "warning":
                (base / "build.log").write_text("warning: cached compiler warning\n")
            elif bad == "log":
                (base / "build.log").unlink()
            elif bad == "binary":
                (base / "doltlite").unlink()
            elif bad == "permission":
                (base / "doltlite").chmod(0o644)
            result = subprocess.run(["bash", "-e", "-c", step("Validate base build")], cwd=root, capture_output=True)
            assert (result.returncode == 0) == (bad == ""), (bad, result)
            checks += 1
        (base / "doltlite").chmod(0o755)
        shutil.copytree(root / "source/build", root / "build")
        (root / ".github/scripts").mkdir(parents=True)
        shutil.copy(scripts / "package-ci-build.sh", root / ".github/scripts")
        (root / "bin/git").write_text("#!/bin/sh\nprintf '%040d\\n' 1\n")
        (root / "bin/git").chmod(0o755)
        (root / "build/unused.o").write_bytes(b"unused" * 1000)
        subprocess.run(["bash", "-e", "-c", step("Package")], cwd=root, env=env, check=True)
        with tarfile.open(root / "benchmark-build.tar.gz") as archive:
            expected = {"build/doltlite", "build/bench_timer_doltlite", "benchmark-baseline/doltlite",
                        "benchmark-baseline/bench_timer_doltlite", "benchmark-candidate-sha", "benchmark-baseline-sha"}
            assert set(archive.getnames()) == expected
            for entry in archive:
                assert archive.extractfile(entry).read() == (root / entry.name).read_bytes()
                assert entry.mode == (root / entry.name).stat().st_mode & 0o777
            archive.extractall(root / "consumer", filter="data")
        for path in sorted(expected - {"benchmark-candidate-sha", "benchmark-baseline-sha"}):
            subprocess.run([str(root / "consumer" / path)], check=True, capture_output=True)
        checks += 1
    print(f"Benchmark builds, cached validation and runtime archive: {checks} checks passed")


if __name__ == "__main__":
    main()
