import os
from pathlib import Path
import shutil
import subprocess
import tempfile


scripts = Path(__file__).resolve().parent
stub = '''#!/usr/bin/env bash
set -eu
name="${0##*/}"
if [ "${KEY_TOOL_FAIL:-}" = "$name" ]; then exit 42; fi
case "$name" in
  cc|clang) echo "compiler $*" ;;
  make) echo make-4 ;;
  dpkg-query) echo "zlib=1.2.13"; echo "tcl=8.6" ;;
  uname) echo x86_64 ;;
esac
'''


def main():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "bin").mkdir()
        (root / "scripts").mkdir()
        source = root / "base"
        source.mkdir()
        for name in ("cc", "clang", "as", "ld", "make", "dpkg-query", "uname"):
            path = root / "bin" / name
            path.write_text(stub)
            path.chmod(0o755)
        for name in ("benchmark-cache-key.sh", "build-optimized-benchmark.sh"):
            shutil.copy(scripts / name, root / "scripts")
        subprocess.run(["git", "init", "-q", str(source)], check=True)
        (source / "source.c").write_text("first\n")
        subprocess.run(["git", "-C", str(source), "add", "source.c"], check=True)
        commit = ["git", "-C", str(source), "-c", "user.name=CI Test", "-c", "user.email=ci@example.com", "commit", "-qm"]
        subprocess.run([*commit, "first"], check=True)
        env = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}")

        def key(extra=None):
            result = subprocess.run(["bash", str(root / "scripts/benchmark-cache-key.sh"), str(source)],
                                    env=dict(env, **(extra or {})), text=True, capture_output=True)
            return result

        baseline = key()
        assert baseline.returncode == 0 and len(baseline.stdout.strip()) == 64, baseline
        assert key().stdout == baseline.stdout
        checks = 1
        for rel in ("bin/cc", "bin/as", "bin/ld", "bin/make", "bin/dpkg-query", "bin/uname",
                    "scripts/build-optimized-benchmark.sh", "scripts/benchmark-cache-key.sh"):
            path = root / rel
            original = path.read_text()
            path.write_text(original.replace("make-4", "make-5").replace("zlib=1.2.13", "zlib=1.3").replace("x86_64", "aarch64") + "\n# changed\n")
            changed = key()
            assert changed.returncode == 0 and changed.stdout != baseline.stdout, (rel, changed)
            path.write_text(original)
            assert key().stdout == baseline.stdout, rel
            checks += 1
        for name, value in (("CFLAGS", "-O0"), ("CPPFLAGS", "-DTEST=1"), ("LDFLAGS", "-static"),
                            ("MAKEFLAGS", "-e"), ("CC", "clang"), ("ImageVersion", "next"), ("LIBRARY_PATH", "/different")):
            changed = key({name: value})
            assert changed.returncode == 0 and changed.stdout != baseline.stdout, (name, changed)
            checks += 1
        clang_key = key({"CC": "clang"}).stdout
        with (root / "bin/clang").open("a") as stream:
            stream.write("\n# changed compiler\n")
        assert key({"CC": "clang"}).stdout != clang_key
        checks += 1
        (source / "source.c").write_text("second\n")
        subprocess.run(["git", "-C", str(source), "add", "source.c"], check=True)
        subprocess.run([*commit, "second"], check=True)
        assert key().stdout != baseline.stdout
        checks += 1
        for tool in ("cc", "make", "dpkg-query", "uname"):
            assert key({"KEY_TOOL_FAIL": tool}).returncode != 0, tool
            checks += 1
    print(f"Benchmark cache identity: {checks} invalidation and failure checks passed")


if __name__ == "__main__":
    main()
