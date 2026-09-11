import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


script = Path(__file__).with_name("sqllogictest_gate.py")
runner = '''#!/usr/bin/env python3
import json, os, pathlib, sys
_, limit, binary, verify, path = sys.argv
assert limit == '300' and verify == '--verify'
with open(os.environ['CALLS'], 'a') as stream:
    stream.write(f'{binary} {pathlib.Path(path).name}\\n')
output, rc = json.loads(pathlib.Path(path).read_text())[binary]
print(output, file=sys.stderr)
sys.exit(rc)
'''
cases = [
    ("pass", "", ("0 errors out of 2 tests", 0), ("0 errors out of 2 tests", 0), 0, []),
    ("stock-only", "", ("0 errors out of 2 tests", 0), ("!DIVERGE 7", 1), 0, []),
    ("known", "case.test 7\ncase.test 7 # duplicate\n", ("!DIVERGE 7\n1 errors out of 2 tests", 1), ("", 0), 0,
     ["OK: case.test (1 known divergences)"]),
    ("unexpected", "", ("!DIVERGE 2 !DIVERGE 10 !DIVERGE 2\n2 errors out of 3 tests", 1), ("", 0), 1,
     ["    line 10\n    line 2", "unexpected=2, crashes=0, to-remove=0"]),
    ("fixed", "case.test 7\n", ("0 errors out of 2 tests", 0), ("", 0), 1,
     ["FIXED: case.test", "unexpected=0, crashes=0, to-remove=1"]),
    ("mixed", "case.test 7\ncase.test 8\n", ("!DIVERGE 7 !DIVERGE 9\n2 errors out of 2 tests", 1), ("", 0), 1,
     ["    line 9", "    line 8", "unexpected=1, crashes=0, to-remove=1"]),
    ("timeout", "case.test 7\n", ("!DIVERGE 7\n1 errors out of 2 tests", 124), ("", 0), 1,
     ["CRASH/TIMEOUT: case.test (doltlite rc=124)", "unexpected=0, crashes=1, to-remove=0"]),
    ("crash", "", ("segmentation fault", 139), ("", 0), 1,
     ["CRASH/TIMEOUT: case.test (doltlite rc=139)"]),
    ("missing-summary", "", ("!DIVERGE 1", 0), ("", 0), 1,
     ["CRASH/TIMEOUT: case.test (doltlite rc=0)"]),
    ("stock-timeout", "", ("!DIVERGE 1\n1 errors out of 2 tests", 1), ("!DIVERGE 1", 124), 0, []),
    ("stale", "missing.test 4\nmissing.test 4\nmissing.test 8\n", ("0 errors out of 2 tests", 0), ("", 0), 1,
     ["missing.test (file missing, 3 entries)", "to-remove=3"]),
    ("comments", "  # comment\n\tcase.test\t7 # reason\n\n", ("!DIVERGE 7\n1 errors out of 2 tests", 1), ("", 0), 0, []),
    ("leading-zero", "case.test 07\n", ("!DIVERGE 7\n1 errors out of 2 tests", 1), ("", 0), 1,
     ["    line 7", "    line 07", "unexpected=1, crashes=0, to-remove=1"]),
    ("missing-manifest", None, ("0 errors out of 2 tests", 0), ("", 0), 0, []),
]


def main():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "timeout").write_text(runner)
        (root / "timeout").chmod(0o755)
        corpus = root / "corpus with spaces"
        corpus.mkdir()
        path = corpus / "case.test"
        manifest, calls = root / "manifest", root / "calls"
        env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", CALLS=str(calls))
        for name, expected, dl, st, rc, needles in cases:
            path.write_text(json.dumps({"doltlite": dl, "stock": st}))
            manifest.unlink(missing_ok=True)
            calls.unlink(missing_ok=True)
            if expected is not None:
                manifest.write_text(expected)
            result = subprocess.run([sys.executable, str(script), "doltlite", "stock",
                                     str(corpus), str(manifest), "300", str(path)],
                                    env=env, capture_output=True, text=True, timeout=10)
            assert result.returncode == rc, (name, result)
            for needle in needles:
                assert needle in result.stdout, (name, needle, result.stdout)
            wanted = ["doltlite case.test"]
            if dl[1] != 124 and "errors out of" in dl[0]:
                wanted.append("stock case.test")
            assert calls.read_text().splitlines() == wanted, name
        second = corpus / "second.test"
        third = corpus / "third.test"
        path.write_text(json.dumps({"doltlite": ("!DIVERGE 3\n1 errors out of 2 tests", 1), "stock": ("", 0)}))
        second.write_text(json.dumps({"doltlite": ("!DIVERGE 5\n1 errors out of 2 tests", 1), "stock": ("", 0)}))
        third.write_text(json.dumps({"doltlite": ("crashed", 139), "stock": ("", 0)}))
        manifest.write_text("case.test 3\nsecond.test 7\nmissing.test 8\nmissing.test 8\n")
        result = subprocess.run([sys.executable, str(script), "doltlite", "stock",
                                 str(corpus), str(manifest), "300", str(path), str(second), str(third)],
                                env=env, capture_output=True, text=True, timeout=10)
        assert result.returncode == 1, result
        for needle in ("files:                  3", "files with divergences: 2",
                       "known divergences:      2", "unexpected=1, crashes=1, to-remove=3"):
            assert needle in result.stdout, result.stdout
    print(f"SQLLogicTest gate: {len(cases) + 1} classification and invocation checks passed")


if __name__ == "__main__":
    main()
