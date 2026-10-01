#!/usr/bin/env python3
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tarfile
import sys

repo = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
checks = 0


def check(condition, message):
    global checks
    if not condition:
        raise AssertionError(message)
    checks += 1


def run(command, cwd, **env):
    return subprocess.run(command, cwd=cwd, env={**os.environ, **env},
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def suite(path, status=0, skip=False):
    path.write_text('printf "%s\\n" "$(basename "$0")" >> "$TRACE"\n'
                    + ('echo "SKIP: missing artifact"\n' if skip else
                       'echo "__SUITE_COMPLETE__"\n') + f'exit {status}\n')


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    test = root / 'test'
    build = root / 'build-coverage'
    (test / 'lib').mkdir(parents=True)
    build.mkdir()
    for name in ('run_doltlite_tests.sh', 'run_guarded_suite.sh'):
        shutil.copy(repo / 'test' / name, test / name)
    engine = build / 'doltlite'
    engine.touch()
    engine.chmod(0o755)
    manifest = test / 'lib/doltlite_suite_manifest.sh'
    manifest.write_text("doltlite_all_suites() { printf '%s\\n' first.sh second.sh last.sh; }\n")
    trace = root / 'trace'
    suite(test / 'first.sh', status=1)
    suite(test / 'second.sh', skip=True)
    suite(test / 'last.sh')
    result = run(['bash', str(test / 'run_doltlite_tests.sh')], root,
                 DOLTLITE_BUILD_DIR=str(build), TRACE=str(trace), DOLTLITE='')
    check(result.returncode != 0, 'native runner accepted failure/skip')
    check('1 passed, 1 failed, 1 skipped' in result.stdout, result.stdout)
    check(trace.read_text().splitlines() == ['first.sh', 'second.sh', 'last.sh'],
          'native runner hid later suites')
    suite(test / 'first.sh')
    trace.unlink()
    result = run(['bash', str(test / 'run_doltlite_tests.sh')], root,
                 DOLTLITE_BUILD_DIR=str(build), TRACE=str(trace), DOLTLITE='')
    check(result.returncode != 0, 'native runner accepted a skipped required suite')
    check('2 passed, 0 failed, 1 skipped' in result.stdout, result.stdout)
    suite(test / 'second.sh')
    (test / 'last.sh').write_text(
        '[ "$1" = "$DOLTLITE_BUILD_DIR/doltlite" ] || exit 1\n'
        'echo __SUITE_COMPLETE__\n')
    result = run(['bash', str(test / 'run_doltlite_tests.sh')], root,
                 DOLTLITE_BUILD_DIR=str(build), TRACE=str(trace), DOLTLITE='')
    check(result.returncode == 0, result.stdout)
    check('3 passed, 0 failed, 0 skipped' in result.stdout, result.stdout)

    (test / 'oracle-buckets').mkdir()
    (test / 'oracle-buckets/sample.txt').write_text('first.sh\nsecond.sh\nlast.sh\n')
    (test / 'assert_stock_reference.sh').write_text('exit 0\n')
    for workflow, step in [('test.yml', 'Version control oracle tests (vs Dolt)'),
                           ('test.yml', 'SQL oracle tests (vs stock SQLite)'),
                           ('sanitizers.yml', 'Version control oracle tests (vs Dolt)'),
                           ('sanitizers.yml', 'SQL oracle tests (vs stock SQLite and Dolt)')]:
        text = (repo / '.github/workflows' / workflow).read_text()
        part = text.split(f'- name: {step}\n', 1)[1]
        code = part.split('run: |\n', 1)[1]
        code = re.split(r'\n(?=    [^ ]|      [^ ])', code)[0]
        code = '\n'.join(line[8:] for line in code.splitlines())
        code = code.replace('${{ matrix.bucket }}', 'sample')
        (root / 'build').mkdir(exist_ok=True)
        if step.startswith('SQL'):
            names = ['sql_oracle_test.sh', 'oracle_import_test.sh', 'oracle_one_test.sh', 'oracle_two_test.sh']
        else:
            names = ['first.sh', 'second.sh', 'last.sh']
        for name in names:
            suite(test / name, status=1 if name == names[0] else 0)
        trace.unlink(missing_ok=True)
        result = run(['bash', '-e', '-c', code], root, TRACE=str(trace))
        check(result.returncode != 0, f'{workflow}: oracle failure accepted')
        check(trace.read_text().splitlines() == names, f'{workflow}: later suites hidden: {result.stdout}')
        for name in names:
            suite(test / name)
        result = run(['bash', '-e', '-c', code], root, TRACE=str(trace))
        check(result.returncode == 0, result.stdout)

with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    scripts = root / '.github/scripts'
    scripts.mkdir(parents=True)
    for name in ('package-coverage-build.sh', 'package-ci-build.sh'):
        shutil.copy(repo / '.github/scripts' / name, scripts / name)
    (root / 'test').mkdir()
    (root / 'test/assert_stock_reference.sh').write_text('exit 0\n')
    payload = root / 'build-coverage'
    payload.mkdir()
    for name in ('doltlite', 'doltlite-remotesrv', 'doltlite_regression_test_c',
                 'testfixture', 'prolly_hash.o', 'blake3.o', 'sample_test',
                 'libdoltlite.a', 'sqlite3.h'):
        (payload / name).write_bytes(name.encode())
        if name in ('doltlite', 'doltlite-remotesrv', 'testfixture'):
            (payload / name).chmod(0o755)
    (root / 'build-stockref').mkdir()
    (root / 'build-stockref/sqlite3').write_bytes(b'stock')
    (root / 'build-stockref/sqlite3').chmod(0o755)
    tools = root / 'bin'
    tools.mkdir()
    strip = tools / 'llvm-strip'
    strip.write_text('#!/usr/bin/env bash\n'
                     'for arg in "$@"; do case "$arg" in *.h) exit 1;; esac; done\n')
    strip.chmod(0o755)
    text = (repo / '.github/workflows/ci-build.yml').read_text().split('  coverage-build:', 1)[1]
    code = text.split('- name: Package\n', 1)[1].split('run: |\n', 1)[1]
    code = code.split('\n    - name:', 1)[0]
    code = '\n'.join(line[8:] for line in code.splitlines())
    result = run(['bash', '-e', '-c', code], root, PATH=f'{tools}:{os.environ["PATH"]}')
    check(result.returncode == 0, result.stdout)
    with tarfile.open(root / 'coverage-build.tar.gz') as archive:
        for name in ('libdoltlite.a', 'sqlite3.h'):
            check(archive.extractfile(f'build-coverage/{name}').read() == name.encode(),
                  f'coverage artifact lost {name}')
    for artifact in ('cli', 'fixture'):
        with tarfile.open(root / f'coverage-{artifact}.tar.gz') as archive:
            check('build-coverage/libdoltlite.a' not in archive.getnames(),
                  f'{artifact} runtime needlessly carries the library')

print(f'Harness housekeeping: {checks} checks passed')
