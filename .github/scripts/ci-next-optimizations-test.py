import os
from pathlib import Path
import shutil
import shlex
import subprocess
import tarfile
import tempfile

repo = Path(__file__).resolve().parents[2]
scripts = repo / '.github/scripts'
checks = 0


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(0o755)


def run(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, timeout=30, **kwargs)


def lint_workers():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / 'test').mkdir()
        (root / '.github/scripts').mkdir(parents=True)
        shutil.copy(repo / 'test/run_lint_selftests.sh', root / 'test')
        shutil.copy(scripts / 'parallel-compile.sh', root / '.github/scripts')
        (root / '.github/scripts/ci-selftests.sh').write_text(
            'ci_selftests() { ci_compile bash \"$root/.github/scripts/ci-optimization-test.sh\"; }\n')
        recipe = run(['make', '-f', 'main.mk', '-n', 'lint', f'TOP={repo}'], cwd=repo)
        assert recipe.returncode == 0, recipe
        commands = [shlex.split(line.replace(str(repo), str(root)))
                    for line in recipe.stdout.replace('\\\n', ' ').splitlines()
                    if '/test/run_lint_selftests.sh' in line]
        assert len(commands) == 1, recipe
        command = commands[0]
        worker = root / 'worker.py'
        worker.write_text('''import os, sys, time
from pathlib import Path
n = sys.argv[1]
root = Path(os.environ['EVENTS'])
(root / ('start-' + n)).touch()
if os.environ['JOBS'] == '3':
    deadline = time.monotonic() + 5
    while len(list(root.glob('start-*'))) != 3:
        assert time.monotonic() < deadline, 'workers did not overlap'
        time.sleep(0.01)
time.sleep(0.1)
(root / ('end-' + n)).touch()
print('worker ' + n)
sys.exit(42 if os.environ['FAIL'] == n else 0)
''')
        for i, name in enumerate(['test/lint_layers_selftest.sh', 'test/stock_oracle_harness_test.sh',
                                  '.github/scripts/ci-optimization-test.sh'], 1):
            executable(root / name, f'#!/bin/bash\nexec python3 "{worker}" {i}\n')
        events = root / 'events'
        for jobs, fail in [('3', str(i)) for i in range(4)] + [('1', '0')]:
            if events.exists():
                shutil.rmtree(events)
            events.mkdir()
            result = run(command,
                         env=dict(os.environ, DOLTLITE_LINT_JOBS=jobs, JOBS=jobs, FAIL=fail,
                                  EVENTS=str(events)))
            assert result.returncode == (42 if fail != '0' else 0), result
            assert sorted(p.name for p in events.glob('end-*')) == ['end-1', 'end-2', 'end-3']
            assert sorted(result.stdout.splitlines()) == ['worker 1', 'worker 2', 'worker 3']
            checks += 1
        for jobs in ('0', '-1', 'two'):
            result = run(command,
                         env=dict(os.environ, DOLTLITE_LINT_JOBS=jobs))
            assert result.returncode != 0
            checks += 1
        assert run(['bash', str(root / 'test/run_lint_selftests.sh')]).returncode != 0
        checks += 1


def macos_keys():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        stub = '''#!/bin/bash
set -eu
name=${0##*/}
[ "${FAIL_TOOL:-}" != "$name" ] || exit 42
case "$name $*" in
  'xcrun --find clang') echo "$TOOLS/clang" ;;
  *) echo "$name $* ${TOOL_VERSION:-1}" ;;
esac
'''
        for tool in ('sw_vers', 'xcrun', 'clang', 'cc', 'make', 'ccache'):
            executable(root / tool, stub)
        env = dict(os.environ, PATH=f'{root}:{os.environ["PATH"]}', TOOLS=str(root),
                   CACHE_BUILD_CONFIGURATION='checked', CACHE_BUILD_CFLAGS='-O2 -g -Werror',
                   CACHE_BUILD_LDFLAGS='')

        def key(**extra):
            return run(['bash', str(scripts / 'macos-build-cache-key.sh')], env=dict(env, **extra))

        baseline = key()
        assert baseline.returncode == 0 and len(baseline.stdout.strip()) == 64
        assert key().stdout == baseline.stdout
        checks += 1
        for name, value in [('CACHE_BUILD_CONFIGURATION', 'asan'), ('CACHE_BUILD_CFLAGS', '-O0'),
                            ('CACHE_BUILD_LDFLAGS', '-fsanitize=address'), ('TOOL_VERSION', 'next'),
                            ('ImageVersion', 'next'), ('CPPFLAGS', '-DCHANGED')]:
            result = key(**{name: value})
            assert result.returncode == 0 and result.stdout != baseline.stdout, result
            checks += 1
        with (root / 'clang').open('a') as f:
            f.write('\n# changed compiler contents\n')
        assert key().stdout != baseline.stdout
        checks += 1
        for tool in ('sw_vers', 'xcrun', 'cc', 'make', 'ccache'):
            assert key(FAIL_TOOL=tool).returncode != 0, tool
            checks += 1
        assert key(CACHE_BUILD_CONFIGURATION='../other').returncode != 0
        checks += 1


def compatibility_keys():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name in ('test/doltlite_compat_test.sh', '.github/scripts/compatibility-cache-key.sh',
                     '.github/actions/compatibility-cache/action.yml'):
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(repo / name, root / name)
        (root / 'src').mkdir()
        (root / 'src/chunk_store.h').write_text('#define CHUNK_STORE_VERSION 12\n#define CHUNK_STORE_MAGIC 7\n')
        (root / 'src/prolly_node.h').write_text('#define PROLLY_NODE_MAGIC 1\n')
        tools = root / 'bin'
        for tool in ('cc', 'make', 'ld', 'dpkg-query'):
            executable(tools / tool, '#!/bin/bash\n[ "${FAIL_TOOL:-}" != "${0##*/}" ] || exit 42\nprintf "%s\\n" "${0##*/} ${TOOL_VERSION:-1}"\n')
        executable(tools / 'sha256sum', '''#!/usr/bin/env python3
import hashlib, pathlib, sys
if len(sys.argv) == 1:
    print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest() + '  -')
else:
    for name in sys.argv[1:]:
        print(hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest() + '  ' + name)
''')
        env = dict(os.environ, PATH=f'{tools}:{os.environ["PATH"]}', GIT_AUTHOR_NAME='CI Test',
                   GIT_AUTHOR_EMAIL='ci@example.invalid', GIT_COMMITTER_NAME='CI Test',
                   GIT_COMMITTER_EMAIL='ci@example.invalid')

        def git(*args):
            result = run(['git', *args], cwd=root, env=env)
            assert result.returncode == 0, result
            return result.stdout.strip()

        git('init', '-q')
        git('add', 'src', 'test', '.github')
        git('commit', '-qm', 'seed')
        for tag in ('v0.11.0', 'v0.48.0', 'v0.49.0', 'v0.50.0'):
            git('tag', tag)
        git('commit', '--allow-empty', '-qm', 'release')
        git('tag', 'v0.50.1')
        git('commit', '--allow-empty', '-qm', 'candidate')
        result = run(['bash', 'test/doltlite_compat_test.sh', '--list-tags'], cwd=root, env=env)
        assert result.returncode == 0, result
        assert result.stdout.split() == ['v0.49.0', 'v0.48.0', 'v0.50.1', 'v0.50.0'], result
        checks += 1
        candidate = git('rev-parse', 'HEAD')
        git('checkout', '--detach', 'v0.50.1')
        exact = run(['bash', 'test/doltlite_compat_test.sh', '--list-tags'], cwd=root, env=env)
        assert exact.returncode == 0 and exact.stdout.split() == ['v0.49.0', 'v0.48.0', 'v0.50.0'], exact
        git('checkout', '--detach', candidate)
        checks += 1

        def key(**extra):
            return run(['bash', '.github/scripts/compatibility-cache-key.sh'], cwd=root,
                       env=dict(env, **extra))

        baseline = key()
        assert baseline.returncode == 0 and len(baseline.stdout.strip()) == 64, baseline
        assert key().stdout == baseline.stdout
        checks += 1
        for name, value in [('TOOL_VERSION', 'next'), ('CFLAGS', '-O0'), ('ImageVersion', 'next'),
                            ('DOLTLITE_COMPAT_TAGS', 'v0.50.0')]:
            changed = key(**{name: value})
            assert changed.returncode == 0 and changed.stdout != baseline.stdout, changed
            checks += 1
        for tool in ('cc', 'make', 'ld', 'dpkg-query'):
            assert key(FAIL_TOOL=tool).returncode != 0
            checks += 1
        assert key(DOLTLITE_COMPAT_TAGS='missing').returncode != 0
        checks += 1
        git('tag', '-f', 'v0.50.1', 'HEAD')
        git('commit', '--allow-empty', '-qm', 'new candidate')
        changed = key()
        assert changed.returncode == 0 and changed.stdout != baseline.stdout
        checks += 1
        git('tag', 'v0.50.2')
        git('commit', '--allow-empty', '-qm', 'after new release')
        assert key().stdout != changed.stdout
        checks += 1


def coverage_archives():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        payload = root / 'payload/build-coverage'
        payload.mkdir(parents=True)
        names = ['doltlite', 'doltlite-remotesrv', 'sqlite3-stock', 'testfixture', 'unit_test']
        for name in names:
            executable(payload / name, f'#!/bin/sh\necho {name}\n')
        (payload / 'blake3.o').write_bytes(b'object')
        command = ['bash', str(scripts / 'package-coverage-build.sh'), str(payload.parent), str(root)]
        result = run(command)
        assert result.returncode == 0, result
        checks += 1
        for archive, expected in [('build', names + ['blake3.o']), ('cli', names[:3]), ('fixture', names[:4])]:
            dest = root / archive
            with tarfile.open(root / f'coverage-{archive}.tar.gz') as tar:
                files = [m for m in tar if m.isfile()]
                assert sorted(Path(m.name).name for m in files) == sorted(expected)
                for m in files:
                    original = payload / Path(m.name).name
                    assert tar.extractfile(m).read() == original.read_bytes()
                    assert m.mode == original.stat().st_mode & 0o777
                tar.extractall(dest, filter='data')
            for name in expected:
                if name == 'blake3.o':
                    continue
                assert run([str(dest / 'build-coverage' / name)]).stdout.strip() == name
            checks += 1
        for name in names[:4]:
            original = (payload / name).read_bytes()
            (payload / name).unlink()
            assert run(command).returncode != 0, name
            executable(payload / name, original.decode())
            checks += 1


def wiring():
    global checks
    import re
    workflow = (repo / '.github/workflows/test.yml').read_text()
    for job, artifact in [('oracle-tests', 'coverage-cli'), ('sql-differential', 'coverage-cli'),
                          ('coverage-remotes', 'coverage-cli'), ('sqlite-regression', 'coverage-fixture'),
                          ('coverage-native', 'coverage-build'), ('source-coverage', 'coverage-build')]:
        text = re.search(r'^  ' + job + r':\n(.*?)(?=^  [\w-]+:|\Z)', workflow, re.M | re.S)[1]
        assert f'name: {artifact}\n' in text and f'tar -xzf {artifact}.tar.gz' in text
        checks += 1
    seed = (repo / '.github/workflows/seed-ci-caches.yml').read_text()
    assert 'uses: ./.github/actions/compatibility-cache' in seed
    assert 'uses: ./.github/actions/compatibility-cache' in (repo / '.github/workflows/ci-build.yml').read_text()
    mac = seed.split('  macos:\n')[1]
    assert "github.event_name != 'push'" in mac
    assert mac.count('uses: ./.github/actions/macos-build-cache') == 2
    for name in ('checked-build', 'asan-build'):
        assert 'uses: ./.github/actions/macos-build-cache' in (repo / f'.github/actions/{name}/action.yml').read_text()
    assert seed.count('runs-on: macos-latest') == 1
    mac_build = (repo / '.github/workflows/macos-build.yml').read_text()
    warning = re.search(r'warning-flags: (.*)', mac_build)[1]
    flags = re.search(r'ASAN_CFLAGS: (.*)\n        (.*)', mac_build)
    asan_flags = (flags[1] + ' ' + flags[2]).replace('${{ matrix.warning-flags }}', warning)
    assert f'cflags: {asan_flags}\n' in mac
    assert f'CFLAGS="{asan_flags}"' in mac
    assert mac_build.count('CFLAGS: -O2 -g -Werror') == 1
    assert mac.count('CFLAGS: -O2 -g -Werror') == 1
    checks += 1


lint_workers()
macos_keys()
compatibility_keys()
coverage_archives()
wiring()
print(f'CI lint scheduling, compiler/release caches and coverage payloads: {checks} checks passed')
