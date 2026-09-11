import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import textwrap

scripts = Path(__file__).resolve().parent
github = scripts.parent


def step(text, name, indent):
    match = re.search(
        rf"^{indent}- name: {re.escape(name)}\n.*?^{indent}  run: \|\n"
        rf"((?:{indent}    [^\n]*\n|\n)+)", text, re.M | re.S)
    assert match, name
    return textwrap.dedent(match[1]).strip()


def write_executable(path, contents):
    path.write_text(contents)
    path.chmod(0o755)


def packages():
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name in ('go', 'rust'):
            (root / 'packaging' / name).mkdir(parents=True)
            (root / 'packaging' / name / 'assemble.sh').touch()
            write_executable(root / 'packaging' / name / 'test-package.sh', r'''#!/bin/bash
set -eu
name=$(basename "$(dirname "$0")")
printf '%s\n' "$CC" > "$name.compiler"
printf '%s\n' "$*" > "$name.args"
touch "$name.started"
for ((i=0; i<200; i++)); do
  if [ -f go.started ] && [ -f rust.started ]; then break; fi
  sleep .01
done
[ -f go.started ] && [ -f rust.started ] || exit 99
printf '%s stdout\n' "$name"
printf '%s stderr\n' "$name" >&2
if [ "$FAIL" = "$name" ] || [ "$FAIL" = both ]; then exit 42; fi
touch "$name.finished"
''')
        (root / 'packaging/rust/test-cache.sh').write_text(
            'set -eu\ntouch invalidation.started\n'
            '[ "$FAIL" != invalidation ] || exit 43\n'
            'echo invalidation-passed\n')
        (root / 'bin').mkdir()
        write_executable(root / 'bin/go', '#!/bin/sh\necho clang\n')
        env = dict(os.environ, PATH=f'{root / "bin"}:{os.environ["PATH"]}',
                   RUNNER_TEMP=str(root))
        env.pop('CC', None)
        for fail in ('', 'go', 'rust', 'both', 'invalidation'):
            for path in root.glob('*.started'):
                path.unlink()
            for path in root.glob('*.finished'):
                path.unlink()
            result = subprocess.run(['bash', str(scripts / 'macos-package-tests.sh'), 'build with spaces'],
                                    cwd=root, env=dict(env, FAIL=fail), text=True,
                                    capture_output=True, timeout=15)
            assert result.returncode == (1 if fail else 0), result
            for name, compiler in (('go', 'clang'), ('rust', 'cc')):
                assert (root / f'{name}.started').exists(), (fail, name)
                assert (root / f'{name}.compiler').read_text() == f'ccache {compiler}\n'
                assert (root / f'{name}.args').read_text() == 'build with spaces\n'
                assert f'{name} stdout' in result.stdout and f'{name} stderr' in result.stdout
                assert f'{name} package checks (exit {42 if fail in (name, "both") else 43 if name == "rust" and fail == "invalidation" else 0})' in result.stdout, result
            assert (root / 'invalidation.started').exists() == (fail not in ('rust', 'both'))
            checks += 1
    return checks


def artifact():
    action = (github / 'actions/asan-build/action.yml').read_text()
    command = step(action, 'Package', '  ').replace('${{ inputs.platform }}', 'macos')
    expected = {'build/doltlite', 'build/sqlite3', 'build/doltlite_regression_test_c',
                'build/libdoltlite.a', 'build/sqlite3.h', 'build/build.log'}
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / 'build').mkdir()
        (root / '.github/scripts').mkdir(parents=True)
        shutil.copy(scripts / 'package-ci-build.sh', root / '.github/scripts')
        for name in expected:
            write_executable(root / name, '#!/bin/sh\nprintf "' + name + '\\n"\n')
        (root / 'build/unneeded.o').write_bytes(b'object' * 100)
        subprocess.run(['bash', '-e', '-c', command], cwd=root, check=True)
        with tarfile.open(root / 'asan-ubsan-macos.tar.gz') as archive:
            assert set(archive.getnames()) == expected
            for member in archive:
                path = root / member.name
                assert archive.extractfile(member).read() == path.read_bytes()
                assert member.mode == path.stat().st_mode & 0o777
            archive.extractall(root / 'consumer', filter='data')
        for name in ('doltlite', 'sqlite3', 'doltlite_regression_test_c'):
            result = subprocess.run([str(root / 'consumer/build' / name)], capture_output=True, check=True)
            assert result.stdout == f'build/{name}\n'.encode()
        checks += 1
        for name in expected:
            path = root / name
            saved = path.read_bytes()
            path.unlink()
            result = subprocess.run(['bash', '-e', '-c', command], cwd=root, capture_output=True)
            assert result.returncode != 0, name
            path.write_bytes(saved)
            checks += 1
    return checks


def checked_build():
    action = (github / 'actions/checked-build/action.yml').read_text()
    command = step(action, 'Build checked Unix binaries', '  ')
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for directory in ('bin', 'build', 'test'):
            (root / directory).mkdir()
        (root / '.github/scripts').mkdir(parents=True)
        shutil.copy(scripts / 'use-macos-compiler-cache.sh', root / '.github/scripts')
        (root / 'ccache/libexec').mkdir(parents=True)
        for name in ('cc', 'c++'):
            write_executable(root / 'ccache/libexec' / name, '#!/bin/bash\nexit 0\n')
        write_executable(root / 'bin/brew', f'#!/bin/bash\necho "{root}/ccache"\n')
        write_executable(root / 'bin/sysctl', '#!/bin/sh\necho 6\n')
        write_executable(root / 'bin/make', r'''#!/bin/bash
set -eu
printf '%s\n' "$*" >> "$LOG"
if [ "$(wc -l < "$LOG")" -eq "$FAIL_AT" ]; then exit 42; fi
if [ "$WARNING" = 1 ]; then echo 'warning: injected'; fi
''')
        for name in ('assert_doltlite_artifacts.sh', 'amalgamation_vec1_option_test.sh'):
            (root / 'test' / name).write_text('exit 0\n')
        env = dict(os.environ, PATH=f'{root / "bin"}:{os.environ["PATH"]}', LOG=str(root / 'commands'))
        for platform in ('ubuntu', 'macos'):
            for fail, warning in [(0, 0), *[(i, 0) for i in range(1, 6)], (0, 1)]:
                (root / 'commands').write_text('')
                result = subprocess.run(['bash', '-e', '-c', command.replace('${{ inputs.platform }}', platform)],
                                        cwd=root, env=dict(env, FAIL_AT=str(fail), WARNING=str(warning)),
                                        capture_output=True)
                expected = [f'-j{6 if platform == "macos" else 2} DOLTLITE_PROLLY_CHECK=1 doltlite doltlite-remotesrv doltlite-lib',
                            'DOLTLITE_PROLLY=0 sqlite3', 'libsqlite3.a USE_AMALGAMATION=0',
                            'DOLTLITE_PROLLY=1 DOLTLITE_PROLLY_CHECK=1 sqlite3.c sqlite3.h', 'lint']
                assert result.returncode == (42 if fail else 1 if warning else 0), result
                assert (root / 'commands').read_text().splitlines() == expected[:fail or (4 if warning else 5)]
                checks += 1
    return checks


def macos_identity():
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        stub = r'''#!/bin/bash
set -eu
name=${0##*/}
[ "${FAIL_TOOL:-}" != "$name" ] || exit 42
case "$name $*" in
  'xcrun --find clang') echo "$TOOLS/clang" ;;
  'xcrun --show-sdk-version') echo "${SDK_VERSION:-26}" ;;
  *) echo "$name $* ${TOOL_VERSION:-1}" ;;
esac
'''
        for name in ('sw_vers', 'xcrun', 'clang', 'cc', 'go', 'rustc', 'cargo', 'ccache'):
            write_executable(root / name, stub)
        env = dict(os.environ, PATH=f'{root}:{os.environ["PATH"]}', TOOLS=str(root))

        def key(extra=None):
            return subprocess.run(['bash', str(scripts / 'macos-package-cache-key.sh')],
                                  env=dict(env, **(extra or {})), capture_output=True, text=True)

        baseline = key()
        assert baseline.returncode == 0 and len(baseline.stdout.strip()) == 64, baseline
        assert key().stdout == baseline.stdout
        checks += 1
        for name, value in (('SDK_VERSION', 'next'), ('TOOL_VERSION', 'next'),
                            ('ImageVersion', 'next'), ('CFLAGS', '-O0'),
                            ('CPPFLAGS', '-DCHANGED'), ('LDFLAGS', '-changed'),
                            ('RUSTFLAGS', '-C opt-level=0'), ('CC', 'clang')):
            changed = key({name: value})
            assert changed.returncode == 0 and changed.stdout != baseline.stdout, (name, changed)
            checks += 1
        with (root / 'clang').open('a') as out:
            out.write('\n# different compiler bytes\n')
        assert key().stdout != baseline.stdout
        checks += 1
        for name in ('sw_vers', 'xcrun', 'cc', 'go', 'rustc', 'cargo', 'ccache'):
            assert key({'FAIL_TOOL': name}).returncode != 0, name
            checks += 1
    return checks


def cache_producers():
    producer = (github / 'workflows/seed-ci-caches.yml').read_text()
    consumer = (github / 'workflows/ci-build.yml').read_text().split('  benchmark-build:\n')[1].split('  asan-ubsan-build:')[0]
    for name in ('Install dependencies', 'Identify base build cache', 'Build base', 'Validate base build'):
        assert step(producer, name, '    ') == step(consumer, name, '    '), name
    for text in (producer, consumer):
        assert 'path: benchmark-baseline\n' in text
        assert 'key: benchmark-base-v1-${{ runner.os }}-${{ runner.arch }}-${{ steps.base-key.outputs.key }}\n' in text
    assert 'restore-keys:' not in producer.split('  macos:')[0]
    assert producer.count("github.ref == format('refs/heads/{0}', github.event.repository.default_branch)") == 2
    assert "github.event_name != 'push'" in producer.split('  macos:')[1]
    assert producer.count('cancel-in-progress: false') == 2
    platform = (github / 'workflows/platform-test.yml').read_text()
    for text in (producer, platform):
        assert 'uses: ./.github/actions/macos-package-cache' in text
        assert 'bash .github/scripts/macos-package-tests.sh build' in text
        assert 'source .github/scripts/use-macos-compiler-cache.sh' in text
        assert 'bash ../.github/scripts/check-compiler-cache.sh' in text
    action = (github / 'actions/macos-package-cache/action.yml').read_text()
    assert 'echo "CCACHE_DIR=$RUNNER_TEMP/doltlite-ccache"' in action
    assert '${{ runner.temp }}/doltlite-ccache' in action
    assert "'src/**'" in action and "'build/sqlite3.c'" in action
    assert 'CCACHE_COMPILERCHECK=content' in action
    return 10


if __name__ == '__main__':
    checks = packages() + artifact() + checked_build() + macos_identity() + cache_producers()
    print(f'CI caches, package workers, checked builds and sanitizer artifacts: {checks} checks passed')
