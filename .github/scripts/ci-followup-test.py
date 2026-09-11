import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import textwrap

repo = Path(__file__).resolve().parents[2]
checks = 0


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(0o755)


def run(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, timeout=30, **kwargs)


def step(path, name, indent='  '):
    match = re.search(rf'^{indent}- name: {re.escape(name)}\n.*?^{indent}  run: \|\n'
                      rf'((?:{indent}    [^\n]*\n|\n)+)', path.read_text(), re.M | re.S)
    assert match, name
    return textwrap.dedent(match[1])


def compiler_cache():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / '.github/scripts').mkdir(parents=True)
        shutil.copy(repo / '.github/scripts/use-macos-compiler-cache.sh', root / '.github/scripts')
        for name in ('cc', 'c++'):
            executable(root / 'ccache/libexec' / name, '#!/bin/bash\nexit 0\n')
        executable(root / 'bin/brew', f'#!/bin/bash\necho "{root}/$2"\n')
        executable(root / 'configure', '#!/bin/bash\nprintf "%s\\n" "${CC:-}" > "$SEEN"\n')
        env = dict(os.environ, PATH=f'{root}/bin:{os.environ["PATH"]}', SEEN=str(root / 'seen'))
        env.pop('CC', None)
        for action, name in [('checked-build', 'Configure Unix build'), ('asan-build', 'Configure')]:
            command = step(repo / f'.github/actions/{action}/action.yml', name)
            command = command.replace('${{ inputs.platform }}', 'macos')
            result = run(['bash', '-e', '-c', command], cwd=root, env=env)
            assert result.returncode == 0, result
            assert (root / 'seen').read_text().strip() == str(root / 'ccache/libexec/cc')
            checks += 1
        executable(root / 'bin/ccache', '''#!/bin/bash
[ "${FAIL_STATS:-0}" = 0 ] || exit 42
if [ "$1" = --print-stats ]; then
  printf 'cache_miss\t%s\ndirect_cache_hit\t%s\nfiles_in_cache\t%s\n' "$MISSES" "$HITS" "$FILES"
fi
''')
        cache = root / 'cache'
        cache.mkdir()
        env.update(CCACHE_DIR=str(cache))
        for misses, hits, files, failure, expected in [(1, 0, 2, 0, True), (0, 1, 2, 0, True),
                (0, 0, 2, 0, False), (1, 0, 0, 0, False), (1, 0, 2, 1, False)]:
            result = run(['bash', str(repo / '.github/scripts/check-compiler-cache.sh')],
                         env=dict(env, MISSES=str(misses), HITS=str(hits), FILES=str(files),
                                  FAIL_STATS=str(failure)))
            assert (result.returncode == 0) == expected, result
            checks += 1


def workers():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        scripts = root / '.github/scripts'
        scripts.mkdir(parents=True)
        (root / 'test').mkdir()
        for name in ('parallel-compile.sh', 'ci-selftests.sh', 'ci-optimization-test.sh'):
            shutil.copy(repo / '.github/scripts' / name, scripts)
        shutil.copy(repo / 'test/run_lint_selftests.sh', root / 'test')
        manifest = (scripts / 'ci-selftests.sh').read_text()
        names = [str((Path('.github/scripts') / name)) for name in
                 re.findall(r'ci_compile (?:python3|bash) "\$script_dir/([^"]+)"', manifest)]
        extras = ['test/lint_layers_selftest.sh', 'test/stock_oracle_harness_test.sh']
        worker = root / 'worker.py'
        worker.write_text('''import fcntl, json, os, sys, time
from pathlib import Path
name = sys.argv[1]
state = Path(os.environ['STATE'])
for event in ['start', 'end']:
    with state.open('r+') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        data = json.load(f)
        data[event].append(name)
        data['active'] += 1 if event == 'start' else -1
        data['max'] = max(data['max'], data['active'])
        f.seek(0)
        json.dump(data, f)
        f.truncate()
    if event == 'start': time.sleep(0.08)
print(name)
sys.exit(42 if os.environ.get('FAIL') == name else 0)
''')
        for name in names + extras:
            if name.endswith('.py'):
                body = f'import runpy, sys\nsys.argv = [{str(worker)!r}, {name!r}]\nrunpy.run_path({str(worker)!r}, run_name="__main__")\n'
            else:
                body = f'#!/bin/bash\nexec python3 "{worker}" "{name}"\n'
            executable(root / name, body)
        for standalone, jobs, fail in [(False, 3, ''), (False, 1, ''), (True, 3, '')] + [
                (False, 3, name) for name in [extras[0], extras[1], names[0], names[-1]]]:
            state = root / 'state.json'
            state.write_text(json.dumps(dict(start=[], end=[], active=0, max=0)))
            command = ['bash', str(scripts / 'ci-optimization-test.sh')] if standalone else [
                'bash', str(root / 'test/run_lint_selftests.sh'), *extras]
            result = run(command, cwd=root, env=dict(os.environ, STATE=str(state), FAIL=fail,
                                                    DOLTLITE_LINT_JOBS=str(jobs)))
            data = json.loads(state.read_text())
            assert result.returncode == (42 if fail else 0), result
            assert data['active'] == 0 and sorted(data['start']) == sorted(data['end']), data
            assert 1 <= data['max'] <= jobs, data
            assert sorted(result.stdout.splitlines()) == sorted(data['end']), result
            if not fail:
                assert sorted(data['end']) == sorted(names + ([] if standalone else extras)), data
                assert data['max'] == jobs, data
            checks += 1


def header_archive():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / '.github/scripts').mkdir(parents=True)
        shutil.copy(repo / '.github/scripts/package-ci-build.sh', root / '.github/scripts')
        (root / 'build').mkdir()
        headers = ['sqlite3.h', 'sqlite_cfg.h', 'parse.h', 'opcodes.h']
        for name in headers + ['doltlite', 'unused.o']:
            (root / 'build' / name).write_text(name)
        command = step(repo / '.github/actions/checked-probes/action.yml', 'Package generated headers')
        result = run(['bash', '-e', '-c', command], cwd=root)
        assert result.returncode == 0, result
        with tarfile.open(root / 'checked-headers.tar.gz') as archive:
            assert sorted(archive.getnames()) == sorted('build/' + h for h in headers)
            for member in archive:
                assert archive.extractfile(member).read() == Path(member.name).name.encode()
        checks += 1
        for header in headers:
            (root / 'build' / header).unlink()
            assert run(['bash', '-e', '-c', command], cwd=root).returncode != 0
            (root / 'build' / header).write_text(header)
            checks += 1
        workflow = (repo / '.github/workflows/ci-build.yml').read_text().split('  no-dead-code:')[1]
        assert 'name: checked-headers\n' in workflow and 'tar -xzf checked-headers.tar.gz' in workflow
        checks += 1


def reference_cache():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name in ('.github/scripts/sqllogictest-cache-key.sh', '.github/scripts/sqllogictest-reference.sh',
                     'test/patch_sqllogictest.pl'):
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(repo / name, root / name)
        for name in ('gcc', 'as', 'ld', 'fossil', 'perl', 'dpkg-query'):
            executable(root / 'bin' / name, '''#!/bin/bash
[ "${FAIL_TOOL:-}" != "${0##*/}" ] || exit 42
echo "${0##*/} ${TOOL_VERSION:-1}"
''')
        executable(root / 'bin/sha256sum', '''#!/usr/bin/env python3
import hashlib, pathlib, sys
if len(sys.argv) == 1:
    print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest() + '  -')
else:
    for name in sys.argv[1:]:
        print(hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest() + '  ' + name)
''')
        env = dict(os.environ, PATH=f'{root}/bin:{os.environ["PATH"]}')
        command = ['bash', str(root / '.github/scripts/sqllogictest-cache-key.sh')]
        baseline = run(command, env=env)
        assert baseline.returncode == 0 and len(baseline.stdout.strip()) == 64, baseline
        assert run(command, env=env).stdout == baseline.stdout
        checks += 1
        for name, value in [('TOOL_VERSION', 'next'), ('ImageVersion', 'next'), ('CPATH', '/other')]:
            changed = run(command, env=dict(env, **{name: value}))
            assert changed.returncode == 0 and changed.stdout != baseline.stdout, changed
            checks += 1
        for name in ('gcc', 'ld', 'fossil', 'perl', 'dpkg-query'):
            assert run(command, env=dict(env, FAIL_TOOL=name)).returncode != 0
            checks += 1
        for name in ('test/patch_sqllogictest.pl', '.github/scripts/sqllogictest-reference.sh', 'bin/gcc'):
            file = root / name
            original = file.read_text()
            file.write_text(original + '\n# changed\n')
            changed = run(command, env=env)
            assert changed.returncode == 0 and changed.stdout != baseline.stdout, changed
            file.write_text(original)
            checks += 1
        identity = step(repo / '.github/actions/sqllogictest-cache/action.yml', 'Identify reference')
        output = root / 'output'
        result = run(['bash', '-e', '-c', identity], cwd=root,
                     env=dict(env, FAIL_TOOL='gcc', GITHUB_OUTPUT=str(output)))
        assert result.returncode != 0 and not output.exists(), result
        checks += 1


def reference_validation():
    global checks
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cache = root / 'cache'
        (cache / 'tree/src').mkdir(parents=True)
        (cache / 'tree/test').mkdir()
        names = ['sqllogictest.c', 'slt_odbc3.c', 'sqlite3.c', 'sqlite3.h', 'md5.o', 'sqlite3.o']
        for name in names:
            (cache / 'tree/src' / name).write_text(name)
        (cache / 'tree/test/a.test').write_text('query I\nSELECT 1\n')
        revision = re.search(r'^revision=(.*)$', (repo / '.github/scripts/sqllogictest-reference.sh').read_text(), re.M)[1]
        (cache / 'revision').write_text(revision + '\n')
        executable(cache / 'tree/src/sqllogictest-stock', '''#!/bin/bash
[ "$1" = --verify ] || exit 42
[ "${FAIL_RUNNER:-0}" = 0 ] || exit 42
printf '%s errors out of 4 tests in smoke.test - 0 skipped.\\n' "${ERRORS:-0}" >&2
''')
        (cache / 'MANIFEST').write_text(''.join(
            hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + str(p.relative_to(cache)) + '\n'
            for p in (cache / 'tree').rglob('*') if p.is_file()))
        command = ['bash', str(repo / '.github/scripts/sqllogictest-reference.sh'), 'validate', str(cache)]
        assert run(command).returncode == 0
        checks += 1
        for env in [dict(os.environ, FAIL_RUNNER='1'), dict(os.environ, ERRORS='1')]:
            assert run(command, env=env).returncode != 0
            checks += 1
        for name in names:
            file = cache / 'tree/src' / name
            file.unlink()
            assert run(command).returncode != 0
            file.write_text(name)
            checks += 1
        (cache / 'tree/test/a.test').write_text('changed corpus\n')
        assert run(command).returncode != 0
        checks += 1
        (cache / 'revision').write_text('wrong\n')
        assert run(command).returncode != 0
        checks += 1


compiler_cache()
workers()
header_archive()
reference_cache()
reference_validation()
print(f'Compiler cache wiring, shared workers, header payloads and SQLLogicTest cache: {checks} checks passed')
