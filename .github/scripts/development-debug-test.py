import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile

repo = Path(__file__).resolve().parents[2]
checks = 0


def check(condition, message):
    global checks
    assert condition, message
    checks += 1


with tempfile.TemporaryDirectory(prefix='development build ') as temp:
    root = Path(temp)
    compiler = root / 'compiler.py'
    compiler.write_text('''
import json, os, pathlib, sys
args = sys.argv[1:]
with open('commands.jsonl', 'a') as f:
    f.write(json.dumps(args) + '\\n')
if os.environ.get('FAIL_ARGUMENT') in args:
    print('injected compiler failure', file=sys.stderr)
    sys.exit(23)
if os.environ.get('FAIL_LINK') and '-c' not in args:
    print('injected linker failure', file=sys.stderr)
    sys.exit(24)
if '-c' in args:
    print('compiler diagnostic', file=sys.stderr)
pathlib.Path(args[args.index('-o') + 1]).write_text('output')
''')

    def run(extra_env=None, extra_flags=()):
        (root / 'commands.jsonl').unlink(missing_ok=True)
        (root / 'testfixture').unlink(missing_ok=True)
        result = subprocess.run([
            sys.executable, str(repo / 'tool/compile-test.py'),
            '--cc', sys.executable, str(compiler), '-g', '-I.',
            '--cflags', '-DSQLITE_TEST=1', '-DVERSION="test version"', *extra_flags,
            '--sources', 'one dir/same.c', 'two dir/same.c', 'libengine.a',
            '--ldflags', '-ltcl', '-lm', '--output', 'testfixture',
        ], cwd=root, env=dict(os.environ, **(extra_env or {})),
           capture_output=True, text=True)
        calls = [json.loads(line) for line in (root / 'commands.jsonl').read_text().splitlines()]
        return result, calls

    result, calls = run()
    check(result.returncode == 0, result.stderr)
    check(len(calls) == 3, 'two compilations and one link are required')
    check(result.stderr.count('compiler diagnostic') == 2, 'compiler diagnostics lost')
    check((root / 'testfixture').is_file(), 'output missing')
    objects = []
    for index, source in enumerate(('one dir/same.c', 'two dir/same.c')):
        args = calls[index]
        check(args[args.index('-c') + 1] == source, 'source argument changed')
        check('-DSQLITE_TEST=1' in args and '-DVERSION="test version"' in args,
              'test flags or quoting changed')
        check('-ltcl' not in args and 'libengine.a' not in args, 'link inputs reached compiler')
        obj = args[args.index('-o') + 1]
        check((root / obj).is_file(), 'object missing')
        objects.append(obj)
    check(objects[0] != objects[1], 'same-named sources overwrite one another')
    link = calls[-1]
    check(link[link.index('-o') + 2:] == objects + ['libengine.a', '-ltcl', '-lm'],
          'link order changed')
    check('-DSQLITE_TEST=1' in link, 'link flags changed')

    for flag in ('-fsanitize=address', '-fsanitize=undefined'):
        result, calls = run(extra_flags=(flag,))
        check(result.returncode == 0, result.stderr)
        check(all(flag in args for args in calls), 'sanitizer flag lost during compile or link')

    for source in ('one dir/same.c', 'two dir/same.c'):
        result, calls = run({'FAIL_ARGUMENT': source})
        check(result.returncode == 23, 'compiler failure hidden')
        check('injected compiler failure' in result.stderr, 'compiler failure diagnostic lost')
        check(all('-c' in args for args in calls), 'linked after a failed compilation')
        check(not (root / 'testfixture').exists(), 'failed build created a binary')

    result, calls = run({'FAIL_LINK': '1'})
    check(result.returncode == 24, 'linker failure hidden')
    check('injected linker failure' in result.stderr, 'linker failure diagnostic lost')
    check(not (root / 'testfixture').exists(), 'failed link created a binary')

workflow = (repo / '.github/workflows/ci-build.yml').read_text()
job = workflow.split('  development-builds:\n', 1)[1].split('  assert-build:\n', 1)[0]
check("if: matrix.configuration == 'All-Debug'" in job, 'All-Debug must use the cached build')
check(job.count("if: matrix.configuration != 'All-Debug'") == 3,
      'All-O0 must retain its install, configure and compile steps')
action = (repo / '.github/actions/development-debug-build/action.yml').read_text()
check('--buildonly --jobs 4 --config All-Debug mdevtest' in action,
      'development targets or worker limit changed')
check("DOLTLITE_SPLIT_TEST_COMPILE: '1'" in action and 'CC: ccache cc' in action,
      'compilations do not reach the cache')
check('check-compiler-cache.sh' in action, 'cache health check missing')
check('actions/cache/save@v4' in action and 'actions/cache/restore@v4' in action,
      'compiler cache must be restored and saved explicitly')
seed = (repo / '.github/workflows/seed-ci-caches.yml').read_text()
check('needs: [benchmark, compatibility, sqllogictest, development-debug,' in seed,
      'cache seeding failure is not watched')

if '--scheduler' in sys.argv:
    for configuration, expected in (
            ('All-Debug', ['fuzzcheck-asan', 'fuzzcheck-ubsan', 'testfixture',
                           'fuzzcheck', 'sessionfuzz', 'sqlite3']),
            ('All-O0', ['testfixture', 'fuzzcheck', 'sessionfuzz', 'sqlite3'])):
        for workers in (1, 4):
            with tempfile.TemporaryDirectory() as temp:
                result = subprocess.run([
                    'tclsh', str(repo / 'test/testrunner.tcl'), '--dryrun', '--buildonly',
                    '--jobs', str(workers), '--config', configuration, 'mdevtest',
                ], cwd=temp, text=True, capture_output=True)
                check(result.returncode == 0, result.stdout + result.stderr)
                log = (Path(temp) / 'testrunner.log').read_text()
                targets = [line.split('bash make.sh ', 1)[1].split()[0]
                           for line in log.splitlines() if 'bash make.sh ' in line]
                check(targets == expected, f'wrong build order or target set: {targets}')
                with sqlite3.connect(Path(temp) / 'testrunner.db') as db:
                    rows = db.execute('SELECT jobid, displayname, depid, cmd FROM jobs').fetchall()
                fixture = next(row for row in rows if '(testfixture)' in row[1])
                shell = next(row for row in rows if '(sqlite3)' in row[1])
                check(str(shell[2]) == str(fixture[0]), 'shell prerequisite lost')
                check('cp sqlite3 ' in shell[3], 'shell copy to fixture directory lost')

if '--compiler' in sys.argv:
    with tempfile.TemporaryDirectory(prefix='development-cache-') as temp:
        root = Path(temp)
        env = dict(os.environ, CCACHE_DIR=str(root / 'cache'), CCACHE_COMPILERCHECK='content')
        (root / 'value.h').write_text('#define VALUE 4\n')
        (root / 'value.c').write_text('#include "value.h"\nint value(void){return VALUE+EXTRA;}\n')
        (root / 'main.c').write_text(
            '#include <stdio.h>\nint value(void);\n'
            'int main(void){printf("%d %s\\n", value(), VERSION);return 0;}\n')

        def compile_probe(expected, extra=1, version='first'):
            result = subprocess.run([
                sys.executable, str(repo / 'tool/compile-test.py'),
                '--cc', 'ccache', 'cc', '-g', '-O0',
                '--cflags', f'-DEXTRA={extra}', f'-DVERSION="{version}"',
                '--sources', 'main.c', 'value.c', '--ldflags', '-lm', '--output', 'probe',
            ], cwd=root, env=env, capture_output=True, text=True)
            check(result.returncode == 0, result.stderr)
            result = subprocess.run([str(root / 'probe')], capture_output=True, text=True, check=True)
            check(result.stdout.strip() == expected, 'cached program has stale inputs: ' + result.stdout)

        compile_probe('5 first')
        subprocess.run(['ccache', '--zero-stats'], env=env, check=True, capture_output=True)
        compile_probe('5 first')
        result = subprocess.run(['ccache', '--print-stats'], env=env, check=True,
                                capture_output=True, text=True)
        stats = dict(line.split() for line in result.stdout.splitlines())
        hits = int(stats.get('direct_cache_hit', 0)) + int(stats.get('preprocessed_cache_hit', 0))
        check(hits == 2, f'warm compilation did not reuse both objects: {stats}')
        (root / 'value.h').write_text('#define VALUE 8\n')
        compile_probe('9 first')
        compile_probe('11 first', extra=3)
        compile_probe('11 second', extra=3, version='second')
        (root / 'value.c').write_text('#include "value.h"\nint value(void){return VALUE+EXTRA+100;}\n')
        compile_probe('111 second', extra=3, version='second')

print(f'Development build caching: {checks} checks passed')
