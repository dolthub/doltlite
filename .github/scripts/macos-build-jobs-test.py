import os
from pathlib import Path
import shutil
import subprocess
import tempfile

repo = Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    (root / 'test/lib').mkdir(parents=True)
    (root / 'build').mkdir()
    (root / 'bin').mkdir()
    shutil.copy(repo / 'test/run_doltlite_regression_case.sh', root / 'test')
    (root / 'test/lib/build_artifacts.sh').write_text(
        'dl_check_archive_flags() { printf "archive-check\\n" >> "$LOG"; }\n')
    for name in ('make', 'cc'):
        path = root / 'bin' / name
        path.write_text('''#!/bin/bash
printf '%s' "${0##*/}" >> "$LOG"
printf ' <%s>' "$@" >> "$LOG"
printf '\\n' >> "$LOG"
if [ "${0##*/}" = make ] && [ "${FAIL_MAKE:-0}" = 1 ]; then exit 42; fi
''')
        path.chmod(0o755)
    runner = root / 'build/doltlite_regression_test_c'
    runner.write_text('#!/bin/bash\nprintf "run <%s>\\n" "$*" >> "$LOG"\n')
    runner.chmod(0o755)
    env = dict(os.environ, PATH=f'{root / "bin"}:{os.environ["PATH"]}',
               LOG=str(root / 'log'), CFLAGS='-O2 -g -Werror',
               DOLTLITE_BUILD_DIR=str(root / 'build'), CC='cc')
    env.pop('DOLTLITE_REGRESSION_PREBUILT', None)
    env.pop('DOLTLITE_REGRESSION_JOBS', None)
    for jobs in ('', '2'):
        (root / 'log').write_text('')
        result = subprocess.run(['bash', str(root / 'test/run_doltlite_regression_case.sh'), 'all'],
                                env=dict(env, DOLTLITE_REGRESSION_JOBS=jobs), capture_output=True)
        assert result.returncode == 0, result
        lines = (root / 'log').read_text().splitlines()
        expected = 'make' + (' <-j2>' if jobs else '')
        expected += f' <-C> <{root / "build"}> <libdoltlite.a>'
        assert lines[0] == expected and lines[1] == 'archive-check', lines
        assert lines[2].startswith('cc <-O2> <-g> <-Werror>') and lines[3] == 'run <all>', lines
    for jobs in ('0', '-1', 'unbounded', '2 extra'):
        (root / 'log').write_text('')
        result = subprocess.run(['bash', str(root / 'test/run_doltlite_regression_case.sh')],
                                env=dict(env, DOLTLITE_REGRESSION_JOBS=jobs), capture_output=True)
        assert result.returncode != 0 and not (root / 'log').read_text(), result
    (root / 'log').write_text('')
    result = subprocess.run(['bash', str(root / 'test/run_doltlite_regression_case.sh')],
                            env=dict(env, DOLTLITE_REGRESSION_JOBS='2', FAIL_MAKE='1'), capture_output=True)
    assert result.returncode == 42 and len((root / 'log').read_text().splitlines()) == 1, result

print('Regression build parallelism: 7 configuration/failure checks passed')
