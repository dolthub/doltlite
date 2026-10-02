#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

repo = Path(__file__).resolve().parents[1]
checks = 0


def check(value, message):
    global checks
    assert value, message
    checks += 1


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    engine = root / 'engine'
    engine.write_text('''#!/usr/bin/env python3
import json, os, sys, time
with open(os.environ['INTEGRITY_TRACE'], 'a') as f:
    f.write(json.dumps(sys.argv[1:])+'\\n')
if 'PRAGMA integrity_check;' in sys.argv:
    expected_null = os.environ.get('INTEGRITY_INIT_NULL', os.environ.get('DOLTLITE_SYSTEM_NULL', '/dev/null'))
    if sys.argv[sys.argv.index('-init')+1] != expected_null:
        print('cannot open init file', file=sys.stderr)
        sys.exit(1)
    time.sleep(float(os.environ.get('INTEGRITY_DELAY', '0')))
    if any(a.endswith('/retired') for a in sys.argv):
        print('branch or revision "retired" not found', file=sys.stderr)
        sys.exit(1)
    print(os.environ.get('INTEGRITY_RESULT', 'ok'))
    sys.exit(int(os.environ.get('INTEGRITY_RC', '0')))
print('row')
sys.exit(int(os.environ.get('SESSION_RC', '0')))
''')
    engine.chmod(0o755)
    db = root / 'file with spaces.db'
    db.touch()
    trace = root / 'trace'
    log = root / 'failures'
    exceptions = root / 'exceptions'
    exceptions.touch()
    guard = Path(sys.argv[1]) if len(sys.argv)>1 else repo / 'test/lib/dltest_engine_guard.pl'
    env = {**os.environ, 'DLTEST_REAL_DOLTLITE': str(engine),
           'DLTEST_ENGINE_INTEGRITY_LOG': str(log),
           'DLTEST_INTEGRITY_EXPECTATIONS': str(exceptions),
           'INTEGRITY_TRACE': str(trace)}

    def run(*args, **extra):
        trace.write_text('')
        log.write_text('')
        return subprocess.run([str(guard), *map(str, args)], env={**env, **extra},
                              input='SELECT 1;', capture_output=True, text=True, timeout=5)

    result = run(db, INTEGRITY_RESULT='missing chunk')
    check(result.returncode == 125, 'matching output hid corruption')
    check(result.stdout == 'row\n', 'probe changed stdout')
    check('missing chunk' in log.read_text(), 'failure not recorded')
    calls = [json.loads(line) for line in trace.read_text().splitlines()]
    check('-readonly' in calls[1] and '-init' in calls[1], 'probe was not read-only/isolated')
    result = run(db, DOLTLITE_SYSTEM_NULL='NUL', INTEGRITY_INIT_NULL='NUL')
    check(result.returncode == 0 and len(trace.read_text().splitlines()) == 2,
          'probe did not use native Windows null device')
    timer = root / 'finished-ms'
    start = int(time.time()*1000)
    result = run(db, DLTEST_ENGINE_FINISHED_MS=str(timer), INTEGRITY_DELAY='0.4')
    finished = int(timer.read_text())
    check(start <= finished < int(time.time()*1000)-250,
          'session timing included the integrity probe')
    check(result.returncode == 0 and len(trace.read_text().splitlines()) == 2,
          'timed session did not check integrity')
    result = run(db, DLTEST_ENGINE_FINISHED_MS=str(timer), INTEGRITY_RESULT='missing chunk')
    check(result.returncode == 125 and 'missing chunk' in log.read_text(),
          'timed session accepted corruption')
    for rc in ('0', '1'):
        result = run(db, SESSION_RC=rc)
        check(result.returncode == int(rc), 'healthy file changed session status')
        check(not log.read_text(), 'healthy file recorded a failure')
    result = run(db, INTEGRITY_RC='1')
    check(result.returncode == 125, 'failed integrity invocation accepted')
    for path in (':memory:', root / 'missing.db', 'file::memory:?mode=memory'):
        result = run(path)
        check(len(trace.read_text().splitlines()) == 1, 'non-file database was probed')
    result = run('-separator', '|', '-cmd', 'SELECT 0;', db)
    check(len(trace.read_text().splitlines()) == 2, 'CLI option hid file database')
    result = run('-lookaside', '128', '64', '-mmap', '0', db)
    check(len(trace.read_text().splitlines()) == 2, 'memory option hid file database')
    result = run('--', db)
    check(len(trace.read_text().splitlines()) == 2, 'option terminator hid file database')
    result = run('file:' + str(db).replace(' ', '%20') + '?mode=ro')
    check(len(trace.read_text().splitlines()) == 2, 'file URI was not probed')
    result = run('file:' + str(db).replace(' ', '%20') + '?mode=rwc')
    calls = [json.loads(line) for line in trace.read_text().splitlines()]
    check(len(calls) == 2 and any(a.endswith('?mode=ro') for a in calls[1]),
          'writable URI was not probed read-only')
    result = run(str(db) + '/retired')
    check(result.returncode == 0 and len(trace.read_text().splitlines()) == 3,
          'retired branch did not check surviving repository')
    exceptions.write_text(str(db) + '\t^non-unique entry in index u$\n')
    result = run(db, INTEGRITY_RESULT='non-unique entry in index u')
    check(result.returncode == 0, 'declared unique violation rejected')
    result = run(db, INTEGRITY_RESULT='non-unique entry in index u\nmissing chunk')
    check(result.returncode == 125, 'exception hid unrelated damage')
    other = root / 'other.db'
    other.touch()
    result = run(other, INTEGRITY_RESULT='non-unique entry in index u')
    check(result.returncode == 125, 'exception leaked to another case')
    exceptions.write_text(str(db) + '\tskip:deliberate damaged header\n')
    result = run(db, SESSION_RC='1', INTEGRITY_RC='1')
    check(result.returncode == 1 and len(trace.read_text().splitlines()) == 1,
          'corruption fixture changed expected engine error')
    exceptions.write_text('')

    common = repo / 'test/lib/doltlite_test_common.sh'
    script = f'''source "{common}"
run_test_lastline matching 'SELECT 1;' row "$TEST_DB"
[ "$FAIL" -eq 1 ]
'''
    result = subprocess.run(['bash', '-c', script], env={**env, 'DOLTLITE': str(engine),
                            'DLTEST_SKIP_ENGINE_FLOOR': '1', 'TEST_DB': str(db),
                            'INTEGRITY_RESULT': 'missing chunk'}, capture_output=True, text=True)
    check(result.returncode == 0, 'native common helper accepted corruption')
    oracle = repo / 'test/lib/vc_oracle_common.sh'
    script = f'''DOLTLITE="{engine}"
source "{oracle}"
vc_oracle_init_execution "$TEST_ROOT"
printf 'SELECT 1;' | vc_oracle_run_doltlite --expect-error "$TEST_DB" | tail -1
pass=1; fail=0; FAILED_NAMES=''
vc_oracle_finish
'''
    result = subprocess.run(['bash', '-c', script], env={**env, 'TEST_DB': str(db),
                            'TEST_ROOT': str(root), 'SESSION_RC': '1',
                            'INTEGRITY_RESULT': 'missing chunk'}, capture_output=True, text=True)
    check(result.returncode != 0 and 'integrity failure' in result.stdout,
          'oracle expected error or pipeline hid corruption')

print(f'Engine integrity guard: {checks} checks passed')
